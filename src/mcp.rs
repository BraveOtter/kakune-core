//! Explicit MCP client support over local stdio and configured HTTP endpoints.
//!
//! This module neither discovers nor installs servers. A caller supplies every
//! executable, argument, working directory, and child environment value.

use std::{
    collections::BTreeMap,
    fmt, io,
    path::PathBuf,
    process::ExitStatus,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::StreamExt;
use reqwest::{
    Client, Url,
    header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderValue},
    redirect::Policy,
};
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{ChildStderr, ChildStdin, ChildStdout, Command},
    task::JoinHandle,
    time::timeout,
};

/// Default maximum size of a single newline-delimited JSON-RPC message.
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 1024 * 1024;
/// Default amount of stderr retained from an MCP server process.
pub const DEFAULT_MAX_STDERR_BYTES: usize = 64 * 1024;
/// Default maximum number of paginated `tools/list` responses accepted.
pub const DEFAULT_MAX_TOOL_PAGES: usize = 100;
/// Default maximum number of tools accepted from one server.
pub const DEFAULT_MAX_TOOLS: usize = 1024;
/// Preferred MCP protocol version for new connections.
pub const MCP_PROTOCOL_VERSION: &str = "2026-07-28";
/// Legacy protocol version used only after an explicit server rejection.
pub const MCP_LEGACY_PROTOCOL_VERSION: &str = "2025-11-25";

/// An explicitly configured local MCP stdio server.
///
/// The child process starts with an empty environment. Values in [`Self::env`]
/// are the only environment variables forwarded, including any credentials.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct McpStdioServerConfig {
    pub command: String,
    pub args: Vec<String>,
    pub current_dir: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
}

/// An explicitly configured MCP Streamable HTTP endpoint.
///
/// The optional bearer token is held only by the client and is never included
/// in diagnostic output.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct McpHttpServerConfig {
    pub endpoint: String,
    pub bearer_token: Option<String>,
}

impl fmt::Debug for McpHttpServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpHttpServerConfig")
            .field("endpoint", &self.endpoint)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Server features the caller requires before using an MCP connection.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct McpRequiredCapabilities {
    pub tools: bool,
}

/// MCP server features advertised by a successful `initialize` response.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct McpServerCapabilities {
    pub tools: bool,
}

/// Limits and timeouts for one MCP server process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpClientOptions {
    pub max_message_bytes: usize,
    pub max_stderr_bytes: usize,
    pub max_tool_pages: usize,
    pub max_tools: usize,
    pub request_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub required_capabilities: McpRequiredCapabilities,
}

impl Default for McpClientOptions {
    fn default() -> Self {
        Self {
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            max_stderr_bytes: DEFAULT_MAX_STDERR_BYTES,
            max_tool_pages: DEFAULT_MAX_TOOL_PAGES,
            max_tools: DEFAULT_MAX_TOOLS,
            request_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(5),
            required_capabilities: McpRequiredCapabilities { tools: true },
        }
    }
}

/// Server information returned by the MCP `initialize` request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpServerInfo {
    pub protocol_version: String,
    pub name: String,
    pub version: String,
    pub instructions: Option<String>,
}

/// A JSON Schema object advertised by an MCP tool.
#[derive(Clone, Debug, PartialEq)]
pub struct McpToolSchema {
    value: Value,
}

impl McpToolSchema {
    /// Creates a schema after ensuring its JSON representation is an object.
    pub fn new(value: Value) -> Result<Self, McpError> {
        if value.is_object() {
            Ok(Self { value })
        } else {
            Err(McpError::Protocol(
                "MCP tool schemas must be JSON objects".to_string(),
            ))
        }
    }

    /// Returns the original JSON Schema object.
    pub fn as_value(&self) -> &Value {
        &self.value
    }

    /// Consumes this wrapper and returns the original JSON Schema object.
    pub fn into_value(self) -> Value {
        self.value
    }
}

/// A discovered MCP tool and its declared schemas.
#[derive(Clone, Debug, PartialEq)]
pub struct McpTool {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: McpToolSchema,
    pub output_schema: Option<McpToolSchema>,
}

/// Typed content returned by an MCP tool call.
#[derive(Clone, Debug, PartialEq)]
pub enum McpToolContent {
    Text {
        text: String,
    },
    Image {
        data: String,
        mime_type: String,
    },
    Audio {
        data: String,
        mime_type: String,
    },
    ResourceLink {
        uri: String,
        name: String,
        description: Option<String>,
        mime_type: Option<String>,
        size: Option<u64>,
    },
    EmbeddedTextResource {
        uri: String,
        mime_type: Option<String>,
        text: String,
    },
    EmbeddedBlobResource {
        uri: String,
        mime_type: Option<String>,
        blob: String,
    },
}

/// A typed result returned from the MCP `tools/call` request.
#[derive(Clone, Debug, PartialEq)]
pub struct McpToolCallResult {
    pub content: Vec<McpToolContent>,
    pub structured_content: Option<Value>,
    pub is_error: bool,
}

/// A server-originated JSON-RPC notification observed while awaiting a response.
#[derive(Clone, Debug, PartialEq)]
pub struct McpNotification {
    pub method: String,
    pub params: Option<Value>,
}

/// Bounded stderr output captured from an MCP server process.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct McpStderrCapture {
    pub bytes: Vec<u8>,
    pub truncated: bool,
}

impl McpStderrCapture {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.bytes).into_owned()
    }
}

enum McpTransport {
    Stdio {
        child: Box<dyn process_wrap::tokio::ChildWrapper>,
        stdin: ChildStdin,
        stdout: BufReader<ChildStdout>,
        stderr: Arc<Mutex<McpStderrCapture>>,
        stderr_task: Option<JoinHandle<io::Result<()>>>,
    },
    Http {
        client: Client,
        endpoint: Url,
        authorization: Option<HeaderValue>,
    },
}

/// A client connected to one explicitly configured MCP server.
pub struct McpClient {
    transport: McpTransport,
    notifications: Vec<McpNotification>,
    next_request_id: u64,
    options: McpClientOptions,
    stopped: bool,
    server_info: McpServerInfo,
    server_capabilities: McpServerCapabilities,
}

impl McpClient {
    /// Starts and initializes a configured MCP stdio server.
    pub async fn connect(
        config: McpStdioServerConfig,
        options: McpClientOptions,
    ) -> Result<Self, McpError> {
        validate_config(&config)?;
        validate_options(&options)?;

        let mut command = Command::new(&config.command);
        command
            .args(&config.args)
            .env_clear()
            .envs(&config.env)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if let Some(current_dir) = &config.current_dir {
            command.current_dir(current_dir);
        }
        let mut child = crate::process_supervisor::spawn_async(command).map_err(McpError::Spawn)?;
        let stdin = child.stdin().take().ok_or(McpError::MissingPipe("stdin"))?;
        let stdout = child
            .stdout()
            .take()
            .ok_or(McpError::MissingPipe("stdout"))?;
        let stderr = child
            .stderr()
            .take()
            .ok_or(McpError::MissingPipe("stderr"))?;
        let captured_stderr = Arc::new(Mutex::new(McpStderrCapture::default()));
        Self::initialize(Self {
            transport: McpTransport::Stdio {
                child,
                stdin,
                stdout: BufReader::new(stdout),
                stderr_task: Some(capture_stderr(
                    stderr,
                    Arc::clone(&captured_stderr),
                    options.max_stderr_bytes,
                )),
                stderr: captured_stderr,
            },
            notifications: Vec::new(),
            next_request_id: 1,
            options,
            stopped: false,
            server_info: empty_server_info(),
            server_capabilities: McpServerCapabilities::default(),
        })
        .await
    }

    /// Connects to and initializes an explicitly configured HTTP MCP endpoint.
    pub async fn connect_http(
        config: McpHttpServerConfig,
        options: McpClientOptions,
    ) -> Result<Self, McpError> {
        let (endpoint, authorization) = validate_http_config(&config)?;
        validate_options(&options)?;
        let client = Client::builder()
            .timeout(options.request_timeout)
            // Never forward an explicitly configured bearer token to a redirect target.
            .redirect(Policy::none())
            .build()
            .map_err(McpError::HttpClient)?;
        Self::initialize(Self {
            transport: McpTransport::Http {
                client,
                endpoint,
                authorization,
            },
            notifications: Vec::new(),
            next_request_id: 1,
            options,
            stopped: false,
            server_info: empty_server_info(),
            server_capabilities: McpServerCapabilities::default(),
        })
        .await
    }

    async fn initialize(mut client: Self) -> Result<Self, McpError> {
        let initialization = match client.initialize_version(MCP_PROTOCOL_VERSION).await {
            Ok(value) => Ok(value),
            Err(error) if explicitly_rejected_protocol_version(&error) => {
                client.initialize_version(MCP_LEGACY_PROTOCOL_VERSION).await
            }
            Err(error) => Err(error),
        };
        let initialization = match initialization {
            Ok(value) => value,
            Err(error) => {
                let _ = client.terminate().await;
                return Err(error);
            }
        };
        let (server_info, server_capabilities) = match parse_server_info(initialization) {
            Ok(value) => value,
            Err(error) => {
                let _ = client.terminate().await;
                return Err(error);
            }
        };
        if !matches!(
            server_info.protocol_version.as_str(),
            MCP_PROTOCOL_VERSION | MCP_LEGACY_PROTOCOL_VERSION
        ) {
            let _ = client.terminate().await;
            return Err(McpError::Protocol(
                "MCP server selected an unsupported protocol version".to_string(),
            ));
        }
        if client.options.required_capabilities.tools && !server_capabilities.tools {
            let _ = client.terminate().await;
            return Err(McpError::UnsupportedCapability("tools"));
        }
        client.server_info = server_info;
        client.server_capabilities = server_capabilities;
        if let Err(error) = client
            .notify("notifications/initialized", Value::Object(Map::new()))
            .await
        {
            let _ = client.terminate().await;
            return Err(error);
        }
        Ok(client)
    }

    async fn initialize_version(&mut self, protocol_version: &str) -> Result<Value, McpError> {
        self.request(
            "initialize",
            json!({
                "protocolVersion": protocol_version,
                "capabilities": {},
                "clientInfo": {"name": "kakune-core", "version": env!("CARGO_PKG_VERSION")}
            }),
        )
        .await
    }

    /// Returns the server information negotiated during initialization.
    pub fn server_info(&self) -> &McpServerInfo {
        &self.server_info
    }

    /// Returns the features advertised by the connected MCP server.
    pub fn server_capabilities(&self) -> &McpServerCapabilities {
        &self.server_capabilities
    }

    /// Discovers all tools, following bounded pagination cursors.
    pub async fn list_tools(&mut self) -> Result<Vec<McpTool>, McpError> {
        self.require_tools()?;
        let mut tools = Vec::new();
        let mut cursor = None;
        for _ in 0..self.options.max_tool_pages {
            let mut params = Map::new();
            if let Some(value) = cursor {
                params.insert("cursor".to_string(), Value::String(value));
            }
            let response = self.request("tools/list", Value::Object(params)).await?;
            let (mut discovered, next_cursor) =
                self.parse_or_terminate(parse_tool_page(response)).await?;
            if tools.len() + discovered.len() > self.options.max_tools {
                let error = McpError::ToolLimit {
                    limit: self.options.max_tools,
                };
                let _ = self.terminate().await;
                return Err(error);
            }
            tools.append(&mut discovered);
            match next_cursor {
                Some(next) => cursor = Some(next),
                None => return Ok(tools),
            }
        }
        let error = McpError::ToolPageLimit {
            limit: self.options.max_tool_pages,
        };
        let _ = self.terminate().await;
        Err(error)
    }

    /// Calls one discovered MCP tool with a JSON object of arguments.
    pub async fn call_tool(
        &mut self,
        name: &str,
        arguments: Map<String, Value>,
    ) -> Result<McpToolCallResult, McpError> {
        self.require_tools()?;
        if name.is_empty() {
            return Err(McpError::Protocol(
                "tool name must not be empty".to_string(),
            ));
        }
        let response = self
            .request("tools/call", json!({"name": name, "arguments": arguments}))
            .await?;
        self.parse_or_terminate(parse_tool_call_result(response))
            .await
    }

    /// Terminates the child process, cancelling its current work if any.
    pub async fn cancel(&mut self) -> Result<(), McpError> {
        self.terminate().await
    }

    /// Returns notifications received while awaiting server responses.
    pub fn take_notifications(&mut self) -> Vec<McpNotification> {
        std::mem::take(&mut self.notifications)
    }

    /// Returns bounded server stderr collected so far.
    pub fn stderr(&self) -> McpStderrCapture {
        match &self.transport {
            McpTransport::Stdio { stderr, .. } => stderr
                .lock()
                .expect("MCP stderr capture lock poisoned")
                .clone(),
            McpTransport::Http { .. } => McpStderrCapture::default(),
        }
    }

    async fn parse_or_terminate<T>(&mut self, result: Result<T, McpError>) -> Result<T, McpError> {
        if result.is_err() {
            let _ = self.terminate().await;
        }
        result
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value, McpError> {
        let result = self.request_inner(method, params).await;
        if matches!(&result, Err(error) if error.is_fatal()) {
            let _ = self.terminate().await;
        }
        result
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<(), McpError> {
        self.ensure_running().await?;
        let message =
            serialize_message(json!({"jsonrpc": "2.0", "method": method, "params": params}))?;
        self.ensure_message_size(&message)?;
        match &mut self.transport {
            McpTransport::Stdio { stdin, .. } => {
                write_stdio_message(stdin, &message, self.options.request_timeout).await
            }
            McpTransport::Http {
                client,
                endpoint,
                authorization,
            } => send_http_message(
                client,
                endpoint,
                authorization.as_ref(),
                &message,
                None,
                self.options.max_message_bytes,
                self.options.request_timeout,
            )
            .await
            .map(|_| ()),
        }
    }

    async fn request_inner(&mut self, method: &str, params: Value) -> Result<Value, McpError> {
        self.ensure_running().await?;
        if method.is_empty() || method.starts_with("rpc.") {
            return Err(McpError::Protocol("request method is invalid".to_string()));
        }
        let request_id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or(McpError::RequestIdExhausted)?;
        let message = serialize_message(json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }))?;
        self.ensure_message_size(&message)?;
        let response = match &mut self.transport {
            McpTransport::Stdio { stdin, stdout, .. } => {
                write_stdio_message(stdin, &message, self.options.request_timeout).await?;
                timeout(
                    self.options.request_timeout,
                    read_stdio_response(
                        stdout,
                        &mut self.notifications,
                        request_id,
                        self.options.max_message_bytes,
                    ),
                )
                .await
                .map_err(|_| McpError::Timeout(self.options.request_timeout))??
            }
            McpTransport::Http {
                client,
                endpoint,
                authorization,
            } => send_http_message(
                client,
                endpoint,
                authorization.as_ref(),
                &message,
                Some(request_id),
                self.options.max_message_bytes,
                self.options.request_timeout,
            )
            .await?
            .ok_or_else(|| McpError::Protocol("MCP HTTP response was empty".to_string()))?,
        };
        match response {
            Response::Result(value) => Ok(value),
            Response::Error(error) => Err(McpError::Remote(error)),
        }
    }

    fn ensure_message_size(&self, message: &[u8]) -> Result<(), McpError> {
        if message.len() > self.options.max_message_bytes {
            return Err(McpError::MessageTooLarge {
                limit: self.options.max_message_bytes,
            });
        }
        Ok(())
    }

    fn require_tools(&self) -> Result<(), McpError> {
        if self.server_capabilities.tools {
            Ok(())
        } else {
            Err(McpError::UnsupportedCapability("tools"))
        }
    }

    async fn ensure_running(&mut self) -> Result<(), McpError> {
        if self.stopped {
            return Err(McpError::Stopped);
        }
        let (status, stderr_task) = match &mut self.transport {
            McpTransport::Stdio {
                child, stderr_task, ..
            } => (child.try_wait().map_err(McpError::Wait)?, stderr_task),
            McpTransport::Http { .. } => return Ok(()),
        };
        if let Some(status) = status {
            self.stopped = true;
            finish_stderr_task(stderr_task, self.options.shutdown_timeout).await?;
            return Err(McpError::Exited(status));
        }
        Ok(())
    }

    async fn terminate(&mut self) -> Result<(), McpError> {
        if self.stopped {
            return Ok(());
        }
        let result = match &mut self.transport {
            McpTransport::Stdio {
                child,
                stdin,
                stderr_task,
                ..
            } => {
                let _ = stdin.shutdown().await;
                child.start_kill().map_err(McpError::Kill)?;
                timeout(self.options.shutdown_timeout, child.wait())
                    .await
                    .map_err(|_| McpError::ShutdownTimeout(self.options.shutdown_timeout))?
                    .map_err(McpError::Wait)?;
                finish_stderr_task(stderr_task, self.options.shutdown_timeout).await
            }
            McpTransport::Http { .. } => Ok(()),
        };
        self.stopped = true;
        result
    }
}

impl Drop for McpClient {
    fn drop(&mut self) {
        if let McpTransport::Stdio {
            child, stderr_task, ..
        } = &mut self.transport
        {
            if !self.stopped {
                let _ = child.start_kill();
            }
            if let Some(task) = stderr_task.take() {
                task.abort();
            }
        }
    }
}

fn validate_config(config: &McpStdioServerConfig) -> Result<(), McpError> {
    validate_process_value("command", &config.command)?;
    for argument in &config.args {
        validate_process_value("argument", argument)?;
    }
    for (name, value) in &config.env {
        if name.is_empty() || name.contains('=') || name.contains('\0') || value.contains('\0') {
            return Err(McpError::InvalidConfig(
                "environment names must be non-empty and values must not contain NUL".to_string(),
            ));
        }
    }
    Ok(())
}

fn validate_http_config(
    config: &McpHttpServerConfig,
) -> Result<(Url, Option<HeaderValue>), McpError> {
    let endpoint = Url::parse(&config.endpoint)
        .map_err(|_| McpError::InvalidHttpConfig("endpoint must be an absolute URL".to_string()))?;
    if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
        return Err(McpError::InvalidHttpConfig(
            "endpoint must use http or https and include a host".to_string(),
        ));
    }
    if !endpoint.username().is_empty() || endpoint.password().is_some() {
        return Err(McpError::InvalidHttpConfig(
            "endpoint must not embed credentials".to_string(),
        ));
    }
    if endpoint.fragment().is_some() {
        return Err(McpError::InvalidHttpConfig(
            "endpoint must not include a fragment".to_string(),
        ));
    }
    let authorization = config
        .bearer_token
        .as_deref()
        .map(|token| {
            if token.is_empty() {
                return Err(McpError::InvalidHttpConfig(
                    "bearer token must not be empty".to_string(),
                ));
            }
            HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
                McpError::InvalidHttpConfig("bearer token contains invalid characters".to_string())
            })
        })
        .transpose()?;
    Ok((endpoint, authorization))
}

fn validate_options(options: &McpClientOptions) -> Result<(), McpError> {
    if options.max_message_bytes == 0
        || options.max_stderr_bytes == 0
        || options.max_tool_pages == 0
        || options.max_tools == 0
    {
        return Err(McpError::InvalidOption(
            "all limits must be greater than zero".to_string(),
        ));
    }
    if options.request_timeout.is_zero() || options.shutdown_timeout.is_zero() {
        return Err(McpError::InvalidOption(
            "timeouts must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

fn validate_process_value(field: &str, value: &str) -> Result<(), McpError> {
    if value.is_empty() || value.contains('\0') {
        Err(McpError::InvalidConfig(format!(
            "{field} must not be empty or contain NUL"
        )))
    } else {
        Ok(())
    }
}

fn serialize_message(value: Value) -> Result<Vec<u8>, McpError> {
    serde_json::to_vec(&value).map_err(McpError::Serialize)
}

fn empty_server_info() -> McpServerInfo {
    McpServerInfo {
        protocol_version: String::new(),
        name: String::new(),
        version: String::new(),
        instructions: None,
    }
}

fn explicitly_rejected_protocol_version(error: &McpError) -> bool {
    let McpError::Remote(error) = error else {
        return false;
    };
    let message = error.message.to_ascii_lowercase();
    matches!(error.code, -32600 | -32602)
        && message.contains("protocol")
        && (message.contains("version") || message.contains("unsupported"))
}

async fn write_stdio_message(
    stdin: &mut ChildStdin,
    message: &[u8],
    request_timeout: Duration,
) -> Result<(), McpError> {
    timeout(request_timeout, async {
        stdin.write_all(message).await.map_err(McpError::Write)?;
        stdin.write_all(b"\n").await.map_err(McpError::Write)?;
        stdin.flush().await.map_err(McpError::Write)
    })
    .await
    .map_err(|_| McpError::Timeout(request_timeout))?
}

async fn read_stdio_response(
    stdout: &mut BufReader<ChildStdout>,
    notifications: &mut Vec<McpNotification>,
    request_id: u64,
    max_message_bytes: usize,
) -> Result<Response, McpError> {
    loop {
        let message = read_message(stdout, max_message_bytes).await?;
        match parse_incoming(&message, request_id)? {
            Incoming::Notification(notification) => notifications.push(notification),
            Incoming::Response(response) => return Ok(response),
        }
    }
}

async fn send_http_message(
    client: &Client,
    endpoint: &Url,
    authorization: Option<&HeaderValue>,
    message: &[u8],
    request_id: Option<u64>,
    max_message_bytes: usize,
    request_timeout: Duration,
) -> Result<Option<Response>, McpError> {
    let mut request = client
        .post(endpoint.clone())
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "application/json")
        .body(message.to_vec());
    if let Some(authorization) = authorization {
        request = request.header(AUTHORIZATION, authorization.clone());
    }
    let response = request
        .send()
        .await
        .map_err(|error| map_http_error(error, request_timeout))?;
    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = read_http_body(response, max_message_bytes, request_timeout).await?;
    if !status.is_success() {
        if let Some(request_id) = request_id
            && is_json_content_type(content_type.as_deref())
            && let Ok(Incoming::Response(Response::Error(error))) =
                parse_incoming(&body, request_id)
        {
            return Err(McpError::Remote(error));
        }
        return Err(McpError::HttpStatus(status.as_u16()));
    }
    let Some(request_id) = request_id else {
        return Ok(None);
    };
    if !is_json_content_type(content_type.as_deref()) {
        return Err(McpError::HttpContentType);
    }
    if body.is_empty() {
        return Ok(None);
    }
    match parse_incoming(&body, request_id)? {
        Incoming::Notification(_) => Err(McpError::Protocol(
            "MCP HTTP response must be a JSON-RPC response".to_string(),
        )),
        Incoming::Response(response) => Ok(Some(response)),
    }
}

fn is_json_content_type(content_type: Option<&str>) -> bool {
    content_type.is_some_and(|value| {
        value
            .split(';')
            .next()
            .is_some_and(|mime| mime.trim() == "application/json")
    })
}

async fn read_http_body(
    response: reqwest::Response,
    max_message_bytes: usize,
    request_timeout: Duration,
) -> Result<Vec<u8>, McpError> {
    if response
        .content_length()
        .is_some_and(|length| length > max_message_bytes as u64)
    {
        return Err(McpError::MessageTooLarge {
            limit: max_message_bytes,
        });
    }
    let read = async {
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| map_http_error(error, request_timeout))?;
            if body.len().saturating_add(chunk.len()) > max_message_bytes {
                return Err(McpError::MessageTooLarge {
                    limit: max_message_bytes,
                });
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    };
    timeout(request_timeout, read)
        .await
        .map_err(|_| McpError::Timeout(request_timeout))?
}

fn map_http_error(error: reqwest::Error, request_timeout: Duration) -> McpError {
    if error.is_timeout() {
        McpError::Timeout(request_timeout)
    } else {
        McpError::Http(error)
    }
}

async fn finish_stderr_task(
    stderr_task: &mut Option<JoinHandle<io::Result<()>>>,
    shutdown_timeout: Duration,
) -> Result<(), McpError> {
    let Some(task) = stderr_task.take() else {
        return Ok(());
    };
    match timeout(shutdown_timeout, task).await {
        Ok(Ok(Ok(()))) => Ok(()),
        Ok(Ok(Err(error))) => Err(McpError::StderrRead(error)),
        Ok(Err(error)) => Err(McpError::StderrTask(error)),
        Err(_) => Err(McpError::StderrTimeout(shutdown_timeout)),
    }
}

async fn read_message(
    reader: &mut BufReader<ChildStdout>,
    max_message_bytes: usize,
) -> Result<Vec<u8>, McpError> {
    let mut message = Vec::new();
    loop {
        let buffer = reader.fill_buf().await.map_err(McpError::Read)?;
        if buffer.is_empty() {
            return Err(McpError::Protocol(
                "MCP server closed stdout before sending a complete message".to_string(),
            ));
        }
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let available = newline.unwrap_or(buffer.len());
        if message.len() + available > max_message_bytes {
            return Err(McpError::MessageTooLarge {
                limit: max_message_bytes,
            });
        }
        message.extend_from_slice(&buffer[..available]);
        reader.consume(newline.map_or(available, |index| index + 1));
        if newline.is_some() {
            if message.last() == Some(&b'\r') {
                message.pop();
            }
            if message.is_empty() {
                return Err(McpError::Protocol(
                    "MCP server sent an empty stdout line".to_string(),
                ));
            }
            return Ok(message);
        }
    }
}

fn capture_stderr(
    mut stderr: ChildStderr,
    captured: Arc<Mutex<McpStderrCapture>>,
    max_bytes: usize,
) -> JoinHandle<io::Result<()>> {
    tokio::spawn(async move {
        let mut buffer = [0_u8; 4096];
        loop {
            let read = stderr.read(&mut buffer).await?;
            if read == 0 {
                return Ok(());
            }
            let mut output = captured.lock().expect("MCP stderr capture lock poisoned");
            let retained = max_bytes.saturating_sub(output.bytes.len()).min(read);
            output.bytes.extend_from_slice(&buffer[..retained]);
            output.truncated |= retained != read;
        }
    })
}

enum Incoming {
    Notification(McpNotification),
    Response(Response),
}

enum Response {
    Result(Value),
    Error(McpRemoteError),
}

fn parse_incoming(message: &[u8], request_id: u64) -> Result<Incoming, McpError> {
    let value: Value = serde_json::from_slice(message).map_err(McpError::ProtocolJson)?;
    let object = value
        .as_object()
        .ok_or_else(|| McpError::Protocol("stdout message must be an object".to_string()))?;
    if object.get("jsonrpc") != Some(&Value::String("2.0".to_string())) {
        return Err(McpError::Protocol(
            "stdout message must declare jsonrpc 2.0".to_string(),
        ));
    }
    if let Some(method) = object.get("method") {
        if object
            .keys()
            .any(|key| key != "jsonrpc" && key != "method" && key != "params")
        {
            return Err(McpError::Protocol(
                "MCP notification contains unsupported fields".to_string(),
            ));
        }
        let method = non_empty_string(method, "notification method")?;
        if method.starts_with("rpc.") {
            return Err(McpError::Protocol(
                "notification method is invalid".to_string(),
            ));
        }
        return Ok(Incoming::Notification(McpNotification {
            method: method.to_string(),
            params: object.get("params").cloned(),
        }));
    }
    if object
        .keys()
        .any(|key| key != "jsonrpc" && key != "id" && key != "result" && key != "error")
    {
        return Err(McpError::Protocol(
            "MCP response contains unsupported fields".to_string(),
        ));
    }
    if object.get("id") != Some(&Value::from(request_id)) {
        return Err(McpError::Protocol(format!(
            "MCP response ID does not match request {request_id}"
        )));
    }
    match (object.get("result"), object.get("error")) {
        (Some(result), None) => Ok(Incoming::Response(Response::Result(result.clone()))),
        (None, Some(error)) => Ok(Incoming::Response(Response::Error(parse_remote_error(
            error,
        )?))),
        _ => Err(McpError::Protocol(
            "MCP response must contain exactly one of result or error".to_string(),
        )),
    }
}

fn parse_remote_error(value: &Value) -> Result<McpRemoteError, McpError> {
    let object = value
        .as_object()
        .ok_or_else(|| McpError::Protocol("MCP error must be an object".to_string()))?;
    let code = object
        .get("code")
        .and_then(Value::as_i64)
        .ok_or_else(|| McpError::Protocol("MCP error code must be an integer".to_string()))?;
    let message = non_empty_string(
        object
            .get("message")
            .ok_or_else(|| McpError::Protocol("MCP error is missing message".to_string()))?,
        "error message",
    )?;
    Ok(McpRemoteError {
        code,
        message: message.to_string(),
        data: object.get("data").cloned(),
    })
}

fn parse_server_info(value: Value) -> Result<(McpServerInfo, McpServerCapabilities), McpError> {
    let object = json_object(&value, "initialize result")?;
    let protocol_version = required_string(object, "protocolVersion")?.to_string();
    let capabilities = json_object(
        object.get("capabilities").ok_or_else(|| {
            McpError::Protocol("initialize result is missing capabilities".to_string())
        })?,
        "initialize capabilities",
    )?;
    let tools = match capabilities.get("tools") {
        Some(value) if value.is_object() => true,
        Some(_) => {
            return Err(McpError::Protocol(
                "initialize capabilities.tools must be an object".to_string(),
            ));
        }
        None => false,
    };
    let server_info = json_object(
        object.get("serverInfo").ok_or_else(|| {
            McpError::Protocol("initialize result is missing serverInfo".to_string())
        })?,
        "serverInfo",
    )?;
    Ok((
        McpServerInfo {
            protocol_version,
            name: required_string(server_info, "name")?.to_string(),
            version: required_string(server_info, "version")?.to_string(),
            instructions: optional_string(object, "instructions")?,
        },
        McpServerCapabilities { tools },
    ))
}

fn parse_tool_page(value: Value) -> Result<(Vec<McpTool>, Option<String>), McpError> {
    let object = json_object(&value, "tools/list result")?;
    let raw_tools = object
        .get("tools")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            McpError::Protocol("tools/list result is missing tools array".to_string())
        })?;
    let tools = raw_tools
        .iter()
        .map(parse_tool)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((tools, optional_string(object, "nextCursor")?))
}

fn parse_tool(value: &Value) -> Result<McpTool, McpError> {
    let object = json_object(value, "tool")?;
    let input_schema = object
        .get("inputSchema")
        .cloned()
        .ok_or_else(|| McpError::Protocol("tool is missing inputSchema".to_string()))?;
    let output_schema = object
        .get("outputSchema")
        .cloned()
        .map(McpToolSchema::new)
        .transpose()?;
    Ok(McpTool {
        name: required_string(object, "name")?.to_string(),
        description: optional_string(object, "description")?,
        input_schema: McpToolSchema::new(input_schema)?,
        output_schema,
    })
}

fn parse_tool_call_result(value: Value) -> Result<McpToolCallResult, McpError> {
    let object = json_object(&value, "tools/call result")?;
    let content = object
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            McpError::Protocol("tools/call result is missing content array".to_string())
        })?
        .iter()
        .map(parse_tool_content)
        .collect::<Result<Vec<_>, _>>()?;
    let is_error = match object.get("isError") {
        Some(Value::Bool(value)) => *value,
        Some(_) => return Err(McpError::Protocol("isError must be a boolean".to_string())),
        None => false,
    };
    Ok(McpToolCallResult {
        content,
        structured_content: object.get("structuredContent").cloned(),
        is_error,
    })
}

fn parse_tool_content(value: &Value) -> Result<McpToolContent, McpError> {
    let object = json_object(value, "tool content")?;
    match required_string(object, "type")? {
        "text" => Ok(McpToolContent::Text {
            text: required_string(object, "text")?.to_string(),
        }),
        "image" => Ok(McpToolContent::Image {
            data: required_string(object, "data")?.to_string(),
            mime_type: required_string(object, "mimeType")?.to_string(),
        }),
        "audio" => Ok(McpToolContent::Audio {
            data: required_string(object, "data")?.to_string(),
            mime_type: required_string(object, "mimeType")?.to_string(),
        }),
        "resource_link" => Ok(McpToolContent::ResourceLink {
            uri: required_string(object, "uri")?.to_string(),
            name: required_string(object, "name")?.to_string(),
            description: optional_string(object, "description")?,
            mime_type: optional_string(object, "mimeType")?,
            size: optional_u64(object, "size")?,
        }),
        "resource" => parse_embedded_resource(object),
        other => Err(McpError::Protocol(format!(
            "unsupported MCP tool content type {other}"
        ))),
    }
}

fn parse_embedded_resource(object: &Map<String, Value>) -> Result<McpToolContent, McpError> {
    let resource = json_object(
        object.get("resource").ok_or_else(|| {
            McpError::Protocol("resource content is missing resource".to_string())
        })?,
        "resource content",
    )?;
    let uri = required_string(resource, "uri")?.to_string();
    let mime_type = optional_string(resource, "mimeType")?;
    match (resource.get("text"), resource.get("blob")) {
        (Some(Value::String(text)), None) => Ok(McpToolContent::EmbeddedTextResource {
            uri,
            mime_type,
            text: text.clone(),
        }),
        (None, Some(Value::String(blob))) => Ok(McpToolContent::EmbeddedBlobResource {
            uri,
            mime_type,
            blob: blob.clone(),
        }),
        _ => Err(McpError::Protocol(
            "embedded resource must contain exactly one text or blob value".to_string(),
        )),
    }
}

fn json_object<'a>(value: &'a Value, context: &str) -> Result<&'a Map<String, Value>, McpError> {
    value
        .as_object()
        .ok_or_else(|| McpError::Protocol(format!("{context} must be an object")))
}

fn required_string<'a>(object: &'a Map<String, Value>, key: &str) -> Result<&'a str, McpError> {
    object
        .get(key)
        .ok_or_else(|| McpError::Protocol(format!("missing {key}")))
        .and_then(|value| non_empty_string(value, key))
}

fn optional_string(object: &Map<String, Value>, key: &str) -> Result<Option<String>, McpError> {
    match object.get(key) {
        Some(value) => Ok(Some(non_empty_string(value, key)?.to_string())),
        None => Ok(None),
    }
}

fn optional_u64(object: &Map<String, Value>, key: &str) -> Result<Option<u64>, McpError> {
    match object.get(key) {
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| McpError::Protocol(format!("{key} must be an unsigned integer"))),
        None => Ok(None),
    }
}

fn non_empty_string<'a>(value: &'a Value, context: &str) -> Result<&'a str, McpError> {
    value
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| McpError::Protocol(format!("{context} must be a non-empty string")))
}

/// An error returned by an MCP server through JSON-RPC.
#[derive(Clone, Debug, PartialEq)]
pub struct McpRemoteError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl fmt::Display for McpRemoteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} ({})", self.message, self.code)
    }
}

/// Failures while starting, communicating with, or cancelling an MCP server.
#[derive(Debug, Error)]
pub enum McpError {
    #[error("invalid MCP stdio configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid MCP HTTP configuration: {0}")]
    InvalidHttpConfig(String),
    #[error("invalid MCP client option: {0}")]
    InvalidOption(String),
    #[error("cannot start MCP server: {0}")]
    Spawn(#[source] io::Error),
    #[error("MCP server {0} pipe was unavailable")]
    MissingPipe(&'static str),
    #[error("cannot write MCP server stdin: {0}")]
    Write(#[source] io::Error),
    #[error("cannot read MCP server stdout: {0}")]
    Read(#[source] io::Error),
    #[error("cannot wait for MCP server: {0}")]
    Wait(#[source] io::Error),
    #[error("cannot terminate MCP server: {0}")]
    Kill(#[source] io::Error),
    #[error("cannot serialize MCP JSON-RPC request: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("cannot create MCP HTTP client: {0}")]
    HttpClient(#[source] reqwest::Error),
    #[error("MCP HTTP request failed: {0}")]
    Http(#[source] reqwest::Error),
    #[error("MCP HTTP endpoint returned status {0}")]
    HttpStatus(u16),
    #[error("MCP HTTP response must have content type application/json")]
    HttpContentType,
    #[error("MCP stdout is not valid JSON: {0}")]
    ProtocolJson(#[source] serde_json::Error),
    #[error("MCP protocol error: {0}")]
    Protocol(String),
    #[error("MCP message exceeds the {limit}-byte limit")]
    MessageTooLarge { limit: usize },
    #[error("MCP request timed out after {0:?}")]
    Timeout(Duration),
    #[error("MCP request ID space is exhausted")]
    RequestIdExhausted,
    #[error("MCP server returned JSON-RPC error: {0}")]
    Remote(McpRemoteError),
    #[error("MCP server has stopped")]
    Stopped,
    #[error("MCP server exited with status {0}")]
    Exited(ExitStatus),
    #[error("MCP server did not exit within {0:?} after cancellation")]
    ShutdownTimeout(Duration),
    #[error("cannot capture MCP server stderr: {0}")]
    StderrRead(#[source] io::Error),
    #[error("MCP stderr task failed: {0}")]
    StderrTask(#[source] tokio::task::JoinError),
    #[error("MCP stderr task did not finish within {0:?}")]
    StderrTimeout(Duration),
    #[error("MCP tools/list exceeded the {limit}-page limit")]
    ToolPageLimit { limit: usize },
    #[error("MCP tools/list exceeded the {limit}-tool limit")]
    ToolLimit { limit: usize },
    #[error("MCP server does not support required capability {0}")]
    UnsupportedCapability(&'static str),
}

impl McpError {
    fn is_fatal(&self) -> bool {
        matches!(
            self,
            Self::Write(_)
                | Self::Read(_)
                | Self::ProtocolJson(_)
                | Self::Protocol(_)
                | Self::MessageTooLarge { .. }
                | Self::Timeout(_)
                | Self::Http(_)
                | Self::HttpStatus(_)
                | Self::HttpContentType
                | Self::Exited(_)
        )
    }
}
