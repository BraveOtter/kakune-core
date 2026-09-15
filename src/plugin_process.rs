//! Explicit host for process-based Kakune plugins.
//!
//! Loading a manifest has no side effects. Callers must explicitly start a
//! [`PluginHost`] from a validated manifest before a plugin process can run.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt, fs, io,
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{ChildStderr, ChildStdin, ChildStdout, Command},
    task::JoinHandle,
    time::timeout,
};

/// The largest supported static manifest file.
pub const MAX_MANIFEST_BYTES: usize = 1024 * 1024;
/// Default maximum size for one newline-delimited JSON-RPC message.
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 1024 * 1024;
/// Default amount of plugin stderr retained by the host.
pub const DEFAULT_MAX_STDERR_BYTES: usize = 64 * 1024;

/// A statically loaded plugin manifest that has passed process-runtime validation.
#[derive(Clone, Debug)]
pub struct PluginManifest {
    pub manifest_version: String,
    pub id: String,
    pub name: String,
    pub version: String,
    pub plugin_protocol: String,
    pub runtime: ProcessRuntime,
    pub author: Option<PluginAuthor>,
    pub description: Option<BTreeMap<String, String>>,
    pub license: Option<String>,
    pub model: Option<String>,
    pub requires: Option<PluginRequirements>,
    pub contributes: Option<PluginContributions>,
    pub permissions: Option<PluginPermissions>,
    manifest_dir: PathBuf,
}

impl PluginManifest {
    /// Loads and validates a static JSON manifest without executing it.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, PluginHostError> {
        let path = path.as_ref();
        let metadata = fs::metadata(path).map_err(|source| PluginHostError::ManifestRead {
            path: path.to_path_buf(),
            source,
        })?;
        let size =
            usize::try_from(metadata.len()).map_err(|_| PluginHostError::ManifestTooLarge {
                limit: MAX_MANIFEST_BYTES,
            })?;
        if size > MAX_MANIFEST_BYTES {
            return Err(PluginHostError::ManifestTooLarge {
                limit: MAX_MANIFEST_BYTES,
            });
        }

        let source = fs::read_to_string(path).map_err(|source| PluginHostError::ManifestRead {
            path: path.to_path_buf(),
            source,
        })?;
        let manifest: RawPluginManifest = serde_json::from_str(&source)
            .map_err(|source| PluginHostError::ManifestParse { source })?;
        manifest.validate(path.parent().unwrap_or_else(|| Path::new(".")))
    }

    fn load_node_definition(
        &self,
        declaration: &str,
    ) -> Result<PluginNodeDefinition, PluginRegistryError> {
        let path = declared_definition_path(&self.manifest_dir, declaration)?;
        let metadata =
            fs::metadata(&path).map_err(|source| PluginRegistryError::DefinitionRead {
                path: path.clone(),
                source,
            })?;
        let size = usize::try_from(metadata.len()).map_err(|_| {
            PluginRegistryError::DefinitionTooLarge {
                path: path.clone(),
                limit: MAX_MANIFEST_BYTES,
            }
        })?;
        if size > MAX_MANIFEST_BYTES {
            return Err(PluginRegistryError::DefinitionTooLarge {
                path,
                limit: MAX_MANIFEST_BYTES,
            });
        }
        let source =
            fs::read_to_string(&path).map_err(|source| PluginRegistryError::DefinitionRead {
                path: path.clone(),
                source,
            })?;
        let definition =
            serde_json::from_str::<RawPluginNodeDefinition>(&source).map_err(|source| {
                PluginRegistryError::DefinitionParse {
                    path: path.clone(),
                    source,
                }
            })?;
        definition.validate(&path)
    }
}

/// Runtime declaration supported by the initial plugin host.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProcessRuntime {
    #[serde(rename = "kind")]
    kind: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
}

/// Plugin author metadata.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginAuthor {
    pub name: String,
}

/// Required external executables declared by a plugin.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginRequirements {
    #[serde(default)]
    pub executables: Vec<RequiredExecutable>,
}

/// A required executable declaration.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequiredExecutable {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub version_args: Vec<String>,
}

/// Plugin contributions declared in its manifest.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginContributions {
    #[serde(default)]
    pub nodes: Vec<String>,
    #[serde(default)]
    pub triggers: Vec<String>,
    #[serde(default)]
    pub providers: Vec<String>,
    #[serde(default)]
    pub services: Vec<String>,
}

/// Plugin permissions declared in its manifest.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PluginPermissions {
    #[serde(default)]
    pub process: Vec<String>,
    #[serde(default)]
    pub filesystem: Vec<String>,
    #[serde(default)]
    pub network: Vec<String>,
    #[serde(default)]
    pub secrets: Vec<String>,
}

impl PluginPermissions {
    pub fn validate(&self) -> Result<(), PluginHostError> {
        for (kind, entries) in [
            ("process", &self.process),
            ("filesystem", &self.filesystem),
            ("network", &self.network),
            ("secrets", &self.secrets),
        ] {
            if entries
                .iter()
                .any(|entry| entry.trim().is_empty() || entry.len() > 512)
            {
                return Err(PluginHostError::ManifestValidation(format!(
                    "permissions.{kind} entries must be non-empty and at most 512 characters"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawPluginManifest {
    manifest_version: String,
    id: String,
    name: String,
    version: String,
    plugin_protocol: String,
    runtime: ProcessRuntime,
    author: Option<PluginAuthor>,
    description: Option<BTreeMap<String, String>>,
    license: Option<String>,
    model: Option<String>,
    requires: Option<PluginRequirements>,
    contributes: Option<PluginContributions>,
    permissions: Option<PluginPermissions>,
}

impl RawPluginManifest {
    fn validate(self, manifest_dir: &Path) -> Result<PluginManifest, PluginHostError> {
        validate_plugin_id(&self.id)?;
        validate_non_empty("name", &self.name)?;
        validate_semver(&self.version)?;
        if self.manifest_version != "1.0" {
            return Err(PluginHostError::ManifestValidation(
                "manifestVersion must be exactly 1.0".to_string(),
            ));
        }
        if self.plugin_protocol != ">=1.0.0 <2.0.0" {
            return Err(PluginHostError::ManifestValidation(
                "pluginProtocol must be exactly >=1.0.0 <2.0.0".to_string(),
            ));
        }
        if self.runtime.kind != "process" {
            return Err(PluginHostError::ManifestValidation(
                "runtime.kind must be process".to_string(),
            ));
        }
        if let Some(permissions) = &self.permissions {
            permissions.validate()?;
        }
        validate_process_value("runtime.command", &self.runtime.command)?;
        for argument in &self.runtime.args {
            validate_process_value("runtime.args", argument)?;
        }
        if let Some(author) = &self.author {
            validate_non_empty("author.name", &author.name)?;
        }
        if let Some(description) = &self.description
            && (description.is_empty() || description.values().any(|text| text.trim().is_empty()))
        {
            return Err(PluginHostError::ManifestValidation(
                "description must contain non-empty localized text".to_string(),
            ));
        }
        if let Some(contributes) = &self.contributes {
            let mut declared_nodes = std::collections::HashSet::new();
            for node in &contributes.nodes {
                validate_definition_declaration(node)?;
                if !declared_nodes.insert(node) {
                    return Err(PluginHostError::ManifestValidation(format!(
                        "contributes.nodes contains duplicate definition {node}"
                    )));
                }
            }
        }

        Ok(PluginManifest {
            manifest_version: self.manifest_version,
            id: self.id,
            name: self.name,
            version: self.version,
            plugin_protocol: self.plugin_protocol,
            runtime: self.runtime,
            author: self.author,
            description: self.description,
            license: self.license,
            model: self.model,
            requires: self.requires,
            contributes: self.contributes,
            permissions: self.permissions,
            manifest_dir: manifest_dir.to_path_buf(),
        })
    }
}

/// A node definition declared by a process plugin.
///
/// Definitions are static metadata loaded only while a caller explicitly
/// registers a previously validated [`PluginManifest`]. The schemas are data
/// for callers and are never evaluated as code by Core.
#[derive(Clone, Debug)]
pub struct PluginNodeDefinition {
    pub node_type: String,
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
    pub output_schema: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawPluginNodeDefinition {
    api_version: String,
    kind: String,
    #[serde(rename = "type")]
    node_type: String,
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default = "empty_json_object")]
    input_schema: Value,
    #[serde(default = "empty_json_object")]
    output_schema: Value,
}

impl RawPluginNodeDefinition {
    fn validate(self, path: &Path) -> Result<PluginNodeDefinition, PluginRegistryError> {
        if self.api_version != "kakune.dev/v1" {
            return Err(PluginRegistryError::DefinitionValidation {
                path: path.to_path_buf(),
                message: "apiVersion must be kakune.dev/v1".to_string(),
            });
        }
        if self.kind != "NodeDefinition" {
            return Err(PluginRegistryError::DefinitionValidation {
                path: path.to_path_buf(),
                message: "kind must be NodeDefinition".to_string(),
            });
        }
        validate_node_type(&self.node_type).map_err(|message| {
            PluginRegistryError::DefinitionValidation {
                path: path.to_path_buf(),
                message,
            }
        })?;
        if self.name.trim().is_empty() {
            return Err(PluginRegistryError::DefinitionValidation {
                path: path.to_path_buf(),
                message: "name must not be empty".to_string(),
            });
        }
        if self
            .description
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(PluginRegistryError::DefinitionValidation {
                path: path.to_path_buf(),
                message: "description must not be empty when provided".to_string(),
            });
        }
        if !self.input_schema.is_object() || !self.output_schema.is_object() {
            return Err(PluginRegistryError::DefinitionValidation {
                path: path.to_path_buf(),
                message: "inputSchema and outputSchema must be JSON objects".to_string(),
            });
        }
        Ok(PluginNodeDefinition {
            node_type: self.node_type,
            name: self.name,
            description: self.description,
            input_schema: self.input_schema,
            output_schema: self.output_schema,
        })
    }
}

fn empty_json_object() -> Value {
    Value::Object(Map::new())
}

/// A node registered explicitly from a local plugin manifest and definition.
#[derive(Clone, Debug)]
pub struct RegisteredPluginNode {
    pub manifest_id: String,
    pub definition: PluginNodeDefinition,
    manifest: PluginManifest,
    options: PluginHostOptions,
}

impl RegisteredPluginNode {
    pub fn uses_secrets(&self) -> bool {
        self.manifest
            .permissions
            .as_ref()
            .is_some_and(|permissions| !permissions.secrets.is_empty())
    }
    /// Starts an isolated plugin process for one node invocation.
    pub async fn execute(
        &self,
        store: crate::Store,
        execution_id: &str,
        node_id: &str,
        inputs: BTreeMap<String, Value>,
    ) -> Result<PluginNodeResult, PluginNodeExecutionError> {
        let mut host = PluginHost::start(&self.manifest, self.options.clone())
            .await
            .map_err(PluginNodeExecutionError::Host)?;
        host.set_host_handler(crate::plugin_services::handler(
            store.clone(),
            execution_id.to_string(),
            node_id.to_string(),
            self.manifest.permissions.clone(),
        ));
        let operation_id = uuid::Uuid::new_v4().to_string();
        let response = {
            let request = host.request(
                "node/execute",
                json!({
                    "executionId": execution_id,
                    "nodeId": node_id,
                    "nodeType": self.definition.node_type,
                    "operationId": operation_id,
                    "inputs": inputs,
                }),
            );
            tokio::pin!(request);
            let cancellation = wait_for_execution_cancellation(store, execution_id.to_string());
            tokio::pin!(cancellation);
            tokio::select! {
                response = &mut request => Some(response),
                _ = &mut cancellation => None,
            }
        };
        let Some(response) = response else {
            let _ = host.cancel_operation(&operation_id).await;
            return Err(PluginNodeExecutionError::Cancelled);
        };
        let shutdown = host.shutdown().await;
        let response = response.map_err(PluginNodeExecutionError::Host)?;
        let result = parse_node_result(response).map_err(PluginNodeExecutionError::Result)?;
        // The operation result is already durable at this point. A plugin that
        // exits non-zero while its isolated host is being reaped must not turn
        // a successful node invocation into a false failure.
        if let Err(error) = shutdown {
            eprintln!("kakune plugin {} shutdown: {error}", self.manifest_id);
        }
        Ok(result)
    }
}

/// The normalized result returned by `node/execute`.
#[derive(Clone, Debug, PartialEq)]
pub struct PluginNodeResult {
    pub route: String,
    pub outputs: BTreeMap<String, Value>,
    pub message: Option<String>,
}

/// Failures while executing a registered plugin node.
#[derive(Debug, Error)]
pub enum PluginNodeExecutionError {
    #[error("plugin host error: {0}")]
    Host(#[source] PluginHostError),
    #[error("invalid node/execute result: {0}")]
    Result(String),
    #[error("plugin operation was cancelled")]
    Cancelled,
}

/// Local, in-memory registry of explicitly registered plugin node definitions.
///
/// This type does not discover, download, install, or activate plugins. A
/// caller must load a manifest and invoke one of the registration methods.
#[derive(Clone, Debug, Default)]
pub struct PluginRegistry {
    nodes: BTreeMap<String, RegisteredPluginNode>,
}

impl PluginRegistry {
    /// Loads a manifest from disk and explicitly registers its declared node definitions.
    pub fn register_manifest_path(
        &mut self,
        path: impl AsRef<Path>,
    ) -> Result<(), PluginRegistryError> {
        let manifest = PluginManifest::load(path).map_err(PluginRegistryError::Manifest)?;
        self.register_manifest(manifest)
    }

    /// Registers all node definitions declared by a validated manifest.
    pub fn register_manifest(
        &mut self,
        manifest: PluginManifest,
    ) -> Result<(), PluginRegistryError> {
        self.register_manifest_with_options(manifest, PluginHostOptions::default())
    }

    /// Registers all node definitions with explicit process-host limits.
    pub fn register_manifest_with_options(
        &mut self,
        manifest: PluginManifest,
        options: PluginHostOptions,
    ) -> Result<(), PluginRegistryError> {
        let declarations = manifest
            .contributes
            .as_ref()
            .map(|contributes| &contributes.nodes)
            .ok_or_else(|| PluginRegistryError::NoNodeDefinitions(manifest.id.clone()))?;
        if declarations.is_empty() {
            return Err(PluginRegistryError::NoNodeDefinitions(manifest.id.clone()));
        }

        let mut additions = Vec::with_capacity(declarations.len());
        for declaration in declarations {
            let definition = manifest.load_node_definition(declaration)?;
            if crate::runtime::is_builtin_node_type(&definition.node_type) {
                return Err(PluginRegistryError::BuiltinNodeType(definition.node_type));
            }
            if self.nodes.contains_key(&definition.node_type)
                || additions.iter().any(|node: &RegisteredPluginNode| {
                    node.definition.node_type == definition.node_type
                })
            {
                return Err(PluginRegistryError::DuplicateNodeType(definition.node_type));
            }
            additions.push(RegisteredPluginNode {
                manifest_id: manifest.id.clone(),
                definition,
                manifest: manifest.clone(),
                options: options.clone(),
            });
        }
        for node in additions {
            self.nodes.insert(node.definition.node_type.clone(), node);
        }
        Ok(())
    }

    /// Finds a registered external node by its exact workflow node type.
    pub fn get(&self, node_type: &str) -> Option<&RegisteredPluginNode> {
        self.nodes.get(node_type)
    }
}

/// Failures while explicitly loading node definitions into a local registry.
#[derive(Debug, Error)]
pub enum PluginRegistryError {
    #[error("cannot load plugin manifest: {0}")]
    Manifest(#[source] PluginHostError),
    #[error("plugin {0} declares no node definitions")]
    NoNodeDefinitions(String),
    #[error("cannot read node definition {path}: {source}")]
    DefinitionRead { path: PathBuf, source: io::Error },
    #[error("node definition {path} exceeds the {limit}-byte limit")]
    DefinitionTooLarge { path: PathBuf, limit: usize },
    #[error("node definition {path} is not valid JSON: {source}")]
    DefinitionParse {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("invalid node definition {path}: {message}")]
    DefinitionValidation { path: PathBuf, message: String },
    #[error("node definition path {0} escapes its plugin directory")]
    DefinitionPath(String),
    #[error("plugin node type {0} is already provided by the built-in catalog")]
    BuiltinNodeType(String),
    #[error("plugin node type {0} is already registered")]
    DuplicateNodeType(String),
}

/// Limits and deadlines for one plugin host process.
#[derive(Clone, Debug)]
pub struct PluginHostOptions {
    pub max_message_bytes: usize,
    pub max_stderr_bytes: usize,
    pub request_timeout: Duration,
    pub shutdown_timeout: Duration,
}

impl Default for PluginHostOptions {
    fn default() -> Self {
        Self {
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            max_stderr_bytes: DEFAULT_MAX_STDERR_BYTES,
            request_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

/// A notification received while waiting for a Core-originated response.
#[derive(Clone, Debug, PartialEq)]
pub struct PluginNotification {
    pub method: String,
    pub params: Option<Value>,
}

/// Bounded stderr output captured from a plugin process.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StderrCapture {
    pub bytes: Vec<u8>,
    pub truncated: bool,
}

impl StderrCapture {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.bytes).into_owned()
    }
}

/// A process host for a single explicitly selected plugin manifest.
pub type HostRequestHandler = Arc<
    dyn Fn(
            String,
            Value,
        )
            -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send>>
        + Send
        + Sync,
>;

pub struct PluginHost {
    host_handler: Option<HostRequestHandler>,
    child: Box<dyn process_wrap::tokio::ChildWrapper>,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr: Arc<Mutex<StderrCapture>>,
    stderr_task: Option<JoinHandle<io::Result<()>>>,
    notifications: VecDeque<PluginNotification>,
    next_request_id: u64,
    options: PluginHostOptions,
    stopped: bool,
}

impl PluginHost {
    /// Starts the process declared by a previously loaded manifest.
    pub async fn start(
        manifest: &PluginManifest,
        options: PluginHostOptions,
    ) -> Result<Self, PluginHostError> {
        if options.max_message_bytes == 0 || options.max_stderr_bytes == 0 {
            return Err(PluginHostError::InvalidOption(
                "message and stderr limits must be greater than zero".to_string(),
            ));
        }
        if options.request_timeout.is_zero() || options.shutdown_timeout.is_zero() {
            return Err(PluginHostError::InvalidOption(
                "request and shutdown timeouts must be greater than zero".to_string(),
            ));
        }

        let mut command = Command::new(&manifest.runtime.command);
        command
            .args(&manifest.runtime.args)
            .current_dir(&manifest.manifest_dir)
            // Plugins never inherit Core credentials or the user environment.
            .env_clear()
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        // Windows process creation and language launchers require these system
        // variables. They are copied individually, not inherited wholesale.
        for key in ["PATH", "SystemRoot", "WINDIR", "ComSpec"] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        let mut child =
            crate::process_supervisor::spawn_async(command).map_err(PluginHostError::Spawn)?;
        let stdin = child
            .stdin()
            .take()
            .ok_or(PluginHostError::MissingPipe("stdin"))?;
        let stdout = child
            .stdout()
            .take()
            .ok_or(PluginHostError::MissingPipe("stdout"))?;
        let stderr = child
            .stderr()
            .take()
            .ok_or(PluginHostError::MissingPipe("stderr"))?;
        let captured_stderr = Arc::new(Mutex::new(StderrCapture::default()));

        Ok(Self {
            host_handler: None,
            child,
            stdin,
            stdout: BufReader::new(stdout),
            stderr_task: Some(capture_stderr(
                stderr,
                Arc::clone(&captured_stderr),
                options.max_stderr_bytes,
            )),
            stderr: captured_stderr,
            notifications: VecDeque::new(),
            next_request_id: 1,
            options,
            stopped: false,
        })
    }

    /// Installs the Core services available to this isolated plugin operation.
    pub fn set_host_handler(&mut self, handler: HostRequestHandler) {
        self.host_handler = Some(handler);
    }

    /// Sends a JSON-RPC request and waits for its matching response.
    pub async fn request(&mut self, method: &str, params: Value) -> Result<Value, PluginHostError> {
        let result = self.request_inner(method, params).await;
        if matches!(&result, Err(error) if error.is_fatal_protocol_error()) {
            let _ = self.terminate().await;
        }
        result
    }

    /// Sends a JSON-RPC notification. Notifications are used for cancellation
    /// because a plugin may be busy handling the original request.
    pub async fn notify(&mut self, method: &str, params: Value) -> Result<(), PluginHostError> {
        self.ensure_running().await?;
        if method.is_empty() || method.starts_with("rpc.") {
            return Err(PluginHostError::Protocol(
                "notification method is invalid".to_string(),
            ));
        }
        let message =
            serde_json::to_vec(&json!({ "jsonrpc": "2.0", "method": method, "params": params }))
                .map_err(PluginHostError::Serialize)?;
        if message.len() > self.options.max_message_bytes {
            return Err(PluginHostError::MessageTooLarge {
                limit: self.options.max_message_bytes,
            });
        }
        self.stdin
            .write_all(&message)
            .await
            .map_err(PluginHostError::Write)?;
        self.stdin
            .write_all(b"\n")
            .await
            .map_err(PluginHostError::Write)?;
        self.stdin.flush().await.map_err(PluginHostError::Write)
    }

    /// Gives a cooperative plugin its operation ID, then forcefully cleans up
    /// the isolated host after the configured grace period.
    pub async fn cancel_operation(&mut self, operation_id: &str) -> Result<(), PluginHostError> {
        let _ = self
            .notify("operation/cancel", json!({ "operationId": operation_id }))
            .await;
        self.terminate().await
    }

    /// Returns notifications received while awaiting request responses.
    pub fn take_notifications(&mut self) -> Vec<PluginNotification> {
        self.notifications.drain(..).collect()
    }

    /// Returns the bounded stderr capture collected so far.
    pub fn stderr(&self) -> StderrCapture {
        self.stderr
            .lock()
            .expect("stderr capture lock poisoned")
            .clone()
    }

    /// Requests graceful shutdown, then closes stdin and waits for process exit.
    pub async fn shutdown(&mut self) -> Result<(), PluginHostError> {
        if self.stopped {
            return Ok(());
        }

        let request_result = self.request_inner("shutdown", Value::Null).await;
        let stop_result = self.terminate().await;
        request_result?;
        stop_result
    }

    async fn request_inner(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<Value, PluginHostError> {
        self.ensure_running().await?;
        if method.is_empty() || method.starts_with("rpc.") {
            return Err(PluginHostError::Protocol(
                "request method must be non-empty and must not start with rpc.".to_string(),
            ));
        }
        let request_id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or(PluginHostError::RequestIdExhausted)?;
        let message = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }))
        .map_err(PluginHostError::Serialize)?;
        if message.len() > self.options.max_message_bytes {
            return Err(PluginHostError::MessageTooLarge {
                limit: self.options.max_message_bytes,
            });
        }
        self.stdin
            .write_all(&message)
            .await
            .map_err(PluginHostError::Write)?;
        self.stdin
            .write_all(b"\n")
            .await
            .map_err(PluginHostError::Write)?;
        self.stdin.flush().await.map_err(PluginHostError::Write)?;

        let response = timeout(self.options.request_timeout, self.read_response(request_id))
            .await
            .map_err(|_| PluginHostError::Timeout(self.options.request_timeout))??;
        match response {
            Response::Result(result) => Ok(result),
            Response::Error(error) => Err(PluginHostError::Remote(error)),
        }
    }

    async fn read_response(&mut self, request_id: u64) -> Result<Response, PluginHostError> {
        loop {
            let line = read_message(&mut self.stdout, self.options.max_message_bytes).await?;
            match parse_incoming(&line, request_id)? {
                IncomingMessage::Notification(notification) => {
                    if self.notifications.len() >= 256 {
                        self.notifications.pop_front();
                    }
                    self.notifications.push_back(notification)
                }
                IncomingMessage::Request { id, method, params } => {
                    let result = if let Some(handler) = &self.host_handler {
                        handler(method, params).await
                    } else {
                        Err("Core service is unavailable for this operation".to_string())
                    };
                    let response = match result {
                        Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
                        Err(message) => {
                            json!({"jsonrpc":"2.0","id":id,"error":{"code":-32000,"message":message}})
                        }
                    };
                    let encoded =
                        serde_json::to_vec(&response).map_err(PluginHostError::Serialize)?;
                    if encoded.len() > self.options.max_message_bytes {
                        return Err(PluginHostError::MessageTooLarge {
                            limit: self.options.max_message_bytes,
                        });
                    }
                    self.stdin
                        .write_all(&encoded)
                        .await
                        .map_err(PluginHostError::Write)?;
                    self.stdin
                        .write_all(b"\n")
                        .await
                        .map_err(PluginHostError::Write)?;
                    self.stdin.flush().await.map_err(PluginHostError::Write)?;
                }
                IncomingMessage::Response(response) => return Ok(response),
            }
        }
    }

    async fn ensure_running(&mut self) -> Result<(), PluginHostError> {
        if self.stopped {
            return Err(PluginHostError::Stopped);
        }
        if let Some(status) = self.child.try_wait().map_err(PluginHostError::Wait)? {
            self.stopped = true;
            self.finish_stderr().await?;
            return Err(PluginHostError::Exited(status));
        }
        Ok(())
    }

    async fn terminate(&mut self) -> Result<(), PluginHostError> {
        if self.stopped {
            return Ok(());
        }
        let _ = self.stdin.shutdown().await;
        let status = match timeout(self.options.shutdown_timeout, self.child.wait()).await {
            Ok(result) => result.map_err(PluginHostError::Wait)?,
            Err(_) => {
                self.child.start_kill().map_err(PluginHostError::Kill)?;
                timeout(self.options.shutdown_timeout, self.child.wait())
                    .await
                    .map_err(|_| PluginHostError::ShutdownTimeout(self.options.shutdown_timeout))?
                    .map_err(PluginHostError::Wait)?
            }
        };
        self.stopped = true;
        self.finish_stderr().await?;
        if status.success() {
            Ok(())
        } else {
            Err(PluginHostError::Exited(status))
        }
    }

    async fn finish_stderr(&mut self) -> Result<(), PluginHostError> {
        let Some(task) = self.stderr_task.take() else {
            return Ok(());
        };
        match timeout(self.options.shutdown_timeout, task).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(source))) => Err(PluginHostError::StderrRead(source)),
            Ok(Err(source)) => Err(PluginHostError::StderrTask(source)),
            Err(_) => Err(PluginHostError::StderrTimeout(
                self.options.shutdown_timeout,
            )),
        }
    }
}

async fn wait_for_execution_cancellation(store: crate::Store, execution_id: String) {
    loop {
        if store
            .execution_cancel_requested(&execution_id)
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

impl Drop for PluginHost {
    fn drop(&mut self) {
        if !self.stopped {
            let _ = self.child.start_kill();
        }
        if let Some(task) = self.stderr_task.take() {
            task.abort();
        }
    }
}

async fn read_message(
    reader: &mut BufReader<ChildStdout>,
    max_message_bytes: usize,
) -> Result<Vec<u8>, PluginHostError> {
    let mut message = Vec::new();
    loop {
        let buffer = reader.fill_buf().await.map_err(PluginHostError::Read)?;
        if buffer.is_empty() {
            return Err(PluginHostError::Protocol(
                "plugin closed stdout before sending a complete message".to_string(),
            ));
        }
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let available = newline.unwrap_or(buffer.len());
        if message.len() + available > max_message_bytes {
            return Err(PluginHostError::MessageTooLarge {
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
                return Err(PluginHostError::Protocol(
                    "plugin sent an empty stdout line; stdout is protocol-only".to_string(),
                ));
            }
            return Ok(message);
        }
    }
}

fn capture_stderr(
    mut stderr: ChildStderr,
    captured: Arc<Mutex<StderrCapture>>,
    max_bytes: usize,
) -> JoinHandle<io::Result<()>> {
    tokio::spawn(async move {
        let mut buffer = [0_u8; 4096];
        loop {
            let read = stderr.read(&mut buffer).await?;
            if read == 0 {
                return Ok(());
            }
            let mut output = captured.lock().expect("stderr capture lock poisoned");
            let remaining = max_bytes.saturating_sub(output.bytes.len());
            let retained = remaining.min(read);
            output.bytes.extend_from_slice(&buffer[..retained]);
            output.truncated |= retained != read;
        }
    })
}

enum IncomingMessage {
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    Notification(PluginNotification),
    Response(Response),
}

enum Response {
    Result(Value),
    Error(JsonRpcError),
}

fn parse_incoming(message: &[u8], request_id: u64) -> Result<IncomingMessage, PluginHostError> {
    let value: Value = serde_json::from_slice(message).map_err(PluginHostError::ProtocolJson)?;
    let object = value.as_object().ok_or_else(|| {
        PluginHostError::Protocol("stdout message must be a JSON object".to_string())
    })?;
    if object.get("jsonrpc") != Some(&Value::String("2.0".to_string())) {
        return Err(PluginHostError::Protocol(
            "stdout message must declare jsonrpc 2.0".to_string(),
        ));
    }

    if let Some(method) = object.get("method") {
        if let Some(id) = object.get("id") {
            if !(id.is_string() || id.is_i64() || id.is_u64())
                || object
                    .keys()
                    .any(|key| !["jsonrpc", "id", "method", "params"].contains(&key.as_str()))
            {
                return Err(PluginHostError::Protocol(
                    "invalid host request".to_string(),
                ));
            }
            let method = method
                .as_str()
                .filter(|method| !method.is_empty() && !method.starts_with("rpc."))
                .ok_or_else(|| {
                    PluginHostError::Protocol("invalid host request method".to_string())
                })?;
            return Ok(IncomingMessage::Request {
                id: id.clone(),
                method: method.to_string(),
                params: object.get("params").cloned().unwrap_or(Value::Null),
            });
        }
        validate_notification(object, method)?;
        return Ok(IncomingMessage::Notification(PluginNotification {
            method: method
                .as_str()
                .expect("validated notification method")
                .to_string(),
            params: object.get("params").cloned(),
        }));
    }
    parse_response(object, request_id).map(IncomingMessage::Response)
}

fn validate_notification(
    object: &Map<String, Value>,
    method: &Value,
) -> Result<(), PluginHostError> {
    if object
        .keys()
        .any(|key| key != "jsonrpc" && key != "method" && key != "params")
    {
        return Err(PluginHostError::Protocol(
            "plugin notification contains unsupported fields".to_string(),
        ));
    }
    let method = method.as_str().ok_or_else(|| {
        PluginHostError::Protocol("plugin notification method must be a string".to_string())
    })?;
    if method.is_empty() || method.starts_with("rpc.") {
        return Err(PluginHostError::Protocol(
            "plugin notification method is invalid".to_string(),
        ));
    }
    Ok(())
}

fn parse_response(
    object: &Map<String, Value>,
    request_id: u64,
) -> Result<Response, PluginHostError> {
    if object
        .keys()
        .any(|key| key != "jsonrpc" && key != "id" && key != "result" && key != "error")
    {
        return Err(PluginHostError::Protocol(
            "plugin response contains unsupported fields".to_string(),
        ));
    }
    if object.get("id") != Some(&Value::from(request_id)) {
        return Err(PluginHostError::Protocol(format!(
            "plugin response ID does not match request {request_id}"
        )));
    }
    match (object.get("result"), object.get("error")) {
        (Some(result), None) => Ok(Response::Result(result.clone())),
        (None, Some(error)) => Ok(Response::Error(parse_error(error)?)),
        _ => Err(PluginHostError::Protocol(
            "plugin response must contain exactly one of result or error".to_string(),
        )),
    }
}

fn parse_error(value: &Value) -> Result<JsonRpcError, PluginHostError> {
    let object = value.as_object().ok_or_else(|| {
        PluginHostError::Protocol("plugin response error must be an object".to_string())
    })?;
    if object
        .keys()
        .any(|key| key != "code" && key != "message" && key != "data")
    {
        return Err(PluginHostError::Protocol(
            "plugin response error contains unsupported fields".to_string(),
        ));
    }
    let code = object.get("code").and_then(Value::as_i64).ok_or_else(|| {
        PluginHostError::Protocol("plugin response error code must be an integer".to_string())
    })?;
    let message = object
        .get("message")
        .and_then(Value::as_str)
        .filter(|message| !message.is_empty())
        .ok_or_else(|| {
            PluginHostError::Protocol(
                "plugin response error message must be a non-empty string".to_string(),
            )
        })?;
    Ok(JsonRpcError {
        code,
        message: message.to_string(),
        data: object.get("data").cloned(),
    })
}

fn validate_plugin_id(id: &str) -> Result<(), PluginHostError> {
    let valid = id.split('.').count() >= 2
        && id.split('.').all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        });
    if valid {
        Ok(())
    } else {
        Err(PluginHostError::ManifestValidation(
            "id must contain at least two dot-separated ASCII alphanumeric or hyphen segments"
                .to_string(),
        ))
    }
}

fn validate_semver(version: &str) -> Result<(), PluginHostError> {
    let core = version.split_once('+').map_or(version, |(core, build)| {
        if build.is_empty() || !valid_semver_identifiers(build) {
            ""
        } else {
            core
        }
    });
    let (core, prerelease) = core
        .split_once('-')
        .map_or((core, None), |(core, pre)| (core, Some(pre)));
    let valid_core = core.split('.').count() == 3
        && core.split('.').all(|part| {
            !part.is_empty()
                && part.bytes().all(|byte| byte.is_ascii_digit())
                && (part == "0" || !part.starts_with('0'))
        });
    if valid_core && prerelease.is_none_or(valid_semver_identifiers) {
        Ok(())
    } else {
        Err(PluginHostError::ManifestValidation(
            "version must be a semantic version".to_string(),
        ))
    }
}

fn valid_semver_identifiers(value: &str) -> bool {
    !value.is_empty()
        && value.split('.').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn validate_non_empty(field: &str, value: &str) -> Result<(), PluginHostError> {
    if value.trim().is_empty() {
        Err(PluginHostError::ManifestValidation(format!(
            "{field} must not be empty"
        )))
    } else {
        Ok(())
    }
}

fn validate_process_value(field: &str, value: &str) -> Result<(), PluginHostError> {
    if value.is_empty() || value.contains('\0') {
        Err(PluginHostError::ManifestValidation(format!(
            "{field} must not be empty or contain NUL"
        )))
    } else {
        Ok(())
    }
}

fn validate_definition_declaration(value: &str) -> Result<(), PluginHostError> {
    let path = Path::new(value);
    if value.is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        return Err(PluginHostError::ManifestValidation(
            "contributes.nodes entries must be relative paths inside the plugin directory"
                .to_string(),
        ));
    }
    Ok(())
}

fn declared_definition_path(
    manifest_dir: &Path,
    declaration: &str,
) -> Result<PathBuf, PluginRegistryError> {
    let path = Path::new(declaration);
    if declaration.is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        return Err(PluginRegistryError::DefinitionPath(declaration.to_string()));
    }
    let root =
        fs::canonicalize(manifest_dir).map_err(|source| PluginRegistryError::DefinitionRead {
            path: manifest_dir.to_path_buf(),
            source,
        })?;
    let resolved = fs::canonicalize(root.join(path)).map_err(|source| {
        PluginRegistryError::DefinitionRead {
            path: root.join(path),
            source,
        }
    })?;
    if resolved.starts_with(&root) {
        Ok(resolved)
    } else {
        Err(PluginRegistryError::DefinitionPath(declaration.to_string()))
    }
}

fn validate_node_type(value: &str) -> Result<(), String> {
    let Some((namespace, version)) = value.rsplit_once('@') else {
        return Err("type must use the namespace.name@major format".to_string());
    };
    if version
        .parse::<u32>()
        .ok()
        .filter(|version| *version > 0)
        .is_none()
        || namespace.split('.').count() < 2
        || namespace.split('.').any(|part| {
            part.is_empty()
                || !part.starts_with(|character: char| character.is_ascii_lowercase())
                || !part.chars().all(|character| {
                    character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
                })
        })
    {
        return Err("type must use the namespace.name@major format".to_string());
    }
    Ok(())
}

fn parse_node_result(value: Value) -> Result<PluginNodeResult, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "result must be an object".to_string())?;
    if object
        .keys()
        .any(|key| key != "route" && key != "outputs" && key != "message")
    {
        return Err("result contains unsupported fields".to_string());
    }
    let route = match object.get("route") {
        Some(value) => value
            .as_str()
            .ok_or_else(|| "route must be a string".to_string())?,
        None => "success",
    };
    validate_route(route)?;
    let outputs = match object.get("outputs") {
        Some(value) => value
            .as_object()
            .ok_or_else(|| "outputs must be an object".to_string())?
            .iter()
            .map(|(name, value)| {
                if name.trim().is_empty() {
                    Err("outputs cannot contain an empty name".to_string())
                } else {
                    Ok((name.clone(), value.clone()))
                }
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?,
        None => BTreeMap::new(),
    };
    let message = match object.get("message") {
        Some(value) => Some(
            value
                .as_str()
                .ok_or_else(|| "message must be a string".to_string())?
                .to_string(),
        ),
        None => None,
    };
    Ok(PluginNodeResult {
        route: route.to_string(),
        outputs,
        message,
    })
}

fn validate_route(value: &str) -> Result<(), String> {
    let valid = !value.is_empty()
        && value.len() <= 63
        && value.starts_with(|character: char| character.is_ascii_lowercase())
        && value.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        });
    if valid {
        Ok(())
    } else {
        Err("route must be a lowercase kebab-case identifier of at most 63 characters".to_string())
    }
}

/// A JSON-RPC error returned by the plugin.
#[derive(Clone, Debug, PartialEq)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl fmt::Display for JsonRpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} ({})", self.message, self.code)
    }
}

/// Failures while loading, communicating with, or stopping a plugin process.
#[derive(Debug, Error)]
pub enum PluginHostError {
    #[error("cannot read plugin manifest {path}: {source}")]
    ManifestRead { path: PathBuf, source: io::Error },
    #[error("plugin manifest exceeds the {limit}-byte limit")]
    ManifestTooLarge { limit: usize },
    #[error("plugin manifest is not valid JSON: {source}")]
    ManifestParse { source: serde_json::Error },
    #[error("invalid plugin manifest: {0}")]
    ManifestValidation(String),
    #[error("invalid plugin host option: {0}")]
    InvalidOption(String),
    #[error("cannot start plugin process: {0}")]
    Spawn(#[source] io::Error),
    #[error("plugin {0} pipe was unavailable")]
    MissingPipe(&'static str),
    #[error("cannot write plugin stdin: {0}")]
    Write(#[source] io::Error),
    #[error("cannot read plugin stdout: {0}")]
    Read(#[source] io::Error),
    #[error("cannot wait for plugin process: {0}")]
    Wait(#[source] io::Error),
    #[error("cannot terminate plugin process: {0}")]
    Kill(#[source] io::Error),
    #[error("cannot serialize JSON-RPC request: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("plugin stdout is not valid JSON-RPC: {0}")]
    ProtocolJson(#[source] serde_json::Error),
    #[error("plugin protocol error: {0}")]
    Protocol(String),
    #[error("plugin message exceeds the {limit}-byte limit")]
    MessageTooLarge { limit: usize },
    #[error("plugin request timed out after {0:?}")]
    Timeout(Duration),
    #[error("plugin request ID space is exhausted")]
    RequestIdExhausted,
    #[error("plugin returned JSON-RPC error: {0}")]
    Remote(JsonRpcError),
    #[error("plugin process has stopped")]
    Stopped,
    #[error("plugin process exited with status {0}")]
    Exited(ExitStatus),
    #[error("plugin process did not exit within {0:?} after shutdown")]
    ShutdownTimeout(Duration),
    #[error("cannot capture plugin stderr: {0}")]
    StderrRead(#[source] io::Error),
    #[error("plugin stderr task failed: {0}")]
    StderrTask(#[source] tokio::task::JoinError),
    #[error("plugin stderr task did not finish within {0:?}")]
    StderrTimeout(Duration),
}

impl PluginHostError {
    fn is_fatal_protocol_error(&self) -> bool {
        matches!(
            self,
            Self::Write(_)
                | Self::Read(_)
                | Self::ProtocolJson(_)
                | Self::Protocol(_)
                | Self::MessageTooLarge { .. }
                | Self::Timeout(_)
                | Self::Exited(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_semantic_versions() {
        for version in ["0.1.0", "1.2.3-alpha.1", "1.2.3+build.5"] {
            assert!(validate_semver(version).is_ok(), "{version}");
        }
        for version in ["1.2", "01.2.3", "1.2.3-", "1.2.3+"] {
            assert!(validate_semver(version).is_err(), "{version}");
        }
    }

    #[test]
    fn rejects_unknown_or_non_process_manifest_fields() {
        let source = r#"{
            "manifestVersion":"1.0",
            "id":"org.example",
            "name":"Example",
            "version":"0.1.0",
            "pluginProtocol":">=1.0.0 <2.0.0",
            "runtime":{"kind":"wasm","command":"plugin"}
        }"#;
        let manifest = serde_json::from_str::<RawPluginManifest>(source).expect("JSON parses");
        assert!(manifest.validate(Path::new(".")).is_err());

        let unknown = source.replace("\"runtime\"", "\"unexpected\":true,\"runtime\"");
        assert!(serde_json::from_str::<RawPluginManifest>(&unknown).is_err());
    }

    #[test]
    fn accepts_notifications_and_requires_matching_response_ids() {
        let notification = br#"{"jsonrpc":"2.0","method":"log/emit","params":{"level":"info"}}"#;
        let parsed = parse_incoming(notification, 4).expect("notification is valid");
        assert!(matches!(parsed, IncomingMessage::Notification(_)));

        let wrong_id = br#"{"jsonrpc":"2.0","id":3,"result":null}"#;
        assert!(parse_incoming(wrong_id, 4).is_err());
    }
}
