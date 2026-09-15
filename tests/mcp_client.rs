use std::{
    collections::BTreeMap,
    env,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use kakune_core::mcp::{
    MCP_LEGACY_PROTOCOL_VERSION, MCP_PROTOCOL_VERSION, McpClient, McpClientOptions, McpError,
    McpHttpServerConfig, McpStdioServerConfig, McpToolContent,
};
use serde_json::{Map, json};

fn helper_command() -> String {
    if let Ok(command) = env::var("KAKUNE_MCP_HELPER") {
        return command;
    }
    if let Ok(command) = env::var("CARGO_BIN_EXE_mcp-helper") {
        return command;
    }
    env::current_exe()
        .ok()
        .and_then(|path| {
            path.parent()
                .and_then(|path| path.parent())
                .map(std::path::Path::to_path_buf)
        })
        .map(|directory| {
            directory
                .join(format!("mcp-helper{}", env::consts::EXE_SUFFIX))
                .to_string_lossy()
                .into_owned()
        })
        .unwrap_or_else(|| "mcp-helper".to_string())
}

fn helper_config(env: BTreeMap<String, String>) -> McpStdioServerConfig {
    McpStdioServerConfig {
        command: helper_command(),
        args: Vec::new(),
        current_dir: None,
        env,
    }
}

#[tokio::test]
async fn verifies_explicit_stdio_initialization_discovery_and_call() {
    let mut child_env = BTreeMap::new();
    child_env.insert(
        "KAKUNE_TEST_EXPLICIT_SECRET".to_string(),
        "configured".to_string(),
    );
    let mut client = McpClient::connect(helper_config(child_env), McpClientOptions::default())
        .await
        .expect("MCP client connects");

    assert_eq!(client.server_info().name, "test-mcp-server");
    assert_eq!(client.server_info().protocol_version, "2025-11-25");
    let tools = client.list_tools().await.expect("tools are discovered");
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].name, "greet");
    assert!(tools[0].input_schema.as_value().is_object());
    assert!(tools[0].output_schema.is_some());

    let result = client
        .call_tool(
            "greet",
            Map::from_iter([(String::from("name"), json!("Ada"))]),
        )
        .await
        .expect("tool call succeeds");
    assert_eq!(
        result.content,
        vec![McpToolContent::Text {
            text: "Hello, Ada!".to_string()
        }]
    );
    assert_eq!(
        result.structured_content,
        Some(json!({"greeting": "Hello, Ada!"}))
    );
    assert!(!result.is_error);
    if let Err(error) = client.cancel().await {
        panic!(
            "child terminates cleanly: {error}; stderr: {}",
            client.stderr().text()
        );
    }
}

#[tokio::test]
async fn verifies_timeout_terminates_the_child() {
    let mut client = McpClient::connect(
        helper_config(BTreeMap::new()),
        McpClientOptions {
            request_timeout: Duration::from_millis(50),
            ..McpClientOptions::default()
        },
    )
    .await
    .expect("MCP client connects");
    assert!(matches!(
        client.call_tool("ignore", Map::new()).await,
        Err(McpError::Timeout(_))
    ));
    assert!(matches!(client.list_tools().await, Err(McpError::Stopped)));
}

#[tokio::test]
async fn verifies_output_limit_terminates_the_child() {
    let mut client = McpClient::connect(
        helper_config(BTreeMap::new()),
        McpClientOptions {
            max_message_bytes: 256,
            ..McpClientOptions::default()
        },
    )
    .await
    .expect("MCP client connects");
    assert!(matches!(
        client.call_tool("oversize", Map::new()).await,
        Err(McpError::MessageTooLarge { limit: 256 })
    ));
    assert!(matches!(client.list_tools().await, Err(McpError::Stopped)));
}

#[derive(Default)]
struct HttpMcpState {
    protocol_versions: Mutex<Vec<String>>,
    authorizations: Mutex<Vec<Option<String>>>,
}

async fn http_mcp_server(
    State(state): State<Arc<HttpMcpState>>,
    headers: HeaderMap,
    Json(request): Json<serde_json::Value>,
) -> Response {
    state
        .authorizations
        .lock()
        .expect("authorization recording lock")
        .push(
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        );
    let method = request["method"].as_str().expect("request method");
    if method == "notifications/initialized" {
        return StatusCode::NO_CONTENT.into_response();
    }
    let id = request["id"].clone();
    let result = match method {
        "initialize" => {
            let version = request["params"]["protocolVersion"]
                .as_str()
                .expect("protocol version")
                .to_string();
            state
                .protocol_versions
                .lock()
                .expect("version recording lock")
                .push(version.clone());
            if version == MCP_PROTOCOL_VERSION {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {"code": -32602, "message": "unsupported protocol version"}
                    })),
                )
                    .into_response();
            }
            assert_eq!(version, MCP_LEGACY_PROTOCOL_VERSION);
            json!({
                "protocolVersion": MCP_LEGACY_PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "http-test-mcp-server", "version": "1.0.0"}
            })
        }
        "tools/list" => json!({
            "tools": [{
                "name": "greet",
                "inputSchema": {"type": "object"}
            }]
        }),
        "tools/call" => {
            if request["params"]["name"] == "oversize" {
                json!({"content": [{"type": "text", "text": "x".repeat(512)}]})
            } else if request["params"]["name"] == "slow" {
                tokio::time::sleep(Duration::from_millis(100)).await;
                json!({"content": []})
            } else {
                json!({"content": [{"type": "text", "text": "Hello from HTTP!"}]})
            }
        }
        other => panic!("unexpected MCP method {other}"),
    };
    Json(json!({"jsonrpc": "2.0", "id": id, "result": result})).into_response()
}

async fn http_mcp_endpoint() -> (String, Arc<HttpMcpState>, tokio::task::JoinHandle<()>) {
    let state = Arc::new(HttpMcpState::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener binds");
    let address = listener.local_addr().expect("test listener address");
    let server_state = Arc::clone(&state);
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/mcp", post(http_mcp_server))
                .with_state(server_state),
        )
        .await
        .expect("test MCP server remains valid");
    });
    (format!("http://{address}/mcp"), state, server)
}

#[tokio::test]
async fn http_client_negotiates_fallback_and_redacts_bearer_secret() {
    let (endpoint, state, server) = http_mcp_endpoint().await;
    let config = McpHttpServerConfig {
        endpoint,
        bearer_token: Some("test-http-secret".to_string()),
    };
    assert!(!format!("{config:?}").contains("test-http-secret"));
    let mut client = McpClient::connect_http(config, McpClientOptions::default())
        .await
        .expect("HTTP MCP client connects");

    assert_eq!(client.server_info().name, "http-test-mcp-server");
    assert!(client.server_capabilities().tools);
    assert_eq!(
        client.list_tools().await.expect("tools list")[0].name,
        "greet"
    );
    assert_eq!(
        client
            .call_tool("greet", Map::new())
            .await
            .expect("tool call")
            .content,
        vec![McpToolContent::Text {
            text: "Hello from HTTP!".to_string()
        }]
    );
    client.cancel().await.expect("HTTP client cancels");

    assert_eq!(
        *state
            .protocol_versions
            .lock()
            .expect("version recording lock"),
        vec![
            MCP_PROTOCOL_VERSION.to_string(),
            MCP_LEGACY_PROTOCOL_VERSION.to_string()
        ]
    );
    assert!(
        state
            .authorizations
            .lock()
            .expect("authorization recording lock")
            .iter()
            .all(|value| value.as_deref() == Some("Bearer test-http-secret"))
    );
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn http_client_enforces_response_limit() {
    let (endpoint, _state, server) = http_mcp_endpoint().await;
    let mut client = McpClient::connect_http(
        McpHttpServerConfig {
            endpoint,
            bearer_token: None,
        },
        McpClientOptions {
            max_message_bytes: 256,
            ..McpClientOptions::default()
        },
    )
    .await
    .expect("HTTP MCP client connects");

    assert!(matches!(
        client.call_tool("oversize", Map::new()).await,
        Err(McpError::MessageTooLarge { limit: 256 })
    ));
    assert!(matches!(client.list_tools().await, Err(McpError::Stopped)));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn http_client_enforces_request_timeout() {
    let (endpoint, _state, server) = http_mcp_endpoint().await;
    let mut client = McpClient::connect_http(
        McpHttpServerConfig {
            endpoint,
            bearer_token: None,
        },
        McpClientOptions {
            request_timeout: Duration::from_millis(20),
            ..McpClientOptions::default()
        },
    )
    .await
    .expect("HTTP MCP client connects");

    assert!(matches!(
        client.call_tool("slow", Map::new()).await,
        Err(McpError::Timeout(_))
    ));
    assert!(matches!(client.list_tools().await, Err(McpError::Stopped)));
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn http_client_rejects_unsafe_endpoint_configuration() {
    assert!(matches!(
        McpClient::connect_http(
            McpHttpServerConfig {
                endpoint: "file:///tmp/mcp".to_string(),
                bearer_token: None,
            },
            McpClientOptions::default(),
        )
        .await,
        Err(McpError::InvalidHttpConfig(_))
    ));
    assert!(matches!(
        McpClient::connect_http(
            McpHttpServerConfig {
                endpoint: "https://user:password@example.test/mcp".to_string(),
                bearer_token: None,
            },
            McpClientOptions::default(),
        )
        .await,
        Err(McpError::InvalidHttpConfig(_))
    ));
}
