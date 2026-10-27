mod support;

use std::{
    collections::BTreeMap,
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
};
use kakune_core::{
    AuthScope, NativeCredentialBackend, SecretBackendKind, Store,
    mcp::{McpClient, McpClientOptions, McpError, McpStdioServerConfig},
};
use reqwest::{Method, header::AUTHORIZATION};
use serde_json::{Value, json};
use support::management::{
    FailureInjector, FakeClock, InProcessCore, ManagementFixture, MonotonicClock,
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

async fn core() -> (ManagementFixture, InProcessCore) {
    let fixture = ManagementFixture::new().expect("fixture should be created");
    let store = Store::open_with_secret_backend(
        fixture.path("core"),
        SecretBackendKind::Native,
        Arc::new(IntegrationCredentials::default()),
    )
    .expect("fake-credential Core store should open");
    let core = InProcessCore::start_with_store(store)
        .await
        .expect("isolated Core should start");
    (fixture, core)
}

#[derive(Default)]
struct IntegrationCredentials(Mutex<HashMap<String, Vec<u8>>>);

impl NativeCredentialBackend for IntegrationCredentials {
    fn write(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.0
            .lock()
            .unwrap()
            .insert(key.to_owned(), value.to_vec());
        Ok(())
    }

    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
}

async fn request(
    client: &reqwest::Client,
    core: &InProcessCore,
    method: Method,
    path: &str,
    token: &str,
    body: Option<Value>,
) -> reqwest::Response {
    let mut request = client
        .request(method, format!("{}/api/v1/manage{path}", core.endpoint))
        .header(AUTHORIZATION, format!("Bearer {token}"));
    if let Some(body) = body {
        request = request.json(&body);
    }
    request.send().await.expect("request should complete")
}

fn provider_profile(id: &str, provider_type: &str, secret_ref: &str) -> Value {
    json!({
        "id": id,
        "displayName": format!("{id} profile"),
        "providerType": provider_type,
        "defaultModel": if provider_type == "minimax" { "MiniMax-M3" } else { "codex-default" },
        "allowedModels": [if provider_type == "minimax" { "MiniMax-M3" } else { "codex-default" }],
        "capabilities": ["text-generation"],
        "auth": if provider_type == "minimax" {
            json!({"type":"apiKey", "secretRef":secret_ref})
        } else {
            json!({"type":"oauthSecret", "secretRef":secret_ref})
        },
        "config": {}
    })
}

#[tokio::test]
async fn providers_are_create_only_and_report_unsupported_auth_and_model_operations_truthfully() {
    let (_fixture, mut core) = core().await;
    let manage = core
        .store
        .create_auth_token("integration-manager".into(), vec![AuthScope::Manage], None)
        .expect("Manage token should be created");
    let read = core
        .store
        .create_auth_token("integration-reader".into(), vec![AuthScope::Read], None)
        .expect("Read token should be created");
    let client = reqwest::Client::new();

    let create = request(
        &client,
        &core,
        Method::POST,
        "/providers",
        &manage.token,
        Some(provider_profile("mm", "minimax", "missing-key")),
    )
    .await;
    assert_eq!(create.status(), StatusCode::CREATED);
    let duplicate = request(
        &client,
        &core,
        Method::POST,
        "/providers",
        &manage.token,
        Some(provider_profile("mm", "minimax", "replacement-key")),
    )
    .await;
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);
    assert_eq!(
        core.store.get_provider_profile("mm").unwrap().unwrap().auth,
        kakune_core::ProviderAuth::ApiKeySecret {
            secret_ref: "missing-key".into()
        },
        "a duplicate add must not overwrite the existing reference"
    );

    assert_eq!(
        request(&client, &core, Method::GET, "/providers", &read.token, None)
            .await
            .status(),
        StatusCode::OK,
        "provider metadata is Read-scoped"
    );
    let list = request(
        &client,
        &core,
        Method::GET,
        "/providers",
        &manage.token,
        None,
    )
    .await;
    assert_eq!(list.status(), StatusCode::FORBIDDEN, "Manage is not Read");

    let test = request(
        &client,
        &core,
        Method::POST,
        "/providers/mm/test",
        &manage.token,
        None,
    )
    .await;
    assert_eq!(test.status(), StatusCode::OK);
    let test: Value = test.json().await.unwrap();
    assert_eq!(test["authentication"]["status"], "failed");
    assert_eq!(test["connectivity"]["status"], "unknown");
    assert_eq!(test["capabilities"][0]["status"], "unsupported");
    assert!(!test.to_string().contains("replacement-key"));

    let models = request(
        &client,
        &core,
        Method::POST,
        "/providers/mm/models",
        &manage.token,
        None,
    )
    .await;
    assert_eq!(models.status(), StatusCode::NOT_IMPLEMENTED);
    let models: Value = models.json().await.unwrap();
    assert_eq!(models["discovery"]["status"], "unsupported");
    assert_eq!(models["models"][0]["provenance"], "configured");

    for (id, provider_type, reference, expected) in [
        ("mm", "minimax", "missing-key", "secureInput"),
        ("cx", "codex", "missing-oauth", "browser"),
    ] {
        if id == "cx" {
            assert_eq!(
                request(
                    &client,
                    &core,
                    Method::POST,
                    "/providers",
                    &manage.token,
                    Some(provider_profile(id, provider_type, reference)),
                )
                .await
                .status(),
                StatusCode::CREATED
            );
        }
        let prepared = request(
            &client,
            &core,
            Method::POST,
            &format!("/providers/{id}/auth/prepare"),
            &manage.token,
            None,
        )
        .await;
        assert_eq!(prepared.status(), StatusCode::OK);
        let prepared: Value = prepared.json().await.unwrap();
        assert_eq!(prepared["mechanism"], expected);
        assert!(!prepared.to_string().contains(reference));
        if expected == "browser" {
            assert!(
                prepared["instructions"]
                    .as_str()
                    .unwrap()
                    .contains("manually")
            );
        }
    }

    const PROVIDER_CANARY: &str = "provider-auth-private-canary";
    let committed = request(
        &client,
        &core,
        Method::POST,
        "/providers/mm/auth/commit",
        &manage.token,
        Some(json!({"secretValue":PROVIDER_CANARY})),
    )
    .await;
    assert_eq!(committed.status(), StatusCode::OK);
    let committed: Value = committed.json().await.unwrap();
    assert!(!committed.to_string().contains(PROVIDER_CANARY));
    let authenticated = request(
        &client,
        &core,
        Method::POST,
        "/providers/mm/test",
        &manage.token,
        None,
    )
    .await;
    let authenticated: Value = authenticated.json().await.unwrap();
    assert_eq!(authenticated["authentication"]["status"], "unknown");
    assert_eq!(authenticated["connectivity"]["status"], "unknown");
    assert_eq!(authenticated["capabilities"][0]["status"], "unsupported");
    assert!(!authenticated.to_string().contains(PROVIDER_CANARY));

    core.shutdown().await.expect("Core should stop cleanly");
}

#[derive(Default)]
struct HttpMcpState {
    tool_calls: Mutex<Vec<Value>>,
}

async fn http_mcp(
    State(state): State<Arc<HttpMcpState>>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> axum::response::Response {
    if request["method"] == "notifications/initialized" {
        return StatusCode::ACCEPTED.into_response();
    }
    let id = request["id"].clone();
    let result = match request["method"].as_str().unwrap_or_default() {
        "initialize" => json!({
            "protocolVersion":"2025-11-25",
            "capabilities":{"tools":{}},
            "serverInfo":{"name":"admin-test-server","version":"1"}
        }),
        "tools/list" => json!({"tools":[
            {"name":"echo","description":"Echo a value","inputSchema":{
                "type":"object","properties":{"name":{"type":"string"}},
                "required":["name"],"additionalProperties":false
            }},
            {"name":"fail","inputSchema":{"type":"object"}}
        ]}),
        "tools/call" => {
            let params = request["params"].clone();
            state.tool_calls.lock().unwrap().push(params.clone());
            let authorization = headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default();
            if params["name"] == "fail" {
                json!({"isError":true,"content":[{"type":"text","text":format!("{authorization} private-proof")}]})
            } else {
                json!({"content":[{"type":"text","text":format!("{authorization} {}", params["arguments"]["name"].as_str().unwrap_or_default())}]})
            }
        }
        other => panic!("unexpected MCP method {other}"),
    };
    (
        StatusCode::OK,
        Json(json!({"jsonrpc":"2.0","id":id,"result":result})),
    )
        .into_response()
}

async fn start_http_mcp() -> (
    String,
    Arc<HttpMcpState>,
    oneshot::Sender<()>,
    JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address: SocketAddr = listener.local_addr().unwrap();
    let state = Arc::new(HttpMcpState::default());
    let app_state = Arc::clone(&state);
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/mcp", post(http_mcp))
                .with_state(app_state),
        )
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    (format!("http://{address}/mcp"), state, stop, task)
}

#[derive(Clone)]
struct PhasedMcpState {
    clock: FakeClock,
    failures: FailureInjector,
    methods: Arc<Mutex<Vec<String>>>,
    initialize_delay: Duration,
    listing_delay: Duration,
}

async fn phased_http_mcp(
    State(state): State<PhasedMcpState>,
    Json(request): Json<Value>,
) -> axum::response::Response {
    let method = request["method"].as_str().unwrap_or_default();
    state.methods.lock().unwrap().push(method.to_owned());
    if method == "notifications/initialized" {
        return StatusCode::ACCEPTED.into_response();
    }
    let id = request["id"].clone();
    match method {
        "initialize" => {
            state.clock.advance(state.initialize_delay);
            tokio::time::sleep(state.initialize_delay).await;
            (
                StatusCode::OK,
                Json(json!({
                    "jsonrpc":"2.0",
                    "id":id,
                    "result":{
                        "protocolVersion":"2025-11-25",
                        "capabilities":{"tools":{}},
                        "serverInfo":{"name":"phased-server","version":"1"}
                    }
                })),
            )
                .into_response()
        }
        "tools/list" => {
            let injected_failure = state.failures.check("mcp.tools.list").is_err();
            state.clock.advance(state.listing_delay);
            tokio::time::sleep(state.listing_delay).await;
            if injected_failure {
                return StatusCode::SERVICE_UNAVAILABLE.into_response();
            }
            (
                StatusCode::OK,
                Json(json!({
                    "jsonrpc":"2.0",
                    "id":id,
                    "result":{"tools":[]}
                })),
            )
                .into_response()
        }
        other => panic!("unexpected phased MCP method {other}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn named_mcp_discovery_uses_one_deadline_across_connection_and_listing() {
    let (_fixture, mut core) = core().await;
    let manage = core
        .store
        .create_auth_token("mcp-budget-manager".into(), vec![AuthScope::Manage], None)
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = PhasedMcpState {
        clock: FakeClock::default(),
        failures: FailureInjector::default(),
        methods: Arc::new(Mutex::new(Vec::new())),
        initialize_delay: Duration::from_secs(2),
        listing_delay: Duration::from_secs(29),
    };
    state
        .failures
        .fail_on_call("mcp.tools.list", 1, "injected late discovery failure");
    let observed_clock = state.clock.clone();
    let observed_failures = state.failures.clone();
    let observed_methods = Arc::clone(&state.methods);
    let (stop_mcp, stopped_mcp) = oneshot::channel();
    let mcp_task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/mcp", post(phased_http_mcp))
                .with_state(state),
        )
        .with_graceful_shutdown(async {
            let _ = stopped_mcp.await;
        })
        .await
        .unwrap();
    });
    let endpoint = format!("http://{address}/mcp");
    let client = reqwest::Client::new();
    let created = request(
        &client,
        &core,
        Method::POST,
        "/mcp",
        &manage.token,
        Some(json!({
            "name":"phased",
            "transport":{"type":"http","endpoint":endpoint,"bearerSecretRef":null}
        })),
    )
    .await;
    assert_eq!(created.status(), StatusCode::CREATED);

    let response = request(
        &client,
        &core,
        Method::POST,
        "/mcp/phased/tools/list",
        &manage.token,
        Some(json!({"limit":1})),
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::GATEWAY_TIMEOUT,
        "connection setup and discovery share the 30-second operation budget"
    );
    let timeout: Value = response.json().await.unwrap();
    assert_eq!(
        timeout["message"],
        "MCP discovery exceeded its aggregate 30-second deadline"
    );
    assert_eq!(
        observed_methods.lock().unwrap().as_slice(),
        ["initialize", "notifications/initialized", "tools/list"]
    );
    assert_eq!(observed_clock.now(), Duration::from_secs(31));
    assert_eq!(observed_failures.calls("mcp.tools.list"), 1);

    let _ = stop_mcp.send(());
    mcp_task.await.unwrap();
    core.shutdown().await.expect("Core should stop cleanly");
}

fn helper_command() -> String {
    std::env::var("CARGO_BIN_EXE_mcp-helper").unwrap_or_else(|_| {
        let current = std::env::current_exe().expect("test executable path");
        current
            .parent()
            .and_then(|path| path.parent())
            .unwrap()
            .join(format!("mcp-helper{}", std::env::consts::EXE_SUFFIX))
            .to_string_lossy()
            .into_owned()
    })
}

fn stdio_helper(args: &[&str], env: BTreeMap<String, String>) -> McpStdioServerConfig {
    McpStdioServerConfig {
        command: helper_command(),
        args: args.iter().map(|argument| (*argument).to_owned()).collect(),
        current_dir: None,
        env,
    }
}

#[tokio::test]
async fn stdio_mcp_notification_overflow_is_explicit_and_stops_the_child() {
    let mut client = McpClient::connect(
        stdio_helper(&["notification-flood"], BTreeMap::new()),
        McpClientOptions::default(),
    )
    .await
    .expect("notification helper should initialize");
    assert!(matches!(
        client.list_tools().await,
        Err(McpError::NotificationLimit { limit: 256 })
    ));
    assert!(matches!(client.list_tools().await, Err(McpError::Stopped)));
}

#[tokio::test]
async fn stdio_mcp_page_overflow_is_explicit_and_stops_the_child() {
    let mut env = BTreeMap::new();
    env.insert("KAKUNE_MCP_HELPER_PAGE_LOOP".to_owned(), "true".to_owned());
    let mut client = McpClient::connect(
        stdio_helper(&[], env),
        McpClientOptions {
            max_tool_pages: 2,
            ..McpClientOptions::default()
        },
    )
    .await
    .expect("paginating helper should initialize");
    assert!(matches!(
        client.list_tools().await,
        Err(McpError::ToolPageLimit { limit: 2 })
    ));
    assert!(matches!(client.list_tools().await, Err(McpError::Stopped)));
}

#[tokio::test]
async fn stdio_mcp_message_overflow_fails_during_initialization() {
    let result = McpClient::connect(
        stdio_helper(&["large-init"], BTreeMap::new()),
        McpClientOptions {
            max_message_bytes: 256,
            ..McpClientOptions::default()
        },
    )
    .await;
    assert!(matches!(
        result,
        Err(McpError::MessageTooLarge { limit: 256 })
    ));
}

#[tokio::test]
async fn stdio_mcp_stderr_is_capped_and_request_timeout_stops_the_child() {
    let mut env = BTreeMap::new();
    env.insert("KAKUNE_MCP_HELPER_STDERR".to_owned(), "large".to_owned());
    let mut noisy = McpClient::connect(
        stdio_helper(&[], env),
        McpClientOptions {
            max_stderr_bytes: 128,
            ..McpClientOptions::default()
        },
    )
    .await
    .expect("noisy helper should initialize");
    noisy
        .list_tools()
        .await
        .expect("tool discovery should complete");
    noisy.cancel().await.expect("noisy child should be reaped");
    assert_eq!(noisy.stderr().bytes.len(), 128);
    assert!(noisy.stderr().truncated);

    let mut timed = McpClient::connect(
        stdio_helper(&[], BTreeMap::new()),
        McpClientOptions {
            request_timeout: Duration::from_millis(50),
            ..McpClientOptions::default()
        },
    )
    .await
    .expect("timeout helper should initialize");
    assert!(matches!(
        timed.call_tool("ignore", serde_json::Map::new()).await,
        Err(McpError::Timeout(_))
    ));
    assert!(matches!(timed.list_tools().await, Err(McpError::Stopped)));
}

#[tokio::test]
async fn named_mcp_discovery_validates_before_call_and_reports_errors_without_echoing_secrets() {
    let (_fixture, mut core) = core().await;
    let manage = core
        .store
        .create_auth_token("mcp-manager".into(), vec![AuthScope::Manage], None)
        .unwrap();
    let read = core
        .store
        .create_auth_token("mcp-reader".into(), vec![AuthScope::Read], None)
        .unwrap();
    let client = reqwest::Client::new();
    let (endpoint, mcp_state, stop_mcp, mcp_task) = start_http_mcp().await;
    const MCP_BEARER_CANARY: &str = "mcp-upstream-bearer-private-proof";
    core.store
        .set_secret_versioned("mcp-bearer", MCP_BEARER_CANARY)
        .expect("fake credential backend should publish MCP bearer secret");

    let add = request(
        &client,
        &core,
        Method::POST,
        "/mcp",
        &manage.token,
        Some(json!({
            "name":"test-http",
            "transport":{"type":"http","endpoint":endpoint,"bearerSecretRef":"mcp-bearer"}
        })),
    )
    .await;
    assert_eq!(add.status(), StatusCode::CREATED);
    let duplicate = request(
        &client,
        &core,
        Method::POST,
        "/mcp",
        &manage.token,
        Some(json!({
            "name":"test-http",
            "transport":{"type":"http","endpoint":"http://127.0.0.1:1/mcp","bearerSecretRef":null}
        })),
    )
    .await;
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);

    assert_eq!(
        request(&client, &core, Method::GET, "/mcp", &read.token, None)
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        request(
            &client,
            &core,
            Method::POST,
            "/mcp/test-http/tools/list",
            &read.token,
            Some(json!({"limit":1}))
        )
        .await
        .status(),
        StatusCode::FORBIDDEN,
        "external discovery requires Manage scope"
    );

    let listed = request(
        &client,
        &core,
        Method::POST,
        "/mcp/test-http/tools/list",
        &manage.token,
        Some(json!({"limit":1})),
    )
    .await;
    assert_eq!(listed.status(), StatusCode::OK);
    let listed: Value = listed.json().await.unwrap();
    assert_eq!(listed["server"]["name"], "admin-test-server");
    assert_eq!(listed["items"].as_array().unwrap().len(), 1);
    let cursor = listed["nextCursor"].as_str().unwrap();

    let database =
        rusqlite::Connection::open(core.store.data_dir().join("kakune.sqlite3")).unwrap();
    let original_revision: String = database
        .query_row(
            "SELECT revision FROM named_mcp_connections WHERE name = 'test-http'",
            [],
            |row| row.get(0),
        )
        .expect("connection should be stored");
    database
        .execute(
            "UPDATE named_mcp_connections SET revision = 'changed-revision' WHERE name = 'test-http'",
            [],
        )
        .expect("test fixture should change the observed revision");
    let stale_page = request(
        &client,
        &core,
        Method::POST,
        "/mcp/test-http/tools/list",
        &manage.token,
        Some(json!({"limit":1,"cursor":cursor})),
    )
    .await;
    assert_eq!(stale_page.status(), StatusCode::CONFLICT);
    assert_ne!(original_revision, "changed-revision");

    let invalid = request(
        &client,
        &core,
        Method::POST,
        "/mcp/test-http/tools/call",
        &manage.token,
        Some(json!({"name":"echo","arguments":{}})),
    )
    .await;
    assert_eq!(invalid.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert!(mcp_state.tool_calls.lock().unwrap().is_empty());

    let success = request(
        &client,
        &core,
        Method::POST,
        "/mcp/test-http/tools/call",
        &manage.token,
        Some(json!({"name":"echo","arguments":{"name":"private-proof"}})),
    )
    .await;
    assert_eq!(success.status(), StatusCode::OK);
    let success: Value = success.json().await.unwrap();
    assert!(!success.to_string().contains("private-proof"));
    assert!(!success.to_string().contains(MCP_BEARER_CANARY));

    let failure = request(
        &client,
        &core,
        Method::POST,
        "/mcp/test-http/tools/call",
        &manage.token,
        Some(json!({"name":"fail","arguments":{}})),
    )
    .await;
    assert_eq!(failure.status(), StatusCode::BAD_GATEWAY);
    let error: Value = failure.json().await.unwrap();
    assert!(!error.to_string().contains("private-proof"));
    assert!(!error.to_string().contains(MCP_BEARER_CANARY));
    assert_eq!(mcp_state.tool_calls.lock().unwrap().len(), 2);
    let database_bytes = std::fs::read(core.store.data_dir().join("kakune.sqlite3")).unwrap();
    assert!(!String::from_utf8_lossy(&database_bytes).contains(MCP_BEARER_CANARY));
    assert!(
        core.store
            .list_events_after_limited(None, 256)
            .unwrap()
            .iter()
            .all(|event| !event.payload.to_string().contains(MCP_BEARER_CANARY))
    );

    stop_mcp.send(()).unwrap();
    mcp_task.await.unwrap();
    core.shutdown().await.expect("Core should stop cleanly");
}

#[tokio::test]
async fn named_stdio_connections_are_validated_without_starting_and_can_discover_tools() {
    let (_fixture, mut core) = core().await;
    let manage = core
        .store
        .create_auth_token("stdio-manager".into(), vec![AuthScope::Manage], None)
        .unwrap();
    let client = reqwest::Client::new();
    let add = request(
        &client,
        &core,
        Method::POST,
        "/mcp",
        &manage.token,
        Some(json!({
            "name":"stdio",
            "transport":{
                "type":"stdio",
                "command":helper_command(),
                "args":[],
                "workingDirectory":std::env::current_dir().unwrap(),
                "environmentSecretRefs":BTreeMap::<String,String>::new()
            }
        })),
    )
    .await;
    assert_eq!(add.status(), StatusCode::CREATED);
    let listed = request(
        &client,
        &core,
        Method::POST,
        "/mcp/stdio/tools/list",
        &manage.token,
        Some(json!({"limit":20})),
    )
    .await;
    assert_eq!(listed.status(), StatusCode::OK);
    let listed: Value = listed.json().await.unwrap();
    assert_eq!(listed["server"]["name"], "test-mcp-server");
    assert_eq!(listed["items"][0]["name"], "greet");
    core.shutdown().await.expect("Core should stop cleanly");
}
