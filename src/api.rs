use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    convert::Infallible,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{Next, from_fn_with_state},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{delete, get, post},
};
use futures_util::stream;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    API_VERSION, AuthPairingStatus, AuthScope, PluginRegistry, ProviderAuth, ProviderProfile,
    ProviderProfileDiagnostic, ProviderProfileStatus, ProviderProfileUpsert, ProviderType, Store,
    WorkflowSourceUpdate,
    config::ApiConfig,
    execute_prepared_workflow_with_plugins,
    mcp::{McpClient, McpClientOptions, McpHttpServerConfig, McpStdioServerConfig, McpToolContent},
    plugin_install::{self, PluginSource},
};

struct AppState {
    store: Store,
    plugins: Mutex<PluginRegistry>,
    api_config: ApiConfig,
    rate_limit: Mutex<RateLimit>,
    minimax_quota_cache: Mutex<HashMap<String, QuotaCacheEntry>>,
}

struct QuotaCacheEntry {
    fetched_at: Instant,
    raw: serde_json::Value,
}

struct RateLimit {
    started_at: Instant,
    requests: u32,
}

fn plugin_snapshot(state: &AppState) -> PluginRegistry {
    state
        .plugins
        .lock()
        .expect("plugin catalog lock poisoned")
        .clone()
}

fn refresh_plugin_catalog(state: &AppState) -> Result<(), ApiError> {
    let registry =
        crate::load_installed_plugin_registry(&state.store).map_err(ApiError::internal)?;
    *state.plugins.lock().expect("plugin catalog lock poisoned") = registry;
    Ok(())
}

pub fn router(store: Store) -> Router {
    router_with_plugins_and_config(store, PluginRegistry::default(), ApiConfig::default())
}

pub fn router_with_plugins(store: Store, plugins: PluginRegistry) -> Router {
    router_with_plugins_and_config(store, plugins, ApiConfig::default())
}

pub fn router_with_plugins_and_config(
    store: Store,
    plugins: PluginRegistry,
    api_config: ApiConfig,
) -> Router {
    let body_limit = api_config.request_body_limit_bytes;
    let state = Arc::new(AppState {
        store,
        plugins: Mutex::new(plugins),
        api_config,
        rate_limit: Mutex::new(RateLimit {
            started_at: Instant::now(),
            requests: 0,
        }),
        minimax_quota_cache: Mutex::new(HashMap::new()),
    });
    let protected = Router::new()
        .route("/api/v1/info", get(info))
        .route("/api/v1/events", get(events))
        .route(
            "/api/v1/auth/tokens",
            get(list_auth_tokens).post(create_auth_token),
        )
        .route("/api/v1/auth/tokens/{id}/revoke", post(revoke_auth_token))
        .route("/api/v1/secrets", get(list_secrets).post(set_secret))
        .route("/api/v1/secrets/{name}", delete(delete_secret))
        .route(
            "/api/v1/providers",
            get(list_provider_profiles).post(upsert_provider_profile),
        )
        .route(
            "/api/v1/providers/{id}",
            get(get_provider_profile)
                .put(update_provider_profile)
                .delete(delete_provider_profile),
        )
        .route(
            "/api/v1/providers/{id}/diagnose",
            post(diagnose_provider_profile),
        )
        .route(
            "/api/v1/providers/minimax/capabilities",
            get(minimax_capabilities),
        )
        .route(
            "/api/v1/providers/minimax/quota/{secret_name}",
            get(minimax_quota),
        )
        .route("/api/v1/artifacts/{id}", get(get_artifact))
        .route("/api/v1/plugins/{name}/manifest", get(plugin_manifest))
        .route("/api/v1/plugins", get(list_plugins))
        .route("/api/v1/plugins/prepare", post(prepare_plugin))
        .route(
            "/api/v1/plugin-installations/{id}/commit",
            post(commit_plugin_installation),
        )
        .route("/api/v1/plugins/{id}/enable", post(enable_plugin))
        .route("/api/v1/plugins/{id}/disable", post(disable_plugin))
        .route("/api/v1/plugins/{id}", delete(delete_plugin))
        .route("/api/v1/mcp/call", post(mcp_call))
        .route(
            "/api/v1/workflows",
            get(list_workflows).post(create_workflow),
        )
        .route("/api/v1/workflows/analyze", post(analyze_workflow))
        .route("/api/v1/workflows/catalog", get(workflow_catalog))
        .route(
            "/api/v1/workflows/{id}/source",
            get(get_workflow_source).put(update_workflow_source),
        )
        .route(
            "/api/v1/workflows/{id}/revisions",
            get(list_workflow_revisions),
        )
        .route(
            "/api/v1/workflows/{id}/revisions/compare",
            get(compare_workflow_revisions),
        )
        .route(
            "/api/v1/workflows/{id}/revisions/{revision}",
            get(get_workflow_revision),
        )
        .route("/api/v1/workflows/{id}/enable", post(enable_workflow))
        .route("/api/v1/workflows/{id}/disable", post(disable_workflow))
        .route(
            "/api/v1/executions",
            get(list_executions).post(create_execution),
        )
        .route("/api/v1/executions/{id}", get(get_execution))
        .route("/api/v1/executions/{id}/trace", get(get_execution_trace))
        .route("/api/v1/executions/{id}/cancel", post(cancel_execution))
        .layer(from_fn_with_state(state.clone(), require_auth));
    Router::new()
        .route("/health/live", get(health))
        .route("/health/ready", get(health))
        .route("/api/v1/auth/pair/claim", post(claim_auth_pairing))
        .route("/api/v1/auth/pair/status", post(auth_pairing_status))
        .route("/api/v1/auth/pair/exchange", post(exchange_auth_pairing))
        .merge(protected)
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(from_fn_with_state(state.clone(), enforce_local_policy))
        .with_state(state)
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn minimax_capabilities() -> Json<serde_json::Value> {
    Json(json!({
        "apiVersion":"kakune.dev/v1",
        "kind":"ProviderModelCapabilities",
        "provider":{"id":"minimax","displayName":"MiniMax Token Plan","capabilities":["text-generation","streaming","tools","structured-output","usage-reporting"]},
        "models":[{"id":"MiniMax-M3","displayName":"MiniMax M3","capabilities":["text-generation","streaming","tools","structured-output","json-schema-output"]}],
        "limits":{"cancellation":"connection-best-effort","tokenBudget":"preventive-output-token-reservation","cost":"unknown-unless-provider-reports"}
    }))
}

/// A direct, non-AI MCP tool invocation. Credentials are always named store
/// references; their values are resolved only while configuring the client.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct McpCallRequest {
    transport: McpCallTransport,
    tool_name: String,
    arguments: serde_json::Map<String, serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(tag = "transport", rename_all = "lowercase", deny_unknown_fields)]
enum McpCallTransport {
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        current_dir: Option<std::path::PathBuf>,
        #[serde(default)]
        environment_secret_refs: BTreeMap<String, String>,
    },
    Http {
        endpoint: String,
        bearer_secret_ref: Option<String>,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpCallResponse {
    transport: McpCallTransportInfo,
    result: McpCallResult,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct McpCallTransportInfo {
    #[serde(rename = "type")]
    transport_type: &'static str,
    server: McpCallServerInfo,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct McpCallServerInfo {
    protocol_version: String,
    name: String,
    version: String,
    instructions: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct McpCallResult {
    content: Vec<McpCallContent>,
    structured_content: Option<serde_json::Value>,
    is_error: bool,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum McpCallContent {
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
    Resource {
        resource: McpCallResource,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct McpCallResource {
    uri: String,
    mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    blob: Option<String>,
}

/// A direct MCP request failed without exposing server or credential details.
#[derive(Clone, Copy, Debug)]
pub struct McpCallError;

/// Runs a single configured MCP call without exposing configuration credentials.
pub async fn call_mcp(
    store: &Store,
    request: McpCallRequest,
) -> Result<McpCallResponse, McpCallError> {
    let (mut client, transport_type) = match request.transport {
        McpCallTransport::Stdio {
            command,
            args,
            current_dir,
            environment_secret_refs,
        } => {
            let mut env = BTreeMap::new();
            for (environment_name, secret_name) in environment_secret_refs {
                let value = store
                    .resolve_secret(&secret_name)
                    .map_err(|_| McpCallError)?;
                env.insert(environment_name, value);
            }
            (
                McpClient::connect(
                    McpStdioServerConfig {
                        command,
                        args,
                        current_dir,
                        env,
                    },
                    McpClientOptions::default(),
                )
                .await
                .map_err(|_| McpCallError)?,
                "stdio",
            )
        }
        McpCallTransport::Http {
            endpoint,
            bearer_secret_ref,
        } => {
            let bearer_token = bearer_secret_ref
                .as_deref()
                .map(|secret_name| store.resolve_secret(secret_name).map_err(|_| McpCallError))
                .transpose()?;
            (
                McpClient::connect_http(
                    McpHttpServerConfig {
                        endpoint,
                        bearer_token,
                    },
                    McpClientOptions::default(),
                )
                .await
                .map_err(|_| McpCallError)?,
                "http",
            )
        }
    };
    let server_info = client.server_info().clone();
    let result = client
        .call_tool(&request.tool_name, request.arguments)
        .await
        .map_err(|_| McpCallError)?;
    let _ = client.cancel().await;
    Ok(McpCallResponse {
        transport: McpCallTransportInfo {
            transport_type,
            server: McpCallServerInfo {
                protocol_version: server_info.protocol_version,
                name: server_info.name,
                version: server_info.version,
                instructions: server_info.instructions,
            },
        },
        result: McpCallResult {
            content: result.content.into_iter().map(mcp_call_content).collect(),
            structured_content: result.structured_content,
            is_error: result.is_error,
        },
    })
}

fn mcp_call_content(content: McpToolContent) -> McpCallContent {
    match content {
        McpToolContent::Text { text } => McpCallContent::Text { text },
        McpToolContent::Image { data, mime_type } => McpCallContent::Image { data, mime_type },
        McpToolContent::Audio { data, mime_type } => McpCallContent::Audio { data, mime_type },
        McpToolContent::ResourceLink {
            uri,
            name,
            description,
            mime_type,
            size,
        } => McpCallContent::ResourceLink {
            uri,
            name,
            description,
            mime_type,
            size,
        },
        McpToolContent::EmbeddedTextResource {
            uri,
            mime_type,
            text,
        } => McpCallContent::Resource {
            resource: McpCallResource {
                uri,
                mime_type,
                text: Some(text),
                blob: None,
            },
        },
        McpToolContent::EmbeddedBlobResource {
            uri,
            mime_type,
            blob,
        } => McpCallContent::Resource {
            resource: McpCallResource {
                uri,
                mime_type,
                text: None,
                blob: Some(blob),
            },
        },
    }
}

async fn mcp_call(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<serde_json::Value>,
) -> Result<Json<McpCallResponse>, ApiError> {
    let request = serde_json::from_value(payload)
        .map_err(|error| ApiError::invalid(format!("invalid MCP call request: {error}")))?;
    call_mcp(&state.store, request)
        .await
        .map(Json)
        .map_err(|_| ApiError::bad_gateway("MCP call could not be completed"))
}

async fn minimax_quota(
    State(state): State<Arc<AppState>>,
    Path(secret_name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    if let Some(entry) = state
        .minimax_quota_cache
        .lock()
        .map_err(|_| ApiError::internal("MiniMax quota cache lock was poisoned"))?
        .get(&secret_name)
        .filter(|entry| entry.fetched_at.elapsed() < Duration::from_secs(60))
    {
        return Ok(Json(
            json!({"provider":"minimax","cached":true,"raw":entry.raw}),
        ));
    }
    let key = state
        .store
        .resolve_secret(&secret_name)
        .map_err(ApiError::invalid)?;
    let quota = crate::minimax::MiniMaxClient::new(key)
        .map_err(|error| ApiError::invalid(error.to_string()))?
        .token_plan_remains()
        .await
        .map_err(|error| ApiError::invalid(error.to_string()))?;
    state
        .minimax_quota_cache
        .lock()
        .map_err(|_| ApiError::internal("MiniMax quota cache lock was poisoned"))?
        .insert(
            secret_name,
            QuotaCacheEntry {
                fetched_at: Instant::now(),
                raw: quota.raw.clone(),
            },
        );
    Ok(Json(
        json!({"provider":"minimax","cached":false,"raw":quota.raw}),
    ))
}

async fn enforce_local_policy(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    if !allowed_host(request.headers())
        && !configured_host(request.headers(), &state.api_config.allowed_hosts)
    {
        return ApiError::forbidden("the Host header is not a local Core address").into_response();
    }
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    if let Some(origin) = &origin
        && !state
            .api_config
            .allowed_origins
            .iter()
            .any(|allowed| allowed == origin)
    {
        return ApiError::forbidden("the request Origin is not allowed by local Core policy")
            .into_response();
    }
    if request.method() == axum::http::Method::OPTIONS {
        let Some(origin) = origin else {
            return ApiError::invalid("CORS preflight requests require an Origin header")
                .into_response();
        };
        return cors_response(StatusCode::NO_CONTENT.into_response(), &origin);
    }
    if let Err(retry_after) = take_rate_limit(&state) {
        return ApiError::rate_limited(retry_after).into_response();
    }
    let response = next.run(request).await;
    if let Some(origin) = origin {
        cors_response(response, &origin)
    } else {
        response
    }
}

fn allowed_host(headers: &HeaderMap) -> bool {
    let Some(host) = headers.get(header::HOST) else {
        return true;
    };
    let Ok(host) = host.to_str() else {
        return false;
    };
    let host = if host.starts_with('[') {
        host.split(']')
            .next()
            .unwrap_or_default()
            .trim_start_matches('[')
    } else {
        host.split(':').next().unwrap_or_default()
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn configured_host(headers: &HeaderMap, allowed: &[String]) -> bool {
    headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .and_then(|host| host.parse::<axum::http::uri::Authority>().ok())
        .is_some_and(|authority| {
            allowed.iter().any(|host| {
                authority
                    .host()
                    .trim_matches(['[', ']'])
                    .eq_ignore_ascii_case(host.trim_matches(['[', ']']))
            })
        })
}

fn cors_response(mut response: Response, origin: &str) -> Response {
    let headers = response.headers_mut();
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_str(origin).expect("validated configured origin must be a header value"),
    );
    headers.insert(header::VARY, HeaderValue::from_static("Origin"));
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("Authorization, Content-Type, If-Match, Last-Event-ID"),
    );
    response
}

fn take_rate_limit(state: &AppState) -> Result<(), u64> {
    let mut rate_limit = state.rate_limit.lock().map_err(|_| 60_u64)?;
    let elapsed = rate_limit.started_at.elapsed();
    if elapsed.as_secs() >= 60 {
        rate_limit.started_at = Instant::now();
        rate_limit.requests = 0;
    }
    if rate_limit.requests >= state.api_config.rate_limit_requests_per_minute {
        return Err(60 - elapsed.as_secs().min(60));
    }
    rate_limit.requests += 1;
    Ok(())
}

async fn require_auth(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    let scope = required_scope(request.method().as_str(), request.uri().path());
    match token.map(|token| state.store.authorize_scope(token, scope)) {
        Some(Ok(true)) => next.run(request).await,
        Some(Ok(false)) | None => {
            ApiError::unauthorized("a valid Bearer token is required").into_response()
        }
        Some(Err(error)) => ApiError::internal(error).into_response(),
    }
}

fn required_scope(method: &str, path: &str) -> AuthScope {
    if path.starts_with("/api/v1/auth/") {
        return AuthScope::Admin;
    }
    if path.starts_with("/api/v1/plugins") && method != "GET" {
        return AuthScope::Admin;
    }
    if path.starts_with("/api/v1/secrets") {
        return AuthScope::Manage;
    }
    if path == "/api/v1/providers/minimax/quota"
        || path.starts_with("/api/v1/providers/minimax/quota/")
    {
        return AuthScope::Manage;
    }
    if method == "POST" && path == "/api/v1/executions" {
        return AuthScope::Run;
    }
    if method == "GET" {
        return AuthScope::Read;
    }
    AuthScope::Manage
}

async fn info(State(state): State<Arc<AppState>>) -> Result<Json<CoreInfo>, ApiError> {
    Ok(Json(CoreInfo {
        api_version: API_VERSION,
        core_version: env!("CARGO_PKG_VERSION"),
        core_id: state.store.core_id().map_err(ApiError::internal)?,
        storage_schema_version: state.store.schema_version().map_err(ApiError::internal)?,
        server_time: timestamp().unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string()),
        capabilities: vec![
            "workflows".to_string(),
            "executions".to_string(),
            "events".to_string(),
            "minimax".to_string(),
            "durable-events".to_string(),
            "native-core-plugin".to_string(),
            "device-pairing".to_string(),
        ],
    }))
}

async fn events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<EventsQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let cursor = query.cursor.as_deref().or_else(|| {
        headers
            .get("last-event-id")
            .and_then(|value| value.to_str().ok())
    });
    let events = match state.store.list_events_after_limited(cursor, 128) {
        Ok(events) => events,
        Err(error) if error.starts_with("event cursor ") => return Err(ApiError::invalid(error)),
        Err(error) => return Err(ApiError::internal(error)),
    };
    let next_cursor = events
        .last()
        .map(|event| event.event_id.clone())
        .or_else(|| {
            cursor
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        });
    let stream = stream::unfold(
        (state.store.clone(), VecDeque::from(events), next_cursor),
        |(store, mut pending, mut cursor)| async move {
            loop {
                if let Some(event) = pending.pop_front() {
                    cursor = Some(event.event_id.clone());
                    let data = serde_json::to_string(&event)
                        .expect("durable event envelopes must be serializable");
                    let event = Event::default()
                        .id(event.event_id)
                        .event(event.event_type)
                        .data(data);
                    return Some((Ok(event), (store, pending, cursor)));
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
                match store.list_events_after_limited(cursor.as_deref(), 128) {
                    Ok(events) => pending = VecDeque::from(events),
                    // Database failures must not turn a transient read failure into a
                    // permanently closed subscription. The next bounded poll retries.
                    Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
                }
            }
        },
    );
    Ok(Sse::new(stream).keep_alive(KeepAlive::default().interval(Duration::from_secs(15))))
}

async fn list_auth_tokens(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ListResponse<crate::AuthTokenRecord>>, ApiError> {
    Ok(Json(ListResponse {
        items: state.store.list_auth_tokens().map_err(ApiError::internal)?,
    }))
}

async fn get_artifact(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let Some((artifact, bytes)) = state.store.read_artifact(&id).map_err(|error| {
        if error.starts_with("artifact id ") {
            ApiError::invalid(error)
        } else {
            ApiError::internal(error)
        }
    })?
    else {
        return Err(ApiError::not_found(format!("artifact {id} was not found")));
    };
    let media_type = artifact
        .media_type
        .parse::<HeaderValue>()
        .map_err(|_| ApiError::internal("persisted artifact has an invalid media type"))?;
    let content_length = bytes
        .len()
        .to_string()
        .parse::<HeaderValue>()
        .map_err(|_| ApiError::internal("artifact size cannot be represented as a header"))?;
    let mut response = bytes.into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, media_type);
    response
        .headers_mut()
        .insert(header::CONTENT_LENGTH, content_length);
    Ok(response)
}

async fn create_auth_token(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CreateAuthTokenRequest>,
) -> Result<(StatusCode, Json<crate::CreatedAuthToken>), ApiError> {
    let token = state
        .store
        .create_auth_token(request.name, request.scopes, request.expires_at)
        .map_err(ApiError::invalid)?;
    Ok((StatusCode::CREATED, Json(token)))
}

async fn claim_auth_pairing(
    State(state): State<Arc<AppState>>,
    Json(request): Json<ClaimAuthPairingRequest>,
) -> Result<Json<AuthPairingResponse>, ApiError> {
    let status = state
        .store
        .claim_auth_pairing_code(
            &request.pairing_code,
            &request.claim_secret,
            &request.device_name,
        )
        .map_err(pairing_api_error)?;
    Ok(Json(AuthPairingResponse {
        core_id: state.store.core_id().map_err(ApiError::internal)?,
        status,
    }))
}

async fn auth_pairing_status(
    State(state): State<Arc<AppState>>,
    Json(request): Json<PairingProofRequest>,
) -> Result<Json<AuthPairingResponse>, ApiError> {
    let status = state
        .store
        .get_auth_pairing_status_for_claim(&request.pairing_code, &request.claim_secret)
        .map_err(pairing_api_error)?;
    Ok(Json(AuthPairingResponse {
        core_id: state.store.core_id().map_err(ApiError::internal)?,
        status,
    }))
}

async fn exchange_auth_pairing(
    State(state): State<Arc<AppState>>,
    Json(request): Json<PairingProofRequest>,
) -> Result<(StatusCode, Json<AuthPairingExchangeResponse>), ApiError> {
    let created = state
        .store
        .exchange_auth_pairing_code(&request.pairing_code, &request.claim_secret)
        .map_err(pairing_api_error)?;
    Ok((
        StatusCode::CREATED,
        Json(AuthPairingExchangeResponse {
            core_id: state.store.core_id().map_err(ApiError::internal)?,
            token: created.token,
            token_id: created.record.id,
            name: created.record.name,
            scopes: created.record.scopes,
            expires_at: created.record.expires_at,
        }),
    ))
}

fn pairing_api_error(error: String) -> ApiError {
    if error.starts_with("pairing QR code is invalid") {
        ApiError::unauthorized(error)
    } else if error.contains("already has a claim") || error.contains("awaiting Core user approval")
    {
        ApiError::conflict(error)
    } else if error.contains("was rejected by the Core user") {
        ApiError::forbidden(error)
    } else if error.starts_with("deviceName") {
        ApiError::invalid(error)
    } else {
        ApiError::internal(error)
    }
}

async fn revoke_auth_token(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    if state
        .store
        .revoke_auth_token(&id)
        .map_err(ApiError::internal)?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found(format!(
            "active token {id} was not found"
        )))
    }
}

async fn list_secrets(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ListResponse<crate::SecretRecord>>, ApiError> {
    Ok(Json(ListResponse {
        items: state.store.list_secrets().map_err(ApiError::internal)?,
    }))
}

async fn set_secret(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SetSecretRequest>,
) -> Result<(StatusCode, Json<crate::SecretRecord>), ApiError> {
    let existed = state
        .store
        .list_secrets()
        .map_err(ApiError::internal)?
        .iter()
        .any(|secret| secret.name == request.name);
    let record = state
        .store
        .set_secret(&request.name, &request.value)
        .map_err(ApiError::invalid)?;
    Ok((
        if existed {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(record),
    ))
}

async fn delete_secret(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    if state
        .store
        .delete_secret(&name)
        .map_err(ApiError::internal)?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found(format!("secret {name} was not found")))
    }
}

async fn list_provider_profiles(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ListResponse<ProviderProfile>>, ApiError> {
    Ok(Json(ListResponse {
        items: state
            .store
            .list_provider_profiles()
            .map_err(ApiError::internal)?,
    }))
}

async fn get_provider_profile(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ProviderProfile>, ApiError> {
    let profile = state
        .store
        .get_provider_profile(&id)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("provider profile {id} was not found")))?;
    Ok(Json(profile))
}

async fn upsert_provider_profile(
    State(state): State<Arc<AppState>>,
    Json(profile): Json<ProviderProfileUpsert>,
) -> Result<(StatusCode, Json<ProviderProfile>), ApiError> {
    save_provider_profile(state, None, profile).await
}

async fn update_provider_profile(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(profile): Json<ProviderProfileUpsert>,
) -> Result<(StatusCode, Json<ProviderProfile>), ApiError> {
    save_provider_profile(state, Some(id), profile).await
}

async fn save_provider_profile(
    state: Arc<AppState>,
    path_id: Option<String>,
    profile: ProviderProfileUpsert,
) -> Result<(StatusCode, Json<ProviderProfile>), ApiError> {
    if let Some(path_id) = path_id
        && profile.id != path_id
    {
        return Err(ApiError::invalid("provider profile id must match the URL"));
    }
    let existed = state
        .store
        .get_provider_profile(&profile.id)
        .map_err(ApiError::internal)?
        .is_some();
    let profile = state
        .store
        .upsert_provider_profile(profile)
        .map_err(ApiError::invalid)?;
    Ok((
        if existed {
            StatusCode::OK
        } else {
            StatusCode::CREATED
        },
        Json(profile),
    ))
}

async fn delete_provider_profile(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .store
        .delete_provider_profile(&id)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("provider profile {id} was not found")))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn diagnose_provider_profile(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ProviderProfile>, ApiError> {
    let profile = state
        .store
        .get_provider_profile(&id)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("provider profile {id} was not found")))?;
    let diagnostic = provider_profile_diagnostic(&state.store, &profile);
    let profile = state
        .store
        .diagnose_provider_profile(&id, diagnostic)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("provider profile {id} was not found")))?;
    Ok(Json(profile))
}

fn provider_profile_diagnostic(
    store: &Store,
    profile: &ProviderProfile,
) -> ProviderProfileDiagnostic {
    match (&profile.provider_type, &profile.auth) {
        (_, ProviderAuth::ApiKeySecret { secret_ref }) => {
            let present = store
                .list_secrets()
                .is_ok_and(|secrets| secrets.iter().any(|secret| secret.name == *secret_ref));
            ProviderProfileDiagnostic {
                status: if present {
                    ProviderProfileStatus::Available
                } else {
                    ProviderProfileStatus::Unavailable
                },
                checked_at: None,
                message: Some(
                    if present {
                        "referenced API key secret is available"
                    } else {
                        "referenced API key secret is unavailable"
                    }
                    .to_string(),
                ),
                details: Some(json!({"check":"apiKeySecret","present":present})),
            }
        }
        (ProviderType::Codex, ProviderAuth::OAuthSecret { secret_ref }) => {
            let available = store
                .resolve_secret(secret_ref)
                .is_ok_and(|secret| crate::codex::CodexOAuthTokens::from_secret(&secret).is_ok());
            ProviderProfileDiagnostic {
                status: if available {
                    ProviderProfileStatus::Available
                } else {
                    ProviderProfileStatus::Unavailable
                },
                checked_at: None,
                message: Some(
                    if available {
                        "ChatGPT OAuth credentials are available"
                    } else {
                        "ChatGPT OAuth credentials are unavailable or invalid"
                    }
                    .to_string(),
                ),
                details: Some(json!({"check":"chatgptOAuth","available":available})),
            }
        }
        // Store validation prevents this combination, but keep diagnosis fail-closed.
        _ => ProviderProfileDiagnostic {
            status: ProviderProfileStatus::Unavailable,
            checked_at: None,
            message: Some("provider authentication configuration is unsupported".to_string()),
            details: Some(json!({"check":"authentication","supported":false})),
        },
    }
}

async fn plugin_manifest(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let plugin = state
        .store
        .list_installed_plugins()
        .map_err(ApiError::internal)?
        .into_iter()
        .find(|plugin| plugin.id == name)
        .ok_or_else(|| ApiError::not_found(format!("plugin {name} is not installed")))?;
    let source = std::fs::read_to_string(&plugin.manifest_path).map_err(|error| {
        ApiError::internal(format!("cannot read installed plugin manifest: {error}"))
    })?;
    serde_json::from_str(&source).map(Json).map_err(|error| {
        ApiError::internal(format!("installed plugin manifest is invalid: {error}"))
    })
}

async fn list_plugins(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ListResponse<crate::InstalledPluginRecord>>, ApiError> {
    Ok(Json(ListResponse {
        items: state
            .store
            .list_installed_plugins()
            .map_err(ApiError::internal)?,
    }))
}

async fn prepare_plugin(
    State(state): State<Arc<AppState>>,
    Json(request): Json<PreparePluginRequest>,
) -> Result<(StatusCode, Json<crate::PreparedPluginInstallRecord>), ApiError> {
    let source = PluginSource::parse(&request.source).map_err(ApiError::invalid)?;
    let prepared = plugin_install::prepare(&state.store, source)
        .await
        .map_err(ApiError::invalid)?;
    Ok((StatusCode::ACCEPTED, Json(prepared.record)))
}

async fn commit_plugin_installation(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(request): Json<CommitPluginInstallationRequest>,
) -> Result<(StatusCode, Json<crate::InstalledPluginRecord>), ApiError> {
    let installed =
        plugin_install::commit(&state.store, &id, &request.digest).map_err(ApiError::invalid)?;
    refresh_plugin_catalog(&state)?;
    Ok((StatusCode::CREATED, Json(installed)))
}

async fn enable_plugin(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowStatus>, ApiError> {
    plugin_status(state, id, true).await
}
async fn disable_plugin(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowStatus>, ApiError> {
    plugin_status(state, id, false).await
}
async fn plugin_status(
    state: Arc<AppState>,
    id: String,
    enabled: bool,
) -> Result<Json<WorkflowStatus>, ApiError> {
    if state
        .store
        .set_plugin_enabled(&id, enabled)
        .map_err(ApiError::internal)?
    {
        refresh_plugin_catalog(&state)?;
        Ok(Json(WorkflowStatus {
            id,
            status: if enabled { "enabled" } else { "disabled" }.to_string(),
        }))
    } else {
        Err(ApiError::not_found("plugin is not installed"))
    }
}
async fn delete_plugin(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .store
        .remove_plugin_install(&id)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found("plugin is not installed"))?;
    refresh_plugin_catalog(&state)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_workflows(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ListResponse<crate::WorkflowRecord>>, ApiError> {
    Ok(Json(ListResponse {
        items: state.store.list_workflows().map_err(ApiError::internal)?,
    }))
}

async fn get_workflow_source(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<(HeaderMap, Json<WorkflowSourceResponse>), ApiError> {
    let record = state
        .store
        .get_workflow_source(&id)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("workflow {id} was not found")))?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{}\"", record.revision)).map_err(|error| {
            ApiError::internal(format!("cannot encode workflow revision: {error}"))
        })?,
    );
    Ok((
        headers,
        Json(WorkflowSourceResponse {
            id,
            source: record.source,
            revision: record.revision,
        }),
    ))
}

async fn list_workflow_revisions(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ListResponse<crate::WorkflowRevisionRecord>>, ApiError> {
    Ok(Json(ListResponse {
        items: state
            .store
            .list_workflow_revisions(&id)
            .map_err(ApiError::internal)?,
    }))
}

async fn get_workflow_revision(
    State(state): State<Arc<AppState>>,
    Path((id, revision)): Path<(String, String)>,
) -> Result<Json<crate::WorkflowRevisionSource>, ApiError> {
    state
        .store
        .workflow_revision(&id, &revision)
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("workflow revision {revision} was not found")))
}

#[derive(Deserialize)]
struct RevisionComparisonQuery {
    base: String,
    head: String,
}

async fn compare_workflow_revisions(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<RevisionComparisonQuery>,
) -> Result<Json<crate::WorkflowRevisionComparison>, ApiError> {
    state
        .store
        .compare_workflow_revisions(&id, &query.base, &query.head)
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("one or both workflow revisions were not found"))
}

async fn analyze_workflow(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<WorkflowSource>,
) -> Result<Json<crate::WorkflowAnalysis>, ApiError> {
    let mut analysis = crate::planner::analyze(&payload.source, &plugin_snapshot(&state));
    if let Some(workflow) = &analysis.workflow {
        let profiles = state
            .store
            .list_provider_profiles()
            .map_err(ApiError::internal)?;
        validate_workflow_ai_profiles(
            &workflow.nodes,
            "/nodes",
            &profiles,
            &mut analysis.diagnostics,
        );
    }
    Ok(Json(analysis))
}

fn validate_workflow_ai_profiles(
    nodes: &[crate::workflow::WorkflowNode],
    pointer: &str,
    profiles: &[ProviderProfile],
    diagnostics: &mut Vec<crate::Diagnostic>,
) {
    for (index, node) in nodes.iter().enumerate() {
        let node_pointer = format!("{pointer}/{index}");
        if node.node_type.starts_with("kakune.ai.") {
            let provider = node
                .with
                .get("provider")
                .and_then(serde_json::Value::as_str);
            let model = node.with.get("model").and_then(serde_json::Value::as_str);
            let expected_type = match node.node_type.as_str() {
                "kakune.ai.codex.exec@1" => Some(ProviderType::Codex),
                "kakune.ai.minimax.messages@1" => Some(ProviderType::MiniMax),
                _ => None,
            };
            let profile = provider.and_then(|id| profiles.iter().find(|profile| profile.id == id));
            match profile {
                None => diagnostics.push(crate::Diagnostic::error(
                    "provider.unknown",
                    "AI node references an unknown provider profile",
                    Some(format!("{node_pointer}/with/provider")),
                    None,
                )),
                Some(profile)
                    if expected_type.is_some_and(|expected| profile.provider_type != expected) =>
                {
                    diagnostics.push(crate::Diagnostic::error(
                        "provider.incompatible",
                        "provider profile is incompatible with this AI node",
                        Some(format!("{node_pointer}/with/provider")),
                        None,
                    ))
                }
                Some(profile)
                    if !model.is_some_and(|model| {
                        profile
                            .allowed_models
                            .iter()
                            .any(|allowed| allowed == model)
                    }) =>
                {
                    diagnostics.push(crate::Diagnostic::error(
                        "provider.model_incompatible",
                        "model is not allowed by the selected provider profile",
                        Some(format!("{node_pointer}/with/model")),
                        None,
                    ))
                }
                Some(_) => {}
            }
        }
        if let Some(body) = &node.body {
            validate_workflow_ai_profiles(
                &body.nodes,
                &format!("{node_pointer}/body/nodes"),
                profiles,
                diagnostics,
            );
        }
        for (branch, subgraph) in &node.branches {
            validate_workflow_ai_profiles(
                &subgraph.nodes,
                &format!("{node_pointer}/branches/{branch}/nodes"),
                profiles,
                diagnostics,
            );
        }
    }
}

async fn workflow_catalog(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let providers = state
        .store
        .list_provider_profiles()
        .map_err(ApiError::internal)?
        .into_iter()
        .map(|profile| {
            json!({
                "id": profile.id,
                "providerType": profile.provider_type,
                "displayName": profile.display_name,
                "allowedModels": profile.allowed_models,
                "defaultModel": profile.default_model,
                "capabilities": profile.capabilities,
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(
        json!({ "nodes": crate::planner::visual_catalog(), "providers": providers }),
    ))
}

async fn create_workflow(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<WorkflowSource>,
) -> Result<(StatusCode, Json<crate::WorkflowRecord>), ApiError> {
    let plugins = plugin_snapshot(&state);
    let workflow = crate::planner::compile(&payload.source, &plugins)
        .map_err(|diagnostics| {
            ApiError::invalid(crate::planner::diagnostics_message(&diagnostics))
        })?
        .ir;
    let record = state
        .store
        .upsert_workflow(&workflow, &payload.source, "enabled")
        .map_err(ApiError::internal)?;
    Ok((StatusCode::CREATED, Json(record)))
}

async fn update_workflow_source(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(payload): Json<WorkflowSource>,
) -> Result<(HeaderMap, Json<crate::WorkflowRecord>), ApiError> {
    let plugins = plugin_snapshot(&state);
    let mut analysis = crate::planner::analyze(&payload.source, &plugins);
    if let Some(workflow) = &analysis.workflow {
        let profiles = state
            .store
            .list_provider_profiles()
            .map_err(ApiError::internal)?;
        validate_workflow_ai_profiles(
            &workflow.nodes,
            "/nodes",
            &profiles,
            &mut analysis.diagnostics,
        );
    }
    if !analysis.diagnostics.is_empty() {
        return Err(ApiError::invalid(crate::planner::diagnostics_message(
            &analysis.diagnostics,
        )));
    }
    // A document with a temporarily unavailable plugin stays editable. Runtime
    // compilation still rejects it until that plugin is installed again.
    let workflow = analysis.workflow.expect("valid analysis has a workflow");
    if workflow.metadata.id != id {
        return Err(ApiError::invalid("metadata.id must match the workflow URL"));
    }
    let expected = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ApiError::invalid("If-Match is required when updating workflow source"))?;
    let record = match state
        .store
        .update_workflow_if_revision(&workflow, &payload.source, expected.trim_matches('"'))
        .map_err(ApiError::internal)?
    {
        WorkflowSourceUpdate::Updated(record) => record,
        WorkflowSourceUpdate::NotFound => {
            return Err(ApiError::not_found(format!("workflow {id} was not found")));
        }
        WorkflowSourceUpdate::Conflict { current_revision } => {
            return Err(ApiError::conflict(format!(
                "workflow source changed (current revision: {current_revision}); fetch the latest revision before saving",
            )));
        }
    };
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        header::ETAG,
        HeaderValue::from_str(&format!("\"{}\"", record.revision))
            .map_err(|error| ApiError::internal(error.to_string()))?,
    );
    Ok((response_headers, Json(record)))
}

async fn enable_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowStatus>, ApiError> {
    state
        .store
        .set_workflow_status(&id, "enabled")
        .map_err(ApiError::internal)?;
    Ok(Json(WorkflowStatus {
        id,
        status: "enabled".to_string(),
    }))
}

async fn disable_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowStatus>, ApiError> {
    state
        .store
        .set_workflow_status(&id, "disabled")
        .map_err(ApiError::internal)?;
    Ok(Json(WorkflowStatus {
        id,
        status: "disabled".to_string(),
    }))
}

async fn create_execution(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<ExecutionRequest>,
) -> Result<(StatusCode, Json<crate::ExecutionRecord>), ApiError> {
    let key = headers
        .get("Idempotency-Key")
        .map(|value| {
            value
                .to_str()
                .map_err(|_| ApiError::invalid("invalid Idempotency-Key"))
        })
        .transpose()?;
    if key.is_some_and(|key| {
        key.is_empty() || key.len() > 128 || !key.bytes().all(|byte| byte.is_ascii_graphic())
    }) {
        return Err(ApiError::invalid(
            "Idempotency-Key must contain 1 to 128 visible ASCII characters",
        ));
    }
    let inputs = payload.inputs.unwrap_or_else(|| json!({}));
    let hash = crate::artifacts::artifact_id(
        json!({"workflowId":payload.workflow_id,"inputs":inputs})
            .to_string()
            .as_bytes(),
    );
    if let Some(key) = key
        && let Some(execution) = state
            .store
            .find_idempotent_execution(key, &hash)
            .map_err(execution_request_error)?
    {
        return Ok((StatusCode::OK, Json(execution)));
    }
    let source = state
        .store
        .get_workflow_source(&payload.workflow_id)
        .map_err(ApiError::internal)?
        .ok_or_else(|| {
            ApiError::not_found(format!("workflow {} was not found", payload.workflow_id))
        })?;
    let plugins = plugin_snapshot(&state);
    let (execution, created) = crate::runtime::prepare_execution_with_context_request(
        &state.store,
        &source.source,
        &source.revision,
        &plugins,
        inputs,
        serde_json::Value::Object(Default::default()),
        key.map(|key| (key, hash.as_str())),
    )
    .map_err(execution_request_error)?;
    if !created {
        return Ok((StatusCode::OK, Json(execution)));
    }
    let store = state.store.clone();
    let background_execution = execution.clone();
    tokio::task::spawn_blocking(move || {
        if let Err(error) =
            execute_prepared_workflow_with_plugins(&store, background_execution.clone(), &plugins)
        {
            eprintln!(
                "kakune execution {} failed: {error}",
                background_execution.id
            );
        }
    });
    Ok((StatusCode::CREATED, Json(execution)))
}

fn execution_request_error(error: String) -> ApiError {
    if error.starts_with("idempotency key") {
        ApiError::conflict(error)
    } else {
        ApiError::internal(error)
    }
}

async fn list_executions(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ListResponse<crate::ExecutionRecord>>, ApiError> {
    Ok(Json(ListResponse {
        items: state.store.list_executions().map_err(ApiError::internal)?,
    }))
}

async fn get_execution(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<ExecutionDetail>, ApiError> {
    let execution = state
        .store
        .get_execution(&id)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("execution {id} was not found")))?;
    let nodes = state
        .store
        .list_node_runs(&id)
        .map_err(ApiError::internal)?;
    let result = state
        .store
        .execution_result(&id)
        .map_err(ApiError::internal)?;
    Ok(Json(ExecutionDetail {
        execution,
        nodes,
        result,
    }))
}

async fn get_execution_trace(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<crate::ExecutionTraceRecord>, ApiError> {
    state
        .store
        .execution_trace(&id)
        .map_err(ApiError::internal)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(format!("execution {id} was not found")))
}

async fn cancel_execution(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<WorkflowStatus>, ApiError> {
    if state
        .store
        .request_execution_cancel(&id)
        .map_err(ApiError::internal)?
    {
        Ok(Json(WorkflowStatus {
            id,
            status: "cancelling".to_string(),
        }))
    } else {
        Err(ApiError::conflict(format!("execution {id} is not running")))
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CoreInfo {
    api_version: &'static str,
    core_version: &'static str,
    core_id: String,
    storage_schema_version: u32,
    server_time: String,
    capabilities: Vec<String>,
}

#[derive(Deserialize)]
struct EventsQuery {
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct WorkflowSource {
    source: String,
}

#[derive(Serialize)]
struct WorkflowSourceResponse {
    id: String,
    source: String,
    revision: String,
}

#[derive(Serialize)]
struct ListResponse<T> {
    items: Vec<T>,
}

#[derive(Serialize)]
struct ExecutionDetail {
    execution: crate::ExecutionRecord,
    nodes: Vec<crate::NodeRunRecord>,
    result: Option<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecutionRequest {
    workflow_id: String,
    #[serde(default)]
    inputs: Option<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateAuthTokenRequest {
    name: String,
    scopes: Vec<AuthScope>,
    expires_at: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClaimAuthPairingRequest {
    pairing_code: String,
    claim_secret: String,
    device_name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PairingProofRequest {
    pairing_code: String,
    claim_secret: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthPairingResponse {
    core_id: String,
    status: AuthPairingStatus,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthPairingExchangeResponse {
    core_id: String,
    token: String,
    token_id: String,
    name: String,
    scopes: Vec<AuthScope>,
    expires_at: Option<String>,
}

#[derive(Deserialize)]
struct SetSecretRequest {
    name: String,
    value: String,
}

#[derive(Deserialize)]
struct PreparePluginRequest {
    source: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommitPluginInstallationRequest {
    digest: String,
}

#[derive(Serialize)]
struct WorkflowStatus {
    id: String,
    status: String,
}

struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_request",
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "not_found",
            message: message.into(),
        }
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
            message: message.into(),
        }
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "conflict",
            message: message.into(),
        }
    }

    fn forbidden(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "forbidden",
            message: message.into(),
        }
    }

    fn rate_limited(retry_after: u64) -> Self {
        Self {
            status: StatusCode::TOO_MANY_REQUESTS,
            code: "rate_limited",
            message: format!("request limit reached; retry after {retry_after} seconds"),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "internal_error",
            message: message.into(),
        }
    }

    fn bad_gateway(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_GATEWAY,
            code: "mcp_call_failed",
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let request_id = uuid::Uuid::new_v4().to_string();
        let retry_after = (self.status == StatusCode::TOO_MANY_REQUESTS).then_some("60");
        let mut response = (
            self.status,
            Json(json!({
                "type": format!("https://kakune.dev/problems/{}", self.code),
                "title": self.status.canonical_reason().unwrap_or("Kakune API error"),
                "status": self.status.as_u16(),
                "detail": self.message,
                "code": self.code,
                "message": self.message,
                "details": null,
                "retryable": self.status.is_server_error() || self.status == StatusCode::TOO_MANY_REQUESTS,
                "requestId": request_id,
                "traceId": request_id
            })),
        )
            .into_response();
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        if let Some(retry_after) = retry_after {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static(retry_after));
        }
        response
    }
}

fn timestamp() -> Result<String, String> {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use crate::{AuthScope, PluginRegistry, Store, config::ApiConfig};

    use super::{router, router_with_plugins_and_config};

    #[tokio::test]
    async fn reports_the_public_core_contract() {
        let directory =
            std::env::temp_dir().join(format!("kakune-api-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let token = store
            .ensure_bootstrap_token()
            .expect("token should be created")
            .expect("a new store has no token");
        let app = router(store);
        let unauthorized = app
            .clone()
            .oneshot(
                Request::get("/api/v1/info")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should be produced");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            unauthorized
                .headers()
                .get("content-type")
                .expect("problem content type"),
            "application/problem+json"
        );
        let unauthorized_body = unauthorized
            .into_body()
            .collect()
            .await
            .expect("body should collect")
            .to_bytes();
        assert!(
            std::str::from_utf8(&unauthorized_body)
                .expect("body should be utf-8")
                .contains("unauthorized")
        );
        assert!(
            std::str::from_utf8(&unauthorized_body)
                .expect("body should be utf-8")
                .contains("https://kakune.dev/problems/unauthorized")
        );
        let response = app
            .oneshot(
                Request::get("/api/v1/info")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should be produced");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body should collect")
            .to_bytes();
        assert!(
            std::str::from_utf8(&body)
                .expect("body should be utf-8")
                .contains("apiVersion")
        );
        assert!(
            std::str::from_utf8(&body)
                .expect("body should be utf-8")
                .contains("coreId")
        );
        std::fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[tokio::test]
    async fn qr_pairing_endpoints_wait_for_local_approval_then_return_one_token() {
        let directory =
            std::env::temp_dir().join(format!("kakune-pairing-api-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let code = format!("kakune_pair_{}", uuid::Uuid::new_v4().simple());
        let claim_secret = format!("gui_claim_{}", uuid::Uuid::new_v4().simple());
        let expires_at = (time::OffsetDateTime::now_utc() + time::Duration::minutes(5))
            .format(&time::format_description::well_known::Rfc3339)
            .expect("expiration should format");
        let invitation = store
            .create_auth_pairing_code(&code, vec![AuthScope::Read, AuthScope::Run], expires_at)
            .expect("pairing invitation should be stored");
        let app = router(store.clone());
        let claim = app
            .clone()
            .oneshot(
                Request::post("/api/v1/auth/pair/claim")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "pairingCode": &code,
                            "claimSecret": &claim_secret,
                            "deviceName": "Kakune GUI test device",
                        })
                        .to_string(),
                    ))
                    .expect("claim request should build"),
            )
            .await
            .expect("claim response should be produced");
        assert_eq!(claim.status(), StatusCode::OK);
        let claim_json: serde_json::Value = serde_json::from_slice(
            &claim
                .into_body()
                .collect()
                .await
                .expect("claim body should collect")
                .to_bytes(),
        )
        .expect("claim response should be JSON");
        assert_eq!(claim_json["status"]["state"], "awaitingApproval");

        let pending_exchange = app
            .clone()
            .oneshot(
                Request::post("/api/v1/auth/pair/exchange")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "pairingCode": &code,
                            "claimSecret": &claim_secret,
                        })
                        .to_string(),
                    ))
                    .expect("exchange request should build"),
            )
            .await
            .expect("pending exchange response should be produced");
        assert_eq!(pending_exchange.status(), StatusCode::CONFLICT);

        assert!(
            store
                .decide_auth_pairing(&invitation.id, true)
                .expect("local approval should succeed")
        );
        let exchange = app
            .clone()
            .oneshot(
                Request::post("/api/v1/auth/pair/exchange")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "pairingCode": &code,
                            "claimSecret": &claim_secret,
                        })
                        .to_string(),
                    ))
                    .expect("exchange request should build"),
            )
            .await
            .expect("exchange response should be produced");
        assert_eq!(exchange.status(), StatusCode::CREATED);
        let exchange_json: serde_json::Value = serde_json::from_slice(
            &exchange
                .into_body()
                .collect()
                .await
                .expect("exchange body should collect")
                .to_bytes(),
        )
        .expect("exchange response should be JSON");
        let token = exchange_json["token"]
            .as_str()
            .expect("exchange response should contain a token");
        assert!(
            store
                .authorize_scope(token, AuthScope::Run)
                .expect("paired token should authorize its requested scope")
        );

        let replay = app
            .oneshot(
                Request::post("/api/v1/auth/pair/exchange")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "pairingCode": &code,
                            "claimSecret": &claim_secret,
                        })
                        .to_string(),
                    ))
                    .expect("replay request should build"),
            )
            .await
            .expect("replay response should be produced");
        assert_eq!(replay.status(), StatusCode::UNAUTHORIZED);

        drop(store);
        std::fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[tokio::test]
    async fn applies_explicit_cors_and_request_rate_limits() {
        let directory =
            std::env::temp_dir().join(format!("kakune-api-policy-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let app = router_with_plugins_and_config(
            store,
            PluginRegistry::default(),
            ApiConfig {
                allowed_origins: vec!["http://localhost:5173".to_string()],
                request_body_limit_bytes: 1_048_576,
                rate_limit_requests_per_minute: 1,
                ..ApiConfig::default()
            },
        );
        let allowed = app
            .clone()
            .oneshot(
                Request::get("/health/live")
                    .header("origin", "http://localhost:5173")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should be produced");
        assert_eq!(allowed.status(), StatusCode::OK);
        assert_eq!(
            allowed
                .headers()
                .get("access-control-allow-origin")
                .expect("CORS header"),
            "http://localhost:5173"
        );
        let denied = app
            .clone()
            .oneshot(
                Request::get("/health/live")
                    .header("origin", "https://untrusted.invalid")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should be produced");
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        let limited = app
            .oneshot(
                Request::get("/health/live")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should be produced");
        assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            limited.headers().get("retry-after").expect("retry header"),
            "60"
        );
        std::fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[tokio::test]
    async fn replays_durable_events_from_an_sse_cursor() {
        let directory =
            std::env::temp_dir().join(format!("kakune-events-api-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let token = store
            .ensure_bootstrap_token()
            .expect("token should be created")
            .expect("a new store has no token");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: sample\n  name: Sample\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: hello\nnodes:\n  - id: hello\n    type: kakune.log@1\n    inputs:\n      message: { literal: hello }\n";
        let workflow = crate::WorkflowDocument::parse(source).expect("workflow should parse");
        store
            .upsert_workflow(&workflow, source, "enabled")
            .expect("workflow should save");
        store
            .set_workflow_status("sample", "disabled")
            .expect("workflow status should save");
        let events = store.list_events_after(None).expect("events should list");
        let app = router(store);
        let response = app
            .clone()
            .oneshot(
                Request::get(format!("/api/v1/events?cursor={}", events[0].event_id))
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should be produced");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .expect("SSE content type should be present"),
            "text/event-stream"
        );
        let body = first_sse_frame(response.into_body()).await;
        assert!(body.contains(&format!("id: {}", events[1].event_id)));
        assert!(body.contains("event: workflow.status_changed"));
        assert!(body.contains("\"eventVersion\":\"1.0\""));
        assert!(!body.contains(&events[0].event_id));
        assert_eq!(body.matches("id: ").count(), 1);
        let response = app
            .oneshot(
                Request::get("/api/v1/events")
                    .header("authorization", format!("Bearer {token}"))
                    .header("last-event-id", &events[0].event_id)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should be produced");
        let body = first_sse_frame(response.into_body()).await;
        assert!(body.contains(&format!("id: {}", events[1].event_id)));
        std::fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[tokio::test]
    async fn manages_provider_profiles_without_returning_secret_values() {
        let directory =
            std::env::temp_dir().join(format!("kakune-provider-api-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let token = store
            .ensure_bootstrap_token()
            .expect("token should be created")
            .expect("a new store has no token");
        let app = router(store);
        let profile = r#"{
            "id":"primary-codex",
            "displayName":"Primary Codex",
            "providerType":"codex",
            "defaultModel":"gpt-5",
            "allowedModels":["gpt-5"],
            "capabilities":["text-generation"],
            "auth":{"type":"oauthSecret","secret_ref":"provider-key"},
            "config":{}
        }"#;
        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v1/providers")
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(profile))
                    .expect("request should build"),
            )
            .await
            .expect("response should be produced");
        assert_eq!(response.status(), StatusCode::CREATED);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body should collect")
            .to_bytes();
        let body = std::str::from_utf8(&body).expect("body should be UTF-8");
        assert!(body.contains("provider-key"));

        let response = app
            .clone()
            .oneshot(
                Request::post("/api/v1/providers/primary-codex/diagnose")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should be produced");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body should collect")
            .to_bytes();
        let body = std::str::from_utf8(&body).expect("body should be UTF-8");
        assert!(body.contains("\"status\":\"unavailable\""));
        assert!(!body.contains("value-that-must-not-be-returned"));

        let response = app
            .clone()
            .oneshot(
                Request::put("/api/v1/providers/not-primary")
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(profile))
                    .expect("request should build"),
            )
            .await
            .expect("response should be produced");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = app
            .oneshot(
                Request::delete("/api/v1/providers/primary-codex")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("response should be produced");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        std::fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    async fn first_sse_frame(mut body: Body) -> String {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(1), body.frame())
            .await
            .expect("an existing durable event should be streamed promptly")
            .expect("SSE body should contain a frame")
            .expect("SSE body frame should be valid");
        let data = frame.into_data().expect("SSE frame should contain data");
        String::from_utf8(data.to_vec()).expect("SSE data should be UTF-8")
    }
}
