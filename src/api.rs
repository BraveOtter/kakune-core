use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    convert::Infallible,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{DefaultBodyLimit, Path, Query, Request, State, rejection::JsonRejection},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::{Next, from_fn_with_state},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{delete, get, post, put},
};
use futures_util::stream;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{
    API_VERSION, AuthPairingStatus, AuthScope, McpConnectionTransport, NewNamedMcpConnection,
    PluginRegistry, ProviderAuth, ProviderProfile, ProviderProfileDiagnostic,
    ProviderProfileStatus, ProviderProfileUpsert, ProviderType, Store, WorkflowDocument,
    WorkflowSourceUpdate,
    config::ApiConfig,
    lifecycle::{CoreLifecycle, execution::TrackedExecutionState},
    management::{
        CursorOrder, DEFAULT_REQUEST_BYTES, MAX_METADATA_SECTION_BYTES, MAX_PAGE_SIZE,
        MAX_RESPONSE_BYTES, ManagementCursor, ManagementError, ManagementErrorCode, ManagementPage,
        SecretValue, WORKFLOW_IMPORT_ENVELOPE_BYTES, WORKFLOW_IMPORT_REQUEST_BYTES,
        WORKFLOW_IMPORT_SOURCE_BYTES,
    },
    mcp::{McpClient, McpClientOptions, McpHttpServerConfig, McpStdioServerConfig, McpToolContent},
    plugin_install::{self, PluginSource},
    reporting::{CoreDiagnosticSource, DiagnosticReporter},
};

struct AppState {
    store: Store,
    plugins: Mutex<PluginRegistry>,
    api_config: ApiConfig,
    lifecycle: CoreLifecycle,
    reporter: DiagnosticReporter,
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
    router_with_lifecycle(store, plugins, api_config, CoreLifecycle::ready())
}

/// Builds an API router whose readiness and execution admission follow the
/// supplied Core lifecycle state.
pub fn router_with_lifecycle(
    store: Store,
    plugins: PluginRegistry,
    api_config: ApiConfig,
    lifecycle: CoreLifecycle,
) -> Router {
    router_with_lifecycle_and_reporter(
        store,
        plugins,
        api_config,
        lifecycle,
        DiagnosticReporter::default(),
    )
}

/// Builds an API router whose runtime diagnostics are sent to an injected
/// reporter instead of being written directly by library code.
pub fn router_with_lifecycle_and_reporter(
    store: Store,
    plugins: PluginRegistry,
    api_config: ApiConfig,
    lifecycle: CoreLifecycle,
    reporter: DiagnosticReporter,
) -> Router {
    let body_limit = api_config.request_body_limit_bytes;
    let plugins = plugins.with_diagnostic_reporter(reporter.clone());
    let state = Arc::new(AppState {
        store,
        plugins: Mutex::new(plugins),
        api_config,
        lifecycle,
        reporter,
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
        .nest(
            "/api/v1/manage",
            Router::new()
                .route("/", get(management_index))
                .route("/workflows", get(management_list_workflows))
                .route("/workflows/resolve", post(management_resolve_workflow))
                .route("/workflows/import", post(management_import_workflow))
                .route("/workflows/{id}/source", get(management_workflow_source))
                .route(
                    "/workflows/{id}/validate",
                    post(management_validate_workflow),
                )
                .route("/workflows/{id}/plan", post(management_plan_workflow))
                .route(
                    "/workflows/{id}/eligibility",
                    put(management_update_workflow_eligibility),
                )
                .route(
                    "/workflows/{id}/executions",
                    post(management_submit_workflow),
                )
                .route("/executions", get(management_list_executions))
                .route("/executions/{id}", get(management_inspect_execution))
                .route("/executions/{id}/cancel", post(management_cancel_execution))
                .route("/executions/{id}/logs", get(management_execution_logs))
                .route("/triggers", get(management_list_triggers))
                .route(
                    "/triggers/{workflow_id}/{trigger_id}",
                    get(management_inspect_trigger),
                )
                .route(
                    "/providers",
                    get(management_list_providers).post(management_create_provider),
                )
                .route("/providers/{id}", get(management_get_provider))
                .route(
                    "/providers/{id}/auth/prepare",
                    post(management_prepare_provider_auth),
                )
                .route(
                    "/providers/{id}/auth/commit",
                    post(management_commit_provider_auth),
                )
                .route("/providers/{id}/test", post(management_test_provider))
                .route("/providers/{id}/models", post(management_provider_models))
                .route("/mcp", get(management_list_mcp).post(management_create_mcp))
                .route("/mcp/{name}", get(management_get_mcp))
                .route("/mcp/{name}/auth", post(management_mcp_auth))
                .route("/mcp/{name}/tools/list", post(management_mcp_list_tools))
                .route("/mcp/{name}/tools/call", post(management_mcp_call_tool))
                .route("/secrets", get(management_list_secrets))
                .route(
                    "/secrets/{name}",
                    put(management_set_secret).delete(management_remove_secret),
                )
                .fallback(management_not_found),
        )
        .layer(from_fn_with_state(state.clone(), enforce_request_size))
        .layer(from_fn_with_state(state.clone(), require_auth));
    Router::new()
        .route("/health/live", get(live_health))
        .route("/health/ready", get(ready_health))
        .route("/api/v1/auth/pair/claim", post(claim_auth_pairing))
        .route("/api/v1/auth/pair/status", post(auth_pairing_status))
        .route("/api/v1/auth/pair/exchange", post(exchange_auth_pairing))
        .merge(protected)
        .layer(DefaultBodyLimit::max(
            body_limit.max(WORKFLOW_IMPORT_REQUEST_BYTES),
        ))
        .layer(from_fn_with_state(state.clone(), enforce_local_policy))
        .with_state(state)
}

async fn live_health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn management_index(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let value = json!({
        "capability": "management-api",
        "apiVersion": "kakune-management/v1",
        "storageSchemaVersion": state.store.schema_version().map_err(ApiError::internal)?,
        "limits": {
            "defaultRequestBytes": DEFAULT_REQUEST_BYTES,
            "workflowImportRequestBytes": WORKFLOW_IMPORT_REQUEST_BYTES,
            "workflowImportSourceBytes": WORKFLOW_IMPORT_SOURCE_BYTES,
            "workflowImportEnvelopeBytes": WORKFLOW_IMPORT_ENVELOPE_BYTES,
            "responseBytes": MAX_RESPONSE_BYTES,
            "metadataSectionBytes": MAX_METADATA_SECTION_BYTES,
            "defaultPageSize": 100,
            "maximumPageSize": MAX_PAGE_SIZE
        }
    });
    if serde_json::to_vec(&value).map_or(true, |bytes| bytes.len() > MAX_METADATA_SECTION_BYTES) {
        return Err(ApiError::internal(
            "management metadata exceeds its response limit",
        ));
    }
    Ok(Json(value))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkflowImportRequest {
    source: String,
    #[serde(default)]
    replace: bool,
    #[serde(default)]
    expected_revision: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkflowSelectorRequest {
    selector: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkflowEligibilityRequest {
    enabled: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkflowExecutionRequest {
    #[serde(default)]
    request_key: Option<String>,
    #[serde(default)]
    input: Option<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WorkflowPageQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecutionPageQuery {
    workflow_id: Option<String>,
    status: Option<String>,
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecutionDetailQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExecutionCancelRequest {
    #[serde(default)]
    wait_seconds: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecutionEventPageQuery {
    limit: Option<usize>,
    cursor: Option<String>,
    high_water: Option<u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowExecutionSubmission {
    execution_id: String,
    status: String,
    workflow_revision: String,
}

async fn management_list_workflows(
    State(state): State<Arc<AppState>>,
    Query(query): Query<WorkflowPageQuery>,
) -> Response {
    let page = match crate::management::list_workflow_page(
        &state.store,
        query.limit,
        query.cursor.as_deref(),
    ) {
        Ok(page) => page,
        Err(error) => return management_error_response(error),
    };
    if serde_json::to_vec(&page).map_or(true, |bytes| bytes.len() > MAX_METADATA_SECTION_BYTES) {
        return management_error_response(ManagementError::new(
            ManagementErrorCode::Internal,
            "workflow metadata page exceeds its response limit",
        ));
    }
    Json(page).into_response()
}

async fn management_resolve_workflow(
    State(state): State<Arc<AppState>>,
    payload: Result<Json<WorkflowSelectorRequest>, JsonRejection>,
) -> Response {
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(_) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "workflow resolution requires a selector",
            ));
        }
    };
    match crate::management::resolve_workflow(&state.store, &payload.selector) {
        Ok(record) => Json(record).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn management_import_workflow(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    payload: Result<Json<WorkflowImportRequest>, JsonRejection>,
) -> Response {
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(_) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "workflow import request must be valid JSON with a source string",
            ));
        }
    };
    let expected_revision = match request_revision(&headers, payload.expected_revision.as_deref()) {
        Ok(revision) => revision,
        Err(error) => return management_error_response(error),
    };
    let existed = WorkflowDocument::parse(&payload.source)
        .ok()
        .and_then(|workflow| {
            state
                .store
                .get_workflow_source(&workflow.metadata.id)
                .ok()
                .flatten()
        })
        .is_some();
    match crate::management::import_workflow(
        &state.store,
        &payload.source,
        payload.replace,
        expected_revision.as_deref(),
        &plugin_snapshot(&state),
    ) {
        Ok(record) => (
            if existed {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            },
            Json(record),
        )
            .into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn management_workflow_source(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match crate::management::workflow_source(&state.store, &id) {
        Ok(source) => Json(json!({
            "id": id,
            "source": source.source,
            "revision": source.revision,
        }))
        .into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn management_validate_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let source = match crate::management::workflow_source(&state.store, &id) {
        Ok(source) => source,
        Err(error) => return management_error_response(error),
    };
    let analysis = crate::management::analyze_workflow(&source.source, &plugin_snapshot(&state));
    if !analysis.is_valid() {
        return management_error_response(
            ManagementError::new(
                ManagementErrorCode::InvalidOperation,
                "stored workflow validation failed",
            )
            .with_details(json!({ "diagnostics": analysis.diagnostics })),
        );
    }
    Json(analysis).into_response()
}

async fn management_plan_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let source = match crate::management::workflow_source(&state.store, &id) {
        Ok(source) => source,
        Err(error) => return management_error_response(error),
    };
    let plan = crate::management::plan_workflow(
        &source.source,
        Some(&source.revision),
        &plugin_snapshot(&state),
    );
    if !plan.diagnostics.is_empty() {
        return management_error_response(
            ManagementError::new(
                ManagementErrorCode::InvalidOperation,
                "stored workflow cannot be planned",
            )
            .with_details(json!({ "diagnostics": plan.diagnostics })),
        );
    }
    if serde_json::to_vec(&plan).map_or(true, |bytes| bytes.len() > MAX_METADATA_SECTION_BYTES) {
        return management_error_response(ManagementError::new(
            ManagementErrorCode::Internal,
            "workflow plan exceeds its response limit",
        ));
    }
    Json(plan).into_response()
}

async fn management_update_workflow_eligibility(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    headers: HeaderMap,
    payload: Result<Json<WorkflowEligibilityRequest>, JsonRejection>,
) -> Response {
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(_) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "eligibility update requires an enabled boolean",
            ));
        }
    };
    let revision = match request_revision(&headers, None) {
        Ok(Some(revision)) => revision,
        Ok(None) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::PreconditionFailed,
                "workflow eligibility update requires If-Match",
            ));
        }
        Err(error) => return management_error_response(error),
    };
    match crate::management::update_workflow_eligibility(
        &state.store,
        &id,
        payload.enabled,
        &revision,
    ) {
        Ok(record) => Json(record).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn management_submit_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    payload: Result<Json<WorkflowExecutionRequest>, JsonRejection>,
) -> Response {
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(_) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "workflow execution request must be valid JSON",
            ));
        }
    };
    let inputs = payload.input.unwrap_or_else(|| json!({}));
    if !inputs.is_object() {
        return management_error_response(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            "workflow execution input must be an object",
        ));
    }
    if payload.request_key.as_deref().is_some_and(|key| {
        key.is_empty() || key.len() > 128 || !key.bytes().all(|byte| byte.is_ascii_graphic())
    }) {
        return management_error_response(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            "requestKey must contain 1 to 128 visible ASCII characters",
        ));
    }
    let hash = crate::artifacts::artifact_id(
        json!({ "workflowId": &id, "input": &inputs })
            .to_string()
            .as_bytes(),
    );
    if let Some(key) = payload.request_key.as_deref() {
        match state.store.find_idempotent_execution(key, &hash) {
            Ok(Some(execution)) => {
                let revision = match state.store.execution_workflow_revision(&execution.id) {
                    Ok(Some(revision)) => revision,
                    _ => {
                        return management_error_response(ManagementError::new(
                            ManagementErrorCode::Internal,
                            "admitted workflow revision is unavailable",
                        ));
                    }
                };
                return (
                    StatusCode::OK,
                    Json(WorkflowExecutionSubmission {
                        execution_id: execution.id,
                        status: execution.status,
                        workflow_revision: revision,
                    }),
                )
                    .into_response();
            }
            Ok(None) => {}
            Err(_) => {
                return management_error_response(ManagementError::new(
                    ManagementErrorCode::Conflict,
                    "requestKey was already used for different workflow content",
                ));
            }
        }
    }
    let mut permit = match state
        .lifecycle
        .admission()
        .admit(TrackedExecutionState::Preparation)
    {
        Ok(permit) => permit,
        Err(_) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::Unsupported,
                "Core is stopping and no longer accepts workflow executions",
            ));
        }
    };
    let plugins = plugin_snapshot(&state);
    let admission = match crate::management::admit_workflow_execution(
        &state.store,
        &id,
        inputs,
        payload
            .request_key
            .as_deref()
            .map(|key| (key, hash.as_str())),
        &plugins,
    ) {
        Ok(admission) => admission,
        Err(error) => return management_error_response(error),
    };
    let execution = admission.execution;
    let created = admission.created;
    if created {
        if permit.transition(TrackedExecutionState::Queued).is_err() {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::Internal,
                "Core could not track admitted workflow execution",
            ));
        }
        let store = state.store.clone();
        let background_execution = execution.clone();
        let shutdown_requested = state.lifecycle.shutdown_signal();
        let reporter = state.reporter.clone();
        tokio::task::spawn_blocking(move || {
            if permit.transition(TrackedExecutionState::Active).is_err() {
                return;
            }
            if let Err(error) = crate::execute_prepared_workflow_with_plugins_and_shutdown(
                &store,
                background_execution.clone(),
                &plugins,
                shutdown_requested,
            ) && error != "Core shutdown requested"
            {
                reporter.report(
                    CoreDiagnosticSource::Api,
                    format!("execution {} failed: {error}", background_execution.id),
                );
            }
        });
    }
    (
        if created {
            StatusCode::ACCEPTED
        } else {
            StatusCode::OK
        },
        Json(WorkflowExecutionSubmission {
            execution_id: execution.id,
            status: execution.status,
            workflow_revision: admission.workflow_revision,
        }),
    )
        .into_response()
}

async fn management_list_executions(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ExecutionPageQuery>,
) -> Response {
    let page = match crate::management::list_execution_page(
        &state.store,
        query.workflow_id.as_deref(),
        query.status.as_deref(),
        query.limit,
        query.cursor.as_deref(),
    ) {
        Ok(page) => page,
        Err(error) => return management_error_response(error),
    };
    if serde_json::to_vec(&page).map_or(true, |bytes| bytes.len() > MAX_METADATA_SECTION_BYTES) {
        return management_error_response(ManagementError::new(
            ManagementErrorCode::PayloadTooLarge,
            "execution metadata page exceeds its 4 MiB section limit",
        ));
    }
    Json(page).into_response()
}

async fn management_inspect_execution(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<ExecutionDetailQuery>,
) -> Response {
    let detail = match crate::management::inspect_execution(
        &state.store,
        &id,
        query.limit,
        query.cursor.as_deref(),
    ) {
        Ok(detail) => detail,
        Err(error) => return management_error_response(error),
    };
    if serde_json::to_vec(&detail).map_or(true, |bytes| bytes.len() > MAX_METADATA_SECTION_BYTES) {
        return management_error_response(ManagementError::new(
            ManagementErrorCode::PayloadTooLarge,
            "execution detail exceeds its 4 MiB section limit",
        ));
    }
    Json(detail).into_response()
}

async fn management_cancel_execution(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    payload: Result<Json<ExecutionCancelRequest>, JsonRejection>,
) -> Response {
    let Json(payload) = match payload {
        Ok(payload) => payload,
        Err(_) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "cancellation request must be valid JSON",
            ));
        }
    };
    match crate::management::cancel_execution(&state.store, &id, payload.wait_seconds).await {
        Ok(outcome) => Json(outcome).into_response(),
        Err(error) => management_error_response(error),
    }
}

async fn management_execution_logs(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Query(query): Query<ExecutionEventPageQuery>,
) -> Response {
    let page = match crate::management::execution_event_page(
        &state.store,
        &id,
        query.limit,
        query.cursor.as_deref(),
        query.high_water,
    ) {
        Ok(page) => page,
        Err(error) => return management_error_response(error),
    };
    if serde_json::to_vec(&page).map_or(true, |bytes| bytes.len() > MAX_METADATA_SECTION_BYTES) {
        return management_error_response(ManagementError::new(
            ManagementErrorCode::PayloadTooLarge,
            "execution event page exceeds its 4 MiB section limit",
        ));
    }
    Json(page).into_response()
}

async fn management_list_triggers(
    State(state): State<Arc<AppState>>,
    Query(query): Query<WorkflowPageQuery>,
) -> Response {
    let page = match crate::management::list_trigger_page(
        &state.store,
        query.limit,
        query.cursor.as_deref(),
    ) {
        Ok(page) => page,
        Err(error) => return management_error_response(error),
    };
    if serde_json::to_vec(&page).map_or(true, |bytes| bytes.len() > MAX_METADATA_SECTION_BYTES) {
        return management_error_response(ManagementError::new(
            ManagementErrorCode::PayloadTooLarge,
            "trigger metadata page exceeds its 4 MiB section limit",
        ));
    }
    Json(page).into_response()
}

async fn management_inspect_trigger(
    State(state): State<Arc<AppState>>,
    Path((workflow_id, trigger_id)): Path<(String, String)>,
) -> Response {
    match crate::management::inspect_trigger(&state.store, &workflow_id, &trigger_id) {
        Ok(trigger) => Json(trigger).into_response(),
        Err(error) => management_error_response(error),
    }
}

fn request_revision(
    headers: &HeaderMap,
    body_revision: Option<&str>,
) -> Result<Option<String>, ManagementError> {
    let header_revision = headers
        .get(header::IF_MATCH)
        .map(|value| {
            value.to_str().map(normalize_revision).map_err(|_| {
                ManagementError::new(
                    ManagementErrorCode::InvalidRequest,
                    "If-Match must contain a valid workflow revision",
                )
            })
        })
        .transpose()?;
    let body_revision = body_revision.map(normalize_revision);
    if header_revision
        .as_ref()
        .zip(body_revision.as_ref())
        .is_some_and(|(header, body)| header != body)
    {
        return Err(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            "If-Match and expectedRevision must identify the same workflow revision",
        ));
    }
    Ok(header_revision.or(body_revision))
}

fn normalize_revision(value: &str) -> String {
    value.trim().trim_matches('"').to_owned()
}

async fn management_not_found() -> Response {
    management_error_response(ManagementError::new(
        ManagementErrorCode::NotFound,
        "management route was not found",
    ))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IntegrationPageQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProviderAuthCommitRequest {
    secret_value: Option<SecretValue>,
    oauth_bundle: Option<SecretValue>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct McpAuthRequest {
    secret_reference: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct McpToolListRequest {
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct McpToolCallRequest {
    name: String,
    arguments: serde_json::Map<String, serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetSecretValueRequest {
    value: SecretValue,
}

async fn management_list_providers(
    State(state): State<Arc<AppState>>,
    Query(query): Query<IntegrationPageQuery>,
) -> Response {
    let items = match state.store.list_provider_profiles() {
        Ok(items) => items,
        Err(_) => {
            return management_error_response(internal_management_error(
                "cannot list provider profiles",
            ));
        }
    };
    match paginate_integration_items(
        items,
        query.limit,
        query.cursor.as_deref(),
        "providers",
        |profile| format!("{}\0{}", profile.id, profile.updated_at),
        |profile| profile.id.clone(),
    ) {
        Ok(page) => bounded_management_json(
            StatusCode::OK,
            serde_json::to_value(page).unwrap_or(serde_json::Value::Null),
        ),
        Err(error) => management_error_response(error),
    }
}

async fn management_get_provider(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.store.get_provider_profile(&id) {
        Ok(Some(profile)) => bounded_management_json(
            StatusCode::OK,
            serde_json::to_value(profile).unwrap_or(serde_json::Value::Null),
        ),
        Ok(None) => management_error_response(ManagementError::new(
            ManagementErrorCode::NotFound,
            "provider profile was not found",
        )),
        Err(_) => {
            management_error_response(internal_management_error("cannot inspect provider profile"))
        }
    }
}

async fn management_create_provider(
    State(state): State<Arc<AppState>>,
    Json(profile): Json<ProviderProfileUpsert>,
) -> Response {
    match state.store.get_provider_profile(&profile.id) {
        Ok(Some(_)) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::Conflict,
                "provider profile already exists; add is create-only",
            ));
        }
        Err(_) => {
            return management_error_response(internal_management_error(
                "cannot inspect provider profile",
            ));
        }
        Ok(None) => {}
    }
    match state.store.create_provider_profile(profile) {
        Ok(profile) => bounded_management_json(
            StatusCode::CREATED,
            serde_json::to_value(profile).unwrap_or(serde_json::Value::Null),
        ),
        Err(error) if error.contains("UNIQUE constraint") => {
            management_error_response(ManagementError::new(
                ManagementErrorCode::Conflict,
                "provider profile already exists; add is create-only",
            ))
        }
        Err(_) => management_error_response(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            "provider profile is invalid",
        )),
    }
}

async fn management_prepare_provider_auth(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match crate::management::prepare_provider_auth(&state.store, &id) {
        Ok(preparation) => bounded_management_json(
            StatusCode::OK,
            serde_json::to_value(preparation).unwrap_or(serde_json::Value::Null),
        ),
        Err(error) => management_error_response(error),
    }
}

async fn management_commit_provider_auth(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(request): Json<ProviderAuthCommitRequest>,
) -> Response {
    let (value, oauth_bundle) = match (request.secret_value, request.oauth_bundle) {
        (Some(value), None) => (value, false),
        (None, Some(value)) => (value, true),
        _ => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "provide exactly one supported authentication payload",
            ));
        }
    };
    match crate::management::commit_provider_auth(&state.store, &id, value, oauth_bundle) {
        Ok(outcome) => bounded_management_json(
            StatusCode::OK,
            json!({
                "status": "committed",
                "secret": outcome.record,
                "cleanupWarning": outcome.cleanup_warning
            }),
        ),
        Err(error) => management_error_response(error),
    }
}

async fn management_test_provider(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match crate::management::test_provider(&state.store, &id) {
        Ok(report) => bounded_management_json(
            StatusCode::OK,
            serde_json::to_value(report).unwrap_or(serde_json::Value::Null),
        ),
        Err(error) => management_error_response(error),
    }
}

async fn management_provider_models(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match crate::management::provider_models(&state.store, &id) {
        Ok(models) => bounded_management_json(StatusCode::NOT_IMPLEMENTED, models),
        Err(error) => management_error_response(error),
    }
}

async fn management_list_mcp(
    State(state): State<Arc<AppState>>,
    Query(query): Query<IntegrationPageQuery>,
) -> Response {
    let items = match state.store.list_named_mcp_connections() {
        Ok(items) => items,
        Err(_) => {
            return management_error_response(internal_management_error(
                "cannot list MCP connections",
            ));
        }
    };
    match paginate_integration_items(
        items,
        query.limit,
        query.cursor.as_deref(),
        "mcp",
        |connection| format!("{}\0{}", connection.name, connection.revision),
        |connection| connection.name.clone(),
    ) {
        Ok(page) => bounded_management_json(
            StatusCode::OK,
            serde_json::to_value(page).unwrap_or(serde_json::Value::Null),
        ),
        Err(error) => management_error_response(error),
    }
}

async fn management_get_mcp(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    match state.store.get_named_mcp_connection(&name) {
        Ok(Some(connection)) => bounded_management_json(
            StatusCode::OK,
            serde_json::to_value(connection).unwrap_or(serde_json::Value::Null),
        ),
        Ok(None) => management_error_response(ManagementError::new(
            ManagementErrorCode::NotFound,
            "MCP connection was not found",
        )),
        Err(_) => {
            management_error_response(internal_management_error("cannot inspect MCP connection"))
        }
    }
}

async fn management_create_mcp(
    State(state): State<Arc<AppState>>,
    Json(connection): Json<NewNamedMcpConnection>,
) -> Response {
    match state.store.get_named_mcp_connection(&connection.name) {
        Ok(Some(_)) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::Conflict,
                "named MCP connection already exists; add is create-only",
            ));
        }
        Err(_) => {
            return management_error_response(internal_management_error(
                "cannot inspect MCP connection",
            ));
        }
        Ok(None) => {}
    }
    match state.store.create_named_mcp_connection(connection) {
        Ok(connection) => bounded_management_json(
            StatusCode::CREATED,
            serde_json::to_value(connection).unwrap_or(serde_json::Value::Null),
        ),
        Err(error) if error.contains("already exists") => {
            management_error_response(ManagementError::new(
                ManagementErrorCode::Conflict,
                "named MCP connection already exists; add is create-only",
            ))
        }
        Err(_) => management_error_response(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            "MCP connection configuration is invalid",
        )),
    }
}

async fn management_mcp_auth(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Json(request): Json<McpAuthRequest>,
) -> Response {
    let connection = match state.store.get_named_mcp_connection(&name) {
        Ok(Some(connection)) => connection,
        Ok(None) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::NotFound,
                "MCP connection was not found",
            ));
        }
        Err(_) => {
            return management_error_response(internal_management_error(
                "cannot inspect MCP connection",
            ));
        }
    };
    if let Some(reference) = request.secret_reference {
        if !matches!(&connection.transport, McpConnectionTransport::Http { .. }) {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::InvalidOperation,
                "--secret binding is supported only for HTTP MCP connections; stdio uses named environment references",
            ));
        }
        let Some(revision) = headers
            .get("if-match")
            .and_then(|value| value.to_str().ok())
        else {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::PreconditionFailed,
                "HTTP MCP reference binding requires If-Match",
            ));
        };
        let revision = normalize_revision(revision);
        match state
            .store
            .bind_named_mcp_bearer_reference(&name, &revision, &reference)
        {
            Ok(connection) => bounded_management_json(
                StatusCode::OK,
                json!({
                    "status": "committed",
                    "connection": connection,
                    "cleanupWarning": null
                }),
            ),
            Err(error) if error.contains("revision is stale") => {
                management_error_response(ManagementError::new(
                    ManagementErrorCode::PreconditionFailed,
                    "MCP connection revision is stale; no binding was changed",
                ))
            }
            Err(error) if error.contains("was not found") => {
                management_error_response(ManagementError::new(
                    ManagementErrorCode::NotFound,
                    "MCP connection or credential reference was not found",
                ))
            }
            Err(_) => management_error_response(ManagementError::new(
                ManagementErrorCode::InvalidOperation,
                "MCP credential reference could not be bound; prior configuration was preserved",
            )),
        }
    } else {
        match crate::management::check_named_mcp_auth(&state.store, &connection) {
            Ok(result) => bounded_management_json(StatusCode::OK, result),
            Err(error) => management_error_response(error),
        }
    }
}

async fn management_mcp_list_tools(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(request): Json<McpToolListRequest>,
) -> Response {
    let connection = match state.store.get_named_mcp_connection(&name) {
        Ok(Some(connection)) => connection,
        Ok(None) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::NotFound,
                "MCP connection was not found",
            ));
        }
        Err(_) => {
            return management_error_response(internal_management_error(
                "cannot inspect MCP connection",
            ));
        }
    };
    let limit = match validate_page_limit(request.limit) {
        Ok(limit) => limit,
        Err(error) => return management_error_response(error),
    };
    let decoded_cursor = match request
        .cursor
        .as_deref()
        .map(ManagementCursor::decode)
        .transpose()
    {
        Ok(cursor) => cursor,
        Err(_) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "MCP discovery cursor is invalid",
            ));
        }
    };
    if let Some(cursor) = &decoded_cursor
        && (cursor.order != CursorOrder::MetadataIdentityAscending
            || cursor.filters.get("collection").map(String::as_str) != Some("mcp-tools")
            || cursor.filters.get("revision").map(String::as_str)
                != Some(connection.revision.as_str())
            || cursor.position.len() != 2)
    {
        return management_error_response(ManagementError::new(
            ManagementErrorCode::Conflict,
            "MCP connection changed; restart tool discovery",
        ));
    }
    let operation = tokio::time::timeout(Duration::from_secs(30), async {
        let mut resolved = crate::management::connect_named_mcp(&state.store, &connection).await?;
        let server = resolved.client.server_info().clone();
        let tools = resolved.client.list_tools().await;
        if let Err(error) = resolved.client.cancel().await {
            return Err(crate::management::map_mcp_operation_error(error));
        }
        let tools = tools.map_err(crate::management::map_mcp_operation_error)?;
        Ok::<_, ManagementError>((server, tools, resolved.secret_values))
    })
    .await;
    let (server, tools, secrets) = match operation {
        Ok(Ok(operation)) => operation,
        Ok(Err(error)) => return management_error_response(error),
        Err(_) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::DeadlineExceeded,
                "MCP discovery exceeded its aggregate 30-second deadline",
            ));
        }
    };
    let mut items = tools
        .into_iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "description": tool.description,
                "inputSchema": tool.input_schema.into_value(),
                "outputSchema": tool.output_schema.map(|schema| schema.into_value())
            })
        })
        .collect::<Vec<_>>();
    items.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
    let serialized = match serde_json::to_vec(&items) {
        Ok(value) => value,
        Err(_) => {
            return management_error_response(internal_management_error(
                "cannot encode discovered MCP schemas",
            ));
        }
    };
    let snapshot = format!("{:x}", Sha256::digest(&serialized));
    let offset = if let Some(cursor) = decoded_cursor {
        let cursor_snapshot = cursor.position[0].as_str();
        if cursor_snapshot != snapshot {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::Conflict,
                "MCP tool discovery changed; restart pagination",
            ));
        }
        cursor.position[1]
            .parse::<usize>()
            .ok()
            .filter(|offset| *offset <= items.len())
            .unwrap_or(usize::MAX)
    } else {
        0
    };
    if offset == usize::MAX {
        return management_error_response(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            "MCP discovery cursor position is invalid",
        ));
    }
    let end = offset.saturating_add(limit).min(items.len());
    let mut page = items[offset..end].to_vec();
    let mut sensitive = secrets
        .iter()
        .map(|secret| secret.expose().to_owned())
        .collect::<Vec<_>>();
    for item in &mut page {
        redact_json_strings(item, &sensitive);
    }
    let next_cursor = if end < items.len() {
        let mut filters = BTreeMap::new();
        filters.insert("collection".to_owned(), "mcp-tools".to_owned());
        filters.insert("revision".to_owned(), connection.revision.clone());
        Some(
            ManagementCursor {
                filters,
                order: CursorOrder::MetadataIdentityAscending,
                high_water: None,
                position: vec![snapshot.clone(), end.to_string()],
            }
            .encode()
            .unwrap_or_default(),
        )
    } else {
        None
    };
    for value in &mut sensitive {
        zeroize::Zeroize::zeroize(value);
    }
    bounded_management_json(
        StatusCode::OK,
        json!({
            "server": {"protocolVersion": server.protocol_version, "name": server.name, "version": server.version},
            "items": page,
            "nextCursor": next_cursor,
            "snapshot": snapshot
        }),
    )
}

async fn management_mcp_call_tool(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(request): Json<McpToolCallRequest>,
) -> Response {
    let connection = match state.store.get_named_mcp_connection(&name) {
        Ok(Some(connection)) => connection,
        Ok(None) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::NotFound,
                "MCP connection was not found",
            ));
        }
        Err(_) => {
            return management_error_response(internal_management_error(
                "cannot inspect MCP connection",
            ));
        }
    };
    let mut sensitive = Vec::new();
    collect_sensitive_strings(
        &serde_json::Value::Object(request.arguments.clone()),
        &mut sensitive,
    );
    let operation = tokio::time::timeout(Duration::from_secs(60), async {
        let mut resolved = crate::management::connect_named_mcp(&state.store, &connection).await?;
        sensitive.extend(
            resolved
                .secret_values
                .iter()
                .map(|secret| secret.expose().to_owned()),
        );
        let result = resolved
            .client
            .call_validated_tool(&request.name, request.arguments)
            .await;
        if let Err(error) = resolved.client.cancel().await {
            return Err(crate::management::map_mcp_operation_error(error));
        }
        result.map_err(crate::management::map_mcp_operation_error)
    })
    .await;
    let result = match operation {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => return management_error_response(error),
        Err(_) => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::DeadlineExceeded,
                "MCP tool operation exceeded its aggregate 60-second deadline",
            ));
        }
    };
    let content = result
        .content
        .into_iter()
        .map(mcp_content_value)
        .collect::<Vec<_>>();
    let mut response = json!({"content":content,"structuredContent":result.structured_content,"isError":result.is_error});
    redact_json_strings(&mut response, &sensitive);
    for value in &mut sensitive {
        zeroize::Zeroize::zeroize(value);
    }
    bounded_management_json(StatusCode::OK, response)
}

fn mcp_content_value(content: McpToolContent) -> serde_json::Value {
    match content {
        McpToolContent::Text { text } => json!({"type":"text","text":text}),
        McpToolContent::Image { data, mime_type } => {
            json!({"type":"image","data":data,"mimeType":mime_type})
        }
        McpToolContent::Audio { data, mime_type } => {
            json!({"type":"audio","data":data,"mimeType":mime_type})
        }
        McpToolContent::ResourceLink {
            uri,
            name,
            description,
            mime_type,
            size,
        } => {
            json!({"type":"resourceLink","uri":uri,"name":name,"description":description,"mimeType":mime_type,"size":size})
        }
        McpToolContent::EmbeddedTextResource {
            uri,
            mime_type,
            text,
        } => json!({"type":"resource","resource":{"uri":uri,"mimeType":mime_type,"text":text}}),
        McpToolContent::EmbeddedBlobResource {
            uri,
            mime_type,
            blob,
        } => json!({"type":"resource","resource":{"uri":uri,"mimeType":mime_type,"blob":blob}}),
    }
}

fn collect_sensitive_strings(value: &serde_json::Value, output: &mut Vec<String>) {
    match value {
        serde_json::Value::String(value) if !value.is_empty() => output.push(value.clone()),
        serde_json::Value::Array(values) => values
            .iter()
            .for_each(|value| collect_sensitive_strings(value, output)),
        serde_json::Value::Object(values) => values
            .values()
            .for_each(|value| collect_sensitive_strings(value, output)),
        _ => {}
    }
}

fn redact_json_strings(value: &mut serde_json::Value, sensitive: &[String]) {
    match value {
        serde_json::Value::String(text) => {
            for secret in sensitive {
                if !secret.is_empty() && text.contains(secret) {
                    *text = text.replace(secret, "[redacted]");
                }
            }
        }
        serde_json::Value::Array(values) => values
            .iter_mut()
            .for_each(|value| redact_json_strings(value, sensitive)),
        serde_json::Value::Object(values) => values
            .values_mut()
            .for_each(|value| redact_json_strings(value, sensitive)),
        _ => {}
    }
}

async fn management_list_secrets(
    State(state): State<Arc<AppState>>,
    Query(query): Query<IntegrationPageQuery>,
) -> Response {
    let items = match state.store.list_secrets() {
        Ok(items) => items,
        Err(_) => {
            return management_error_response(internal_management_error(
                "cannot list secret metadata",
            ));
        }
    };
    match paginate_integration_items(
        items,
        query.limit,
        query.cursor.as_deref(),
        "secrets",
        |secret| format!("{}\0{}", secret.name, secret.updated_at),
        |secret| secret.name.clone(),
    ) {
        Ok(page) => bounded_management_json(
            StatusCode::OK,
            serde_json::to_value(page).unwrap_or(serde_json::Value::Null),
        ),
        Err(error) => management_error_response(error),
    }
}

async fn management_set_secret(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(request): Json<SetSecretValueRequest>,
) -> Response {
    let existed = state
        .store
        .list_secrets()
        .ok()
        .is_some_and(|items| items.iter().any(|item| item.name == name));
    match state
        .store
        .set_secret_versioned(&name, request.value.expose())
    {
        Ok(outcome) => bounded_management_json(
            if existed {
                StatusCode::OK
            } else {
                StatusCode::CREATED
            },
            json!({"status":"committed","secret":outcome.record,"cleanupWarning":outcome.cleanup_warning}),
        ),
        Err(_) => management_error_response(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            "secret could not be committed; prior access was preserved",
        )),
    }
}

async fn management_remove_secret(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    match state.store.remove_secret_versioned(&name) {
        Ok(outcome) if outcome.removed => bounded_management_json(
            StatusCode::OK,
            serde_json::to_value(outcome).unwrap_or(serde_json::Value::Null),
        ),
        Ok(_) => management_error_response(ManagementError::new(
            ManagementErrorCode::NotFound,
            "secret was not found",
        )),
        Err(_) => management_error_response(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            "secret could not be removed",
        )),
    }
}

fn paginate_integration_items<T>(
    items: Vec<T>,
    requested_limit: Option<usize>,
    cursor_value: Option<&str>,
    collection: &str,
    snapshot_identity: impl Fn(&T) -> String,
    identity: impl Fn(&T) -> String,
) -> Result<ManagementPage<T>, ManagementError> {
    let limit = validate_page_limit(requested_limit)?;
    let identities = items.iter().map(&snapshot_identity).collect::<Vec<_>>();
    let encoded = serde_json::to_vec(&identities)
        .map_err(|_| internal_management_error("cannot encode metadata page snapshot"))?;
    let snapshot = format!("{:x}", Sha256::digest(encoded));
    let mut filters = BTreeMap::new();
    filters.insert("collection".to_owned(), collection.to_owned());
    filters.insert("snapshot".to_owned(), snapshot.clone());
    let start = if let Some(cursor_value) = cursor_value {
        let cursor = ManagementCursor::decode(cursor_value).map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "metadata cursor is invalid",
            )
        })?;
        cursor
            .validate_binding(&filters, CursorOrder::MetadataIdentityAscending, None)
            .map_err(|_| {
                ManagementError::new(
                    ManagementErrorCode::Conflict,
                    "metadata changed; restart pagination",
                )
            })?;
        let after = cursor.position.first().ok_or_else(|| {
            ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "metadata cursor has no position",
            )
        })?;
        items
            .iter()
            .position(|item| identity(item) == *after)
            .map(|index| index + 1)
            .ok_or_else(|| {
                ManagementError::new(
                    ManagementErrorCode::Conflict,
                    "metadata changed; restart pagination",
                )
            })?
    } else {
        0
    };
    let end = start.saturating_add(limit).min(items.len());
    let page = items
        .into_iter()
        .skip(start)
        .take(end - start)
        .collect::<Vec<_>>();
    let next_cursor = if end < identities.len() {
        let last = page
            .last()
            .map(&identity)
            .ok_or_else(|| internal_management_error("cannot advance metadata cursor"))?;
        Some(
            ManagementCursor {
                filters,
                order: CursorOrder::MetadataIdentityAscending,
                high_water: None,
                position: vec![last],
            }
            .encode()
            .map_err(|_| internal_management_error("cannot encode metadata cursor"))?,
        )
    } else {
        None
    };
    Ok(ManagementPage {
        items: page,
        next_cursor,
        snapshot: Some(snapshot),
    })
}

fn validate_page_limit(limit: Option<usize>) -> Result<usize, ManagementError> {
    let limit = limit.unwrap_or(100);
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            format!("page limit must be between 1 and {MAX_PAGE_SIZE}"),
        ));
    }
    Ok(limit)
}

fn bounded_management_json(status: StatusCode, value: serde_json::Value) -> Response {
    if serde_json::to_vec(&value).map_or(true, |bytes| bytes.len() > MAX_METADATA_SECTION_BYTES) {
        return management_error_response(ManagementError::new(
            ManagementErrorCode::PayloadTooLarge,
            "management response section exceeds the 4 MiB limit",
        ));
    }
    (status, Json(value)).into_response()
}

fn internal_management_error(message: &str) -> ManagementError {
    ManagementError::new(ManagementErrorCode::Internal, message)
}

fn management_error_response(error: ManagementError) -> Response {
    let status =
        StatusCode::from_u16(error.code.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    (status, Json(error)).into_response()
}

async fn enforce_request_size(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    let is_management = path == "/api/v1/manage" || path.starts_with("/api/v1/manage/");
    let is_import = path == "/api/v1/manage/workflows/import";
    let limit = if is_import {
        WORKFLOW_IMPORT_REQUEST_BYTES
    } else if is_management {
        DEFAULT_REQUEST_BYTES
    } else {
        state.api_config.request_body_limit_bytes
    };
    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, limit).await {
        Ok(bytes) => bytes,
        Err(_) if is_management => {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::PayloadTooLarge,
                "encoded management request exceeds its route limit",
            ));
        }
        Err(_) => {
            return ApiError::payload_too_large("request body exceeds configured limit")
                .into_response();
        }
    };
    if is_import
        && let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes)
        && let Some(object) = value.as_object()
        && let Some(source) = object.get("source").and_then(serde_json::Value::as_str)
    {
        if source.len() > WORKFLOW_IMPORT_SOURCE_BYTES {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::PayloadTooLarge,
                "decoded workflow source exceeds 1 MiB",
            ));
        }
        let mut envelope = value;
        if let Some(object) = envelope.as_object_mut() {
            object.remove("source");
        }
        if serde_json::to_vec(&envelope).map_or(true, |encoded| {
            encoded.len() > WORKFLOW_IMPORT_ENVELOPE_BYTES
        }) {
            return management_error_response(ManagementError::new(
                ManagementErrorCode::PayloadTooLarge,
                "encoded workflow import envelope exceeds 64 KiB",
            ));
        }
    }
    next.run(Request::from_parts(parts, Body::from(bytes)))
        .await
}

async fn ready_health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if state.lifecycle.is_ready() {
        (StatusCode::OK, Json(json!({ "status": "ready" })))
    } else {
        let phase = match state.lifecycle.phase() {
            crate::lifecycle::LifecyclePhase::Starting => "starting",
            crate::lifecycle::LifecyclePhase::Ready => "ready",
            crate::lifecycle::LifecyclePhase::Stopping => "stopping",
            crate::lifecycle::LifecyclePhase::Failed => "failed",
        };
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "not_ready", "phase": phase })),
        )
    }
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
    let path = request.uri().path();
    let is_management = path == "/api/v1/manage" || path.starts_with("/api/v1/manage/");
    let scope = if is_management {
        required_management_scope(request.method().as_str(), path)
    } else {
        required_scope(request.method().as_str(), path)
    };
    match token.map(|token| state.store.authorize_scope(token, scope)) {
        Some(Ok(true)) => next.run(request).await,
        Some(Ok(false)) if is_management => management_error_response(ManagementError::new(
            ManagementErrorCode::Forbidden,
            "the credential does not include the required management scope",
        )),
        None if is_management => management_error_response(ManagementError::new(
            ManagementErrorCode::Unauthenticated,
            "a valid Core credential is required",
        )),
        Some(Ok(false)) | None => {
            ApiError::unauthorized("a valid Bearer token is required").into_response()
        }
        Some(Err(_)) if is_management => management_error_response(ManagementError::new(
            ManagementErrorCode::Internal,
            "Core could not verify the management credential",
        )),
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

fn required_management_scope(method: &str, path: &str) -> AuthScope {
    let route = path.strip_prefix("/api/v1/manage").unwrap_or(path);
    if route == "/secrets" || route.starts_with("/secrets/") {
        return AuthScope::Manage;
    }
    if method == "POST"
        && (route == "/workflows/resolve"
            || route.ends_with("/validate")
            || route.ends_with("/plan"))
    {
        return AuthScope::Read;
    }
    if method == "POST"
        && (route == "/workflows/import"
            || route.ends_with("/eligibility")
            || route.starts_with("/providers")
            || route.starts_with("/mcp"))
    {
        return AuthScope::Manage;
    }
    if method == "POST"
        && (route.starts_with("/workflows/") && route.ends_with("/executions")
            || route.starts_with("/executions/") && route.ends_with("/cancel"))
    {
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
            "management-api".to_string(),
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
        .create_workflow(&workflow, &payload.source, "enabled")
        .map_err(ApiError::internal)?
        .ok_or_else(|| {
            ApiError::conflict(format!("workflow {} already exists", workflow.metadata.id))
        })?;
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
    let mut permit = state
        .lifecycle
        .admission()
        .admit(TrackedExecutionState::Preparation)
        .map_err(|_| ApiError::unavailable("Core is stopping and no longer accepts executions"))?;
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
    permit
        .transition(TrackedExecutionState::Queued)
        .map_err(|error| ApiError::internal(error.to_string()))?;
    let store = state.store.clone();
    let background_execution = execution.clone();
    let shutdown_requested = state.lifecycle.shutdown_signal();
    let reporter = state.reporter.clone();
    tokio::task::spawn_blocking(move || {
        if permit.transition(TrackedExecutionState::Active).is_err() {
            return;
        }
        if let Err(error) = crate::runtime::execute_prepared_workflow_with_plugins_and_shutdown(
            &store,
            background_execution.clone(),
            &plugins,
            shutdown_requested,
        ) && error != "Core shutdown requested"
        {
            reporter.report(
                CoreDiagnosticSource::Api,
                format!("execution {} failed: {error}", background_execution.id),
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

    fn payload_too_large(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            code: "payload_too_large",
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

    fn unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "core_not_accepting_work",
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
            "auth":{"type":"oauthSecret","secretRef":"provider-key"},
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
