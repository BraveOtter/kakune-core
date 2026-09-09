use std::{convert::Infallible, sync::Arc};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{
        IntoResponse, Response,
        sse::{Event, Sse},
    },
    routing::{get, post},
};
use futures_util::stream;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{API_VERSION, Store, WorkflowDocument, run_workflow};

#[derive(Clone)]
struct AppState {
    store: Store,
}

pub fn router(store: Store) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/info", get(info))
        .route("/api/v1/events", get(events))
        .route("/api/v1/plugins/{name}/manifest", get(plugin_manifest))
        .route(
            "/api/v1/workflows",
            get(list_workflows).post(create_workflow),
        )
        .route("/api/v1/workflows/analyze", post(analyze_workflow))
        .route(
            "/api/v1/workflows/{name}",
            get(get_workflow).put(update_workflow),
        )
        .route("/api/v1/workflows/{name}/run", post(run_saved_workflow))
        .route("/api/v1/executions", get(list_executions))
        .route("/api/v1/executions/{id}", get(get_execution))
        .with_state(Arc::new(AppState { store }))
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn info() -> Json<CoreInfo> {
    Json(CoreInfo {
        api_version: API_VERSION,
        core_version: env!("CARGO_PKG_VERSION"),
        server_time: timestamp().unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string()),
        capabilities: vec![
            "workflows".to_string(),
            "executions".to_string(),
            "events".to_string(),
            "native-core-plugin".to_string(),
        ],
    })
}

async fn events() -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let event = Event::default()
        .event("core.ready")
        .data(json!({ "apiVersion": API_VERSION }).to_string());
    Sse::new(stream::once(async move { Ok(event) }))
}

async fn plugin_manifest(Path(name): Path<String>) -> Result<Json<serde_json::Value>, ApiError> {
    if name != "@kakune/core" {
        return Err(ApiError::not_found(format!(
            "plugin {name} is not installed"
        )));
    }
    Ok(Json(json!({
        "apiVersion": "kakune.dev/v1",
        "kind": "Plugin",
        "metadata": {
            "name": "@kakune/core",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Built-in local filesystem and logging steps"
        },
        "entrypoint": { "module": "builtin:@kakune/core" },
        "capabilities": ["log", "read-file", "write-file"]
    })))
}

async fn list_workflows(
    State(state): State<Arc<AppState>>,
) -> Result<Json<ListResponse<crate::WorkflowRecord>>, ApiError> {
    Ok(Json(ListResponse {
        items: state.store.list_workflows().map_err(ApiError::internal)?,
    }))
}

async fn get_workflow(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<WorkflowSourceResponse>, ApiError> {
    let source = state
        .store
        .get_workflow_source(&name)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("workflow {name} was not found")))?;
    Ok(Json(WorkflowSourceResponse { name, source }))
}

async fn analyze_workflow(
    Json(payload): Json<WorkflowSource>,
) -> Result<Json<AnalyzeResponse>, ApiError> {
    let workflow = WorkflowDocument::parse(&payload.source).map_err(ApiError::invalid)?;
    Ok(Json(AnalyzeResponse {
        valid: true,
        name: workflow.metadata.name,
        step_count: workflow.spec.steps.len(),
    }))
}

async fn create_workflow(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<WorkflowSource>,
) -> Result<(StatusCode, Json<crate::WorkflowRecord>), ApiError> {
    let workflow = WorkflowDocument::parse(&payload.source).map_err(ApiError::invalid)?;
    let record = state
        .store
        .upsert_workflow(&workflow, &payload.source, "enabled")
        .map_err(ApiError::internal)?;
    Ok((StatusCode::CREATED, Json(record)))
}

async fn update_workflow(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(payload): Json<WorkflowSource>,
) -> Result<Json<crate::WorkflowRecord>, ApiError> {
    let workflow = WorkflowDocument::parse(&payload.source).map_err(ApiError::invalid)?;
    if workflow.metadata.name != name {
        return Err(ApiError::invalid(
            "metadata.name must match the workflow URL",
        ));
    }
    let record = state
        .store
        .upsert_workflow(&workflow, &payload.source, "enabled")
        .map_err(ApiError::internal)?;
    Ok(Json(record))
}

async fn run_saved_workflow(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<crate::ExecutionRecord>, ApiError> {
    let source = state
        .store
        .get_workflow_source(&name)
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::not_found(format!("workflow {name} was not found")))?;
    let workflow = WorkflowDocument::parse(&source).map_err(ApiError::internal)?;
    let execution = run_workflow(&state.store, &workflow).map_err(ApiError::internal)?;
    Ok(Json(execution))
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
    let steps = state
        .store
        .list_step_runs(&id)
        .map_err(ApiError::internal)?;
    Ok(Json(ExecutionDetail { execution, steps }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CoreInfo {
    api_version: &'static str,
    core_version: &'static str,
    server_time: String,
    capabilities: Vec<String>,
}

#[derive(Deserialize)]
struct WorkflowSource {
    source: String,
}

#[derive(Serialize)]
struct WorkflowSourceResponse {
    name: String,
    source: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AnalyzeResponse {
    valid: bool,
    name: String,
    step_count: usize,
}

#[derive(Serialize)]
struct ListResponse<T> {
    items: Vec<T>,
}

#[derive(Serialize)]
struct ExecutionDetail {
    execution: crate::ExecutionRecord,
    steps: Vec<crate::StepRunRecord>,
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
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

    use crate::Store;

    use super::router;

    #[tokio::test]
    async fn reports_the_public_core_contract() {
        let directory =
            std::env::temp_dir().join(format!("kakune-api-test-{}", uuid::Uuid::new_v4()));
        let app = router(Store::open(directory.clone()).expect("store should open"));
        let response = app
            .oneshot(
                Request::get("/api/v1/info")
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
        std::fs::remove_dir_all(directory).expect("temporary data should be removed");
    }
}
