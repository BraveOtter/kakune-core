//! Shared, console-free types and limits for the additive management API.

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use zeroize::Zeroize;

use crate::{
    ExecutionRecord, McpConnectionTransport, NamedMcpConnection, PluginRegistry,
    ProviderProfileDiagnostic, ProviderProfileStatus, ProviderType, SecretSetOutcome, Store,
    WorkflowAnalysis, WorkflowRecord, WorkflowSourceUpdate, WorkflowStatusUpdate,
    mcp::{McpClient, McpClientOptions, McpHttpServerConfig, McpStdioServerConfig},
    planner::{self, PlanView},
    scheduler::RegisteredTriggerProjection,
    store::{
        EventRecord, ExecutionCancellationOutcome, ExecutionNodeSummary, WorkflowSourceRecord,
    },
};

pub struct WorkflowExecutionAdmission {
    pub execution: ExecutionRecord,
    pub created: bool,
    pub workflow_revision: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionSummaryView {
    pub id: String,
    pub workflow_id: String,
    pub workflow_name: String,
    pub status: String,
    pub created_at: String,
    pub completed_at: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionSection<T> {
    pub availability: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guidance: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionNodesSection {
    pub availability: String,
    pub items: Vec<ExecutionNodeView>,
    pub next_cursor: Option<String>,
    pub guidance: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionNodeView {
    pub node_id: String,
    pub status: String,
    pub details_availability: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionTraceSection {
    pub availability: String,
    pub span_count: u64,
    pub guidance: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionDetailView {
    pub id: String,
    pub workflow_id: String,
    pub workflow_name: String,
    pub workflow_revision: Option<String>,
    pub status: String,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub pinned: bool,
    pub error: ExecutionSection<()>,
    pub result: ExecutionSection<serde_json::Value>,
    pub nodes: ExecutionNodesSection,
    pub trace: ExecutionTraceSection,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CancellationView {
    pub outcome: String,
    pub observed_status: Option<String>,
    pub pending: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionEventPage {
    pub execution_id: String,
    pub high_water: u64,
    pub items: Vec<EventRecord>,
    pub next_cursor: Option<String>,
    pub retention_availability: String,
    pub truncated: bool,
}

pub const DEFAULT_REQUEST_BYTES: usize = 1_048_576;
pub const WORKFLOW_IMPORT_REQUEST_BYTES: usize = 8_388_608;
pub const WORKFLOW_IMPORT_SOURCE_BYTES: usize = 1_048_576;
pub const WORKFLOW_IMPORT_ENVELOPE_BYTES: usize = 65_536;
pub const MAX_RESPONSE_BYTES: usize = 16_777_216;
pub const MAX_METADATA_SECTION_BYTES: usize = 4_194_304;
pub const DEFAULT_PAGE_SIZE: usize = 100;
pub const DEFAULT_EVENT_PAGE_SIZE: usize = 200;
pub const MAX_PAGE_SIZE: usize = 1_000;
pub const CONNECT_TIMEOUT_SECONDS: u64 = 10;
pub const REQUEST_TIMEOUT_SECONDS: u64 = 120;

pub fn analyze_workflow(source: &str, plugins: &PluginRegistry) -> WorkflowAnalysis {
    crate::analyze_workflow_with_plugins(source, plugins)
}

pub fn plan_workflow(source: &str, revision: Option<&str>, plugins: &PluginRegistry) -> PlanView {
    planner::plan_view(source, revision, plugins)
}

pub fn resolve_workflow(store: &Store, selector: &str) -> Result<WorkflowRecord, ManagementError> {
    let selector = selector.trim();
    let (by_id, by_name, value) = if let Some(value) = selector.strip_prefix("id:") {
        (true, false, value)
    } else if let Some(value) = selector.strip_prefix("name:") {
        (false, true, value)
    } else {
        (true, true, selector)
    };
    if value.is_empty() {
        return Err(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            "workflow selector must not be empty",
        ));
    }
    let records = store.list_workflows().map_err(|_| {
        ManagementError::new(ManagementErrorCode::Internal, "cannot list workflows")
    })?;
    let matches = records
        .into_iter()
        .filter(|record| (by_id && record.id == value) || (by_name && record.name == value))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Err(ManagementError::new(
            ManagementErrorCode::NotFound,
            format!("workflow {selector} was not found"),
        )),
        [record] => Ok(record.clone()),
        _ => Err(ManagementError::new(
            ManagementErrorCode::Conflict,
            format!("workflow selector {selector} is ambiguous; use id: or name:"),
        )),
    }
}

pub fn import_workflow(
    store: &Store,
    source: &str,
    replace: bool,
    expected_revision: Option<&str>,
    plugins: &PluginRegistry,
) -> Result<WorkflowRecord, ManagementError> {
    if source.len() > WORKFLOW_IMPORT_SOURCE_BYTES {
        return Err(ManagementError::new(
            ManagementErrorCode::PayloadTooLarge,
            "workflow source exceeds the 1 MiB UTF-8 limit",
        ));
    }
    let analysis = analyze_workflow(source, plugins);
    if !analysis.is_valid() {
        return Err(ManagementError::new(
            ManagementErrorCode::InvalidOperation,
            format!(
                "workflow validation failed: {}",
                diagnostics_message(&analysis)
            ),
        ));
    }
    let workflow = analysis
        .workflow
        .expect("a diagnostic-free analysis contains the parsed workflow");
    let existing = store.list_workflows().map_err(|_| {
        ManagementError::new(ManagementErrorCode::Internal, "cannot inspect workflows")
    })?;
    let same_id = existing
        .iter()
        .find(|record| record.id == workflow.metadata.id);
    let same_name = existing
        .iter()
        .find(|record| record.name == workflow.metadata.name);

    if let Some(record) = same_id {
        if !replace {
            return Err(ManagementError::new(
                ManagementErrorCode::Conflict,
                format!(
                    "workflow {} already exists; explicit replacement is required",
                    record.id
                ),
            ));
        }
        let expected = expected_revision.ok_or_else(|| {
            ManagementError::new(
                ManagementErrorCode::PreconditionFailed,
                "workflow replacement requires the observed revision",
            )
        })?;
        if same_name.is_some_and(|name_record| name_record.id != record.id) {
            return Err(ManagementError::new(
                ManagementErrorCode::Conflict,
                format!(
                    "workflow name {} belongs to a different workflow",
                    workflow.metadata.name
                ),
            ));
        }
        return match store
            .update_workflow_if_revision(&workflow, source, expected)
            .map_err(|_| {
                ManagementError::new(ManagementErrorCode::Internal, "cannot replace workflow")
            })? {
            WorkflowSourceUpdate::Updated(record) => Ok(record),
            WorkflowSourceUpdate::NotFound => Err(ManagementError::new(
                ManagementErrorCode::NotFound,
                format!("workflow {} was not found", workflow.metadata.id),
            )),
            WorkflowSourceUpdate::Conflict { current_revision } => Err(ManagementError::new(
                ManagementErrorCode::PreconditionFailed,
                format!("workflow revision changed; current revision is {current_revision}"),
            )),
        };
    }
    if same_name.is_some() || expected_revision.is_some() {
        return Err(ManagementError::new(
            ManagementErrorCode::Conflict,
            format!(
                "workflow name {} is already registered or its revision is unknown",
                workflow.metadata.name
            ),
        ));
    }
    store
        .create_workflow(&workflow, source, "disabled")
        .map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::Conflict,
                "workflow identity or name already exists",
            )
        })?
        .ok_or_else(|| {
            ManagementError::new(
                ManagementErrorCode::Conflict,
                "workflow identity already exists",
            )
        })
}

pub fn update_workflow_eligibility(
    store: &Store,
    id: &str,
    enabled: bool,
    expected_revision: &str,
) -> Result<WorkflowRecord, ManagementError> {
    match store
        .set_workflow_status_if_revision(
            id,
            if enabled { "enabled" } else { "disabled" },
            expected_revision,
        )
        .map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::Internal,
                "cannot update workflow eligibility",
            )
        })? {
        WorkflowStatusUpdate::Updated(record) => Ok(record),
        WorkflowStatusUpdate::NotFound => Err(ManagementError::new(
            ManagementErrorCode::NotFound,
            format!("workflow {id} was not found"),
        )),
        WorkflowStatusUpdate::Conflict { current_revision } => Err(ManagementError::new(
            ManagementErrorCode::PreconditionFailed,
            format!("workflow revision changed; current revision is {current_revision}"),
        )),
    }
}

pub fn workflow_source(store: &Store, id: &str) -> Result<WorkflowSourceRecord, ManagementError> {
    store
        .get_workflow_source(id)
        .map_err(|_| {
            ManagementError::new(ManagementErrorCode::Internal, "cannot read workflow source")
        })?
        .ok_or_else(|| {
            ManagementError::new(
                ManagementErrorCode::NotFound,
                format!("workflow {id} was not found"),
            )
        })
}

/// Validates, compiles and atomically admits a manual run without changing the
/// workflow definition or its trigger eligibility.
pub fn admit_workflow_execution(
    store: &Store,
    id: &str,
    input: Value,
    request_identity: Option<(&str, &str)>,
    plugins: &PluginRegistry,
) -> Result<WorkflowExecutionAdmission, ManagementError> {
    if !input.is_object() {
        return Err(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            "workflow execution input must be an object",
        ));
    }
    let source = workflow_source(store, id)?;
    let (execution, created) = crate::runtime::prepare_canonical_execution_with_context_request(
        store,
        &source.source,
        &source.revision,
        plugins,
        input,
        serde_json::json!({}),
        request_identity,
    )
    .map_err(|error| {
        if error.contains("revision conflict") {
            ManagementError::new(ManagementErrorCode::PreconditionFailed, error)
        } else if error.contains("was not found") {
            ManagementError::new(ManagementErrorCode::NotFound, error)
        } else if error.contains("validation") || error.contains("limit") {
            ManagementError::new(ManagementErrorCode::InvalidOperation, error)
        } else {
            ManagementError::new(
                ManagementErrorCode::Internal,
                "workflow execution could not be admitted",
            )
        }
    })?;
    let workflow_revision = store
        .execution_workflow_revision(&execution.id)
        .map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::Internal,
                "cannot inspect admitted workflow revision",
            )
        })?
        .ok_or_else(|| {
            ManagementError::new(
                ManagementErrorCode::Internal,
                "admitted workflow revision is unavailable",
            )
        })?;
    Ok(WorkflowExecutionAdmission {
        execution,
        created,
        workflow_revision,
    })
}

pub fn list_workflow_page(
    store: &Store,
    limit: Option<usize>,
    cursor: Option<&str>,
) -> Result<ManagementPage<WorkflowRecord>, ManagementError> {
    let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE);
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            format!("workflow page limit must be from 1 to {MAX_PAGE_SIZE}"),
        ));
    }
    let empty_filters = BTreeMap::new();
    let after = if let Some(cursor) = cursor {
        let cursor = ManagementCursor::decode(cursor).map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "workflow cursor is invalid",
            )
        })?;
        cursor
            .validate_binding(&empty_filters, CursorOrder::MetadataIdentityAscending, None)
            .map_err(|_| {
                ManagementError::new(
                    ManagementErrorCode::InvalidRequest,
                    "workflow cursor does not match this list snapshot",
                )
            })?;
        cursor.position.first().cloned()
    } else {
        None
    };
    let mut records = store
        .list_workflows_after(after.as_deref(), limit + 1)
        .map_err(|_| {
            ManagementError::new(ManagementErrorCode::Internal, "cannot list workflows")
        })?;
    let has_more = records.len() > limit;
    if has_more {
        records.truncate(limit);
    }
    let next_cursor = if has_more {
        records.last().map(|record| {
            ManagementCursor {
                filters: empty_filters,
                order: CursorOrder::MetadataIdentityAscending,
                high_water: None,
                position: vec![record.id.clone()],
            }
            .encode()
            .expect("workflow cursor fields are JSON serializable")
        })
    } else {
        None
    };
    Ok(ManagementPage {
        items: records,
        next_cursor,
        snapshot: None,
    })
}

pub fn list_execution_page(
    store: &Store,
    workflow_id: Option<&str>,
    status: Option<&str>,
    limit: Option<usize>,
    cursor: Option<&str>,
) -> Result<ManagementPage<ExecutionSummaryView>, ManagementError> {
    let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE);
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            format!("execution page limit must be from 1 to {MAX_PAGE_SIZE}"),
        ));
    }
    if status.is_some_and(|status| {
        !matches!(
            status,
            "queued"
                | "running"
                | "cancelling"
                | "succeeded"
                | "failed"
                | "cancelled"
                | "timed_out"
                | "interrupted"
        )
    }) {
        return Err(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            "execution status filter is invalid",
        ));
    }
    let filters = execution_filters(workflow_id, status);
    let before = if let Some(cursor) = cursor {
        let cursor = ManagementCursor::decode(cursor).map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "execution cursor is invalid",
            )
        })?;
        cursor
            .validate_binding(&filters, CursorOrder::ExecutionCreatedDescending, None)
            .map_err(|_| {
                ManagementError::new(
                    ManagementErrorCode::InvalidRequest,
                    "execution cursor does not match these filters",
                )
            })?;
        if cursor.position.len() != 2 {
            return Err(ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "execution cursor position is invalid",
            ));
        }
        Some((cursor.position[0].clone(), cursor.position[1].clone()))
    } else {
        None
    };
    let (records, has_more) = store
        .list_execution_page(
            workflow_id,
            status,
            before
                .as_ref()
                .map(|(created_at, id)| (created_at.as_str(), id.as_str())),
            limit,
        )
        .map_err(|_| {
            ManagementError::new(ManagementErrorCode::Internal, "cannot list executions")
        })?;
    let items = records
        .into_iter()
        .map(execution_summary)
        .collect::<Vec<_>>();
    let next_cursor = if has_more {
        items.last().map(|record| {
            ManagementCursor {
                filters,
                order: CursorOrder::ExecutionCreatedDescending,
                high_water: None,
                position: vec![record.created_at.clone(), record.id.clone()],
            }
            .encode()
            .expect("execution cursor is JSON serializable")
        })
    } else {
        None
    };
    Ok(ManagementPage {
        items,
        next_cursor,
        snapshot: None,
    })
}

fn execution_filters(workflow_id: Option<&str>, status: Option<&str>) -> BTreeMap<String, String> {
    let mut filters = BTreeMap::new();
    if let Some(workflow_id) = workflow_id {
        filters.insert("workflowId".to_owned(), workflow_id.to_owned());
    }
    if let Some(status) = status {
        filters.insert("status".to_owned(), status.to_owned());
    }
    filters
}

fn execution_summary(record: ExecutionRecord) -> ExecutionSummaryView {
    ExecutionSummaryView {
        id: record.id,
        workflow_id: record.workflow_name.clone(),
        workflow_name: record.workflow_name,
        status: record.status,
        created_at: record.created_at,
        completed_at: record.completed_at,
    }
}

pub fn inspect_execution(
    store: &Store,
    id: &str,
    limit: Option<usize>,
    cursor: Option<&str>,
) -> Result<ExecutionDetailView, ManagementError> {
    let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE);
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            format!("execution section limit must be from 1 to {MAX_PAGE_SIZE}"),
        ));
    }
    let execution = store
        .get_execution(id)
        .map_err(|_| {
            ManagementError::new(ManagementErrorCode::Internal, "cannot inspect execution")
        })?
        .ok_or_else(|| {
            ManagementError::new(
                ManagementErrorCode::NotFound,
                format!("execution {id} was not found"),
            )
        })?;
    let filters = BTreeMap::from([
        ("executionId".to_owned(), id.to_owned()),
        ("section".to_owned(), "nodes".to_owned()),
    ]);
    let after_row_id = if let Some(cursor) = cursor {
        let cursor = ManagementCursor::decode(cursor).map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "execution section cursor is invalid",
            )
        })?;
        cursor
            .validate_binding(&filters, CursorOrder::NodeRowIdAscending, None)
            .map_err(|_| {
                ManagementError::new(
                    ManagementErrorCode::InvalidRequest,
                    "execution section cursor does not match this execution",
                )
            })?;
        if cursor.position.len() != 1 {
            return Err(ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "execution section cursor position is invalid",
            ));
        }
        cursor.position[0].parse::<u64>().map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "execution section cursor is invalid",
            )
        })?
    } else {
        0
    };
    let (nodes, has_more) = store
        .execution_node_page(id, after_row_id, limit + 1)
        .map_err(|error| {
            if error.contains("was not found") {
                ManagementError::new(ManagementErrorCode::NotFound, error)
            } else {
                ManagementError::new(
                    ManagementErrorCode::Internal,
                    "cannot inspect execution nodes",
                )
            }
        })?;
    let has_more = has_more || nodes.len() > limit;
    let mut nodes = nodes;
    if nodes.len() > limit {
        nodes.truncate(limit);
    }
    let node_items = nodes.iter().map(execution_node_view).collect::<Vec<_>>();
    let next_cursor = if has_more {
        nodes.last().map(|node| {
            ManagementCursor {
                filters,
                order: CursorOrder::NodeRowIdAscending,
                high_water: None,
                position: vec![node.row_id.to_string()],
            }
            .encode()
            .expect("node cursor is JSON serializable")
        })
    } else {
        None
    };
    let result_size = store.execution_result_size(id).map_err(|_| {
        ManagementError::new(
            ManagementErrorCode::Internal,
            "cannot inspect execution result metadata",
        )
    })?;
    let result = match result_size {
        None => ExecutionSection {
            availability: "not_recorded".to_owned(),
            value: None,
            guidance: Some("No execution result was recorded.".to_owned()),
        },
        Some(size) if size > MAX_METADATA_SECTION_BYTES as u64 => ExecutionSection {
            availability: "truncated".to_owned(),
            value: None,
            guidance: Some("Execution result exceeds the 4 MiB section limit.".to_owned()),
        },
        Some(_) => ExecutionSection {
            availability: "unavailable".to_owned(),
            value: None,
            guidance: Some(
                "Stored result safety cannot be verified without resolving all referenced secrets; the payload was omitted.".to_owned(),
            ),
        },
    };
    let trace_count = store.execution_trace_span_count(id).map_err(|_| {
        ManagementError::new(
            ManagementErrorCode::Internal,
            "cannot inspect execution trace metadata",
        )
    })?;
    let error = if execution.error.is_some() {
        ExecutionSection {
            availability: "unavailable".to_owned(),
            value: None,
            guidance: Some(
                "Stored error safety cannot be verified; the free-form payload was omitted."
                    .to_owned(),
            ),
        }
    } else {
        ExecutionSection {
            availability: "not_recorded".to_owned(),
            value: None,
            guidance: None,
        }
    };
    let workflow_revision = store.execution_workflow_revision(id).map_err(|_| {
        ManagementError::new(
            ManagementErrorCode::Internal,
            "cannot inspect admitted workflow revision",
        )
    })?;
    let started_at = store.execution_started_at(id).map_err(|_| {
        ManagementError::new(
            ManagementErrorCode::Internal,
            "cannot inspect execution timing",
        )
    })?;
    let pinned = store
        .execution_is_pinned(id)
        .map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::Internal,
                "cannot inspect execution retention state",
            )
        })?
        .unwrap_or(false);
    Ok(ExecutionDetailView {
        id: execution.id,
        workflow_id: execution.workflow_name.clone(),
        workflow_name: execution.workflow_name,
        workflow_revision,
        status: execution.status,
        created_at: execution.created_at,
        started_at,
        completed_at: execution.completed_at,
        pinned,
        error,
        result,
        nodes: ExecutionNodesSection {
            availability: "available".to_owned(),
            items: node_items,
            next_cursor,
            guidance: Some(
                "Node status is available; messages, results, and errors are omitted because their safety cannot be verified.".to_owned(),
            ),
        },
        trace: ExecutionTraceSection {
            availability: "unavailable".to_owned(),
            span_count: trace_count,
            guidance: "Trace attributes and free-form fields are omitted because their safety cannot be verified.".to_owned(),
        },
    })
}

fn execution_node_view(node: &ExecutionNodeSummary) -> ExecutionNodeView {
    ExecutionNodeView {
        node_id: node.node_id.clone(),
        status: node.status.clone(),
        details_availability: "unavailable".to_owned(),
    }
}

pub async fn cancel_execution(
    store: &Store,
    id: &str,
    wait_seconds: u64,
) -> Result<CancellationView, ManagementError> {
    if wait_seconds > 30 {
        return Err(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            "cancellation observation wait must be from 0 to 30 seconds",
        ));
    }
    let initial = store.request_execution_cancel_outcome(id).map_err(|_| {
        ManagementError::new(
            ManagementErrorCode::Internal,
            "cannot request execution cancellation",
        )
    })?;
    let (outcome, mut observed_status) = match initial {
        ExecutionCancellationOutcome::Requested { observed_status } => {
            ("requested", Some(observed_status))
        }
        ExecutionCancellationOutcome::AlreadyRequested { observed_status } => {
            ("already_requested", Some(observed_status))
        }
        ExecutionCancellationOutcome::CancelledBeforeStart { observed_status } => {
            ("cancelled_before_start", Some(observed_status))
        }
        ExecutionCancellationOutcome::TerminalUnchanged { observed_status } => {
            ("terminal_unchanged", Some(observed_status))
        }
        ExecutionCancellationOutcome::NotFound => {
            return Err(ManagementError::new(
                ManagementErrorCode::NotFound,
                format!("execution {id} was not found"),
            ));
        }
    };
    if wait_seconds > 0 && matches!(outcome, "requested" | "already_requested") {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(wait_seconds);
        loop {
            let current = store
                .get_execution(id)
                .map_err(|_| {
                    ManagementError::new(
                        ManagementErrorCode::Internal,
                        "cannot observe cancellation",
                    )
                })?
                .ok_or_else(|| {
                    ManagementError::new(
                        ManagementErrorCode::NotFound,
                        format!("execution {id} was not found"),
                    )
                })?;
            observed_status = Some(current.status.clone());
            if !matches!(current.status.as_str(), "queued" | "running" | "cancelling") {
                break;
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100).min(deadline - now)).await;
        }
    }
    let pending = observed_status
        .as_deref()
        .is_some_and(|status| matches!(status, "queued" | "running" | "cancelling"));
    Ok(CancellationView {
        outcome: outcome.to_owned(),
        observed_status,
        pending,
    })
}

pub fn execution_event_page(
    store: &Store,
    execution_id: &str,
    limit: Option<usize>,
    cursor: Option<&str>,
    requested_high_water: Option<u64>,
) -> Result<ExecutionEventPage, ManagementError> {
    let limit = limit.unwrap_or(DEFAULT_EVENT_PAGE_SIZE);
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            format!("execution event limit must be from 1 to {MAX_PAGE_SIZE}"),
        ));
    }
    let (current_high_water, _first_retained) = store
        .execution_event_bounds(execution_id)
        .map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::Internal,
                "cannot inspect execution history",
            )
        })?
        .ok_or_else(|| {
            ManagementError::new(
                ManagementErrorCode::NotFound,
                format!("execution {execution_id} was not found"),
            )
        })?;
    let filters = BTreeMap::from([
        ("executionId".to_owned(), execution_id.to_owned()),
        ("section".to_owned(), "events".to_owned()),
    ]);
    let (high_water, after_sequence) = if let Some(cursor) = cursor {
        let cursor = ManagementCursor::decode(cursor).map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "execution event cursor is invalid",
            )
        })?;
        cursor
            .validate_binding(
                &filters,
                CursorOrder::EventSequenceAscending,
                cursor.high_water,
            )
            .map_err(|_| {
                ManagementError::new(
                    ManagementErrorCode::InvalidRequest,
                    "execution event cursor does not match this snapshot",
                )
            })?;
        let high_water = cursor.high_water.ok_or_else(|| {
            ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "execution event cursor has no high-water mark",
            )
        })?;
        if requested_high_water.is_some_and(|requested| requested != high_water)
            || high_water > current_high_water
            || cursor.position.len() != 1
        {
            return Err(ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "execution event cursor does not match the requested high-water snapshot",
            ));
        }
        let after = cursor.position[0].parse::<u64>().map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "execution event cursor is invalid",
            )
        })?;
        (high_water, after)
    } else {
        let high_water = requested_high_water.unwrap_or(current_high_water);
        if high_water > current_high_water {
            return Err(ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "execution event high-water exceeds the current history",
            ));
        }
        (high_water, 0)
    };
    let events = store
        .execution_event_page(execution_id, after_sequence, high_water, limit + 1)
        .map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::Internal,
                "cannot read execution history",
            )
        })?;
    let has_more = events.len() > limit;
    let mut items = events;
    if has_more {
        items.truncate(limit);
    }
    let truncated = items.iter().any(|event| {
        event
            .payload
            .get("availability")
            .and_then(serde_json::Value::as_str)
            == Some("truncated")
    });
    let next_cursor = if has_more {
        items.last().map(|event| {
            ManagementCursor {
                filters,
                order: CursorOrder::EventSequenceAscending,
                high_water: Some(high_water),
                position: vec![event.sequence.to_string()],
            }
            .encode()
            .expect("event cursor is JSON serializable")
        })
    } else {
        None
    };
    Ok(ExecutionEventPage {
        execution_id: execution_id.to_owned(),
        high_water,
        items,
        next_cursor,
        retention_availability: "unknown".to_owned(),
        truncated,
    })
}

pub fn list_trigger_page(
    store: &Store,
    limit: Option<usize>,
    cursor: Option<&str>,
) -> Result<ManagementPage<RegisteredTriggerProjection>, ManagementError> {
    let limit = limit.unwrap_or(DEFAULT_PAGE_SIZE);
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(ManagementError::new(
            ManagementErrorCode::InvalidRequest,
            format!("trigger page limit must be from 1 to {MAX_PAGE_SIZE}"),
        ));
    }
    let mut triggers = crate::scheduler::registered_trigger_projections(store).map_err(|_| {
        ManagementError::new(
            ManagementErrorCode::Internal,
            "cannot inspect registered triggers",
        )
    })?;
    triggers.sort_by(|left, right| {
        left.workflow_id
            .cmp(&right.workflow_id)
            .then_with(|| left.trigger_id.cmp(&right.trigger_id))
    });
    let filters = BTreeMap::new();
    let after = if let Some(cursor) = cursor {
        let cursor = ManagementCursor::decode(cursor).map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "trigger cursor is invalid",
            )
        })?;
        cursor
            .validate_binding(
                &filters,
                CursorOrder::TriggerCompositeIdentityAscending,
                None,
            )
            .map_err(|_| {
                ManagementError::new(
                    ManagementErrorCode::InvalidRequest,
                    "trigger cursor does not match this list",
                )
            })?;
        if cursor.position.len() != 2 {
            return Err(ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "trigger cursor position is invalid",
            ));
        }
        Some((cursor.position[0].clone(), cursor.position[1].clone()))
    } else {
        None
    };
    let after_position = after
        .as_ref()
        .map(|(workflow, trigger)| (workflow.as_str(), trigger.as_str()));
    if let Some((workflow, trigger)) = after_position {
        triggers.retain(|item| {
            item.workflow_id.as_str() > workflow
                || (item.workflow_id == workflow && item.trigger_id.as_str() > trigger)
        });
    }
    let has_more = triggers.len() > limit;
    if has_more {
        triggers.truncate(limit);
    }
    let next_cursor = if has_more {
        triggers.last().map(|item| {
            ManagementCursor {
                filters,
                order: CursorOrder::TriggerCompositeIdentityAscending,
                high_water: None,
                position: vec![item.workflow_id.clone(), item.trigger_id.clone()],
            }
            .encode()
            .expect("trigger cursor is JSON serializable")
        })
    } else {
        None
    };
    Ok(ManagementPage {
        items: triggers,
        next_cursor,
        snapshot: None,
    })
}

pub fn inspect_trigger(
    store: &Store,
    workflow_id: &str,
    trigger_id: &str,
) -> Result<RegisteredTriggerProjection, ManagementError> {
    crate::scheduler::registered_trigger_projections(store)
        .map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::Internal,
                "cannot inspect registered triggers",
            )
        })?
        .into_iter()
        .find(|item| item.workflow_id == workflow_id && item.trigger_id == trigger_id)
        .ok_or_else(|| {
            ManagementError::new(
                ManagementErrorCode::NotFound,
                format!("trigger {workflow_id}/{trigger_id} was not found"),
            )
        })
}

fn diagnostics_message(analysis: &WorkflowAnalysis) -> String {
    analysis
        .diagnostics
        .iter()
        .map(|diagnostic| diagnostic.message.as_str())
        .collect::<Vec<_>>()
        .join("; ")
}

/// A stable error code shared by the management HTTP and CLI adapters.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagementErrorCode {
    InvalidRequest,
    Unauthenticated,
    Forbidden,
    NotFound,
    Conflict,
    PreconditionFailed,
    PayloadTooLarge,
    InvalidOperation,
    Unsupported,
    UpstreamFailure,
    DeadlineExceeded,
    Internal,
}

impl ManagementErrorCode {
    pub fn http_status(self) -> u16 {
        match self {
            Self::InvalidRequest => 400,
            Self::Unauthenticated => 401,
            Self::Forbidden => 403,
            Self::NotFound => 404,
            Self::Conflict => 409,
            Self::PreconditionFailed => 412,
            Self::PayloadTooLarge => 413,
            Self::InvalidOperation => 422,
            Self::Unsupported => 501,
            Self::UpstreamFailure => 502,
            Self::DeadlineExceeded => 504,
            Self::Internal => 500,
        }
    }

    pub fn guidance(self) -> &'static str {
        match self {
            Self::InvalidRequest => "Check the request syntax and documented limits.",
            Self::Unauthenticated => "Provide a valid Core credential.",
            Self::Forbidden => "Use a credential with the required management scope.",
            Self::NotFound => "Verify the selected resource and Core context.",
            Self::Conflict => "Refresh the resource and resolve the reported conflict.",
            Self::PreconditionFailed => {
                "Read the current revision and retry with its revision guard."
            }
            Self::PayloadTooLarge => {
                "Reduce the encoded request or source to its documented limit."
            }
            Self::InvalidOperation => "Correct the supplied definition or operation input.",
            Self::Unsupported => "This Core does not support the requested operation.",
            Self::UpstreamFailure => {
                "Check the configured integration and retry explicitly if safe."
            }
            Self::DeadlineExceeded => "The bounded operation did not finish before its deadline.",
            Self::Internal => {
                "Inspect sanitized Core diagnostics; do not repeat mutations automatically."
            }
        }
    }
}

/// A sanitized error envelope. `details` is for non-sensitive field diagnostics only.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagementError {
    pub code: ManagementErrorCode,
    pub message: String,
    pub guidance: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl ManagementError {
    pub fn new(code: ManagementErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: sanitize_message(&message.into()),
            guidance: code.guidance().to_owned(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }
}

fn sanitize_message(message: &str) -> String {
    // Avoid accidentally echoing common credential-bearing values into API
    // errors. Domain services should still provide purpose-written messages.
    let mut sanitized = message.to_owned();
    for marker in ["Bearer ", "bearer ", "api_key=", "apiKey=", "token="] {
        while let Some(start) = sanitized.find(marker) {
            let value_start = start + marker.len();
            let value_end = sanitized[value_start..]
                .find(char::is_whitespace)
                .map_or(sanitized.len(), |end| value_start + end);
            sanitized.replace_range(start..value_end, "[redacted]");
        }
    }
    sanitized
}

/// A request payload whose `Debug` output never prints the secret value.
#[derive(Clone, Deserialize, Serialize)]
#[serde(transparent)]
pub struct SecretValue(String);

impl SecretValue {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl Drop for SecretValue {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretValue([redacted])")
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderAuthPreparation {
    pub provider_type: ProviderType,
    pub mechanism: String,
    pub instructions: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IntegrationCheck {
    pub status: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderCapabilityCheck {
    pub capability: String,
    pub status: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderTestReport {
    pub profile_id: String,
    pub checked_at: Option<String>,
    pub connectivity: IntegrationCheck,
    pub authentication: IntegrationCheck,
    pub capabilities: Vec<ProviderCapabilityCheck>,
}

/// An integration client owns resolved credentials for one requested operation
/// only. Retained values are used to redact echoes before the result is exposed.
pub struct ResolvedMcpClient {
    pub client: McpClient,
    pub secret_values: Vec<SecretValue>,
}

pub fn prepare_provider_auth(
    store: &Store,
    id: &str,
) -> Result<ProviderAuthPreparation, ManagementError> {
    let profile = store
        .get_provider_profile(id)
        .map_err(|_| internal_error("cannot inspect provider profile"))?
        .ok_or_else(|| {
            ManagementError::new(
                ManagementErrorCode::NotFound,
                "provider profile was not found",
            )
        })?;
    let (mechanism, instructions) = match profile.provider_type {
        ProviderType::MiniMax => (
            "secureInput",
            "Enter the MiniMax API key using hidden terminal input or explicit stdin. The value is stored only in the configured secret reference.",
        ),
        ProviderType::Codex => (
            "browser",
            "Complete ChatGPT authorization in a browser. If browser launch is unavailable, open the authorization URL printed by the CLI manually.",
        ),
    };
    Ok(ProviderAuthPreparation {
        provider_type: profile.provider_type,
        mechanism: mechanism.to_owned(),
        instructions: instructions.to_owned(),
    })
}

pub fn commit_provider_auth(
    store: &Store,
    id: &str,
    value: SecretValue,
    oauth_bundle: bool,
) -> Result<SecretSetOutcome, ManagementError> {
    let profile = store
        .get_provider_profile(id)
        .map_err(|_| internal_error("cannot inspect provider profile"))?
        .ok_or_else(|| {
            ManagementError::new(
                ManagementErrorCode::NotFound,
                "provider profile was not found",
            )
        })?;
    let secret_ref = match (&profile.provider_type, &profile.auth, oauth_bundle) {
        (ProviderType::MiniMax, crate::ProviderAuth::ApiKeySecret { secret_ref }, false) => {
            secret_ref
        }
        (ProviderType::Codex, crate::ProviderAuth::OAuthSecret { secret_ref }, true) => secret_ref,
        _ => {
            return Err(ManagementError::new(
                ManagementErrorCode::InvalidRequest,
                "authentication material does not match the provider profile",
            ));
        }
    };
    if oauth_bundle {
        let mut tokens =
            crate::codex::CodexOAuthTokens::from_secret(value.expose()).map_err(|_| {
                ManagementError::new(
                    ManagementErrorCode::InvalidRequest,
                    "Codex authorization did not return a valid token bundle",
                )
            })?;
        tokens.zeroize_secrets();
    }
    store
        .set_secret_versioned(secret_ref, value.expose())
        .map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::Internal,
                "provider authentication could not be committed; prior access was preserved",
            )
        })
}

pub fn test_provider(store: &Store, id: &str) -> Result<ProviderTestReport, ManagementError> {
    let profile = store
        .get_provider_profile(id)
        .map_err(|_| internal_error("cannot inspect provider profile"))?
        .ok_or_else(|| {
            ManagementError::new(
                ManagementErrorCode::NotFound,
                "provider profile was not found",
            )
        })?;
    let reference = match &profile.auth {
        crate::ProviderAuth::ApiKeySecret { secret_ref }
        | crate::ProviderAuth::OAuthSecret { secret_ref } => secret_ref,
    };
    let authentication = match store.resolve_secret(reference) {
        Ok(value) => {
            let secret = SecretValue::new(value);
            let valid_local_bundle = match profile.provider_type {
                ProviderType::MiniMax => true,
                ProviderType::Codex => {
                    match crate::codex::CodexOAuthTokens::from_secret(secret.expose()) {
                        Ok(mut tokens) => {
                            tokens.zeroize_secrets();
                            true
                        }
                        Err(_) => false,
                    }
                }
            };
            drop(secret);
            if valid_local_bundle {
                IntegrationCheck {
                    status: "unknown".to_owned(),
                    message:
                        "credential is available locally; upstream authentication was not attempted"
                            .to_owned(),
                }
            } else {
                IntegrationCheck {
                    status: "failed".to_owned(),
                    message: "configured credential is invalid".to_owned(),
                }
            }
        }
        Err(_) => IntegrationCheck {
            status: "failed".to_owned(),
            message: "configured credential reference is unavailable".to_owned(),
        },
    };
    let report = ProviderTestReport {
        profile_id: profile.id.clone(),
        checked_at: None,
        connectivity: IntegrationCheck {
            status: "unknown".to_owned(),
            message: "no supported connectivity probe is configured for this provider profile"
                .to_owned(),
        },
        authentication: authentication.clone(),
        capabilities: profile
            .capabilities
            .iter()
            .map(|capability| ProviderCapabilityCheck {
                capability: capability.clone(),
                status: "unsupported".to_owned(),
                message: "live capability checks are not supported; no generation request was sent"
                    .to_owned(),
            })
            .collect(),
    };
    let diagnostic_status = if authentication.status == "failed" {
        ProviderProfileStatus::Unavailable
    } else {
        ProviderProfileStatus::Unknown
    };
    let details = serde_json::to_value(&report).unwrap_or(Value::Null);
    store
        .diagnose_provider_profile(
            id,
            ProviderProfileDiagnostic {
                status: diagnostic_status,
                checked_at: None,
                message: Some(
                    "provider checks completed with unsupported or unknown upstream evidence"
                        .to_owned(),
                ),
                details: Some(details),
            },
        )
        .map_err(|_| internal_error("cannot persist provider diagnostic"))?;
    let checked_at = store
        .get_provider_profile(id)
        .map_err(|_| internal_error("cannot read provider diagnostic"))?
        .and_then(|profile| profile.diagnostic.checked_at);
    Ok(ProviderTestReport {
        checked_at,
        ..report
    })
}

pub fn provider_models(store: &Store, id: &str) -> Result<Value, ManagementError> {
    let profile = store
        .get_provider_profile(id)
        .map_err(|_| internal_error("cannot inspect provider profile"))?
        .ok_or_else(|| {
            ManagementError::new(
                ManagementErrorCode::NotFound,
                "provider profile was not found",
            )
        })?;
    Ok(serde_json::json!({
        "providerId": profile.id,
        "discovery": {
            "status": "unsupported",
            "message": "live model discovery is not supported for this provider"
        },
        "models": profile.allowed_models.iter().map(|model| serde_json::json!({
            "id": model,
            "provenance": "configured",
            "isDefault": model == &profile.default_model
        })).collect::<Vec<_>>()
    }))
}

pub async fn connect_named_mcp(
    store: &Store,
    connection: &NamedMcpConnection,
) -> Result<ResolvedMcpClient, ManagementError> {
    let mut secret_values = Vec::new();
    let client = match &connection.transport {
        McpConnectionTransport::Stdio {
            command,
            args,
            working_directory,
            environment_secret_refs,
        } => {
            let mut env = BTreeMap::new();
            for (environment_name, secret_ref) in environment_secret_refs {
                let value = resolve_secret_value(store, secret_ref)?;
                env.insert(environment_name.clone(), value.expose().to_owned());
                secret_values.push(value);
            }
            McpClient::connect(
                McpStdioServerConfig {
                    command: command.clone(),
                    args: args.clone(),
                    current_dir: Some(working_directory.clone()),
                    env,
                },
                McpClientOptions::default(),
            )
            .await
        }
        McpConnectionTransport::Http {
            endpoint,
            bearer_secret_ref,
        } => {
            let bearer_token = if let Some(secret_ref) = bearer_secret_ref {
                let value = resolve_secret_value(store, secret_ref)?;
                let token = value.expose().to_owned();
                secret_values.push(value);
                Some(token)
            } else {
                None
            };
            McpClient::connect_http(
                McpHttpServerConfig {
                    endpoint: endpoint.clone(),
                    bearer_token,
                },
                McpClientOptions::default(),
            )
            .await
        }
    }
    .map_err(map_mcp_operation_error)?;
    Ok(ResolvedMcpClient {
        client,
        secret_values,
    })
}

pub fn check_named_mcp_auth(
    store: &Store,
    connection: &NamedMcpConnection,
) -> Result<Value, ManagementError> {
    let mut references = Vec::new();
    match &connection.transport {
        McpConnectionTransport::Http {
            bearer_secret_ref: Some(reference),
            ..
        } => references.push(reference),
        McpConnectionTransport::Http {
            bearer_secret_ref: None,
            ..
        } => {}
        McpConnectionTransport::Stdio {
            environment_secret_refs,
            ..
        } => references.extend(environment_secret_refs.values()),
    }
    if references.is_empty() {
        return Ok(serde_json::json!({
            "status": "notRequired",
            "message": "no authentication references are configured"
        }));
    }
    for reference in references {
        drop(resolve_secret_value(store, reference)?);
    }
    Ok(serde_json::json!({
        "status": "available",
        "message": "configured credential references are available; upstream validity was not tested"
    }))
}

fn resolve_secret_value(store: &Store, reference: &str) -> Result<SecretValue, ManagementError> {
    store
        .resolve_secret(reference)
        .map(SecretValue::new)
        .map_err(|_| {
            ManagementError::new(
                ManagementErrorCode::NotFound,
                "a configured integration credential reference is unavailable",
            )
        })
}

pub fn map_mcp_operation_error(error: crate::mcp::McpError) -> ManagementError {
    use crate::mcp::McpError;
    match error {
        McpError::AggregateDeadline(_) | McpError::Timeout(_) => ManagementError::new(
            ManagementErrorCode::DeadlineExceeded,
            "MCP operation exceeded its bounded deadline",
        ),
        McpError::ToolNotFound => ManagementError::new(
            ManagementErrorCode::NotFound,
            "the requested MCP tool was not found",
        ),
        McpError::InvalidToolInput | McpError::InvalidToolSchema => ManagementError::new(
            ManagementErrorCode::InvalidOperation,
            "MCP tool input or discovered schema is invalid",
        ),
        _ => ManagementError::new(
            ManagementErrorCode::UpstreamFailure,
            "MCP operation failed; upstream details were omitted",
        ),
    }
}

fn internal_error(message: &str) -> ManagementError {
    ManagementError::new(ManagementErrorCode::Internal, message)
}

/// Generic one-page result. Cursors represent a fixed filter/order/high-water
/// snapshot and deliberately contain no credential or authorization data.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagementPage<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum CursorOrder {
    ExecutionCreatedDescending,
    MetadataIdentityAscending,
    NodeRowIdAscending,
    TriggerCompositeIdentityAscending,
    EventSequenceAscending,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagementCursor {
    pub filters: BTreeMap<String, String>,
    pub order: CursorOrder,
    pub high_water: Option<u64>,
    pub position: Vec<String>,
}

impl ManagementCursor {
    pub fn encode(&self) -> Result<String, String> {
        let bytes = serde_json::to_vec(self).map_err(|error| error.to_string())?;
        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write as _;
            write!(&mut encoded, "{byte:02x}").map_err(|error| error.to_string())?;
        }
        Ok(encoded)
    }

    pub fn decode(value: &str) -> Result<Self, String> {
        if value.is_empty() || !value.len().is_multiple_of(2) || value.len() > 16_384 {
            return Err("management cursor is malformed or exceeds its limit".to_owned());
        }
        let mut bytes = Vec::with_capacity(value.len() / 2);
        for pair in value.as_bytes().as_chunks::<2>().0 {
            let pair = std::str::from_utf8(pair).map_err(|_| "management cursor is invalid")?;
            bytes.push(u8::from_str_radix(pair, 16).map_err(|_| "management cursor is invalid")?);
        }
        serde_json::from_slice(&bytes).map_err(|_| "management cursor is invalid".to_owned())
    }

    pub fn validate_binding(
        &self,
        filters: &BTreeMap<String, String>,
        order: CursorOrder,
        high_water: Option<u64>,
    ) -> Result<(), String> {
        if &self.filters != filters || self.order != order || self.high_water != high_water {
            return Err("management cursor does not match the requested snapshot".to_owned());
        }
        Ok(())
    }
}

/// Stable execution ordering key: creation time descending, then ID ascending.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionOrderKey {
    pub created_at: String,
    pub id: String,
}

pub fn sort_execution_order(items: &mut [ExecutionOrderKey]) {
    items.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| left.id.cmp(&right.id))
    });
}

pub fn sort_metadata_identity(items: &mut [String]) {
    items.sort();
}

pub fn sort_event_sequence(items: &mut [u64]) {
    items.sort();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn management_error_codes_have_the_contract_status_mapping() {
        let cases = [
            (ManagementErrorCode::InvalidRequest, 400),
            (ManagementErrorCode::Unauthenticated, 401),
            (ManagementErrorCode::Forbidden, 403),
            (ManagementErrorCode::NotFound, 404),
            (ManagementErrorCode::Conflict, 409),
            (ManagementErrorCode::PreconditionFailed, 412),
            (ManagementErrorCode::PayloadTooLarge, 413),
            (ManagementErrorCode::InvalidOperation, 422),
            (ManagementErrorCode::Unsupported, 501),
            (ManagementErrorCode::UpstreamFailure, 502),
            (ManagementErrorCode::DeadlineExceeded, 504),
        ];
        for (code, expected) in cases {
            assert_eq!(code.http_status(), expected);
            assert!(!code.guidance().is_empty());
        }
    }

    #[test]
    fn cursor_binds_filters_order_and_high_water_without_credentials() {
        let filters = BTreeMap::from([("status".to_owned(), "failed".to_owned())]);
        let cursor = ManagementCursor {
            filters: filters.clone(),
            order: CursorOrder::EventSequenceAscending,
            high_water: Some(42),
            position: vec!["17".into()],
        };
        let encoded = cursor.encode().expect("cursor should encode");
        let decoded = ManagementCursor::decode(&encoded).expect("cursor should decode");
        decoded
            .validate_binding(&filters, CursorOrder::EventSequenceAscending, Some(42))
            .expect("matching snapshot should validate");
        assert!(
            decoded
                .validate_binding(&filters, CursorOrder::EventSequenceAscending, Some(43))
                .is_err()
        );
        assert!(!encoded.contains("credential"));
    }

    #[test]
    fn pagination_orderings_have_stable_tie_breakers() {
        let mut executions = vec![
            ExecutionOrderKey {
                created_at: "2026-01-01".into(),
                id: "b".into(),
            },
            ExecutionOrderKey {
                created_at: "2026-01-02".into(),
                id: "z".into(),
            },
            ExecutionOrderKey {
                created_at: "2026-01-01".into(),
                id: "a".into(),
            },
        ];
        sort_execution_order(&mut executions);
        assert_eq!(
            executions
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            ["z", "a", "b"]
        );
        let mut metadata = vec!["z".into(), "a".into()];
        sort_metadata_identity(&mut metadata);
        assert_eq!(metadata, ["a", "z"]);
        let mut events = vec![9, 2, 5];
        sort_event_sequence(&mut events);
        assert_eq!(events, [2, 5, 9]);
    }

    #[test]
    fn secret_debug_and_error_messages_do_not_echo_common_credentials() {
        let secret = SecretValue("private-proof".into());
        assert_eq!(secret.expose(), "private-proof");
        assert!(!format!("{secret:?}").contains("private-proof"));
        let error = ManagementError::new(
            ManagementErrorCode::UpstreamFailure,
            "upstream rejected Authorization: Bearer private-proof",
        );
        assert!(!error.message.contains("private-proof"));
    }
}
