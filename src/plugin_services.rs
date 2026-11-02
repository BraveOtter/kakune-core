//! Core services scoped to one authorized plugin operation.
use crate::{
    Store,
    plugin_process::{HostRequestHandler, PluginPermissions},
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

pub fn handler(
    store: Store,
    execution: String,
    node: String,
    permissions: Option<PluginPermissions>,
) -> HostRequestHandler {
    let spans = Arc::new(Mutex::new(HashSet::<String>::new()));
    let artifacts = Arc::new(Mutex::new(HashSet::<String>::new()));
    Arc::new(move |method, params| {
        let (store, execution, node, permissions, spans, artifacts) = (
            store.clone(),
            execution.clone(),
            node.clone(),
            permissions.clone(),
            spans.clone(),
            artifacts.clone(),
        );
        Box::pin(async move {
            if store.execution_cancel_requested(&execution)? {
                return Err("execution cancellation requested".into());
            }
            match method.as_str() {
                "secret/resolve" => {
                    let name = required(&params, "name")?;
                    if !permissions
                        .as_ref()
                        .is_some_and(|policy| policy.secrets.iter().any(|allowed| allowed == name))
                    {
                        return Err("secret access was not granted to this plugin".into());
                    }
                    Ok(json!({"value":store.resolve_secret(name)?}))
                }
                "artifact/write" => {
                    let data = required(&params, "text")?;
                    if data.len() > 512 * 1024 {
                        return Err("artifact text exceeds the inline service limit".into());
                    }
                    let record = store.put_artifact(
                        data.as_bytes(),
                        params["mediaType"]
                            .as_str()
                            .unwrap_or("text/plain; charset=utf-8"),
                        params["name"].as_str(),
                    )?;
                    artifacts
                        .lock()
                        .map_err(|_| "artifact access lock failed")?
                        .insert(record.id.clone());
                    serde_json::to_value(record).map_err(|error| error.to_string())
                }
                "artifact/read" => {
                    let id = required(&params, "id")?;
                    if !artifacts
                        .lock()
                        .map_err(|_| "artifact access lock failed")?
                        .contains(id)
                    {
                        return Err("artifact is outside this plugin operation".into());
                    }
                    let (record, bytes) = store.read_artifact(id)?.ok_or("artifact not found")?;
                    if bytes.len() > 512 * 1024 {
                        return Err("artifact exceeds the inline service limit".into());
                    }
                    Ok(
                        json!({"artifact":record,"text":String::from_utf8(bytes).map_err(|_| "artifact is not UTF-8 text")?}),
                    )
                }
                "trace/startSpan" => {
                    let parent = params["parentSpanId"].as_str();
                    if parent.is_some_and(|id| !spans.lock().is_ok_and(|owned| owned.contains(id)))
                    {
                        return Err("parent span is outside this plugin operation".into());
                    }
                    let id = store.start_trace_span(
                        &execution,
                        parent,
                        ("tool", required(&params, "name")?),
                        Some(&node),
                        None,
                        params.get("attributes").unwrap_or(&json!({})),
                    )?;
                    spans
                        .lock()
                        .map_err(|_| "span access lock failed")?
                        .insert(id.clone());
                    Ok(json!({"spanId":id}))
                }
                "trace/endSpan" => {
                    let id = required(&params, "spanId")?;
                    if !spans
                        .lock()
                        .map_err(|_| "span access lock failed")?
                        .remove(id)
                    {
                        return Err(
                            "span is outside this plugin operation or already closed".into()
                        );
                    }
                    let status = required(&params, "status")?;
                    if !["succeeded", "failed", "cancelled"].contains(&status) {
                        return Err("invalid terminal span status".into());
                    }
                    store.finish_trace_span(id, status, params["error"].as_str())?;
                    Ok(json!({}))
                }
                _ => Err(format!("unsupported Core service: {method}")),
            }
        })
    })
}

fn required<'a>(value: &'a Value, name: &str) -> Result<&'a str, String> {
    value
        .get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("service parameter {name} is required"))
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginRemovalReport {
    pub plugin_id: String,
    pub status: String,
    pub active_executions: u64,
    pub affected_workflows: Vec<String>,
    pub message: String,
}

/// Atomically publishes the removal barrier and disables workflows that
/// reference node types actually declared by the installed plugin.
pub fn request_removal(store: &Store, plugin_id: &str) -> Result<PluginRemovalReport, String> {
    let plugin = store
        .list_installed_plugins()?
        .into_iter()
        .find(|plugin| plugin.id == plugin_id)
        .ok_or_else(|| format!("plugin {plugin_id} is not installed"))?;
    let lifecycle = store
        .plugin_removal_state(plugin_id)?
        .ok_or_else(|| "plugin lifecycle record is missing".to_owned())?;
    if lifecycle.status == "removal failed" {
        store.resume_plugin_removal(plugin_id)?;
        if store.plugin_execution_pin_count(plugin_id)? == 0 {
            reconcile_one(store, plugin_id)?;
        }
        return removal_report(store, plugin_id, Vec::new());
    }

    let node_types =
        crate::plugin_diagnostics::declared_node_types(Path::new(&plugin.manifest_path))?;
    let workflows = store.list_workflows()?;
    if workflows.len() > 10_000 {
        return Err("plugin removal reference scan exceeds the workflow limit".to_owned());
    }
    let mut affected = Vec::new();
    for workflow in workflows {
        let Some(source) = store.get_workflow_source(&workflow.id)? else {
            return Err(format!(
                "workflow {} disappeared during removal review",
                workflow.id
            ));
        };
        if crate::plugin_diagnostics::workflow_references(&source.source, &node_types)? {
            affected.push(workflow.id);
        }
    }
    if affected.len() > 10_000 {
        return Err("plugin removal affects too many workflows".to_owned());
    }
    let ids = affected.iter().map(String::as_str).collect::<Vec<_>>();
    let active_executions = store.request_plugin_removal(plugin_id, &ids)?;
    if active_executions == 0 {
        reconcile_one(store, plugin_id)?;
    }
    removal_report(store, plugin_id, affected)
}

/// Retries pending removals after startup and terminal execution transitions.
/// Failed removals remain observable until an operator explicitly retries.
pub fn reconcile_pending_removals(store: &Store) -> Result<Vec<PluginRemovalReport>, String> {
    let plugins = store.list_installed_plugins()?;
    if plugins.len() > 10_000 {
        return Err("plugin removal reconciliation exceeds the installation limit".to_owned());
    }
    let mut results = Vec::new();
    for plugin in plugins {
        let Some(state) = store.plugin_removal_state(&plugin.id)? else {
            continue;
        };
        if state.status != "pending removal" {
            continue;
        }
        let pins = store.plugin_execution_pin_count(&plugin.id)?;
        if pins > 0 {
            continue;
        }
        reconcile_one(store, &plugin.id)?;
        results.push(removal_report(store, &plugin.id, Vec::new())?);
    }
    Ok(results)
}

fn removal_report(
    store: &Store,
    plugin_id: &str,
    affected_workflows: Vec<String>,
) -> Result<PluginRemovalReport, String> {
    let lifecycle = store.plugin_removal_state(plugin_id)?;
    let active_executions = store.plugin_execution_pin_count(plugin_id)?;
    let (status, message) = match lifecycle {
        Some(state) if state.status == "pending removal" => (
            state.status,
            "removal is pending until all admitted executions finish".to_owned(),
        ),
        Some(state) if state.status == "removal failed" => (
            state.status,
            state
                .failure_details
                .unwrap_or_else(|| "removal failed; retry with plugin remove".to_owned()),
        ),
        None => ("removed".to_owned(), "plugin removal completed".to_owned()),
        Some(state) => (state.status, "plugin removal has not completed".to_owned()),
    };
    Ok(PluginRemovalReport {
        plugin_id: plugin_id.to_owned(),
        status,
        active_executions,
        affected_workflows,
        message,
    })
}

fn reconcile_one(store: &Store, plugin_id: &str) -> Result<(), String> {
    if store.plugin_execution_pin_count(plugin_id)? > 0 {
        return Ok(());
    }
    let Some(plugin) = store
        .list_installed_plugins()?
        .into_iter()
        .find(|plugin| plugin.id == plugin_id)
    else {
        return Ok(());
    };
    let outcome = remove_plugin_files(store.data_dir(), &plugin.manifest_path);
    if let Err(category) = outcome {
        store.mark_plugin_removal_failed(plugin_id, category)?;
        return Ok(());
    }
    if store.finalize_plugin_removal(plugin_id).is_err() {
        store.mark_plugin_removal_failed(plugin_id, "storage finalization failed")?;
    }
    Ok(())
}

fn remove_plugin_files(data_dir: &Path, manifest_path: &str) -> Result<(), &'static str> {
    let root = data_dir.join("plugins");
    let manifest = PathBuf::from(manifest_path);
    let package = manifest.parent().ok_or("invalid plugin path")?;
    if !package.starts_with(&root) || package == root {
        return Err("plugin path outside managed directory");
    }
    if !package.exists() {
        return Ok(());
    }
    let root_canonical = fs::canonicalize(&root).map_err(|_| "managed directory unavailable")?;
    let package_metadata = fs::symlink_metadata(package).map_err(|_| "plugin path unavailable")?;
    if package_metadata.file_type().is_symlink() || !package_metadata.is_dir() {
        return Err("plugin path is not a managed directory");
    }
    let package_canonical = fs::canonicalize(package).map_err(|_| "plugin path unavailable")?;
    if !package_canonical.starts_with(&root_canonical) {
        return Err("plugin path outside managed directory");
    }
    fs::remove_dir_all(package).map_err(|_| "plugin filesystem removal failed")
}
