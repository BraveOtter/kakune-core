//! Core services scoped to one authorized plugin operation.
use crate::{
    Store,
    plugin_process::{HostRequestHandler, PluginPermissions},
};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
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
