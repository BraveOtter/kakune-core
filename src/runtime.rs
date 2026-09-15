use std::{
    collections::{BTreeMap, HashSet},
    fs,
    io::Read,
    path::{Component, Path},
    process::{Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use futures_util::StreamExt;
use reqwest::{
    Client, Method, Url,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use serde_json::{Map, Number, Value};

use crate::{
    ExecutionRecord, ProviderAuth, ProviderProfile, ProviderProfileStatus, ProviderType, Store,
    WorkflowDocument, planner,
    plugin_process::{PluginRegistry, RegisteredPluginNode},
    workflow::{WorkflowBinding, WorkflowNode, WorkflowSubgraph},
};

pub fn run_workflow(store: &Store, workflow: &WorkflowDocument) -> Result<ExecutionRecord, String> {
    run_workflow_with_plugins(store, workflow, &PluginRegistry::default())
}

/// Runs a workflow using only plugin nodes explicitly present in `plugins`.
pub fn run_workflow_with_plugins(
    store: &Store,
    workflow: &WorkflowDocument,
    plugins: &PluginRegistry,
) -> Result<ExecutionRecord, String> {
    run_workflow_with_context(
        store,
        workflow,
        plugins,
        Value::Object(Map::new()),
        Value::Object(Map::new()),
    )
}

pub fn run_workflow_with_context(
    store: &Store,
    workflow: &WorkflowDocument,
    plugins: &PluginRegistry,
    input_values: Value,
    trigger_values: Value,
) -> Result<ExecutionRecord, String> {
    let source_record = store.get_workflow_source(&workflow.metadata.id)?;
    let source = source_record
        .as_ref()
        .map(|record| record.source.clone())
        .unwrap_or_else(|| serde_yaml::to_string(workflow).unwrap_or_default());
    let revision = source_record
        .as_ref()
        .map(|record| record.revision.as_str())
        .unwrap_or("ephemeral");
    let execution = prepare_execution_with_context(
        store,
        &source,
        revision,
        plugins,
        input_values,
        trigger_values,
    )?;
    execute_prepared_workflow_with_plugins(store, execution, plugins)
}

/// Validates and snapshots the exact source that will be executed. The caller
/// can return the execution ID immediately and run it in a background worker.
pub fn prepare_execution_with_plugins(
    store: &Store,
    source: &str,
    revision: &str,
    plugins: &PluginRegistry,
) -> Result<ExecutionRecord, String> {
    prepare_execution_with_context(
        store,
        source,
        revision,
        plugins,
        Value::Object(Map::new()),
        Value::Object(Map::new()),
    )
}

pub fn prepare_execution_with_context(
    store: &Store,
    source: &str,
    revision: &str,
    plugins: &PluginRegistry,
    input_values: Value,
    trigger_values: Value,
) -> Result<ExecutionRecord, String> {
    prepare_execution_with_context_request(
        store,
        source,
        revision,
        plugins,
        input_values,
        trigger_values,
        None,
    )
    .map(|(execution, _)| execution)
}

pub fn prepare_execution_with_context_request(
    store: &Store,
    source: &str,
    revision: &str,
    plugins: &PluginRegistry,
    input_values: Value,
    trigger_values: Value,
    identity: Option<(&str, &str)>,
) -> Result<(ExecutionRecord, bool), String> {
    if !input_values.is_object() || !trigger_values.is_object() {
        return Err("execution inputs and trigger values must be objects".to_string());
    }
    let mut compiled = planner::compile(source, plugins)
        .map_err(|diagnostics| planner::diagnostics_message(&diagnostics))?;
    validate_provider_profiles(store, &compiled.ir.nodes)?;
    compiled.plan.input_values = input_values;
    compiled.plan.trigger_values = trigger_values;
    let plan_value = serde_json::to_value(&compiled.plan)
        .map_err(|error| format!("cannot encode execution plan: {error}"))?;
    let execution = store.create_execution_with_plan_request(
        &compiled.ir.metadata.id,
        revision,
        &plan_value,
        identity,
    )?;
    Ok(execution)
}

/// Finishes a previously prepared execution. It is intentionally separate from
/// preparation so the HTTP API can make cancellation observable immediately.
pub fn execute_prepared_workflow_with_plugins(
    store: &Store,
    mut execution: ExecutionRecord,
    plugins: &PluginRegistry,
) -> Result<ExecutionRecord, String> {
    let plan: planner::ExecutionPlan = serde_json::from_value(
        store
            .execution_plan(&execution.id)?
            .ok_or_else(|| format!("execution {} has no persisted plan", execution.id))?,
    )
    .map_err(|error| format!("cannot decode persisted execution plan: {error}"))?;
    if execution.status == "queued" {
        loop {
            if store.claim_execution_slot(
                &execution.id,
                plan.policy
                    .concurrency
                    .as_ref()
                    .and_then(|policy| policy.max_runs),
            )? {
                execution.status = "running".to_string();
                break;
            }
            if store.execution_cancel_requested(&execution.id)? {
                store.finish_execution(&execution.id, "cancelled", None)?;
                execution.status = "cancelled".to_string();
                execution.completed_at = Some(timestamp()?);
                return Ok(execution);
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
    let mut control = RunControl::new(plan.policy.timeout.as_deref())?;
    if let Some(timeout) = plan.policy.timeout.as_deref() {
        control.deadline = Some(
            Instant::now()
                + store.execution_time_remaining(&execution.id, parse_duration(timeout)?)?,
        );
    }
    let outcome = execute_nodes(store, &execution.id, &plan, plugins, &control);
    match outcome {
        Ok(()) => {
            store.finish_execution(&execution.id, "succeeded", None)?;
            Ok(ExecutionRecord {
                status: "succeeded".to_string(),
                completed_at: Some(timestamp()?),
                ..execution
            })
        }
        Err(error) if error == "execution cancellation requested" => {
            store.finish_execution(&execution.id, "cancelled", None)?;
            Ok(ExecutionRecord {
                status: "cancelled".to_string(),
                completed_at: Some(timestamp()?),
                ..execution
            })
        }
        Err(error) if error == "execution timed out" => {
            store.finish_execution(&execution.id, "timed_out", Some(&error))?;
            Err(error)
        }
        Err(error) => {
            store.finish_execution(&execution.id, "failed", Some(&error))?;
            Err(error)
        }
    }
}

fn execute_nodes(
    store: &Store,
    execution_id: &str,
    plan: &planner::ExecutionPlan,
    plugins: &PluginRegistry,
    control: &RunControl,
) -> Result<(), String> {
    let context = ExecutionContext {
        store,
        execution_id,
        plugins,
        control,
        scope: "",
        active_span_id: None,
        max_parallel_nodes: plan.policy.max_parallel_nodes.unwrap_or(8) as usize,
        sensitive: plan
            .nodes
            .iter()
            .any(|planned| graph_uses_secret(std::slice::from_ref(&planned.node), plugins)),
        ai_budget_tokens: plan
            .policy
            .ai
            .as_ref()
            .and_then(|policy| policy.budget_tokens),
    };
    let node_outputs = execute_graph(
        &context,
        &plan
            .nodes
            .iter()
            .map(|planned| planned.node.clone())
            .collect::<Vec<_>>(),
        &plan.entry,
        BTreeMap::from([
            ("$input".to_string(), plan.input_values.clone()),
            ("$trigger".to_string(), plan.trigger_values.clone()),
        ]),
    )?;
    let mut outputs = resolve_inputs(store, &plan.outputs, &node_outputs, &BTreeMap::new())?;
    for (name, binding) in &plan.outputs {
        if binding_uses_secret(binding) {
            outputs.insert(name.clone(), Value::String("[redacted]".to_string()));
        }
    }
    store.set_execution_result(execution_id, &Value::Object(outputs.into_iter().collect()))?;
    Ok(())
}

/// Shared arguments threaded through every node execution step. Bundling them
/// keeps `execute_*` helpers within the lint budget and makes ownership of the
/// runtime state explicit at the call site.
struct ExecutionContext<'a> {
    store: &'a Store,
    execution_id: &'a str,
    plugins: &'a PluginRegistry,
    control: &'a RunControl,
    scope: &'a str,
    active_span_id: Option<&'a str>,
    ai_budget_tokens: Option<u64>,
    max_parallel_nodes: usize,
    sensitive: bool,
}

fn execute_graph(
    context: &ExecutionContext<'_>,
    nodes: &[WorkflowNode],
    entry: &str,
    mut locals: BTreeMap<String, Value>,
) -> Result<BTreeMap<String, BTreeMap<String, Value>>, String> {
    let local_control = context.control.fork();
    let context = &ExecutionContext {
        control: &local_control,
        ..*context
    };
    let mut settled = HashSet::new();
    let mut activated = HashSet::from([entry.to_owned()]);
    let mut outputs = BTreeMap::new();
    let predecessors: BTreeMap<_, Vec<_>> = nodes
        .iter()
        .map(|node| {
            (
                node.id.clone(),
                nodes
                    .iter()
                    .filter(|candidate| {
                        candidate
                            .on
                            .values()
                            .any(|targets| targets.iter().any(|id| id == node.id))
                    })
                    .map(|node| node.id.clone())
                    .collect(),
            )
        })
        .collect();
    let mut dependencies = BTreeMap::new();
    for node in nodes {
        let mut refs = HashSet::new();
        for binding in node.inputs.values() {
            binding_dependencies(binding, &mut refs);
        }
        dependencies.insert(node.id.clone(), refs);
    }
    while settled.len() < nodes.len() {
        context.control.check(context.store, context.execution_id)?;
        // Close unselected paths before testing joins. A closed path contributes
        // no activation, but it must never leave a join waiting forever.
        let mut closed = true;
        while closed {
            closed = false;
            for node in nodes {
                if !settled.contains(&node.id)
                    && !activated.contains(&node.id)
                    && predecessors[&node.id].iter().all(|id| settled.contains(id))
                {
                    settled.insert(node.id.clone());
                    closed = true;
                }
            }
        }
        let ready: Vec<_> = nodes
            .iter()
            .filter(|node| {
                !settled.contains(&node.id)
                    && activated.contains(&node.id)
                    && predecessors[&node.id].iter().all(|id| settled.contains(id))
                    && dependencies[&node.id].iter().all(|id| settled.contains(id))
            })
            .collect();
        if ready.is_empty() {
            if settled.len() == nodes.len() {
                break;
            }
            return Err(
                "graph cannot make progress: an input or control dependency is unavailable"
                    .to_string(),
            );
        }
        let mut writers = HashSet::new();
        for node in &ready {
            if node.node_type == "kakune.flow.set-variable@1" {
                let name = node
                    .with
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or("variable name is missing")?;
                if !writers.insert(name) {
                    return Err(format!(
                        "concurrent writes to variable {name} require an explicit reducer"
                    ));
                }
            }
        }
        for batch in ready.chunks(context.max_parallel_nodes) {
            let prepared = batch
                .iter()
                .map(|node| resolve_inputs(context.store, &node.inputs, &outputs, &locals))
                .collect::<Result<Vec<_>, _>>()?;
            let results = thread::scope(|scope| {
                let tasks: Vec<_> = batch
                    .iter()
                    .zip(&prepared)
                    .map(|(node, inputs)| {
                        let locals = &locals;
                        scope.spawn(move || {
                            let result = execute_node_with_retry(context, node, inputs, locals);
                            if result.is_err() {
                                context
                                    .control
                                    .abort
                                    .store(true, std::sync::atomic::Ordering::Release);
                            }
                            result
                        })
                    })
                    .collect();
                tasks
                    .into_iter()
                    .map(|task| {
                        task.join()
                            .unwrap_or_else(|_| Err("node worker panicked".to_string()))
                    })
                    .collect::<Vec<_>>()
            });
            let mut failure = None;
            for ((node, inputs), result) in batch.iter().zip(prepared).zip(results) {
                match result {
                    Ok(result) => {
                        let sensitive =
                            context.sensitive || node.inputs.values().any(binding_uses_secret);
                        context.store.record_node_outcome(
                            context.execution_id,
                            &scoped_node_id(context.scope, &node.id),
                            "succeeded",
                            if sensitive {
                                Some("sensitive result redacted")
                            } else {
                                result.message.as_deref()
                            },
                            Some(&node_result_value(&result, sensitive)),
                            None,
                        )?;
                        if let Some(targets) = node.on.get(&result.route) {
                            activated.extend(targets.iter().map(str::to_string));
                        }
                        outputs.insert(node.id.clone(), result.outputs);
                        settled.insert(node.id.clone());
                        if node.node_type == "kakune.flow.set-variable@1" {
                            let name = node
                                .with
                                .get("name")
                                .and_then(Value::as_str)
                                .expect("validated variable name");
                            locals.insert(
                                format!("${name}"),
                                inputs
                                    .get("value")
                                    .cloned()
                                    .expect("validated variable value"),
                            );
                        }
                    }
                    Err(error) => {
                        context.store.record_node_outcome(
                            context.execution_id,
                            &scoped_node_id(context.scope, &node.id),
                            "failed",
                            None,
                            None,
                            Some(&error),
                        )?;
                        if failure.is_none() || error != "sibling node failed" {
                            failure = Some(error);
                        }
                    }
                }
            }
            if let Some(error) = failure {
                return Err(error);
            }
        }
    }
    Ok(outputs)
}

fn binding_dependencies(binding: &WorkflowBinding, refs: &mut HashSet<String>) {
    fn expression(value: &Value, refs: &mut HashSet<String>) {
        if let Some(from) = value.get("from").and_then(Value::as_str) {
            reference(from, refs);
        }
        if let Some(expr) = value.get("expr") {
            expression(expr, refs);
        }
        if let Some(args) = value.get("args").and_then(Value::as_array) {
            for arg in args {
                expression(arg, refs);
            }
        }
    }
    fn reference(from: &str, refs: &mut HashSet<String>) {
        if !from.starts_with('$')
            && let Some((node, _)) = from.split_once('.')
        {
            refs.insert(node.to_string());
        }
    }
    match binding {
        WorkflowBinding::From { from } => reference(from, refs),
        WorkflowBinding::Expr { expr } => expression(expr, refs),
        _ => {}
    }
}

fn graph_uses_secret(nodes: &[WorkflowNode], plugins: &PluginRegistry) -> bool {
    nodes.iter().any(|node| {
        node.inputs.values().any(binding_uses_secret)
            || plugins
                .get(&node.node_type)
                .is_some_and(|plugin| plugin.uses_secrets())
            || node
                .body
                .as_ref()
                .is_some_and(|body| graph_uses_secret(&body.nodes, plugins))
            || node
                .branches
                .values()
                .any(|branch| graph_uses_secret(&branch.nodes, plugins))
    })
}

fn execute_node_with_retry(
    context: &ExecutionContext<'_>,
    node: &WorkflowNode,
    inputs: &BTreeMap<String, Value>,
    locals: &BTreeMap<String, Value>,
) -> Result<NodeResult, String> {
    let checkpoint_id = scoped_node_id(context.scope, &node.id);
    if let Some(saved) = context
        .store
        .checkpoint_result(context.execution_id, &checkpoint_id)?
    {
        return serde_json::from_value(saved)
            .map_err(|error| format!("invalid node checkpoint: {error}"));
    }
    let checkpoint_status = match node.node_type.as_str() {
        "kakune.flow.delay@1" => "waiting",
        "kakune.flow.foreach@1" | "kakune.flow.loop@1" | "kakune.flow.switch@1" => "container",
        _ => "running",
    };
    context
        .store
        .begin_checkpoint(context.execution_id, &checkpoint_id, checkpoint_status)?;
    let retry = node.retry.as_ref();
    let attempts = retry.map_or(1, |policy| policy.max_attempts);
    for attempt in 1..=attempts {
        let scoped_id = scoped_node_id(context.scope, &node.id);
        let span_id = context.store.start_trace_span(
            context.execution_id,
            context.active_span_id,
            ("node", &node.node_type),
            Some(&scoped_id),
            Some(attempt),
            &serde_json::json!({ "scope": context.scope }),
        )?;
        let span_context = ExecutionContext {
            active_span_id: Some(&span_id),
            ..*context
        };
        match execute_node(&span_context, node, inputs, locals) {
            Ok(result) => {
                let sensitive = context.sensitive || node.inputs.values().any(binding_uses_secret);
                let saved = if sensitive {
                    None
                } else {
                    Some(serde_json::to_value(&result).map_err(|error| error.to_string())?)
                };
                context.store.complete_checkpoint(
                    context.execution_id,
                    &checkpoint_id,
                    saved.as_ref(),
                )?;
                context
                    .store
                    .finish_trace_span(&span_id, "succeeded", None)?;
                return Ok(result);
            }
            Err(error)
                if attempt < attempts
                    && error != "execution cancellation requested"
                    && error != "execution timed out"
                    && error != "sibling node failed" =>
            {
                context
                    .store
                    .finish_trace_span(&span_id, "failed", Some(&error))?;
                let policy = retry.expect("retry policy exists when attempts exceed one");
                let exponent = attempt.saturating_sub(1).min(20);
                let base = policy.initial_backoff_ms.saturating_mul(1_u64 << exponent);
                let capped = policy
                    .max_backoff_ms
                    .map_or(base, |maximum| base.min(maximum));
                // Deterministic jitter keeps tests reproducible while still avoiding
                // synchronized retry bursts across sequential attempts.
                let jitter = policy
                    .jitter_ms
                    .map_or(0, |maximum| attempt as u64 % (maximum + 1));
                wait_with_control(
                    context.store,
                    context.execution_id,
                    Duration::from_millis(capped.saturating_add(jitter)),
                    context.control,
                )?;
            }
            Err(error) => {
                let status = if error == "execution cancellation requested"
                    || error == "execution timed out"
                {
                    "cancelled"
                } else {
                    "failed"
                };
                context
                    .store
                    .finish_trace_span(&span_id, status, Some(&error))?;
                return Err(error);
            }
        }
    }
    unreachable!("a retry loop always returns")
}

#[derive(serde::Serialize, serde::Deserialize)]
struct NodeResult {
    route: String,
    outputs: BTreeMap<String, Value>,
    message: Option<String>,
}

struct RunControl {
    deadline: Option<Instant>,
    abort: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ancestors: Vec<std::sync::Arc<std::sync::atomic::AtomicBool>>,
}

impl RunControl {
    fn new(timeout: Option<&str>) -> Result<Self, String> {
        Ok(Self {
            abort: Default::default(),
            ancestors: Vec::new(),
            deadline: timeout
                .map(parse_duration)
                .transpose()?
                .map(|duration| Instant::now() + duration),
        })
    }

    fn fork(&self) -> Self {
        let mut ancestors = self.ancestors.clone();
        ancestors.push(self.abort.clone());
        Self {
            deadline: self.deadline,
            abort: Default::default(),
            ancestors,
        }
    }

    fn check(&self, store: &Store, execution_id: &str) -> Result<(), String> {
        if self.abort.load(std::sync::atomic::Ordering::Acquire)
            || self
                .ancestors
                .iter()
                .any(|flag| flag.load(std::sync::atomic::Ordering::Acquire))
        {
            return Err("sibling node failed".to_string());
        }
        if store.execution_cancel_requested(execution_id)? {
            return Err("execution cancellation requested".to_string());
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err("execution timed out".to_string());
        }
        Ok(())
    }

    fn remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }
}

fn execute_node(
    context: &ExecutionContext<'_>,
    node: &WorkflowNode,
    inputs: &BTreeMap<String, Value>,
    locals: &BTreeMap<String, Value>,
) -> Result<NodeResult, String> {
    let ExecutionContext {
        store,
        execution_id,
        plugins,
        control,
        ..
    } = context;
    let mut outputs = BTreeMap::new();
    let result = match node.node_type.as_str() {
        "kakune.flow.end@1" => NodeResult {
            route: "success".to_string(),
            outputs,
            message: None,
        },
        "kakune.log@1" => {
            let message = input_string(inputs, "message")?.to_owned();
            NodeResult {
                route: "success".to_string(),
                outputs,
                message: Some(message),
            }
        }
        "kakune.flow.if@1" => NodeResult {
            route: if input_boolean(inputs, "condition")? {
                "true".to_string()
            } else {
                "false".to_string()
            },
            outputs,
            message: None,
        },
        "kakune.flow.switch@1" => execute_switch(context, node, inputs, locals)?,
        "kakune.flow.foreach@1" => execute_foreach(context, node, inputs, locals)?,
        "kakune.flow.loop@1" => execute_loop(context, node, inputs, locals)?,
        "kakune.flow.delay@1" => {
            let milliseconds = input_u64(inputs, "durationMs")?;
            let duration = store.durable_delay_remaining(
                execution_id,
                &scoped_node_id(context.scope, &node.id),
                Duration::from_millis(milliseconds),
            )?;
            wait_with_control(store, execution_id, duration, control)?;
            NodeResult {
                route: "success".to_string(),
                outputs,
                message: Some(format!("waited {milliseconds}ms")),
            }
        }
        "kakune.flow.join@1" | "kakune.flow.merge@1" => NodeResult {
            route: "success".to_string(),
            outputs: inputs.clone(),
            message: None,
        },
        "kakune.flow.set-variable@1" => NodeResult {
            route: "success".to_string(),
            outputs: BTreeMap::from([(
                "value".to_string(),
                inputs
                    .get("value")
                    .cloned()
                    .expect("validated variable value"),
            )]),
            message: None,
        },
        "kakune.flow.pass@1" => NodeResult {
            route: "success".to_string(),
            outputs: inputs.clone(),
            message: None,
        },
        "kakune.ai.codex.exec@1" => execute_codex(context, inputs, node)?,
        "kakune.ai.minimax.messages@1" => execute_minimax(context, inputs, node)?,
        "kakune.ai.generate@1" => execute_ai_generate(context, inputs, node)?,
        "kakune.ai.extract@1" => execute_ai_extract(context, inputs, node)?,
        "kakune.ai.boolean@1" => execute_ai_boolean(context, inputs, node)?,
        "kakune.ai.choose@1" => execute_ai_choose(context, inputs, node)?,
        "kakune.ai.agent@1" => execute_ai_agent(context, inputs, node)?,
        "kakune.ai.agent-result@1" => execute_ai_agent_result(inputs)?,
        "kakune.mcp.call@1" => execute_mcp(context, inputs, node)?,
        "kakune.fs.write-text@1" => {
            let relative_path = input_string(inputs, "path")?;
            let content = input_string(inputs, "content")?;
            let path = workspace_path(&store.workspace_dir()?, relative_path)?;
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .map_err(|error| format!("cannot create parent directory: {error}"))?;
            }
            fs::write(&path, content)
                .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
            outputs.insert("path".to_string(), Value::String(relative_path.to_owned()));
            NodeResult {
                route: "success".to_string(),
                outputs,
                message: Some(format!("wrote {relative_path}")),
            }
        }
        "kakune.fs.read-text@1" => {
            let relative_path = input_string(inputs, "path")?;
            let path = workspace_path(&store.workspace_dir()?, relative_path)?;
            let content = fs::read_to_string(&path)
                .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
            outputs.insert(
                "text".to_string(),
                inline_or_artifact(
                    store,
                    content.as_bytes(),
                    "text/plain; charset=utf-8",
                    Some(relative_path),
                )?,
            );
            NodeResult {
                route: "success".to_string(),
                outputs,
                message: Some(format!("read {relative_path}")),
            }
        }
        "kakune.fs.copy@1" => {
            let source = input_string(inputs, "source")?;
            let destination = input_string(inputs, "destination")?;
            let source_path = workspace_path(&store.workspace_dir()?, source)?;
            let destination_path = workspace_path(&store.workspace_dir()?, destination)?;
            if !source_path.is_file() {
                return Err(format!("source {source} must be an existing regular file"));
            }
            if let Some(parent) = destination_path.parent() {
                fs::create_dir_all(parent)
                    .map_err(|error| format!("cannot create destination parent: {error}"))?;
            }
            fs::copy(&source_path, &destination_path).map_err(|error| {
                format!(
                    "cannot copy {} to {}: {error}",
                    source_path.display(),
                    destination_path.display()
                )
            })?;
            outputs.insert("source".to_string(), Value::String(source.to_owned()));
            outputs.insert(
                "destination".to_string(),
                Value::String(destination.to_owned()),
            );
            NodeResult {
                route: "success".to_string(),
                outputs,
                message: Some(format!("copied {source} to {destination}")),
            }
        }
        "kakune.fs.move@1" => {
            let source = input_string(inputs, "source")?;
            let destination = input_string(inputs, "destination")?;
            let source_path = workspace_path(&store.workspace_dir()?, source)?;
            let destination_path = workspace_path(&store.workspace_dir()?, destination)?;
            if !source_path.is_file() {
                return Err(format!("source {source} must be an existing regular file"));
            }
            if let Some(parent) = destination_path.parent() {
                fs::create_dir_all(parent)
                    .map_err(|error| format!("cannot create destination parent: {error}"))?;
            }
            fs::rename(&source_path, &destination_path).map_err(|error| {
                format!(
                    "cannot move {} to {}: {error}",
                    source_path.display(),
                    destination_path.display()
                )
            })?;
            outputs.insert("source".to_string(), Value::String(source.to_owned()));
            outputs.insert(
                "destination".to_string(),
                Value::String(destination.to_owned()),
            );
            NodeResult {
                route: "success".to_string(),
                outputs,
                message: Some(format!("moved {source} to {destination}")),
            }
        }
        "kakune.http.request@1" => execute_http(store, execution_id, inputs, node, control)?,
        "kakune.process.run@1" => execute_process(store, execution_id, inputs, node, control)?,
        other => {
            let plugin = plugins
                .get(other)
                .ok_or_else(|| format!("node type {other} is not installed"))?;
            execute_plugin_node(plugin, store, execution_id, node, inputs)?
        }
    };
    Ok(result)
}

fn execute_switch(
    context: &ExecutionContext<'_>,
    node: &WorkflowNode,
    inputs: &BTreeMap<String, Value>,
    locals: &BTreeMap<String, Value>,
) -> Result<NodeResult, String> {
    let value = inputs
        .get("value")
        .ok_or_else(|| "input value is required".to_string())?;
    let selector = switch_selector(value)?;
    let branch_name = node
        .with
        .get("cases")
        .and_then(Value::as_object)
        .and_then(|cases| cases.get(&selector))
        .and_then(Value::as_str)
        .or_else(|| node.with.get("default").and_then(Value::as_str))
        .ok_or_else(|| format!("switch node {} has no branch for {selector}", node.id))?;
    let branch = node.branches.get(branch_name).ok_or_else(|| {
        format!(
            "switch node {} references unavailable branch {branch_name}",
            node.id
        )
    })?;
    let mut branch_locals = locals.clone();
    branch_locals.insert("$value".to_string(), value.clone());
    let child_scope = format!("{}/{}/{}", context.scope, node.id, branch_name);
    let child_context = ExecutionContext {
        scope: &child_scope,
        ..*context
    };
    let mut outputs = execute_subgraph(&child_context, branch, branch_locals)?;
    outputs.insert("branch".to_string(), Value::String(branch_name.to_string()));
    Ok(NodeResult {
        route: branch_name.to_string(),
        outputs,
        message: None,
    })
}

fn execute_foreach(
    context: &ExecutionContext<'_>,
    node: &WorkflowNode,
    inputs: &BTreeMap<String, Value>,
    locals: &BTreeMap<String, Value>,
) -> Result<NodeResult, String> {
    let items = inputs
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(|| "input items must be an array".to_string())?;
    let concurrency =
        node.with
            .get("maxConcurrency")
            .and_then(Value::as_u64)
            .ok_or_else(|| "foreach maxConcurrency is unavailable".to_string())? as usize;
    let on_error = node
        .with
        .get("onError")
        .and_then(Value::as_str)
        .unwrap_or("fail");
    let body = node
        .body
        .as_ref()
        .ok_or_else(|| format!("foreach node {} has no body", node.id))?;
    let mut results = Vec::with_capacity(items.len());

    for (chunk_index, chunk) in items.chunks(concurrency).enumerate() {
        let item_scopes: Vec<String> = chunk
            .iter()
            .enumerate()
            .map(|(offset, _)| {
                let index = chunk_index * concurrency + offset;
                format!("{}/{}/items/{index}", context.scope, node.id)
            })
            .collect();
        let chunk_results = thread::scope(|threads| {
            let mut tasks = Vec::with_capacity(chunk.len());
            for (offset, item) in chunk.iter().enumerate() {
                let index = chunk_index * concurrency + offset;
                let mut item_locals = locals.clone();
                item_locals.insert("$item".to_string(), item.clone());
                item_locals.insert("$index".to_string(), Value::from(index));
                let item_context = ExecutionContext {
                    scope: &item_scopes[offset],
                    ..*context
                };
                tasks.push(
                    threads.spawn(move || execute_subgraph(&item_context, body, item_locals)),
                );
            }
            tasks
                .into_iter()
                .map(|task| {
                    task.join()
                        .map_err(|_| "foreach worker panicked".to_string())
                })
                .collect::<Result<Vec<_>, _>>()
        })?;
        for (offset, outcome) in chunk_results.into_iter().enumerate() {
            let index = chunk_index * concurrency + offset;
            let item = items[index].clone();
            match outcome {
                Ok(outputs) => results.push(serde_json::json!({
                    "index": index,
                    "item": item,
                    "outputs": outputs,
                })),
                Err(error) if on_error == "continue" => results.push(serde_json::json!({
                    "index": index,
                    "item": item,
                    "error": error,
                })),
                Err(error) => return Err(format!("foreach item {index} failed: {error}")),
            }
        }
    }
    Ok(NodeResult {
        route: "success".to_string(),
        outputs: BTreeMap::from([("results".to_string(), Value::Array(results))]),
        message: None,
    })
}

fn execute_loop(
    context: &ExecutionContext<'_>,
    node: &WorkflowNode,
    inputs: &BTreeMap<String, Value>,
    locals: &BTreeMap<String, Value>,
) -> Result<NodeResult, String> {
    let max_iterations = node
        .with
        .get("maxIterations")
        .and_then(Value::as_u64)
        .ok_or_else(|| "loop maxIterations is unavailable".to_string())?;
    let body = node
        .body
        .as_ref()
        .ok_or_else(|| format!("loop node {} has no body", node.id))?;
    let mut state = inputs
        .get("state")
        .cloned()
        .ok_or_else(|| "input state is required".to_string())?;
    let mut last_outputs = BTreeMap::new();
    for iteration in 0..max_iterations {
        let mut iteration_locals = locals.clone();
        iteration_locals.insert("$state".to_string(), state.clone());
        iteration_locals.insert("$iteration".to_string(), Value::from(iteration));
        let iteration_scope = format!("{}/{}/iterations/{iteration}", context.scope, node.id);
        let iteration_context = ExecutionContext {
            scope: &iteration_scope,
            ..*context
        };
        last_outputs = execute_subgraph(&iteration_context, body, iteration_locals)?;
        state = last_outputs
            .get("state")
            .cloned()
            .ok_or_else(|| format!("loop node {} body did not produce state", node.id))?;
        let keep_going = last_outputs
            .get("continue")
            .and_then(Value::as_bool)
            .ok_or_else(|| {
                format!(
                    "loop node {} body output continue must be a boolean",
                    node.id
                )
            })?;
        if !keep_going {
            last_outputs.insert("iterations".to_string(), Value::from(iteration + 1));
            last_outputs.insert("state".to_string(), state);
            last_outputs.insert("completed".to_string(), Value::Bool(true));
            return Ok(NodeResult {
                route: "success".to_string(),
                outputs: last_outputs,
                message: None,
            });
        }
    }
    last_outputs.insert("iterations".to_string(), Value::from(max_iterations));
    last_outputs.insert("state".to_string(), state);
    last_outputs.insert("completed".to_string(), Value::Bool(false));
    Ok(NodeResult {
        route: "maxIterations".to_string(),
        outputs: last_outputs,
        message: Some(format!("stopped after {max_iterations} iterations")),
    })
}

fn execute_subgraph(
    context: &ExecutionContext<'_>,
    graph: &WorkflowSubgraph,
    locals: BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>, String> {
    let store = context.store;
    let node_outputs = execute_graph(context, &graph.nodes, &graph.entry, locals.clone())?;
    resolve_inputs(store, &graph.outputs, &node_outputs, &locals)
}

fn switch_selector(value: &Value) -> Result<String, String> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Bool(_) | Value::Number(_) | Value::Null => Ok(value.to_string()),
        Value::Array(_) | Value::Object(_) => {
            Err("switch input value must be a string, number, boolean, or null".to_string())
        }
    }
}

fn scoped_node_id(scope: &str, node_id: &str) -> String {
    if scope.is_empty() {
        node_id.to_string()
    } else {
        format!("{scope}/{node_id}")
    }
}

fn execute_plugin_node(
    plugin: &RegisteredPluginNode,
    store: &Store,
    execution_id: &str,
    node: &WorkflowNode,
    inputs: &BTreeMap<String, Value>,
) -> Result<NodeResult, String> {
    let plugin = plugin.clone();
    let execution_id = execution_id.to_string();
    let node_id = node.id.clone();
    let inputs = inputs.clone();
    let store = store.clone();
    let task = std::thread::Builder::new()
        .name("kakune-plugin-node".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .map_err(|error| format!("cannot start plugin runtime: {error}"))?;
            runtime
                .block_on(plugin.execute(store, &execution_id, &node_id, inputs))
                .map_err(|error| match error {
                    crate::plugin_process::PluginNodeExecutionError::Cancelled => {
                        "execution cancellation requested".to_string()
                    }
                    error => error.to_string(),
                })
        })
        .map_err(|error| format!("cannot start plugin node task: {error}"))?;
    let result = task
        .join()
        .map_err(|_| "plugin node task panicked".to_string())??;
    Ok(NodeResult {
        route: result.route,
        outputs: result.outputs,
        message: result.message,
    })
}

fn node_result_value(result: &NodeResult, sensitive: bool) -> Value {
    serde_json::json!({
        "route": result.route,
        "outputs": if sensitive { Value::String("[redacted]".to_string()) } else { serde_json::to_value(&result.outputs).unwrap_or(Value::Null) },
        "message": if sensitive { Some("sensitive result redacted") } else { result.message.as_deref() },
    })
}

fn binding_uses_secret(binding: &WorkflowBinding) -> bool {
    match binding {
        WorkflowBinding::Secret { .. } => true,
        WorkflowBinding::Expr { expr } => expression_uses_secret(expr),
        WorkflowBinding::Literal { .. } | WorkflowBinding::From { .. } => false,
    }
}

fn expression_uses_secret(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(expression_uses_secret),
        Value::Object(values) => {
            values.contains_key("secret") || values.values().any(expression_uses_secret)
        }
        _ => false,
    }
}

pub(crate) fn is_builtin_node_type(node_type: &str) -> bool {
    matches!(
        node_type,
        "kakune.flow.end@1"
            | "kakune.log@1"
            | "kakune.flow.if@1"
            | "kakune.flow.switch@1"
            | "kakune.flow.foreach@1"
            | "kakune.flow.loop@1"
            | "kakune.flow.delay@1"
            | "kakune.flow.join@1"
            | "kakune.flow.merge@1"
            | "kakune.flow.set-variable@1"
            | "kakune.flow.pass@1"
            | "kakune.fs.write-text@1"
            | "kakune.fs.read-text@1"
            | "kakune.fs.copy@1"
            | "kakune.fs.move@1"
            | "kakune.http.request@1"
            | "kakune.process.run@1"
            | "kakune.ai.codex.exec@1"
            | "kakune.ai.minimax.messages@1"
            | "kakune.ai.generate@1"
            | "kakune.ai.extract@1"
            | "kakune.ai.boolean@1"
            | "kakune.ai.choose@1"
            | "kakune.ai.agent@1"
            | "kakune.ai.agent-result@1"
            | "kakune.mcp.call@1"
    )
}

const MAX_HTTP_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const INLINE_RESULT_BYTES: usize = 64 * 1024;
const MAX_PROCESS_OUTPUT_BYTES: usize = 2 * 1024 * 1024;

fn execute_http(
    store: &Store,
    execution_id: &str,
    inputs: &BTreeMap<String, Value>,
    node: &WorkflowNode,
    control: &RunControl,
) -> Result<NodeResult, String> {
    control.check(store, execution_id)?;
    let url = input_string(inputs, "url")?.to_string();
    let method = node
        .with
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or("GET")
        .to_string();
    let method_label = method.clone();
    let timeout_seconds = node
        .with
        .get("timeoutSeconds")
        .and_then(Value::as_u64)
        .unwrap_or(30);
    if !(1..=120).contains(&timeout_seconds) {
        return Err("HTTP timeoutSeconds must be from 1 to 120".to_string());
    }
    let headers = inputs
        .get("headers")
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()));
    let body = inputs.get("body").cloned();
    let timeout = control
        .remaining()
        .map_or(Duration::from_secs(timeout_seconds), |remaining| {
            remaining.min(Duration::from_secs(timeout_seconds))
        });
    if timeout.is_zero() {
        return Err("execution timed out".to_string());
    }
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let result = (|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("cannot start HTTP runtime: {error}"))?;
            runtime.block_on(send_http_request(url, method, timeout, headers, body))
        })();
        let _ = sender.send(result);
    });
    let response = loop {
        match receiver.recv_timeout(Duration::from_millis(20)) {
            Ok(result) => break result?,
            Err(mpsc::RecvTimeoutError::Timeout) => control.check(store, execution_id)?,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err("HTTP worker panicked".to_string());
            }
        }
    };
    let body = match serde_json::to_vec(&response.body) {
        Ok(bytes) if bytes.len() > INLINE_RESULT_BYTES => inline_or_artifact(
            store,
            &bytes,
            response
                .content_type
                .as_deref()
                .unwrap_or("application/json"),
            Some("http-response"),
        )?,
        _ => response.body,
    };
    let mut outputs = BTreeMap::from([
        ("status".to_string(), Value::from(response.status)),
        ("headers".to_string(), Value::Object(response.headers)),
        ("body".to_string(), body),
    ]);
    if let Some(content_type) = response.content_type {
        outputs.insert("contentType".to_string(), Value::String(content_type));
    }
    let route = if (200..300).contains(&response.status) {
        "success"
    } else {
        "httpError"
    };
    Ok(NodeResult {
        route: route.to_string(),
        outputs,
        message: Some(format!(
            "HTTP {} returned {}",
            method_label, response.status
        )),
    })
}

struct HttpResponse {
    status: u16,
    headers: Map<String, Value>,
    content_type: Option<String>,
    body: Value,
}

async fn send_http_request(
    url: String,
    method: String,
    timeout: Duration,
    raw_headers: Value,
    body: Option<Value>,
) -> Result<HttpResponse, String> {
    let parsed_url =
        Url::parse(&url).map_err(|_| "HTTP url must be an absolute URL".to_string())?;
    if !matches!(parsed_url.scheme(), "http" | "https") || parsed_url.host_str().is_none() {
        return Err("HTTP url must use http or https and include a host".to_string());
    }
    if !parsed_url.username().is_empty() || parsed_url.password().is_some() {
        return Err("HTTP url must not embed credentials".to_string());
    }
    let method =
        Method::from_bytes(method.as_bytes()).map_err(|_| "HTTP method is invalid".to_string())?;
    let headers = http_headers(raw_headers)?;
    let client = Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|error| format!("cannot create HTTP client: {error}"))?;
    let mut request = client.request(method, parsed_url).headers(headers);
    if let Some(body) = body {
        request = if body.is_object() || body.is_array() {
            request.json(&body)
        } else if let Some(text) = body.as_str() {
            request.body(text.to_string())
        } else {
            request.body(body.to_string())
        };
    }
    let response = request
        .send()
        .await
        .map_err(|error| format!("HTTP request failed: {error}"))?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_HTTP_RESPONSE_BYTES as u64)
    {
        return Err(format!(
            "HTTP response exceeds the {} byte limit",
            MAX_HTTP_RESPONSE_BYTES
        ));
    }
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let mut headers = Map::new();
    for (name, value) in response.headers() {
        if let Ok(value) = value.to_str() {
            headers.insert(name.as_str().to_string(), Value::String(value.to_string()));
        }
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| format!("cannot read HTTP response: {error}"))?;
        if bytes.len().saturating_add(chunk.len()) > MAX_HTTP_RESPONSE_BYTES {
            return Err(format!(
                "HTTP response exceeds the {} byte limit",
                MAX_HTTP_RESPONSE_BYTES
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let body = if content_type
        .as_deref()
        .is_some_and(|value| value.contains("json"))
    {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
    } else {
        Value::String(String::from_utf8_lossy(&bytes).into_owned())
    };
    Ok(HttpResponse {
        status,
        headers,
        content_type,
        body,
    })
}

fn http_headers(value: Value) -> Result<HeaderMap, String> {
    let values = value
        .as_object()
        .ok_or_else(|| "HTTP headers must be an object of strings".to_string())?;
    let mut headers = HeaderMap::new();
    for (name, value) in values {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| format!("HTTP header name {name} is invalid"))?;
        let value = value
            .as_str()
            .ok_or_else(|| "HTTP headers must be an object of strings".to_string())?;
        let value =
            HeaderValue::from_str(value).map_err(|_| "HTTP header value is invalid".to_string())?;
        headers.append(name, value);
    }
    Ok(headers)
}

fn inline_or_artifact(
    store: &Store,
    bytes: &[u8],
    media_type: &str,
    name: Option<&str>,
) -> Result<Value, String> {
    if bytes.len() <= INLINE_RESULT_BYTES {
        return String::from_utf8(bytes.to_vec())
            .map(Value::String)
            .map_err(|_| "inline text result is not valid UTF-8".to_string());
    }
    serde_json::to_value(store.put_artifact(bytes, media_type, name)?)
        .map_err(|error| format!("cannot serialize artifact reference: {error}"))
}

fn execute_process(
    store: &Store,
    execution_id: &str,
    inputs: &BTreeMap<String, Value>,
    node: &WorkflowNode,
    control: &RunControl,
) -> Result<NodeResult, String> {
    let command = node
        .with
        .get("command")
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty()
                && !Path::new(value).is_absolute()
                && !value.contains('/')
                && !value.contains('\\')
        })
        .ok_or_else(|| {
            "process with.command must be a non-empty executable name without a path".to_string()
        })?;
    let args = inputs
        .get("args")
        .map(|value| {
            serde_json::from_value::<Vec<String>>(value.clone())
                .map_err(|_| "process input args must be an array of strings".to_string())
        })
        .transpose()?
        .unwrap_or_default();
    let timeout_seconds = node
        .with
        .get("timeoutSeconds")
        .and_then(Value::as_u64)
        .unwrap_or(30);
    if !(1..=3_600).contains(&timeout_seconds) {
        return Err("process timeoutSeconds must be from 1 to 3600".to_string());
    }
    let mut process_command = Command::new(command);
    process_command
        .args(args)
        .current_dir(store.workspace_dir()?)
        .stdin(if inputs.contains_key("stdin") {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut process = crate::process_supervisor::spawn_sync(process_command)
        .map_err(|error| format!("cannot start process {command}: {error}"))?;
    let stdin_writer = if let Some(stdin) = inputs.get("stdin") {
        use std::io::Write;
        let text = stdin
            .as_str()
            .ok_or_else(|| "process input stdin must be a string".to_string())?
            .to_string();
        let mut pipe = process
            .stdin()
            .take()
            .expect("stdin is piped when supplied");
        Some(thread::spawn(move || pipe.write_all(text.as_bytes())))
    } else {
        None
    };
    let stdout = process.stdout().take().expect("stdout is piped");
    let stderr = process.stderr().take().expect("stderr is piped");
    let stdout_reader = thread::spawn(move || read_process_output(stdout));
    let stderr_reader = thread::spawn(move || read_process_output(stderr));
    let deadline = Instant::now()
        + control
            .remaining()
            .map_or(Duration::from_secs(timeout_seconds), |remaining| {
                remaining.min(Duration::from_secs(timeout_seconds))
            });
    let status = loop {
        if let Err(error) = control.check(store, execution_id) {
            break Err(error);
        }
        if Instant::now() >= deadline {
            break Err("process timed out".to_string());
        }
        match process.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) => thread::sleep(Duration::from_millis(20)),
            Err(error) => break Err(format!("cannot wait for process: {error}")),
        }
    };
    // A descendant can retain pipes after its parent exits. Close the complete
    // task group before joining the pipe workers, including on error paths.
    let _ = process.kill();
    let _ = process.wait();
    let stdin_result = stdin_writer
        .map(|worker| {
            worker
                .join()
                .map_err(|_| "process stdin writer panicked".to_string())
                .and_then(|result| {
                    result.map_err(|error| format!("cannot write process stdin: {error}"))
                })
        })
        .transpose();
    let (stdout, stdout_limited) = stdout_reader
        .join()
        .map_err(|_| "process stdout reader panicked".to_string())?;
    let (stderr, stderr_limited) = stderr_reader
        .join()
        .map_err(|_| "process stderr reader panicked".to_string())?;
    let status = status?;
    stdin_result?;
    if stdout_limited || stderr_limited {
        return Err(format!(
            "process output exceeds the {MAX_PROCESS_OUTPUT_BYTES} byte limit"
        ));
    }
    let stdout = inline_or_artifact(
        store,
        &stdout,
        "text/plain; charset=utf-8",
        Some("process-stdout"),
    )?;
    let stderr = inline_or_artifact(
        store,
        &stderr,
        "text/plain; charset=utf-8",
        Some("process-stderr"),
    )?;
    let exit_code = status.code().unwrap_or(-1);
    Ok(NodeResult {
        route: if status.success() {
            "success"
        } else {
            "exitError"
        }
        .to_string(),
        outputs: BTreeMap::from([
            ("stdout".to_string(), stdout),
            ("stderr".to_string(), stderr),
            ("exitCode".to_string(), Value::from(exit_code)),
        ]),
        message: Some(format!("process {command} exited with {exit_code}")),
    })
}

fn read_process_output(mut reader: impl Read) -> (Vec<u8>, bool) {
    let mut kept = Vec::new();
    let mut limited = false;
    let mut chunk = [0_u8; 8_192];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) | Err(_) => return (kept, limited),
            Ok(length) => {
                let remaining = MAX_PROCESS_OUTPUT_BYTES.saturating_sub(kept.len());
                let copied = remaining.min(length);
                kept.extend_from_slice(&chunk[..copied]);
                limited |= copied < length;
            }
        }
    }
}

fn validate_provider_profiles(store: &Store, nodes: &[WorkflowNode]) -> Result<(), String> {
    for node in nodes {
        if matches!(
            node.node_type.as_str(),
            "kakune.ai.generate@1"
                | "kakune.ai.extract@1"
                | "kakune.ai.boolean@1"
                | "kakune.ai.choose@1"
                | "kakune.ai.agent@1"
                | "kakune.ai.minimax.messages@1"
                | "kakune.ai.codex.exec@1"
        ) {
            let expected = if node.node_type == "kakune.ai.codex.exec@1" {
                ProviderType::Codex
            } else {
                ProviderType::MiniMax
            };
            let expected = if node.node_type != "kakune.ai.codex.exec@1"
                && node.node_type != "kakune.ai.minimax.messages@1"
            {
                let id = node
                    .with
                    .get("provider")
                    .and_then(Value::as_str)
                    .ok_or("with.provider is required")?;
                store
                    .get_provider_profile(id)?
                    .ok_or("provider profile was not found")?
                    .provider_type
            } else {
                expected
            };
            let _ = profile_for_node(store, node, expected)?;
        }
        for branch in node.branches.values() {
            validate_provider_profiles(store, &branch.nodes)?;
        }
        if let Some(body) = &node.body {
            validate_provider_profiles(store, &body.nodes)?;
        }
    }
    Ok(())
}

fn selected_model(node: &WorkflowNode) -> Result<&str, String> {
    node.with
        .get("model")
        .and_then(Value::as_str)
        .filter(|model| !model.trim().is_empty())
        .ok_or_else(|| format!("node {} requires a non-empty with.model", node.id))
}

fn profile_for_node(
    store: &Store,
    node: &WorkflowNode,
    expected_type: ProviderType,
) -> Result<ProviderProfile, String> {
    let profile_id = node
        .with
        .get("provider")
        .and_then(Value::as_str)
        .filter(|provider| !provider.trim().is_empty())
        .ok_or_else(|| format!("node {} requires a non-empty with.provider", node.id))?;
    let profile = store
        .get_provider_profile(profile_id)?
        .ok_or_else(|| format!("provider profile {profile_id} was not found"))?;
    if profile.provider_type != expected_type {
        return Err(format!(
            "provider profile {profile_id} has type {:?}, which cannot execute {}",
            profile.provider_type, node.node_type
        ));
    }
    let model = selected_model(node)?;
    if !profile
        .allowed_models
        .iter()
        .any(|allowed| allowed == model)
    {
        return Err(format!(
            "model {model} is not permitted by provider profile {profile_id}"
        ));
    }
    if profile.diagnostic.status == ProviderProfileStatus::Unavailable {
        return Err(format!(
            "provider profile {profile_id} is unavailable: {}",
            profile
                .diagnostic
                .message
                .as_deref()
                .unwrap_or("run provider diagnose after correcting its configuration")
        ));
    }
    Ok(profile)
}

fn execute_codex(
    context: &ExecutionContext<'_>,
    inputs: &BTreeMap<String, Value>,
    node: &WorkflowNode,
) -> Result<NodeResult, String> {
    let prompt = input_string(inputs, "prompt")?;
    call_codex(
        context,
        node,
        serde_json::json!({"input":[{"role":"user","content":prompt}]}),
    )
}

fn call_codex(
    context: &ExecutionContext<'_>,
    node: &WorkflowNode,
    request: Value,
) -> Result<NodeResult, String> {
    use crate::codex::{CodexCancellation, CodexClient, CodexOAuthTokens};

    let profile = profile_for_node(context.store, node, ProviderType::Codex)?;
    let timeout_seconds = profile
        .config
        .get("timeoutSeconds")
        .and_then(Value::as_u64)
        .unwrap_or(300);
    if timeout_seconds == 0 || timeout_seconds > 3_600 {
        return Err("Codex timeoutSeconds must be from 1 to 3600".to_string());
    }
    let secret_ref = match &profile.auth {
        ProviderAuth::OAuthSecret { secret_ref } => secret_ref.clone(),
        _ => return Err("Codex profiles require a ChatGPT OAuth secret reference".to_string()),
    };
    let tokens = CodexOAuthTokens::from_secret(&context.store.resolve_secret(&secret_ref)?)
        .map_err(|error| error.to_string())?;
    let model = selected_model(node)?.to_string();
    let provider_span_id = context.store.start_trace_span(
        context.execution_id,
        context.active_span_id,
        ("provider", "codex"),
        Some(&scoped_node_id(context.scope, &node.id)),
        None,
        &serde_json::json!({ "providerId": profile.id, "modelRequested": &model }),
    )?;
    let cancellation = CodexCancellation::new();
    let worker_cancellation = cancellation.clone();
    let (sender, receiver) = mpsc::sync_channel(1);
    let worker = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("cannot start Codex runtime: {error}"))?;
        let result = runtime.block_on(async move {
            CodexClient::new(model, Duration::from_secs(timeout_seconds), tokens)
                .map_err(|error| error.to_string())?
                .run_request(request, &worker_cancellation)
                .await
                .map_err(|error| error.to_string())
        });
        sender
            .send(result)
            .map_err(|_| "Codex response receiver closed".to_string())
    });
    let result = loop {
        match receiver.recv_timeout(Duration::from_millis(25)) {
            Ok(result) => break result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Err(error) = context.control.check(context.store, context.execution_id) {
                    cancellation.cancel();
                    let _ = worker
                        .join()
                        .map_err(|_| "Codex worker panicked".to_string())?;
                    return Err(error);
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = worker
                    .join()
                    .map_err(|_| "Codex worker panicked".to_string())?;
                return Err("Codex worker ended without a response".to_string());
            }
        }
    };
    let _ = worker
        .join()
        .map_err(|_| "Codex worker panicked".to_string())?;
    let result = result?;
    if let Some(tokens) = result.refreshed_tokens.as_ref() {
        context.store.set_secret(
            &secret_ref,
            &tokens.to_secret().map_err(|error| error.to_string())?,
        )?;
    }
    context.control.check(context.store, context.execution_id)?;
    let text = Value::String(result.text);
    let usage: Vec<Value> = result
        .usage
        .into_iter()
        .map(|usage| {
            serde_json::json!({
                "inputTokens": usage.input_tokens,
                "cachedInputTokens": usage.cached_input_tokens,
                "outputTokens": usage.output_tokens,
            })
        })
        .collect();
    let usage_value = (!usage.is_empty()).then(|| Value::Array(usage.clone()));
    let raw = result.raw;
    context
        .store
        .record_provider_invocation(crate::store::ProviderInvocation {
            execution_id: context.execution_id,
            node_id: &scoped_node_id(context.scope, &node.id),
            provider: &profile.id,
            model_requested: selected_model(node)?,
            model_reported: raw.get("model").and_then(Value::as_str),
            usage: usage_value.as_ref(),
            raw: &raw,
            span_id: Some(&provider_span_id),
        })?;
    context
        .store
        .finish_trace_span(&provider_span_id, "succeeded", None)?;
    Ok(NodeResult {
        route: "success".to_string(),
        outputs: BTreeMap::from([
            ("output".to_string(), text),
            ("usage".to_string(), Value::Array(usage)),
            ("events".to_string(), raw.clone()),
            ("provider".to_string(), Value::String(profile.id)),
            (
                "modelRequested".to_string(),
                Value::String(selected_model(node)?.to_string()),
            ),
        ]),
        message: Some("Codex task completed".to_string()),
    })
}

fn execute_minimax(
    context: &ExecutionContext<'_>,
    inputs: &BTreeMap<String, Value>,
    node: &WorkflowNode,
) -> Result<NodeResult, String> {
    use crate::minimax::Message;
    let messages: Vec<Message> = serde_json::from_value(
        inputs
            .get("messages")
            .cloned()
            .ok_or_else(|| "input messages is required".to_string())?,
    )
    .map_err(|error| format!("input messages is not a valid MiniMax message list: {error}"))?;
    let response = call_model(context, inputs, node, messages, None, None)?;
    let usage = response_usage(&response);
    let mut outputs = BTreeMap::from([
        ("content".to_string(), Value::Array(response.content)),
        ("raw".to_string(), response.raw),
        (
            "modelRequested".to_string(),
            Value::String(selected_model(node)?.to_string()),
        ),
    ]);
    if let Some(model) = &response.model {
        outputs.insert("modelReported".to_string(), Value::String(model.clone()));
    }
    if let Some(usage) = usage {
        outputs.insert("usage".to_string(), usage);
    }
    Ok(NodeResult {
        route: "success".to_string(),
        outputs,
        message: Some("MiniMax request completed".to_string()),
    })
}

fn execute_ai_generate(
    context: &ExecutionContext<'_>,
    inputs: &BTreeMap<String, Value>,
    node: &WorkflowNode,
) -> Result<NodeResult, String> {
    let prompt = input_string(inputs, "prompt")?;
    let response = call_model(
        context,
        inputs,
        node,
        minimax_user_messages(prompt),
        None,
        None,
    )?;
    let text = response_text(&response.content)?;
    let mut outputs = BTreeMap::from([
        ("text".to_string(), Value::String(text)),
        (
            "modelRequested".to_string(),
            Value::String(selected_model(node)?.to_string()),
        ),
    ]);
    if let Some(model) = &response.model {
        outputs.insert("modelReported".to_string(), Value::String(model.clone()));
    }
    if let Some(usage) = response_usage(&response) {
        outputs.insert("usage".to_string(), usage);
    }
    Ok(NodeResult {
        route: "success".to_string(),
        outputs,
        message: Some("MiniMax generation completed".to_string()),
    })
}

fn execute_ai_extract(
    context: &ExecutionContext<'_>,
    inputs: &BTreeMap<String, Value>,
    node: &WorkflowNode,
) -> Result<NodeResult, String> {
    let schema = required_object(inputs, "schema")?;
    let tool = structured_output_tool("submit_result", &schema);
    let response = call_model(
        context,
        inputs,
        node,
        minimax_user_messages(input_string(inputs, "prompt")?),
        Some(vec![tool]),
        Some(serde_json::json!({"type":"tool","name":"submit_result"})),
    )?;
    let value = structured_value(&response.content)?;
    validate_schema(&schema, &value)?;
    let mut outputs = BTreeMap::from([("value".to_string(), value)]);
    if let Some(usage) = response_usage(&response) {
        outputs.insert("usage".to_string(), usage);
    }
    Ok(NodeResult {
        route: "success".to_string(),
        outputs,
        message: Some("MiniMax extraction completed".to_string()),
    })
}

fn execute_ai_boolean(
    context: &ExecutionContext<'_>,
    inputs: &BTreeMap<String, Value>,
    node: &WorkflowNode,
) -> Result<NodeResult, String> {
    let schema = serde_json::json!({"type":"object","required":["value"],"additionalProperties":false,"properties":{"value":{"type":"boolean"}}})
        .as_object().cloned().expect("literal schema is an object");
    let response = call_model(
        context,
        inputs,
        node,
        minimax_user_messages(input_string(inputs, "prompt")?),
        Some(vec![structured_output_tool("submit_result", &schema)]),
        Some(serde_json::json!({"type":"tool","name":"submit_result"})),
    )?;
    let value = structured_value(&response.content)?;
    validate_schema(&schema, &value)?;
    let value = value
        .get("value")
        .and_then(Value::as_bool)
        .ok_or_else(|| "AI boolean result has no boolean value".to_string())?;
    let mut outputs = BTreeMap::from([("value".to_string(), Value::Bool(value))]);
    if let Some(usage) = response_usage(&response) {
        outputs.insert("usage".to_string(), usage);
    }
    Ok(NodeResult {
        route: if value { "true" } else { "false" }.to_string(),
        outputs,
        message: Some("AI boolean completed".to_string()),
    })
}

fn execute_ai_choose(
    context: &ExecutionContext<'_>,
    inputs: &BTreeMap<String, Value>,
    node: &WorkflowNode,
) -> Result<NodeResult, String> {
    let choices = inputs
        .get("choices")
        .and_then(Value::as_array)
        .ok_or_else(|| "input choices must be an array".to_string())?;
    let choices = choices
        .iter()
        .map(Value::as_str)
        .collect::<Option<Vec<_>>>()
        .filter(|values| !values.is_empty() && values.iter().all(|value| valid_route(value)))
        .ok_or_else(|| "input choices must be non-empty stable route identifiers".to_string())?;
    let schema = serde_json::json!({"type":"object","required":["choice"],"additionalProperties":false,"properties":{"choice":{"type":"string","enum":choices}}})
        .as_object().cloned().expect("literal schema is an object");
    let response = call_model(
        context,
        inputs,
        node,
        minimax_user_messages(input_string(inputs, "prompt")?),
        Some(vec![structured_output_tool("submit_result", &schema)]),
        Some(serde_json::json!({"type":"tool","name":"submit_result"})),
    )?;
    let value = structured_value(&response.content)?;
    validate_schema(&schema, &value)?;
    let choice = value
        .get("choice")
        .and_then(Value::as_str)
        .ok_or_else(|| "AI choice result has no choice".to_string())?
        .to_string();
    let mut outputs = BTreeMap::from([("choice".to_string(), Value::String(choice.clone()))]);
    if let Some(usage) = response_usage(&response) {
        outputs.insert("usage".to_string(), usage);
    }
    Ok(NodeResult {
        route: choice,
        outputs,
        message: Some("AI choice completed".to_string()),
    })
}

fn execute_ai_agent(
    context: &ExecutionContext<'_>,
    inputs: &BTreeMap<String, Value>,
    node: &WorkflowNode,
) -> Result<NodeResult, String> {
    use crate::minimax::{Message, MessageContent, MessageRole};
    let schema = required_object(inputs, "schema")?;
    let max_model_calls = bounded_with(node, "maxModelCalls", 4, 1, 20)?;
    let max_tool_calls = bounded_with(node, "maxToolCalls", 8, 0, 64)?;
    let max_result_bytes = bounded_with(node, "maxResultBytes", 65_536, 1, 1_048_576)? as usize;
    let tools = inputs
        .get("tools")
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let tools = tools
        .as_array()
        .ok_or_else(|| "input tools must be an array".to_string())?
        .clone();
    for tool in &tools {
        validate_agent_tool(tool)?;
    }
    let mut model_tools = tools.clone();
    model_tools.push(structured_output_tool("submit_result", &schema));
    let mut messages = minimax_user_messages(input_string(inputs, "task")?);
    let mut steps = Vec::new();
    let mut tool_calls = 0_u64;
    for round in 1..=max_model_calls {
        context.control.check(context.store, context.execution_id)?;
        let response = call_model(
            context,
            inputs,
            node,
            messages.clone(),
            Some(model_tools.clone()),
            None,
        )?;
        let calls = tool_calls_from(&response.content)?;
        if let Some(final_call) = calls.iter().find(|call| call.name == "submit_result") {
            if calls.len() != 1 {
                return Err(
                    "agent must return its final result without additional tool calls".to_string(),
                );
            }
            validate_schema(&schema, &final_call.input)?;
            let encoded =
                serde_json::to_vec(&final_call.input).map_err(|error| error.to_string())?;
            if encoded.len() > max_result_bytes {
                return Err("agent final result exceeds maxResultBytes".to_string());
            }
            let mut outputs = BTreeMap::from([
                ("result".to_string(), final_call.input.clone()),
                ("steps".to_string(), Value::Array(steps)),
            ]);
            if let Some(usage) = response_usage(&response) {
                outputs.insert("usage".to_string(), usage);
            }
            return Ok(NodeResult {
                route: "success".to_string(),
                outputs,
                message: Some(format!("AI agent completed in {round} model calls")),
            });
        }
        if calls.is_empty() {
            let value = structured_value(&response.content)?;
            validate_schema(&schema, &value)?;
            let encoded = serde_json::to_vec(&value).map_err(|error| error.to_string())?;
            if encoded.len() > max_result_bytes {
                return Err("agent final result exceeds maxResultBytes".to_string());
            }
            let mut outputs = BTreeMap::from([
                ("result".to_string(), value),
                ("steps".to_string(), Value::Array(steps)),
            ]);
            if let Some(usage) = response_usage(&response) {
                outputs.insert("usage".to_string(), usage);
            }
            return Ok(NodeResult {
                route: "success".to_string(),
                outputs,
                message: Some(format!("AI agent completed in {round} model calls")),
            });
        }
        tool_calls = tool_calls.saturating_add(calls.len() as u64);
        if tool_calls > max_tool_calls {
            return Err("agent maximum tool calls exceeded".to_string());
        }
        messages.push(Message {
            role: MessageRole::Assistant,
            content: MessageContent::Blocks(response.content),
        });
        for call in calls {
            let result = execute_agent_tool_with_context(context, &tools, &call.name, &call.input)?;
            let result_bytes = serde_json::to_vec(&result).map_err(|error| error.to_string())?;
            if result_bytes.len() > max_result_bytes {
                return Err("agent tool result exceeds maxResultBytes".to_string());
            }
            steps.push(serde_json::json!({"round":round,"tool":call.name,"input":call.input,"result":result}));
            messages.push(Message { role: MessageRole::User, content: MessageContent::Blocks(vec![serde_json::json!({"type":"tool_result","tool_use_id":call.id,"content":serde_json::to_string(&result).map_err(|error| error.to_string())?})]) });
        }
    }
    Err("agent maximum model calls exceeded without a valid structured result".to_string())
}

fn execute_ai_agent_result(inputs: &BTreeMap<String, Value>) -> Result<NodeResult, String> {
    let value = inputs
        .get("value")
        .cloned()
        .ok_or_else(|| "input value is required".to_string())?;
    validate_schema(&required_object(inputs, "schema")?, &value)?;
    Ok(NodeResult {
        route: "success".to_string(),
        outputs: BTreeMap::from([("result".to_string(), value)]),
        message: Some("agent result validated".to_string()),
    })
}

fn call_model(
    context: &ExecutionContext<'_>,
    inputs: &BTreeMap<String, Value>,
    node: &WorkflowNode,
    messages: Vec<crate::minimax::Message>,
    tools: Option<Vec<Value>>,
    tool_choice: Option<Value>,
) -> Result<crate::minimax::MessagesResponse, String> {
    let profile_id = node
        .with
        .get("provider")
        .and_then(Value::as_str)
        .ok_or("with.provider is required")?;
    if context
        .store
        .get_provider_profile(profile_id)?
        .is_some_and(|profile| profile.provider_type == ProviderType::Codex)
    {
        let max_tokens = input_u64(inputs, "maxTokens")?;
        context.store.reserve_provider_tokens(
            context.execution_id,
            profile_id,
            max_tokens,
            context.ai_budget_tokens,
        )?;
        let request = crate::codex_adapter::request(messages, tools, tool_choice)?;
        let result = call_codex(context, node, request)?;
        return crate::codex_adapter::response(
            result
                .outputs
                .get("events")
                .cloned()
                .ok_or("Codex response missing")?,
        );
    }
    use crate::minimax::{MessagesRequest, MiniMaxClient, MiniMaxConfig};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    context.control.check(context.store, context.execution_id)?;
    let profile = profile_for_node(context.store, node, ProviderType::MiniMax)?;
    let subscription_key = match &profile.auth {
        ProviderAuth::ApiKeySecret { secret_ref } => context.store.resolve_secret(secret_ref)?,
        ProviderAuth::OAuthSecret { .. } => {
            return Err("MiniMax profiles require an API-key secret reference".to_string());
        }
    };
    let model = selected_model(node)?.to_string();
    let max_tokens = input_u64(inputs, "maxTokens")?;
    let max_tokens = u32::try_from(max_tokens)
        .map_err(|_| "input maxTokens must fit in an unsigned 32-bit integer".to_string())?;
    context.store.reserve_provider_tokens(
        context.execution_id,
        &profile.id,
        u64::from(max_tokens),
        context.ai_budget_tokens,
    )?;
    let config = match profile.config.get("baseUrl").and_then(Value::as_str) {
        Some(base_url) => MiniMaxConfig::default().with_base_url(base_url),
        None => MiniMaxConfig::default(),
    };
    let requested_model = model.clone();
    let provider_span_id = context.store.start_trace_span(
        context.execution_id,
        context.active_span_id,
        ("provider", "minimax"),
        Some(&scoped_node_id(context.scope, &node.id)),
        None,
        &serde_json::json!({ "providerId": profile.id, "modelRequested": &requested_model }),
    )?;
    let mut request = MessagesRequest::new(model, max_tokens, messages);
    request.tools = tools;
    request.tool_choice = tool_choice;
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker_cancelled = cancelled.clone();
    let (sender, receiver) = mpsc::sync_channel(1);
    let worker = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("cannot start MiniMax runtime: {error}"))?;
        let result = runtime.block_on(async move {
            MiniMaxClient::with_config(subscription_key, config)
                .map_err(|error| error.to_string())?
                .messages_cancellable(&request, worker_cancelled)
                .await
                .map_err(|error| error.to_string())
        });
        sender
            .send(result)
            .map_err(|_| "MiniMax response receiver closed".to_string())
    });
    let response = loop {
        match receiver.recv_timeout(Duration::from_millis(25)) {
            Ok(response) => break response,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Err(error) = context.control.check(context.store, context.execution_id) {
                    cancelled.store(true, Ordering::Release);
                    let _ = worker
                        .join()
                        .map_err(|_| "MiniMax worker panicked".to_string())?;
                    return Err(error);
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = worker
                    .join()
                    .map_err(|_| "MiniMax worker panicked".to_string())?;
                return Err("MiniMax worker ended without a response".to_string());
            }
        }
    };
    let _ = worker
        .join()
        .map_err(|_| "MiniMax worker panicked".to_string())?;
    let response = response?;
    context.control.check(context.store, context.execution_id)?;
    let usage = response_usage(&response);
    context
        .store
        .record_provider_invocation(crate::store::ProviderInvocation {
            execution_id: context.execution_id,
            node_id: &scoped_node_id(context.scope, &node.id),
            provider: &profile.id,
            model_requested: &requested_model,
            model_reported: response.model.as_deref(),
            usage: usage.as_ref(),
            raw: &response.raw,
            span_id: Some(&provider_span_id),
        })?;
    context
        .store
        .finish_trace_span(&provider_span_id, "succeeded", None)?;
    Ok(response)
}

fn minimax_user_messages(prompt: &str) -> Vec<crate::minimax::Message> {
    vec![crate::minimax::Message {
        role: crate::minimax::MessageRole::User,
        content: crate::minimax::MessageContent::Text(prompt.to_string()),
    }]
}

fn response_usage(response: &crate::minimax::MessagesResponse) -> Option<Value> {
    response.usage.as_ref().map(|usage| {
        serde_json::json!({
            "inputTokens": usage.input_tokens,
            "outputTokens": usage.output_tokens,
            "totalTokens": usage.total_tokens,
            "cacheCreationInputTokens": usage.cache_creation_input_tokens,
            "cacheReadInputTokens": usage.cache_read_input_tokens,
        })
    })
}

fn response_text(content: &[Value]) -> Result<String, String> {
    let text = content
        .iter()
        .filter_map(|block| {
            (block.get("type").and_then(Value::as_str) == Some("text"))
                .then(|| block.get("text").and_then(Value::as_str))
                .flatten()
        })
        .collect::<String>();
    if text.is_empty() {
        Err("MiniMax response has no text content".to_string())
    } else {
        Ok(text)
    }
}

fn structured_output_tool(name: &str, schema: &serde_json::Map<String, Value>) -> Value {
    serde_json::json!({"name":name,"description":"Return the final structured result exactly once.","input_schema":schema})
}

fn structured_value(content: &[Value]) -> Result<Value, String> {
    if let Some(input) = content.iter().find_map(|block| {
        (block.get("type").and_then(Value::as_str) == Some("tool_use"))
            .then(|| block.get("input"))
            .flatten()
            .cloned()
    }) {
        return Ok(input);
    }
    let text = response_text(content)?;
    serde_json::from_str(&text)
        .map_err(|error| format!("MiniMax structured result is not JSON: {error}"))
}

fn validate_schema(schema: &serde_json::Map<String, Value>, value: &Value) -> Result<(), String> {
    let schema = Value::Object(schema.clone());
    let validator = jsonschema::validator_for(&schema)
        .map_err(|error| format!("invalid output schema: {error}"))?;
    if let Some(error) = validator.iter_errors(value).next() {
        return Err(format!(
            "MiniMax structured result does not match schema: {error}"
        ));
    }
    Ok(())
}

fn required_object(
    inputs: &BTreeMap<String, Value>,
    name: &str,
) -> Result<serde_json::Map<String, Value>, String> {
    inputs
        .get(name)
        .and_then(Value::as_object)
        .cloned()
        .ok_or_else(|| format!("input {name} must be an object"))
}

fn bounded_with(
    node: &WorkflowNode,
    name: &str,
    default: u64,
    minimum: u64,
    maximum: u64,
) -> Result<u64, String> {
    let value = node.with.get(name).map_or(Some(default), Value::as_u64);
    value
        .filter(|value| (minimum..=maximum).contains(value))
        .ok_or_else(|| format!("{name} must be an integer from {minimum} to {maximum}"))
}

fn valid_route(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value.starts_with(|character: char| character.is_ascii_lowercase())
        && value.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
}

struct AgentToolCall {
    id: String,
    name: String,
    input: Value,
}

fn tool_calls_from(content: &[Value]) -> Result<Vec<AgentToolCall>, String> {
    content
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
        .map(|block| {
            Ok(AgentToolCall {
                id: block
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| "agent tool call has no id".to_string())?
                    .to_string(),
                name: block
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| "agent tool call has no name".to_string())?
                    .to_string(),
                input: block
                    .get("input")
                    .cloned()
                    .unwrap_or(Value::Object(Default::default())),
            })
        })
        .collect()
}

fn validate_agent_tool(tool: &Value) -> Result<(), String> {
    let object = tool
        .as_object()
        .ok_or_else(|| "agent tools must be objects".to_string())?;
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .filter(|value| valid_route(value))
        .ok_or_else(|| "agent tool name must be a stable identifier".to_string())?;
    if object
        .get("input_schema")
        .and_then(Value::as_object)
        .is_none()
    {
        return Err(format!("agent tool {name} requires an input_schema"));
    }
    match object.get("type").and_then(Value::as_str) {
        Some("echo") => {}
        Some("mcp") if object.get("server").and_then(Value::as_object).is_some() => {}
        _ => {
            return Err(format!(
                "agent tool {name} must use the supported echo type or an MCP server configuration"
            ));
        }
    }
    Ok(())
}

fn execute_agent_tool(tools: &[Value], name: &str, input: &Value) -> Result<Value, String> {
    let tool = tools
        .iter()
        .find(|tool| tool.get("name").and_then(Value::as_str) == Some(name))
        .ok_or_else(|| format!("agent attempted unpermitted tool {name}"))?;
    validate_agent_tool(tool)?;
    let schema = tool
        .get("input_schema")
        .and_then(Value::as_object)
        .expect("validated tool schema");
    validate_schema(schema, input)?;
    match tool.get("type").and_then(Value::as_str) {
        Some("echo") => Ok(serde_json::json!({"echo":input})),
        Some("mcp") => Err(format!(
            "agent MCP tool {name} requires an execution context"
        )),
        _ => Err(format!("agent tool {name} is not supported")),
    }
}

fn execute_agent_tool_with_context(
    context: &ExecutionContext<'_>,
    tools: &[Value],
    name: &str,
    input: &Value,
) -> Result<Value, String> {
    let span_id = context.store.start_trace_span(
        context.execution_id,
        context.active_span_id,
        ("tool", name),
        None,
        None,
        &serde_json::json!({ "tool": name }),
    )?;
    let result = (|| {
        let tool = tools
            .iter()
            .find(|tool| tool.get("name").and_then(Value::as_str) == Some(name))
            .ok_or_else(|| format!("agent attempted unpermitted tool {name}"))?;
        if tool.get("type").and_then(Value::as_str) == Some("mcp") {
            validate_agent_tool(tool)?;
            let schema = tool
                .get("input_schema")
                .and_then(Value::as_object)
                .expect("validated tool schema");
            validate_schema(schema, input)?;
            execute_agent_mcp_tool(context, tool, name, input)
        } else {
            execute_agent_tool(tools, name, input)
        }
    })();
    match &result {
        Ok(_) => context
            .store
            .finish_trace_span(&span_id, "succeeded", None)?,
        Err(error) => context
            .store
            .finish_trace_span(&span_id, "failed", Some(error))?,
    }
    result
}

fn execute_agent_mcp_tool(
    context: &ExecutionContext<'_>,
    tool: &Value,
    name: &str,
    input: &Value,
) -> Result<Value, String> {
    use crate::mcp::{
        McpClient, McpClientOptions, McpHttpServerConfig, McpStdioServerConfig, McpToolContent,
    };

    let server = tool
        .get("server")
        .and_then(Value::as_object)
        .ok_or_else(|| format!("MCP agent tool {name} has no server configuration"))?;
    let transport = server
        .get("transport")
        .and_then(Value::as_str)
        .unwrap_or("stdio")
        .to_string();
    let remote_name = tool
        .get("remoteName")
        .and_then(Value::as_str)
        .unwrap_or(name)
        .to_string();
    let arguments = input
        .as_object()
        .cloned()
        .ok_or_else(|| format!("MCP agent tool {name} input must be an object"))?;
    let env = resolve_mcp_secret_environment(context.store, server)?;
    let bearer = server
        .get("bearerSecretRef")
        .and_then(Value::as_str)
        .map(|secret| context.store.resolve_secret(secret))
        .transpose()?;
    let command = server
        .get("command")
        .and_then(Value::as_str)
        .map(str::to_string);
    let args = server
        .get("args")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| format!("MCP agent tool {name} server.args must be strings"))?
        .unwrap_or_default();
    let endpoint = server
        .get("endpoint")
        .and_then(Value::as_str)
        .map(str::to_string);
    let result = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("cannot start MCP runtime: {error}"))?;
        runtime.block_on(async move {
            let mut client = match transport.as_str() {
                "stdio" => {
                    McpClient::connect(
                        McpStdioServerConfig {
                            command: command
                                .ok_or_else(|| "MCP stdio tool has no command".to_string())?,
                            args,
                            current_dir: None,
                            env,
                        },
                        McpClientOptions::default(),
                    )
                    .await
                }
                "http" => {
                    McpClient::connect_http(
                        McpHttpServerConfig {
                            endpoint: endpoint
                                .ok_or_else(|| "MCP HTTP tool has no endpoint".to_string())?,
                            bearer_token: bearer,
                        },
                        McpClientOptions::default(),
                    )
                    .await
                }
                _ => return Err("MCP tool transport is unsupported".to_string()),
            }
            .map_err(|_| "MCP tool connection failed".to_string())?;
            let output = client
                .call_tool(&remote_name, arguments)
                .await
                .map_err(|_| "MCP tool call failed".to_string());
            let _ = client.cancel().await;
            output
        })
    })
    .join()
    .map_err(|_| "MCP agent worker panicked".to_string())??;
    let content: Vec<Value> = result
        .content
        .into_iter()
        .map(|item| match item {
            McpToolContent::Text { text } => serde_json::json!({"type":"text","text":text}),
            McpToolContent::Image { data, mime_type } => serde_json::json!({"type":"image","data":data,"mimeType":mime_type}),
            McpToolContent::Audio { data, mime_type } => serde_json::json!({"type":"audio","data":data,"mimeType":mime_type}),
            McpToolContent::ResourceLink { uri, name, description, mime_type, size } => serde_json::json!({"type":"resource_link","uri":uri,"name":name,"description":description,"mimeType":mime_type,"size":size}),
            McpToolContent::EmbeddedTextResource { uri, mime_type, text } => serde_json::json!({"type":"resource","uri":uri,"mimeType":mime_type,"text":text}),
            McpToolContent::EmbeddedBlobResource { uri, mime_type, blob } => serde_json::json!({"type":"resource","uri":uri,"mimeType":mime_type,"blob":blob}),
        })
        .collect();
    Ok(serde_json::json!({
        "content": content,
        "structuredContent": result.structured_content,
        "isError": result.is_error,
    }))
}

fn resolve_mcp_secret_environment(
    store: &Store,
    configuration: &Map<String, Value>,
) -> Result<BTreeMap<String, String>, String> {
    let Some(references) = configuration.get("environmentSecretRefs") else {
        return Ok(BTreeMap::new());
    };
    let references = references
        .as_object()
        .ok_or_else(|| "MCP environmentSecretRefs must be an object".to_string())?;
    references
        .iter()
        .map(|(name, secret)| {
            if name.is_empty() || name.contains('=') || name.contains('\0') {
                return Err("MCP environment variable name is invalid".to_string());
            }
            let secret = secret
                .as_str()
                .ok_or_else(|| "MCP environment secret reference must be a string".to_string())?;
            Ok((name.clone(), store.resolve_secret(secret)?))
        })
        .collect()
}

fn execute_mcp(
    context: &ExecutionContext<'_>,
    inputs: &BTreeMap<String, Value>,
    node: &WorkflowNode,
) -> Result<NodeResult, String> {
    use crate::mcp::{
        McpClient, McpClientOptions, McpHttpServerConfig, McpStdioServerConfig, McpToolContent,
    };

    let transport = node
        .with
        .get("transport")
        .and_then(Value::as_str)
        .unwrap_or("stdio")
        .to_string();
    let env = inputs
        .get("environment")
        .map(|value| {
            value
                .as_object()
                .ok_or_else(|| "input environment must be an object of strings".to_string())?
                .iter()
                .map(|(key, value)| {
                    value
                        .as_str()
                        .map(|value| (key.clone(), value.to_string()))
                        .ok_or_else(|| "input environment must be an object of strings".to_string())
                })
                .collect::<Result<BTreeMap<_, _>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    let tool = input_string(inputs, "tool")?.to_string();
    let arguments = inputs
        .get("arguments")
        .and_then(Value::as_object)
        .cloned()
        .ok_or_else(|| "input arguments must be an object".to_string())?;
    let http_bearer = node
        .with
        .get("bearerSecretRef")
        .and_then(Value::as_str)
        .map(|name| context.store.resolve_secret(name))
        .transpose()?;
    let command = node
        .with
        .get("command")
        .and_then(Value::as_str)
        .map(str::to_string);
    let args: Vec<String> = node
        .with
        .get("args")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| "MCP node with.args must be an array of strings".to_string())?
        .unwrap_or_default();
    let endpoint = node
        .with
        .get("endpoint")
        .and_then(Value::as_str)
        .map(str::to_string);
    let result = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("cannot start MCP runtime: {error}"))?;
        runtime.block_on(async move {
            let mut client = match transport.as_str() {
                "stdio" => {
                    McpClient::connect(
                        McpStdioServerConfig {
                            command: command.ok_or_else(|| {
                                "MCP stdio node with.command is required".to_string()
                            })?,
                            args,
                            current_dir: None,
                            env,
                        },
                        McpClientOptions::default(),
                    )
                    .await
                }
                "http" => {
                    McpClient::connect_http(
                        McpHttpServerConfig {
                            endpoint: endpoint.ok_or_else(|| {
                                "MCP HTTP node with.endpoint is required".to_string()
                            })?,
                            bearer_token: http_bearer,
                        },
                        McpClientOptions::default(),
                    )
                    .await
                }
                other => return Err(format!("unsupported MCP transport {other}")),
            }
            .map_err(|error| error.to_string())?;
            let result = client
                .call_tool(&tool, arguments)
                .await
                .map_err(|error| error.to_string());
            let _ = client.cancel().await;
            result
        })
    })
    .join()
    .map_err(|_| "MCP worker panicked".to_string())??;
    let content = result
        .content
        .into_iter()
        .map(|item| match item {
            McpToolContent::Text { text } => serde_json::json!({ "type": "text", "text": text }),
            McpToolContent::Image { data, mime_type } => serde_json::json!({ "type": "image", "data": data, "mimeType": mime_type }),
            McpToolContent::Audio { data, mime_type } => serde_json::json!({ "type": "audio", "data": data, "mimeType": mime_type }),
            McpToolContent::ResourceLink { uri, name, description, mime_type, size } => serde_json::json!({ "type": "resource_link", "uri": uri, "name": name, "description": description, "mimeType": mime_type, "size": size }),
            McpToolContent::EmbeddedTextResource { uri, mime_type, text } => serde_json::json!({ "type": "resource", "uri": uri, "mimeType": mime_type, "text": text }),
            McpToolContent::EmbeddedBlobResource { uri, mime_type, blob } => serde_json::json!({ "type": "resource", "uri": uri, "mimeType": mime_type, "blob": blob }),
        })
        .collect();
    let mut outputs = BTreeMap::from([
        ("content".to_string(), Value::Array(content)),
        ("isError".to_string(), Value::Bool(result.is_error)),
    ]);
    if let Some(structured) = result.structured_content {
        outputs.insert("structuredContent".to_string(), structured);
    }
    Ok(NodeResult {
        route: "success".to_string(),
        outputs,
        message: Some("MCP tool call completed".to_string()),
    })
}

fn resolve_inputs(
    store: &Store,
    bindings: &BTreeMap<String, WorkflowBinding>,
    outputs: &BTreeMap<String, BTreeMap<String, Value>>,
    locals: &BTreeMap<String, Value>,
) -> Result<BTreeMap<String, Value>, String> {
    bindings
        .iter()
        .map(|(name, binding)| {
            Ok((
                name.clone(),
                resolve_binding(store, binding, outputs, locals)?,
            ))
        })
        .collect()
}

fn resolve_binding(
    store: &Store,
    binding: &WorkflowBinding,
    outputs: &BTreeMap<String, BTreeMap<String, Value>>,
    locals: &BTreeMap<String, Value>,
) -> Result<Value, String> {
    match binding {
        WorkflowBinding::Literal { literal } => Ok(literal.clone()),
        WorkflowBinding::From { from } => {
            if from.starts_with('$') {
                return resolve_local(locals, from);
            }
            let (node_id, output_name) = from
                .split_once('.')
                .ok_or_else(|| format!("binding source {from} must use node.output"))?;
            outputs
                .get(node_id)
                .and_then(|node_outputs| node_outputs.get(output_name))
                .cloned()
                .ok_or_else(|| format!("binding source {from} is not available"))
        }
        WorkflowBinding::Expr { expr } => evaluate_expression(store, expr, outputs, locals),
        WorkflowBinding::Secret { secret } => store.resolve_secret(secret).map(Value::String),
    }
}

fn resolve_local(locals: &BTreeMap<String, Value>, from: &str) -> Result<Value, String> {
    if let Some(value) = locals.get(from) {
        return Ok(value.clone());
    }
    let Some((root, path)) = from.split_once('.') else {
        return Err(format!("local binding source {from} is not available"));
    };
    let mut value = locals
        .get(root)
        .ok_or_else(|| format!("local binding source {from} is not available"))?;
    for segment in path.split('.') {
        value = value
            .as_object()
            .and_then(|object| object.get(segment))
            .ok_or_else(|| format!("local binding source {from} is not available"))?;
    }
    Ok(value.clone())
}

/// Evaluates the deliberately small, data-only expression language used by workflow
/// bindings.  Expressions never evaluate source code or access the operating system:
/// their operands are bindings and their result is a JSON value.
fn evaluate_expression(
    store: &Store,
    expression: &Value,
    outputs: &BTreeMap<String, BTreeMap<String, Value>>,
    locals: &BTreeMap<String, Value>,
) -> Result<Value, String> {
    let object = expression
        .as_object()
        .ok_or_else(|| "expression must be an object".to_string())?;
    let operation = object
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| "expression.op must be a string".to_string())?;
    let raw_args = object
        .get("args")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("expression {operation} requires an args array"))?;
    let args = raw_args
        .iter()
        .map(|argument| evaluate_expression_argument(store, argument, outputs, locals))
        .collect::<Result<Vec<_>, _>>()?;

    match operation {
        "equal" => binary_compare(&args, operation, |left, right| left == right),
        "notEqual" => binary_compare(&args, operation, |left, right| left != right),
        "greaterThan" => ordered_compare(&args, operation, |left, right| left > right),
        "greaterThanOrEqual" => ordered_compare(&args, operation, |left, right| left >= right),
        "lessThan" => ordered_compare(&args, operation, |left, right| left < right),
        "lessThanOrEqual" => ordered_compare(&args, operation, |left, right| left <= right),
        "and" => Ok(Value::Bool(
            args.iter()
                .map(boolean_argument)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .all(|value| value),
        )),
        "or" => Ok(Value::Bool(
            args.iter()
                .map(boolean_argument)
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .any(|value| value),
        )),
        "not" => Ok(Value::Bool(!boolean_argument(exactly_one(
            &args, operation,
        )?)?)),
        "add" => numeric_fold(&args, operation, 0.0, |left, right| left + right),
        "subtract" => numeric_subtract(&args, operation),
        "multiply" => numeric_fold(&args, operation, 1.0, |left, right| left * right),
        "divide" => {
            require_arity(&args, operation, 2)?;
            let divisor = number_argument(&args[1], operation)?;
            if divisor == 0.0 {
                return Err("expression divide cannot divide by zero".to_string());
            }
            number_value(number_argument(&args[0], operation)? / divisor)
        }
        "modulo" => {
            require_arity(&args, operation, 2)?;
            let divisor = number_argument(&args[1], operation)?;
            if divisor == 0.0 {
                return Err("expression modulo cannot divide by zero".to_string());
            }
            number_value(number_argument(&args[0], operation)? % divisor)
        }
        "concat" => Ok(Value::String(
            args.iter()
                .map(value_to_text)
                .collect::<Result<Vec<_>, _>>()?
                .join(""),
        )),
        "array" => Ok(Value::Array(args)),
        "object" => expression_object(&args),
        "get" => {
            require_arity(&args, operation, 2)?;
            let key = args[1]
                .as_str()
                .ok_or_else(|| "expression get requires a string property name".to_string())?;
            match &args[0] {
                Value::Object(value) => Ok(value.get(key).cloned().unwrap_or(Value::Null)),
                Value::Array(value) => key
                    .parse::<usize>()
                    .ok()
                    .and_then(|index| value.get(index))
                    .cloned()
                    .ok_or_else(|| format!("expression get cannot find array index {key}")),
                _ => Err("expression get requires an object or array".to_string()),
            }
        }
        "coalesce" => Ok(args
            .into_iter()
            .find(|value| !value.is_null())
            .unwrap_or(Value::Null)),
        "contains" => {
            require_arity(&args, operation, 2)?;
            let result =
                match &args[0] {
                    Value::String(value) => value.contains(args[1].as_str().ok_or_else(|| {
                        "expression contains requires a string needle".to_string()
                    })?),
                    Value::Array(values) => values.contains(&args[1]),
                    Value::Object(values) => args[1]
                        .as_str()
                        .map(|key| values.contains_key(key))
                        .unwrap_or(false),
                    _ => {
                        return Err(
                            "expression contains requires a string, array, or object".to_string()
                        );
                    }
                };
            Ok(Value::Bool(result))
        }
        other => Err(format!("unsupported expression operation {other}")),
    }
}

fn evaluate_expression_argument(
    store: &Store,
    argument: &Value,
    outputs: &BTreeMap<String, BTreeMap<String, Value>>,
    locals: &BTreeMap<String, Value>,
) -> Result<Value, String> {
    let Some(object) = argument.as_object() else {
        return Ok(argument.clone());
    };
    if object.len() != 1 {
        return Ok(argument.clone());
    }
    if let Some(literal) = object.get("literal") {
        return Ok(literal.clone());
    }
    if let Some(from) = object.get("from").and_then(Value::as_str) {
        return resolve_binding(
            store,
            &WorkflowBinding::From {
                from: from.to_string(),
            },
            outputs,
            locals,
        );
    }
    if let Some(secret) = object.get("secret").and_then(Value::as_str) {
        return resolve_binding(
            store,
            &WorkflowBinding::Secret {
                secret: secret.to_string(),
            },
            outputs,
            locals,
        );
    }
    if let Some(expr) = object.get("expr") {
        return evaluate_expression(store, expr, outputs, locals);
    }
    Ok(argument.clone())
}

fn require_arity(args: &[Value], operation: &str, expected: usize) -> Result<(), String> {
    if args.len() == expected {
        Ok(())
    } else {
        Err(format!(
            "expression {operation} requires exactly {expected} arguments"
        ))
    }
}

fn exactly_one<'a>(args: &'a [Value], operation: &str) -> Result<&'a Value, String> {
    require_arity(args, operation, 1)?;
    Ok(&args[0])
}

fn binary_compare(
    args: &[Value],
    operation: &str,
    predicate: impl FnOnce(&Value, &Value) -> bool,
) -> Result<Value, String> {
    require_arity(args, operation, 2)?;
    Ok(Value::Bool(predicate(&args[0], &args[1])))
}

fn ordered_compare(
    args: &[Value],
    operation: &str,
    predicate: impl FnOnce(f64, f64) -> bool,
) -> Result<Value, String> {
    require_arity(args, operation, 2)?;
    Ok(Value::Bool(predicate(
        number_argument(&args[0], operation)?,
        number_argument(&args[1], operation)?,
    )))
}

fn numeric_fold(
    args: &[Value],
    operation: &str,
    initial: f64,
    operation_fn: impl Fn(f64, f64) -> f64,
) -> Result<Value, String> {
    if args.is_empty() {
        return Err(format!(
            "expression {operation} requires at least one argument"
        ));
    }
    let value = args.iter().try_fold(initial, |total, value| {
        Ok::<_, String>(operation_fn(total, number_argument(value, operation)?))
    })?;
    number_value(value)
}

fn numeric_subtract(args: &[Value], operation: &str) -> Result<Value, String> {
    if args.is_empty() {
        return Err(format!(
            "expression {operation} requires at least one argument"
        ));
    }
    let first = number_argument(&args[0], operation)?;
    let value = args[1..].iter().try_fold(first, |total, value| {
        Ok::<_, String>(total - number_argument(value, operation)?)
    })?;
    number_value(value)
}

fn number_argument(value: &Value, operation: &str) -> Result<f64, String> {
    value
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| format!("expression {operation} requires numeric arguments"))
}

fn number_value(value: f64) -> Result<Value, String> {
    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        return Ok(Value::Number(Number::from(value as i64)));
    }
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| "expression result is not a finite number".to_string())
}

fn boolean_argument(value: &Value) -> Result<bool, String> {
    value
        .as_bool()
        .ok_or_else(|| "expression boolean operations require boolean arguments".to_string())
}

fn value_to_text(value: &Value) -> Result<String, String> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Null => Ok("null".to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err("expression concat accepts only scalar values".to_string()),
    }
}

fn expression_object(args: &[Value]) -> Result<Value, String> {
    if !args.len().is_multiple_of(2) {
        return Err("expression object requires an even number of key/value arguments".to_string());
    }
    let mut value = Map::new();
    let (pairs, remainder) = args.as_chunks::<2>();
    debug_assert!(remainder.is_empty());
    for [key_value, member_value] in pairs {
        let key = key_value
            .as_str()
            .ok_or_else(|| "expression object keys must be strings".to_string())?;
        value.insert(key.to_string(), member_value.clone());
    }
    Ok(Value::Object(value))
}

fn input_string<'a>(inputs: &'a BTreeMap<String, Value>, name: &str) -> Result<&'a str, String> {
    inputs
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("input {name} must be a string"))
}

fn input_boolean(inputs: &BTreeMap<String, Value>, name: &str) -> Result<bool, String> {
    match inputs.get(name) {
        Some(Value::Bool(value)) => Ok(*value),
        Some(Value::String(value)) if value == "true" => Ok(true),
        Some(Value::String(value)) if value == "false" => Ok(false),
        _ => Err(format!("input {name} must be a boolean")),
    }
}

fn input_u64(inputs: &BTreeMap<String, Value>, name: &str) -> Result<u64, String> {
    match inputs.get(name) {
        Some(Value::Number(value)) => value
            .as_u64()
            .ok_or_else(|| format!("input {name} must be a non-negative integer")),
        Some(Value::String(value)) => value
            .parse()
            .map_err(|_| format!("input {name} must be a non-negative integer")),
        _ => Err(format!("input {name} must be a non-negative integer")),
    }
}

fn wait_with_control(
    store: &Store,
    execution_id: &str,
    duration: Duration,
    control: &RunControl,
) -> Result<(), String> {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        control.check(store, execution_id)?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        thread::sleep(remaining.min(Duration::from_millis(20)));
    }
    control.check(store, execution_id)
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let value = value.trim();
    let (number, suffix) = value
        .chars()
        .position(|character| !character.is_ascii_digit())
        .map(|index| value.split_at(index))
        .ok_or_else(|| "workflow policy.timeout must use a unit such as 30s or 2m".to_string())?;
    let amount = number
        .parse::<u64>()
        .ok()
        .filter(|amount| *amount > 0)
        .ok_or_else(|| "workflow policy.timeout must be a positive duration".to_string())?;
    let multiplier = match suffix {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        _ => return Err("workflow policy.timeout supports ms, s, m, or h".to_string()),
    };
    amount
        .checked_mul(multiplier)
        .map(Duration::from_millis)
        .ok_or_else(|| "workflow policy.timeout is too large".to_string())
}

fn workspace_path(root: &Path, input: &str) -> Result<std::path::PathBuf, String> {
    let candidate = Path::new(input);
    if candidate.is_absolute()
        || candidate.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err("path must be relative to the Kakune workspace".to_string());
    }
    let root = fs::canonicalize(root)
        .map_err(|error| format!("cannot resolve workspace root: {error}"))?;
    let mut checked = root.clone();
    for component in candidate.components() {
        checked.push(component.as_os_str());
        if checked.exists() {
            let resolved = fs::canonicalize(&checked)
                .map_err(|error| format!("cannot resolve workspace path: {error}"))?;
            if !resolved.starts_with(&root) {
                return Err("path resolves outside the Kakune workspace".to_string());
            }
        }
    }
    Ok(root.join(candidate))
}

fn timestamp() -> Result<String, String> {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|error| format!("cannot format current time: {error}"))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Read, Write},
        net::TcpListener,
        thread,
        time::Duration,
    };

    use crate::{
        PluginRegistry, ProviderAuth, ProviderProfileUpsert, ProviderType, Store, WorkflowDocument,
        execute_prepared_workflow_with_plugins, prepare_execution_with_context,
        prepare_execution_with_plugins, run_workflow,
    };
    use serde_json::Value;

    #[test]
    fn recovery_reuses_completed_nodes_and_original_delay_deadline() {
        let dir = std::env::temp_dir().join(format!("kakune-checkpoints-{}", uuid::Uuid::new_v4()));
        let store = Store::open(dir.clone()).unwrap();
        let source = r#"apiVersion: kakune/v1
kind: Workflow
metadata: {id: resume, name: Resume}
triggers: [{id: manual, type: kakune.trigger.manual@1}]
entry: start
nodes:
  - id: start
    type: kakune.flow.pass@1
    on: {success: wait}
  - id: wait
    type: kakune.flow.delay@1
    inputs: {durationMs: {literal: 60000}}
"#;
        let plugins = PluginRegistry::default();
        let workflow = WorkflowDocument::parse(source).unwrap();
        let saved = store.upsert_workflow(&workflow, source, "enabled").unwrap();
        let run =
            prepare_execution_with_plugins(&store, source, &saved.revision, &plugins).unwrap();
        assert_eq!(run.status, "running");
        store.begin_checkpoint(&run.id, "start", "running").unwrap();
        store
            .complete_checkpoint(
                &run.id,
                "start",
                Some(&serde_json::json!({"route":"success","outputs":{},"message":null})),
            )
            .unwrap();
        store.begin_checkpoint(&run.id, "wait", "waiting").unwrap();
        // Simulate a deadline that passed while the process was offline.
        store
            .durable_delay_remaining(&run.id, "wait", Duration::ZERO)
            .unwrap();
        let stale = store
            .start_trace_span(
                &run.id,
                None,
                ("node", "delay"),
                Some("wait"),
                Some(1),
                &serde_json::json!({}),
            )
            .unwrap();
        assert_eq!(store.recover_incomplete_executions().unwrap(), 1);
        let resumed = store.queued_executions().unwrap().pop().unwrap();
        let result = execute_prepared_workflow_with_plugins(&store, resumed, &plugins).unwrap();
        assert_eq!(result.status, "succeeded");
        let trace = store.execution_trace(&run.id).unwrap().unwrap();
        assert!(
            trace
                .spans
                .iter()
                .all(|span| span.node_id.as_deref() != Some("start"))
        );
        assert_eq!(
            trace
                .spans
                .iter()
                .find(|span| span.id == stale)
                .unwrap()
                .status,
            "cancelled"
        );
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn runs_native_write_file_node() {
        let directory =
            std::env::temp_dir().join(format!("kakune-runtime-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: write\n  name: Write\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: write\nnodes:\n  - id: write\n    type: kakune.fs.write-text@1\n    inputs:\n      path: { literal: notes/hello.txt }\n      content: { literal: hello }\n";
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        let execution = run_workflow(&store, &workflow).expect("workflow should run");
        assert_eq!(execution.status, "succeeded");
        assert_eq!(
            fs::read_to_string(directory.join("workspace/notes/hello.txt"))
                .expect("output should exist"),
            "hello"
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn validates_profile_type_and_permitted_model_before_execution() {
        let directory = std::env::temp_dir().join(format!(
            "kakune-provider-runtime-test-{}",
            uuid::Uuid::new_v4()
        ));
        let store = Store::open(directory.clone()).expect("store should open");
        store
            .create_provider_profile(ProviderProfileUpsert {
                id: "codex-safe".to_string(),
                display_name: "Codex Safe".to_string(),
                provider_type: ProviderType::Codex,
                default_model: "gpt-5.6-terra".to_string(),
                allowed_models: vec!["gpt-5.6-terra".to_string()],
                capabilities: vec!["text-generation".to_string()],
                auth: ProviderAuth::OAuthSecret {
                    secret_ref: "codex-safe-oauth".to_string(),
                },
                config: serde_json::json!({}),
            })
            .expect("profile should save");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: profile-check\n  name: Profile check\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: run\nnodes:\n  - id: run\n    type: kakune.ai.codex.exec@1\n    with:\n      provider: codex-safe\n      model: gpt-5.6-terra\n    inputs:\n      prompt: { literal: hello }\n";
        assert!(
            prepare_execution_with_plugins(&store, source, "test", &PluginRegistry::default())
                .is_ok()
        );
        let rejected = source.replace("gpt-5.6-terra", "not-permitted");
        assert!(
            prepare_execution_with_plugins(&store, &rejected, "test", &PluginRegistry::default())
                .expect_err("disallowed model must fail before execution")
                .contains("not permitted")
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn copies_and_moves_files_only_within_the_workspace() {
        let directory =
            std::env::temp_dir().join(format!("kakune-runtime-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let workspace = directory.join("workspace");
        fs::create_dir_all(&workspace).expect("workspace should be created");
        fs::write(workspace.join("original.txt"), "preserved content")
            .expect("fixture should be written");
        let source = r#"
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: relocate
  name: Relocate file
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: copy
nodes:
  - id: copy
    type: kakune.fs.copy@1
    inputs:
      source: { literal: original.txt }
      destination: { literal: staging/copy.txt }
    on: { success: move }
  - id: move
    type: kakune.fs.move@1
    inputs:
      source: { literal: staging/copy.txt }
      destination: { literal: archive/final.txt }
"#;
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        let execution = run_workflow(&store, &workflow).expect("workflow should run");
        assert_eq!(execution.status, "succeeded");
        assert_eq!(
            fs::read_to_string(workspace.join("original.txt")).unwrap(),
            "preserved content"
        );
        assert!(!workspace.join("staging/copy.txt").exists());
        assert_eq!(
            fs::read_to_string(workspace.join("archive/final.txt")).unwrap(),
            "preserved content"
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn executes_bounded_http_requests_and_routes_non_success_responses() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        let address = listener
            .local_addr()
            .expect("listener should have an address");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("request should arrive");
            let mut request = [0_u8; 1024];
            let received = stream
                .read(&mut request)
                .expect("request should be readable");
            let request = String::from_utf8_lossy(&request[..received]);
            assert!(request.starts_with("POST /status HTTP/1.1"));
            assert!(
                request.contains("x-kakune-test: yes") || request.contains("X-Kakune-Test: yes")
            );
            stream.write_all(b"HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}")
                .expect("response should be written");
        });
        let directory =
            std::env::temp_dir().join(format!("kakune-runtime-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = format!(
            r#"
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: request
  name: HTTP request
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: request
nodes:
  - id: request
    type: kakune.http.request@1
    with: {{ method: POST, timeoutSeconds: 5 }}
    inputs:
      url: {{ literal: http://{address}/status }}
      headers: {{ literal: {{ x-kakune-test: "yes" }} }}
      body: {{ literal: {{ action: inspect }} }}
"#
        );
        let workflow = WorkflowDocument::parse(&source).expect("workflow should parse");
        let execution = run_workflow(&store, &workflow).expect("workflow should run");
        let result = store
            .list_node_runs(&execution.id)
            .expect("node runs should list")
            .into_iter()
            .find(|node| node.node_id == "request")
            .and_then(|node| node.result)
            .expect("HTTP result should persist");
        assert_eq!(
            result.pointer("/route"),
            Some(&Value::String("success".to_string()))
        );
        assert_eq!(result.pointer("/outputs/status"), Some(&Value::from(202)));
        assert_eq!(result.pointer("/outputs/body/ok"), Some(&Value::Bool(true)));
        server.join().expect("server should complete");
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn routes_a_decision_to_the_selected_node() {
        let directory =
            std::env::temp_dir().join(format!("kakune-runtime-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: branch\n  name: Branch\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: decide\nnodes:\n  - id: decide\n    type: kakune.flow.if@1\n    inputs:\n      condition: { literal: true }\n    on:\n      \"true\": yes\n      \"false\": no\n  - id: yes\n    type: kakune.log@1\n    inputs:\n      message: { literal: selected }\n  - id: no\n    type: kakune.log@1\n    inputs:\n      message: { literal: skipped }\n";
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        let execution = run_workflow(&store, &workflow).expect("workflow should run");
        let nodes = store
            .list_node_runs(&execution.id)
            .expect("node runs should list");
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[1].node_id, "yes");
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn switch_runs_the_selected_branch_and_exposes_its_outputs() {
        let directory =
            std::env::temp_dir().join(format!("kakune-runtime-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = r#"
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: switch
  name: Switch
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: choose
nodes:
  - id: choose
    type: kakune.flow.switch@1
    inputs:
      value: { literal: blue }
    with:
      cases: { blue: selected }
      default: fallback
    branches:
      selected:
        entry: emit
        nodes:
          - id: emit
            type: kakune.flow.pass@1
            inputs:
              result: { literal: selected }
        outputs:
          result: { from: emit.result }
      fallback:
        entry: emit
        nodes:
          - id: emit
            type: kakune.flow.pass@1
            inputs:
              result: { literal: fallback }
        outputs:
          result: { from: emit.result }
    on:
      selected: done
  - id: done
    type: kakune.flow.end@1
"#;
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        let execution = run_workflow(&store, &workflow).expect("workflow should run");
        let nodes = store
            .list_node_runs(&execution.id)
            .expect("node runs should list");
        let choice = nodes
            .iter()
            .find(|node| node.node_id == "choose")
            .expect("switch outcome persists");
        assert_eq!(
            choice
                .result
                .as_ref()
                .and_then(|result| result.pointer("/outputs/branch")),
            Some(&Value::String("selected".to_string()))
        );
        assert_eq!(
            choice
                .result
                .as_ref()
                .and_then(|result| result.pointer("/outputs/result")),
            Some(&Value::String("selected".to_string()))
        );
        assert!(nodes.iter().any(|node| node.node_id == "done"));
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn foreach_preserves_input_order_and_can_continue_after_an_error() {
        let directory =
            std::env::temp_dir().join(format!("kakune-runtime-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        fs::create_dir_all(directory.join("workspace")).expect("workspace should be created");
        fs::write(directory.join("workspace/present.txt"), "present")
            .expect("fixture should be written");
        let source = r#"
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: foreach
  name: Foreach
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: each
nodes:
  - id: each
    type: kakune.flow.foreach@1
    inputs:
      items: { literal: [present.txt, missing.txt] }
    with:
      maxConcurrency: 2
      onError: continue
    body:
      entry: read
      nodes:
        - id: read
          type: kakune.fs.read-text@1
          inputs:
            path: { from: $item }
      outputs:
        text: { from: read.text }
"#;
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        let execution = run_workflow(&store, &workflow).expect("workflow should run");
        let nodes = store
            .list_node_runs(&execution.id)
            .expect("node runs should list");
        let result = nodes
            .iter()
            .find(|node| node.node_id == "each")
            .and_then(|node| node.result.as_ref())
            .and_then(|result| result.pointer("/outputs/results"))
            .and_then(Value::as_array)
            .expect("foreach result persists");
        assert_eq!(
            result[0].pointer("/item"),
            Some(&Value::String("present.txt".to_string()))
        );
        assert_eq!(
            result[0].pointer("/outputs/text"),
            Some(&Value::String("present".to_string()))
        );
        assert_eq!(
            result[1].pointer("/item"),
            Some(&Value::String("missing.txt".to_string()))
        );
        assert!(result[1].get("error").is_some());
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn loop_stops_at_its_required_iteration_bound() {
        let directory =
            std::env::temp_dir().join(format!("kakune-runtime-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = r#"
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: loop
  name: Loop
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: repeat
nodes:
  - id: repeat
    type: kakune.flow.loop@1
    inputs:
      state: { literal: unchanged }
    with:
      maxIterations: 3
    body:
      entry: pass
      nodes:
        - id: pass
          type: kakune.flow.pass@1
          inputs:
            state: { from: $state }
            continue: { literal: true }
      outputs:
        state: { from: pass.state }
        continue: { from: pass.continue }
"#;
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        let execution = run_workflow(&store, &workflow).expect("workflow should run");
        let nodes = store
            .list_node_runs(&execution.id)
            .expect("node runs should list");
        let result = nodes
            .iter()
            .find(|node| node.node_id == "repeat")
            .and_then(|node| node.result.as_ref())
            .expect("loop result persists");
        assert_eq!(
            result.pointer("/route"),
            Some(&Value::String("maxIterations".to_string()))
        );
        assert_eq!(result.pointer("/outputs/iterations"), Some(&Value::from(3)));
        assert_eq!(
            result.pointer("/outputs/completed"),
            Some(&Value::Bool(false))
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn rejects_an_unregistered_node_inside_a_structured_body() {
        let directory =
            std::env::temp_dir().join(format!("kakune-runtime-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = r#"
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: unregistered-body
  name: Unregistered body
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: each
nodes:
  - id: each
    type: kakune.flow.foreach@1
    inputs: { items: { literal: [one] } }
    with: { maxConcurrency: 1 }
    body:
      entry: external
      nodes:
        - id: external
          type: org.example.external@1
"#;
        let workflow =
            WorkflowDocument::parse(source).expect("workflow remains syntactically valid");
        let error = run_workflow(&store, &workflow).expect_err("unregistered body must not run");
        assert!(error.contains("unregistered node type"));
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn evaluates_declarative_expression_bindings_without_executing_code() {
        let directory =
            std::env::temp_dir().join(format!("kakune-runtime-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = r#"
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: expressions
  name: Expressions
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: values
nodes:
  - id: values
    type: kakune.flow.pass@1
    inputs:
      total:
        expr:
          op: add
          args:
            - literal: 2
            - literal: 3
      message:
        expr:
          op: concat
          args:
            - literal: total=
            - expr:
                op: add
                args: [{ literal: 2 }, { literal: 3 }]
      selected:
        expr:
          op: get
          args:
            - literal: { name: Kakune }
            - literal: name
    on: { success: done }
  - id: done
    type: kakune.flow.end@1
"#;
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        let execution = run_workflow(&store, &workflow).expect("workflow should run");
        let result = store
            .list_node_runs(&execution.id)
            .expect("node runs should list")
            .into_iter()
            .find(|node| node.node_id == "values")
            .and_then(|node| node.result)
            .expect("values result should persist");
        assert_eq!(result.pointer("/outputs/total"), Some(&Value::from(5)));
        assert_eq!(
            result.pointer("/outputs/message"),
            Some(&Value::String("total=5".to_string()))
        );
        assert_eq!(
            result.pointer("/outputs/selected"),
            Some(&Value::String("Kakune".to_string()))
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn persists_large_results_as_artifacts_and_snapshots_the_plan() {
        let directory =
            std::env::temp_dir().join(format!("kakune-artifact-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let workspace = store.workspace_dir().expect("workspace should open");
        let content = "x".repeat(super::INLINE_RESULT_BYTES + 1);
        fs::write(workspace.join("large.txt"), &content).expect("fixture should write");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: artifact\n  name: Artifact\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: read\nnodes:\n  - id: read\n    type: kakune.fs.read-text@1\n    inputs:\n      path: { literal: large.txt }\noutputs:\n  value: { from: read.text }\n";
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        store
            .upsert_workflow(&workflow, source, "enabled")
            .expect("workflow should save");
        let execution = run_workflow(&store, &workflow).expect("workflow should run");
        let node = store
            .list_node_runs(&execution.id)
            .expect("node runs should list")
            .pop()
            .expect("read should run");
        assert!(
            node.result
                .as_ref()
                .and_then(|result| result.pointer("/outputs/text/id"))
                .is_some()
        );
        assert!(
            store
                .execution_plan(&execution.id)
                .expect("plan should load")
                .is_some()
        );
        assert!(
            store
                .execution_result(&execution.id)
                .expect("result should load")
                .is_some()
        );
        let events =
            serde_json::to_string(&store.list_events_after(None).expect("events should list"))
                .expect("events should serialize");
        assert!(!events.contains(&content));
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn workflow_deadline_leaves_a_timed_out_execution() {
        let directory =
            std::env::temp_dir().join(format!("kakune-timeout-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: timeout\n  name: Timeout\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: wait\npolicy: { timeout: 20ms }\nnodes:\n  - id: wait\n    type: kakune.flow.delay@1\n    inputs:\n      durationMs: { literal: 500 }\n";
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        store
            .upsert_workflow(&workflow, source, "enabled")
            .expect("workflow should save");
        let error = run_workflow(&store, &workflow).expect_err("deadline should fail execution");
        assert_eq!(error, "execution timed out");
        assert_eq!(
            store.list_executions().expect("executions should list")[0].status,
            "timed_out"
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn cancellation_interrupts_a_running_delay() {
        let directory = std::env::temp_dir().join(format!(
            "kakune-runtime-cancel-test-{}",
            uuid::Uuid::new_v4()
        ));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: cancel\n  name: Cancel\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: wait\nnodes:\n  - id: wait\n    type: kakune.flow.delay@1\n    inputs:\n      durationMs: { literal: 5000 }\n";
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        store
            .upsert_workflow(&workflow, source, "enabled")
            .expect("workflow should save");
        let runner_store = store.clone();
        let runner = thread::spawn(move || run_workflow(&runner_store, &workflow));
        let execution_id = loop {
            if let Some(execution) = store
                .list_executions()
                .expect("executions should list")
                .into_iter()
                .next()
            {
                break execution.id;
            }
            thread::sleep(Duration::from_millis(5));
        };
        assert!(
            store
                .request_execution_cancel(&execution_id)
                .expect("cancel should persist")
        );
        let execution = runner
            .join()
            .expect("runner should join")
            .expect("cancelled run is a result");
        assert_eq!(execution.status, "cancelled");
        assert_eq!(
            store
                .get_execution(&execution_id)
                .expect("execution should load")
                .expect("execution exists")
                .status,
            "cancelled"
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn joins_parallel_routes_and_merges_an_alternative_without_deadlock() {
        let directory =
            std::env::temp_dir().join(format!("kakune-flow-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: parallel\n  name: Parallel\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: fork\nnodes:\n  - id: fork\n    type: kakune.flow.pass@1\n    on:\n      success: [left, right]\n  - id: left\n    type: kakune.flow.pass@1\n    on: { success: joined }\n  - id: right\n    type: kakune.flow.pass@1\n    on: { success: joined }\n  - id: joined\n    type: kakune.flow.join@1\n    on: { success: choose }\n  - id: choose\n    type: kakune.flow.if@1\n    inputs: { condition: { literal: true } }\n    on: { 'true': selected, 'false': discarded }\n  - id: selected\n    type: kakune.flow.pass@1\n    on: { success: merged }\n  - id: discarded\n    type: kakune.flow.pass@1\n    on: { success: merged }\n  - id: merged\n    type: kakune.flow.merge@1\n    on: { success: end }\n  - id: end\n    type: kakune.flow.end@1\n";
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        assert_eq!(
            run_workflow(&store, &workflow)
                .expect("flow should finish")
                .status,
            "succeeded"
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn resolves_execution_inputs_and_scope_variables() {
        let directory =
            std::env::temp_dir().join(format!("kakune-scope-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: scoped\n  name: Scoped\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: set\nnodes:\n  - id: set\n    type: kakune.flow.set-variable@1\n    with: { name: greeting }\n    inputs: { value: { from: '$input.message' } }\n    on: { success: log }\n  - id: log\n    type: kakune.log@1\n    inputs: { message: { from: '$greeting' } }\n";
        let execution = prepare_execution_with_context(
            &store,
            source,
            "test",
            &PluginRegistry::default(),
            serde_json::json!({ "message": "hello" }),
            serde_json::json!({}),
        )
        .expect("execution should prepare");
        assert_eq!(
            execute_prepared_workflow_with_plugins(&store, execution, &PluginRegistry::default())
                .expect("execution should run")
                .status,
            "succeeded"
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }

    #[test]
    fn rejects_invalid_structured_results_and_unpermitted_agent_tools() {
        let schema = serde_json::json!({
            "type": "object",
            "required": ["ok"],
            "properties": { "ok": { "type": "boolean" } }
        });
        assert!(
            super::validate_schema(
                schema.as_object().expect("literal schema is an object"),
                &serde_json::json!({ "ok": "not a boolean" })
            )
            .is_err()
        );
        let tools = vec![serde_json::json!({
            "name": "echo",
            "type": "echo",
            "input_schema": { "type": "object" }
        })];
        assert!(super::execute_agent_tool(&tools, "not-allowed", &serde_json::json!({})).is_err());
    }

    #[cfg(windows)]
    #[test]
    fn runs_a_native_process_with_bounded_outputs() {
        let directory =
            std::env::temp_dir().join(format!("kakune-process-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: process\n  name: Process\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: command\nnodes:\n  - id: command\n    type: kakune.process.run@1\n    with: { command: cmd }\n    inputs:\n      args: { literal: [/C, echo hello] }\n";
        let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
        store
            .upsert_workflow(&workflow, source, "enabled")
            .expect("workflow should save");
        let execution = run_workflow(&store, &workflow).expect("process should run");
        let result = store
            .list_node_runs(&execution.id)
            .expect("node runs should list")[0]
            .result
            .clone()
            .expect("result should persist");
        assert_eq!(result.pointer("/outputs/exitCode"), Some(&Value::from(0)));
        assert!(
            result
                .pointer("/outputs/stdout")
                .and_then(Value::as_str)
                .is_some_and(|stdout| stdout.contains("hello"))
        );
        drop(store);
        fs::remove_dir_all(directory).expect("temporary data should be removed");
    }
}
