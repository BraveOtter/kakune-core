use std::collections::{BTreeMap, HashMap, HashSet};

use saphyr::{LoadableYamlNode, Yaml};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowDocument {
    pub api_version: String,
    pub kind: String,
    pub metadata: WorkflowMetadata,
    #[serde(default)]
    pub inputs: Value,
    pub triggers: Vec<WorkflowTrigger>,
    pub entry: String,
    pub nodes: Vec<WorkflowNode>,
    #[serde(default)]
    pub outputs: BTreeMap<String, WorkflowBinding>,
    #[serde(default)]
    pub policy: WorkflowPolicy,
    #[serde(default)]
    pub layout: WorkflowLayout,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowMetadata {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowTrigger {
    pub id: String,
    #[serde(rename = "type")]
    pub node_type: String,
    #[serde(default)]
    pub with: Map<String, Value>,
    #[serde(default)]
    pub map: BTreeMap<String, WorkflowBinding>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowNode {
    pub id: String,
    #[serde(rename = "type")]
    pub node_type: String,
    #[serde(default)]
    pub with: Map<String, Value>,
    #[serde(default)]
    pub inputs: BTreeMap<String, WorkflowBinding>,
    #[serde(default)]
    pub on: BTreeMap<String, ControlTargets>,
    #[serde(default)]
    pub retry: Option<RetryPolicy>,
    #[serde(default)]
    pub branches: BTreeMap<String, WorkflowSubgraph>,
    #[serde(default)]
    pub body: Option<WorkflowSubgraph>,
}

/// A route can activate one node or fan out to several independent branches.
/// Keeping the scalar form valid preserves the concise YAML used by existing workflows.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum ControlTargets {
    One(String),
    Many(Vec<String>),
}

impl ControlTargets {
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        match self {
            Self::One(target) => std::slice::from_ref(target).iter().map(String::as_str),
            Self::Many(targets) => targets.iter().map(String::as_str),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::One(_) => 1,
            Self::Many(targets) => targets.len(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    #[serde(default)]
    pub max_backoff_ms: Option<u64>,
    #[serde(default)]
    pub jitter_ms: Option<u64>,
}

/// A structured, acyclic graph owned by a control node.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowSubgraph {
    pub entry: String,
    pub nodes: Vec<WorkflowNode>,
    #[serde(default)]
    pub outputs: BTreeMap<String, WorkflowBinding>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum WorkflowBinding {
    Literal { literal: Value },
    From { from: String },
    Expr { expr: Value },
    Secret { secret: String },
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowPolicy {
    pub timeout: Option<String>,
    pub max_parallel_nodes: Option<u32>,
    pub concurrency: Option<ConcurrencyPolicy>,
    #[serde(default)]
    pub ai: Option<AiPolicy>,
}

/// Limits that Core can enforce before a provider request is sent. These are
/// token reservations, not an assertion about a provider's billed amount.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AiPolicy {
    pub budget_tokens: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ConcurrencyPolicy {
    pub max_runs: Option<u32>,
    pub overflow: Option<OverflowPolicy>,
    pub max_queued: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OverflowPolicy {
    Queue,
    Reject,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowLayout {
    #[serde(default)]
    pub nodes: BTreeMap<String, NodePosition>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NodePosition {
    pub x: f64,
    pub y: f64,
}

impl WorkflowDocument {
    pub fn parse(source: &str) -> Result<Self, String> {
        if source.len() > 1_048_576 {
            return Err("workflow source exceeds the 1 MiB limit".to_string());
        }
        let mut documents =
            Yaml::load_from_str(source).map_err(|error| format!("invalid YAML: {error}"))?;
        if documents.len() != 1 {
            return Err("a workflow file must contain exactly one YAML document".to_string());
        }
        let document = documents
            .pop()
            .ok_or_else(|| "workflow is empty".to_string())?;
        let value = yaml_to_json(&document)?;
        let workflow: Self = serde_json::from_value(value)
            .map_err(|error| format!("workflow does not match kakune/v1: {error}"))?;
        workflow.validate()?;
        Ok(workflow)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.api_version != "kakune/v1" {
            return Err("apiVersion must be kakune/v1".to_string());
        }
        if self.kind != "Workflow" {
            return Err("kind must be Workflow".to_string());
        }
        validate_id("metadata.id", &self.metadata.id)?;
        if self.metadata.name.trim().is_empty() || self.metadata.name.len() > 200 {
            return Err("metadata.name must contain at least one non-whitespace character and be at most 200 characters".to_string());
        }
        if !self.inputs.is_null() && !self.inputs.is_object() {
            return Err("inputs must be a JSON Schema object".to_string());
        }
        if self.triggers.is_empty() {
            return Err("triggers must contain at least one trigger".to_string());
        }
        if self.nodes.is_empty() {
            return Err("nodes must contain at least one node".to_string());
        }
        if self
            .policy
            .max_parallel_nodes
            .is_some_and(|limit| !(1..=64).contains(&limit))
        {
            return Err("policy.maxParallelNodes must be between 1 and 64".to_string());
        }
        if let Some(concurrency) = &self.policy.concurrency {
            if concurrency.max_runs.is_some_and(|value| value == 0) {
                return Err("policy.concurrency.maxRuns must be positive".to_string());
            }
            match concurrency.overflow {
                Some(OverflowPolicy::Queue)
                    if concurrency.max_queued.is_none_or(|value| value == 0) =>
                {
                    return Err(
                        "policy.concurrency.maxQueued must be positive when overflow is queue"
                            .to_string(),
                    );
                }
                Some(OverflowPolicy::Reject) if concurrency.max_queued.is_some() => {
                    return Err(
                        "policy.concurrency.maxQueued is only valid when overflow is queue"
                            .to_string(),
                    );
                }
                _ => {}
            }
        }
        if self
            .policy
            .ai
            .as_ref()
            .and_then(|policy| policy.budget_tokens)
            .is_some_and(|value| value == 0)
        {
            return Err("policy.ai.budgetTokens must be positive".to_string());
        }

        let mut trigger_ids = HashSet::new();
        for trigger in &self.triggers {
            validate_id("trigger.id", &trigger.id)?;
            validate_node_type("trigger.type", &trigger.node_type)?;
            if !trigger_ids.insert(trigger.id.as_str()) {
                return Err(format!("duplicate trigger id: {}", trigger.id));
            }
            validate_bindings(&trigger.map, "trigger.map")?;
        }

        let mut node_ids = HashSet::new();
        for node in &self.nodes {
            validate_id("node.id", &node.id)?;
            validate_node_type("node.type", &node.node_type)?;
            if !node_ids.insert(node.id.as_str()) {
                return Err(format!("duplicate node id: {}", node.id));
            }
            validate_bindings(&node.inputs, "node.inputs")?;
            validate_ai_node(node, "node")?;
        }
        if !node_ids.contains(self.entry.as_str()) {
            return Err(format!("entry references unknown node {}", self.entry));
        }
        for node in &self.nodes {
            for (route, targets) in &node.on {
                validate_id("node.on route", route)?;
                if targets.len() == 0 {
                    return Err(format!("node {} route {route} cannot be empty", node.id));
                }
                for target in targets.iter() {
                    if !node_ids.contains(target) {
                        return Err(format!(
                            "node {} route {route} references unknown node {target}",
                            node.id
                        ));
                    }
                }
            }
        }
        for node_id in self.layout.nodes.keys() {
            if !node_ids.contains(node_id.as_str()) {
                return Err(format!("layout references unknown node {node_id}"));
            }
        }
        validate_bindings(&self.outputs, "outputs")?;
        ensure_acyclic(&self.nodes)?;
        validate_convergences(&self.nodes, "workflow")?;
        for node in &self.nodes {
            validate_structured_node(node, "node")?;
            validate_retry(node, "node")?;
        }
        Ok(())
    }

    /// Validates the node catalog used by an execution, including nested bodies.
    pub fn validate_with_plugins(
        &self,
        plugins: &crate::plugin_process::PluginRegistry,
    ) -> Result<(), String> {
        self.validate()?;
        for node in &self.nodes {
            validate_node_catalog(node, plugins)?;
        }
        Ok(())
    }

    pub fn node(&self, id: &str) -> Option<&WorkflowNode> {
        self.nodes.iter().find(|node| node.id == id)
    }
}

fn validate_node_catalog(
    node: &WorkflowNode,
    plugins: &crate::plugin_process::PluginRegistry,
) -> Result<(), String> {
    if !crate::runtime::is_builtin_node_type(&node.node_type)
        && plugins.get(&node.node_type).is_none()
    {
        return Err(format!(
            "node {} uses unregistered node type {}; execution requires a built-in node or an explicitly registered plugin",
            node.id, node.node_type
        ));
    }
    for branch in node.branches.values() {
        for child in &branch.nodes {
            validate_node_catalog(child, plugins)?;
        }
    }
    if let Some(body) = &node.body {
        for child in &body.nodes {
            validate_node_catalog(child, plugins)?;
        }
    }
    Ok(())
}

fn validate_structured_node(node: &WorkflowNode, field: &str) -> Result<(), String> {
    match node.node_type.as_str() {
        "kakune.flow.switch@1" => {
            validate_control_keys(node, field, &["cases", "default"])?;
            if node.body.is_some() {
                return Err(format!("{field} {} switch cannot contain body", node.id));
            }
            if !node.inputs.contains_key("value") {
                return Err(format!("{field} {} switch requires inputs.value", node.id));
            }
            let cases = node
                .with
                .get("cases")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    format!("{field} {} switch with.cases must be an object", node.id)
                })?;
            if cases.is_empty() {
                return Err(format!(
                    "{field} {} switch with.cases must not be empty",
                    node.id
                ));
            }
            for (selector, branch) in cases {
                let branch = branch.as_str().ok_or_else(|| {
                    format!(
                        "{field} {} switch case {selector} must name a branch",
                        node.id
                    )
                })?;
                if !node.branches.contains_key(branch) {
                    return Err(format!(
                        "{field} {} switch case {selector} references unknown branch {branch}",
                        node.id
                    ));
                }
            }
            if let Some(default) = node.with.get("default") {
                let default = default.as_str().ok_or_else(|| {
                    format!("{field} {} switch with.default must name a branch", node.id)
                })?;
                if !node.branches.contains_key(default) {
                    return Err(format!(
                        "{field} {} switch default references unknown branch {default}",
                        node.id
                    ));
                }
            }
            for (name, branch) in &node.branches {
                validate_id("switch branch", name)?;
                validate_subgraph(
                    branch,
                    &format!("{field} {} branch {name}", node.id),
                    Some("switch"),
                )?;
                if branch.outputs.contains_key("branch") {
                    return Err(format!(
                        "{field} {} branch {name} cannot define reserved output branch",
                        node.id
                    ));
                }
            }
        }
        "kakune.flow.foreach@1" => {
            validate_control_keys(node, field, &["maxConcurrency", "onError"])?;
            if !node.branches.is_empty() {
                return Err(format!(
                    "{field} {} foreach cannot contain branches",
                    node.id
                ));
            }
            if !node.inputs.contains_key("items") {
                return Err(format!("{field} {} foreach requires inputs.items", node.id));
            }
            validate_foreach_config(node, field)?;
            let body = node
                .body
                .as_ref()
                .ok_or_else(|| format!("{field} {} foreach requires body", node.id))?;
            validate_subgraph(body, &format!("{field} {} body", node.id), Some("foreach"))?;
        }
        "kakune.flow.loop@1" => {
            validate_control_keys(node, field, &["maxIterations"])?;
            if !node.branches.is_empty() {
                return Err(format!("{field} {} loop cannot contain branches", node.id));
            }
            if !node.inputs.contains_key("state") {
                return Err(format!("{field} {} loop requires inputs.state", node.id));
            }
            validate_loop_config(node, field)?;
            let body = node
                .body
                .as_ref()
                .ok_or_else(|| format!("{field} {} loop requires body", node.id))?;
            validate_subgraph(body, &format!("{field} {} body", node.id), Some("loop"))?;
            for name in ["state", "continue"] {
                if !body.outputs.contains_key(name) {
                    return Err(format!(
                        "{field} {} loop body outputs.{name} is required",
                        node.id
                    ));
                }
            }
        }
        "kakune.flow.join@1" | "kakune.flow.merge@1" => {
            validate_control_keys(node, field, &[])?;
            if !node.branches.is_empty() || node.body.is_some() {
                return Err(format!(
                    "{field} {} cannot contain a body or branches",
                    node.id
                ));
            }
        }
        "kakune.flow.delay@1" => {
            validate_control_keys(node, field, &[])?;
            if !node.inputs.contains_key("durationMs") {
                return Err(format!(
                    "{field} {} delay requires inputs.durationMs",
                    node.id
                ));
            }
        }
        "kakune.flow.set-variable@1" => {
            validate_control_keys(node, field, &["name"])?;
            if !node.inputs.contains_key("value")
                || !node.with.get("name").is_some_and(Value::is_string)
            {
                return Err(format!(
                    "{field} {} set-variable requires with.name and inputs.value",
                    node.id
                ));
            }
        }
        _ => {
            if !node.branches.is_empty() || node.body.is_some() {
                return Err(format!(
                    "{field} {} type {} does not support structured subgraphs",
                    node.id, node.node_type
                ));
            }
        }
    }
    Ok(())
}

fn validate_ai_node(node: &WorkflowNode, field: &str) -> Result<(), String> {
    if !matches!(
        node.node_type.as_str(),
        "kakune.ai.generate@1"
            | "kakune.ai.extract@1"
            | "kakune.ai.boolean@1"
            | "kakune.ai.choose@1"
            | "kakune.ai.agent@1"
            | "kakune.ai.minimax.messages@1"
            | "kakune.ai.codex.exec@1"
    ) {
        return Ok(());
    }
    if node.inputs.contains_key("subscriptionKey") {
        return Err(format!(
            "{field} {} does not accept inputs.subscriptionKey; configure credentials through the provider",
            node.id
        ));
    }
    if node.inputs.contains_key("model") {
        return Err(format!(
            "{field} {} does not accept inputs.model; configure with.model instead",
            node.id
        ));
    }
    for name in ["provider", "model"] {
        if !node
            .with
            .get(name)
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
        {
            return Err(format!(
                "{field} {} requires with.{name} to be a non-empty string",
                node.id
            ));
        }
    }
    Ok(())
}

fn validate_retry(node: &WorkflowNode, field: &str) -> Result<(), String> {
    let Some(retry) = &node.retry else {
        return Ok(());
    };
    if !(1..=10).contains(&retry.max_attempts)
        || retry.initial_backoff_ms == 0
        || retry.max_backoff_ms.is_some_and(|value| value == 0)
    {
        return Err(format!(
            "{field} {} retry must use 1..10 attempts and positive backoff",
            node.id
        ));
    }
    // Repeating an external effect after a lost response is unsafe unless its
    // workflow author has explicitly declared the operation idempotent.
    if matches!(
        node.node_type.as_str(),
        "kakune.http.request@1" | "kakune.process.run@1"
    ) && node.with.get("idempotent") != Some(&Value::Bool(true))
    {
        return Err(format!(
            "{field} {} retries on external effects require with.idempotent: true",
            node.id
        ));
    }
    Ok(())
}

fn validate_convergences(nodes: &[WorkflowNode], field: &str) -> Result<(), String> {
    let mut incoming = BTreeMap::<&str, usize>::new();
    for node in nodes {
        for targets in node.on.values() {
            for target in targets.iter() {
                *incoming.entry(target).or_default() += 1;
            }
        }
    }
    for node in nodes {
        if incoming.get(node.id.as_str()).copied().unwrap_or_default() > 1
            && !matches!(
                node.node_type.as_str(),
                "kakune.flow.join@1" | "kakune.flow.merge@1"
            )
        {
            return Err(format!(
                "{field} convergence at {} requires kakune.flow.join@1 or kakune.flow.merge@1",
                node.id
            ));
        }
    }
    Ok(())
}

fn validate_control_keys(node: &WorkflowNode, field: &str, allowed: &[&str]) -> Result<(), String> {
    for key in node.with.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(format!(
                "{field} {} type {} does not support with.{key}",
                node.id, node.node_type
            ));
        }
    }
    Ok(())
}

fn validate_foreach_config(node: &WorkflowNode, field: &str) -> Result<(), String> {
    let concurrency = node
        .with
        .get("maxConcurrency")
        .and_then(Value::as_u64)
        .filter(|value| (1..=64).contains(value))
        .ok_or_else(|| {
            format!(
                "{field} {} foreach with.maxConcurrency must be an integer from 1 to 64",
                node.id
            )
        })?;
    let _ = concurrency;
    if let Some(on_error) = node.with.get("onError")
        && !matches!(on_error.as_str(), Some("fail" | "continue"))
    {
        return Err(format!(
            "{field} {} foreach with.onError must be fail or continue",
            node.id
        ));
    }
    Ok(())
}

fn validate_loop_config(node: &WorkflowNode, field: &str) -> Result<(), String> {
    node.with
        .get("maxIterations")
        .and_then(Value::as_u64)
        .filter(|value| (1..=10_000).contains(value))
        .ok_or_else(|| {
            format!(
                "{field} {} loop with.maxIterations is required and must be an integer from 1 to 10000",
                node.id
            )
        })?;
    Ok(())
}

fn validate_subgraph(
    graph: &WorkflowSubgraph,
    field: &str,
    scope: Option<&str>,
) -> Result<(), String> {
    if graph.nodes.is_empty() {
        return Err(format!("{field}.nodes must contain at least one node"));
    }
    let mut ids = HashSet::new();
    for node in &graph.nodes {
        validate_id("subgraph node.id", &node.id)?;
        validate_node_type("subgraph node.type", &node.node_type)?;
        if !ids.insert(node.id.as_str()) {
            return Err(format!("{field} contains duplicate node id {}", node.id));
        }
        validate_bindings(&node.inputs, "subgraph node.inputs")?;
        validate_ai_node(node, field)?;
        validate_structured_node(node, field)?;
        validate_retry(node, field)?;
    }
    if !ids.contains(graph.entry.as_str()) {
        return Err(format!(
            "{field}.entry references unknown node {}",
            graph.entry
        ));
    }
    for node in &graph.nodes {
        for (route, targets) in &node.on {
            validate_id("subgraph node.on route", route)?;
            if targets.len() == 0 {
                return Err(format!(
                    "{field} node {} route {route} cannot be empty",
                    node.id
                ));
            }
            for target in targets.iter() {
                if !ids.contains(target) {
                    return Err(format!(
                        "{field} node {} route {route} references unknown node {target}",
                        node.id
                    ));
                }
            }
        }
    }
    validate_bindings(&graph.outputs, &format!("{field}.outputs"))?;
    validate_convergences(&graph.nodes, field)?;
    for binding in graph
        .nodes
        .iter()
        .flat_map(|node| node.inputs.values())
        .chain(graph.outputs.values())
    {
        if let WorkflowBinding::From { from } = binding
            && from.starts_with('$')
            && !matches!(
                (scope, from.as_str()),
                (Some("foreach"), "$item" | "$index")
                    | (Some("loop"), "$state" | "$iteration")
                    | (Some("switch"), "$value")
            )
            && !(from == "$input"
                || from == "$trigger"
                || from.starts_with("$input.")
                || from.starts_with("$trigger."))
        {
            return Err(format!("{field} does not provide local binding {from}"));
        }
    }
    ensure_acyclic(&graph.nodes)
}

fn validate_bindings(
    bindings: &BTreeMap<String, WorkflowBinding>,
    field: &str,
) -> Result<(), String> {
    for (name, binding) in bindings {
        if name.trim().is_empty() {
            return Err(format!("{field} cannot contain an empty input name"));
        }
        match binding {
            WorkflowBinding::From { from } if from.trim().is_empty() => {
                return Err(format!("{field}.{name}.from must not be empty"));
            }
            WorkflowBinding::Secret { secret } if secret.trim().is_empty() => {
                return Err(format!("{field}.{name}.secret must not be empty"));
            }
            WorkflowBinding::Expr { expr } => {
                validate_expression(expr, &format!("{field}.{name}.expr"), 0)?
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_expression(value: &Value, field: &str, depth: usize) -> Result<(), String> {
    if depth > 32 {
        return Err(format!(
            "{field} exceeds the maximum expression depth of 32"
        ));
    }
    let expression = value
        .as_object()
        .ok_or_else(|| format!("{field} must be an object"))?;
    if expression.len() != 2 || !expression.contains_key("op") || !expression.contains_key("args") {
        return Err(format!("{field} must contain only op and args"));
    }
    let operation = expression
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{field}.op must be a string"))?;
    if !matches!(
        operation,
        "equal"
            | "notEqual"
            | "greaterThan"
            | "greaterThanOrEqual"
            | "lessThan"
            | "lessThanOrEqual"
            | "and"
            | "or"
            | "not"
            | "add"
            | "subtract"
            | "multiply"
            | "divide"
            | "modulo"
            | "concat"
            | "array"
            | "object"
            | "get"
            | "coalesce"
            | "contains"
    ) {
        return Err(format!("{field}.op {operation} is not supported"));
    }
    let arguments = expression
        .get("args")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{field}.args must be an array"))?;
    if arguments.len() > 128 {
        return Err(format!("{field}.args exceeds the maximum of 128 arguments"));
    }
    for (index, argument) in arguments.iter().enumerate() {
        validate_expression_argument(argument, &format!("{field}.args[{index}]"), depth + 1)?;
    }
    Ok(())
}

fn validate_expression_argument(value: &Value, field: &str, depth: usize) -> Result<(), String> {
    let Some(value) = value.as_object() else {
        return Ok(());
    };
    if value.len() != 1 {
        return Ok(());
    }
    if let Some(from) = value.get("from") {
        return from
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(|_| ())
            .ok_or_else(|| format!("{field}.from must be a non-empty string"));
    }
    if let Some(secret) = value.get("secret") {
        return secret
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(|_| ())
            .ok_or_else(|| format!("{field}.secret must be a non-empty string"));
    }
    if let Some(expression) = value.get("expr") {
        return validate_expression(expression, &format!("{field}.expr"), depth);
    }
    Ok(())
}

fn validate_id(field: &str, value: &str) -> Result<(), String> {
    let valid = !value.is_empty()
        && value.len() <= 63
        && value.starts_with(|character: char| character.is_ascii_lowercase())
        && value.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        });
    if valid {
        Ok(())
    } else {
        Err(format!(
            "{field} must be a lowercase kebab-case identifier of at most 63 characters"
        ))
    }
}

fn validate_node_type(field: &str, value: &str) -> Result<(), String> {
    let Some((namespace, version)) = value.rsplit_once('@') else {
        return Err(format!("{field} must use the namespace.name@major format"));
    };
    if version
        .parse::<u32>()
        .ok()
        .filter(|version| *version > 0)
        .is_none()
        || namespace.split('.').count() < 2
        || namespace.split('.').any(|part| !is_identifier_part(part))
    {
        return Err(format!("{field} must use the namespace.name@major format"));
    }
    Ok(())
}

fn is_identifier_part(value: &str) -> bool {
    !value.is_empty()
        && value.starts_with(|character: char| character.is_ascii_lowercase())
        && value.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
}

fn ensure_acyclic(nodes: &[WorkflowNode]) -> Result<(), String> {
    fn visit<'a>(
        id: &'a str,
        by_id: &HashMap<&'a str, &'a WorkflowNode>,
        visiting: &mut HashSet<&'a str>,
        visited: &mut HashSet<&'a str>,
    ) -> Result<(), String> {
        if visited.contains(id) {
            return Ok(());
        }
        if !visiting.insert(id) {
            return Err(format!(
                "workflow contains an unstructured control-flow cycle at {id}"
            ));
        }
        let node = by_id
            .get(id)
            .ok_or_else(|| format!("node {id} was not found"))?;
        for targets in node.on.values() {
            for target in targets.iter() {
                visit(target, by_id, visiting, visited)?;
            }
        }
        visiting.remove(id);
        visited.insert(id);
        Ok(())
    }

    let by_id = nodes.iter().map(|node| (node.id.as_str(), node)).collect();
    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    for node in nodes {
        visit(&node.id, &by_id, &mut visiting, &mut visited)?;
    }
    Ok(())
}

fn yaml_to_json(node: &Yaml<'_>) -> Result<Value, String> {
    if node.is_alias() || node.is_tag_node() {
        return Err("workflow aliases and tags are not supported".to_string());
    }
    if let Some(sequence) = node.as_sequence() {
        return sequence
            .iter()
            .map(yaml_to_json)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array);
    }
    if let Some(mapping) = node.as_mapping() {
        let mut output = Map::new();
        for (key, value) in mapping {
            let key = key
                .as_cow()
                .map(|value| value.to_string())
                .or_else(|| key.as_str().map(str::to_owned))
                .ok_or_else(|| "workflow mapping keys must be strings".to_string())?;
            if output.insert(key.clone(), yaml_to_json(value)?).is_some() {
                return Err(format!("workflow contains duplicate key: {key}"));
            }
        }
        return Ok(Value::Object(output));
    }
    if let Some(value) = node.as_cow() {
        return Ok(Value::String(value.to_string()));
    }
    if let Some(value) = node.as_str() {
        return Ok(Value::String(value.to_owned()));
    }
    if let Some(value) = node.as_bool() {
        return Ok(Value::Bool(value));
    }
    if let Some(value) = node.as_integer() {
        return Ok(Value::Number(value.into()));
    }
    if let Some(value) = node.as_floating_point() {
        return Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| "workflow contains a non-finite number".to_string());
    }
    if node.is_null() {
        return Ok(Value::Null);
    }
    Err("workflow contains an unsupported YAML value".to_string())
}

#[cfg(test)]
mod tests {
    use super::WorkflowDocument;

    const VALID: &str = r#"
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: write-note
  name: Write note
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: write
nodes:
  - id: write
    type: kakune.fs.write-text@1
    inputs:
      path: { literal: notes/hello.txt }
      content: { literal: hello }
"#;

    #[test]
    fn parses_a_canonical_workflow() {
        let workflow = WorkflowDocument::parse(VALID).expect("workflow should parse");
        assert_eq!(workflow.metadata.id, "write-note");
        assert_eq!(workflow.entry, "write");
    }

    #[test]
    fn rejects_the_provisional_steps_syntax() {
        let legacy = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: legacy\n  name: Legacy\nspec:\n  steps: []\n";
        assert!(WorkflowDocument::parse(legacy).is_err());
    }

    #[test]
    fn rejects_unstructured_control_flow_cycles() {
        let source = VALID.replace(
            "content: { literal: hello }",
            "content: { literal: hello }\n    on:\n      success: write",
        );
        assert!(WorkflowDocument::parse(&source).is_err());
    }

    #[test]
    fn rejects_cycles_inside_structured_bodies() {
        let source = r#"
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: structured-cycle
  name: Structured cycle
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: each
nodes:
  - id: each
    type: kakune.flow.foreach@1
    inputs:
      items: { literal: [one] }
    with:
      maxConcurrency: 1
    body:
      entry: first
      nodes:
        - id: first
          type: kakune.flow.pass@1
          on: { success: second }
        - id: second
          type: kakune.flow.pass@1
          on: { success: first }
"#;
        let error = WorkflowDocument::parse(source).expect_err("cycle must be rejected");
        assert!(error.contains("unstructured control-flow cycle"));
    }

    #[test]
    fn requires_a_bounded_loop() {
        let source = r#"
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: unbounded-loop
  name: Unbounded loop
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: repeat
nodes:
  - id: repeat
    type: kakune.flow.loop@1
    inputs: { state: { literal: initial } }
    body:
      entry: pass
      nodes:
        - id: pass
          type: kakune.flow.pass@1
      outputs:
        state: { literal: initial }
        continue: { literal: false }
"#;
        let error = WorkflowDocument::parse(source).expect_err("loop bound must be required");
        assert!(error.contains("maxIterations is required"));
    }

    #[test]
    fn rejects_ai_credential_inputs() {
        let source = r#"
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: insecure-ai
  name: Insecure AI
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: generate
nodes:
  - id: generate
    type: kakune.ai.generate@1
    inputs:
      subscriptionKey: { secret: minimax-key }
      maxTokens: { literal: 32 }
      prompt: { literal: hello }
    with:
      provider: minimax
      model: MiniMax-M3
"#;
        let error = WorkflowDocument::parse(source).expect_err("credential input must be rejected");
        assert!(error.contains("does not accept inputs.subscriptionKey"));
    }

    #[test]
    fn rejects_ai_model_inputs() {
        let source = r#"
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: input-model-ai
  name: Input model AI
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: generate
nodes:
  - id: generate
    type: kakune.ai.generate@1
    inputs:
      model: { literal: MiniMax-M3 }
      maxTokens: { literal: 32 }
      prompt: { literal: hello }
    with:
      provider: minimax
      model: MiniMax-M3
"#;
        let error = WorkflowDocument::parse(source).expect_err("model input must be rejected");
        assert!(error.contains("does not accept inputs.model"));
    }

    #[test]
    fn requires_stable_provider_and_model_config_for_ai_nodes() {
        let source = r#"
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: incomplete-ai-config
  name: Incomplete AI config
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: generate
nodes:
  - id: generate
    type: kakune.ai.generate@1
    inputs:
      maxTokens: { literal: 32 }
      prompt: { literal: hello }
    with:
      provider: minimax
      model: "   "
"#;
        let error = WorkflowDocument::parse(source).expect_err("blank model must be rejected");
        assert!(error.contains("with.model to be a non-empty string"));
    }

    #[test]
    fn accepts_ai_provider_and_model_config_without_credential_or_model_inputs() {
        let source = r#"
apiVersion: kakune/v1
kind: Workflow
metadata:
  id: configured-ai
  name: Configured AI
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: generate
nodes:
  - id: generate
    type: kakune.ai.generate@1
    inputs:
      maxTokens: { literal: 32 }
      prompt: { literal: hello }
    with:
      provider: minimax
      model: MiniMax-M3
"#;
        WorkflowDocument::parse(source).expect("provider and model config should be valid");
    }
}
