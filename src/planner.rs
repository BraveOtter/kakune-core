use std::collections::{HashMap, HashSet};

use saphyr::{LoadableYamlNode, MarkedYaml, YamlData};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    Diagnostic, PluginRegistry, SourceRange, WorkflowDocument,
    diagnostic::range_at,
    workflow::{WorkflowBinding, WorkflowNode, WorkflowPolicy},
};

const MAX_NODES: usize = 512;
const MAX_EDGES: usize = 2_048;

/// Public analysis result. The syntax tree and execution plan deliberately stay
/// private implementation details.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowAnalysis {
    pub workflow: Option<WorkflowDocument>,
    pub diagnostics: Vec<Diagnostic>,
}

impl WorkflowAnalysis {
    pub fn is_valid(&self) -> bool {
        self.workflow.is_some() && self.diagnostics.is_empty()
    }
}

/// The private, semantic plan recorded with every execution. It is JSON only
/// for durable inspection; it is not a public compatibility contract.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecutionPlan {
    pub workflow_id: String,
    pub nodes: Vec<PlannedNode>,
    pub entry: String,
    pub outputs: std::collections::BTreeMap<String, WorkflowBinding>,
    pub policy: WorkflowPolicy,
    #[serde(default = "empty_object")]
    pub input_values: Value,
    #[serde(default = "empty_object")]
    pub trigger_values: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PlannedNode {
    pub id: String,
    #[serde(rename = "type")]
    pub node_type: String,
    pub data_dependencies: Vec<String>,
    pub control_dependencies: Vec<String>,
    /// The executable node snapshot. Runtime consumes this plan rather than
    /// reparsing or reaching back into the mutable workflow document.
    pub node: WorkflowNode,
}

/// Compilation produces an immutable execution plan alongside the semantic
/// workflow IR used by the persistence boundary.  The two deliberately have
/// different responsibilities: the plan is a run snapshot, the IR is what a
/// workflow update stores.
#[derive(Clone, Debug)]
pub(crate) struct CompiledWorkflow {
    pub ir: WorkflowDocument,
    pub plan: ExecutionPlan,
}

/// Private AST/CST boundary. Saphyr remains encapsulated: only the source and
/// semantic IR cross into the rest of Core.
struct YamlSyntaxDocument<'a> {
    source: &'a str,
}

pub(crate) fn analyze(source: &str, plugins: &PluginRegistry) -> WorkflowAnalysis {
    let syntax = YamlSyntaxDocument { source };
    let index = YamlIndex::build(syntax.source);
    let workflow = match WorkflowDocument::parse(syntax.source) {
        Ok(workflow) => workflow,
        Err(error) => {
            return WorkflowAnalysis {
                workflow: None,
                diagnostics: vec![diagnostic(
                    syntax.source,
                    &index,
                    "yaml.invalid",
                    error,
                    Some("".to_string()),
                )],
            };
        }
    };
    let diagnostics = validate_semantics(&workflow, plugins, syntax.source, &index);
    WorkflowAnalysis {
        workflow: Some(workflow),
        diagnostics,
    }
}

/// Serializable node metadata consumed by visual clients. The planner keeps
/// execution definitions private while publishing only the stable port model.
pub fn visual_catalog() -> Vec<Value> {
    const TYPES: &[&str] = &[
        "kakune.flow.end@1",
        "kakune.log@1",
        "kakune.flow.pass@1",
        "kakune.flow.if@1",
        "kakune.flow.delay@1",
        "kakune.flow.join@1",
        "kakune.flow.merge@1",
        "kakune.flow.set-variable@1",
        "kakune.fs.write-text@1",
        "kakune.fs.read-text@1",
        "kakune.fs.copy@1",
        "kakune.fs.move@1",
        "kakune.http.request@1",
        "kakune.process.run@1",
        "kakune.ai.generate@1",
        "kakune.ai.minimax.messages@1",
        "kakune.ai.extract@1",
        "kakune.ai.boolean@1",
        "kakune.ai.choose@1",
        "kakune.ai.agent@1",
        "kakune.ai.codex.exec@1",
        "kakune.ai.agent-result@1",
        "kakune.mcp.call@1",
    ];
    let mut catalog = TYPES
        .iter()
        .filter_map(|node_type| {
            let definition = node_definition(node_type)?;
            let ports = |items: &[(&str, ValueKind)], required: bool| {
                items
                    .iter()
                    .map(|(name, kind)| {
                        json!({
                            "name": name,
                            "type": value_kind_name(*kind),
                            "required": required,
                        })
                    })
                    .collect::<Vec<_>>()
            };
            let mut inputs = ports(definition.required_inputs, true);
            inputs.extend(ports(definition.optional_inputs, false));
            Some(json!({
                "type": node_type,
                "inputs": inputs,
                "outputs": ports(definition.outputs, false),
                "routes": definition.routes,
                "dynamicInputs": definition.dynamic_inputs,
                "dynamicOutputs": definition.dynamic_outputs,
                "dynamicRoutes": definition.dynamic_routes,
            }))
        })
        .collect::<Vec<_>>();
    // Structured flow nodes are executed by the runtime rather than the flat
    // planner catalog, but their visual shape is still part of the public DSL.
    catalog.extend([
        json!({ "type": "kakune.flow.switch@1", "inputs": [], "outputs": [], "routes": [], "dynamicInputs": false, "dynamicOutputs": false, "dynamicRoutes": true, "subgraph": "branches" }),
        json!({ "type": "kakune.flow.foreach@1", "inputs": [{"name":"items","type":"array","required":true}], "outputs": [{"name":"results","type":"array","required":false}], "routes": ["success"], "dynamicInputs": false, "dynamicOutputs": false, "dynamicRoutes": false, "subgraph": "body" }),
        json!({ "type": "kakune.flow.loop@1", "inputs": [], "outputs": [], "routes": ["success"], "dynamicInputs": true, "dynamicOutputs": true, "dynamicRoutes": false, "subgraph": "body" }),
    ]);
    catalog
}

fn value_kind_name(kind: ValueKind) -> &'static str {
    match kind {
        ValueKind::Any => "any",
        ValueKind::String => "string",
        ValueKind::Boolean => "boolean",
        ValueKind::Integer => "integer",
        ValueKind::Object => "object",
        ValueKind::Array => "array",
    }
}

pub(crate) fn compile(
    source: &str,
    plugins: &PluginRegistry,
) -> Result<CompiledWorkflow, Vec<Diagnostic>> {
    let analysis = analyze(source, plugins);
    if !analysis.diagnostics.is_empty() {
        return Err(analysis.diagnostics);
    }
    let workflow = analysis
        .workflow
        .expect("a diagnostic-free analysis always has a workflow");
    if let Err(error) = workflow.validate_with_plugins(plugins) {
        let index = YamlIndex::build(source);
        return Err(vec![diagnostic(
            source,
            &index,
            "catalog.unknown_node",
            error,
            Some("/nodes".to_string()),
        )]);
    }
    let by_id = workflow
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<HashMap<_, _>>();
    let nodes = workflow
        .nodes
        .iter()
        .map(|node| PlannedNode {
            id: node.id.clone(),
            node_type: node.node_type.clone(),
            data_dependencies: node
                .inputs
                .values()
                .filter_map(|binding| match binding {
                    WorkflowBinding::From { from } if !from.starts_with('$') => {
                        from.split_once('.').map(|(id, _)| id.to_string())
                    }
                    _ => None,
                })
                .collect(),
            control_dependencies: by_id
                .values()
                .filter(|candidate| {
                    candidate
                        .on
                        .values()
                        .any(|targets| targets.iter().any(|target| target == node.id))
                })
                .map(|candidate| candidate.id.clone())
                .collect(),
            node: node.clone(),
        })
        .collect();
    Ok(CompiledWorkflow {
        plan: ExecutionPlan {
            workflow_id: workflow.metadata.id.clone(),
            entry: workflow.entry.clone(),
            outputs: workflow.outputs.clone(),
            policy: workflow.policy.clone(),
            input_values: empty_object(),
            trigger_values: empty_object(),
            nodes,
        },
        ir: workflow,
    })
}

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

pub(crate) fn diagnostics_message(diagnostics: &[Diagnostic]) -> String {
    diagnostics
        .iter()
        .map(|diagnostic| diagnostic.message.as_str())
        .collect::<Vec<_>>()
        .join("; ")
}

fn validate_semantics(
    workflow: &WorkflowDocument,
    plugins: &PluginRegistry,
    source: &str,
    index: &YamlIndex<'_>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    if workflow.nodes.len() > MAX_NODES {
        diagnostics.push(diagnostic(
            source,
            index,
            "workflow.node_limit",
            format!("workflow exceeds the maximum of {MAX_NODES} nodes"),
            Some("/nodes".to_string()),
        ));
    }
    let edge_count = workflow
        .nodes
        .iter()
        .map(|node| {
            node.on
                .values()
                .map(crate::workflow::ControlTargets::len)
                .sum::<usize>()
        })
        .sum::<usize>();
    if edge_count > MAX_EDGES {
        diagnostics.push(diagnostic(
            source,
            index,
            "workflow.edge_limit",
            format!("workflow exceeds the maximum of {MAX_EDGES} control edges"),
            Some("/nodes".to_string()),
        ));
    }
    if let Some(timeout) = workflow.policy.timeout.as_deref()
        && !valid_duration(timeout)
    {
        diagnostics.push(diagnostic(
            source,
            index,
            "policy.invalid_timeout",
            "policy.timeout must be a positive duration using ms, s, m, or h",
            Some("/policy/timeout".to_string()),
        ));
    }
    let by_id = workflow
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<HashMap<_, _>>();
    let reachable = reachable_nodes(workflow);
    for (node_index, node) in workflow.nodes.iter().enumerate() {
        let pointer = format!("/nodes/{node_index}");
        if !reachable.contains(node.id.as_str()) {
            diagnostics.push(diagnostic(
                source,
                index,
                "workflow.unreachable_node",
                format!(
                    "node {} is not reachable from entry {}",
                    node.id, workflow.entry
                ),
                Some(pointer.clone()),
            ));
        }
        let definition = node_definition(&node.node_type);
        if definition.is_none()
            && !crate::runtime::is_builtin_node_type(&node.node_type)
            && plugins.get(&node.node_type).is_none()
        {
            diagnostics.push(diagnostic(
                source,
                index,
                "catalog.unknown_node",
                format!("node {} uses unknown type {}", node.id, node.node_type),
                Some(format!("{pointer}/type")),
            ));
            continue;
        }
        if let Some(definition) = definition {
            validate_node(node, definition, &pointer, source, index, &mut diagnostics);
            validate_routes(node, definition, &pointer, source, index, &mut diagnostics);
        }
        for (name, binding) in &node.inputs {
            validate_binding_reference(
                binding,
                name,
                node,
                &by_id,
                workflow,
                source,
                index,
                &pointer,
                &mut diagnostics,
            );
        }
    }
    for (name, binding) in &workflow.outputs {
        validate_output_binding(
            binding,
            name,
            &by_id,
            workflow,
            source,
            index,
            &mut diagnostics,
        );
    }
    diagnostics
}

#[derive(Clone, Copy)]
struct NodeDefinition {
    required_inputs: &'static [(&'static str, ValueKind)],
    optional_inputs: &'static [(&'static str, ValueKind)],
    outputs: &'static [(&'static str, ValueKind)],
    routes: &'static [&'static str],
    dynamic_inputs: bool,
    dynamic_outputs: bool,
    dynamic_routes: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ValueKind {
    Any,
    String,
    Boolean,
    Integer,
    Object,
    Array,
}

fn node_definition(node_type: &str) -> Option<NodeDefinition> {
    use ValueKind::{Any, Array, Boolean, Integer, Object, String};
    let definition = match node_type {
        "kakune.flow.end@1" => NodeDefinition {
            required_inputs: &[],
            optional_inputs: &[],
            outputs: &[],
            routes: &["success"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.log@1" => NodeDefinition {
            required_inputs: &[("message", String)],
            optional_inputs: &[],
            outputs: &[],
            routes: &["success"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.flow.pass@1" => NodeDefinition {
            required_inputs: &[],
            optional_inputs: &[],
            outputs: &[],
            routes: &["success"],
            dynamic_inputs: true,
            dynamic_outputs: true,
            dynamic_routes: false,
        },
        "kakune.flow.if@1" => NodeDefinition {
            required_inputs: &[("condition", Boolean)],
            optional_inputs: &[],
            outputs: &[],
            routes: &["true", "false"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.flow.delay@1" => NodeDefinition {
            required_inputs: &[("durationMs", Integer)],
            optional_inputs: &[],
            outputs: &[],
            routes: &["success"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.flow.join@1" | "kakune.flow.merge@1" => NodeDefinition {
            required_inputs: &[],
            optional_inputs: &[],
            outputs: &[],
            routes: &["success"],
            dynamic_inputs: true,
            dynamic_outputs: true,
            dynamic_routes: false,
        },
        "kakune.flow.set-variable@1" => NodeDefinition {
            required_inputs: &[("value", Any)],
            optional_inputs: &[],
            outputs: &[("value", Any)],
            routes: &["success"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.fs.write-text@1" => NodeDefinition {
            required_inputs: &[("path", String), ("content", String)],
            optional_inputs: &[],
            outputs: &[("path", String)],
            routes: &["success"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.fs.read-text@1" => NodeDefinition {
            required_inputs: &[("path", String)],
            optional_inputs: &[],
            outputs: &[("text", String)],
            routes: &["success"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.fs.copy@1" | "kakune.fs.move@1" => NodeDefinition {
            required_inputs: &[("source", String), ("destination", String)],
            optional_inputs: &[],
            outputs: &[("source", String), ("destination", String)],
            routes: &["success"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.http.request@1" => NodeDefinition {
            required_inputs: &[("url", String)],
            optional_inputs: &[("headers", Object), ("body", Any)],
            outputs: &[("status", Integer), ("headers", Object), ("body", Any)],
            routes: &["success", "httpError"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.process.run@1" => NodeDefinition {
            required_inputs: &[],
            optional_inputs: &[("args", Array), ("stdin", String)],
            outputs: &[
                ("stdout", String),
                ("stderr", String),
                ("exitCode", Integer),
            ],
            routes: &["success", "exitError"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.ai.generate@1" => NodeDefinition {
            required_inputs: &[("maxTokens", Integer), ("prompt", String)],
            optional_inputs: &[],
            outputs: &[
                ("text", String),
                ("usage", Object),
                ("modelRequested", String),
                ("modelReported", String),
            ],
            routes: &["success"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.ai.minimax.messages@1" => NodeDefinition {
            required_inputs: &[("maxTokens", Integer), ("messages", Array)],
            optional_inputs: &[],
            outputs: &[
                ("content", Array),
                ("raw", Any),
                ("usage", Object),
                ("modelRequested", String),
                ("modelReported", String),
            ],
            routes: &["success"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.ai.extract@1" => NodeDefinition {
            required_inputs: &[
                ("maxTokens", Integer),
                ("prompt", String),
                ("schema", Object),
            ],
            optional_inputs: &[],
            outputs: &[("value", Any), ("usage", Object)],
            routes: &["success"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.ai.boolean@1" => NodeDefinition {
            required_inputs: &[("maxTokens", Integer), ("prompt", String)],
            optional_inputs: &[],
            outputs: &[("value", Boolean), ("usage", Object)],
            routes: &["true", "false"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.ai.choose@1" => NodeDefinition {
            required_inputs: &[
                ("maxTokens", Integer),
                ("prompt", String),
                ("choices", Array),
            ],
            optional_inputs: &[],
            outputs: &[("choice", String), ("usage", Object)],
            routes: &[],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: true,
        },
        "kakune.ai.agent@1" => NodeDefinition {
            required_inputs: &[("maxTokens", Integer), ("task", String), ("schema", Object)],
            optional_inputs: &[("tools", Array)],
            outputs: &[("result", Any), ("usage", Object), ("steps", Array)],
            routes: &["success"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.ai.codex.exec@1" => NodeDefinition {
            required_inputs: &[("prompt", String)],
            optional_inputs: &[],
            outputs: &[("output", Any), ("usage", Array)],
            routes: &["success"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.ai.agent-result@1" => NodeDefinition {
            required_inputs: &[("value", Any), ("schema", Object)],
            optional_inputs: &[],
            outputs: &[("result", Any)],
            routes: &["success"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        "kakune.mcp.call@1" => NodeDefinition {
            required_inputs: &[("tool", String), ("arguments", Object)],
            optional_inputs: &[("environment", Object)],
            outputs: &[
                ("content", Array),
                ("isError", Boolean),
                ("structuredContent", Any),
            ],
            routes: &["success"],
            dynamic_inputs: false,
            dynamic_outputs: false,
            dynamic_routes: false,
        },
        _ => return None,
    };
    Some(definition)
}

fn validate_node(
    node: &WorkflowNode,
    definition: NodeDefinition,
    pointer: &str,
    source: &str,
    index: &YamlIndex<'_>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for (name, _) in definition.required_inputs {
        if !node.inputs.contains_key(*name) {
            diagnostics.push(diagnostic(
                source,
                index,
                "catalog.required_input",
                format!("node {} requires input {name}", node.id),
                Some(format!("{pointer}/inputs")),
            ));
        }
    }
    if !definition.dynamic_inputs {
        for (name, binding) in &node.inputs {
            let expected = definition
                .required_inputs
                .iter()
                .chain(definition.optional_inputs)
                .find_map(|(known, kind)| (*known == name).then_some(*kind));
            match expected {
                None => diagnostics.push(diagnostic(
                    source,
                    index,
                    "catalog.unknown_input",
                    format!("node {} does not expose input {name}", node.id),
                    Some(format!("{pointer}/inputs/{name}")),
                )),
                Some(kind) => validate_literal_type(
                    binding,
                    kind,
                    source,
                    index,
                    format!("{pointer}/inputs/{name}"),
                    diagnostics,
                ),
            }
        }
    }
    if node.node_type == "kakune.process.run@1"
        && !node.with.get("command").is_some_and(Value::is_string)
    {
        diagnostics.push(diagnostic(
            source,
            index,
            "process.command",
            format!("node {} requires with.command as a string", node.id),
            Some(format!("{pointer}/with/command")),
        ));
    }
    if matches!(
        node.node_type.as_str(),
        "kakune.http.request@1" | "kakune.process.run@1"
    ) && let Some(timeout) = node.with.get("timeoutSeconds")
    {
        let maximum = if node.node_type == "kakune.http.request@1" {
            120
        } else {
            3_600
        };
        if !timeout
            .as_u64()
            .is_some_and(|value| (1..=maximum).contains(&value))
        {
            diagnostics.push(diagnostic(
                source,
                index,
                "catalog.invalid_timeout",
                format!(
                    "node {} timeoutSeconds must be an integer from 1 to {maximum}",
                    node.id
                ),
                Some(format!("{pointer}/with/timeoutSeconds")),
            ));
        }
    }
}

fn validate_routes(
    node: &WorkflowNode,
    definition: NodeDefinition,
    pointer: &str,
    source: &str,
    index: &YamlIndex<'_>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for route in node.on.keys() {
        if !definition.dynamic_routes && !definition.routes.contains(&route.as_str()) {
            diagnostics.push(diagnostic(
                source,
                index,
                "catalog.unknown_route",
                format!("node {} does not expose route {route}", node.id),
                Some(format!("{pointer}/on/{route}")),
            ));
        }
    }
}

fn validate_literal_type(
    binding: &WorkflowBinding,
    expected: ValueKind,
    source: &str,
    index: &YamlIndex<'_>,
    pointer: String,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let WorkflowBinding::Literal { literal } = binding else {
        return;
    };
    let matches = match expected {
        ValueKind::Any => true,
        ValueKind::String => literal.is_string(),
        ValueKind::Boolean => literal.is_boolean(),
        ValueKind::Integer => literal.as_i64().is_some() || literal.as_u64().is_some(),
        ValueKind::Object => literal.is_object(),
        ValueKind::Array => literal.is_array(),
    };
    if !matches {
        diagnostics.push(diagnostic(
            source,
            index,
            "catalog.incompatible_literal",
            "literal does not match the input type",
            Some(pointer),
        ));
    }
}

#[allow(clippy::too_many_arguments)]
fn validate_binding_reference(
    binding: &WorkflowBinding,
    name: &str,
    consumer: &WorkflowNode,
    by_id: &HashMap<&str, &WorkflowNode>,
    workflow: &WorkflowDocument,
    source: &str,
    index: &YamlIndex<'_>,
    pointer: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let WorkflowBinding::From { from } = binding else {
        return;
    };
    if from.starts_with('$') {
        return;
    }
    let Some((producer_id, output)) = from.split_once('.') else {
        return;
    };
    let Some(producer) = by_id.get(producer_id) else {
        return;
    };
    if !control_reaches(workflow, producer_id, &consumer.id) {
        diagnostics.push(diagnostic(
            source,
            index,
            "binding.unavailable",
            format!(
                "binding source {from} is not guaranteed before node {}",
                consumer.id
            ),
            Some(format!("{pointer}/inputs/{name}/from")),
        ));
    }
    if let Some(definition) = node_definition(&producer.node_type)
        && !definition.dynamic_outputs
        && !definition.outputs.iter().any(|(known, _)| *known == output)
    {
        diagnostics.push(diagnostic(
            source,
            index,
            "binding.unknown_output",
            format!("node {producer_id} does not expose output {output}"),
            Some(format!("{pointer}/inputs/{name}/from")),
        ));
    }
}

fn validate_output_binding(
    binding: &WorkflowBinding,
    name: &str,
    by_id: &HashMap<&str, &WorkflowNode>,
    workflow: &WorkflowDocument,
    source: &str,
    index: &YamlIndex<'_>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let WorkflowBinding::From { from } = binding else {
        return;
    };
    let Some((node_id, output)) = from.split_once('.') else {
        return;
    };
    let Some(node) = by_id.get(node_id) else {
        return;
    };
    if !reachable_nodes(workflow).contains(node_id) {
        diagnostics.push(diagnostic(
            source,
            index,
            "binding.unreachable_output",
            format!("workflow output {name} references unreachable node {node_id}"),
            Some(format!("/outputs/{name}/from")),
        ));
    }
    if let Some(definition) = node_definition(&node.node_type)
        && !definition.dynamic_outputs
        && !definition.outputs.iter().any(|(known, _)| *known == output)
    {
        diagnostics.push(diagnostic(
            source,
            index,
            "binding.unknown_output",
            format!("node {node_id} does not expose output {output}"),
            Some(format!("/outputs/{name}/from")),
        ));
    }
}

fn reachable_nodes(workflow: &WorkflowDocument) -> HashSet<&str> {
    let by_id = workflow
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<HashMap<_, _>>();
    let mut pending = vec![workflow.entry.as_str()];
    let mut reachable = HashSet::new();
    while let Some(id) = pending.pop() {
        if !reachable.insert(id) {
            continue;
        }
        if let Some(node) = by_id.get(id) {
            pending.extend(
                node.on
                    .values()
                    .flat_map(crate::workflow::ControlTargets::iter),
            );
        }
    }
    reachable
}

fn control_reaches(workflow: &WorkflowDocument, from: &str, target: &str) -> bool {
    let by_id = workflow
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect::<HashMap<_, _>>();
    let mut pending = vec![from];
    let mut visited = HashSet::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        if id == target {
            return true;
        }
        if let Some(node) = by_id.get(id) {
            pending.extend(
                node.on
                    .values()
                    .flat_map(crate::workflow::ControlTargets::iter),
            );
        }
    }
    false
}

fn diagnostic(
    source: &str,
    index: &YamlIndex,
    code: impl Into<String>,
    message: impl Into<String>,
    pointer: Option<String>,
) -> Diagnostic {
    let range = pointer
        .as_deref()
        .and_then(|pointer| index.range_for_pointer(source, pointer));
    Diagnostic::error(code, message, pointer, range)
}

/// A pre-parsed YAML document with the spans needed to resolve diagnostic
/// pointers into real source positions.  `WorkflowDocument::parse` already
/// consumes the Saphyr output; we keep a marked copy to power the
/// diagnostics without a second parse.
struct YamlIndex<'a> {
    root: Option<MarkedYaml<'a>>,
}

impl<'a> YamlIndex<'a> {
    fn build(source: &'a str) -> Self {
        let root = MarkedYaml::load_from_str(source)
            .ok()
            .and_then(|mut docs| docs.pop());
        Self { root }
    }

    /// Resolves a JSON pointer into a concrete source range by walking the
    /// marked YAML tree.  Returns `None` when the path cannot be located so
    /// callers can distinguish "we know exactly where" from "we don't".
    fn range_for_pointer(&self, source: &str, pointer: &str) -> Option<SourceRange> {
        let root = self.root.as_ref()?;
        let segments: Vec<String> = pointer
            .split('/')
            .skip(1)
            .map(decode_pointer_segment)
            .collect();
        if segments.is_empty() {
            return Some(span_to_range(
                source,
                root.span.start.index(),
                root.span.end.index(),
            ));
        }
        let mut node = root;
        let mut key_to_match: &str = segments.first()?.as_str();
        let mut last_key_span = (root.span.start.index(), root.span.end.index());
        for segment in &segments[1..] {
            match &node.data {
                YamlData::Mapping(entries) => {
                    let (key, value) = entries.iter().find(|(map_key, _)| {
                        map_key_span_key(map_key).as_deref() == Some(key_to_match)
                    })?;
                    last_key_span = (key.span.start.index(), key.span.end.index());
                    node = value;
                }
                YamlData::Sequence(items) => {
                    let index: usize = key_to_match.parse().ok()?;
                    node = items.get(index)?;
                }
                _ => return None,
            }
            key_to_match = segment.as_str();
        }
        // On the final segment, return the key/element span if the path ends
        // with a named property, otherwise the value span for a numeric index.
        match &node.data {
            YamlData::Mapping(entries) => entries
                .iter()
                .find(|(map_key, _)| map_key_span_key(map_key).as_deref() == Some(key_to_match))
                .map(|(map_key, _)| {
                    span_to_range(source, map_key.span.start.index(), map_key.span.end.index())
                }),
            YamlData::Sequence(items) => key_to_match
                .parse::<usize>()
                .ok()
                .and_then(|index| items.get(index))
                .map(|item| span_to_range(source, item.span.start.index(), item.span.end.index())),
            _ => Some(span_to_range(source, last_key_span.0, last_key_span.1)),
        }
    }
}

fn decode_pointer_segment(segment: &str) -> String {
    segment.replace("~1", "/").replace("~0", "~")
}

fn map_key_span_key(node: &MarkedYaml<'_>) -> Option<String> {
    if let Some(value) = node.data.as_str() {
        return Some(value.to_string());
    }
    if let YamlData::Representation(cow, _, _) = &node.data {
        return Some(cow.to_string());
    }
    None
}

fn span_to_range(source: &str, start: usize, end: usize) -> SourceRange {
    let start = start.min(source.len());
    let end = end.min(source.len()).max(start);
    range_at(source, start, end - start)
}

#[cfg(test)]
mod tests_diagnostic_ranges {
    use super::YamlIndex;

    const SOURCE: &str = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: x\n  name: X\ntriggers:\n  - id: t\n    type: kakune.trigger.manual@1\nentry: write\nnodes:\n  - id: write\n    type: kakune.fs.write-text@1\n    inputs:\n      path: { literal: notes/hello.txt }\n      content: { literal: hi }\npolicy:\n  timeout: 30s\noutputs:\n  greeting: { from: write.path }\n";

    #[test]
    fn resolves_a_named_key_to_its_real_offset() {
        let index = YamlIndex::build(SOURCE);
        let range = index
            .range_for_pointer(SOURCE, "/policy/timeout")
            .expect("named key resolves");
        assert!(
            range.start.byte_offset > 0,
            "range should be past the header"
        );
        let snippet = &SOURCE[range.start.byte_offset as usize..range.end.byte_offset as usize];
        assert!(
            snippet.contains("timeout") || snippet.contains("30s"),
            "range should overlap the timeout field; got {snippet:?}"
        );
    }

    #[test]
    fn returns_none_for_unknown_keys() {
        let index = YamlIndex::build(SOURCE);
        assert!(
            index
                .range_for_pointer(SOURCE, "/nodes/0/inputs/missing")
                .is_none()
        );
    }
}

fn valid_duration(value: &str) -> bool {
    let value = value.trim();
    let Some(index) = value
        .chars()
        .position(|character| !character.is_ascii_digit())
    else {
        return false;
    };
    matches!(&value[index..], "ms" | "s" | "m" | "h")
        && value[..index].parse::<u64>().is_ok_and(|amount| amount > 0)
}

#[cfg(test)]
mod tests {
    use crate::PluginRegistry;

    use super::analyze;

    #[test]
    fn returns_pointers_for_semantic_errors_without_executing() {
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: invalid\n  name: Invalid\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: write\nnodes:\n  - id: write\n    type: kakune.fs.write-text@1\n    inputs:\n      path: { literal: file.txt }\n      unexpected: { literal: value }\n";
        let analysis = analyze(source, &PluginRegistry::default());
        let diagnostic = analysis
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "catalog.required_input")
            .expect("missing input should diagnose");
        assert_eq!(diagnostic.pointer.as_deref(), Some("/nodes/0/inputs"));
        assert!(diagnostic.range.is_some());
    }

    #[test]
    fn rejects_unreachable_references_during_planning() {
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: bindings\n  name: Bindings\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: first\nnodes:\n  - id: first\n    type: kakune.log@1\n    inputs: { message: { literal: first } }\n  - id: later\n    type: kakune.fs.read-text@1\n    inputs: { path: { literal: note.txt } }\n  - id: write\n    type: kakune.fs.write-text@1\n    inputs:\n      path: { literal: output.txt }\n      content: { from: later.text }\n";
        let analysis = analyze(source, &PluginRegistry::default());
        assert!(
            analysis
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "binding.unavailable")
        );
    }

    #[test]
    fn catalogs_configured_ai_nodes_without_credential_or_model_inputs() {
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: configured-ai\n  name: Configured AI\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: generate\nnodes:\n  - id: generate\n    type: kakune.ai.generate@1\n    with:\n      provider: minimax\n      model: MiniMax-M3\n    inputs:\n      maxTokens: { literal: 32 }\n      prompt: { literal: hello }\n";
        let analysis = analyze(source, &PluginRegistry::default());
        assert!(analysis.is_valid(), "{:?}", analysis.diagnostics);
    }

    #[test]
    fn catalogs_codex_and_mcp_builtin_contracts() {
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: builtins\n  name: Builtins\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: codex\nnodes:\n  - id: codex\n    type: kakune.ai.codex.exec@1\n    with:\n      provider: codex\n      model: gpt-5\n    inputs:\n      prompt: { literal: hello }\n    on: { success: mcp }\n  - id: mcp\n    type: kakune.mcp.call@1\n    with:\n      command: mcp-server\n    inputs:\n      tool: { literal: echo }\n      arguments: { literal: {} }\n";
        let analysis = analyze(source, &PluginRegistry::default());
        assert!(analysis.is_valid(), "{:?}", analysis.diagnostics);
    }
}
