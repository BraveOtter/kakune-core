use std::collections::HashSet;

use saphyr::{LoadableYamlNode, Yaml};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowDocument {
    pub api_version: String,
    pub kind: String,
    pub metadata: WorkflowMetadata,
    pub spec: WorkflowSpec,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkflowMetadata {
    pub name: String,
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkflowSpec {
    pub triggers: std::collections::BTreeMap<String, Value>,
    pub steps: Vec<WorkflowStep>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WorkflowStep {
    pub id: String,
    pub plugin: String,
    #[serde(default)]
    pub with: Map<String, Value>,
    #[serde(default)]
    pub needs: Vec<String>,
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
        let document = documents.pop().expect("one document was checked above");
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
        validate_id("metadata.name", &self.metadata.name)?;
        if self.spec.triggers.is_empty() {
            return Err("spec.triggers must contain at least one trigger".to_string());
        }
        if self.spec.steps.is_empty() {
            return Err("spec.steps must contain at least one step".to_string());
        }

        let mut ids = HashSet::new();
        for step in &self.spec.steps {
            validate_id("step.id", &step.id)?;
            if !ids.insert(step.id.as_str()) {
                return Err(format!("duplicate step id: {}", step.id));
            }
            if !is_plugin_id(&step.plugin) {
                return Err(format!("step {} has an invalid plugin id", step.id));
            }
            let mut needs = HashSet::new();
            for dependency in &step.needs {
                if dependency == &step.id {
                    return Err(format!("step {} cannot depend on itself", step.id));
                }
                if !needs.insert(dependency.as_str()) {
                    return Err(format!("step {} repeats dependency {dependency}", step.id));
                }
            }
        }
        for step in &self.spec.steps {
            for dependency in &step.needs {
                if !ids.contains(dependency.as_str()) {
                    return Err(format!(
                        "step {} depends on unknown step {dependency}",
                        step.id
                    ));
                }
            }
        }
        ensure_acyclic(&self.spec.steps)
    }
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

fn is_plugin_id(value: &str) -> bool {
    let Some((scope, name)) = value
        .strip_prefix('@')
        .and_then(|value| value.split_once('/'))
    else {
        return false;
    };
    !scope.is_empty()
        && !name.is_empty()
        && scope.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
        && name.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-'
        })
}

fn ensure_acyclic(steps: &[WorkflowStep]) -> Result<(), String> {
    fn visit<'a>(
        id: &'a str,
        by_id: &std::collections::HashMap<&'a str, &'a WorkflowStep>,
        visiting: &mut HashSet<&'a str>,
        visited: &mut HashSet<&'a str>,
    ) -> Result<(), String> {
        if visited.contains(id) {
            return Ok(());
        }
        if !visiting.insert(id) {
            return Err(format!("workflow contains a dependency cycle at {id}"));
        }
        let step = by_id
            .get(id)
            .expect("dependencies were validated before cycle detection");
        for dependency in &step.needs {
            visit(dependency, by_id, visiting, visited)?;
        }
        visiting.remove(id);
        visited.insert(id);
        Ok(())
    }

    let by_id = steps.iter().map(|step| (step.id.as_str(), step)).collect();
    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    for step in steps {
        visit(&step.id, &by_id, &mut visiting, &mut visited)?;
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
            // Saphyr deliberately preserves mapping keys as raw representations so
            // duplicate YAML keys can be detected before scalar normalization.
            let key = key
                .as_cow()
                .map(|value| value.as_ref())
                .or_else(|| key.as_str())
                .ok_or_else(|| "workflow mapping keys must be strings".to_string())?;
            if output
                .insert(key.to_owned(), yaml_to_json(value)?)
                .is_some()
            {
                return Err(format!("workflow contains duplicate key: {key}"));
            }
        }
        return Ok(Value::Object(output));
    }

    // Workflow metadata and first-party plugin inputs are string-oriented in this
    // milestone. Preserving scalar text also avoids normalizing mapping-key-like
    // values before the schema layer validates them.
    if let Some(value) = node.as_cow() {
        return Ok(Value::String(value.to_string()));
    }
    if let Some(value) = node.as_str() {
        return Ok(Value::String(value.to_owned()));
    }
    let value = node.clone();
    if let Some(value) = value.as_bool() {
        return Ok(Value::Bool(value));
    }
    if let Some(value) = value.as_integer() {
        return Ok(Value::Number(value.into()));
    }
    if let Some(value) = value.as_floating_point() {
        return Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| "workflow contains a non-finite number".to_string());
    }
    if value.is_null() {
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
  name: write-note
spec:
  triggers:
    manual: {}
  steps:
    - id: note
      plugin: "@kakune/core"
      with:
        action: log
        message: hello
"#;

    #[test]
    fn parses_a_valid_workflow() {
        let workflow = WorkflowDocument::parse(VALID).expect("workflow should parse");
        assert_eq!(workflow.metadata.name, "write-note");
    }

    #[test]
    fn rejects_dependency_cycles() {
        let source = VALID.replace(
            "plugin: \"@kakune/core\"",
            "plugin: \"@kakune/core\"\n      needs: [note]",
        );
        assert!(WorkflowDocument::parse(&source).is_err());
    }
}
