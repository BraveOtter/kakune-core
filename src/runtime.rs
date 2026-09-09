use std::{
    collections::HashSet,
    fs,
    path::{Component, Path},
};

use serde_json::Value;

use crate::{ExecutionRecord, Store, WorkflowDocument, workflow::WorkflowStep};

pub fn run_workflow(store: &Store, workflow: &WorkflowDocument) -> Result<ExecutionRecord, String> {
    let execution = store.create_execution(&workflow.metadata.name)?;
    let outcome = execute_steps(store, &execution.id, &workflow.spec.steps);
    match outcome {
        Ok(()) => {
            store.finish_execution(&execution.id, "succeeded", None)?;
            Ok(ExecutionRecord {
                status: "succeeded".to_string(),
                completed_at: Some(timestamp()?),
                ..execution
            })
        }
        Err(error) => {
            store.finish_execution(&execution.id, "failed", Some(&error))?;
            Err(error)
        }
    }
}

fn execute_steps(store: &Store, execution_id: &str, steps: &[WorkflowStep]) -> Result<(), String> {
    let mut completed = HashSet::new();
    while completed.len() < steps.len() {
        let next = steps
            .iter()
            .find(|step| {
                !completed.contains(step.id.as_str())
                    && step
                        .needs
                        .iter()
                        .all(|need| completed.contains(need.as_str()))
            })
            .ok_or_else(|| {
                "workflow cannot make progress; dependency plan is invalid".to_string()
            })?;
        match execute_step(store, next) {
            Ok(message) => {
                store.record_step_run(execution_id, &next.id, "succeeded", message.as_deref())?;
                completed.insert(next.id.as_str());
            }
            Err(error) => {
                store.record_step_run(execution_id, &next.id, "failed", Some(&error))?;
                return Err(format!("step {} failed: {error}", next.id));
            }
        }
    }
    Ok(())
}

fn execute_step(store: &Store, step: &WorkflowStep) -> Result<Option<String>, String> {
    if step.plugin != "@kakune/core" {
        return Err(format!("plugin {} is not installed", step.plugin));
    }
    let action = required_string(&step.with, "action")?;
    match action {
        "log" => Ok(Some(required_string(&step.with, "message")?.to_owned())),
        "write-file" => {
            let relative_path = required_string(&step.with, "path")?;
            let content = required_string(&step.with, "content")?;
            let path = workspace_path(&store.workspace_dir()?, relative_path)?;
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)
                    .map_err(|error| format!("cannot create parent directory: {error}"))?;
            }
            fs::write(&path, content)
                .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
            Ok(Some(format!("wrote {}", relative_path)))
        }
        "read-file" => {
            let relative_path = required_string(&step.with, "path")?;
            let path = workspace_path(&store.workspace_dir()?, relative_path)?;
            let content = fs::read_to_string(&path)
                .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
            Ok(Some(content))
        }
        other => Err(format!("@kakune/core does not provide action {other}")),
    }
}

fn required_string<'a>(
    values: &'a serde_json::Map<String, Value>,
    name: &str,
) -> Result<&'a str, String> {
    values
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("with.{name} must be a string"))
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
        return Err("with.path must be a relative path inside the Kakune workspace".to_string());
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
    use std::fs;

    use crate::{Store, WorkflowDocument, run_workflow};

    #[test]
    fn runs_native_write_file_step() {
        let directory =
            std::env::temp_dir().join(format!("kakune-runtime-test-{}", uuid::Uuid::new_v4()));
        let store = Store::open(directory.clone()).expect("store should open");
        let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  name: write\nspec:\n  triggers:\n    manual: {}\n  steps:\n    - id: write\n      plugin: '@kakune/core'\n      with:\n        action: write-file\n        path: notes/hello.txt\n        content: hello\n";
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
}
