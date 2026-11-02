use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Barrier},
    thread,
};

use kakune_core::{
    Store, WorkflowDocument,
    plugin_install::{self, PluginSource},
    plugin_services,
};
use rusqlite::{Connection, params};
use serde_json::json;
use uuid::Uuid;

fn temp_dir(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join("opencode");
    fs::create_dir_all(&root).expect("approved temporary root exists");
    let path = root.join(format!("kakune-plugin-{label}-{}", Uuid::new_v4()));
    fs::create_dir(&path).expect("isolated fixture directory is created");
    path
}

fn copy_tree(source: &std::path::Path, target: &std::path::Path) {
    fs::create_dir_all(target).expect("fixture directory is created");
    for entry in fs::read_dir(source).expect("fixture source can be read") {
        let entry = entry.expect("fixture entry can be read");
        let path = entry.path();
        let destination = target.join(entry.file_name());
        if path.is_dir() {
            copy_tree(&path, &destination);
        } else {
            fs::copy(path, destination).expect("fixture file is copied");
        }
    }
}

#[tokio::test]
async fn pending_removal_disables_nested_dependents_survives_restart_and_finalizes_after_use() {
    let source_dir = temp_dir("remove-source");
    let data_dir = temp_dir("remove-data");
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/canonical_commands/valid");
    copy_tree(&fixture, &source_dir);
    let workflow_source = include_str!("fixtures/canonical_commands/nested-plugin.kakune.yaml");
    let workflow: WorkflowDocument =
        serde_yaml::from_str(workflow_source).expect("nested workflow parses");

    let store = Store::open(data_dir.clone()).expect("isolated Store opens");
    let prepared = plugin_install::prepare(&store, PluginSource::Local(source_dir.clone()))
        .await
        .expect("plugin package stages");
    let plugin = plugin_install::commit(&store, &prepared.record.id, &prepared.record.digest)
        .expect("plugin package installs");
    let workflow_record = store
        .create_workflow(&workflow, workflow_source, "enabled")
        .expect("workflow stores")
        .expect("workflow is new");
    let execution = store
        .create_execution(&workflow.metadata.id)
        .expect("manual execution is admitted");
    store
        .acquire_plugin_uses(&execution.id, &[&plugin.id])
        .expect("execution pins installed plugin content");

    let report =
        plugin_services::request_removal(&store, &plugin.id).expect("removal request is published");
    assert_eq!(report.status, "pending removal");
    assert_eq!(report.active_executions, 1);
    assert_eq!(
        report.affected_workflows.as_slice(),
        std::slice::from_ref(&workflow.metadata.id)
    );
    assert_eq!(
        store
            .get_workflow_source(&workflow_record.id)
            .unwrap()
            .unwrap()
            .source,
        workflow_source,
        "removal must not rewrite workflow YAML"
    );
    assert_eq!(
        store.list_workflows().unwrap()[0].status,
        "disabled",
        "dependent triggers are disabled atomically"
    );
    let blocked = store.admit_plan_with_plugin_uses(
        &workflow.metadata.id,
        &workflow_record.revision,
        &json!({"policy":{}}),
        &[&plugin.id],
        None,
        false,
    );
    assert!(
        blocked.is_err(),
        "pending removal also rejects manual admission"
    );
    let automatic = store.admit_plan_with_plugin_uses(
        &workflow.metadata.id,
        &workflow_record.revision,
        &json!({"policy":{}}),
        &[&plugin.id],
        None,
        true,
    );
    assert!(
        automatic.is_err(),
        "pending removal also rejects trigger admission"
    );
    assert!(
        store
            .set_workflow_status(&workflow.metadata.id, "enabled")
            .is_err(),
        "pending removal cannot re-enable a dependent workflow"
    );
    assert_eq!(store.list_workflows().unwrap()[0].status, "disabled");

    drop(store);
    let reopened = Store::open(data_dir.clone()).expect("Store reopens without reset");
    assert_eq!(
        reopened
            .plugin_removal_state(&plugin.id)
            .unwrap()
            .unwrap()
            .status,
        "pending removal",
        "pending intent survives Core restart"
    );
    assert!(
        plugin_services::reconcile_pending_removals(&reopened)
            .unwrap()
            .is_empty(),
        "pinned execution keeps removal pending"
    );
    reopened
        .finish_execution(&execution.id, "succeeded", None)
        .expect("terminal execution transition releases its pin and reconciles");
    assert!(reopened.list_installed_plugins().unwrap().is_empty());
    assert_eq!(reopened.list_workflows().unwrap()[0].status, "disabled");

    let reinstalled = plugin_install::prepare(&reopened, PluginSource::Local(source_dir.clone()))
        .await
        .expect("plugin can be reinstalled");
    plugin_install::commit(
        &reopened,
        &reinstalled.record.id,
        &reinstalled.record.digest,
    )
    .expect("reinstall succeeds");
    assert_eq!(reopened.list_workflows().unwrap()[0].status, "disabled");

    drop(reopened);
    fs::remove_dir_all(source_dir).expect("source fixture is cleaned");
    fs::remove_dir_all(data_dir).expect("data fixture is cleaned");
}

#[tokio::test]
async fn plugin_removal_keeps_pins_for_queued_running_cancelling_and_recoverable_work() {
    let source_dir = temp_dir("pin-states-source");
    let data_dir = temp_dir("pin-states-data");
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/canonical_commands/valid");
    copy_tree(&fixture, &source_dir);
    let store = Store::open(data_dir.clone()).expect("isolated Store opens");
    let prepared = plugin_install::prepare(&store, PluginSource::Local(source_dir.clone()))
        .await
        .expect("plugin package stages");
    let plugin = plugin_install::commit(&store, &prepared.record.id, &prepared.record.digest)
        .expect("plugin package activates");

    let connection =
        Connection::open(data_dir.join("kakune.sqlite3")).expect("test database opens");
    let mut executions = Vec::new();
    for status in ["queued", "running", "cancelling", "recoverable"] {
        let execution = store
            .create_execution("pin-state-fixture")
            .expect("execution is created");
        connection
            .execute(
                "UPDATE executions SET status=?1 WHERE id=?2",
                params![status, execution.id],
            )
            .expect("fixture execution enters the requested durable state");
        store
            .acquire_plugin_uses(&execution.id, &[&plugin.id])
            .expect("nonterminal execution pins its plugin content");
        executions.push(execution.id);
    }
    assert_eq!(store.plugin_execution_pin_count(&plugin.id).unwrap(), 4);

    let active_executions = store
        .request_plugin_removal(&plugin.id, &[])
        .expect("removal barrier is published");
    assert_eq!(active_executions, 4);
    assert!(store.finalize_plugin_removal(&plugin.id).is_err());
    assert_eq!(store.plugin_execution_pin_count(&plugin.id).unwrap(), 4);

    for execution in executions {
        connection
            .execute(
                "UPDATE executions SET status='running' WHERE id=?1",
                [&execution],
            )
            .expect("fixture transitions to a terminalizable running state");
        store
            .finish_execution(&execution, "succeeded", None)
            .expect("terminal transition releases the pin");
    }
    assert!(store.list_installed_plugins().unwrap().is_empty());
    drop(connection);
    drop(store);
    fs::remove_dir_all(source_dir).expect("source fixture is cleaned");
    fs::remove_dir_all(data_dir).expect("data fixture is cleaned");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_plugin_admission_and_removal_are_serialized() {
    let source_dir = temp_dir("race-source");
    let data_dir = temp_dir("race-data");
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/canonical_commands/valid");
    copy_tree(&fixture, &source_dir);
    let workflow_source = include_str!("fixtures/canonical_commands/nested-plugin.kakune.yaml");
    let workflow: WorkflowDocument =
        serde_yaml::from_str(workflow_source).expect("workflow parses");
    let store = Store::open(data_dir.clone()).expect("isolated Store opens");
    let prepared = plugin_install::prepare(&store, PluginSource::Local(source_dir.clone()))
        .await
        .expect("plugin package stages");
    let plugin = plugin_install::commit(&store, &prepared.record.id, &prepared.record.digest)
        .expect("plugin package activates");
    let workflow_record = store
        .create_workflow(&workflow, workflow_source, "enabled")
        .expect("dependent workflow stores")
        .expect("workflow is new");

    let barrier = Arc::new(Barrier::new(3));
    let admission_store = store.clone();
    let admission_barrier = Arc::clone(&barrier);
    let plugin_id = plugin.id.clone();
    let workflow_id = workflow.metadata.id.clone();
    let revision = workflow_record.revision.clone();
    let admission = thread::spawn(move || {
        admission_barrier.wait();
        admission_store.admit_plan_with_plugin_uses(
            &workflow_id,
            &revision,
            &json!({"policy":{}}),
            &[&plugin_id],
            None,
            true,
        )
    });

    let removal_store = store.clone();
    let removal_barrier = Arc::clone(&barrier);
    let removal_id = plugin.id.clone();
    let removal = thread::spawn(move || {
        removal_barrier.wait();
        plugin_services::request_removal(&removal_store, &removal_id)
    });
    barrier.wait();

    let admission = admission.join().expect("admission worker joins");
    let removal = removal
        .join()
        .expect("removal worker joins")
        .expect("removal is reported");
    match admission {
        Ok((execution, _)) => {
            assert_eq!(removal.status, "pending removal");
            assert_eq!(removal.active_executions, 1);
            assert_eq!(store.plugin_execution_pin_count(&plugin.id).unwrap(), 1);
            store
                .finish_execution(&execution.id, "succeeded", None)
                .expect("terminal transition reconciles pending removal");
        }
        Err(_) => {
            assert_eq!(removal.status, "removed");
            assert_eq!(store.plugin_execution_pin_count(&plugin.id).unwrap(), 0);
        }
    }
    assert!(store.list_installed_plugins().unwrap().is_empty());
    drop(store);
    fs::remove_dir_all(source_dir).expect("source fixture is cleaned");
    fs::remove_dir_all(data_dir).expect("data fixture is cleaned");
}

#[test]
fn failed_removal_is_observable_and_explicit_retry_does_not_enable_workflows() {
    let package =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/canonical_commands/valid");
    let data = temp_dir("remove-failure-data");
    let store = Store::open(data.clone()).expect("isolated Store opens");
    let source = fs::read_to_string(package.join("kakune-plugin.json")).unwrap();
    let manifest: serde_json::Value = serde_json::from_str(&source).unwrap();
    let manifest_path = package.join("kakune-plugin.json");
    let record = kakune_core::InstalledPluginRecord {
        id: manifest["id"].as_str().unwrap().to_owned(),
        name: manifest["name"].as_str().unwrap().to_owned(),
        version: manifest["version"].as_str().unwrap().to_owned(),
        manifest_path: manifest_path.display().to_string(),
        installed_at: "2026-01-01T00:00:00Z".to_owned(),
        enabled: true,
        policy: json!({}),
        digest: "sha256:test".to_owned(),
        resolved_lock: json!({}),
        provenance: json!({}),
    };
    store.upsert_plugin_install(&record).unwrap();

    let report = plugin_services::request_removal(&store, &record.id)
        .expect("path validation failure becomes a durable removal failure");
    assert_eq!(report.status, "removal failed");
    assert_eq!(
        store
            .plugin_removal_state(&record.id)
            .unwrap()
            .unwrap()
            .status,
        "removal failed"
    );
    assert!(
        store
            .list_installed_plugins()
            .unwrap()
            .iter()
            .all(|plugin| !plugin.enabled)
    );

    drop(store);
    fs::remove_dir_all(data).expect("data fixture is cleaned");
}
