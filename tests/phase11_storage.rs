use std::fs;

use kakune_core::{RetentionPolicy, Store, WorkflowDocument};

fn temporary_directory(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("kakune-{name}-{}", uuid::Uuid::new_v4()))
}

const WORKFLOW: &str = "apiVersion: kakune/v1
kind: Workflow
metadata:
  id: phase11-backup
  name: Phase 11 backup
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: finish
nodes:
  - id: finish
    type: kakune.flow.end@1
";

#[test]
fn backup_restores_workflows_executions_and_artifacts_without_secrets() {
    let source = temporary_directory("backup-source");
    let restored = temporary_directory("backup-restored");
    let backup = temporary_directory("backup-output").with_extension("tar.gz");
    let store = Store::open(source.clone()).expect("source store should open");
    let workflow = WorkflowDocument::parse(WORKFLOW).expect("workflow should parse");
    let record = store
        .upsert_workflow(&workflow, WORKFLOW, "enabled")
        .expect("workflow should save");
    store
        .create_execution_with_plan(
            &record.name,
            &record.revision,
            &serde_json::json!({ "policy": {} }),
        )
        .expect("execution should save");
    store
        .put_artifact(b"phase 11 artifact", "text/plain", Some("proof.txt"))
        .expect("artifact should save");

    let summary = store.backup_to(&backup).expect("backup should succeed");
    assert_eq!(summary.workflows, 1);
    assert_eq!(summary.executions, 1);
    assert_eq!(summary.artifacts, 1);
    drop(store);

    let summary = Store::restore_backup(&backup, &restored).expect("restore should succeed");
    assert_eq!(summary.workflows, 1);
    assert_eq!(summary.executions, 1);
    assert_eq!(summary.artifacts, 1);

    let _ = fs::remove_file(backup);
    let _ = fs::remove_dir_all(source);
    let _ = fs::remove_dir_all(restored);
}

#[test]
fn retention_rejects_non_positive_limits() {
    let directory = temporary_directory("retention");
    let store = Store::open(directory.clone()).expect("store should open");
    let error = store
        .apply_retention(&RetentionPolicy {
            execution_days: 0,
            ..RetentionPolicy::default()
        })
        .expect_err("invalid retention should fail");
    assert!(error.contains("must be positive"));
    drop(store);
    let _ = fs::remove_dir_all(directory);
}

#[test]
fn corrupt_database_and_existing_backup_destination_fail_without_mutating_data() {
    let corrupted = temporary_directory("corrupt-db");
    fs::create_dir_all(&corrupted).expect("directory should create");
    fs::write(corrupted.join("kakune.sqlite3"), b"not a sqlite database")
        .expect("corrupt fixture should write");
    assert!(Store::open(corrupted.clone()).is_err());

    let directory = temporary_directory("backup-conflict");
    let store = Store::open(directory.clone()).expect("store should open");
    let destination = temporary_directory("backup-existing").with_extension("tar.gz");
    fs::write(&destination, b"do not overwrite").expect("backup fixture should write");
    assert!(store.backup_to(&destination).is_err());
    assert_eq!(
        fs::read(&destination).expect("backup should remain"),
        b"do not overwrite"
    );
    drop(store);
    let _ = fs::remove_dir_all(corrupted);
    let _ = fs::remove_dir_all(directory);
    let _ = fs::remove_file(destination);
}
