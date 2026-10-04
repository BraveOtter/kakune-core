use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

use kakune_core::{
    AuthScope, RetentionPolicy, Store, WorkflowDocument,
    initialization::{
        CredentialAccess, CredentialAccessError, CredentialReference, CredentialSecret,
        InitializationPaths, initialize,
    },
};

fn temporary_directory(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("kakune-{name}-{}", uuid::Uuid::new_v4()))
}

fn initialization_fixture(name: &str) -> PathBuf {
    let root = std::env::temp_dir()
        .join("opencode")
        .join(format!("kakune-{name}-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).expect("isolated fixture directory should be created");
    root
}

fn initialization_paths(root: &Path) -> InitializationPaths {
    InitializationPaths {
        data_dir: root.join("data"),
        config_file: root.join("configuration").join("kakune.yaml"),
        context_file: root.join("client").join("contexts.json"),
    }
}

struct ExistingCredential {
    token: String,
    reads: AtomicUsize,
    writes: AtomicUsize,
}

impl ExistingCredential {
    fn new(token: String) -> Self {
        Self {
            token,
            reads: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
        }
    }
}

impl CredentialAccess for ExistingCredential {
    fn resolve_reference(
        &self,
        reference: Option<&str>,
    ) -> Result<CredentialReference, CredentialAccessError> {
        CredentialReference::new(reference.unwrap_or("env:KAKUNE_TOKEN").to_owned())
    }

    fn write_secure(
        &self,
        _reference: &CredentialReference,
        _secret: &CredentialSecret,
    ) -> Result<(), CredentialAccessError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        Err(CredentialAccessError::SecureWriteFailed)
    }

    fn read(
        &self,
        _reference: &CredentialReference,
    ) -> Result<CredentialSecret, CredentialAccessError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        Ok(CredentialSecret::new(self.token.clone()))
    }
}

const LEGACY_WORKFLOW: &str = "apiVersion: kakune/v1
kind: Workflow
metadata:
  id: legacy-preservation
  name: Legacy Preservation
triggers:
  - id: manual
    type: kakune.trigger.manual@1
entry: finish
nodes:
  - id: finish
    type: kakune.flow.end@1
";

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

fn migration_database_snapshot(path: &Path) -> serde_json::Value {
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("database snapshot should open read-only");
    let core_id: String = connection
        .query_row("SELECT core_id FROM core_state WHERE id = 1", [], |row| {
            row.get(0)
        })
        .expect("database identity should be present");
    let workflows = connection
        .prepare("SELECT id, name, source, status, updated_at FROM workflows ORDER BY id")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let executions = connection
        .prepare(
            "SELECT id, workflow_name, status, created_at, completed_at, error
             FROM executions ORDER BY id",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let tokens = connection
        .prepare(
            "SELECT id, name, scopes, created_at, expires_at, revoked_at
             FROM auth_tokens ORDER BY id",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    serde_json::json!({
        "coreId": core_id,
        "workflows": workflows,
        "executions": executions,
        "tokens": tokens,
    })
}

fn database_schema_version(path: &Path) -> i64 {
    rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .expect("database should open read-only")
        .query_row(
            "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
            [],
            |row| row.get(0),
        )
        .expect("schema version should be readable")
}

#[test]
fn initialization_migrates_legacy_store_with_recoverable_backup() {
    let root = initialization_fixture("initialization-legacy-upgrade");
    let paths = initialization_paths(&root);
    let config_parent = paths.config_file.parent().unwrap();
    fs::create_dir_all(config_parent).expect("configuration parent should be created");
    fs::write(
        &paths.config_file,
        serde_yaml::to_string(&kakune_core::CoreConfig::default()).unwrap(),
    )
    .expect("valid configuration should be written");

    let store = Store::open(paths.data_dir.clone()).expect("current store should open");
    let core_id = store.core_id().expect("installation identity should exist");
    let workflow = WorkflowDocument::parse(LEGACY_WORKFLOW).expect("legacy fixture should parse");
    store
        .upsert_workflow(&workflow, LEGACY_WORKFLOW, "disabled")
        .expect("workflow should be persisted");
    store
        .create_execution("Legacy Preservation")
        .expect("execution should be persisted");
    let token = store
        .create_auth_token(
            "preserved legacy admin".to_string(),
            vec![AuthScope::Admin],
            None,
        )
        .expect("existing administrative credential should be persisted");
    drop(store);

    let database = paths.data_dir.join("kakune.sqlite3");
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute("DELETE FROM schema_migrations WHERE version = 15", [])
        .expect("fixture should become a supported version-14 store");
    connection
        .execute("DROP TABLE auth_pairing_codes", [])
        .expect("version-15 table should be absent in the legacy fixture");
    drop(connection);
    assert_eq!(database_schema_version(&database), 14);
    let before = migration_database_snapshot(&database);

    let context_parent = paths.context_file.parent().unwrap();
    fs::create_dir_all(context_parent).expect("context parent should be created");
    fs::write(
        &paths.context_file,
        serde_json::to_vec_pretty(&serde_json::json!({
            "format": "kakune-contexts/v1",
            "exportedAt": "2026-10-01T12:00:00Z",
            "activeContextId": "local",
            "contexts": [{
                "id": "local",
                "name": "Existing local",
                "endpoint": "https://custom-local.example.test:9443",
                "expectedCoreId": core_id,
                "credentialRef": format!("keychain:kakune/core/{core_id}")
            }]
        }))
        .unwrap(),
    )
    .expect("existing local context should be written");
    let credentials = ExistingCredential::new(token.token);

    let report = initialize(paths.clone(), &credentials)
        .expect("supported old store should migrate and preserve existing access");
    assert!(report.is_complete());
    assert_eq!(
        report.configuration,
        kakune_core::initialization::ComponentState::Reused
    );
    assert_eq!(
        report.storage,
        kakune_core::initialization::ComponentState::Reused
    );
    assert_eq!(
        report.identity,
        kakune_core::initialization::ComponentState::Reused
    );
    assert_eq!(
        report.client_access,
        kakune_core::initialization::ComponentState::Reused
    );
    assert_eq!(
        report.local_context,
        kakune_core::initialization::ComponentState::Reused
    );
    assert_eq!(credentials.reads.load(Ordering::Relaxed), 1);
    assert_eq!(credentials.writes.load(Ordering::Relaxed), 0);
    assert_eq!(database_schema_version(&database), 15);
    assert_eq!(migration_database_snapshot(&database), before);

    let backup = paths
        .data_dir
        .join("backups")
        .join("kakune.sqlite3.before-migration-v15.sqlite3");
    assert!(backup.is_file(), "pre-upgrade database backup should exist");
    assert_eq!(database_schema_version(&backup), 14);
    assert_eq!(migration_database_snapshot(&backup), before);

    let _ = fs::remove_dir_all(root);
}

#[test]
fn initialization_rejects_corrupt_and_unsupported_storage_without_mutation() {
    for state in ["corrupt", "unsupported"] {
        let root = initialization_fixture(&format!("initialization-{state}-storage"));
        let paths = initialization_paths(&root);
        fs::create_dir_all(&paths.data_dir).expect("data directory should be created");
        let database = paths.data_dir.join("kakune.sqlite3");
        if state == "corrupt" {
            fs::write(&database, b"not a SQLite database")
                .expect("corrupt fixture should be written");
        } else {
            let store = Store::open(paths.data_dir.clone()).expect("fixture store should open");
            drop(store);
            let connection = rusqlite::Connection::open(&database).unwrap();
            connection
                .execute("INSERT INTO schema_migrations (version) VALUES (999)", [])
                .expect("unsupported version should be recorded");
            drop(connection);
        }
        let original = fs::read(&database).expect("original database bytes should be captured");
        let credentials = ExistingCredential::new("unused-token".to_string());

        let error = initialize(paths.clone(), &credentials)
            .expect_err("corrupt or unsupported storage must fail conservatively");
        assert!(!error.to_string().contains("kakune_"));
        assert_eq!(
            error.report.storage,
            kakune_core::initialization::ComponentState::Incomplete
        );
        assert_eq!(
            error.report.client_access,
            kakune_core::initialization::ComponentState::NotAttempted
        );
        assert_eq!(
            error.resource,
            kakune_core::initialization::InitializationResource::Storage
        );
        assert_eq!(
            error.category,
            kakune_core::initialization::InitializationFailureCategory::UnsupportedStorage
        );
        assert_eq!(
            error.correction,
            kakune_core::initialization::InitializationCorrection::InspectResource
        );
        assert_eq!(credentials.reads.load(Ordering::Relaxed), 0);
        assert_eq!(credentials.writes.load(Ordering::Relaxed), 0);
        assert_eq!(
            fs::read(&database).unwrap(),
            original,
            "{state} database must remain unchanged"
        );
        assert!(
            !paths.context_file.exists(),
            "{state} storage failure must not publish metadata"
        );

        let _ = fs::remove_dir_all(root);
    }
}

#[test]
fn failed_initialization_upgrade_keeps_original_data_and_backup_recoverable() {
    let root = initialization_fixture("initialization-failed-upgrade");
    let paths = initialization_paths(&root);
    let store = Store::open(paths.data_dir.clone()).expect("current store should open");
    let workflow = WorkflowDocument::parse(LEGACY_WORKFLOW).unwrap();
    store
        .upsert_workflow(&workflow, LEGACY_WORKFLOW, "disabled")
        .unwrap();
    store.create_execution("Legacy Preservation").unwrap();
    store
        .create_auth_token(
            "failed-upgrade admin".to_string(),
            vec![AuthScope::Admin],
            None,
        )
        .unwrap();
    drop(store);

    let database = paths.data_dir.join("kakune.sqlite3");
    let connection = rusqlite::Connection::open(&database).unwrap();
    connection
        .execute("DELETE FROM schema_migrations WHERE version = 15", [])
        .unwrap();
    // This collision forces the v15 table-creation step to fail transactionally.
    connection
        .execute("DROP TABLE auth_pairing_codes", [])
        .expect("migration target should be removed before the conflicting fixture is created");
    connection
        .execute_batch("CREATE TABLE auth_pairing_codes (preserved_fixture INTEGER)")
        .expect("conflicting migration target should be prepared");
    drop(connection);
    let before = migration_database_snapshot(&database);
    assert_eq!(database_schema_version(&database), 14);

    let credentials = ExistingCredential::new("unused-token".to_string());
    let error = initialize(paths.clone(), &credentials)
        .expect_err("a failed supported migration must not report a complete installation");
    assert!(!error.to_string().contains("kakune_"));
    assert_eq!(
        error.report.storage,
        kakune_core::initialization::ComponentState::Incomplete
    );
    assert_eq!(
        error.report.identity,
        kakune_core::initialization::ComponentState::NotAttempted
    );
    assert_eq!(
        error.report.client_access,
        kakune_core::initialization::ComponentState::NotAttempted
    );
    assert_eq!(
        error.resource,
        kakune_core::initialization::InitializationResource::Storage
    );
    assert_eq!(
        error.category,
        kakune_core::initialization::InitializationFailureCategory::StorageUpgradeFailed
    );
    assert_eq!(
        error.correction,
        kakune_core::initialization::InitializationCorrection::RepairOrRestoreStorage
    );
    assert_eq!(credentials.reads.load(Ordering::Relaxed), 0);
    assert_eq!(credentials.writes.load(Ordering::Relaxed), 0);
    assert_eq!(database_schema_version(&database), 14);
    assert_eq!(migration_database_snapshot(&database), before);
    assert!(!paths.context_file.exists());

    let backup = paths
        .data_dir
        .join("backups")
        .join("kakune.sqlite3.before-migration-v15.sqlite3");
    assert!(
        backup.is_file(),
        "failed upgrade should retain its pre-upgrade backup"
    );
    assert_eq!(database_schema_version(&backup), 14);
    assert_eq!(migration_database_snapshot(&backup), before);

    let _ = fs::remove_dir_all(root);
}
