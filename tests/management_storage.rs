mod support;

use std::{
    collections::HashMap,
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Barrier, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use kakune_core::{
    CoreConfig, McpConnectionTransport, NativeCredentialBackend, SecretBackendKind, Store,
    WorkflowDocument, analyze_workflow,
    observation::{self, ObservationStatus},
};
use rusqlite::Connection;
use support::management::{FilesystemSnapshot, ManagementFixture};

const SOURCE: &str = "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: observe, name: Observe}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: start\nnodes:\n  - id: start\n    type: kakune.flow.pass@1\n";

#[test]
fn absent_and_corrupt_storage_observation_never_creates_or_rewrites_files() {
    let fixture = ManagementFixture::new().expect("fixture root should be created");
    let absent_dir = fixture.path("absent-installation");
    let before_absent = FilesystemSnapshot::capture(&absent_dir)
        .expect("absent path should have an empty snapshot");
    let absent = observation::observe_installation(&absent_dir, None);
    assert_eq!(absent.database.status, ObservationStatus::Absent);
    assert_eq!(absent.configuration.status, ObservationStatus::Absent);
    before_absent
        .ensure_unchanged(&absent_dir)
        .expect("observation must not create the installation");

    let corrupt_dir = fixture.path("corrupt-installation");
    fs::create_dir_all(&corrupt_dir).expect("fixture directory should be created");
    let database = corrupt_dir.join("kakune.sqlite3");
    fs::write(&database, b"not a sqlite database").expect("corrupt fixture should be written");
    let before_corrupt =
        FilesystemSnapshot::capture(&corrupt_dir).expect("corrupt fixture should be snapshot-able");
    let corrupt = observation::observe_database(&database);
    assert_eq!(corrupt.status, ObservationStatus::Corrupt);
    before_corrupt
        .ensure_unchanged(&corrupt_dir)
        .expect("corrupt observation must not create sidecars or rewrite the database");
}

#[test]
fn configuration_observation_is_capped_and_never_publishes_defaults() {
    let fixture = ManagementFixture::new().expect("fixture root should be created");
    let directory = fixture.path("configuration-observation");
    fs::create_dir_all(&directory).expect("fixture directory should be created");
    let config_path = directory.join("kakune.yaml");
    fs::write(
        &config_path,
        serde_yaml::to_string(&CoreConfig::default()).expect("default config should serialize"),
    )
    .expect("existing config should be written");
    let before = FilesystemSnapshot::capture(&directory)
        .expect("configuration fixture should be snapshot-able");
    let healthy = observation::observe_configuration(&config_path);
    assert_eq!(healthy.status, ObservationStatus::Healthy);
    assert!(healthy.config.is_some());
    before
        .ensure_unchanged(&directory)
        .expect("valid configuration observation must be read-only");

    fs::write(&config_path, b"not: [valid yaml").expect("invalid fixture should be written");
    let before_invalid = FilesystemSnapshot::capture(&directory)
        .expect("invalid config fixture should be snapshot-able");
    let invalid = observation::observe_configuration(&config_path);
    assert_eq!(invalid.status, ObservationStatus::Corrupt);
    assert!(invalid.config.is_none());
    before_invalid
        .ensure_unchanged(&directory)
        .expect("invalid configuration observation must not repair or rewrite it");

    fs::write(&config_path, vec![b'x'; 1_048_577]).expect("oversized fixture should be written");
    let oversized = observation::observe_configuration(&config_path);
    assert_eq!(oversized.status, ObservationStatus::Corrupt);
    assert!(oversized.guidance.contains("1 MiB"));
}

#[test]
fn database_observation_classifies_schema_zero_through_fifteen_and_newer_without_writes() {
    let fixture = ManagementFixture::new().expect("fixture root should be created");
    let data_dir = fixture.path("schema-observation");
    let store = Store::open(data_dir.clone()).expect("latest test store should open");
    drop(store);
    let database = data_dir.join("kakune.sqlite3");

    for version in 0..=15 {
        set_claimed_schema_version(&database, version);
        let before =
            FilesystemSnapshot::capture(&data_dir).expect("schema fixture should be snapshot-able");
        let result = observation::observe_database(&database);
        assert_eq!(result.status, ObservationStatus::Legacy, "schema {version}");
        assert_eq!(result.schema_version, Some(version));
        before
            .ensure_unchanged(&data_dir)
            .expect("legacy observation must not change database or sidecars");
    }

    set_claimed_schema_version(&database, 16);
    let before_current =
        FilesystemSnapshot::capture(&data_dir).expect("current fixture should be snapshot-able");
    let current = observation::observe_database(&database);
    assert_eq!(current.status, ObservationStatus::Healthy);
    assert_eq!(current.schema_version, Some(16));
    before_current
        .ensure_unchanged(&data_dir)
        .expect("current observation must be read-only");

    set_claimed_schema_version(&database, 17);
    let before_newer =
        FilesystemSnapshot::capture(&data_dir).expect("newer fixture should be snapshot-able");
    let newer = observation::observe_database(&database);
    assert_eq!(newer.status, ObservationStatus::Incompatible);
    assert_eq!(newer.schema_version, Some(17));
    assert!(newer.guidance.contains("newer"));
    before_newer
        .ensure_unchanged(&data_dir)
        .expect("newer-schema observation must not mutate the installation");
}

#[test]
fn live_wal_is_reported_unavailable_without_touching_auxiliary_files() {
    let fixture = ManagementFixture::new().expect("fixture root should be created");
    let data_dir = fixture.path("live-wal");
    let store = Store::open(data_dir.clone()).expect("test store should open");
    drop(store);
    let database = data_dir.join("kakune.sqlite3");
    let connection = Connection::open(&database).expect("database should open for fixture setup");
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .expect("WAL mode should be configured");
    connection
        .execute(
            "UPDATE core_state SET event_sequence = event_sequence + 1",
            [],
        )
        .expect("fixture write should leave live WAL state");
    let wal = sidecar(&database, "-wal");
    let shm = sidecar(&database, "-shm");
    assert!(
        wal.exists() && shm.exists(),
        "fixture should contain live WAL files"
    );

    let before =
        FilesystemSnapshot::capture(&data_dir).expect("live-WAL fixture should be snapshot-able");
    let result = observation::observe_database(&database);
    assert_eq!(result.status, ObservationStatus::Unavailable);
    assert!(result.guidance.contains("WAL"));
    before
        .ensure_unchanged(&data_dir)
        .expect("observer must not read or mutate live WAL sidecars");
    drop(connection);
}

#[test]
fn schema_sixteen_upgrade_backs_up_before_changes_and_preserves_existing_data() {
    let fixture = ManagementFixture::new().expect("fixture root should be created");
    let data_dir = fixture.path("upgrade-success");
    let store = Store::open(data_dir.clone()).expect("test store should open");
    let workflow = WorkflowDocument::parse(SOURCE).expect("workflow source should parse");
    store
        .upsert_workflow(&workflow, SOURCE, "disabled")
        .expect("workflow should be stored");
    drop(store);
    let database = data_dir.join("kakune.sqlite3");
    downgrade_to_schema_fifteen(&database);

    let upgraded = Store::open(data_dir.clone()).expect("schema 15 should upgrade to 16");
    assert_eq!(
        upgraded
            .schema_version()
            .expect("schema version should read"),
        16
    );
    assert_eq!(
        upgraded
            .list_workflows()
            .expect("workflows should list")
            .len(),
        1
    );
    drop(upgraded);

    let backup_dir = data_dir.join("backups");
    let backup = fs::read_dir(&backup_dir)
        .expect("migration backup directory should exist")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().contains("before-migration-v16"))
        })
        .expect("a recoverable schema-15 backup should exist");
    let backup_connection = Connection::open(&backup).expect("backup should be a SQLite database");
    let backup_version: i64 = backup_connection
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .expect("backup schema version should be readable");
    assert_eq!(backup_version, 15);
    let backup_count: i64 = backup_connection
        .query_row("SELECT COUNT(*) FROM workflows", [], |row| row.get(0))
        .expect("workflow data should be present in the backup");
    assert_eq!(backup_count, 1);
}

#[test]
fn backup_failure_and_transactional_upgrade_failure_leave_source_schema_recoverable() {
    let fixture = ManagementFixture::new().expect("fixture root should be created");
    let failed_backup_dir = fixture.path("backup-failure");
    let store = Store::open(failed_backup_dir.clone()).expect("test store should open");
    drop(store);
    let failed_backup_database = failed_backup_dir.join("kakune.sqlite3");
    downgrade_to_schema_fifteen(&failed_backup_database);
    fs::write(failed_backup_dir.join("backups"), b"not a directory")
        .expect("backup failure fixture should be created");
    let before_bytes = fs::read(&failed_backup_database).expect("database should be readable");
    assert!(Store::open(failed_backup_dir.clone()).is_err());
    assert_eq!(
        fs::read(&failed_backup_database).expect("database should remain readable"),
        before_bytes,
        "backup failure must happen before schema, bookkeeping, or WAL writes"
    );

    let failed_upgrade_dir = fixture.path("transactional-upgrade-failure");
    let store = Store::open(failed_upgrade_dir.clone()).expect("test store should open");
    let workflow = WorkflowDocument::parse(SOURCE).expect("workflow source should parse");
    store
        .upsert_workflow(&workflow, SOURCE, "disabled")
        .expect("workflow should be stored");
    drop(store);
    let failed_upgrade_database = failed_upgrade_dir.join("kakune.sqlite3");
    downgrade_to_schema_fifteen(&failed_upgrade_database);
    let connection = Connection::open(&failed_upgrade_database)
        .expect("fixture database should open for injected failure");
    connection
        .execute_batch("CREATE INDEX execution_events_lookup ON events(sequence);")
        .expect("conflicting index should be installed");
    drop(connection);

    assert!(Store::open(failed_upgrade_dir.clone()).is_err());
    let connection = Connection::open(&failed_upgrade_database)
        .expect("failed upgrade database should remain readable");
    let version: i64 = connection
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .expect("migration bookkeeping should roll back");
    assert_eq!(version, 15);
    let source_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM workflows WHERE id = 'observe'",
            [],
            |row| row.get(0),
        )
        .expect("existing workflow should remain queryable");
    assert_eq!(source_count, 1);

    let backup = fs::read_dir(failed_upgrade_dir.join("backups"))
        .expect("failed migration should retain its backup")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().contains("before-migration-v16"))
        })
        .expect("failed migration backup should exist");
    let restore_dir = fixture.path("restored-backup");
    fs::create_dir_all(&restore_dir).expect("restore directory should be separate");
    let restored_database = restore_dir.join("kakune.sqlite3");
    fs::copy(&backup, &restored_database).expect("backup should restore to a separate directory");
    let restored = Connection::open(&restored_database).expect("restored database should open");
    let restored_count: i64 = restored
        .query_row(
            "SELECT COUNT(*) FROM workflows WHERE id = 'observe'",
            [],
            |row| row.get(0),
        )
        .expect("restored workflow should remain queryable");
    assert_eq!(restored_count, 1);
}

fn set_claimed_schema_version(database: &Path, version: u32) {
    let connection = Connection::open(database).expect("database should open for fixture setup");
    connection
        .pragma_update(None, "journal_mode", "DELETE")
        .expect("fixture journal mode should be checkpointed");
    connection
        .execute(
            "DELETE FROM schema_migrations WHERE version > ?1",
            [version],
        )
        .expect("later migration records should be removed");
    if version > 0 {
        connection
            .execute(
                "INSERT OR IGNORE INTO schema_migrations (version) VALUES (?1)",
                [version],
            )
            .expect("requested schema record should exist");
    }
}

fn downgrade_to_schema_fifteen(database: &Path) {
    let connection = Connection::open(database).expect("database should open for fixture setup");
    connection
        .pragma_update(None, "journal_mode", "DELETE")
        .expect("fixture journal mode should be checkpointed");
    connection
        .execute_batch(
            "DROP TABLE named_mcp_connections;
             DROP INDEX execution_events_lookup;
             ALTER TABLE secrets DROP COLUMN credential_version;
             DELETE FROM schema_migrations WHERE version > 15;",
        )
        .expect("schema-16 additions should be removed from the version-15 fixture");
}

fn sidecar(database: &Path, suffix: &str) -> std::path::PathBuf {
    let mut name = database.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}

#[derive(Default)]
struct FakeNativeCredentialBackend {
    values: Mutex<HashMap<String, Vec<u8>>>,
    fail_next_write: AtomicBool,
    fail_next_read: AtomicBool,
    fail_delete: AtomicBool,
    block_next_write: AtomicBool,
    write_entered: (Mutex<bool>, Condvar),
    release_write: (Mutex<bool>, Condvar),
}

impl NativeCredentialBackend for FakeNativeCredentialBackend {
    fn write(&self, key: &str, value: &[u8]) -> Result<(), String> {
        if self.block_next_write.swap(false, Ordering::SeqCst) {
            let (entered, entered_cv) = &self.write_entered;
            *entered.lock().unwrap() = true;
            entered_cv.notify_all();
            let (released, release_cv) = &self.release_write;
            let mut released = released.lock().unwrap();
            while !*released {
                released = release_cv.wait(released).unwrap();
            }
        }
        if self.fail_next_write.swap(false, Ordering::SeqCst) {
            return Err("injected credential staging failure".into());
        }
        self.values
            .lock()
            .unwrap()
            .insert(key.to_owned(), value.to_vec());
        Ok(())
    }

    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        if self.fail_next_read.swap(false, Ordering::SeqCst) {
            return Err("injected credential readback failure".into());
        }
        Ok(self.values.lock().unwrap().get(key).cloned())
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        if self.fail_delete.load(Ordering::SeqCst) {
            return Err("injected credential cleanup failure".into());
        }
        self.values.lock().unwrap().remove(key);
        Ok(())
    }
}

impl FakeNativeCredentialBackend {
    fn await_blocked_write(&self) {
        let (entered, cv) = &self.write_entered;
        let mut entered = entered.lock().unwrap();
        while !*entered {
            entered = cv.wait(entered).unwrap();
        }
    }

    fn release_blocked_write(&self) {
        let (released, cv) = &self.release_write;
        *released.lock().unwrap() = true;
        cv.notify_all();
    }
}

#[test]
fn versioned_native_secret_publication_preserves_access_and_reports_cleanup_state() {
    let fixture = ManagementFixture::new().unwrap();
    let credentials = Arc::new(FakeNativeCredentialBackend::default());
    let store = Store::open_with_secret_backend(
        fixture.path("versioned-secrets"),
        SecretBackendKind::Native,
        credentials.clone(),
    )
    .unwrap();
    store
        .set_secret_versioned("api-key", "first-value")
        .expect("initial credential should publish");
    assert_eq!(store.resolve_secret("api-key").unwrap(), "first-value");

    credentials.fail_next_write.store(true, Ordering::SeqCst);
    assert!(
        store
            .set_secret_versioned("api-key", "staging-failure")
            .is_err()
    );
    assert_eq!(store.resolve_secret("api-key").unwrap(), "first-value");

    credentials.fail_next_read.store(true, Ordering::SeqCst);
    assert!(
        store
            .set_secret_versioned("api-key", "readback-failure")
            .is_err()
    );
    assert_eq!(store.resolve_secret("api-key").unwrap(), "first-value");

    let database = rusqlite::Connection::open(store.data_dir().join("kakune.sqlite3")).unwrap();
    database
        .execute_batch(
            "CREATE TRIGGER reject_secret_publication BEFORE UPDATE ON secrets
             BEGIN SELECT RAISE(ABORT, 'injected metadata publication failure'); END;",
        )
        .unwrap();
    assert!(
        store
            .set_secret_versioned("api-key", "metadata-failure")
            .is_err()
    );
    database
        .execute_batch("DROP TRIGGER reject_secret_publication;")
        .unwrap();
    assert_eq!(store.resolve_secret("api-key").unwrap(), "first-value");

    credentials.fail_delete.store(true, Ordering::SeqCst);
    let committed = store
        .set_secret_versioned("api-key", "second-value")
        .expect("old-version cleanup failure must not undo publication");
    assert!(committed.cleanup_warning.is_some());
    assert_eq!(store.resolve_secret("api-key").unwrap(), "second-value");

    let removal = store
        .remove_secret_versioned("api-key")
        .expect("logical removal should commit before native cleanup");
    assert!(removal.removed);
    assert!(removal.cleanup_warning.is_some());
    assert!(store.resolve_secret("api-key").is_err());
    assert!(!store.remove_secret_versioned("unknown").unwrap().removed);
}

#[test]
fn versioned_vault_secret_publication_round_trips_and_preserves_access_on_failure() {
    const CHILD_MARKER: &str = "KAKUNE_SPEC003_VAULT_STORAGE_TEST_CHILD";
    if std::env::var(CHILD_MARKER).as_deref() != Ok("1") {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "versioned_vault_secret_publication_round_trips_and_preserves_access_on_failure",
                "--nocapture",
            ])
            .env(CHILD_MARKER, "1")
            .env(
                "KAKUNE_VAULT_PASSPHRASE",
                "deterministic-versioned-vault-test-passphrase",
            )
            .output()
            .expect("vault test child should start");
        assert!(
            output.status.success(),
            "vault test child failed: stdout={}; stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let fixture = ManagementFixture::new().unwrap();
    let credentials = Arc::new(FakeNativeCredentialBackend::default());
    let data_dir = fixture.path("versioned-vault-secrets");
    let store = Store::open_with_secret_backend(
        data_dir.clone(),
        SecretBackendKind::Vault,
        credentials.clone(),
    )
    .unwrap();
    let first = store
        .set_secret_versioned("vault-api-key", "first-vault-value")
        .expect("vault credential should publish after readback verification");
    assert_eq!(first.record.backend, "vault");
    assert_eq!(
        store.resolve_secret("vault-api-key").unwrap(),
        "first-vault-value"
    );
    assert!(credentials.values.lock().unwrap().is_empty());

    let database = Connection::open(data_dir.join("kakune.sqlite3")).unwrap();
    let first_version: String = database
        .query_row(
            "SELECT credential_version FROM secrets WHERE name = 'vault-api-key'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let salt_path = data_dir.join("vault-salt-v1");
    let salt = fs::read(&salt_path).unwrap();
    fs::write(&salt_path, b"invalid salt").unwrap();
    assert!(
        store
            .set_secret_versioned("vault-api-key", "failed-vault-staging")
            .is_err()
    );
    fs::write(&salt_path, salt).unwrap();
    assert_eq!(
        store.resolve_secret("vault-api-key").unwrap(),
        "first-vault-value"
    );
    let version_after_staging_failure: String = database
        .query_row(
            "SELECT credential_version FROM secrets WHERE name = 'vault-api-key'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version_after_staging_failure, first_version);

    database
        .execute_batch(
            "CREATE TRIGGER reject_vault_publication BEFORE UPDATE ON secrets
             BEGIN SELECT RAISE(ABORT, 'injected vault metadata publication failure'); END;",
        )
        .unwrap();
    assert!(
        store
            .set_secret_versioned("vault-api-key", "failed-vault-publication")
            .is_err()
    );
    database
        .execute_batch("DROP TRIGGER reject_vault_publication;")
        .unwrap();
    assert_eq!(
        store.resolve_secret("vault-api-key").unwrap(),
        "first-vault-value"
    );

    let updated = store
        .set_secret_versioned("vault-api-key", "second-vault-value")
        .expect("vault replacement should publish");
    assert!(updated.cleanup_warning.is_none());
    assert_eq!(
        store.resolve_secret("vault-api-key").unwrap(),
        "second-vault-value"
    );
    let database_bytes = fs::read(data_dir.join("kakune.sqlite3")).unwrap();
    assert!(!String::from_utf8_lossy(&database_bytes).contains("second-vault-value"));
    assert!(
        !store
            .remove_secret_versioned("missing-vault-key")
            .unwrap()
            .removed
    );
    assert!(
        store
            .remove_secret_versioned("vault-api-key")
            .unwrap()
            .removed
    );
    assert!(store.resolve_secret("vault-api-key").is_err());
}

#[test]
fn secret_names_are_serialized_with_a_five_second_lock_and_vault_reads_never_create_salt() {
    let fixture = ManagementFixture::new().unwrap();
    let credentials = Arc::new(FakeNativeCredentialBackend::default());
    let store = Store::open_with_secret_backend(
        fixture.path("secret-lock"),
        SecretBackendKind::Native,
        credentials.clone(),
    )
    .unwrap();
    credentials.block_next_write.store(true, Ordering::SeqCst);
    let writer_store = store.clone();
    let writer = thread::spawn(move || writer_store.set_secret_versioned("serialized", "first"));
    credentials.await_blocked_write();

    let started = Instant::now();
    let competing = store.set_secret_versioned("serialized", "second");
    let elapsed = started.elapsed();
    assert!(
        competing.is_err(),
        "same-name writer must not publish concurrently"
    );
    assert!(
        elapsed >= Duration::from_secs(4),
        "lock expired too early: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(9),
        "secret lock was not bounded: {elapsed:?}"
    );
    credentials.release_blocked_write();
    writer.join().unwrap().unwrap();
    assert_eq!(store.resolve_secret("serialized").unwrap(), "first");

    let vault_dir = fixture.path("legacy-vault-secret");
    let vault = Store::open_with_secret_backend(
        vault_dir.clone(),
        SecretBackendKind::Vault,
        Arc::new(FakeNativeCredentialBackend::default()),
    )
    .unwrap();
    rusqlite::Connection::open(vault.data_dir().join("kakune.sqlite3"))
        .unwrap()
        .execute(
            "INSERT INTO secrets (name, nonce, ciphertext, updated_at, backend, credential_version)
             VALUES ('legacy-vault', X'000000000000000000000000', X'00', '2026-10-26T00:00:00Z', 'vault', NULL)",
            [],
        )
        .unwrap();
    // Opening a WAL-backed store does not necessarily create shared-memory
    // state until its first read. Establish that normal SQLite state before
    // asserting that legacy vault resolution itself does not initialize files.
    vault.list_secrets().unwrap();
    let before = FilesystemSnapshot::capture(&vault_dir).unwrap();
    assert!(vault.resolve_secret("legacy-vault").is_err());
    before
        .ensure_unchanged(&vault_dir)
        .expect("read-only legacy vault resolution must not create salt or other files");
    assert!(!vault_dir.join("vault-salt-v1").exists());
}

#[test]
fn named_mcp_connections_persist_revision_guarded_secret_references_without_values() {
    let fixture = ManagementFixture::new().unwrap();
    let data_dir = fixture.path("named-mcp-storage");
    let store = Store::open(data_dir.clone()).unwrap();
    let input = kakune_core::NewNamedMcpConnection {
        name: "http-tools".into(),
        transport: McpConnectionTransport::Http {
            endpoint: "https://mcp.example.test/tools".into(),
            bearer_secret_ref: Some("first-ref".into()),
        },
    };
    let created = store
        .create_named_mcp_connection(input.clone())
        .expect("connection should be created without contacting its endpoint");
    assert!(matches!(
        created.transport,
        McpConnectionTransport::Http { ref bearer_secret_ref, .. }
            if bearer_secret_ref.as_deref() == Some("first-ref")
    ));
    assert!(store.create_named_mcp_connection(input).is_err());
    assert!(
        store
            .update_named_mcp_bearer_reference("http-tools", "stale", "second-ref")
            .is_err()
    );
    let updated = store
        .update_named_mcp_bearer_reference("http-tools", &created.revision, "second-ref")
        .expect("current revision should permit a reference-only update");
    assert_ne!(updated.revision, created.revision);
    assert!(matches!(
        updated.transport,
        McpConnectionTransport::Http { ref bearer_secret_ref, .. }
            if bearer_secret_ref.as_deref() == Some("second-ref")
    ));
    drop(store);

    let reopened = Store::open(data_dir).unwrap();
    let loaded = reopened
        .get_named_mcp_connection("http-tools")
        .unwrap()
        .unwrap();
    assert_eq!(loaded.revision, updated.revision);
    let serialized = serde_json::to_string(&loaded).unwrap();
    assert!(serialized.contains("second-ref"));
    assert!(!serialized.contains("private credential value"));
}

fn run_context_command(
    fixture: &ManagementFixture,
    args: &[String],
) -> io::Result<(i32, Vec<u8>, Vec<u8>)> {
    run_local_command(fixture, args, &[])
}

fn run_local_command(
    fixture: &ManagementFixture,
    args: &[String],
    environment: &[(&str, &str)],
) -> io::Result<(i32, Vec<u8>, Vec<u8>)> {
    let id = uuid::Uuid::new_v4();
    let stdout_path = fixture.path(format!("context-child-{id}.stdout"));
    let stderr_path = fixture.path(format!("context-child-{id}.stderr"));
    let stdout = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stdout_path)?;
    let stderr = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stderr_path)?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_kakune"));
    command
        .args(args)
        .env_remove("KAKUNE_TOKEN")
        .envs(environment.iter().copied())
        .current_dir(fixture.root())
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let mut child = command.spawn()?;
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if started.elapsed() >= Duration::from_secs(12) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "context command exceeded the 12-second fixture limit",
            ));
        }
        thread::sleep(Duration::from_millis(10));
    };
    let mut stdout = Vec::new();
    fs::File::open(stdout_path)?.read_to_end(&mut stdout)?;
    let mut stderr = Vec::new();
    fs::File::open(stderr_path)?.read_to_end(&mut stderr)?;
    Ok((status.code().unwrap_or(1), stdout, stderr))
}

fn context_args(path: &Path, args: &[&str]) -> Vec<String> {
    let mut result = args
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();
    result.push("--contexts-file".to_owned());
    result.push(path.to_string_lossy().into_owned());
    result
}

#[test]
fn legacy_context_revision_defaults_to_zero_and_import_limits_are_non_mutating() {
    let fixture = ManagementFixture::new().expect("fixture root should be created");
    let collection = fixture.path("legacy contexts.json");
    let original = serde_json::json!({
        "format": "kakune-contexts/v1",
        "exportedAt": "2026-10-26T00:00:00Z",
        "activeContextId": null,
        "contexts": [{
            "id": "legacy",
            "name": "Legacy",
            "endpoint": "https://legacy.example.test",
            "expectedCoreId": null,
            "color": null,
            "credentialRef": "env:KAKUNE_LEGACY_TOKEN"
        }]
    });
    fs::write(&collection, serde_json::to_vec(&original).unwrap()).unwrap();
    let export = fixture.path("legacy export.json");
    let result = run_context_command(
        &fixture,
        &context_args(
            &collection,
            &[
                "context",
                "export",
                "legacy",
                "--output",
                export.to_str().unwrap(),
            ],
        ),
    )
    .expect("legacy export command should finish");
    assert_eq!(result.0, 0, "{}", String::from_utf8_lossy(&result.2));
    let exported: serde_json::Value = serde_json::from_slice(&fs::read(&export).unwrap()).unwrap();
    assert_eq!(
        exported["revision"], 0,
        "legacy collection has revision zero"
    );
    assert_eq!(exported["contexts"].as_array().unwrap().len(), 1);
    assert_eq!(exported["contexts"][0]["id"], "legacy");
    assert_eq!(
        exported["contexts"][0]["credentialRef"],
        "env:KAKUNE_LEGACY_TOKEN"
    );
    assert_eq!(
        fs::read(&collection).unwrap(),
        serde_json::to_vec(&original).unwrap()
    );

    let before = fs::read(&collection).unwrap();
    let oversized = fixture.path("oversized import.json");
    fs::write(&oversized, vec![b' '; 1_048_577]).unwrap();
    let result = run_context_command(
        &fixture,
        &context_args(
            &collection,
            &["context", "import", oversized.to_str().unwrap()],
        ),
    )
    .expect("oversized import should fail in a bounded command");
    assert_eq!(result.0, 1);
    assert_eq!(fs::read(&collection).unwrap(), before);

    let too_many = fixture.path("too many contexts.json");
    let entries = (0..1_001)
        .map(|index| {
            serde_json::json!({
                "id": format!("context-{index}"),
                "name": format!("Context {index}"),
                "endpoint": format!("https://core-{index}.example.test"),
                "expectedCoreId": null,
                "color": null,
                "credentialRef": null
            })
        })
        .collect::<Vec<_>>();
    fs::write(
        &too_many,
        serde_json::to_vec(&serde_json::json!({
            "format": "kakune-contexts/v1",
            "exportedAt": "2026-10-26T00:00:00Z",
            "activeContextId": null,
            "contexts": entries
        }))
        .unwrap(),
    )
    .unwrap();
    let result = run_context_command(
        &fixture,
        &context_args(
            &collection,
            &["context", "import", too_many.to_str().unwrap()],
        ),
    )
    .expect("oversized entry-count import should fail");
    assert_eq!(result.0, 1);
    assert_eq!(fs::read(&collection).unwrap(), before);
}

#[test]
fn concurrent_context_mutations_preserve_entries_selection_and_revision() {
    let fixture = ManagementFixture::new().expect("fixture root should be created");
    let collection = fixture.path("concurrent contexts.json");
    for (id, endpoint) in [
        ("alpha", "https://alpha.example.test"),
        ("beta", "https://beta.example.test"),
    ] {
        let result = run_context_command(
            &fixture,
            &context_args(&collection, &["context", "add", id, "--endpoint", endpoint]),
        )
        .unwrap();
        assert_eq!(result.0, 0, "{}", String::from_utf8_lossy(&result.2));
    }
    let selected = run_context_command(
        &fixture,
        &context_args(&collection, &["context", "use", "alpha"]),
    )
    .unwrap();
    assert_eq!(selected.0, 0);

    thread::scope(|scope| {
        let select_beta = scope.spawn(|| {
            run_context_command(
                &fixture,
                &context_args(&collection, &["context", "use", "beta"]),
            )
        });
        let remove_alpha = scope.spawn(|| {
            run_context_command(
                &fixture,
                &context_args(&collection, &["context", "remove", "alpha"]),
            )
        });
        assert_eq!(select_beta.join().unwrap().unwrap().0, 0);
        assert_eq!(remove_alpha.join().unwrap().unwrap().0, 0);
    });

    thread::scope(|scope| {
        let workers = (0..8)
            .map(|index| {
                let fixture = &fixture;
                let collection = &collection;
                scope.spawn(move || {
                    let id = format!("worker-{index}");
                    let endpoint = format!("https://worker-{index}.example.test");
                    run_context_command(
                        fixture,
                        &context_args(
                            collection,
                            &["context", "add", &id, "--endpoint", &endpoint],
                        ),
                    )
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            let result = worker.join().unwrap().unwrap();
            assert_eq!(result.0, 0, "{}", String::from_utf8_lossy(&result.2));
        }
    });

    let batch = fixture.path("concurrent import.json");
    fs::write(
        &batch,
        serde_json::to_vec(&serde_json::json!({
            "format": "kakune-contexts/v1",
            "exportedAt": "2026-10-26T00:00:00Z",
            "activeContextId": "zeta",
            "contexts": [{
                "id": "zeta",
                "name": "Zeta",
                "endpoint": "https://zeta.example.test",
                "expectedCoreId": null,
                "color": null,
                "credentialRef": null
            }]
        }))
        .unwrap(),
    )
    .unwrap();
    thread::scope(|scope| {
        let add_eta = scope.spawn(|| {
            run_context_command(
                &fixture,
                &context_args(
                    &collection,
                    &[
                        "context",
                        "add",
                        "eta",
                        "--endpoint",
                        "https://eta.example.test",
                    ],
                ),
            )
        });
        let import_zeta = scope.spawn(|| {
            run_context_command(
                &fixture,
                &context_args(&collection, &["context", "import", batch.to_str().unwrap()]),
            )
        });
        assert_eq!(add_eta.join().unwrap().unwrap().0, 0);
        assert_eq!(import_zeta.join().unwrap().unwrap().0, 0);
    });

    let collection_value: serde_json::Value =
        serde_json::from_slice(&fs::read(&collection).unwrap()).unwrap();
    assert_eq!(collection_value["activeContextId"], "beta");
    assert_eq!(collection_value["contexts"].as_array().unwrap().len(), 11);
    assert_eq!(collection_value["revision"], 15);
}

#[test]
fn doctor_filesystem_and_credential_observations_leave_schema_matrix_unchanged() {
    let fixture = ManagementFixture::new().expect("fixture root should be created");
    let expected_database = [
        ("absent", "failed", 1),
        ("corrupt", "failed", 1),
        ("current", "healthy", 0),
        ("legacy", "warning", 0),
        ("newer", "failed", 1),
        ("wal", "unknown", 1),
    ];

    for (case, expected_status, expected_exit) in expected_database {
        let data_dir = fixture.path(format!("doctor-matrix-{case}"));
        let config = data_dir.join("kakune.yaml");
        let database = data_dir.join("kakune.sqlite3");
        let mut live_connection = None;
        if case != "absent" {
            fs::create_dir_all(&data_dir).expect("doctor fixture directory should be created");
            fs::write(
                &config,
                serde_yaml::to_string(&CoreConfig::default()).unwrap(),
            )
            .expect("valid config fixture should be written");
            if case == "corrupt" {
                fs::write(&database, b"not a SQLite database").unwrap();
            } else {
                drop(Store::open(data_dir.clone()).expect("fixture database should initialize"));
                if case == "wal" {
                    let connection = Connection::open(&database)
                        .expect("live WAL fixture connection should open");
                    connection
                        .pragma_update(None, "journal_mode", "WAL")
                        .expect("live WAL journal should be enabled");
                    connection
                        .execute(
                            "UPDATE core_state SET event_sequence = event_sequence + 1",
                            [],
                        )
                        .expect("live WAL fixture should leave an uncheckpointed write");
                    assert!(sidecar(&database, "-wal").exists());
                    assert!(sidecar(&database, "-shm").exists());
                    live_connection = Some(connection);
                } else {
                    let connection = Connection::open(&database)
                        .expect("database should reopen for deterministic checkpointing");
                    connection
                        .pragma_update(None, "journal_mode", "DELETE")
                        .expect("fixture should not leave WAL sidecars");
                    drop(connection);
                    match case {
                        "legacy" => set_claimed_schema_version(&database, 15),
                        "newer" => set_claimed_schema_version(&database, 17),
                        _ => {}
                    }
                }
            }
        }

        let before =
            FilesystemSnapshot::capture(&data_dir).expect("doctor fixture should be snapshot-able");
        let args = vec![
            "doctor".to_owned(),
            "--data-dir".to_owned(),
            data_dir.to_string_lossy().into_owned(),
            "--config".to_owned(),
            config.to_string_lossy().into_owned(),
            "--json".to_owned(),
        ];
        let output = run_local_command(&fixture, &args, &[("KAKUNE_TOKEN", "doctor-matrix-token")])
            .expect("doctor should finish without opening or migrating the database");
        assert_eq!(
            output.0,
            expected_exit,
            "case {case}: {}",
            String::from_utf8_lossy(&output.2)
        );
        assert!(!String::from_utf8_lossy(&output.1).contains("doctor-matrix-token"));
        let report: serde_json::Value = serde_json::from_slice(&output.1).unwrap();
        let database_finding = report["findings"]
            .as_array()
            .unwrap()
            .iter()
            .find(|finding| finding["area"] == "database")
            .unwrap();
        assert_eq!(database_finding["status"], expected_status, "case {case}");
        before
            .ensure_unchanged(&data_dir)
            .unwrap_or_else(|error| panic!("doctor changed {case} fixture: {error}"));
        drop(live_connection);
    }
}

#[test]
fn collection_lock_wait_is_bounded_and_does_not_publish_partial_contexts() {
    let fixture = ManagementFixture::new().expect("fixture root should be created");
    let collection = fixture.path("locked contexts.json");
    let initial = run_context_command(
        &fixture,
        &context_args(
            &collection,
            &[
                "context",
                "add",
                "alpha",
                "--endpoint",
                "https://alpha.example.test",
            ],
        ),
    )
    .unwrap();
    assert_eq!(initial.0, 0);
    let original = fs::read(&collection).unwrap();
    let lock_path = PathBuf::from(format!("{}.lock", collection.display()));
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)
        .unwrap();
    lock.try_lock().expect("test should own collection lock");

    let started = Instant::now();
    let timed_out = run_context_command(
        &fixture,
        &context_args(
            &collection,
            &[
                "context",
                "add",
                "beta",
                "--endpoint",
                "https://beta.example.test",
            ],
        ),
    )
    .expect("lock contention should return instead of hanging");
    let elapsed = started.elapsed();
    assert_eq!(timed_out.0, 1);
    assert!(
        elapsed >= Duration::from_secs(4),
        "lock ended too early: {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(9),
        "lock was not bounded: {elapsed:?}"
    );
    assert_eq!(fs::read(&collection).unwrap(), original);
}

#[test]
fn recursive_workflow_analysis_checks_nested_references_and_aggregate_graph_bounds() {
    let nested_reference = "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: nested, name: Nested}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: outer\nnodes:\n  - id: outer\n    type: kakune.flow.foreach@1\n    with: {maxConcurrency: 1}\n    inputs: {items: {literal: []}}\n    body:\n      entry: inner\n      nodes:\n        - id: inner\n          type: kakune.flow.pass@1\n          inputs: {value: {from: missing.output}}\n";
    let analysis = analyze_workflow(nested_reference);
    assert!(
        !analysis.is_valid(),
        "nested references must be checked in their own lexical graph scope"
    );
    assert!(analysis.diagnostics.iter().any(|diagnostic| {
        diagnostic.code.starts_with("binding.") && diagnostic.message.contains("missing")
    }));

    let invalid_lexical_expression = "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: lexical, name: Lexical}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: outer\nnodes:\n  - id: outer\n    type: kakune.flow.foreach@1\n    with: {maxConcurrency: 1}\n    inputs: {items: {literal: []}}\n    body:\n      entry: inner\n      nodes:\n        - id: inner\n          type: kakune.flow.pass@1\n          inputs: {value: {expr: {op: coalesce, args: [{from: \"$missing.value\"}]}}}\n";
    let analysis = analyze_workflow(invalid_lexical_expression);
    assert!(
        analysis
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "binding.unknown_scope"),
        "expression references must be checked against their lexical scope: {:?}",
        analysis.diagnostics
    );

    let valid_lexical_expression = invalid_lexical_expression.replace("$missing.value", "$item");
    assert!(
        analyze_workflow(&valid_lexical_expression).is_valid(),
        "foreach bodies expose their item binding to nested expressions"
    );

    let incompatible_type = "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: typed, name: Typed}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: start\nnodes: [{id: start, type: kakune.log@1, inputs: {message: {literal: 7}}}]\n";
    assert!(
        analyze_workflow(incompatible_type)
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "catalog.incompatible_literal"),
        "node schemas enforce literal input types"
    );

    let oversized_source = format!("{SOURCE}\n#{}", "x".repeat(1_048_577));
    assert!(
        !analyze_workflow(&oversized_source).is_valid(),
        "source length is bounded before parsing"
    );

    let mut aggregate = String::from(
        "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: aggregate, name: Aggregate}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: outer\nnodes:\n  - id: outer\n    type: kakune.flow.foreach@1\n    with: {maxConcurrency: 1}\n    inputs: {items: {literal: []}}\n    body:\n      entry: body-000\n      nodes:\n",
    );
    for index in 0..512 {
        aggregate.push_str(&format!(
            "        - id: body-{index:03}\n          type: kakune.flow.pass@1\n"
        ));
        if index < 511 {
            aggregate.push_str(&format!(
                "          on:\n            success: body-{:03}\n",
                index + 1
            ));
        }
    }
    let analysis = analyze_workflow(&aggregate);
    assert!(
        analysis
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "workflow.node_limit"),
        "the 512-node cap applies across nested scopes, not separately to each graph"
    );

    let mut too_many_edges = String::from(
        "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: edges, name: Edges}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: start\nnodes:\n  - id: start\n    type: kakune.flow.pass@1\n    on:\n      success: [",
    );
    for edge in 0..2_049 {
        if edge > 0 {
            too_many_edges.push_str(", ");
        }
        too_many_edges.push_str("end");
    }
    too_many_edges.push_str("]\n  - id: end\n    type: kakune.flow.merge@1\n");
    assert!(
        analyze_workflow(&too_many_edges)
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "workflow.edge_limit"),
        "aggregate control edges are bounded separately from node count"
    );

    fn append_nested_control_graph(source: &mut String, level: usize, indent: usize) {
        let spaces = " ".repeat(indent);
        if level == 33 {
            source.push_str(&format!(
                "{spaces}- id: level-{level:02}\n{spaces}  type: kakune.flow.pass@1\n"
            ));
            return;
        }
        source.push_str(&format!(
            "{spaces}- id: level-{level:02}\n{spaces}  type: kakune.flow.foreach@1\n{spaces}  with: {{maxConcurrency: 1}}\n{spaces}  inputs: {{items: {{literal: []}}}}\n{spaces}  body:\n{spaces}    entry: level-{:02}\n{spaces}    nodes:\n",
            level + 1
        ));
        append_nested_control_graph(source, level + 1, indent + 6);
    }
    let mut deep = String::from(
        "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: deep, name: Deep}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: level-00\nnodes:\n",
    );
    append_nested_control_graph(&mut deep, 0, 2);
    assert!(
        !analyze_workflow(&deep).is_valid(),
        "structured workflow nesting is capped at 32 levels"
    );
}

#[test]
fn admitted_plan_and_revision_remain_pinned_after_source_replacement() {
    let fixture = ManagementFixture::new().expect("fixture should be created");
    let data_dir = fixture.path("pinned-revision");
    let store = Store::open(data_dir.clone()).expect("isolated store should open");
    let first_source = SOURCE.replace("Observe", "Observe First");
    let first = WorkflowDocument::parse(&first_source).expect("first source should parse");
    let registration = store
        .create_workflow(&first, &first_source, "disabled")
        .expect("workflow should be created")
        .expect("identity should be new");
    let plan = serde_json::json!({
        "workflowId": "observe",
        "entry": "start",
        "policy": {
            "concurrency": {
                "maxRuns": 1,
                "overflow": "queue",
                "maxQueued": 2
            }
        },
        "inputValues": {},
        "triggerValues": {"type":"interval", "timestamp":123},
        "nodes": [{"id":"start", "type":"kakune.flow.pass@1", "node":{"id":"start"}}]
    });
    let execution = store
        .create_execution_with_plan("observe", &registration.revision, &plan)
        .expect("active admission should persist the immutable plan");
    assert_eq!(execution.status, "running");
    let queued = store
        .create_execution_with_plan("observe", &registration.revision, &plan)
        .expect("queued admission should persist the immutable plan");
    assert_eq!(queued.status, "queued");

    let second_source = SOURCE.replace("Observe", "Observe Second");
    let second = WorkflowDocument::parse(&second_source).expect("replacement should parse");
    let replacement = store
        .update_workflow_if_revision(&second, &second_source, &registration.revision)
        .expect("revision-guarded replacement should complete");
    let kakune_core::WorkflowSourceUpdate::Updated(replacement) = replacement else {
        panic!("matching revision should be replaced");
    };
    assert_ne!(registration.revision, replacement.revision);
    for execution_id in [&execution.id, &queued.id] {
        assert_eq!(
            store
                .execution_workflow_revision(execution_id)
                .expect("admitted revision should remain readable")
                .as_deref(),
            Some(registration.revision.as_str()),
            "replacement must not rewrite an admitted revision"
        );
        assert_eq!(
            store
                .execution_plan(execution_id)
                .expect("admitted plan should remain readable")
                .expect("admitted plan should exist"),
            plan,
            "replacement must not rewrite an admitted plan or trigger input"
        );
    }
    assert_eq!(
        store
            .get_workflow_source("observe")
            .expect("current source should be readable")
            .expect("workflow should remain registered")
            .source,
        second_source,
        "future admissions resolve the replacement source"
    );
    drop(store);
    let restarted = Store::open(data_dir).expect("store should reopen after restart");
    assert_eq!(
        restarted
            .recover_incomplete_executions()
            .expect("recoverable active work should requeue"),
        1
    );
    let recovered = restarted
        .get_execution(&execution.id)
        .expect("recovered execution should load")
        .expect("recovered execution should remain present");
    let queued_after_restart = restarted
        .get_execution(&queued.id)
        .expect("queued execution should load")
        .expect("queued execution should remain present");
    assert_eq!(recovered.status, "queued");
    assert_eq!(queued_after_restart.status, "queued");
    for execution_id in [&execution.id, &queued.id] {
        assert_eq!(
            restarted.execution_plan(execution_id).unwrap().unwrap(),
            plan
        );
        assert_eq!(
            restarted
                .execution_workflow_revision(execution_id)
                .unwrap()
                .as_deref(),
            Some(registration.revision.as_str())
        );
    }
}

#[test]
fn workflow_resolution_and_revision_guarded_automatic_admission_are_atomic() {
    let fixture = ManagementFixture::new().expect("fixture should be created");
    let store = Store::open(fixture.path("workflow-resolution")).expect("store should open");
    let first_source = SOURCE
        .replace("observe", "same")
        .replace("Observe", "other");
    let first = WorkflowDocument::parse(&first_source).expect("first workflow should parse");
    store
        .create_workflow(&first, &first_source, "disabled")
        .expect("first workflow should insert");
    let second_source = SOURCE
        .replace("observe", "other")
        .replace("Observe", "same");
    let second = WorkflowDocument::parse(&second_source).expect("second workflow should parse");
    store
        .create_workflow(&second, &second_source, "disabled")
        .expect("second workflow should insert");
    let ambiguous = kakune_core::management::resolve_workflow(&store, "same")
        .expect_err("a bare ID/name collision must not guess");
    assert_eq!(
        ambiguous.code,
        kakune_core::management::ManagementErrorCode::Conflict
    );
    assert_eq!(
        kakune_core::management::resolve_workflow(&store, "id:same")
            .expect("explicit ID should resolve")
            .id,
        "same"
    );
    assert_eq!(
        kakune_core::management::resolve_workflow(&store, "name:same")
            .expect("explicit name should resolve")
            .id,
        "other"
    );

    let revision = kakune_core::management::resolve_workflow(&store, "id:same")
        .unwrap()
        .revision;
    let barrier = Arc::new(Barrier::new(3));
    let replacements = ["Race One", "Race Two"].map(|name| {
        let store = store.clone();
        let barrier = Arc::clone(&barrier);
        let source = SOURCE.replace("observe", "same").replace("Observe", name);
        let workflow = WorkflowDocument::parse(&source).expect("replacement should parse");
        let expected_revision = revision.clone();
        thread::spawn(move || {
            barrier.wait();
            store.update_workflow_if_revision(&workflow, &source, &expected_revision)
        })
    });
    barrier.wait();
    let outcomes = replacements
        .into_iter()
        .map(|thread| {
            thread
                .join()
                .expect("replacement thread should finish")
                .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, kakune_core::WorkflowSourceUpdate::Updated(_)))
            .count(),
        1,
        "exactly one concurrent replacement may consume a revision"
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, kakune_core::WorkflowSourceUpdate::Conflict { .. }))
            .count(),
        1
    );

    let record = store
        .list_workflows()
        .unwrap()
        .into_iter()
        .find(|record| record.id == "same")
        .unwrap();
    let plan = serde_json::json!({
        "workflowId": "same",
        "entry": "start",
        "policy": {},
        "inputValues": {},
        "triggerValues": {"type":"interval"},
        "nodes": [{"id":"start", "type":"kakune.flow.pass@1", "node":{"id":"start"}}]
    });
    assert!(
        store
            .create_execution_with_plan_if_current_revision_request(
                "same",
                &record.revision,
                &plan,
                None,
                true,
            )
            .unwrap_err()
            .contains("not enabled"),
        "automatic admission must reject a disabled registration"
    );
    let kakune_core::WorkflowStatusUpdate::Updated(enabled) = store
        .set_workflow_status_if_revision("same", "enabled", &record.revision)
        .expect("eligibility should update")
    else {
        panic!("matching revision should enable the workflow");
    };
    let (automatic, created) = store
        .create_execution_with_plan_if_current_revision_request(
            "same",
            &enabled.revision,
            &plan,
            None,
            true,
        )
        .expect("enabled workflow should be admitted");
    assert!(created);
    assert!(store.execution_plan(&automatic.id).unwrap().is_some());
    store
        .set_workflow_status_if_revision("same", "disabled", &enabled.revision)
        .expect("workflow should disable");
    assert!(
        store
            .create_execution_with_plan_if_current_revision_request(
                "same",
                &enabled.revision,
                &plan,
                None,
                true,
            )
            .unwrap_err()
            .contains("not enabled"),
        "disable must close automatic admission"
    );
    let (manual, created) = store
        .create_execution_with_plan_if_current_revision_request(
            "same",
            &enabled.revision,
            &plan,
            None,
            false,
        )
        .expect("manual execution remains allowed while disabled");
    assert!(created);
    assert_ne!(automatic.id, manual.id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn management_cancellation_is_conditional_and_log_pages_hold_a_fixed_high_water() {
    let fixture = ManagementFixture::new().expect("fixture should be created");
    let mut core = support::management::InProcessCore::start(fixture.path("execution-contracts"))
        .await
        .expect("isolated Core should start");
    let source = SOURCE
        .replace("observe", "bounded")
        .replace("Observe", "Bounded");
    let workflow = WorkflowDocument::parse(&source).expect("workflow source should parse");
    let registration = core
        .store
        .create_workflow(&workflow, &source, "disabled")
        .expect("workflow should be registered")
        .expect("workflow identity should be new");
    let plan = serde_json::json!({
        "workflowId": "bounded",
        "entry": "start",
        "policy": {"concurrency":{"maxRuns":1,"overflow":"queue","maxQueued":8}},
        "inputValues": {},
        "triggerValues": {},
        "nodes": [{"id":"start","type":"kakune.flow.pass@1","node":{"id":"start"}}]
    });
    let active = core
        .store
        .create_execution_with_plan("bounded", &registration.revision, &plan)
        .expect("first execution should be admitted");
    assert_eq!(active.status, "running");
    let queued = core
        .store
        .create_execution_with_plan("bounded", &registration.revision, &plan)
        .expect("second execution should queue");
    assert_eq!(queued.status, "queued");

    let client = reqwest::Client::new();
    let cancel_queued = client
        .post(format!(
            "{}/api/v1/manage/executions/{}/cancel",
            core.endpoint, queued.id
        ))
        .bearer_auth(&core.token)
        .json(&serde_json::json!({"waitSeconds":0}))
        .send()
        .await
        .expect("queued cancellation should respond");
    assert_eq!(cancel_queued.status(), reqwest::StatusCode::OK);
    let queued_outcome: serde_json::Value = cancel_queued.json().await.unwrap();
    assert_eq!(queued_outcome["outcome"], "cancelled_before_start");
    assert_eq!(queued_outcome["observedStatus"], "cancelled");
    assert_eq!(queued_outcome["pending"], false);
    let terminal = core.store.get_execution(&queued.id).unwrap().unwrap();
    assert_eq!(terminal.status, "cancelled");
    assert!(terminal.completed_at.is_some());

    let cancel_running = client
        .post(format!(
            "{}/api/v1/manage/executions/{}/cancel",
            core.endpoint, active.id
        ))
        .bearer_auth(&core.token)
        .json(&serde_json::json!({"waitSeconds":1}))
        .send()
        .await
        .expect("running cancellation should respond after bounded observation");
    assert_eq!(cancel_running.status(), reqwest::StatusCode::OK);
    let running_outcome: serde_json::Value = cancel_running.json().await.unwrap();
    assert_eq!(running_outcome["outcome"], "requested");
    assert_eq!(running_outcome["observedStatus"], "cancelling");
    assert_eq!(running_outcome["pending"], true);

    let repeated = client
        .post(format!(
            "{}/api/v1/manage/executions/{}/cancel",
            core.endpoint, active.id
        ))
        .bearer_auth(&core.token)
        .json(&serde_json::json!({"waitSeconds":0}))
        .send()
        .await
        .expect("repeated cancellation should respond");
    assert_eq!(repeated.status(), reqwest::StatusCode::OK);
    assert_eq!(
        repeated.json::<serde_json::Value>().await.unwrap()["outcome"],
        "already_requested"
    );

    // Completion is allowed to win after a request; it must not be rewritten
    // by a later duplicate runtime completion.
    core.store
        .finish_execution(&active.id, "succeeded", None)
        .expect("completion may win the request race");
    core.store
        .finish_execution(&active.id, "cancelled", None)
        .expect("duplicate terminal completion should be a no-op");
    let final_state = core.store.get_execution(&active.id).unwrap().unwrap();
    assert_eq!(final_state.status, "succeeded");
    assert!(final_state.completed_at.is_some());

    let created = core
        .store
        .create_execution("bounded")
        .expect("log snapshot execution should be created");
    core.store
        .finish_execution(&created.id, "succeeded", None)
        .expect("log snapshot execution should finish");
    let logs_url = format!(
        "{}/api/v1/manage/executions/{}/logs?limit=1",
        core.endpoint, created.id
    );
    let first = client
        .get(&logs_url)
        .bearer_auth(&core.token)
        .send()
        .await
        .expect("first log page should respond");
    assert_eq!(first.status(), reqwest::StatusCode::OK);
    let first: serde_json::Value = first.json().await.unwrap();
    let high_water = first["highWater"].as_u64().expect("snapshot high-water");
    let cursor = first["nextCursor"].as_str().expect("continued page cursor");
    let first_sequence = first["items"][0]["sequence"].as_u64().unwrap();

    core.store
        .record_node_outcome(
            &created.id,
            "start",
            "succeeded",
            Some("safe status"),
            Some(&serde_json::json!({"value":"later"})),
            None,
        )
        .expect("a concurrent event should be appended after the snapshot");
    let second = client
        .get(format!(
            "{}/api/v1/manage/executions/{}/logs?limit=10&cursor={cursor}",
            core.endpoint, created.id
        ))
        .bearer_auth(&core.token)
        .send()
        .await
        .expect("continuation page should respond");
    assert_eq!(second.status(), reqwest::StatusCode::OK);
    let second: serde_json::Value = second.json().await.unwrap();
    assert_eq!(second["highWater"], high_water);
    let sequences = second["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["sequence"].as_u64().unwrap())
        .collect::<Vec<_>>();
    assert!(sequences.iter().all(|sequence| *sequence > first_sequence));
    assert!(sequences.iter().all(|sequence| *sequence <= high_water));
    assert_eq!(second["retentionAvailability"], "unknown");

    let tampered = cursor.replacen('a', "b", 1);
    let invalid = client
        .get(format!(
            "{}/api/v1/manage/executions/{}/logs?limit=10&cursor={tampered}",
            core.endpoint, created.id
        ))
        .bearer_auth(&core.token)
        .send()
        .await
        .expect("tampered cursor should return a bounded error");
    assert_eq!(invalid.status(), reqwest::StatusCode::BAD_REQUEST);

    core.shutdown().await.expect("Core should stop cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trigger_projection_includes_disabled_manual_orphan_and_unknown_live_evidence_without_writes()
 {
    let fixture = ManagementFixture::new().expect("fixture should be created");
    let mut core = support::management::InProcessCore::start(fixture.path("trigger-projections"))
        .await
        .expect("isolated Core should start");
    let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: scheduled, name: Scheduled}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}, {id: interval, type: kakune.trigger.interval@1, with: {everySeconds: 60}}]\nentry: start\nnodes: [{id: start, type: kakune.flow.pass@1}]\n";
    let workflow = WorkflowDocument::parse(source).expect("workflow source should parse");
    core.store
        .create_workflow(&workflow, source, "disabled")
        .expect("disabled workflow should be stored");
    let database = core.store.data_dir().join("kakune.sqlite3");
    let connection = Connection::open(&database).expect("fixture database should open");
    connection
        .execute(
            "INSERT INTO scheduled_triggers (workflow_id, trigger_id, next_run_at, updated_at, last_run_at, completed_at) VALUES ('scheduled', 'interval', '2026-10-26T01:00:00Z', '2026-10-26T00:00:00Z', '2026-10-25T23:00:00Z', NULL)",
            [],
        )
        .expect("durable schedule observation should be seeded");
    connection
        .pragma_update(None, "foreign_keys", "OFF")
        .expect("fixture may seed an orphan schedule record");
    connection
        .execute(
            "INSERT INTO scheduled_triggers (workflow_id, trigger_id, next_run_at, updated_at) VALUES ('orphan/flow', 'cron/a%', '2026-10-26T02:00:00Z', '2026-10-26T00:00:00Z')",
            [],
        )
        .expect("retained orphan schedule should be seeded");
    drop(connection);

    let event_count_before = core.store.list_events_after(None).unwrap().len();
    let client = reqwest::Client::new();
    let list = client
        .get(format!("{}/api/v1/manage/triggers", core.endpoint))
        .bearer_auth(&core.token)
        .send()
        .await
        .expect("trigger projection should respond");
    assert_eq!(list.status(), reqwest::StatusCode::OK);
    let list: serde_json::Value = list.json().await.unwrap();
    let items = list["items"].as_array().unwrap();
    let manual = items
        .iter()
        .find(|item| item["workflowId"] == "scheduled" && item["triggerId"] == "manual")
        .expect("manual definition should be projected");
    assert_eq!(manual["eligibility"], "disabled");
    assert_eq!(manual["effectiveState"], "disabled");
    let interval = items
        .iter()
        .find(|item| item["workflowId"] == "scheduled" && item["triggerId"] == "interval")
        .expect("disabled scheduled definition should be projected");
    assert_eq!(interval["eligibility"], "disabled");
    assert_eq!(interval["nextRunAt"], "2026-10-26T01:00:00Z");
    assert_eq!(interval["lastRunAt"], "2026-10-25T23:00:00Z");
    let orphan = items
        .iter()
        .find(|item| item["workflowId"] == "orphan/flow")
        .expect("orphan durable schedules should remain visible");
    assert_eq!(orphan["effectiveState"], "unknown");
    assert!(orphan["unavailableReason"].is_string());

    let detail = client
        .get(format!(
            "{}/api/v1/manage/triggers/orphan%2Fflow/cron%2Fa%25",
            core.endpoint
        ))
        .bearer_auth(&core.token)
        .send()
        .await
        .expect("separately encoded composite trigger path should respond");
    assert_eq!(detail.status(), reqwest::StatusCode::OK);
    assert_eq!(
        detail.json::<serde_json::Value>().await.unwrap()["triggerId"],
        "cron/a%"
    );

    assert_eq!(
        core.store.list_events_after(None).unwrap().len(),
        event_count_before,
        "trigger projection must not fire, reconcile, or append events"
    );
    core.shutdown().await.expect("Core should stop cleanly");
}
