mod support;

use std::{
    fs,
    time::{Duration, Instant},
};

use kakune_core::{
    Store,
    observation::{self, ObservationStatus},
};
use rusqlite::Connection;
use support::management::{FilesystemSnapshot, ManagementFixture};

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn doctor_uses_isolated_snapshot_for_healthy_and_locked_databases() {
    let fixture = ManagementFixture::new().unwrap();
    let directory = fixture.path("doctor-snapshot");
    drop(Store::open(directory.clone()).unwrap());
    let path = directory.join("kakune.sqlite3");
    let db = Connection::open(&path).unwrap();
    db.pragma_update(None, "journal_mode", "DELETE").unwrap();
    let executable = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("kakune");
    let run = || {
        let output = std::process::Command::new(&executable)
            .args(["doctor", "--data-dir"])
            .arg(&directory)
            .arg("--json")
            .output()
            .unwrap();
        let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        report["findings"]
            .as_array()
            .unwrap()
            .iter()
            .find(|finding| finding["area"] == "database")
            .unwrap()["status"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let before = FilesystemSnapshot::capture(&directory).unwrap();
    assert_eq!(run(), "healthy");
    before.ensure_unchanged(&directory).unwrap();
    db.execute_batch("BEGIN EXCLUSIVE").unwrap();
    assert_eq!(run(), "unknown");
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sqlite_external_writer_detects_existing_lock"])
        .env("KAKUNE_TEST_LOCKED_DB", &path)
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "{}",
        String::from_utf8_lossy(&child.stdout)
    );
    db.execute_batch("ROLLBACK").unwrap();
    before.ensure_unchanged(&directory).unwrap();
}

#[test]
fn installation_observation_uses_the_injected_database_executor_without_initializing() {
    struct UnavailableExecutor;
    impl observation::DatabaseObservationExecutor for UnavailableExecutor {
        fn observe_database(&self, path: &std::path::Path) -> observation::DatabaseObservation {
            observation::DatabaseObservation {
                path: path.to_path_buf(),
                status: ObservationStatus::Unavailable,
                schema_version: None,
                workflow_count: None,
                execution_count: None,
                guidance: "isolated executor unavailable".to_owned(),
            }
        }
    }
    let fixture = ManagementFixture::new().unwrap();
    let data = fixture.path("injected-observation");
    let before = FilesystemSnapshot::capture(&data).unwrap();
    let result = observation::observe_installation_with_executor(&data, None, &UnavailableExecutor);
    assert_eq!(result.database.path, data.join("kakune.sqlite3"));
    assert_eq!(result.database.status, ObservationStatus::Unavailable);
    assert_eq!(result.database.guidance, "isolated executor unavailable");
    before.ensure_unchanged(&data).unwrap();
}

#[test]
fn database_observation_preserves_missing_legacy_newer_and_corrupt_stores() {
    let fixture = ManagementFixture::new().unwrap();
    let missing = fixture.path("missing");
    let before = FilesystemSnapshot::capture(&missing).unwrap();
    assert_eq!(
        observation::observe_installation(&missing, None)
            .database
            .status,
        ObservationStatus::Absent
    );
    before.ensure_unchanged(&missing).unwrap();

    let legacy = fixture.path("legacy");
    fs::create_dir_all(&legacy).unwrap();
    let path = legacy.join("kakune.sqlite3");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(include_str!("fixtures/canonical_commands/schema-zero.sql"))
        .unwrap();
    drop(connection);
    let before = FilesystemSnapshot::capture(&legacy).unwrap();
    assert_eq!(
        observation::observe_database(&path).status,
        if cfg!(windows) {
            ObservationStatus::Legacy
        } else {
            ObservationStatus::Unavailable
        }
    );
    before.ensure_unchanged(&legacy).unwrap();

    let newer = fixture.path("newer");
    drop(Store::open(newer.clone()).unwrap());
    let path = newer.join("kakune.sqlite3");
    let connection = Connection::open(&path).unwrap();
    connection
        .pragma_update(None, "journal_mode", "DELETE")
        .unwrap();
    connection
        .execute("INSERT INTO schema_migrations (version) VALUES (9999)", [])
        .unwrap();
    drop(connection);
    let before = FilesystemSnapshot::capture(&newer).unwrap();
    assert_eq!(
        observation::observe_database(&path).status,
        if cfg!(windows) {
            ObservationStatus::Incompatible
        } else {
            ObservationStatus::Unavailable
        }
    );
    before.ensure_unchanged(&newer).unwrap();

    let corrupt = fixture.path("corrupt");
    fs::create_dir_all(&corrupt).unwrap();
    let path = corrupt.join("kakune.sqlite3");
    fs::write(&path, b"fake-secret-not-sqlite").unwrap();
    let before = FilesystemSnapshot::capture(&corrupt).unwrap();
    let observed = observation::observe_database(&path);
    assert_eq!(
        observed.status,
        if cfg!(windows) {
            ObservationStatus::Corrupt
        } else {
            ObservationStatus::Unavailable
        }
    );
    assert!(!format!("{observed:?}").contains("fake-secret-not-sqlite"));
    before.ensure_unchanged(&corrupt).unwrap();
}

#[test]
fn busy_database_observation_is_bounded_and_does_not_claim_corruption() {
    let fixture = ManagementFixture::new().unwrap();
    let directory = fixture.path("busy");
    drop(Store::open(directory.clone()).unwrap());
    let path = directory.join("kakune.sqlite3");
    let connection = Connection::open(&path).unwrap();
    connection
        .pragma_update(None, "journal_mode", "DELETE")
        .unwrap();
    let before = FilesystemSnapshot::capture(&directory).unwrap();
    connection.execute_batch("BEGIN EXCLUSIVE;").unwrap();
    let started = Instant::now();
    let observed = observation::observe_database(&path);
    assert!(started.elapsed() < Duration::from_secs(8));
    assert_eq!(observed.status, ObservationStatus::Unavailable);
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "sqlite_external_writer_detects_existing_lock"])
            .env("KAKUNE_TEST_LOCKED_DB", &path)
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
    }
    connection.execute_batch("ROLLBACK").unwrap();
    before.ensure_unchanged(&directory).unwrap();
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn sqlite_external_writer_detects_existing_lock() {
    let Some(path) = std::env::var_os("KAKUNE_TEST_LOCKED_DB") else {
        return;
    };
    let connection = Connection::open(path).unwrap();
    connection.busy_timeout(Duration::ZERO).unwrap();
    assert!(
        connection.execute_batch("BEGIN EXCLUSIVE").is_err(),
        "observation must not release the parent process's SQLite lock"
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn isolated_worker_reads_legacy_database_without_mutating_it() {
    let fixture = ManagementFixture::new().unwrap();
    let directory = fixture.path("worker-legacy");
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("kakune.sqlite3");
    let db = Connection::open(&path).unwrap();
    db.execute_batch(include_str!("fixtures/canonical_commands/schema-zero.sql"))
        .unwrap();
    drop(db);
    let before = FilesystemSnapshot::capture(&directory).unwrap();
    let exe = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("kakune-observation-helper");
    let output = std::process::Command::new(exe).arg(&path).output().unwrap();
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["status"], "legacy");
    assert_eq!(value["schemaVersion"], 0);
    before.ensure_unchanged(&directory).unwrap();
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn isolated_worker_does_not_release_parent_sqlite_writer_lock() {
    let fixture = ManagementFixture::new().unwrap();
    let directory = fixture.path("worker-locked");
    drop(Store::open(directory.clone()).unwrap());
    let path = directory.join("kakune.sqlite3");
    let db = Connection::open(&path).unwrap();
    db.pragma_update(None, "journal_mode", "DELETE").unwrap();
    db.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let exe = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("kakune-observation-helper");
    let output = std::process::Command::new(exe).arg(&path).output().unwrap();
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["status"], "unavailable");
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sqlite_external_writer_detects_existing_lock"])
        .env("KAKUNE_TEST_LOCKED_DB", &path)
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "{}",
        String::from_utf8_lossy(&child.stdout)
    );
    db.execute_batch("ROLLBACK").unwrap();
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn isolated_executor_rejects_current_schema_missing_executions_table() {
    use kakune_core::observation::DatabaseObservationExecutor;

    let fixture = ManagementFixture::new().unwrap();
    let data = fixture.path("missing-executions");
    drop(Store::open(data.clone()).unwrap());
    let path = data.join("kakune.sqlite3");
    let db = Connection::open(&path).unwrap();
    db.pragma_update(None, "journal_mode", "DELETE").unwrap();
    db.execute_batch("PRAGMA foreign_keys=OFF; DROP TABLE executions;")
        .unwrap();
    drop(db);
    let before = FilesystemSnapshot::capture(&data).unwrap();
    let executable = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("kakune-observation-helper");
    let executor = observation::IsolatedDatabaseObservationExecutor::new(executable).unwrap();
    assert_eq!(
        executor.observe_database(&path).status,
        ObservationStatus::Corrupt
    );
    assert!(executor.observe_plugins(&data).is_err());
    before.ensure_unchanged(&data).unwrap();
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn injected_isolated_executor_observes_and_respects_sqlite_locks() {
    use kakune_core::observation::DatabaseObservationExecutor;
    let fixture = ManagementFixture::new().unwrap();
    let directory = fixture.path("injected-worker");
    drop(Store::open(directory.clone()).unwrap());
    let path = directory.join("kakune.sqlite3");
    let db = Connection::open(&path).unwrap();
    db.pragma_update(None, "journal_mode", "DELETE").unwrap();
    let exe = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("kakune-observation-helper");
    let executor = observation::IsolatedDatabaseObservationExecutor::new(exe).unwrap();
    let before = FilesystemSnapshot::capture(&directory).unwrap();
    let result = executor.observe_database(&path);
    assert_eq!(result.status, ObservationStatus::Healthy);
    assert_eq!(result.schema_version, Some(17));
    before.ensure_unchanged(&directory).unwrap();
    db.execute_batch("BEGIN EXCLUSIVE").unwrap();
    assert_eq!(
        executor.observe_database(&path).status,
        ObservationStatus::Unavailable
    );
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sqlite_external_writer_detects_existing_lock"])
        .env("KAKUNE_TEST_LOCKED_DB", &path)
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "{}",
        String::from_utf8_lossy(&child.stdout)
    );
    db.execute_batch("ROLLBACK").unwrap();
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn local_cli_lists_catalog_in_isolated_helper_and_preserves_writer_locks() {
    let fixture = ManagementFixture::new().unwrap();
    let directory = fixture.path("cli-isolated-catalog");
    drop(Store::open(directory.clone()).unwrap());
    let path = directory.join("kakune.sqlite3");
    let db = Connection::open(&path).unwrap();
    db.pragma_update(None, "journal_mode", "DELETE").unwrap();
    db.execute("INSERT INTO installed_plugins (id,name,version,manifest_path,installed_at) VALUES ('example','Example','1.0.0','manifest','now')", []).unwrap();
    let executable_dir = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let before = FilesystemSnapshot::capture(&directory).unwrap();
    let result = std::process::Command::new(executable_dir.join("kakune"))
        .args(["plugin", "list", "--data-dir"])
        .arg(&directory)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&result.stdout),
        "ID\tVERSION\tSTATUS\tACTIVE EXECUTIONS\tNAME\nexample\t1.0.0\tactive\t0\tExample\n"
    );
    before.ensure_unchanged(&directory).unwrap();
    db.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let result = std::process::Command::new(executable_dir.join("kakune"))
        .args(["plugin", "list", "--data-dir"])
        .arg(&directory)
        .output()
        .unwrap();
    assert!(!result.status.success());
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sqlite_external_writer_detects_existing_lock"])
        .env("KAKUNE_TEST_LOCKED_DB", &path)
        .output()
        .unwrap();
    assert!(
        child.status.success(),
        "{}",
        String::from_utf8_lossy(&child.stdout)
    );
    db.execute_batch("ROLLBACK").unwrap();
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn missing_or_malformed_helper_fails_closed_without_mutating_storage() {
    use kakune_core::observation::DatabaseObservationExecutor;
    use std::os::unix::fs::PermissionsExt;

    let fixture = ManagementFixture::new().unwrap();
    let data = fixture.path("missing-helper-data");
    drop(Store::open(data.clone()).unwrap());
    let path = data.join("kakune.sqlite3");
    let db = Connection::open(&path).unwrap();
    db.pragma_update(None, "journal_mode", "DELETE").unwrap();
    drop(db);
    let before = FilesystemSnapshot::capture(&data).unwrap();
    let helper = fixture.path("missing-helper");
    let executor = observation::IsolatedDatabaseObservationExecutor::new(helper.clone()).unwrap();
    assert_eq!(
        executor.observe_database(&path).status,
        ObservationStatus::Unavailable
    );
    assert!(executor.observe_plugins(&data).is_err());

    fs::write(&helper, "#!/bin/sh\nprintf '{invalid json'\n").unwrap();
    fs::set_permissions(&helper, fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(
        executor.observe_database(&path).status,
        ObservationStatus::Unavailable
    );
    assert!(executor.observe_plugins(&data).is_err());

    let binary = fixture.path("kakune");
    fs::copy(
        std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("kakune"),
        &binary,
    )
    .unwrap();
    let output = std::process::Command::new(&binary)
        .args(["doctor", "--data-dir"])
        .arg(&data)
        .arg("--json")
        .output()
        .unwrap();
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let database = report["findings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|finding| finding["area"] == "database")
        .unwrap();
    assert_eq!(database["status"], "unknown");
    let output = std::process::Command::new(binary)
        .args(["plugin", "list", "--data-dir"])
        .arg(&data)
        .output()
        .unwrap();
    assert!(!output.status.success());
    before.ensure_unchanged(&data).unwrap();
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn isolated_executor_bounds_stalled_and_oversized_helper_output() {
    use kakune_core::observation::DatabaseObservationExecutor;
    use std::os::unix::fs::PermissionsExt;
    let fixture = ManagementFixture::new().unwrap();
    let data = fixture.path("helper-input");
    fs::write(&data, b"present").unwrap();
    let helper = fixture.path("helper.sh");
    fs::write(&helper, "#!/bin/sh\nwhile :; do :; done\n").unwrap();
    fs::set_permissions(&helper, fs::Permissions::from_mode(0o700)).unwrap();
    let executor = observation::IsolatedDatabaseObservationExecutor::new(helper.clone()).unwrap();
    let start = Instant::now();
    assert_eq!(
        executor.observe_database(&data).status,
        ObservationStatus::Unavailable
    );
    assert!(start.elapsed() < Duration::from_secs(8));
    fs::write(&helper, "#!/bin/sh\ni=0\nwhile [ \"$i\" -lt 7000 ]; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'; i=$((i+1)); done\n").unwrap();
    let result = executor.observe_database(&data);
    assert_eq!(result.status, ObservationStatus::Unavailable);
    assert!(result.guidance.len() < 256);
}

#[test]
fn wal_mode_without_sidecars_does_not_create_sqlite_auxiliary_files() {
    let fixture = ManagementFixture::new().unwrap();
    let directory = fixture.path("closed-wal");
    drop(Store::open(directory.clone()).unwrap());
    let path = directory.join("kakune.sqlite3");
    let before = FilesystemSnapshot::capture(&directory).unwrap();
    let result = observation::observe_database(&path);
    assert_eq!(result.status, ObservationStatus::Unavailable);
    before.ensure_unchanged(&directory).unwrap();
}

#[test]
fn observation_ignores_large_unrelated_catalog_without_writes() {
    let fixture = ManagementFixture::new().unwrap();
    let directory = fixture.path("large-catalog");
    drop(Store::open(directory.clone()).unwrap());
    let path = directory.join("kakune.sqlite3");
    let db = Connection::open(&path).unwrap();
    db.pragma_update(None, "journal_mode", "DELETE").unwrap();
    for index in 0..256 {
        db.execute_batch(&format!("CREATE TABLE unrelated_{index} (value TEXT);"))
            .unwrap();
    }
    drop(db);
    let before = FilesystemSnapshot::capture(&directory).unwrap();
    let result = observation::observe_database(&path);
    assert_eq!(
        result.status,
        if cfg!(windows) {
            ObservationStatus::Healthy
        } else {
            ObservationStatus::Unavailable
        }
    );
    before.ensure_unchanged(&directory).unwrap();
}

#[cfg(not(windows))]
#[test]
fn unsupported_snapshot_reports_unavailable_without_modifying_a_healthy_database() {
    let fixture = ManagementFixture::new().unwrap();
    let directory = fixture.path("unsupported-snapshot");
    drop(Store::open(directory.clone()).unwrap());
    let path = directory.join("kakune.sqlite3");
    let db = Connection::open(&path).unwrap();
    db.pragma_update(None, "journal_mode", "DELETE").unwrap();
    drop(db);
    let before = FilesystemSnapshot::capture(&directory).unwrap();
    let observation = observation::observe_database(&path);
    assert_eq!(observation.status, ObservationStatus::Unavailable);
    assert!(
        observation
            .guidance
            .contains("safe local database snapshot")
    );
    before.ensure_unchanged(&directory).unwrap();
}

#[test]
fn sqlite_read_only_open_can_create_sidecars_after_journal_mode_changes() {
    let fixture = ManagementFixture::new().unwrap();
    let directory = fixture.path("mode-change-race");
    drop(Store::open(directory.clone()).unwrap());
    let path = directory.join("kakune.sqlite3");
    let writer = Connection::open(&path).unwrap();
    writer
        .pragma_update(None, "journal_mode", "DELETE")
        .unwrap();
    drop(writer);
    let header = fs::read(&path).unwrap();
    assert_eq!((header[18], header[19]), (1, 1));

    // The writer changes mode after the observer's header check, then exits.
    let writer = Connection::open(&path).unwrap();
    writer.pragma_update(None, "journal_mode", "WAL").unwrap();
    drop(writer);
    assert!(!directory.join("kakune.sqlite3-wal").exists());
    assert!(!directory.join("kakune.sqlite3-shm").exists());

    let reader =
        Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    reader
        .query_row("SELECT COUNT(*) FROM workflows", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
    assert!(directory.join("kakune.sqlite3-wal").exists());
    assert!(directory.join("kakune.sqlite3-shm").exists());
}
