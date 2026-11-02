mod support;

use std::{fs, process::Command};

use kakune_core::{InstalledPluginRecord, Store};
use rusqlite::Connection;
use support::management::{FilesystemSnapshot, ManagementFixture};

#[test]
fn plugin_list_does_not_initialize_a_missing_installation() {
    let fixture = ManagementFixture::new().unwrap();
    let data = fixture.path("missing");
    let before = FilesystemSnapshot::capture(&data).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .args(["plugin", "list", "--data-dir"])
        .arg(&data)
        .env("KAKUNE_TOKEN", "fake-secret-observation")
        .output()
        .unwrap();
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!combined.contains("fake-secret-observation"));
    assert!(!output.status.success(), "missing storage must be reported");
    before.ensure_unchanged(&data).unwrap();
}

#[test]
fn inventory_read_only_commands_preserve_old_storage_and_execution_sentinels() {
    let fixture = ManagementFixture::new().unwrap();
    let data = fixture.path("old-store");
    fs::create_dir_all(&data).unwrap();
    let db_path = data.join("kakune.sqlite3");
    let db = Connection::open(&db_path).unwrap();
    db.execute_batch(include_str!("fixtures/canonical_commands/schema-zero.sql"))
        .unwrap();
    drop(db);
    let workflow = fixture.path("sentinel.yaml");
    fs::write(
        &workflow,
        include_str!("fixtures/canonical_commands/workflow-sentinel.kakune.yaml"),
    )
    .unwrap();
    let plugin_marker = fixture.path("canonical-plugin-executed.txt");
    let workflow_marker = fixture.path("canonical-workflow-executed.txt");
    let contexts = fixture.path("absent-contexts.json");
    let config = fixture.path("absent-config.yaml");
    let old_before = FilesystemSnapshot::capture(&data).unwrap();
    let secret = "fake-secret-inventory-matrix";
    let data = data.to_string_lossy().into_owned();
    let workflow = workflow.to_string_lossy().into_owned();
    let contexts = contexts.to_string_lossy().into_owned();
    let config = config.to_string_lossy().into_owned();
    let commands: Vec<(&str, Vec<&str>, bool)> = vec![
        ("version", vec!["version"], true),
        (
            "doctor",
            vec!["doctor", "--data-dir", &data, "--config", &config, "--json"],
            false,
        ),
        (
            "daemon status",
            vec!["daemon", "status", "--data-dir", &data],
            true,
        ),
        (
            "context list",
            vec!["context", "list", "--contexts-file", &contexts],
            true,
        ),
        (
            "workflow validate",
            vec![
                "workflow",
                "validate",
                "--file",
                &workflow,
                "--data-dir",
                &data,
            ],
            true,
        ),
        (
            "workflow plan",
            vec!["workflow", "plan", "--file", &workflow, "--data-dir", &data],
            true,
        ),
        (
            "plugin list",
            vec!["plugin", "list", "--data-dir", &data],
            false,
        ),
        (
            "workflow list",
            vec!["--context-file", &contexts, "workflow", "list"],
            false,
        ),
        (
            "stored workflow validate",
            vec![
                "--context-file",
                &contexts,
                "workflow",
                "validate",
                "missing",
            ],
            false,
        ),
        (
            "stored workflow plan",
            vec!["--context-file", &contexts, "workflow", "plan", "missing"],
            false,
        ),
        (
            "execution list",
            vec!["--context-file", &contexts, "execution", "list"],
            false,
        ),
        (
            "execution inspect",
            vec![
                "--context-file",
                &contexts,
                "execution",
                "inspect",
                "missing",
            ],
            false,
        ),
        (
            "execution logs",
            vec!["--context-file", &contexts, "execution", "logs", "missing"],
            false,
        ),
        (
            "trigger list",
            vec!["--context-file", &contexts, "trigger", "list"],
            false,
        ),
        (
            "trigger inspect",
            vec!["--context-file", &contexts, "trigger", "inspect", "missing"],
            false,
        ),
        (
            "provider list",
            vec!["--context-file", &contexts, "provider", "list"],
            false,
        ),
        (
            "provider models",
            vec!["--context-file", &contexts, "provider", "models", "missing"],
            false,
        ),
        (
            "mcp list",
            vec!["--context-file", &contexts, "mcp", "list"],
            false,
        ),
        (
            "mcp inspect",
            vec!["--context-file", &contexts, "mcp", "inspect", "missing"],
            false,
        ),
        (
            "secret list",
            vec!["--context-file", &contexts, "secret", "list"],
            false,
        ),
    ];
    for (name, args, succeeds) in commands {
        let output = Command::new(env!("CARGO_BIN_EXE_kakune"))
            .args(args)
            .current_dir(fixture.root())
            .env_remove("KAKUNE_ENDPOINT")
            .env("KAKUNE_TOKEN", secret)
            .output()
            .unwrap();
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.status.success(), succeeds, "{name}: {combined}");
        assert!(!combined.contains(secret), "{name} disclosed a secret");
        old_before
            .ensure_unchanged(std::path::Path::new(&data))
            .unwrap_or_else(|e| panic!("{name} changed storage: {e}"));
        assert!(!workflow_marker.exists(), "{name} executed the workflow");
        assert!(!plugin_marker.exists(), "{name} executed a plugin");
        assert!(
            !std::path::Path::new(&contexts).exists(),
            "{name} created a context collection"
        );
    }
}

#[test]
fn installed_plugin_and_workflow_sentinels_are_never_executed_by_observers() {
    let fixture = ManagementFixture::new().unwrap();
    let data = fixture.path("current-store");
    let marker = fixture.path("plugin-executed.txt");
    let workflow_marker = fixture.path("canonical-workflow-executed.txt");
    let manifest_path = fixture.path("kakune-plugin.json");
    let executable = if cfg!(windows) {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/canonical_commands/plugin-sentinel.cmd"
        )
    } else {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/canonical_commands/plugin-sentinel.sh"
        )
    };
    let manifest = serde_json::json!({"manifestVersion":"1.0", "id":"org.example.sentinel", "name":"Sentinel", "version":"0.1.0", "pluginProtocol":">=1.0.0 <2.0.0", "runtime":{"kind":"process", "command":executable, "args":[marker]}, "contributes":{"nodes":[], "triggers":[], "providers":[], "services":[]}});
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let store = Store::open(data.clone()).unwrap();
    store
        .upsert_plugin_install(&InstalledPluginRecord {
            id: "org.example.sentinel".into(),
            name: "Sentinel".into(),
            version: "0.1.0".into(),
            manifest_path: manifest_path.to_string_lossy().into_owned(),
            installed_at: "now".into(),
            enabled: true,
            policy: serde_json::json!({}),
            digest: "digest".into(),
            resolved_lock: serde_json::json!({}),
            provenance: serde_json::json!({}),
        })
        .unwrap();
    drop(store);
    let db = Connection::open(data.join("kakune.sqlite3")).unwrap();
    db.pragma_update(None, "journal_mode", "DELETE").unwrap();
    drop(db);
    let workflow = fixture.path("sentinel.yaml");
    fs::write(
        &workflow,
        include_str!("fixtures/canonical_commands/workflow-sentinel.kakune.yaml"),
    )
    .unwrap();
    let before = FilesystemSnapshot::capture(&data).unwrap();
    for args in [
        vec!["plugin", "list", "--data-dir", data.to_str().unwrap()],
        vec![
            "workflow",
            "validate",
            "--file",
            workflow.to_str().unwrap(),
            "--data-dir",
            data.to_str().unwrap(),
        ],
        vec![
            "workflow",
            "plan",
            "--file",
            workflow.to_str().unwrap(),
            "--data-dir",
            data.to_str().unwrap(),
        ],
    ] {
        let expect_success =
            cfg!(any(windows, target_os = "linux", target_os = "macos")) || args[0] != "plugin";
        let output = Command::new(env!("CARGO_BIN_EXE_kakune"))
            .args(args)
            .current_dir(fixture.root())
            .env("KAKUNE_TOKEN", "fake-plugin-secret")
            .output()
            .unwrap();
        assert!(
            output.status.success() == expect_success,
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("fake-plugin-secret"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("fake-plugin-secret"));
        assert!(!marker.exists());
        assert!(!workflow_marker.exists());
        before.ensure_unchanged(&data).unwrap();
    }
}

#[test]
fn plugin_list_rejects_oversized_metadata_without_rewriting_store() {
    let fixture = ManagementFixture::new().unwrap();
    let data = fixture.path("oversized-catalog");
    drop(Store::open(data.clone()).unwrap());
    let db = Connection::open(data.join("kakune.sqlite3")).unwrap();
    db.pragma_update(None, "journal_mode", "DELETE").unwrap();
    db.execute("INSERT INTO installed_plugins (id,name,version,manifest_path,installed_at) VALUES (?1,?2,'1','manifest','now')", rusqlite::params!["large", "x".repeat(8192)]).unwrap();
    drop(db);
    let before = FilesystemSnapshot::capture(&data).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .args(["plugin", "list", "--data-dir"])
        .arg(&data)
        .current_dir(fixture.root())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.len() < 1024);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(if cfg!(windows) {
        stderr.contains("limit")
    } else if cfg!(any(target_os = "linux", target_os = "macos")) {
        stderr.contains("unavailable")
    } else {
        stderr.contains("snapshot")
    });
    before.ensure_unchanged(&data).unwrap();
}

#[test]
fn plugin_list_under_exclusive_writer_lock_does_not_change_store() {
    let fixture = ManagementFixture::new().unwrap();
    let data = fixture.path("locked-catalog");
    drop(Store::open(data.clone()).unwrap());
    let db = Connection::open(data.join("kakune.sqlite3")).unwrap();
    db.pragma_update(None, "journal_mode", "DELETE").unwrap();
    // Snapshot before acquiring SQLite's process-scoped POSIX locks: opening
    // and closing the database here would release those locks on Unix.
    let before = FilesystemSnapshot::capture(&data).unwrap();
    db.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let _: i64 = db
        .query_row("SELECT COUNT(*) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .unwrap();
    let start = std::time::Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .args(["plugin", "list", "--data-dir"])
        .arg(&data)
        .current_dir(fixture.root())
        .env("KAKUNE_TOKEN", "busy-writer-fake-secret")
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(start.elapsed() < std::time::Duration::from_secs(12));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("busy-writer-fake-secret"));
    before.ensure_unchanged(&data).unwrap();
    db.execute_batch("ROLLBACK").unwrap();
}
