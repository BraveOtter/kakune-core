mod support;

use std::{fs, process::Command};
use support::management::{InProcessCore, ManagementFixture};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn workflow_list_targets_only_the_selected_core() {
    let fixture = ManagementFixture::new().unwrap();
    let mut first = InProcessCore::start(fixture.path("first")).await.unwrap();
    let mut second = InProcessCore::start(fixture.path("second")).await.unwrap();
    for (core, id) in [(&first, "first-workflow"), (&second, "second-workflow")] {
        let source = include_str!("../examples/write-note.kakune.yaml").replace("write-note", id);
        let workflow = kakune_core::WorkflowDocument::parse(&source).unwrap();
        core.store
            .upsert_workflow(&workflow, &source, "disabled")
            .unwrap();
    }
    let contexts = fixture.path("contexts.json");
    fs::write(&contexts, serde_json::to_vec(&serde_json::json!({
        "format":"kakune-contexts/v1", "exportedAt":"2026-10-26T00:00:00Z",
        "activeContextId":"first", "revision":0,
        "contexts":[
            {"id":"first", "name":"First", "endpoint":first.endpoint, "expectedCoreId":first.store.core_id().unwrap(), "color":null, "credentialRef":null},
            {"id":"second", "name":"Second", "endpoint":second.endpoint, "expectedCoreId":second.store.core_id().unwrap(), "color":null, "credentialRef":null}
        ]
    })).unwrap()).unwrap();
    let run = |args: &[&str], token: &str| {
        Command::new(env!("CARGO_BIN_EXE_kakune"))
            .env("KAKUNE_DATA_DIR", fixture.path("unused-local"))
            .env("KAKUNE_TOKEN", token)
            .arg("--context-file")
            .arg(&contexts)
            .args(args)
            .output()
            .unwrap()
    };
    let list = run(&["workflow", "list", "--json"], &first.token);
    assert!(
        list.status.success(),
        "{}",
        String::from_utf8_lossy(&list.stderr)
    );
    let first_output = String::from_utf8_lossy(&list.stdout);
    assert!(first_output.contains("first-workflow"));
    assert!(!first_output.contains("second-workflow"));
    let select = run(&["context", "use", "second"], &second.token);
    assert!(
        select.status.success(),
        "{}",
        String::from_utf8_lossy(&select.stderr)
    );
    let list = run(&["workflow", "list", "--json"], &second.token);
    assert!(
        list.status.success(),
        "{}",
        String::from_utf8_lossy(&list.stderr)
    );
    let second_output = String::from_utf8_lossy(&list.stdout);
    assert!(second_output.contains("second-workflow"));
    assert!(!second_output.contains("first-workflow"));
    let execution = run(
        &["workflow", "run", "id:second-workflow", "--json"],
        &second.token,
    );
    assert!(
        execution.status.success(),
        "{}",
        String::from_utf8_lossy(&execution.stderr)
    );
    assert_eq!(first.store.list_executions().unwrap().len(), 0);
    assert_eq!(second.store.list_executions().unwrap().len(), 1);
    assert!(!fixture.path("unused-local").exists());
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
}

#[test]
fn global_context_file_is_used_by_context_management() {
    let root = std::env::temp_dir()
        .join("opencode")
        .join(format!("canonical-context-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).expect("isolated test directory");
    let contexts = root.join("contexts.json");
    for args in [
        vec![
            "context",
            "add",
            "example",
            "--endpoint",
            "http://127.0.0.1:9876",
        ],
        vec!["context", "use", "example"],
        vec!["context", "inspect", "example"],
        vec!["context", "list"],
        vec!["context", "remove", "example"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_kakune"))
            .env("KAKUNE_DATA_DIR", root.join("default-data"))
            .arg("--context-file")
            .arg(&contexts)
            .args(&args)
            .output()
            .expect("run CLI");
        assert!(
            output.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(contexts.exists(), "global context file was not used");
    }
    fs::remove_dir_all(root).expect("remove isolated test directory");
}

#[test]
fn positional_standalone_runs_locally_without_a_context() {
    let temp = std::env::temp_dir()
        .join("opencode")
        .join(format!("canonical-workflows-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&temp).expect("isolated test directory");
    let workflow = temp.join("workflow.yaml");
    fs::write(
        &workflow,
        include_str!("../examples/write-note.kakune.yaml"),
    )
    .expect("workflow fixture");
    let output = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .args(["workflow", "run"])
        .arg(&workflow)
        .arg("--standalone")
        .arg("--data-dir")
        .arg(temp.join("data"))
        .arg("--context-file")
        .arg(temp.join("missing-contexts.json"))
        .output()
        .expect("run CLI");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!temp.join("missing-contexts.json").exists());
    fs::remove_dir_all(temp).expect("remove isolated test directory");
}

#[test]
fn positional_standalone_rejects_explicit_context_before_creating_data() {
    let root = std::env::temp_dir()
        .join("opencode")
        .join(format!("canonical-workflows-{}", uuid::Uuid::new_v4()));
    let output = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .args([
            "workflow",
            "run",
            "missing.yaml",
            "--standalone",
            "--context",
            "remote",
            "--data-dir",
        ])
        .arg(&root)
        .output()
        .expect("run CLI");
    assert!(!output.status.success());
    assert!(!root.exists());
}

#[test]
fn context_export_and_import_use_global_override_and_do_not_export_secret_values() {
    let root = std::env::temp_dir()
        .join("opencode")
        .join(format!("canonical-context-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let source = root.join("source.json");
    let target = root.join("target.json");
    let exported = root.join("export.json");
    let command = |contexts: &std::path::Path, args: &[&std::ffi::OsStr]| {
        Command::new(env!("CARGO_BIN_EXE_kakune"))
            .env("KAKUNE_DATA_DIR", root.join("default-data"))
            .env("KAKUNE_TOKEN", "private-context-canary")
            .arg("--context-file")
            .arg(contexts)
            .args(args)
            .output()
            .unwrap()
    };
    let add = command(
        &source,
        &[
            "context".as_ref(),
            "add".as_ref(),
            "remote".as_ref(),
            "--endpoint".as_ref(),
            "https://example.invalid".as_ref(),
            "--credential-ref".as_ref(),
            "keyring:fixture".as_ref(),
        ],
    );
    assert!(
        add.status.success(),
        "{}",
        String::from_utf8_lossy(&add.stderr)
    );
    let export = command(
        &source,
        &[
            "context".as_ref(),
            "export".as_ref(),
            "remote".as_ref(),
            "--output".as_ref(),
            exported.as_os_str(),
        ],
    );
    assert!(
        export.status.success(),
        "{}",
        String::from_utf8_lossy(&export.stderr)
    );
    assert!(
        !fs::read_to_string(&exported)
            .unwrap()
            .contains("private-context-canary")
    );
    let import = command(
        &target,
        &["context".as_ref(), "import".as_ref(), exported.as_os_str()],
    );
    assert!(
        import.status.success(),
        "{}",
        String::from_utf8_lossy(&import.stderr)
    );
    let inspect = command(
        &target,
        &["context".as_ref(), "inspect".as_ref(), "remote".as_ref()],
    );
    assert!(inspect.status.success());
    assert!(String::from_utf8_lossy(&inspect.stdout).contains("example.invalid"));
    assert!(!String::from_utf8_lossy(&inspect.stdout).contains("private-context-canary"));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn invalid_context_import_and_unknown_selection_preserve_existing_collection() {
    let root = std::env::temp_dir()
        .join("opencode")
        .join(format!("canonical-context-errors-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let contexts = root.join("contexts.json");
    let import = root.join("invalid.json");
    fs::write(&import, b"{not valid json}").unwrap();
    let invoke = |args: &[&std::ffi::OsStr]| {
        Command::new(env!("CARGO_BIN_EXE_kakune"))
            .env("KAKUNE_DATA_DIR", root.join("unused"))
            .arg("--context-file")
            .arg(&contexts)
            .args(args)
            .output()
            .unwrap()
    };
    let added = invoke(&[
        "context".as_ref(),
        "add".as_ref(),
        "existing".as_ref(),
        "--endpoint".as_ref(),
        "http://127.0.0.1:9876".as_ref(),
    ]);
    assert!(
        added.status.success(),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );
    let selected = invoke(&["context".as_ref(), "use".as_ref(), "existing".as_ref()]);
    assert!(selected.status.success());
    let before = fs::read(&contexts).unwrap();
    let rejected = invoke(&["context".as_ref(), "use".as_ref(), "missing".as_ref()]);
    assert!(!rejected.status.success());
    assert_eq!(fs::read(&contexts).unwrap(), before);
    let rejected = invoke(&["context".as_ref(), "import".as_ref(), import.as_os_str()]);
    assert!(!rejected.status.success());
    assert_eq!(fs::read(&contexts).unwrap(), before);
    assert!(!root.join("unused").exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn invalid_nested_workflow_fails_before_standalone_runtime_creation() {
    let root = std::env::temp_dir()
        .join("opencode")
        .join(format!("canonical-nested-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let workflow = root.join("invalid.yaml");
    fs::write(
        &workflow,
        include_str!("fixtures/canonical_commands/nested-plugin.kakune.yaml"),
    )
    .unwrap();
    let data = root.join("data");
    let output = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .env("KAKUNE_DATA_DIR", root.join("default-data"))
        .args(["workflow", "run"])
        .arg(&workflow)
        .arg("--standalone")
        .arg("--data-dir")
        .arg(&data)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        !data.exists(),
        "invalid nested graph must fail before runtime creation"
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn ordinary_workflow_run_does_not_fall_back_when_selected_core_is_unavailable() {
    let root = std::env::temp_dir()
        .join("opencode")
        .join(format!("canonical-offline-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let contexts = root.join("contexts.json");
    let workflow = root.join("workflow.yaml");
    let data = root.join("runtime-data");
    fs::write(
        &workflow,
        include_str!("../examples/write-note.kakune.yaml"),
    )
    .unwrap();
    let run = |args: &[&std::ffi::OsStr]| {
        Command::new(env!("CARGO_BIN_EXE_kakune"))
            .env("KAKUNE_DATA_DIR", &data)
            .env("KAKUNE_TOKEN", "test-token")
            .arg("--context-file")
            .arg(&contexts)
            .args(args)
            .output()
            .unwrap()
    };
    let add = run(&[
        "context".as_ref(),
        "add".as_ref(),
        "offline".as_ref(),
        "--endpoint".as_ref(),
        "http://127.0.0.1:1".as_ref(),
    ]);
    assert!(
        add.status.success(),
        "{}",
        String::from_utf8_lossy(&add.stderr)
    );
    let select = run(&["context".as_ref(), "use".as_ref(), "offline".as_ref()]);
    assert!(
        select.status.success(),
        "{}",
        String::from_utf8_lossy(&select.stderr)
    );
    let result = run(&[
        "workflow".as_ref(),
        "run".as_ref(),
        workflow.as_os_str(),
        "--data-dir".as_ref(),
        data.as_os_str(),
    ]);
    assert!(!result.status.success());
    assert!(
        !data.exists(),
        "unavailable Core must not cause a local execution"
    );
    fs::remove_dir_all(root).unwrap();
}
