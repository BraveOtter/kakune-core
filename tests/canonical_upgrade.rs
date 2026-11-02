mod support;

use std::{fs, process::Command};

use kakune_core::{AuthScope, InstalledPluginRecord, Store, WorkflowDocument};
use serde_json::json;
use support::management::{FilesystemSnapshot, ManagementFixture};

const WORKFLOW_SOURCE: &str = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: preserved-workflow\n  name: Preserved Workflow\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: start\nnodes:\n  - id: start\n    type: kakune.flow.pass@1\n";

#[test]
fn removed_cli_paths_preserve_existing_workflows_plugins_credentials_and_storage() {
    let fixture = ManagementFixture::new().expect("isolated fixture should be created");
    let data_dir = fixture.path("existing-core");
    fs::create_dir_all(&data_dir).unwrap();
    let config_path = fixture.path("kakune.yaml");
    fs::write(
        &config_path,
        serde_yaml::to_string(&kakune_core::CoreConfig::default()).unwrap(),
    )
    .unwrap();
    let contexts_path = fixture.path("contexts.json");
    fs::write(
        &contexts_path,
        br#"{"format":"kakune-contexts/v1","activeContextId":"local","contexts":[{"id":"local","name":"Local","endpoint":"http://127.0.0.1:8787","credentialRef":"env:KAKUNE_TOKEN"}]}"#,
    )
    .unwrap();

    let store = Store::open(data_dir.clone()).expect("existing Core store should open");
    let workflow = WorkflowDocument::parse(WORKFLOW_SOURCE).unwrap();
    store
        .create_workflow(&workflow, WORKFLOW_SOURCE, "disabled")
        .unwrap()
        .expect("workflow should be inserted");
    let credential = store
        .create_auth_token(
            "preserved administrator".to_owned(),
            vec![AuthScope::Admin],
            None,
        )
        .expect("credential should be created");

    let plugin_dir = data_dir.join("plugins").join("org.example.preserved");
    fs::create_dir_all(&plugin_dir).unwrap();
    let manifest = plugin_dir.join("manifest.json");
    fs::write(&manifest, br#"{"identity":"org.example.preserved"}"#).unwrap();
    store
        .upsert_plugin_install(&InstalledPluginRecord {
            id: "org.example.preserved".to_owned(),
            name: "Preserved plugin".to_owned(),
            version: "1.0.0".to_owned(),
            manifest_path: manifest.to_string_lossy().into_owned(),
            installed_at: "2026-11-02T00:00:00Z".to_owned(),
            enabled: true,
            policy: json!({}),
            digest: "sha256:preserved".to_owned(),
            resolved_lock: json!({}),
            provenance: json!({}),
        })
        .unwrap();
    let core_id = store.core_id().unwrap();
    drop(store);

    let before = FilesystemSnapshot::capture(fixture.root()).unwrap();
    let data = data_dir.to_string_lossy().into_owned();
    let workflow_file = fixture.path("workflow.yaml").to_string_lossy().into_owned();
    let provider_file = fixture.path("profile.json").to_string_lossy().into_owned();
    let mcp_file = fixture.path("call.json").to_string_lossy().into_owned();
    let backup = fixture
        .path("backup.sqlite3")
        .to_string_lossy()
        .into_owned();
    let restored = fixture.path("restored").to_string_lossy().into_owned();
    let plugin_dir = fixture.path("package").to_string_lossy().into_owned();
    let removed = vec![
        vec!["run".to_owned(), workflow_file],
        vec![
            "executions".to_owned(),
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec![
            "inspect".to_owned(),
            "execution-1".to_owned(),
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec!["auth".to_owned()],
        vec![
            "auth".to_owned(),
            "recover".to_owned(),
            "--data-dir".to_owned(),
            data.clone(),
            "--config".to_owned(),
            config_path.to_string_lossy().into_owned(),
        ],
        vec![
            "auth".to_owned(),
            "pair".to_owned(),
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec![
            "auth".to_owned(),
            "tokens".to_owned(),
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec![
            "auth".to_owned(),
            "revoke".to_owned(),
            "token-1".to_owned(),
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec!["storage".to_owned()],
        vec![
            "storage".to_owned(),
            "backup".to_owned(),
            backup.clone(),
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec!["storage".to_owned(), "restore".to_owned(), backup, restored],
        vec![
            "storage".to_owned(),
            "retain".to_owned(),
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec![
            "storage".to_owned(),
            "compact".to_owned(),
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec![
            "storage".to_owned(),
            "pin".to_owned(),
            "execution-1".to_owned(),
            "--value".to_owned(),
            "true".to_owned(),
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec![
            "provider".to_owned(),
            "upsert".to_owned(),
            provider_file,
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec![
            "provider".to_owned(),
            "status".to_owned(),
            "provider-1".to_owned(),
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec![
            "provider".to_owned(),
            "remove".to_owned(),
            "provider-1".to_owned(),
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec![
            "provider".to_owned(),
            "login".to_owned(),
            "provider-1".to_owned(),
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec![
            "mcp".to_owned(),
            "call".to_owned(),
            mcp_file,
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec![
            "plugin".to_owned(),
            "prepare".to_owned(),
            plugin_dir,
            "--data-dir".to_owned(),
            data.clone(),
        ],
        vec![
            "plugin".to_owned(),
            "commit".to_owned(),
            "prepared-1".to_owned(),
            "--digest".to_owned(),
            "sha256:test".to_owned(),
            "--data-dir".to_owned(),
            data.clone(),
        ],
    ];

    for candidate in &removed {
        let mut command = Command::new(env!("CARGO_BIN_EXE_kakune"));
        command
            .args(candidate)
            .env("KAKUNE_DATA_DIR", &data_dir)
            .env("KAKUNE_TOKEN", &credential.token)
            .current_dir(fixture.root());
        let output = command.output().expect("CLI should run");
        assert_eq!(
            output.status.code(),
            Some(2),
            "removed path should fail before touching storage: {candidate:?}\nstderr={}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stdout.is_empty());
        before
            .ensure_unchanged(fixture.root())
            .expect("removed path must not mutate installation files");
    }

    let reopened = Store::open(data_dir.clone()).unwrap();
    assert_eq!(reopened.core_id().unwrap(), core_id);
    assert!(
        reopened
            .authorize_scope(&credential.token, AuthScope::Admin)
            .unwrap()
    );
    assert_eq!(
        reopened
            .get_workflow_source("preserved-workflow")
            .unwrap()
            .unwrap()
            .source,
        WORKFLOW_SOURCE
    );
    let plugins = reopened.list_installed_plugins().unwrap();
    assert_eq!(plugins.len(), 1);
    assert_eq!(plugins[0].id, "org.example.preserved");
    assert_eq!(
        fs::read(&manifest).unwrap(),
        br#"{"identity":"org.example.preserved"}"#
    );
    drop(reopened);
    before
        .ensure_unchanged(fixture.root())
        .expect("verification should leave the preserved installation unchanged");
}
