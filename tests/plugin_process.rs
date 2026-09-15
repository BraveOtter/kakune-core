use std::{
    env, fs,
    path::{Path, PathBuf},
    time::Duration,
};

use kakune_core::plugin_install::{PluginSource, commit, prepare};
use kakune_core::plugin_process::{
    PluginHost, PluginHostError, PluginHostOptions, PluginManifest, PluginRegistry,
};
use kakune_core::{
    Store, WorkflowDocument, execute_prepared_workflow_with_plugins,
    prepare_execution_with_plugins, run_workflow_with_plugins,
};
use serde_json::{Value, json};

fn helper_command() -> String {
    if let Ok(command) = env::var("KAKUNE_PLUGIN_HELPER") {
        return command;
    }
    if let Ok(command) = env::var("CARGO_BIN_EXE_plugin-helper") {
        return command;
    }
    env::current_exe()
        .ok()
        .and_then(|path| {
            path.parent()
                .and_then(|path| path.parent())
                .map(Path::to_path_buf)
        })
        .map(|directory| {
            directory
                .join(format!("plugin-helper{}", env::consts::EXE_SUFFIX))
                .to_string_lossy()
                .into_owned()
        })
        .unwrap_or_else(|| "plugin-helper".to_string())
}

fn helper_manifest(command: &str) -> Value {
    json!({
        "manifestVersion": "1.0",
        "id": "org.example.test-plugin",
        "name": "Test plugin",
        "version": "0.1.0",
        "pluginProtocol": ">=1.0.0 <2.0.0",
        "runtime": {
            "kind": "process",
            "command": command,
            "args": []
        }
    })
}

#[tokio::test]
async fn bidirectional_requests_are_serviced_while_a_node_request_is_pending() {
    let path = write_manifest(&helper_command());
    let manifest = PluginManifest::load(&path).unwrap();
    let mut host = PluginHost::start(&manifest, PluginHostOptions::default())
        .await
        .unwrap();
    let denied = host.request("host-roundtrip", json!({})).await.unwrap();
    assert!(denied.get("error").is_some());
    host.set_host_handler(std::sync::Arc::new(|method, params| {
        Box::pin(async move {
            assert_eq!(method, "artifact/write");
            assert_eq!(params["text"], "roundtrip");
            Ok(json!({"id":"authorized-artifact"}))
        })
    }));
    let allowed = host.request("host-roundtrip", json!({})).await.unwrap();
    assert_eq!(allowed["result"]["id"], "authorized-artifact");
    host.shutdown().await.unwrap();
    drop(host);
    fs::remove_dir_all(path.parent().unwrap()).unwrap();
}

fn write_manifest(command: &str) -> PathBuf {
    let directory = temp_directory("process");
    let manifest_path = directory.join("kakune-plugin.json");
    let source = helper_manifest(command);
    fs::write(&manifest_path, source.to_string()).expect("manifest written");
    manifest_path
}

fn write_node_manifest(command: &str) -> PathBuf {
    let manifest_path = write_manifest(command);
    let directory = manifest_path.parent().expect("manifest parent");
    fs::create_dir(directory.join("definitions")).expect("definition directory created");
    let manifest = json!({
        "manifestVersion": "1.0",
        "id": "org.example.test-plugin",
        "name": "Test plugin",
        "version": "0.1.0",
        "pluginProtocol": ">=1.0.0 <2.0.0",
        "runtime": {
            "kind": "process",
            "command": command,
            "args": []
        },
        "contributes": {
            "nodes": ["definitions/greet.json"],
            "triggers": [],
            "providers": [],
            "services": []
        }
    });
    let definition = json!({
        "apiVersion": "kakune.dev/v1",
        "kind": "NodeDefinition",
        "type": "org.example.greet@1",
        "name": "Greet",
        "inputSchema": {"type": "object"},
        "outputSchema": {"type": "object"}
    });
    fs::write(&manifest_path, manifest.to_string()).expect("manifest written");
    fs::write(
        directory.join("definitions/greet.json"),
        definition.to_string(),
    )
    .expect("definition written");
    manifest_path
}

fn temp_directory(name: &str) -> PathBuf {
    let directory = env::temp_dir().join(format!(
        "kakune-plugin-process-test-{name}-{}",
        uuid::Uuid::new_v4()
    ));
    fs::create_dir(&directory).expect("test directory created");
    directory
}

fn remove_manifest(path: &Path) {
    let directory = path.parent().expect("manifest parent");
    fs::remove_dir_all(directory).expect("test directory removed");
}

#[tokio::test]
async fn verifies_process_protocol_and_bounded_stderr() {
    let manifest_path = write_manifest(&helper_command());
    let manifest = PluginManifest::load(&manifest_path).expect("manifest loads");
    let mut host = PluginHost::start(
        &manifest,
        PluginHostOptions {
            max_stderr_bytes: 8,
            ..PluginHostOptions::default()
        },
    )
    .await
    .expect("host starts");

    let response = host
        .request("initialize", json!({"host": "test"}))
        .await
        .expect("request succeeds");
    assert_eq!(response, json!({"ok": true}));
    assert_eq!(
        host.take_notifications(),
        vec![kakune_core::plugin_process::PluginNotification {
            method: "log/emit".to_string(),
            params: Some(json!({"message": "started"})),
        }]
    );
    host.shutdown().await.expect("host shuts down cleanly");
    let stderr = host.stderr();
    assert_eq!(stderr.bytes.len(), 8);
    assert!(stderr.truncated);
    remove_manifest(&manifest_path);
}

#[tokio::test]
async fn verifies_request_timeout_and_clean_termination() {
    let manifest_path = write_manifest(&helper_command());
    let manifest = PluginManifest::load(&manifest_path).expect("manifest loads");
    let mut host = PluginHost::start(
        &manifest,
        PluginHostOptions {
            request_timeout: Duration::from_millis(50),
            ..PluginHostOptions::default()
        },
    )
    .await
    .expect("host starts");

    assert!(matches!(
        host.request("ignore", Value::Null).await,
        Err(PluginHostError::Timeout(_))
    ));
    assert!(matches!(
        host.request("initialize", Value::Null).await,
        Err(PluginHostError::Stopped)
    ));
    remove_manifest(&manifest_path);
}

#[tokio::test]
async fn verifies_message_size_limit() {
    let manifest_path = write_manifest(&helper_command());
    let manifest = PluginManifest::load(&manifest_path).expect("manifest loads");
    let mut host = PluginHost::start(
        &manifest,
        PluginHostOptions {
            max_message_bytes: 128,
            ..PluginHostOptions::default()
        },
    )
    .await
    .expect("host starts");

    assert!(matches!(
        host.request("oversize", Value::Null).await,
        Err(PluginHostError::MessageTooLarge { limit: 128 })
    ));
    assert!(matches!(
        host.request("initialize", Value::Null).await,
        Err(PluginHostError::Stopped)
    ));
    remove_manifest(&manifest_path);
}

#[test]
fn executes_an_explicitly_registered_plugin_node() {
    let manifest_path = write_node_manifest(&helper_command());
    let manifest = PluginManifest::load(&manifest_path).expect("manifest loads");
    let mut registry = PluginRegistry::default();
    registry
        .register_manifest(manifest)
        .expect("node registers");
    let directory = temp_directory("runtime");
    let store = Store::open(directory.clone()).expect("store opens");
    let workflow = WorkflowDocument::parse(
        "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: plugin-node\n  name: Plugin node\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: greet\nnodes:\n  - id: greet\n    type: org.example.greet@1\n    inputs:\n      name: { literal: Ada }\n",
    )
    .expect("workflow parses");

    let execution = run_workflow_with_plugins(&store, &workflow, &registry).expect("workflow runs");
    let node_runs = store.list_node_runs(&execution.id).expect("node runs list");
    assert_eq!(node_runs.len(), 1);
    assert_eq!(node_runs[0].status, "succeeded");
    assert_eq!(
        node_runs[0].result,
        Some(json!({
            "route": "success",
            "outputs": {"greeting": "Hello, Ada!"},
            "message": "greeted Ada"
        }))
    );
    assert_eq!(node_runs[0].error, None);
    drop(store);
    remove_manifest(&manifest_path);
    fs::remove_dir_all(directory).expect("temporary data removed");
}

#[test]
fn executes_an_explicitly_registered_plugin_node_in_a_structured_body() {
    let manifest_path = write_node_manifest(&helper_command());
    let manifest = PluginManifest::load(&manifest_path).expect("manifest loads");
    let mut registry = PluginRegistry::default();
    registry
        .register_manifest(manifest)
        .expect("node registers");
    let directory = temp_directory("runtime-body");
    let store = Store::open(directory.clone()).expect("store opens");
    let workflow = WorkflowDocument::parse(
        "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: plugin-body\n  name: Plugin body\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: each\nnodes:\n  - id: each\n    type: kakune.flow.foreach@1\n    inputs:\n      items: { literal: [Ada] }\n    with:\n      maxConcurrency: 1\n    body:\n      entry: greet\n      nodes:\n        - id: greet\n          type: org.example.greet@1\n          inputs:\n            name: { from: $item }\n      outputs:\n        greeting: { from: greet.greeting }\n",
    )
    .expect("workflow parses");

    let execution = run_workflow_with_plugins(&store, &workflow, &registry).expect("workflow runs");
    let node_runs = store.list_node_runs(&execution.id).expect("node runs list");
    assert!(node_runs.iter().any(|node| {
        node.result
            .as_ref()
            .and_then(|result| result.pointer("/outputs/greeting"))
            == Some(&json!("Hello, Ada!"))
    }));
    drop(store);
    remove_manifest(&manifest_path);
    fs::remove_dir_all(directory).expect("temporary data removed");
}

#[test]
fn persists_plugin_node_errors() {
    let manifest_path = write_node_manifest(&helper_command());
    let manifest = PluginManifest::load(&manifest_path).expect("manifest loads");
    let mut registry = PluginRegistry::default();
    registry
        .register_manifest(manifest)
        .expect("node registers");
    let directory = temp_directory("runtime-error");
    let store = Store::open(directory.clone()).expect("store opens");
    let workflow = WorkflowDocument::parse(
        "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: plugin-error\n  name: Plugin error\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: fail\nnodes:\n  - id: fail\n    type: org.example.greet@1\n    inputs:\n      name: { literal: fail }\n",
    )
    .expect("workflow parses");

    assert!(run_workflow_with_plugins(&store, &workflow, &registry).is_err());
    let execution = store
        .list_executions()
        .expect("executions list")
        .pop()
        .expect("execution persists");
    let node_runs = store.list_node_runs(&execution.id).expect("node runs list");
    assert_eq!(node_runs[0].status, "failed");
    assert_eq!(node_runs[0].result, None);
    assert!(
        node_runs[0]
            .error
            .as_deref()
            .is_some_and(|error| error.contains("plugin returned JSON-RPC error: rejected"))
    );
    drop(store);
    remove_manifest(&manifest_path);
    fs::remove_dir_all(directory).expect("temporary data removed");
}

#[tokio::test]
async fn prepares_without_executing_and_commits_only_the_reviewed_digest() {
    let source = temp_directory("prepare");
    fs::create_dir(source.join("nodes")).expect("node directory created");
    let marker = source.join("executed.txt");
    fs::write(
        source.join("plugin.cmd"),
        format!("@echo executed > \"{}\"\r\n", marker.display()),
    )
    .expect("plugin entrypoint written");
    fs::write(source.join("nodes/greet.json"), json!({
        "apiVersion": "kakune.dev/v1", "kind": "NodeDefinition", "type": "org.example.prepared@1",
        "name": "Prepared", "inputSchema": {"type":"object"}, "outputSchema": {"type":"object"}
    }).to_string()).expect("definition written");
    fs::write(source.join("kakune.plugin.json"), json!({
        "manifestVersion": "1.0", "id": "org.example.prepared", "name": "Prepared plugin", "version": "0.1.0",
        "pluginProtocol": ">=1.0.0 <2.0.0", "runtime": {"kind":"process", "command":"plugin.cmd"},
        "contributes": {"nodes": ["nodes/greet.json"]}, "permissions": {"filesystem": ["read:workspace"]}
    }).to_string()).expect("manifest written");
    let data = temp_directory("prepare-data");
    let store = Store::open(data.clone()).expect("store opens");

    let prepared = prepare(&store, PluginSource::Local(source.clone()))
        .await
        .expect("prepares");
    assert!(
        !marker.exists(),
        "inspection must not execute the plugin entrypoint"
    );
    assert!(commit(&store, &prepared.record.id, "sha256:incorrect").is_err());
    let installed =
        commit(&store, &prepared.record.id, &prepared.record.digest).expect("commits digest");
    assert_eq!(installed.digest, prepared.record.digest);
    assert_eq!(installed.policy["filesystem"], json!(["read:workspace"]));
    assert_eq!(installed.provenance["kind"], "local");
    assert!(Path::new(&installed.manifest_path).is_file());
    assert!(!marker.exists());
    drop(store);
    fs::remove_dir_all(source).expect("source removed");
    fs::remove_dir_all(data).expect("data removed");
}

#[test]
fn cancellation_terminates_a_running_plugin_host() {
    let manifest_path = write_node_manifest(&helper_command());
    let manifest = PluginManifest::load(&manifest_path).expect("manifest loads");
    let mut registry = PluginRegistry::default();
    registry
        .register_manifest(manifest)
        .expect("node registers");
    let directory = temp_directory("cancel");
    let store = Store::open(directory.clone()).expect("store opens");
    let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: plugin-cancel\n  name: Plugin cancel\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: greet\nnodes:\n  - id: greet\n    type: org.example.greet@1\n    inputs:\n      name: { literal: wait }\n";
    let execution = prepare_execution_with_plugins(&store, source, "manual", &registry)
        .expect("execution prepares");
    let worker_store = store.clone();
    let worker_registry = registry.clone();
    let worker_execution = execution.clone();
    let worker = std::thread::spawn(move || {
        execute_prepared_workflow_with_plugins(&worker_store, worker_execution, &worker_registry)
    });
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        store
            .request_execution_cancel(&execution.id)
            .expect("cancellation records")
    );
    let result = worker
        .join()
        .expect("worker joins")
        .expect("cancellation is a terminal result");
    assert_eq!(result.status, "cancelled");
    assert_eq!(
        store
            .get_execution(&execution.id)
            .expect("execution loads")
            .expect("execution exists")
            .status,
        "cancelled"
    );
    drop(store);
    remove_manifest(&manifest_path);
    fs::remove_dir_all(directory).expect("temporary data removed");
}
