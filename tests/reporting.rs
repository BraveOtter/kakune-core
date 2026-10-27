use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::http::StatusCode;
use kakune_core::{
    CoreDiagnosticSource, DiagnosticReporter, PluginRegistry, Store, WorkflowDocument, api,
    config::ApiConfig, lifecycle::CoreLifecycle, plugin_process::PluginManifest,
};
use serde_json::{Value, json};

#[tokio::test]
async fn api_execution_failures_use_the_injected_diagnostic_reporter() {
    let data_dir = temp_directory("api-reporting");
    let plugin_dir = temp_directory("api-reporting-plugin");
    fs::create_dir(plugin_dir.join("definitions")).expect("definition directory created");
    let manifest_path = plugin_dir.join("kakune-plugin.json");
    let helper = plugin_helper_command();
    let manifest = json!({
        "manifestVersion": "1.0",
        "id": "org.example.reporting-plugin",
        "name": "Reporting plugin",
        "version": "0.1.0",
        "pluginProtocol": ">=1.0.0 <2.0.0",
        "runtime": {"kind": "process", "command": helper, "args": []},
        "contributes": {"nodes": ["definitions/fail.json"]}
    });
    let definition = json!({
        "apiVersion": "kakune.dev/v1",
        "kind": "NodeDefinition",
        "type": "org.example.greet@1",
        "name": "Reporting failure",
        "inputSchema": {"type": "object"},
        "outputSchema": {"type": "object"}
    });
    fs::write(&manifest_path, manifest.to_string()).expect("manifest written");
    fs::write(
        plugin_dir.join("definitions/fail.json"),
        definition.to_string(),
    )
    .expect("definition written");

    let mut plugins = PluginRegistry::default();
    plugins
        .register_manifest(PluginManifest::load(&manifest_path).expect("manifest loads"))
        .expect("plugin node registers");
    let store = Store::open(data_dir.clone()).expect("store opens");
    let token = store
        .ensure_bootstrap_token()
        .expect("bootstrap token is available")
        .expect("fresh store needs a bootstrap token");
    let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: api-report\n  name: API reporting\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: fail\nnodes:\n  - id: fail\n    type: org.example.greet@1\n    inputs:\n      name: { literal: fail }\n";
    let workflow = WorkflowDocument::parse(source).expect("workflow parses");
    store
        .upsert_workflow(&workflow, source, "enabled")
        .expect("workflow persists");

    let captured = Arc::new(Mutex::new(Vec::new()));
    let reporter = {
        let captured = Arc::clone(&captured);
        DiagnosticReporter::new(move |diagnostic| {
            captured.lock().expect("diagnostic lock").push(diagnostic);
        })
    };
    let app = api::router_with_lifecycle_and_reporter(
        store.clone(),
        plugins,
        ApiConfig::default(),
        CoreLifecycle::ready(),
        reporter,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("API listener binds");
    let address = listener.local_addr().expect("listener address available");
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("reporting API should serve");
    });
    let response = reqwest::Client::new()
        .post(format!("http://{address}/api/v1/executions"))
        .bearer_auth(token)
        .json(&json!({"workflowId": "api-report", "inputs": {}}))
        .send()
        .await
        .expect("execution request completes");
    assert_eq!(response.status(), StatusCode::CREATED);
    let execution: Value = response.json().await.expect("execution is JSON");
    let execution_id = execution["id"]
        .as_str()
        .expect("response contains execution ID")
        .to_string();

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let failed = store
                .get_execution(&execution_id)
                .expect("execution observation succeeds")
                .is_some_and(|record| record.status == "failed");
            let reported = captured
                .lock()
                .expect("diagnostic lock")
                .iter()
                .any(|item| {
                    item.source == CoreDiagnosticSource::Api
                        && item.message.contains(&execution_id)
                        && item.message.contains("failed")
                });
            if failed && reported {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("execution failure should be reported");

    {
        let captured = captured.lock().expect("diagnostic lock");
        assert!(
            captured.iter().any(|item| {
                item.source == CoreDiagnosticSource::Api
                    && item.message.contains(&execution_id)
                    && item
                        .message
                        .contains("plugin returned JSON-RPC error: rejected")
            }),
            "unexpected API diagnostics: {captured:?}"
        );
    }
    server.abort();
    let _ = server.await;
    drop(store);
    remove_directory(&data_dir);
    remove_directory(&plugin_dir);
}

fn plugin_helper_command() -> String {
    if let Ok(command) = env::var("CARGO_BIN_EXE_plugin-helper") {
        return command;
    }
    env::current_exe()
        .ok()
        .and_then(|path| path.parent().and_then(Path::parent).map(Path::to_path_buf))
        .map(|directory| {
            directory
                .join(format!("plugin-helper{}", env::consts::EXE_SUFFIX))
                .to_string_lossy()
                .into_owned()
        })
        .unwrap_or_else(|| "plugin-helper".to_string())
}

fn temp_directory(label: &str) -> PathBuf {
    let path = env::temp_dir().join(format!("kakune-reporting-{label}-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&path).expect("temporary directory created");
    path
}

fn remove_directory(path: &Path) {
    let mut last_error = None;
    for _ in 0..20 {
        match fs::remove_dir_all(path) {
            Ok(()) => return,
            Err(error) => {
                last_error = Some(error);
                std::thread::sleep(Duration::from_millis(25));
            }
        }
    }
    panic!(
        "test-owned reporting fixture should be removed: {}",
        last_error.expect("cleanup error should be recorded")
    );
}
