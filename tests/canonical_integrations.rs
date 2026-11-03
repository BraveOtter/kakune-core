#![expect(
    clippy::duplicate_mod,
    reason = "The reused standalone integration suites each own the same fixture module."
)]

#[path = "command_surface.rs"]
mod cli_acceptance;
#[path = "integration_administration.rs"]
mod integration_acceptance;
mod support;

use kakune_core::{AuthScope, NativeCredentialBackend, SecretBackendKind, Store};
use reqwest::{Client, StatusCode};
use std::{
    collections::HashMap,
    fs,
    process::Command,
    sync::{Arc, Mutex},
};
use support::management::{InProcessCore, ManagementFixture};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execution_and_trigger_commands_report_state_and_reject_missing_ids() {
    let fixture = ManagementFixture::new().unwrap();
    let mut core = InProcessCore::start(fixture.path("execution-core"))
        .await
        .unwrap();
    let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: flow, name: Flow}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: start\nnodes: [{id: start, type: kakune.flow.pass@1}]\n";
    let workflow = kakune_core::WorkflowDocument::parse(source).unwrap();
    core.store
        .create_workflow(&workflow, source, "disabled")
        .unwrap();
    let execution = core.store.create_execution("flow").unwrap();
    core.store
        .set_execution_result(
            &execution.id,
            &serde_json::json!({"secret":"sensitive-execution-canary"}),
        )
        .unwrap();
    let contexts = fixture.path("contexts.json");
    fs::write(
        &contexts,
        serde_json::to_vec(&serde_json::json!({
            "format":"kakune-contexts/v1", "exportedAt":"2026-10-26T00:00:00Z",
            "activeContextId":"selected", "revision":0,
            "contexts":[{"id":"selected", "name":"Selected", "endpoint":core.endpoint,
                "expectedCoreId":core.store.core_id().unwrap(), "color":null, "credentialRef":null}]
        }))
        .unwrap(),
    )
    .unwrap();
    let invoke = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_kakune"))
            .env("KAKUNE_DATA_DIR", fixture.path("unused-local"))
            .env("KAKUNE_TOKEN", &core.token)
            .arg("--context-file")
            .arg(&contexts)
            .args(args)
            .output()
            .unwrap()
    };
    for args in [
        vec!["execution", "list", "--json"],
        vec!["execution", "inspect", &execution.id, "--json"],
        vec!["execution", "logs", &execution.id, "--json"],
        vec!["trigger", "list", "--json"],
    ] {
        let result = invoke(&args);
        assert!(
            result.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(!String::from_utf8_lossy(&result.stdout).contains("sensitive-execution-canary"));
    }
    let trigger_list = invoke(&["trigger", "list", "--json"]);
    let triggers: serde_json::Value = serde_json::from_slice(&trigger_list.stdout).unwrap();
    let trigger_id = triggers["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["triggerId"] == "manual")
        .unwrap()["compositeId"]
        .as_str()
        .unwrap();
    let trigger = invoke(&["trigger", "inspect", trigger_id, "--json"]);
    assert!(
        trigger.status.success(),
        "{}",
        String::from_utf8_lossy(&trigger.stderr)
    );
    let trigger: serde_json::Value = serde_json::from_slice(&trigger.stdout).unwrap();
    assert_eq!(trigger["triggerId"], "manual");
    let cancel = invoke(&[
        "execution",
        "cancel",
        &execution.id,
        "--wait-seconds",
        "0",
        "--json",
    ]);
    assert!(
        cancel.status.success(),
        "{}",
        String::from_utf8_lossy(&cancel.stderr)
    );
    let body: serde_json::Value = serde_json::from_slice(&cancel.stdout).unwrap();
    assert_eq!(body["outcome"], "requested");
    for args in [
        vec!["execution", "inspect", "missing-id"],
        vec!["trigger", "inspect", "missing-id"],
    ] {
        let result = invoke(&args);
        assert!(!result.status.success(), "{args:?}");
        assert!(!String::from_utf8_lossy(&result.stderr).contains("sensitive-execution-canary"));
    }
    assert!(!fixture.path("unused-local").exists());
    core.shutdown().await.unwrap();
}

#[derive(Default)]
struct Credentials(Mutex<HashMap<String, Vec<u8>>>);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_test_verifies_tls_and_authentication_through_cli_and_core() {
    use axum::{Router, routing::post};
    let fixture = ManagementFixture::new().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let tls = kakune_core::config::ApiConfig {
        tls_cert: Some(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/provider-test-cert.pem"
            )
            .into(),
        ),
        tls_key: Some(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/provider-test-leaf-key.pem"
            )
            .into(),
        ),
        ..Default::default()
    };
    let app = Router::new().route(
        "/v1/messages",
        post(|headers: axum::http::HeaderMap| async move {
            if headers.get("authorization").and_then(|v| v.to_str().ok())
                == Some("Bearer fake-provider-key")
            {
                (
                    StatusCode::OK,
                    axum::Json(serde_json::json!({"content":[]})),
                )
            } else {
                (
                    StatusCode::UNAUTHORIZED,
                    axum::Json(serde_json::json!({"error":"rejected"})),
                )
            }
        }),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let provider = tokio::spawn(async move {
        kakune_core::server::serve(listener, app, &tls, async {
            let _ = stopped.await;
        })
        .await
        .unwrap();
    });
    let ca =
        reqwest::Certificate::from_pem(include_bytes!("fixtures/provider-test-ca.pem")).unwrap();
    // Verify the local TLS fixture without ambient proxy settings so a later
    // CLI failure can distinguish endpoint setup from proxy routing.
    let direct_tls_response = Client::builder()
        .no_proxy()
        .add_root_certificate(ca.clone())
        .build()
        .unwrap()
        .post(format!("https://127.0.0.1:{port}/v1/messages"))
        .bearer_auth("fake-provider-key")
        .header("anthropic-version", "2023-06-01")
        .json(&serde_json::json!({
            "model": "fixture-model",
            "max_tokens": 1,
            "messages": [{"role": "user", "content": "Hi"}]
        }))
        .send()
        .await
        .unwrap_or_else(|error| panic!("direct loopback TLS control failed: {error:?}"));
    let direct_tls_status = direct_tls_response.status();
    assert_eq!(direct_tls_status, StatusCode::OK);
    let store = Store::open_with_secret_backend(
        fixture.path("provider-success"),
        SecretBackendKind::Native,
        Arc::new(Credentials::default()),
    )
    .unwrap();
    store
        .create_provider_profile(kakune_core::ProviderProfileUpsert {
            id: "minimax".into(),
            display_name: "MiniMax".into(),
            provider_type: kakune_core::ProviderType::MiniMax,
            default_model: "fixture-model".into(),
            allowed_models: vec!["fixture-model".into()],
            capabilities: vec!["text-generation".into()],
            auth: kakune_core::ProviderAuth::ApiKeySecret {
                secret_ref: "provider-key".into(),
            },
            config: serde_json::json!({"baseUrl": format!("https://127.0.0.1:{port}")}),
        })
        .unwrap();
    store
        .set_secret_versioned("provider-key", "fake-provider-key")
        .unwrap();
    let mut core =
        InProcessCore::start_with_provider_ca(store.clone(), Default::default(), Some(ca))
            .await
            .unwrap();
    let contexts = fixture.path("provider-success-contexts.json");
    fs::write(&contexts, serde_json::to_vec(&serde_json::json!({
        "format":"kakune-contexts/v1", "exportedAt":"2026-10-26T00:00:00Z", "activeContextId":"selected", "revision":0,
        "contexts":[{"id":"selected", "name":"Selected", "endpoint":core.endpoint,
            "expectedCoreId":core.store.core_id().unwrap(), "color":null, "credentialRef":null}]
    })).unwrap()).unwrap();
    let invoke = || {
        Command::new(env!("CARGO_BIN_EXE_kakune"))
            .env("KAKUNE_DATA_DIR", fixture.path("unused"))
            .env("KAKUNE_TOKEN", &core.token)
            .arg("--context-file")
            .arg(&contexts)
            .args(["provider", "test", "minimax", "--json"])
            .output()
            .unwrap()
    };
    let result = invoke();
    assert!(
        result.status.success(),
        "stderr: {}\nstdout: {}\nloopback TLS control status: {direct_tls_status}",
        String::from_utf8_lossy(&result.stderr),
        String::from_utf8_lossy(&result.stdout),
    );
    let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["connectivity"]["status"], "passed");
    assert_eq!(report["authentication"]["status"], "passed");
    assert!(!String::from_utf8_lossy(&result.stdout).contains("fake-provider-key"));
    store
        .set_secret_versioned("provider-key", "wrong-fake-key")
        .unwrap();
    let result = invoke();
    assert!(!result.status.success());
    let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["connectivity"]["status"], "passed");
    assert_eq!(report["authentication"]["status"], "failed");
    assert!(!String::from_utf8_lossy(&result.stdout).contains("wrong-fake-key"));
    core.shutdown().await.unwrap();
    stop.send(()).unwrap();
    provider.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_test_reports_local_tls_failure_through_cli_and_core_without_leaking_credentials()
{
    let fixture = ManagementFixture::new().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        if let Ok((connection, _)) = listener.accept().await {
            drop(connection);
        }
    });
    let store = Store::open_with_secret_backend(
        fixture.path("provider"),
        SecretBackendKind::Native,
        Arc::new(Credentials::default()),
    )
    .unwrap();
    store
        .create_provider_profile(kakune_core::ProviderProfileUpsert {
            id: "minimax".into(),
            display_name: "MiniMax".into(),
            provider_type: kakune_core::ProviderType::MiniMax,
            default_model: "MiniMax-M3".into(),
            allowed_models: vec!["MiniMax-M3".into()],
            capabilities: vec!["text-generation".into()],
            auth: kakune_core::ProviderAuth::ApiKeySecret {
                secret_ref: "provider-key".into(),
            },
            config: serde_json::json!({"baseUrl": format!("https://{address}")}),
        })
        .unwrap();
    const CANARY: &str = "fake-provider-private-secret";
    store.set_secret_versioned("provider-key", CANARY).unwrap();
    let mut core = InProcessCore::start_with_store(store).await.unwrap();
    let contexts = fixture.path("provider-contexts.json");
    fs::write(&contexts, serde_json::to_vec(&serde_json::json!({
        "format":"kakune-contexts/v1", "exportedAt":"2026-10-26T00:00:00Z", "activeContextId":"selected", "revision":0,
        "contexts":[{"id":"selected", "name":"Selected", "endpoint":core.endpoint,
            "expectedCoreId":core.store.core_id().unwrap(), "color":null, "credentialRef":null}]
    })).unwrap()).unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .env("KAKUNE_DATA_DIR", fixture.path("unused"))
        .env("KAKUNE_TOKEN", &core.token)
        .arg("--context-file")
        .arg(contexts)
        .args(["provider", "test", "minimax", "--json"])
        .output()
        .unwrap();
    assert!(!result.status.success());
    let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["connectivity"]["status"], "failed");
    assert_eq!(report["authentication"]["status"], "unknown");
    assert!(!String::from_utf8_lossy(&result.stdout).contains(CANARY));
    assert!(!String::from_utf8_lossy(&result.stderr).contains(CANARY));
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    core.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_test_exits_unsuccessfully_when_expired_codex_credentials_are_unverified() {
    let fixture = ManagementFixture::new().unwrap();
    let store = Store::open_with_secret_backend(
        fixture.path("codex"),
        SecretBackendKind::Native,
        Arc::new(Credentials::default()),
    )
    .unwrap();
    store
        .create_provider_profile(kakune_core::ProviderProfileUpsert {
            id: "codex".into(),
            display_name: "Codex".into(),
            provider_type: kakune_core::ProviderType::Codex,
            default_model: "codex-default".into(),
            allowed_models: vec!["codex-default".into()],
            capabilities: vec!["text-generation".into()],
            auth: kakune_core::ProviderAuth::OAuthSecret {
                secret_ref: "codex-key".into(),
            },
            config: serde_json::json!({}),
        })
        .unwrap();
    store
        .set_secret_versioned(
            "codex-key",
            &serde_json::json!({
                "access":"fake-private-access", "refresh":"fake-private-refresh", "expiresAtMs":0
            })
            .to_string(),
        )
        .unwrap();
    let mut core = InProcessCore::start_with_store(store).await.unwrap();
    let contexts = fixture.path("codex-contexts.json");
    fs::write(&contexts, serde_json::to_vec(&serde_json::json!({
        "format":"kakune-contexts/v1", "exportedAt":"2026-10-26T00:00:00Z", "activeContextId":"selected", "revision":0,
        "contexts":[{"id":"selected", "name":"Selected", "endpoint":core.endpoint,
            "expectedCoreId":core.store.core_id().unwrap(), "color":null, "credentialRef":null}]
    })).unwrap()).unwrap();
    let result = Command::new(env!("CARGO_BIN_EXE_kakune"))
        .env("KAKUNE_DATA_DIR", fixture.path("unused"))
        .env("KAKUNE_TOKEN", &core.token)
        .arg("--context-file")
        .arg(contexts)
        .args(["provider", "test", "codex", "--json"])
        .output()
        .unwrap();
    assert!(!result.status.success());
    let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(report["authentication"]["status"], "unknown");
    assert!(!String::from_utf8_lossy(&result.stdout).contains("fake-private-access"));
    assert!(!String::from_utf8_lossy(&result.stderr).contains("fake-private-refresh"));
    core.shutdown().await.unwrap();
}

impl NativeCredentialBackend for Credentials {
    fn write(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.0
            .lock()
            .unwrap()
            .insert(key.to_owned(), value.to_vec());
        Ok(())
    }
    fn read(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }
    fn delete(&self, key: &str) -> Result<(), String> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
}

#[tokio::test]
async fn secret_lifecycle_is_authorized_redacted_and_uses_injected_credential_storage() {
    let fixture = ManagementFixture::new().unwrap();
    let store = Store::open_with_secret_backend(
        fixture.path("secrets"),
        SecretBackendKind::Native,
        Arc::new(Credentials::default()),
    )
    .unwrap();
    let mut core = InProcessCore::start_with_store(store.clone())
        .await
        .unwrap();
    let read = store
        .create_auth_token("reader".into(), vec![AuthScope::Read], None)
        .unwrap();
    let client = Client::new();
    let url = format!("{}/api/v1/manage/secrets/fixture-key", core.endpoint);
    const CANARY: &str = "fake-private-secret-canary";
    let denied = client
        .put(&url)
        .bearer_auth(&read.token)
        .json(&serde_json::json!({"value": CANARY}))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::FORBIDDEN);
    assert!(!denied.text().await.unwrap().contains(CANARY));
    assert!(store.resolve_secret("fixture-key").is_err());
    let created = client
        .put(&url)
        .bearer_auth(&core.token)
        .json(&serde_json::json!({"value": CANARY}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    assert!(!created.text().await.unwrap().contains(CANARY));
    assert_eq!(store.resolve_secret("fixture-key").unwrap(), CANARY);
    let listed = client
        .get(format!("{}/api/v1/manage/secrets", core.endpoint))
        .bearer_auth(&core.token)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.status(), StatusCode::OK);
    let body = listed.text().await.unwrap();
    assert!(body.contains("fixture-key"));
    assert!(!body.contains(CANARY));
    let removed = client
        .delete(&url)
        .bearer_auth(&core.token)
        .send()
        .await
        .unwrap();
    assert!(removed.status().is_success());
    assert!(!removed.text().await.unwrap().contains(CANARY));
    assert!(store.resolve_secret("fixture-key").is_err());
    core.shutdown().await.unwrap();
}

#[tokio::test]
async fn execution_trigger_provider_mcp_and_secret_observations_require_authentication() {
    let fixture = ManagementFixture::new().unwrap();
    let mut core = InProcessCore::start(fixture.path("core")).await.unwrap();
    let client = Client::new();
    for path in ["/executions", "/triggers", "/providers", "/mcp", "/secrets"] {
        let response = client
            .get(format!("{}/api/v1/manage{path}", core.endpoint))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
        let authorized = client
            .get(format!("{}/api/v1/manage{path}", core.endpoint))
            .bearer_auth(&core.token)
            .send()
            .await
            .unwrap();
        assert_eq!(authorized.status(), StatusCode::OK, "{path}");
    }
    core.shutdown().await.unwrap();
}
