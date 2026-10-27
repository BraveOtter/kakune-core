mod support;

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{Json, Router, http::StatusCode, routing::get};
use kakune_core::{
    AuthScope, ConnectionContext, NativeCredentialBackend, SecretBackendKind, Store,
    remote::ManagementClient,
};
use reqwest::{Method, header::AUTHORIZATION};
use serde_json::{Value, json};
use support::management::{
    FailureInjector, FakeClock, InProcessCore, ManagementFixture, MonotonicClock,
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

async fn management_core() -> (ManagementFixture, InProcessCore) {
    let fixture = ManagementFixture::new().expect("fixture root should be created");
    let core = InProcessCore::start(fixture.path("core"))
        .await
        .expect("isolated Core should start");
    (fixture, core)
}

#[derive(Default)]
struct ApiCredentialBackend(Mutex<HashMap<String, Vec<u8>>>);

impl NativeCredentialBackend for ApiCredentialBackend {
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

async fn management_request(
    client: &reqwest::Client,
    core: &InProcessCore,
    method: Method,
    path: &str,
    token: &str,
    body: Option<Value>,
) -> reqwest::Response {
    let mut request = client
        .request(method, format!("{}/api/v1/manage{path}", core.endpoint))
        .header(AUTHORIZATION, format!("Bearer {token}"));
    if let Some(body) = body {
        request = request.json(&body);
    }
    request.send().await.expect("request should complete")
}

#[tokio::test]
async fn management_capability_is_negotiated_and_errors_use_sanitized_envelopes() {
    let (_fixture, mut core) = management_core().await;
    let client = reqwest::Client::new();
    let info: Value = client
        .get(format!("{}/api/v1/info", core.endpoint))
        .header(AUTHORIZATION, format!("Bearer {}", core.token))
        .send()
        .await
        .expect("info request should complete")
        .error_for_status()
        .expect("info request should succeed")
        .json()
        .await
        .expect("info response should be JSON");
    assert!(
        info["capabilities"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item == "management-api")),
        "Core info must advertise management API capability"
    );

    let response = management_request(
        &client,
        &core,
        Method::GET,
        "/not-a-route",
        &core.token,
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body: Value = response
        .json()
        .await
        .expect("error envelope should be JSON");
    assert_eq!(body["code"], "not_found");
    assert!(body["message"].is_string());
    assert!(body["guidance"].is_string());
    assert!(body.to_string().find("Bearer ").is_none());

    let unauthorized = client
        .get(format!("{}/api/v1/manage", core.endpoint))
        .send()
        .await
        .expect("unauthenticated management request should complete");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    let error: Value = unauthorized
        .json()
        .await
        .expect("unauthorized response should be a JSON envelope");
    assert_eq!(error["code"], "unauthenticated");
    assert!(error["guidance"].is_string());

    core.shutdown().await.expect("Core should stop cleanly");
}

#[tokio::test]
async fn management_scopes_are_selected_by_operation_not_http_verb_alone() {
    let (_fixture, mut core) = management_core().await;
    let read = core
        .store
        .create_auth_token("read-only".into(), vec![AuthScope::Read], None)
        .expect("read token should be created");
    let manage = core
        .store
        .create_auth_token("management".into(), vec![AuthScope::Manage], None)
        .expect("manage token should be created");
    let run = core
        .store
        .create_auth_token("runner".into(), vec![AuthScope::Run], None)
        .expect("run token should be created");
    let client = reqwest::Client::new();

    assert_eq!(
        management_request(&client, &core, Method::GET, "", &read.token, None)
            .await
            .status(),
        StatusCode::OK
    );
    let import = Some(json!({ "source": "workflow-source" }));
    assert_eq!(
        management_request(
            &client,
            &core,
            Method::POST,
            "/workflows/import",
            &read.token,
            import.clone(),
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        management_request(
            &client,
            &core,
            Method::POST,
            "/workflows/import",
            &manage.token,
            import,
        )
        .await
        .status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "Manage scope reaches workflow validation; this source is not a workflow"
    );
    assert_eq!(
        management_request(
            &client,
            &core,
            Method::POST,
            "/executions/e-1/cancel",
            &read.token,
            Some(json!({ "waitSeconds": 0 })),
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_ne!(
        management_request(
            &client,
            &core,
            Method::POST,
            "/executions/e-1/cancel",
            &run.token,
            Some(json!({ "waitSeconds": 0 })),
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        management_request(&client, &core, Method::GET, "/secrets", &read.token, None)
            .await
            .status(),
        StatusCode::FORBIDDEN,
        "secret metadata is Manage-scoped even for GET"
    );

    core.shutdown().await.expect("Core should stop cleanly");
}

#[tokio::test]
async fn import_has_independent_encoded_source_and_envelope_limits() {
    let (_fixture, mut core) = management_core().await;
    let client = reqwest::Client::new();
    const SOURCE_LIMIT: usize = 1_048_576;
    const ENVELOPE_LIMIT: usize = 65_536;

    // NULs are one source byte each but expand to six bytes in JSON. This
    // proves the source cap is measured after JSON decoding, independently of
    // the encoded request cap.
    let source = format!("{}é", "\0".repeat(SOURCE_LIMIT - 2));
    assert_eq!(source.len(), SOURCE_LIMIT);
    let accepted = management_request(
        &client,
        &core,
        Method::POST,
        "/workflows/import",
        &core.token,
        Some(json!({ "source": source })),
    )
    .await;
    assert_eq!(
        accepted.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "the exact source limit should pass the transport guard and reach validation"
    );

    let overflow = management_request(
        &client,
        &core,
        Method::POST,
        "/workflows/import",
        &core.token,
        Some(json!({ "source": "x".repeat(SOURCE_LIMIT + 1) })),
    )
    .await;
    assert_eq!(overflow.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let exact_envelope = envelope_with_encoded_non_source_len(ENVELOPE_LIMIT);
    assert_eq!(encoded_non_source_len(&exact_envelope), ENVELOPE_LIMIT);
    assert_eq!(
        management_request(
            &client,
            &core,
            Method::POST,
            "/workflows/import",
            &core.token,
            Some(exact_envelope),
        )
        .await
        .status(),
        StatusCode::BAD_REQUEST,
        "the exact non-source envelope limit should pass transport and reach DTO validation"
    );
    let overflowing_envelope = envelope_with_encoded_non_source_len(ENVELOPE_LIMIT + 1);
    assert_eq!(
        management_request(
            &client,
            &core,
            Method::POST,
            "/workflows/import",
            &core.token,
            Some(overflowing_envelope),
        )
        .await
        .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );

    // Non-import routes keep the ordinary 1 MiB encoded request ceiling.
    let exact_ordinary = client
        .post(format!("{}/api/v1/manage/not-a-route", core.endpoint))
        .header(AUTHORIZATION, format!("Bearer {}", core.token))
        .body("x".repeat(SOURCE_LIMIT))
        .send()
        .await
        .expect("exact ordinary request should complete");
    assert_eq!(exact_ordinary.status(), StatusCode::NOT_FOUND);
    let ordinary = client
        .post(format!("{}/api/v1/manage/not-a-route", core.endpoint))
        .header(AUTHORIZATION, format!("Bearer {}", core.token))
        .body("x".repeat(SOURCE_LIMIT + 1))
        .send()
        .await
        .expect("oversized ordinary request should complete with 413");
    assert_eq!(ordinary.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(
        core.store
            .list_workflows()
            .expect("workflow state should remain readable")
            .is_empty(),
        "rejected import requests must not mutate registrations"
    );

    core.shutdown().await.expect("Core should stop cleanly");
}

fn encoded_non_source_len(value: &Value) -> usize {
    let mut non_source = value.clone();
    non_source
        .as_object_mut()
        .expect("import envelope should be an object")
        .remove("source");
    serde_json::to_vec(&non_source)
        .expect("envelope should serialize")
        .len()
}

fn envelope_with_encoded_non_source_len(target: usize) -> Value {
    let mut low = 0;
    let mut high = target;
    while low < high {
        let middle = (low + high) / 2;
        let value = json!({ "source": "x", "metadata": "m".repeat(middle) });
        if encoded_non_source_len(&value) < target {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    json!({ "source": "x", "metadata": "m".repeat(low) })
}

#[tokio::test]
async fn remote_management_transport_rejects_redirects_and_incompatible_identity() {
    let redirected = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/api/v1/info",
        get({
            let redirected = Arc::clone(&redirected);
            move || {
                let redirected = Arc::clone(&redirected);
                async move {
                    redirected.fetch_add(1, Ordering::SeqCst);
                    (StatusCode::TEMPORARY_REDIRECT, [("location", "/target")])
                }
            }
        }),
    );
    let (endpoint, stop, task) = serve_ephemeral(app).await;
    let selected = context(endpoint.clone(), None);
    assert!(
        ManagementClient::connect(&selected, "test-token".into(), None)
            .await
            .is_err(),
        "the transport must not follow redirects during capability negotiation"
    );
    assert_eq!(redirected.load(Ordering::SeqCst), 1);
    stop.send(()).expect("server should stop");
    task.await.expect("server task should join");

    let selected = context("http://example.invalid".into(), None);
    assert!(
        ManagementClient::connect(&selected, "test-token".into(), None)
            .await
            .is_err(),
        "non-loopback HTTP must fail before any network access"
    );
}

async fn serve_ephemeral(app: Router) -> (String, oneshot::Sender<()>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should bind");
    let address: SocketAddr = listener
        .local_addr()
        .expect("listener address should exist");
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .expect("test server should stop cleanly");
    });
    (format!("http://{address}"), stop, task)
}

fn context(endpoint: String, expected_core_id: Option<&str>) -> ConnectionContext {
    ConnectionContext {
        id: "selected".into(),
        name: "Selected Core".into(),
        endpoint,
        expected_core_id: expected_core_id.map(str::to_owned),
        color: None,
        credential_ref: None,
    }
}

#[tokio::test]
async fn expected_core_identity_is_checked_before_management_use() {
    let fixture = ManagementFixture::new().expect("fixture root should be created");
    let core = InProcessCore::start(fixture.path("identity-core"))
        .await
        .expect("isolated Core should start");
    let selected = context(core.endpoint.clone(), Some("another-core"));
    assert!(
        ManagementClient::connect(&selected, core.token.clone(), None)
            .await
            .is_err(),
        "expected identity mismatch must be rejected"
    );
    let mut core = core;
    core.shutdown().await.expect("Core should stop cleanly");
}

#[tokio::test]
async fn a_failed_mutating_remote_request_is_not_retried() {
    let calls = Arc::new(AtomicUsize::new(0));
    let failures = FailureInjector::default();
    failures.fail_on_call("workflow.import", 1, "injected response loss");
    let clock = FakeClock::default();
    let app = Router::new().route(
        "/api/v1/info",
        get(|| async {
            Json(json!({
                "apiVersion": kakune_core::API_VERSION,
                "coreId": "transport-test-core",
                "capabilities": ["management-api"]
            }))
        }),
    );
    let app = app.route(
        "/api/v1/manage/workflows/import",
        axum::routing::post({
            let calls = Arc::clone(&calls);
            let failures = failures.clone();
            let clock = clock.clone();
            move || {
                let calls = Arc::clone(&calls);
                let failures = failures.clone();
                let clock = clock.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    clock.advance(Duration::from_secs(7));
                    if failures.check("workflow.import").is_err() {
                        StatusCode::SERVICE_UNAVAILABLE
                    } else {
                        StatusCode::OK
                    }
                }
            }
        }),
    );
    let (endpoint, stop, task) = serve_ephemeral(app).await;
    let selected = context(endpoint, None);
    let client = ManagementClient::connect(&selected, "test-token".into(), None)
        .await
        .expect("Core handshake should succeed");
    assert!(
        client
            .request(
                Method::POST,
                &["workflows", "import"],
                Some(json!({ "source": "small" })),
                None,
            )
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(failures.calls("workflow.import"), 1);
    assert_eq!(clock.now(), Duration::from_secs(7));
    stop.send(()).expect("server should stop");
    task.await.expect("server task should join");
}

#[tokio::test]
async fn execution_cursors_bind_filters_and_log_pages_keep_their_high_water_mark() {
    let (_fixture, mut core) = management_core().await;
    let read = core
        .store
        .create_auth_token("cursor-reader".into(), vec![AuthScope::Read], None)
        .expect("read token should be created");
    let client = reqwest::Client::new();

    let first = core
        .store
        .create_execution("cursor-workflow")
        .expect("first execution should be created");
    let _second = core
        .store
        .create_execution("cursor-workflow")
        .expect("second execution should be created");
    let executions = management_request(
        &client,
        &core,
        Method::GET,
        "/executions?limit=1",
        &read.token,
        None,
    )
    .await;
    assert_eq!(executions.status(), StatusCode::OK);
    let executions: Value = executions.json().await.unwrap();
    let execution_cursor = executions["nextCursor"]
        .as_str()
        .expect("first execution page should continue");
    let filtered = management_request(
        &client,
        &core,
        Method::GET,
        &format!("/executions?limit=1&status=running&cursor={execution_cursor}"),
        &read.token,
        None,
    )
    .await;
    assert_eq!(
        filtered.status(),
        StatusCode::BAD_REQUEST,
        "a cursor cannot be replayed with different filters"
    );
    let tampered = management_request(
        &client,
        &core,
        Method::GET,
        "/executions?limit=1&cursor=not-a-valid-cursor",
        &read.token,
        None,
    )
    .await;
    assert_eq!(tampered.status(), StatusCode::BAD_REQUEST);

    core.store
        .finish_execution(&first.id, "succeeded", None)
        .expect("execution should finish and append a second event");
    let first_log_page = management_request(
        &client,
        &core,
        Method::GET,
        &format!("/executions/{}/logs?limit=1", first.id),
        &read.token,
        None,
    )
    .await;
    assert_eq!(first_log_page.status(), StatusCode::OK);
    let first_log_page: Value = first_log_page.json().await.unwrap();
    let high_water = first_log_page["highWater"]
        .as_u64()
        .expect("log snapshot should expose its high-water sequence");
    let log_cursor = first_log_page["nextCursor"]
        .as_str()
        .expect("first event page should continue");

    core.store
        .record_node_outcome(
            &first.id,
            "after-snapshot",
            "succeeded",
            None,
            Some(&json!({"value":"late event"})),
            None,
        )
        .expect("a later event should be appended after the snapshot");
    let second_log_page = management_request(
        &client,
        &core,
        Method::GET,
        &format!("/executions/{}/logs?limit=10&cursor={log_cursor}", first.id),
        &read.token,
        None,
    )
    .await;
    assert_eq!(second_log_page.status(), StatusCode::OK);
    let second_log_page: Value = second_log_page.json().await.unwrap();
    assert_eq!(second_log_page["highWater"], high_water);
    assert_eq!(second_log_page["items"].as_array().unwrap().len(), 1);
    assert_ne!(
        second_log_page["items"][0]["type"], "execution.node.outcome",
        "events appended after the initial page are outside its snapshot"
    );

    core.shutdown().await.expect("Core should stop cleanly");
}

#[tokio::test]
async fn canonical_client_requires_advertised_capability_and_sanitizes_upstream_errors() {
    let info_calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/api/v1/info",
        get({
            let info_calls = Arc::clone(&info_calls);
            move || {
                let info_calls = Arc::clone(&info_calls);
                async move {
                    info_calls.fetch_add(1, Ordering::SeqCst);
                    Json(json!({
                        "apiVersion": kakune_core::API_VERSION,
                        "coreId": "older-core",
                        "capabilities": []
                    }))
                }
            }
        }),
    );
    let (endpoint, stop, task) = serve_ephemeral(app).await;
    let selected = context(endpoint, None);
    let error = ManagementClient::connect(&selected, "test-token".into(), None)
        .await
        .err()
        .expect("older Core should not negotiate management capability");
    assert!(error.contains("upgrade"));
    assert_eq!(info_calls.load(Ordering::SeqCst), 1);
    stop.send(()).expect("server should stop");
    task.await.expect("server task should join");

    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/api/v1/info",
        get(|| async {
            Json(json!({
                "apiVersion": kakune_core::API_VERSION,
                "coreId": "current-core",
                "capabilities": ["management-api"]
            }))
        }),
    );
    let app = app.route(
        "/api/v1/manage/probe",
        axum::routing::post({
            let calls = Arc::clone(&calls);
            move || {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    (
                        StatusCode::BAD_GATEWAY,
                        Json(json!({"message":"Bearer private-proof"})),
                    )
                }
            }
        }),
    );
    let (endpoint, stop, task) = serve_ephemeral(app).await;
    let client = ManagementClient::connect(&context(endpoint, None), "test-token".into(), None)
        .await
        .expect("management capability should negotiate");
    let error = client
        .request(Method::POST, &["probe"], Some(json!({"active":true})), None)
        .await
        .expect_err("upstream failure should be reported");
    assert!(error.contains("502"));
    assert!(!error.contains("private-proof"));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "mutations are never retried"
    );
    stop.send(()).expect("server should stop");
    task.await.expect("server task should join");
}

#[tokio::test]
async fn canonical_client_caps_responses_and_rejects_oversized_requests_before_sending() {
    let probe_calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/api/v1/info",
        get(|| async {
            Json(json!({
                "apiVersion": kakune_core::API_VERSION,
                "coreId": "response-cap-core",
                "capabilities": ["management-api"]
            }))
        }),
    );
    let app = app.route(
        "/api/v1/manage/large",
        get(|| async move {
            (
                StatusCode::OK,
                axum::body::Body::from(vec![b'x'; 16_777_217]),
            )
        }),
    );
    let app = app.route(
        "/api/v1/manage/probe",
        axum::routing::post({
            let probe_calls = Arc::clone(&probe_calls);
            move || {
                let probe_calls = Arc::clone(&probe_calls);
                async move {
                    probe_calls.fetch_add(1, Ordering::SeqCst);
                    StatusCode::OK
                }
            }
        }),
    );
    let (endpoint, stop, task) = serve_ephemeral(app).await;
    let client = ManagementClient::connect(&context(endpoint, None), "test-token".into(), None)
        .await
        .expect("management capability should negotiate");
    let error = client
        .request(Method::GET, &["large"], None, None)
        .await
        .expect_err("oversized response should fail");
    assert!(error.contains("16 MiB"));
    let error = client
        .request(
            Method::POST,
            &["probe"],
            Some(json!({ "payload": "x".repeat(1_048_576) })),
            None,
        )
        .await
        .expect_err("oversized encoded management body should fail locally");
    assert!(error.contains("encoded route limit"));
    assert_eq!(probe_calls.load(Ordering::SeqCst), 0);
    stop.send(()).expect("server should stop");
    task.await.expect("server task should join");
}

#[tokio::test]
async fn workflow_routes_enforce_scopes_revisions_idempotency_and_non_executing_checks() {
    let (_fixture, mut core) = management_core().await;
    let read = core
        .store
        .create_auth_token("workflow-reader".into(), vec![AuthScope::Read], None)
        .expect("read token should be created");
    let manage = core
        .store
        .create_auth_token("workflow-manager".into(), vec![AuthScope::Manage], None)
        .expect("management token should be created");
    let run = core
        .store
        .create_auth_token("workflow-runner".into(), vec![AuthScope::Run], None)
        .expect("run token should be created");
    let client = reqwest::Client::new();
    let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: api-workflow, name: API Workflow}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: start\nnodes: [{id: start, type: kakune.flow.pass@1}]\n";

    assert_eq!(
        management_request(
            &client,
            &core,
            Method::POST,
            "/workflows/import",
            &read.token,
            Some(json!({"source":source})),
        )
        .await
        .status(),
        StatusCode::FORBIDDEN,
        "imports require Manage scope"
    );
    let imported = management_request(
        &client,
        &core,
        Method::POST,
        "/workflows/import",
        &manage.token,
        Some(json!({"source":source})),
    )
    .await;
    assert_eq!(imported.status(), StatusCode::CREATED);
    let registration: Value = imported.json().await.expect("registration should be JSON");
    assert_eq!(registration["status"], "disabled");
    let id = registration["id"].as_str().expect("registration ID");
    let revision = registration["revision"]
        .as_str()
        .expect("workflow revision");

    let stale_replacement = management_request(
        &client,
        &core,
        Method::POST,
        "/workflows/import",
        &manage.token,
        Some(json!({
            "source": source.replace("API Workflow", "Stale Replacement"),
            "replace": true,
            "expectedRevision": "stale-revision"
        })),
    )
    .await;
    assert_eq!(
        stale_replacement.status(),
        StatusCode::PRECONDITION_FAILED,
        "stale source replacement must not overwrite the observed revision"
    );
    assert_eq!(
        core.store
            .get_workflow_source(id)
            .expect("current source should remain readable")
            .expect("registration should remain present")
            .source,
        source
    );

    let resolve = management_request(
        &client,
        &core,
        Method::POST,
        "/workflows/resolve",
        &read.token,
        Some(json!({"selector":"name:API Workflow"})),
    )
    .await;
    assert_eq!(resolve.status(), StatusCode::OK);
    assert_eq!(resolve.json::<Value>().await.unwrap()["id"], id);

    let before_checks = core
        .store
        .list_executions()
        .expect("executions should list")
        .len();
    for operation in ["validate", "plan"] {
        let response = management_request(
            &client,
            &core,
            Method::POST,
            &format!("/workflows/{id}/{operation}"),
            &read.token,
            None,
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{operation} is Read-scoped"
        );
        let payload: Value = response.json().await.expect("check result should be JSON");
        assert!(payload.is_object());
        match operation {
            "validate" => assert!(payload.get("workflow").is_some()),
            "plan" => {
                assert!(payload.get("scopes").is_some());
                assert!(payload.get("bounds").is_some());
            }
            _ => unreachable!(),
        }
    }
    assert_eq!(
        core.store
            .list_executions()
            .expect("executions should list")
            .len(),
        before_checks,
        "validation and planning must not execute nodes or create executions"
    );

    let eligibility_path = format!("/workflows/{id}/eligibility");
    let read_update = management_request(
        &client,
        &core,
        Method::PUT,
        &eligibility_path,
        &read.token,
        Some(json!({"enabled":true})),
    )
    .await;
    assert_eq!(read_update.status(), StatusCode::FORBIDDEN);
    let stale = client
        .put(format!("{}/api/v1/manage{eligibility_path}", core.endpoint))
        .header(AUTHORIZATION, format!("Bearer {}", manage.token))
        .header("If-Match", "stale-revision")
        .json(&json!({"enabled":true}))
        .send()
        .await
        .expect("stale conditional update should complete");
    assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(
        core.store
            .list_workflows()
            .expect("workflow should still list")[0]
            .status,
        "disabled",
        "stale eligibility changes must not mutate state"
    );
    let changed = client
        .put(format!("{}/api/v1/manage{eligibility_path}", core.endpoint))
        .header(AUTHORIZATION, format!("Bearer {}", manage.token))
        .header("If-Match", revision)
        .json(&json!({"enabled":true}))
        .send()
        .await
        .expect("matching conditional update should complete");
    assert_eq!(changed.status(), StatusCode::OK);

    let executions_path = format!("/workflows/{id}/executions");
    assert_eq!(
        management_request(
            &client,
            &core,
            Method::POST,
            &executions_path,
            &read.token,
            Some(json!({"requestKey":"workflow-idempotency"})),
        )
        .await
        .status(),
        StatusCode::FORBIDDEN,
        "manual submission requires Run scope"
    );
    let submit = |request: Value| {
        client
            .post(format!("{}/api/v1/manage{executions_path}", core.endpoint))
            .header(AUTHORIZATION, format!("Bearer {}", run.token))
            .json(&request)
            .send()
    };
    let submitted = submit(json!({"requestKey":"workflow-idempotency"}))
        .await
        .expect("run-scoped submission should complete");
    assert_eq!(submitted.status(), StatusCode::ACCEPTED);
    let submitted: Value = submitted.json().await.expect("submission should be JSON");
    let execution_id = submitted["executionId"]
        .as_str()
        .expect("submission returns one execution ID");
    let repeated = submit(json!({"requestKey":"workflow-idempotency"}))
        .await
        .expect("same idempotency request should complete");
    let repeated: Value = repeated.json().await.expect("repeat should be JSON");
    assert_eq!(repeated["executionId"], execution_id);
    let changed_request = submit(json!({
        "requestKey":"workflow-idempotency",
        "input":{"different":true}
    }))
    .await
    .expect("changed idempotency request should complete");
    assert_eq!(changed_request.status(), StatusCode::CONFLICT);

    core.shutdown().await.expect("Core should stop cleanly");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execution_and_trigger_routes_enforce_scopes_limits_encoding_and_redaction() {
    let (_fixture, mut core) = management_core().await;
    let read = core
        .store
        .create_auth_token("execution-reader".into(), vec![AuthScope::Read], None)
        .expect("read token should be created");
    let run = core
        .store
        .create_auth_token("execution-runner".into(), vec![AuthScope::Run], None)
        .expect("run token should be created");
    let manage = core
        .store
        .create_auth_token("execution-manager".into(), vec![AuthScope::Manage], None)
        .expect("manage token should be created");

    let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: encoded-workflow, name: Encoded Workflow}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: start\nnodes: [{id: start, type: kakune.flow.pass@1}]\n";
    let workflow = kakune_core::WorkflowDocument::parse(source).unwrap();
    core.store
        .create_workflow(&workflow, source, "disabled")
        .unwrap();
    let active = core
        .store
        .create_execution("encoded-workflow")
        .expect("fixture execution should be created");
    core.store
        .set_execution_result(
            &active.id,
            &json!({"authorization":"Bearer private-proof", "nested":{"apiKey":"private-proof"}}),
        )
        .expect("canary result should be recorded");
    for node_id in ["first", "second", "third"] {
        core.store
            .record_node_outcome(
                &active.id,
                node_id,
                "succeeded",
                Some("private-proof in an unsafe historical message"),
                Some(&json!({"token":"private-proof", "value":node_id})),
                Some("Bearer private-proof"),
            )
            .expect("canary node outcome should be recorded");
    }
    let span = core
        .store
        .start_trace_span(
            &active.id,
            None,
            ("node", "private-proof span"),
            None,
            None,
            &json!({"accessToken":"private-proof"}),
        )
        .expect("canary trace should be recorded");
    core.store
        .finish_trace_span(&span, "succeeded", Some("private-proof trace error"))
        .expect("trace span should finish");
    core.store
        .finish_execution(&active.id, "failed", Some("Bearer private-proof"))
        .expect("fixture execution should finish with an error");
    let cancellable = core
        .store
        .create_execution("encoded-workflow")
        .expect("cancellation fixture execution should be created");

    // Simulate a pre-management historical row written before the recording
    // sanitizer existed. Exposure must still fail closed.
    let database = core.store.data_dir().join("kakune.sqlite3");
    let connection = rusqlite::Connection::open(database).expect("fixture database should open");
    let (core_id, sequence): (String, i64) = connection
        .query_row(
            "SELECT core_id, event_sequence + 1 FROM core_state WHERE id = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("current event sequence should be readable");
    connection
        .execute(
            "UPDATE core_state SET event_sequence = ?1 WHERE id = 1",
            [sequence],
        )
        .expect("sequence should be advanced for fixture event");
    connection
        .execute(
            "INSERT INTO events (event_id,event_version,core_id,sequence,timestamp,type,resource_id,execution_id,payload) VALUES (?1,'1.0',?2,?3,'2026-10-26T00:00:00Z','execution.legacy-canary','encoded-workflow',?4,?5)",
            rusqlite::params![
                uuid::Uuid::new_v4().to_string(),
                core_id,
                sequence,
                active.id,
                r#"{"message":"private-proof","result":{"accessToken":"private-proof"},"error":"Bearer private-proof"}"#,
            ],
        )
        .expect("legacy event fixture should be inserted");
    drop(connection);
    let connection = rusqlite::Connection::open(core.store.data_dir().join("kakune.sqlite3"))
        .expect("fixture database should open for an orphan schedule");
    connection
        .pragma_update(None, "foreign_keys", "OFF")
        .expect("fixture may seed a retained orphan schedule");
    connection
        .execute(
            "INSERT INTO scheduled_triggers (workflow_id, trigger_id, next_run_at, updated_at) VALUES ('orphan/flow', 'cron/a%', '2026-10-26T01:00:00Z', '2026-10-26T00:00:00Z')",
            [],
        )
        .expect("legacy orphan schedule should be inserted");
    drop(connection);

    let client = reqwest::Client::new();
    for (path, method, expected) in [
        ("/executions", Method::GET, StatusCode::OK),
        (
            &format!("/executions/{}", active.id),
            Method::GET,
            StatusCode::OK,
        ),
        (
            &format!("/executions/{}/logs", active.id),
            Method::GET,
            StatusCode::OK,
        ),
        ("/triggers", Method::GET, StatusCode::OK),
    ] {
        assert_eq!(
            management_request(&client, &core, method, path, &read.token, None)
                .await
                .status(),
            expected,
            "Read scope should observe {path}"
        );
    }
    assert_eq!(
        management_request(
            &client,
            &core,
            Method::GET,
            "/executions",
            &manage.token,
            None
        )
        .await
        .status(),
        StatusCode::FORBIDDEN,
        "Manage alone is not Read"
    );
    assert_eq!(
        management_request(
            &client,
            &core,
            Method::POST,
            &format!("/executions/{}/cancel", cancellable.id),
            &read.token,
            Some(json!({"waitSeconds":0})),
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );

    let pending = management_request(
        &client,
        &core,
        Method::POST,
        &format!("/executions/{}/cancel", cancellable.id),
        &run.token,
        Some(json!({"waitSeconds":1})),
    )
    .await;
    assert_eq!(pending.status(), StatusCode::OK);
    let pending: Value = pending.json().await.unwrap();
    assert_eq!(pending["pending"], true);
    assert_eq!(pending["observedStatus"], "cancelling");
    let unknown = management_request(
        &client,
        &core,
        Method::POST,
        "/executions/unknown-id/cancel",
        &run.token,
        Some(json!({"waitSeconds":0})),
    )
    .await;
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);

    let detail = client
        .get(format!(
            "{}/api/v1/manage/executions/{}?limit=1",
            core.endpoint, active.id
        ))
        .bearer_auth(&read.token)
        .send()
        .await
        .expect("bounded execution detail should respond");
    assert_eq!(detail.status(), StatusCode::OK);
    let detail: Value = detail.json().await.unwrap();
    assert_eq!(detail["nodes"]["items"].as_array().unwrap().len(), 1);
    let node_cursor = detail["nodes"]["nextCursor"]
        .as_str()
        .expect("node section should expose one-page continuation");
    assert!(!detail.to_string().contains("private-proof"));
    assert_eq!(detail["result"]["availability"], "unavailable");
    assert_eq!(detail["trace"]["availability"], "unavailable");

    let next_detail = client
        .get(format!(
            "{}/api/v1/manage/executions/{}?limit=1&cursor={node_cursor}",
            core.endpoint, active.id
        ))
        .bearer_auth(&read.token)
        .send()
        .await
        .expect("node section continuation should respond");
    assert_eq!(next_detail.status(), StatusCode::OK);
    let next_detail: Value = next_detail.json().await.unwrap();
    assert_eq!(next_detail["nodes"]["items"].as_array().unwrap().len(), 1);
    assert_ne!(
        detail["nodes"]["items"][0]["nodeId"],
        next_detail["nodes"]["items"][0]["nodeId"]
    );

    let logs = management_request(
        &client,
        &core,
        Method::GET,
        &format!("/executions/{}/logs", active.id),
        &read.token,
        None,
    )
    .await;
    assert_eq!(logs.status(), StatusCode::OK);
    let logs: Value = logs.json().await.unwrap();
    assert!(!logs.to_string().contains("private-proof"));
    assert!(logs["items"].as_array().unwrap().iter().all(|event| {
        event["payload"].get("result").is_none()
            && event["payload"].get("error").is_none()
            && event["payload"].get("message").is_none()
    }));

    let encoded_trigger = client
        .get(format!(
            "{}/api/v1/manage/triggers/encoded-workflow/manual",
            core.endpoint
        ))
        .bearer_auth(&read.token)
        .send()
        .await
        .expect("encoded trigger path should respond");
    assert_eq!(encoded_trigger.status(), StatusCode::OK);
    let escaped_trigger = client
        .get(format!(
            "{}/api/v1/manage/triggers/orphan%2Fflow/cron%2Fa%25",
            core.endpoint
        ))
        .bearer_auth(&read.token)
        .send()
        .await
        .expect("separately encoded composite trigger path should respond");
    assert_eq!(escaped_trigger.status(), StatusCode::OK);
    let escaped_trigger: Value = escaped_trigger.json().await.unwrap();
    assert_eq!(escaped_trigger["workflowId"], "orphan/flow");
    assert_eq!(escaped_trigger["triggerId"], "cron/a%");

    core.shutdown().await.expect("Core should stop cleanly");
}

#[tokio::test]
async fn integration_management_routes_use_operation_scopes_including_secret_metadata_gets() {
    let (_fixture, mut core) = management_core().await;
    let read = core
        .store
        .create_auth_token("integration-read".into(), vec![AuthScope::Read], None)
        .unwrap();
    let manage = core
        .store
        .create_auth_token("integration-manage".into(), vec![AuthScope::Manage], None)
        .unwrap();
    let client = reqwest::Client::new();

    let profile = json!({
        "id":"provider-scope",
        "displayName":"Provider scope fixture",
        "providerType":"minimax",
        "defaultModel":"MiniMax-M3",
        "allowedModels":["MiniMax-M3"],
        "capabilities":["text-generation"],
        "auth":{"type":"apiKey","secretRef":"missing"},
        "config":{}
    });
    assert_eq!(
        management_request(
            &client,
            &core,
            Method::POST,
            "/providers",
            &read.token,
            Some(profile.clone()),
        )
        .await
        .status(),
        StatusCode::FORBIDDEN,
        "provider create requires Manage"
    );
    assert_eq!(
        management_request(
            &client,
            &core,
            Method::POST,
            "/providers",
            &manage.token,
            Some(profile),
        )
        .await
        .status(),
        StatusCode::CREATED
    );
    for path in ["/providers", "/providers/provider-scope"] {
        assert_eq!(
            management_request(&client, &core, Method::GET, path, &read.token, None)
                .await
                .status(),
            StatusCode::OK,
            "provider metadata {path} is Read-scoped"
        );
        assert_eq!(
            management_request(&client, &core, Method::GET, path, &manage.token, None)
                .await
                .status(),
            StatusCode::FORBIDDEN,
            "Manage alone must not grant Read for {path}"
        );
    }
    for path in [
        "/providers/provider-scope/auth/prepare",
        "/providers/provider-scope/test",
        "/providers/provider-scope/models",
    ] {
        assert_eq!(
            management_request(&client, &core, Method::POST, path, &read.token, None)
                .await
                .status(),
            StatusCode::FORBIDDEN,
            "active provider operation {path} requires Manage"
        );
    }

    let mcp = json!({
        "name":"scope-http",
        "transport":{"type":"http","endpoint":"https://mcp.example.test/mcp","bearerSecretRef":null}
    });
    assert_eq!(
        management_request(
            &client,
            &core,
            Method::POST,
            "/mcp",
            &read.token,
            Some(mcp.clone())
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        management_request(
            &client,
            &core,
            Method::POST,
            "/mcp",
            &manage.token,
            Some(mcp),
        )
        .await
        .status(),
        StatusCode::CREATED
    );
    assert_eq!(
        management_request(&client, &core, Method::GET, "/mcp", &read.token, None)
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        management_request(
            &client,
            &core,
            Method::GET,
            "/mcp/scope-http",
            &read.token,
            None
        )
        .await
        .status(),
        StatusCode::OK
    );
    let no_auth = management_request(
        &client,
        &core,
        Method::POST,
        "/mcp/scope-http/auth",
        &manage.token,
        Some(json!({"secretReference":null})),
    )
    .await;
    assert_eq!(no_auth.status(), StatusCode::OK);
    let no_auth: Value = no_auth.json().await.unwrap();
    assert_eq!(no_auth["status"], "notRequired");

    let missing_reference_connection = management_request(
        &client,
        &core,
        Method::POST,
        "/mcp",
        &manage.token,
        Some(json!({
            "name":"scope-missing-reference",
            "transport":{"type":"http","endpoint":"https://mcp.example.test/mcp","bearerSecretRef":"missing-reference"}
        })),
    )
    .await;
    assert_eq!(missing_reference_connection.status(), StatusCode::CREATED);
    let missing_reference_auth = management_request(
        &client,
        &core,
        Method::POST,
        "/mcp/scope-missing-reference/auth",
        &manage.token,
        Some(json!({"secretReference":null})),
    )
    .await;
    assert_eq!(missing_reference_auth.status(), StatusCode::NOT_FOUND);
    for path in [
        "/mcp/scope-http/auth",
        "/mcp/scope-http/tools/list",
        "/mcp/scope-http/tools/call",
    ] {
        assert_eq!(
            management_request(
                &client,
                &core,
                Method::POST,
                path,
                &read.token,
                Some(json!({"name":"tool","arguments":{}})),
            )
            .await
            .status(),
            StatusCode::FORBIDDEN,
            "MCP active operation {path} requires Manage"
        );
    }
    assert_eq!(
        management_request(&client, &core, Method::GET, "/secrets", &read.token, None)
            .await
            .status(),
        StatusCode::FORBIDDEN,
        "secret metadata is Manage-scoped even for GET"
    );
    assert_eq!(
        management_request(&client, &core, Method::GET, "/secrets", &manage.token, None)
            .await
            .status(),
        StatusCode::OK
    );

    core.shutdown().await.expect("Core should stop cleanly");
}

#[tokio::test]
async fn secret_publication_and_mcp_reference_binding_are_separate_redacted_commits() {
    let fixture = ManagementFixture::new().unwrap();
    let credentials = Arc::new(ApiCredentialBackend::default());
    let store = Store::open_with_secret_backend(
        fixture.path("secret-api"),
        SecretBackendKind::Native,
        credentials,
    )
    .unwrap();
    let mut core = InProcessCore::start_with_store(store.clone())
        .await
        .expect("injected Core should start");
    let manage = core
        .store
        .create_auth_token("secret-manager".into(), vec![AuthScope::Manage], None)
        .unwrap();
    let read = core
        .store
        .create_auth_token("secret-reader".into(), vec![AuthScope::Read], None)
        .unwrap();
    const CANARY: &str = "kakune-secret-canary-do-not-print";
    let client = reqwest::Client::new();

    let secret = client
        .put(format!(
            "{}/api/v1/manage/secrets/new-reference",
            core.endpoint
        ))
        .bearer_auth(&manage.token)
        .json(&json!({"value":CANARY}))
        .send()
        .await
        .unwrap();
    assert_eq!(secret.status(), StatusCode::CREATED);
    let secret_body: Value = secret.json().await.unwrap();
    assert!(!secret_body.to_string().contains(CANARY));
    assert!(!format!("{:?}", kakune_core::management::SecretValue::new(CANARY)).contains(CANARY));

    let added = management_request(
        &client,
        &core,
        Method::POST,
        "/mcp",
        &manage.token,
        Some(json!({
            "name":"reference-binding",
            "transport":{"type":"http","endpoint":"https://mcp.example.test/mcp","bearerSecretRef":null}
        })),
    )
    .await;
    assert_eq!(added.status(), StatusCode::CREATED);
    let added: Value = added.json().await.unwrap();
    let revision = added["revision"].as_str().unwrap().to_owned();

    let binding_url = format!("{}/api/v1/manage/mcp/reference-binding/auth", core.endpoint);
    let value_binding = client
        .post(&binding_url)
        .bearer_auth(&manage.token)
        .header("If-Match", &revision)
        .json(&json!({"secretReference":"new-reference","secretValue":CANARY}))
        .send()
        .await
        .unwrap();
    assert_eq!(value_binding.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let value_binding_body = value_binding.text().await.unwrap();
    assert!(!value_binding_body.contains(CANARY));
    assert_eq!(
        client
            .post(&binding_url)
            .bearer_auth(&manage.token)
            .json(&json!({"secretReference":"new-reference"}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::PRECONDITION_FAILED,
        "HTTP reference binding requires If-Match"
    );
    let stale = client
        .post(&binding_url)
        .bearer_auth(&manage.token)
        .header("If-Match", "stale-revision")
        .json(&json!({"secretReference":"new-reference"}))
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), StatusCode::PRECONDITION_FAILED);
    let missing = client
        .post(&binding_url)
        .bearer_auth(&manage.token)
        .header("If-Match", &revision)
        .json(&json!({"secretReference":"missing-reference"}))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    let unchanged: Value = management_request(
        &client,
        &core,
        Method::GET,
        "/mcp/reference-binding",
        &read.token,
        None,
    )
    .await
    .json()
    .await
    .unwrap();
    assert!(unchanged["transport"]["bearerSecretRef"].is_null());
    assert_eq!(unchanged["revision"], revision);

    let committed = client
        .post(&binding_url)
        .bearer_auth(&manage.token)
        .header("If-Match", &revision)
        .json(&json!({"secretReference":"new-reference"}))
        .send()
        .await
        .unwrap();
    assert_eq!(committed.status(), StatusCode::OK);
    let committed: Value = committed.json().await.unwrap();
    assert_ne!(committed["connection"]["revision"], revision);
    assert_eq!(
        committed["connection"]["transport"]["bearerSecretRef"],
        "new-reference"
    );
    assert!(!committed.to_string().contains(CANARY));

    let metadata: Value =
        management_request(&client, &core, Method::GET, "/secrets", &manage.token, None)
            .await
            .json()
            .await
            .unwrap();
    assert!(metadata.to_string().contains("new-reference"));
    assert!(!metadata.to_string().contains(CANARY));
    let database_bytes = std::fs::read(store.data_dir().join("kakune.sqlite3")).unwrap();
    assert!(!String::from_utf8_lossy(&database_bytes).contains(CANARY));

    assert!(store.resolve_secret("new-reference").is_ok());
    let removed = client
        .delete(format!(
            "{}/api/v1/manage/secrets/new-reference",
            core.endpoint
        ))
        .bearer_auth(&manage.token)
        .send()
        .await
        .unwrap();
    assert_eq!(removed.status(), StatusCode::OK);
    assert!(store.resolve_secret("new-reference").is_err());
    core.shutdown().await.expect("Core should stop cleanly");
}
