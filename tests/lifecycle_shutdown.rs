mod support;

use std::{
    fs,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    routing::get,
};
use kakune_core::{
    PluginRegistry, Store, WorkflowDocument, api,
    config::ApiConfig,
    execute_prepared_workflow_with_plugins, execute_prepared_workflow_with_plugins_and_shutdown,
    lifecycle::{
        CoreLifecycle, Deadline, LifecyclePhase, SystemMonotonicClock,
        execution::TrackedExecutionState,
    },
    prepare_execution_with_plugins,
};
use support::lifecycle::LifecycleFixture;
use tokio::time::Instant;
use tower::ServiceExt;

async fn hold_http_request(State(entered): State<Arc<AtomicBool>>) -> &'static str {
    entered.store(true, Ordering::Release);
    std::future::pending::<&'static str>().await
}

#[tokio::test]
async fn readiness_tracks_lifecycle_and_stopping_rejects_new_execution_requests() {
    let fixture = LifecycleFixture::new().expect("fixture should be isolated");
    let store = Store::open(fixture.data_dir()).expect("store should open");
    let token = store
        .ensure_bootstrap_token()
        .expect("test token should be created")
        .expect("fresh test storage should have a bootstrap token");
    let lifecycle = CoreLifecycle::starting();
    let app = api::router_with_lifecycle(
        store,
        PluginRegistry::default(),
        ApiConfig::default(),
        lifecycle.clone(),
    );

    let live = app
        .clone()
        .oneshot(Request::get("/health/live").body(Body::empty()).unwrap())
        .await
        .expect("liveness request should complete");
    assert_eq!(live.status(), StatusCode::OK);
    let starting = app
        .clone()
        .oneshot(Request::get("/health/ready").body(Body::empty()).unwrap())
        .await
        .expect("readiness request should complete");
    assert_eq!(starting.status(), StatusCode::SERVICE_UNAVAILABLE);

    lifecycle
        .mark_ready()
        .expect("starting Core should become ready");
    let ready = app
        .clone()
        .oneshot(Request::get("/health/ready").body(Body::empty()).unwrap())
        .await
        .expect("readiness request should complete");
    assert_eq!(ready.status(), StatusCode::OK);

    let accepted = lifecycle
        .admission()
        .admit(TrackedExecutionState::Preparation)
        .expect("ready Core should admit work");
    lifecycle
        .begin_stopping()
        .expect("shutdown should close admission");
    assert_eq!(lifecycle.phase(), LifecyclePhase::Stopping);
    assert!(
        lifecycle
            .admission()
            .admit(TrackedExecutionState::Queued)
            .is_err()
    );

    let stopping = app
        .clone()
        .oneshot(Request::get("/health/ready").body(Body::empty()).unwrap())
        .await
        .expect("readiness request should complete");
    assert_eq!(stopping.status(), StatusCode::SERVICE_UNAVAILABLE);

    let execution = app
        .oneshot(
            Request::post("/api/v1/executions")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(r#"{"workflowId":"missing"}"#))
                .unwrap(),
        )
        .await
        .expect("execution request should complete");
    assert_eq!(execution.status(), StatusCode::SERVICE_UNAVAILABLE);
    drop(accepted);
}

#[test]
fn a_blocking_worker_keeps_the_runtime_lease_until_its_storage_work_finishes() {
    use kakune_core::lifecycle::{Deadline, SystemMonotonicClock, ownership::RuntimeLease};

    let fixture = LifecycleFixture::new().expect("fixture should be isolated");
    let data_dir = fixture.data_dir();
    fs::create_dir_all(&data_dir).expect("data directory should exist");
    let clock = SystemMonotonicClock::default();
    let first = RuntimeLease::acquire(
        &data_dir,
        uuid::Uuid::new_v4(),
        &clock,
        &kakune_core::lifecycle::ThreadWaiter,
    )
    .expect("first instance should acquire its lease");
    let lifecycle = CoreLifecycle::starting();
    lifecycle
        .mark_ready()
        .expect("test Core should become ready");
    let worker = lifecycle
        .admission()
        .admit(TrackedExecutionState::Active)
        .expect("worker should be admitted");
    lifecycle.begin_stopping().expect("shutdown should begin");

    let zero_deadline = Deadline::after(&clock, Duration::ZERO);
    let competing = RuntimeLease::acquire_with_deadline(
        &data_dir,
        uuid::Uuid::new_v4(),
        zero_deadline,
        &clock,
        &kakune_core::lifecycle::ThreadWaiter,
    );
    assert!(
        competing.is_err(),
        "the in-flight worker's lease must remain held"
    );
    assert_eq!(lifecycle.admission().snapshot().unwrap().active, 1);

    drop(worker);
    drop(first);
    let restarted = RuntimeLease::acquire(
        &data_dir,
        uuid::Uuid::new_v4(),
        &clock,
        &kakune_core::lifecycle::ThreadWaiter,
    )
    .expect("a new instance may acquire the lease after worker completion");
    drop(restarted);
}

#[test]
fn cooperative_shutdown_recovers_from_checkpoints_without_repeating_completed_nodes() {
    let fixture = LifecycleFixture::new().expect("fixture should be isolated");
    let store = Store::open(fixture.data_dir()).expect("store should open");
    let source = r#"apiVersion: kakune/v1
kind: Workflow
metadata: { id: shutdown-checkpoint, name: Shutdown checkpoint }
triggers: [{ id: manual, type: kakune.trigger.manual@1 }]
entry: first
nodes:
  - id: first
    type: kakune.flow.pass@1
    on: { success: [wait] }
  - id: wait
    type: kakune.flow.delay@1
    inputs: { durationMs: { literal: 1000 } }
    on: { success: [finish] }
  - id: finish
    type: kakune.flow.pass@1
"#;
    let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
    store
        .upsert_workflow(&workflow, source, "enabled")
        .expect("workflow should be persisted");
    let plugins = PluginRegistry::default();
    let revision = store
        .get_workflow_source("shutdown-checkpoint")
        .expect("workflow source should load")
        .expect("workflow source should exist")
        .revision;
    let execution = prepare_execution_with_plugins(&store, source, &revision, &plugins)
        .expect("execution should be prepared");
    let execution_id = execution.id.clone();
    let lifecycle = CoreLifecycle::starting();
    lifecycle.mark_ready().expect("Core should become ready");
    let shutdown_requested = lifecycle.shutdown_signal();
    let worker_store = store.clone();
    let worker_plugins = plugins.clone();
    let worker = thread::spawn(move || {
        execute_prepared_workflow_with_plugins_and_shutdown(
            &worker_store,
            execution,
            &worker_plugins,
            shutdown_requested,
        )
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let first_completed = store
            .list_node_runs(&execution_id)
            .expect("node state should load")
            .iter()
            .any(|node| node.node_id == "first" && node.status == "succeeded");
        let still_running = store
            .get_execution(&execution_id)
            .expect("execution lookup should succeed")
            .is_some_and(|record| record.status == "running");
        if first_completed && still_running {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "workflow should reach the delay checkpoint; node states: {:?}; execution: {:?}",
            store
                .list_node_runs(&execution_id)
                .expect("node state should load"),
            store
                .get_execution(&execution_id)
                .expect("execution lookup should succeed")
        );
        thread::sleep(Duration::from_millis(10));
    }

    lifecycle
        .begin_stopping()
        .expect("shutdown should close admission");
    assert_eq!(
        worker
            .join()
            .expect("worker thread should join")
            .unwrap_err(),
        "Core shutdown requested"
    );
    assert_eq!(
        store
            .get_execution(&execution_id)
            .expect("execution should load")
            .expect("execution should remain persisted")
            .status,
        "running",
        "shutdown must not fabricate a terminal result"
    );

    assert_eq!(
        store
            .recover_incomplete_executions()
            .expect("restart recovery should complete"),
        1
    );
    let queued = store
        .get_execution(&execution_id)
        .expect("execution should load")
        .expect("execution should remain persisted");
    assert_eq!(queued.status, "queued");
    let resumed = execute_prepared_workflow_with_plugins(&store, queued, &plugins)
        .expect("restart should resume the workflow");
    assert_eq!(resumed.status, "succeeded");
    let trace = store
        .execution_trace(&execution_id)
        .expect("execution trace should load")
        .expect("execution trace should exist");
    assert_eq!(
        trace
            .spans
            .iter()
            .filter(|span| span.node_id.as_deref() == Some("first"))
            .count(),
        1,
        "a completed checkpoint must not be executed a second time"
    );
}

#[tokio::test]
async fn http_and_tls_connections_share_the_absolute_core_drain_deadline() {
    for use_tls in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let address = listener.local_addr().expect("test listener has an address");
        let entered = Arc::new(AtomicBool::new(false));
        let app = Router::new()
            .route("/hold", get(hold_http_request))
            .with_state(Arc::clone(&entered));
        let config = if use_tls {
            ApiConfig {
                tls_cert: Some(
                    concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/tests/fixtures/localhost-cert.pem"
                    )
                    .into(),
                ),
                tls_key: Some(
                    concat!(
                        env!("CARGO_MANIFEST_DIR"),
                        "/tests/fixtures/localhost-key.pem"
                    )
                    .into(),
                ),
                ..ApiConfig::default()
            }
        } else {
            ApiConfig::default()
        };
        let server_config = config.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            kakune_core::server::serve_until(listener, app, &server_config, async move {
                stopped
                    .await
                    .expect("test shutdown should send the shared deadline")
            })
            .await
        });
        let scheme = if use_tls { "https" } else { "http" };
        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(use_tls)
            .build()
            .expect("test client should build");
        let request = tokio::spawn(async move {
            client
                .get(format!("{scheme}://{address}/hold"))
                .send()
                .await
        });
        let entered_deadline = Instant::now() + Duration::from_secs(5);
        while !entered.load(Ordering::Acquire) && Instant::now() < entered_deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            entered.load(Ordering::Acquire),
            "{scheme} handler should be active before shutdown"
        );

        let started = Instant::now();
        stop.send(Instant::now() + Duration::from_millis(100))
            .expect("server should still own the shutdown receiver");
        let result = tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .expect("server should finish bounded connection drain")
            .expect("server task should join");
        result.expect("server should stop after the shared deadline");
        assert!(started.elapsed() < Duration::from_secs(2));
        request.abort();
        let _ = request.await;
    }
}

#[tokio::test]
async fn scheduler_trigger_work_is_admitted_tracked_and_joined_during_shutdown() {
    let fixture = LifecycleFixture::new().expect("fixture should be isolated");
    let store = Store::open(fixture.data_dir()).expect("store should open");
    let source = r#"apiVersion: kakune/v1
kind: Workflow
metadata: { id: startup-drain, name: Startup drain }
triggers: [{ id: startup, type: kakune.trigger.startup@1 }]
entry: wait
nodes:
  - id: wait
    type: kakune.flow.delay@1
    inputs: { durationMs: { literal: 10000 } }
"#;
    let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
    store
        .upsert_workflow(&workflow, source, "enabled")
        .expect("trigger workflow should be enabled");
    let lifecycle = CoreLifecycle::starting();
    let mut scheduler = kakune_core::start_managed_scheduler(store.clone(), lifecycle.clone());
    scheduler
        .wait_started()
        .await
        .expect("scheduler loop should start");
    lifecycle.mark_ready().expect("Core should become ready");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if !store
            .list_executions()
            .expect("execution list should load")
            .is_empty()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "startup trigger should submit tracked work"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let before_shutdown = lifecycle
        .admission()
        .snapshot()
        .expect("admission state should be readable");
    assert!(
        before_shutdown.outstanding() > 0,
        "trigger execution should still own a permit: {before_shutdown:?}; executions: {:?}",
        store.list_executions().expect("execution list should load")
    );

    lifecycle
        .begin_stopping()
        .expect("shutdown should close scheduler/API admission");
    let shutdown_deadline = Instant::now() + Duration::from_secs(3);
    scheduler
        .stop_until(shutdown_deadline)
        .await
        .expect("scheduler and watchers should stop before the shared deadline");
    let clock = SystemMonotonicClock::default();
    let drain_deadline = Deadline::after(&clock, Duration::from_secs(3));
    let admission = lifecycle.admission();
    let drain =
        tokio::task::spawn_blocking(move || admission.wait_for_drain(drain_deadline, &clock));
    let drained = tokio::time::timeout(Duration::from_secs(3), drain)
        .await
        .expect("tracked trigger execution should cooperatively drain")
        .expect("drain waiter should join")
        .expect("accepted trigger work should finish shutdown checkpointing");
    assert!(drained.is_drained());
    let final_execution = store
        .list_executions()
        .expect("execution list should load")
        .into_iter()
        .next()
        .expect("trigger execution should remain inspectable");
    assert!(
        final_execution.status == "running" || final_execution.status == "queued",
        "shutdown must leave incomplete work recoverable, not successful: {final_execution:?}"
    );
}
