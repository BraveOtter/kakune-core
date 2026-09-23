use std::{
    fs,
    sync::{Arc, Mutex},
};

use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use futures_util::StreamExt;
use kakune_core::{Store, WorkflowDocument, api};
use reqwest::header::AUTHORIZATION;
use serde_json::{Value, json};

#[tokio::test]
async fn workflow_create_rejects_duplicate_ids_without_overwriting_the_saved_source() {
    let directory =
        std::env::temp_dir().join(format!("kakune-create-api-{}", uuid::Uuid::new_v4()));
    let store = Store::open(directory.clone()).expect("store should open");
    let token = store
        .ensure_bootstrap_token()
        .expect("token should be created")
        .expect("a new store needs a bootstrap token");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let server = tokio::spawn(async move {
        axum::serve(listener, api::router(store))
            .await
            .expect("ephemeral Core should serve");
    });
    let client = reqwest::Client::new();
    let endpoint = format!("http://{address}/api/v1/workflows");
    let original = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: unique-flow\n  name: Original\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: start\nnodes:\n  - id: start\n    type: kakune.flow.pass@1\n";
    let created = client
        .post(&endpoint)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .json(&json!({ "source": original }))
        .send()
        .await
        .expect("workflow create request should complete");
    assert_eq!(created.status(), StatusCode::CREATED);

    let replacement = original.replace("Original", "Replacement");
    let duplicate = client
        .post(&endpoint)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .json(&json!({ "source": replacement }))
        .send()
        .await
        .expect("duplicate workflow request should complete");
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);

    let saved: Value = client
        .get(&endpoint)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .expect("workflow list request should complete")
        .error_for_status()
        .expect("workflow list should succeed")
        .json()
        .await
        .expect("workflow list should be JSON");
    assert_eq!(saved["items"][0]["name"], "Original");

    drop(client);
    server.abort();
    let _ = server.await;
    let mut cleanup_error = None;
    for _ in 0..20 {
        match fs::remove_dir_all(&directory) {
            Ok(()) => return,
            Err(error) => {
                cleanup_error = Some(error);
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
    }
    panic!(
        "temporary Core data should be removed: {}",
        cleanup_error.expect("cleanup should fail before panic")
    );
}

#[tokio::test]
async fn ephemeral_core_serves_a_consumer_over_http_and_sse() {
    let directory = std::env::temp_dir().join(format!("kakune-api-smoke-{}", uuid::Uuid::new_v4()));
    let store = Store::open(directory.clone()).expect("store should open");
    let token = store
        .ensure_bootstrap_token()
        .expect("token should be created")
        .expect("a new store needs a bootstrap token");
    let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata:\n  id: smoke\n  name: Smoke\ntriggers:\n  - id: manual\n    type: kakune.trigger.manual@1\nentry: log\nnodes:\n  - id: log\n    type: kakune.log@1\n    inputs:\n      message: { literal: smoke }\n";
    let workflow = WorkflowDocument::parse(source).expect("workflow should parse");
    store
        .upsert_workflow(&workflow, source, "enabled")
        .expect("workflow should persist");
    store
        .set_workflow_status("smoke", "disabled")
        .expect("event should persist");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener
        .local_addr()
        .expect("listener should have an address");
    let server = tokio::spawn(async move {
        axum::serve(listener, api::router(store))
            .await
            .expect("ephemeral Core should serve");
    });
    let client = reqwest::Client::new();
    let base_url = format!("http://{address}");

    let health: Value = client
        .get(format!("{base_url}/health/live"))
        .send()
        .await
        .expect("health request should complete")
        .error_for_status()
        .expect("health should succeed")
        .json()
        .await
        .expect("health should be JSON");
    assert_eq!(health["status"], "ok");
    let info: Value = client
        .get(format!("{base_url}/api/v1/info"))
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .expect("info request should complete")
        .error_for_status()
        .expect("info should succeed")
        .json()
        .await
        .expect("info should be JSON");
    assert!(info["coreId"].as_str().is_some());
    let response = client
        .get(format!("{base_url}/api/v1/events"))
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .expect("SSE request should complete")
        .error_for_status()
        .expect("SSE should succeed");
    let mut stream = response.bytes_stream();
    let mut events = String::new();
    while !events.contains("event: workflow.status_changed") {
        let chunk = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("durable events should arrive promptly")
            .expect("SSE stream should remain open")
            .expect("SSE chunk should be valid");
        events.push_str(std::str::from_utf8(&chunk).expect("SSE should be UTF-8"));
    }
    assert!(events.contains("event: workflow.status_changed"));
    assert!(events.contains("\"eventVersion\":\"1.0\""));

    drop(stream);
    drop(client);
    server.abort();
    let _ = server.await;
    // An SSE connection owns a clone of the router state until Hyper observes
    // its disconnect. On Windows SQLite keeps the database handle open for a
    // short moment after that, so make test cleanup wait for the actual close.
    let mut cleanup_error = None;
    for _ in 0..20 {
        match fs::remove_dir_all(&directory) {
            Ok(()) => return,
            Err(error) => {
                cleanup_error = Some(error);
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
    }
    panic!(
        "temporary Core data should be removed: {}",
        cleanup_error.expect("cleanup should fail before panic")
    );
}

#[derive(Default)]
struct DirectMcpState {
    authorizations: Mutex<Vec<Option<String>>>,
}

async fn direct_mcp_server(
    State(state): State<Arc<DirectMcpState>>,
    headers: HeaderMap,
    Json(request): Json<Value>,
) -> Response {
    state
        .authorizations
        .lock()
        .expect("authorization lock")
        .push(
            headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        );
    if request["method"] == "notifications/initialized" {
        return StatusCode::NO_CONTENT.into_response();
    }
    let id = request["id"].clone();
    let result = match request["method"].as_str().expect("MCP method") {
        "initialize" => json!({
            "protocolVersion": "2026-07-28",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "direct-api-test-server", "version": "1.0.0"}
        }),
        "tools/call" => json!({
            "content": [{"type": "text", "text": "Hello, Ada!"}],
            "structuredContent": {"greeting": "Hello, Ada!"}
        }),
        method => panic!("unexpected MCP method {method}"),
    };
    Json(json!({"jsonrpc": "2.0", "id": id, "result": result})).into_response()
}

#[tokio::test]
async fn direct_mcp_api_call_uses_configured_http_transport() {
    let directory = std::env::temp_dir().join(format!("kakune-mcp-api-{}", uuid::Uuid::new_v4()));
    let store = Store::open(directory.clone()).expect("store should open");
    let token = store
        .ensure_bootstrap_token()
        .expect("token should be created")
        .expect("new store needs a bootstrap token");

    let mcp_state = Arc::new(DirectMcpState::default());
    let mcp_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("MCP listener should bind");
    let mcp_address = mcp_listener
        .local_addr()
        .expect("MCP listener should have an address");
    let mcp_server = tokio::spawn({
        let mcp_state = Arc::clone(&mcp_state);
        async move {
            axum::serve(
                mcp_listener,
                Router::new()
                    .route("/mcp", post(direct_mcp_server))
                    .with_state(mcp_state),
            )
            .await
            .expect("MCP test server should remain valid");
        }
    });

    let core_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("Core listener should bind");
    let core_address = core_listener
        .local_addr()
        .expect("Core listener should have an address");
    let core_server = tokio::spawn(async move {
        axum::serve(core_listener, api::router(store))
            .await
            .expect("Core test server should remain valid");
    });

    let response = reqwest::Client::new()
        .post(format!("http://{core_address}/api/v1/mcp/call"))
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .json(&json!({
            "transport": {
                "transport": "http",
                "endpoint": format!("http://{mcp_address}/mcp")
            },
            "toolName": "greet",
            "arguments": {"name": "Ada"}
        }))
        .send()
        .await
        .expect("direct MCP request should complete");
    let status = response.status();
    let response = response
        .text()
        .await
        .expect("direct MCP response should be text");
    assert_eq!(status, StatusCode::OK, "{response}");

    assert!(response.contains("direct-api-test-server"));
    assert!(response.contains("Hello, Ada!"));
    assert!(
        mcp_state
            .authorizations
            .lock()
            .expect("authorization lock")
            .iter()
            .all(Option::is_none)
    );

    core_server.abort();
    let _ = core_server.await;
    mcp_server.abort();
    let _ = mcp_server.await;
    fs::remove_dir_all(directory).expect("temporary Core data should be removed");
}
