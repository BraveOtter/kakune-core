use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use kakune_core::{Store, api};
use serde_json::{Value, json};
use tower::ServiceExt;

#[test]
fn concurrent_admission_and_restart_keep_one_execution_per_request() {
    let dir = std::env::temp_dir().join(format!("kakune-idempotency-{}", uuid::Uuid::new_v4()));
    let store = Store::open(dir.clone()).unwrap();
    let barrier = std::sync::Barrier::new(8);
    let outcomes = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    store
                        .create_execution_with_plan_request(
                            "workflow",
                            "revision",
                            &json!({"policy":{}}),
                            Some(("retry-key", "request-hash")),
                        )
                        .unwrap()
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(outcomes.iter().filter(|(_, created)| *created).count(), 1);
    assert!(
        outcomes
            .iter()
            .all(|(record, _)| record.id == outcomes[0].0.id)
    );
    drop(store);
    let store = Store::open(dir.clone()).unwrap();
    assert_eq!(
        store
            .find_idempotent_execution("retry-key", "request-hash")
            .unwrap()
            .unwrap()
            .id,
        outcomes[0].0.id
    );
    assert!(
        store
            .find_idempotent_execution("retry-key", "changed-request")
            .is_err()
    );
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn public_api_replays_the_record_and_rejects_changed_payloads() {
    let dir = std::env::temp_dir().join(format!("kakune-idempotency-api-{}", uuid::Uuid::new_v4()));
    let store = Store::open(dir.clone()).unwrap();
    let token = store.ensure_bootstrap_token().unwrap().unwrap();
    let original = store
        .create_execution_with_plan_request(
            "flow",
            "revision",
            &json!({"policy":{}}),
            Some((
                "known-key",
                &hash(&json!({"workflowId":"flow","inputs":{}})),
            )),
        )
        .unwrap()
        .0;
    let app = api::router(store.clone());
    for (inputs, status) in [(json!({}), 200), (json!({"changed":true}), 409)] {
        let request = Request::post("/api/v1/executions")
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .header("Idempotency-Key", "known-key")
            .body(Body::from(
                json!({"workflowId":"flow","inputs":inputs}).to_string(),
            ))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status().as_u16(), status);
        if status == 200 {
            let value: Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(value["id"], original.id);
        }
    }
    drop(app);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

fn hash(value: &Value) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(value.to_string().as_bytes()))
}
