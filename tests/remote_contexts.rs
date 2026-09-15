use kakune_core::{ConnectionContext, Store, api, remote::RemoteClient};
use reqwest::Method;
use serde_json::json;

#[tokio::test]
async fn two_real_cores_keep_sources_and_executions_isolated() {
    let mut running = Vec::new();
    for name in ["first", "second"] {
        let dir = std::env::temp_dir().join(format!("kakune-remote-{}", uuid::Uuid::new_v4()));
        let store = Store::open(dir.clone()).unwrap();
        let token = store.ensure_bootstrap_token().unwrap().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let context = ConnectionContext {
            id: name.into(),
            name: name.into(),
            endpoint: format!("http://{}", listener.local_addr().unwrap()),
            expected_core_id: None,
            color: None,
            credential_ref: None,
        };
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let app = api::router(store.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
                .unwrap();
        });
        let client = RemoteClient::connect(&context, token.clone(), None)
            .await
            .unwrap();
        let mut wrong = context.clone();
        wrong.expected_core_id = Some("different-core".into());
        assert!(RemoteClient::connect(&wrong, token, None).await.is_err());
        running.push((client, stop, task, store, dir));
    }
    let source = "apiVersion: kakune/v1\nkind: Workflow\nmetadata: {id: isolated, name: Isolated}\ntriggers: [{id: manual, type: kakune.trigger.manual@1}]\nentry: start\nnodes:\n  - id: start\n    type: kakune.flow.pass@1\n";
    let first = &running[0].0;
    first.save_workflow(source).await.unwrap();
    assert!(
        running[1]
            .0
            .request(
                Method::GET,
                &["workflows", "isolated", "source"],
                None,
                None
            )
            .await
            .is_err()
    );
    let run = first
        .request(
            Method::POST,
            &["executions"],
            Some(json!({"workflowId":"isolated"})),
            None,
        )
        .await
        .unwrap();
    let id = run["id"].as_str().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let execution = first
                .request(Method::GET, &["executions", id], None, None)
                .await
                .unwrap();
            if execution["execution"]["status"] == "succeeded" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        running[1]
            .0
            .request(Method::GET, &["executions", id], None, None)
            .await
            .is_err()
    );
    assert!(
        first
            .request(
                Method::PUT,
                &["workflows", "isolated", "source"],
                Some(json!({"source":source})),
                Some("stale-revision")
            )
            .await
            .is_err()
    );
    for (client, stop, task, store, dir) in running {
        drop(client);
        stop.send(()).unwrap();
        task.await.unwrap();
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
