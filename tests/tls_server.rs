use kakune_core::{Store, api, config::ApiConfig, server};

#[tokio::test]
async fn tls_serves_authenticated_api_and_rejects_untrusted_certificates() {
    let dir = std::env::temp_dir().join(format!("kakune-tls-{}", uuid::Uuid::new_v4()));
    let store = Store::open(dir.clone()).unwrap();
    let token = store.ensure_bootstrap_token().unwrap().unwrap();
    let config = ApiConfig {
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
        ..Default::default()
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "https://localhost:{}/api/v1/info",
        listener.local_addr().unwrap().port()
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let app = api::router(store.clone());
    let task = tokio::spawn(async move {
        server::serve(listener, app, &config, async {
            let _ = stopped.await;
        })
        .await
    });
    assert!(reqwest::Client::new().get(&url).send().await.is_err());
    let cert =
        reqwest::Certificate::from_pem(include_bytes!("fixtures/localhost-cert.pem")).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()
        .unwrap();
    assert_eq!(client.get(&url).send().await.unwrap().status(), 401);
    assert_eq!(
        client
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let _ = stop.send(());
    drop(client);
    task.await.unwrap().unwrap();
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}
