use crate::management::IntegrationCheck;
use futures_util::StreamExt;
use std::time::Duration;

fn check(status: &str, message: &str) -> IntegrationCheck {
    IntegrationCheck {
        status: status.to_owned(),
        message: message.to_owned(),
    }
}

pub(crate) async fn probe_codex(
    tokens: &crate::codex::CodexOAuthTokens,
    model: &str,
) -> (IntegrationCheck, IntegrationCheck) {
    if tokens.expired() {
        return (
            check("unknown", "provider connection was not attempted"),
            check(
                "unknown",
                "OAuth access token has expired; refresh authentication before testing",
            ),
        );
    }
    probe_codex_endpoint(
        crate::codex::CODEX_RESPONSES_URL,
        &tokens.access,
        tokens.account_id.as_deref(),
        model,
    )
    .await
}

async fn probe_codex_endpoint(
    url: &str,
    access: &str,
    account: Option<&str>,
    model: &str,
) -> (IntegrationCheck, IntegrationCheck) {
    let unknown = || check("unknown", "upstream authentication could not be verified");
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none())
        .build()
    {
        Ok(client) => client,
        Err(_) => {
            return (
                check("failed", "cannot initialize provider client"),
                unknown(),
            );
        }
    };
    let mut request = client
        .post(url)
        .bearer_auth(access)
        .header("originator", "kakune")
        .header("User-Agent", concat!("kakune/", env!("CARGO_PKG_VERSION")))
        .json(
            &serde_json::json!({"model": model, "store": false, "stream": true,
            "include": ["reasoning.encrypted_content"],
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "Hi"}]}]}),
        );
    if let Some(account) = account {
        request = request.header("ChatGPT-Account-Id", account);
    }
    let response = match request.send().await {
        Ok(response) => response,
        Err(_) => return (check("failed", "provider connection failed"), unknown()),
    };
    let connectivity = check("passed", "provider endpoint responded");
    let status = response.status();
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        return (
            connectivity,
            check("failed", "provider rejected authentication"),
        );
    }
    if !status.is_success() {
        return (connectivity, unknown());
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            return (connectivity, unknown());
        };
        if bytes.len().saturating_add(chunk.len()) > 65_536 {
            return (connectivity, unknown());
        }
        bytes.extend_from_slice(&chunk);
    }
    let completed = std::str::from_utf8(&bytes).ok().is_some_and(|text| {
        text.lines()
            .filter_map(|line| line.trim().strip_prefix("data:"))
            .filter_map(|data| serde_json::from_str::<serde_json::Value>(data.trim()).ok())
            .any(|event| {
                event.get("type").and_then(serde_json::Value::as_str) == Some("response.completed")
                    && event
                        .get("response")
                        .is_some_and(serde_json::Value::is_object)
            })
    });
    if completed {
        (
            connectivity,
            check("passed", "provider accepted the configured credentials"),
        )
    } else {
        (connectivity, unknown())
    }
}

pub(crate) async fn probe_minimax(
    base_url: &str,
    secret: &str,
    model: &str,
    additional_ca: Option<&reqwest::Certificate>,
) -> (IntegrationCheck, IntegrationCheck) {
    let Ok(mut url) = reqwest::Url::parse(base_url) else {
        return (
            check("failed", "invalid configured provider URL"),
            check("unknown", "authentication was not attempted"),
        );
    };
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return (
            check("failed", "invalid configured provider URL"),
            check("unknown", "authentication was not attempted"),
        );
    }
    if let Ok(mut segments) = url.path_segments_mut() {
        segments.pop_if_empty().extend(["v1", "messages"]);
    }
    probe_minimax_endpoint_with_ca(url.as_str(), secret, model, additional_ca).await
}

#[cfg(test)]
async fn probe_minimax_endpoint(
    url: &str,
    secret: &str,
    model: &str,
) -> (IntegrationCheck, IntegrationCheck) {
    probe_minimax_endpoint_with_ca(url, secret, model, None).await
}

async fn probe_minimax_endpoint_with_ca(
    url: &str,
    secret: &str,
    model: &str,
    additional_ca: Option<&reqwest::Certificate>,
) -> (IntegrationCheck, IntegrationCheck) {
    let unknown = || check("unknown", "upstream authentication could not be verified");
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::none());
    if let Some(ca) = additional_ca {
        builder = builder.add_root_certificate(ca.clone());
    }
    let client = match builder.build() {
        Ok(client) => client,
        Err(_) => {
            return (
                check("failed", "cannot initialize provider client"),
                unknown(),
            );
        }
    };
    let response = match client.post(url)
        .bearer_auth(secret)
        .header("anthropic-version", "2023-06-01")
        .json(&serde_json::json!({"model": model, "max_tokens": 1, "messages": [{"role":"user", "content":"Hi"}]}))
        .send().await {
            Ok(response) => response,
            Err(_) => return (check("failed", "provider connection failed"), unknown()),
        };
    let connectivity = check("passed", "provider endpoint responded");
    let status = response.status();
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        return (
            connectivity,
            check("failed", "provider rejected authentication"),
        );
    }
    if !status.is_success() {
        return (connectivity, unknown());
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            return (connectivity, unknown());
        };
        if bytes.len().saturating_add(chunk.len()) > 65_536 {
            return (connectivity, unknown());
        }
        bytes.extend_from_slice(&chunk);
    }
    let valid = serde_json::from_slice::<serde_json::Value>(&bytes)
        .ok()
        .and_then(|value| {
            value
                .get("content")
                .and_then(|content| content.as_array())
                .cloned()
        })
        .is_some();
    if valid {
        (
            connectivity,
            check("passed", "provider accepted the configured credentials"),
        )
    } else {
        (connectivity, unknown())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, http::StatusCode, routing::post};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn expired_codex_access_does_not_contact_provider_or_claim_authentication() {
        let tokens = crate::codex::CodexOAuthTokens {
            access: "fake-access".into(),
            refresh: "fake-refresh".into(),
            expires_at_ms: 0,
            account_id: None,
        };
        let (connectivity, authentication) = probe_codex(&tokens, "model").await;
        assert_eq!(connectivity.status, "unknown");
        assert_eq!(authentication.status, "unknown");
        assert!(authentication.message.contains("expired"));
    }

    #[tokio::test]
    async fn codex_probe_sends_oauth_and_requires_completed_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/responses", post(|headers: axum::http::HeaderMap, axum::Json(body): axum::Json<serde_json::Value>| async move {
                assert_eq!(headers.get("authorization").unwrap(), "Bearer fake-access");
                assert_eq!(headers.get("chatgpt-account-id").unwrap(), "account");
                assert_eq!(body["stream"], true);
                "data: {\"type\":\"response.completed\",\"response\":{\"output\":[]}}\n\n"
            }))).await.unwrap();
        });
        let (connectivity, authentication) = probe_codex_endpoint(
            &format!("http://{address}/responses"),
            "fake-access",
            Some("account"),
            "model",
        )
        .await;
        assert_eq!(connectivity.status, "passed");
        assert_eq!(authentication.status, "passed");
        server.abort();
    }

    #[tokio::test]
    async fn codex_probe_reports_rejection_without_exposing_response_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/responses",
                    post(|| async { (StatusCode::UNAUTHORIZED, "fake-access") }),
                ),
            )
            .await
            .unwrap();
        });
        let (connectivity, authentication) = probe_codex_endpoint(
            &format!("http://{address}/responses"),
            "fake-access",
            None,
            "model",
        )
        .await;
        assert_eq!(connectivity.status, "passed");
        assert_eq!(authentication.status, "failed");
        assert!(!authentication.message.contains("fake-access"));
        server.abort();
    }

    #[tokio::test]
    async fn codex_probe_does_not_report_success_for_an_incomplete_stream() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/responses",
                    post(|| async { "data: {\"type\":\"response.created\"}\n\n" }),
                ),
            )
            .await
            .unwrap();
        });
        let (connectivity, authentication) = probe_codex_endpoint(
            &format!("http://{address}/responses"),
            "fake-access",
            None,
            "model",
        )
        .await;
        assert_eq!(connectivity.status, "passed");
        assert_eq!(authentication.status, "unknown");
        server.abort();
    }

    #[tokio::test]
    async fn minimax_probe_does_not_treat_server_errors_as_invalid_credentials() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/messages",
                    post(|| async { StatusCode::SERVICE_UNAVAILABLE }),
                ),
            )
            .await
            .unwrap();
        });
        let (connectivity, authentication) = probe_minimax_endpoint(
            &format!("http://{address}/v1/messages"),
            "fake-secret",
            "model",
        )
        .await;
        assert_eq!(connectivity.status, "passed");
        assert_eq!(authentication.status, "unknown");
        server.abort();
    }

    #[tokio::test]
    async fn minimax_probe_rejects_oversized_response_without_exposing_it() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/messages", post(|| async { "x".repeat(65_537) })),
            )
            .await
            .unwrap();
        });
        let (connectivity, authentication) = probe_minimax_endpoint(
            &format!("http://{address}/v1/messages"),
            "fake-secret",
            "model",
        )
        .await;
        assert_eq!(connectivity.status, "passed");
        assert_eq!(authentication.status, "unknown");
        assert!(!authentication.message.contains("fake-secret"));
        server.abort();
    }

    #[tokio::test]
    async fn probe_sends_credentials_and_recognizes_a_valid_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/messages",
                    post(|headers: axum::http::HeaderMap| async move {
                        assert_eq!(headers.get("authorization").unwrap(), "Bearer fake-secret");
                        axum::Json(serde_json::json!({"content": [{"type":"text", "text":"ok"}]}))
                    }),
                ),
            )
            .await
            .unwrap();
        });
        let (connectivity, authentication) = probe_minimax_endpoint(
            &format!("http://{address}/v1/messages"),
            "fake-secret",
            "model",
        )
        .await;
        assert_eq!(connectivity.status, "passed");
        assert_eq!(authentication.status, "passed");
        server.abort();
    }

    #[tokio::test]
    async fn probe_reports_http_authentication_failure_without_echoing_secrets() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/messages",
                    post(|| async { (StatusCode::UNAUTHORIZED, "fake-secret") }),
                ),
            )
            .await
            .unwrap();
        });
        let url = format!("http://{address}/v1/messages");
        let (connectivity, authentication) =
            probe_minimax_endpoint(&url, "fake-secret", "model").await;
        assert_eq!(connectivity.status, "passed");
        assert_eq!(authentication.status, "failed");
        assert!(!authentication.message.contains("fake-secret"));
        server.abort();
    }

    #[tokio::test]
    async fn probe_distinguishes_unreachable_server_from_rejected_credentials() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let (connectivity, authentication) = probe_minimax_endpoint(
            &format!("http://{address}/v1/messages"),
            "fake-secret",
            "model",
        )
        .await;
        assert_eq!(connectivity.status, "failed");
        assert_eq!(authentication.status, "unknown");
    }
}
