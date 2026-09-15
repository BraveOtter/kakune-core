//! CLI/client transport over the same public API used by the GUI.
use crate::ConnectionContext;
use reqwest::{Client, Method, Url};
use serde_json::Value;
use std::time::Duration;

pub struct RemoteClient {
    client: Client,
    base: Url,
    token: String,
}

impl RemoteClient {
    pub async fn connect(
        context: &ConnectionContext,
        token: String,
        ca: Option<&[u8]>,
    ) -> Result<Self, String> {
        let base = Url::parse(&context.endpoint).map_err(|_| "invalid Core endpoint")?;
        if !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
        {
            return Err(
                "Core endpoint must not contain credentials, a query or a fragment".to_string(),
            );
        }
        let local = base.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if base.scheme() != "https" && !(base.scheme() == "http" && local) {
            return Err("remote Core endpoints require HTTPS".to_string());
        }
        if token.trim().is_empty() {
            return Err(
                "a Core API token is required (KAKUNE_TOKEN or the context credential reference)"
                    .to_string(),
            );
        }
        let mut builder = Client::builder()
            .timeout(Duration::from_secs(120))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(ca) = ca {
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(ca).map_err(|_| "invalid Core CA certificate")?,
            );
        }
        let transport = Self {
            client: builder.build().map_err(|error| error.to_string())?,
            base,
            token,
        };
        let info = transport
            .request(Method::GET, &["info"], None, None)
            .await?;
        if context
            .expected_core_id
            .as_deref()
            .is_some_and(|id| info.get("coreId").and_then(Value::as_str) != Some(id))
        {
            return Err("Core identity differs from the selected context".to_string());
        }
        if info.get("apiVersion").and_then(Value::as_str) != Some(crate::API_VERSION) {
            return Err("Core API version is incompatible".to_string());
        }
        Ok(transport)
    }

    pub async fn request(
        &self,
        method: Method,
        path: &[&str],
        body: Option<Value>,
        revision: Option<&str>,
    ) -> Result<Value, String> {
        let mut url = self.base.clone();
        {
            let mut parts = url
                .path_segments_mut()
                .map_err(|_| "invalid Core base URL")?;
            parts.pop_if_empty().extend(["api", "v1"]).extend(path);
        }
        let mut request = self.client.request(method, url).bearer_auth(&self.token);
        if let Some(body) = body {
            request = request.json(&body);
        }
        if let Some(revision) = revision {
            request = request.header("If-Match", revision);
        }
        let mut response = request
            .send()
            .await
            .map_err(|error| format!("Core request failed: {error}"))?;
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
            if bytes.len() + chunk.len() > 16 * 1024 * 1024 {
                return Err("Core response exceeded 16 MiB".to_string());
            }
            bytes.extend_from_slice(&chunk);
        }
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).map_err(|_| "Core returned invalid JSON")?
        };
        if !status.is_success() {
            return Err(format!(
                "Core returned {status}: {}",
                value
                    .get("message")
                    .or_else(|| value.get("detail"))
                    .and_then(Value::as_str)
                    .unwrap_or("request rejected")
            ));
        }
        Ok(value)
    }

    pub async fn save_workflow(&self, source: &str) -> Result<Value, String> {
        let analysis = self
            .request(
                Method::POST,
                &["workflows", "analyze"],
                Some(serde_json::json!({"source":source})),
                None,
            )
            .await?;
        if analysis
            .get("diagnostics")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty())
        {
            return Err(format!(
                "workflow validation failed: {}",
                analysis["diagnostics"]
            ));
        }
        let id = analysis
            .pointer("/workflow/metadata/id")
            .and_then(Value::as_str)
            .ok_or("workflow has no ID")?;
        let workflows = self
            .request(Method::GET, &["workflows"], None, None)
            .await?;
        let exists = workflows["items"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item["id"].as_str() == Some(id)));
        if exists {
            let previous = self
                .request(Method::GET, &["workflows", id, "source"], None, None)
                .await?;
            let revision = previous["revision"]
                .as_str()
                .ok_or("Core did not return a workflow revision")?;
            self.request(
                Method::PUT,
                &["workflows", id, "source"],
                Some(serde_json::json!({"source":source})),
                Some(revision),
            )
            .await
        } else {
            self.request(
                Method::POST,
                &["workflows"],
                Some(serde_json::json!({"source":source})),
                None,
            )
            .await
        }
    }
}
