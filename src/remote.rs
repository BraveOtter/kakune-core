//! CLI/client transport over the same public API used by the GUI.
use crate::ConnectionContext;
use reqwest::{Client, Method, Url};
use serde_json::Value;
use std::time::Duration;

use crate::management::{
    CONNECT_TIMEOUT_SECONDS, DEFAULT_REQUEST_BYTES, MAX_RESPONSE_BYTES, REQUEST_TIMEOUT_SECONDS,
    WORKFLOW_IMPORT_ENVELOPE_BYTES, WORKFLOW_IMPORT_REQUEST_BYTES, WORKFLOW_IMPORT_SOURCE_BYTES,
};

pub struct RemoteClient {
    client: Client,
    base: Url,
    token: String,
    capabilities: Vec<String>,
}

/// Canonical management transport. It is constructed only from an explicitly
/// resolved context and never falls back to legacy routes or another Core.
pub struct ManagementClient {
    transport: RemoteClient,
}

struct RequestOptions<'a> {
    request_limit: Option<usize>,
    query: &'a [(String, String)],
    allow_unsupported: bool,
}

impl RemoteClient {
    pub async fn connect(
        context: &ConnectionContext,
        token: String,
        ca: Option<&[u8]>,
    ) -> Result<Self, String> {
        let base = validate_endpoint(&context.endpoint)?;
        if token.trim().is_empty() {
            return Err(
                "a Core API token is required (KAKUNE_TOKEN or the context credential reference)"
                    .to_string(),
            );
        }
        let mut builder = Client::builder()
            .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECONDS))
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECONDS))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(ca) = ca {
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(ca).map_err(|_| "invalid Core CA certificate")?,
            );
        }
        let mut transport = Self {
            client: builder.build().map_err(|error| error.to_string())?,
            base,
            token,
            capabilities: Vec::new(),
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
        transport.capabilities = info
            .get("capabilities")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        Ok(transport)
    }

    pub async fn request(
        &self,
        method: Method,
        path: &[&str],
        body: Option<Value>,
        revision: Option<&str>,
    ) -> Result<Value, String> {
        self.request_at(&["api", "v1"], method, path, body, revision, None)
            .await
    }

    async fn request_at(
        &self,
        prefix: &[&str],
        method: Method,
        path: &[&str],
        body: Option<Value>,
        revision: Option<&str>,
        request_limit: Option<usize>,
    ) -> Result<Value, String> {
        self.request_at_with_query(
            prefix,
            method,
            path,
            body,
            revision,
            RequestOptions {
                request_limit,
                query: &[],
                allow_unsupported: false,
            },
        )
        .await
    }

    async fn request_at_with_query(
        &self,
        prefix: &[&str],
        method: Method,
        path: &[&str],
        body: Option<Value>,
        revision: Option<&str>,
        options: RequestOptions<'_>,
    ) -> Result<Value, String> {
        let mut url = self.base.clone();
        {
            let mut parts = url
                .path_segments_mut()
                .map_err(|_| "invalid Core base URL")?;
            parts.pop_if_empty().extend(prefix).extend(path);
        }
        if !options.query.is_empty() {
            url.query_pairs_mut().extend_pairs(
                options
                    .query
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            );
        }
        let mut request = self.client.request(method, url).bearer_auth(&self.token);
        if let Some(body) = body {
            let encoded =
                serde_json::to_vec(&body).map_err(|_| "management request could not be encoded")?;
            if options
                .request_limit
                .is_some_and(|limit| encoded.len() > limit)
            {
                return Err("management request exceeds its encoded route limit".to_owned());
            }
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(encoded);
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
            if chunk.len() > MAX_RESPONSE_BYTES.saturating_sub(bytes.len()) {
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
            if status == reqwest::StatusCode::NOT_IMPLEMENTED && options.allow_unsupported {
                return Ok(value);
            }
            if options.request_limit.is_some() {
                if status == reqwest::StatusCode::NOT_IMPLEMENTED {
                    return Err("the selected Core reports this integration capability as unsupported (HTTP 501)".to_owned());
                }
                return Err(format!("Core management request failed with HTTP {status}"));
            }
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

impl ManagementClient {
    pub async fn connect(
        context: &ConnectionContext,
        token: String,
        ca: Option<&[u8]>,
    ) -> Result<Self, String> {
        let transport = RemoteClient::connect(context, token, ca).await?;
        if !transport
            .capabilities
            .iter()
            .any(|capability| capability == "management-api")
        {
            return Err(
                "selected Core does not advertise the management API; upgrade that Core and retry"
                    .to_owned(),
            );
        }
        Ok(Self { transport })
    }

    pub async fn request(
        &self,
        method: Method,
        path: &[&str],
        body: Option<Value>,
        revision: Option<&str>,
    ) -> Result<Value, String> {
        let import = path == ["workflows", "import"];
        if let Some(value) = body.as_ref() {
            let limit = if import {
                WORKFLOW_IMPORT_REQUEST_BYTES
            } else {
                DEFAULT_REQUEST_BYTES
            };
            let encoded =
                serde_json::to_vec(value).map_err(|_| "management request could not be encoded")?;
            if encoded.len() > limit {
                return Err("management request exceeds its encoded route limit".to_owned());
            }
            if import {
                if let Some(source) = value.get("source").and_then(Value::as_str)
                    && source.len() > WORKFLOW_IMPORT_SOURCE_BYTES
                {
                    return Err("workflow source exceeds the 1 MiB UTF-8 limit".to_owned());
                }
                let mut envelope = value.clone();
                if let Some(object) = envelope.as_object_mut() {
                    object.remove("source");
                }
                if serde_json::to_vec(&envelope).map_or(true, |encoded| {
                    encoded.len() > WORKFLOW_IMPORT_ENVELOPE_BYTES
                }) {
                    return Err("workflow import envelope exceeds the 64 KiB limit".to_owned());
                }
            }
        }
        self.transport
            .request_at(
                &["api", "v1", "manage"],
                method,
                path,
                body,
                revision,
                Some(if import {
                    WORKFLOW_IMPORT_REQUEST_BYTES
                } else {
                    DEFAULT_REQUEST_BYTES
                }),
            )
            .await
    }

    pub async fn request_with_query(
        &self,
        method: Method,
        path: &[&str],
        query: &[(String, String)],
    ) -> Result<Value, String> {
        self.transport
            .request_at_with_query(
                &["api", "v1", "manage"],
                method,
                path,
                None,
                None,
                RequestOptions {
                    request_limit: Some(DEFAULT_REQUEST_BYTES),
                    query,
                    allow_unsupported: false,
                },
            )
            .await
    }

    /// Returns an explicit unsupported (501) capability payload to callers
    /// that need to display separately labeled configured metadata.
    pub async fn request_preserving_unsupported(
        &self,
        method: Method,
        path: &[&str],
    ) -> Result<Value, String> {
        self.transport
            .request_at_with_query(
                &["api", "v1", "manage"],
                method,
                path,
                None,
                None,
                RequestOptions {
                    request_limit: Some(DEFAULT_REQUEST_BYTES),
                    query: &[],
                    allow_unsupported: true,
                },
            )
            .await
    }
}

fn validate_endpoint(endpoint: &str) -> Result<Url, String> {
    let base = Url::parse(endpoint).map_err(|_| "invalid Core endpoint")?;
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
    Ok(base)
}
