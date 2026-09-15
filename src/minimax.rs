//! Minimal MiniMax Token Plan client using its Anthropic-compatible API.
//!
//! A subscription key is supplied to [`MiniMaxClient::new`] or
//! [`MiniMaxClient::with_config`] for the lifetime of the client only. This
//! module neither persists nor logs the key.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures_util::StreamExt;
use reqwest::{
    Client, StatusCode, Url,
    header::{CONTENT_TYPE, HeaderMap, HeaderValue, RETRY_AFTER},
    redirect::Policy,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Default base URL for MiniMax's Anthropic-compatible API.
pub const DEFAULT_BASE_URL: &str = "https://api.minimax.io/anthropic";

/// Token Plan quota endpoint published by MiniMax.
pub const TOKEN_PLAN_REMAINS_URL: &str = "https://www.minimax.io/v1/token_plan/remains";

const MAX_CONFIGURED_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONFIGURED_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_ERROR_MESSAGE_BYTES: usize = 4096;

/// Resource limits for one provider request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MiniMaxLimits {
    request_body_bytes: usize,
    response_body_bytes: usize,
    timeout: Duration,
}

impl MiniMaxLimits {
    /// Creates bounded request limits.
    pub fn new(
        request_body_bytes: usize,
        response_body_bytes: usize,
        timeout: Duration,
    ) -> Result<Self, MiniMaxError> {
        if request_body_bytes == 0 || request_body_bytes > MAX_CONFIGURED_BODY_BYTES {
            return Err(MiniMaxError::InvalidLimits);
        }
        if response_body_bytes == 0 || response_body_bytes > MAX_CONFIGURED_BODY_BYTES {
            return Err(MiniMaxError::InvalidLimits);
        }
        if timeout.is_zero() || timeout > MAX_CONFIGURED_TIMEOUT {
            return Err(MiniMaxError::InvalidLimits);
        }
        Ok(Self {
            request_body_bytes,
            response_body_bytes,
            timeout,
        })
    }

    pub fn request_body_bytes(&self) -> usize {
        self.request_body_bytes
    }

    pub fn response_body_bytes(&self) -> usize {
        self.response_body_bytes
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

impl Default for MiniMaxLimits {
    fn default() -> Self {
        Self {
            request_body_bytes: 1024 * 1024,
            response_body_bytes: 1024 * 1024,
            timeout: Duration::from_secs(30),
        }
    }
}

/// Non-secret MiniMax client configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MiniMaxConfig {
    base_url: String,
    limits: MiniMaxLimits,
}

impl MiniMaxConfig {
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    pub fn with_limits(mut self, limits: MiniMaxLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn limits(&self) -> &MiniMaxLimits {
        &self.limits
    }
}

impl Default for MiniMaxConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            limits: MiniMaxLimits::default(),
        }
    }
}

/// Anthropic message roles accepted by MiniMax.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MessageRole {
    User,
    Assistant,
}

/// Message content as either Anthropic shorthand text or complete content blocks.
///
/// Block values are intentionally retained as JSON so tool-use and future
/// provider blocks can be sent again without losing provider-defined fields.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub enum MessageContent {
    Text(String),
    Blocks(Vec<Value>),
}

/// A conversation message submitted to `/v1/messages`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Message {
    pub role: MessageRole,
    pub content: MessageContent,
}

/// An Anthropic-compatible messages request.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MessagesRequest {
    pub model: String,
    pub max_tokens: u32,
    pub messages: Vec<Message>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<MessageContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_sequences: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
}

impl MessagesRequest {
    pub fn new(model: impl Into<String>, max_tokens: u32, messages: Vec<Message>) -> Self {
        Self {
            model: model.into(),
            max_tokens,
            messages,
            system: None,
            temperature: None,
            top_p: None,
            stop_sequences: None,
            tools: None,
            tool_choice: None,
            stream: None,
        }
    }
}

/// Usage reported by MiniMax, when the response includes it.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
    pub cache_read_input_tokens: Option<u64>,
}

/// A non-streaming messages response. `raw` preserves all provider content blocks.
#[derive(Clone, Debug, PartialEq)]
pub struct MessagesResponse {
    pub id: Option<String>,
    pub model: Option<String>,
    pub role: Option<MessageRole>,
    pub content: Vec<Value>,
    pub stop_reason: Option<String>,
    pub usage: Option<Usage>,
    pub raw: Value,
}

/// The unnormalized Token Plan quota response.
#[derive(Clone, Debug, PartialEq)]
pub struct TokenPlanRemainsResponse {
    pub raw: Value,
}

/// A complete server-sent event returned by MiniMax. The data is retained
/// verbatim because event schemas vary by model and provider release.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamEvent {
    pub event: Option<String>,
    pub data: Value,
    pub raw_data: String,
}

/// Errors returned while constructing or executing a MiniMax request.
#[derive(Debug, Error)]
pub enum MiniMaxError {
    #[error("MiniMax subscription key must not be empty or contain control characters")]
    InvalidSubscriptionKey,
    #[error(
        "MiniMax base URL must be an absolute HTTPS URL without credentials, query, or fragment"
    )]
    InvalidBaseUrl,
    #[error("MiniMax request limits must be non-zero and no greater than 16 MiB or 120 seconds")]
    InvalidLimits,
    #[error(
        "MiniMax messages requests require a model, a positive max_tokens value, and at least one message"
    )]
    InvalidMessagesRequest,
    #[error("MiniMax request body is {actual} bytes, exceeding the {limit}-byte limit")]
    RequestTooLarge { actual: usize, limit: usize },
    #[error("MiniMax response body exceeds the {limit}-byte limit")]
    ResponseTooLarge { limit: usize },
    #[error("MiniMax HTTP request could not be constructed")]
    RequestBuild(#[source] reqwest::Error),
    #[error("MiniMax HTTP request failed")]
    Transport(#[source] reqwest::Error),
    #[error("MiniMax request was cancelled")]
    Cancelled,
    #[error("MiniMax rate limited the request{retry_after:?}")]
    RateLimited {
        retry_after: Option<u64>,
        message: Option<String>,
    },
    #[error("MiniMax reports Token Plan quota is exhausted: {message:?}")]
    QuotaExceeded { message: Option<String> },
    #[error("MiniMax returned HTTP {status}")]
    Http {
        status: u16,
        error_type: Option<String>,
        message: Option<String>,
    },
    #[error("MiniMax returned an invalid JSON response: {0}")]
    InvalidResponse(String),
}

/// Client for the MiniMax Token Plan APIs.
///
/// This type deliberately does not implement `Debug`, preventing accidental
/// logging of its in-memory subscription key.
pub struct MiniMaxClient {
    http: Client,
    base_url: Url,
    subscription_key: String,
    limits: MiniMaxLimits,
}

impl MiniMaxClient {
    pub fn new(subscription_key: impl Into<String>) -> Result<Self, MiniMaxError> {
        Self::with_config(subscription_key, MiniMaxConfig::default())
    }

    pub fn with_config(
        subscription_key: impl Into<String>,
        config: MiniMaxConfig,
    ) -> Result<Self, MiniMaxError> {
        let subscription_key = subscription_key.into();
        if subscription_key.is_empty() || subscription_key.chars().any(char::is_control) {
            return Err(MiniMaxError::InvalidSubscriptionKey);
        }
        let base_url = parse_base_url(&config.base_url)?;
        let limits = MiniMaxLimits::new(
            config.limits.request_body_bytes,
            config.limits.response_body_bytes,
            config.limits.timeout,
        )?;
        let http = Client::builder()
            .connect_timeout(limits.timeout)
            .timeout(limits.timeout)
            .redirect(Policy::none())
            .build()
            .map_err(MiniMaxError::RequestBuild)?;
        Ok(Self {
            http,
            base_url,
            subscription_key,
            limits,
        })
    }

    /// Sends a non-streaming Anthropic-compatible messages request.
    pub async fn messages(
        &self,
        request: &MessagesRequest,
    ) -> Result<MessagesResponse, MiniMaxError> {
        validate_messages_request(request)?;
        let body = serialize_request(request, self.limits.request_body_bytes)?;
        let response = self
            .http
            .execute(self.messages_http_request(body)?)
            .await
            .map_err(MiniMaxError::Transport)?;
        let (status, headers, body) =
            read_response_body(response, self.limits.response_body_bytes).await?;
        if !status.is_success() {
            return Err(parse_http_error(status, &headers, &body));
        }
        parse_messages_response(&body)
    }

    /// Cancels the HTTP future promptly when Core marks the execution cancelled.
    /// Dropping reqwest's future closes the local connection; it cannot promise
    /// that the remote provider has already stopped billable computation.
    pub async fn messages_cancellable(
        &self,
        request: &MessagesRequest,
        cancelled: Arc<AtomicBool>,
    ) -> Result<MessagesResponse, MiniMaxError> {
        tokio::select! {
            response = self.messages(request) => response,
            () = async {
                while !cancelled.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            } => Err(MiniMaxError::Cancelled),
        }
    }

    /// Sends a streaming request and returns each fully received SSE event.
    /// Consumers can expose events as they arrive by using this isolated API;
    /// Core also persists the final response rather than synthesising tokens.
    pub async fn stream_messages(
        &self,
        request: &MessagesRequest,
    ) -> Result<Vec<StreamEvent>, MiniMaxError> {
        let mut request = request.clone();
        request.stream = Some(true);
        validate_messages_request(&request)?;
        let body = serialize_request(&request, self.limits.request_body_bytes)?;
        let response = self
            .http
            .execute(self.messages_http_request(body)?)
            .await
            .map_err(MiniMaxError::Transport)?;
        let (status, headers, body) =
            read_response_body(response, self.limits.response_body_bytes).await?;
        if !status.is_success() {
            return Err(parse_http_error(status, &headers, &body));
        }
        parse_sse_events(&body)
    }

    fn messages_http_request(&self, body: Vec<u8>) -> Result<reqwest::Request, MiniMaxError> {
        self.http
            .post(endpoint(&self.base_url, &["v1", "messages"]))
            .bearer_auth(&self.subscription_key)
            .header("anthropic-version", "2023-06-01")
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .body(body)
            .build()
            .map_err(MiniMaxError::RequestBuild)
    }

    /// Fetches the Token Plan quota without imposing an undocumented schema.
    pub async fn token_plan_remains(&self) -> Result<TokenPlanRemainsResponse, MiniMaxError> {
        let response = self
            .http
            .get(TOKEN_PLAN_REMAINS_URL)
            .bearer_auth(&self.subscription_key)
            .send()
            .await
            .map_err(MiniMaxError::Transport)?;
        let (status, headers, body) =
            read_response_body(response, self.limits.response_body_bytes).await?;
        if !status.is_success() {
            return Err(parse_http_error(status, &headers, &body));
        }
        let raw = serde_json::from_slice(&body)
            .map_err(|error| MiniMaxError::InvalidResponse(error.to_string()))?;
        Ok(TokenPlanRemainsResponse { raw })
    }
}

fn parse_base_url(value: &str) -> Result<Url, MiniMaxError> {
    let url = Url::parse(value).map_err(|_| MiniMaxError::InvalidBaseUrl)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(MiniMaxError::InvalidBaseUrl);
    }
    Ok(url)
}

fn endpoint(base_url: &Url, path: &[&str]) -> Url {
    let mut url = base_url.clone();
    let mut segments = url
        .path_segments_mut()
        .expect("validated absolute HTTP URLs have path segments");
    segments.pop_if_empty();
    segments.extend(path);
    drop(segments);
    url
}

fn validate_messages_request(request: &MessagesRequest) -> Result<(), MiniMaxError> {
    if request.model.trim().is_empty() || request.max_tokens == 0 || request.messages.is_empty() {
        return Err(MiniMaxError::InvalidMessagesRequest);
    }
    Ok(())
}

fn serialize_request<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>, MiniMaxError> {
    let body = serde_json::to_vec(value)
        .map_err(|error| MiniMaxError::InvalidResponse(error.to_string()))?;
    if body.len() > limit {
        return Err(MiniMaxError::RequestTooLarge {
            actual: body.len(),
            limit,
        });
    }
    Ok(body)
}

async fn read_response_body(
    response: reqwest::Response,
    limit: usize,
) -> Result<(StatusCode, HeaderMap, Vec<u8>), MiniMaxError> {
    let status = response.status();
    let headers = response.headers().clone();
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(MiniMaxError::ResponseTooLarge { limit });
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(MiniMaxError::Transport)?;
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(MiniMaxError::ResponseTooLarge { limit });
        }
        body.extend_from_slice(&chunk);
    }
    Ok((status, headers, body))
}

fn parse_http_error(status: StatusCode, headers: &HeaderMap, body: &[u8]) -> MiniMaxError {
    let parsed = serde_json::from_slice::<Value>(body).ok();
    let error = parsed
        .as_ref()
        .and_then(|value| value.get("error"))
        .and_then(Value::as_object);
    let base_response = parsed
        .as_ref()
        .and_then(|value| value.get("base_resp"))
        .and_then(Value::as_object);
    let error_type = error
        .and_then(|value| value.get("type"))
        .or_else(|| parsed.as_ref().and_then(|value| value.get("type")))
        .or_else(|| base_response.and_then(|value| value.get("status_code")))
        .and_then(value_as_string)
        .map(truncate_error_message);
    let message = error
        .and_then(|value| value.get("message"))
        .or_else(|| parsed.as_ref().and_then(|value| value.get("message")))
        .or_else(|| base_response.and_then(|value| value.get("status_msg")))
        .and_then(Value::as_str)
        .map(truncate_error_message);
    if status == StatusCode::TOO_MANY_REQUESTS {
        return MiniMaxError::RateLimited {
            retry_after: headers
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok()),
            message,
        };
    }
    if matches!(
        error_type.as_deref(),
        Some("insufficient_quota" | "quota_exceeded" | "token_plan_exhausted")
    ) {
        return MiniMaxError::QuotaExceeded { message };
    }
    MiniMaxError::Http {
        status: status.as_u16(),
        error_type,
        message,
    }
}

fn parse_sse_events(body: &[u8]) -> Result<Vec<StreamEvent>, MiniMaxError> {
    let text = std::str::from_utf8(body)
        .map_err(|error| MiniMaxError::InvalidResponse(format!("stream is not UTF-8: {error}")))?;
    let mut events = Vec::new();
    for frame in text.replace("\r\n", "\n").split("\n\n") {
        let mut event = None;
        let mut data = Vec::new();
        for line in frame.lines() {
            if let Some(value) = line.strip_prefix("event:") {
                event = Some(value.trim().to_string());
            } else if let Some(value) = line.strip_prefix("data:") {
                data.push(value.trim_start());
            }
        }
        if data.is_empty() {
            continue;
        }
        let raw_data = data.join("\n");
        let data = if raw_data == "[DONE]" {
            Value::String(raw_data.clone())
        } else {
            serde_json::from_str(&raw_data).map_err(|error| {
                MiniMaxError::InvalidResponse(format!("invalid SSE data: {error}"))
            })?
        };
        events.push(StreamEvent {
            event,
            data,
            raw_data,
        });
    }
    Ok(events)
}

fn value_as_string(value: &Value) -> Option<&str> {
    value.as_str()
}

fn truncate_error_message(value: &str) -> String {
    if value.len() <= MAX_ERROR_MESSAGE_BYTES {
        return value.to_string();
    }
    let mut end = MAX_ERROR_MESSAGE_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

fn parse_messages_response(body: &[u8]) -> Result<MessagesResponse, MiniMaxError> {
    let raw: Value = serde_json::from_slice(body)
        .map_err(|error| MiniMaxError::InvalidResponse(error.to_string()))?;
    let object = raw.as_object().ok_or_else(|| {
        MiniMaxError::InvalidResponse("response must be a JSON object".to_string())
    })?;
    let content = object
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            MiniMaxError::InvalidResponse("response content must be an array".to_string())
        })?
        .clone();
    Ok(MessagesResponse {
        id: object
            .get("id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        model: object
            .get("model")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        role: object
            .get("role")
            .and_then(Value::as_str)
            .and_then(|value| serde_json::from_value(Value::String(value.to_string())).ok()),
        content,
        stop_reason: object
            .get("stop_reason")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        usage: object.get("usage").and_then(extract_usage),
        raw,
    })
}

fn extract_usage(value: &Value) -> Option<Usage> {
    let usage = value.as_object()?;
    Some(Usage {
        input_tokens: usage.get("input_tokens").and_then(Value::as_u64),
        output_tokens: usage.get("output_tokens").and_then(Value::as_u64),
        total_tokens: usage.get("total_tokens").and_then(Value::as_u64),
        cache_creation_input_tokens: usage
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64),
        cache_read_input_tokens: usage.get("cache_read_input_tokens").and_then(Value::as_u64),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request() -> MessagesRequest {
        MessagesRequest::new(
            "MiniMax-M3",
            128,
            vec![Message {
                role: MessageRole::User,
                content: MessageContent::Text("Hello".to_string()),
            }],
        )
    }

    #[test]
    fn serializes_anthropic_messages_request() {
        let mut request = request();
        request.system = Some(MessageContent::Text("Be concise".to_string()));
        request.tools = Some(vec![json!({
            "name": "weather",
            "input_schema": { "type": "object" }
        })]);

        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({
                "model": "MiniMax-M3",
                "max_tokens": 128,
                "messages": [{ "role": "user", "content": "Hello" }],
                "system": "Be concise",
                "tools": [{
                    "name": "weather",
                    "input_schema": { "type": "object" }
                }]
            })
        );
    }

    #[test]
    fn builds_the_default_messages_endpoint() {
        let url = parse_base_url(DEFAULT_BASE_URL).unwrap();
        assert_eq!(
            endpoint(&url, &["v1", "messages"]).as_str(),
            "https://api.minimax.io/anthropic/v1/messages"
        );
        assert_eq!(
            TOKEN_PLAN_REMAINS_URL,
            "https://www.minimax.io/v1/token_plan/remains"
        );
    }

    #[test]
    fn builds_an_authenticated_anthropic_request_without_networking() {
        let client = MiniMaxClient::new("test-subscription-key").unwrap();
        let wire_request =
            client.messages_http_request(serialize_request(&request(), 1024).unwrap());
        let wire_request = wire_request.unwrap();

        assert_eq!(wire_request.method(), reqwest::Method::POST);
        assert_eq!(
            wire_request.url().as_str(),
            "https://api.minimax.io/anthropic/v1/messages"
        );
        assert_eq!(
            wire_request.headers().get("authorization").unwrap(),
            "Bearer test-subscription-key"
        );
        assert_eq!(
            wire_request.headers().get("anthropic-version").unwrap(),
            "2023-06-01"
        );
        assert_eq!(
            wire_request.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }

    #[test]
    fn rejects_insecure_or_ambiguous_base_urls() {
        for url in [
            "http://api.minimax.io/anthropic",
            "https://key@example.test/anthropic",
            "https://example.test/anthropic?redirect=true",
            "https://example.test/anthropic#fragment",
        ] {
            assert!(matches!(
                parse_base_url(url),
                Err(MiniMaxError::InvalidBaseUrl)
            ));
        }
    }

    #[test]
    fn enforces_request_size_limit_before_networking() {
        let error = serialize_request(&request(), 1).unwrap_err();
        assert!(matches!(
            error,
            MiniMaxError::RequestTooLarge { limit: 1, .. }
        ));
    }

    #[test]
    fn parses_anthropic_error_bodies() {
        let error = parse_http_error(
            StatusCode::TOO_MANY_REQUESTS,
            &HeaderMap::new(),
            br#"{"type":"error","error":{"type":"rate_limit_error","message":"try again later"}}"#,
        );
        match error {
            MiniMaxError::RateLimited {
                retry_after,
                message,
            } => {
                assert_eq!(retry_after, None);
                assert_eq!(message.as_deref(), Some("try again later"));
            }
            _ => panic!("expected a rate limit error"),
        }
    }

    #[test]
    fn extracts_reported_usage_and_preserves_content_blocks() {
        let response = parse_messages_response(
            br#"{
                "id":"msg_1",
                "model":"MiniMax-M3",
                "role":"assistant",
                "content":[{"type":"text","text":"Hello"},{"type":"tool_use","id":"tool_1"}],
                "usage":{"input_tokens":12,"output_tokens":4,"cache_read_input_tokens":3}
            }"#,
        )
        .unwrap();
        assert_eq!(response.content.len(), 2);
        let usage = response.usage.as_ref().unwrap();
        assert_eq!(usage.input_tokens, Some(12));
        assert_eq!(usage.output_tokens, Some(4));
    }

    #[test]
    fn parses_sse_events_without_inventing_provider_fields() {
        let events = parse_sse_events(b"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"Hi\"}}\n\ndata: [DONE]\n\n").unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event.as_deref(), Some("content_block_delta"));
        assert_eq!(events[0].data["delta"]["text"], "Hi");
        assert_eq!(events[1].data, Value::String("[DONE]".to_string()));
    }

    #[test]
    fn classifies_quota_and_preserves_retry_after() {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_static("12"));
        assert!(matches!(
            parse_http_error(
                StatusCode::TOO_MANY_REQUESTS,
                &headers,
                br#"{"error":{"message":"slow down"}}"#
            ),
            MiniMaxError::RateLimited {
                retry_after: Some(12),
                ..
            }
        ));
        assert!(matches!(
            parse_http_error(
                StatusCode::FORBIDDEN,
                &HeaderMap::new(),
                br#"{"error":{"type":"insufficient_quota","message":"empty"}}"#
            ),
            MiniMaxError::QuotaExceeded { .. }
        ));
    }

    #[tokio::test]
    #[ignore = "requires an explicitly configured MiniMax Token Plan Subscription Key"]
    async fn live_messages_request_requires_explicit_credential() {
        let key = std::env::var("KAKUNE_MINIMAX_SUBSCRIPTION_KEY")
            .expect("set KAKUNE_MINIMAX_SUBSCRIPTION_KEY to run this live test");
        let response = MiniMaxClient::new(key)
            .expect("credential should be valid")
            .messages(&request())
            .await
            .expect("MiniMax should accept the documented messages request");
        assert!(!response.content.is_empty());
    }
}
