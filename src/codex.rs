//! Direct ChatGPT subscription OAuth and Codex Responses client.
//!
//! The endpoints used here match the ChatGPT Plus/Pro flow used by OpenCode.
//! They are not an OpenAI public API contract and may change independently.

use std::{
    fmt,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
    time::timeout,
};
use uuid::Uuid;

const CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const ISSUER: &str = "https://auth.openai.com";
const CODEX_RESPONSES_URL: &str = "https://chatgpt.com/backend-api/codex/responses";
const CALLBACK_ADDR: &str = "127.0.0.1:1455";
const CALLBACK_PATH: &str = "/auth/callback";
const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// OAuth credentials stored encrypted in Kakune's secret vault.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexOAuthTokens {
    pub refresh: String,
    pub access: String,
    pub expires_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
}

impl fmt::Debug for CodexOAuthTokens {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexOAuthTokens")
            .field("refresh", &"REDACTED")
            .field("access", &"REDACTED")
            .field("expires_at_ms", &self.expires_at_ms)
            .field("account_id", &self.account_id)
            .finish()
    }
}

impl CodexOAuthTokens {
    pub fn from_secret(value: &str) -> Result<Self, CodexError> {
        serde_json::from_str(value).map_err(CodexError::InvalidStoredTokens)
    }

    pub fn to_secret(&self) -> Result<String, CodexError> {
        serde_json::to_string(self).map_err(CodexError::EncodeTokens)
    }

    fn expired(&self) -> bool {
        self.expires_at_ms <= now_millis().saturating_add(30_000)
    }
}

/// A cancellation signal for a direct HTTP request.
#[derive(Clone, Debug, Default)]
pub struct CodexCancellation {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl CodexCancellation {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    async fn cancelled(&self) {
        while !self.cancelled.load(Ordering::Acquire) {
            self.notify.notified().await;
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CodexUsage {
    pub input_tokens: Option<u64>,
    pub cached_input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CodexTaskResult {
    pub text: String,
    pub usage: Option<CodexUsage>,
    pub raw: Value,
    /// Set when a refresh token exchange changed the encrypted token payload.
    #[serde(skip)]
    pub refreshed_tokens: Option<CodexOAuthTokens>,
}

#[derive(Clone, Debug)]
pub struct CodexClient {
    client: Client,
    model: String,
    timeout: Duration,
    tokens: CodexOAuthTokens,
}

impl CodexClient {
    pub fn new(
        model: String,
        timeout: Duration,
        tokens: CodexOAuthTokens,
    ) -> Result<Self, CodexError> {
        if model.trim().is_empty() || timeout.is_zero() {
            return Err(CodexError::InvalidRequest(
                "model and timeout must be non-empty".to_string(),
            ));
        }
        Ok(Self {
            client: Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(timeout)
                .build()
                .map_err(CodexError::HttpClient)?,
            model,
            timeout,
            tokens,
        })
    }

    pub async fn run(
        self,
        prompt: String,
        cancellation: &CodexCancellation,
    ) -> Result<CodexTaskResult, CodexError> {
        if prompt.trim().is_empty() {
            return Err(CodexError::InvalidRequest(
                "prompt must not be empty".to_string(),
            ));
        }
        self.run_request(
            json!({"input":[{"role":"user","content":[{"type":"input_text","text":prompt}]}]}),
            cancellation,
        )
        .await
    }

    pub async fn run_request(
        mut self,
        mut body: Value,
        cancellation: &CodexCancellation,
    ) -> Result<CodexTaskResult, CodexError> {
        let request = body
            .as_object_mut()
            .ok_or_else(|| CodexError::InvalidRequest("request must be an object".into()))?;
        request.insert("model".into(), json!(self.model));
        request.insert("store".into(), json!(false));
        request.insert("stream".into(), json!(true));
        request.insert("include".into(), json!(["reasoning.encrypted_content"]));
        if cancellation.cancelled.load(Ordering::Acquire) {
            return Err(CodexError::Cancelled);
        }
        let refreshed_tokens = if self.tokens.expired() {
            self.tokens = tokio::select! {
                result = timeout(self.timeout, refresh_tokens(&self.client, &self.tokens.refresh)) => result.map_err(|_| CodexError::Timeout(self.timeout))??,
                _ = cancellation.cancelled() => return Err(CodexError::Cancelled),
            };
            Some(self.tokens.clone())
        } else {
            None
        };
        let mut request = self
            .client
            .post(CODEX_RESPONSES_URL)
            .bearer_auth(&self.tokens.access)
            .header("originator", "kakune")
            .header("User-Agent", concat!("kakune/", env!("CARGO_PKG_VERSION")))
            .json(&body);
        if let Some(account_id) = &self.tokens.account_id {
            request = request.header("ChatGPT-Account-Id", account_id);
        }
        if let Some(residency) = jwt_claim(&self.tokens.access, "chatgpt_compute_residency")
            .or_else(|| {
                nested_jwt_claim(
                    &self.tokens.access,
                    "https://api.openai.com/auth",
                    "chatgpt_compute_residency",
                )
            })
            .filter(|value| value != "no_constraint")
        {
            request = request.header("x-openai-internal-codex-residency", residency);
        }
        let response = tokio::select! {
            response = timeout(self.timeout, request.send()) => response.map_err(|_| CodexError::Timeout(self.timeout))??,
            _ = cancellation.cancelled() => return Err(CodexError::Cancelled),
        };
        let status = response.status();
        let body = timeout(
            self.timeout,
            read_response_body(response, cancellation, MAX_RESPONSE_BYTES),
        )
        .await
        .map_err(|_| CodexError::Timeout(self.timeout))??;
        if !status.is_success() {
            return Err(CodexError::Response {
                status,
                body: bounded_message(&body),
            });
        }
        let (raw, text, usage) = parse_stream_response(&body)?;
        Ok(CodexTaskResult {
            text,
            usage,
            raw,
            refreshed_tokens,
        })
    }
}

fn parse_stream_response(body: &[u8]) -> Result<(Value, String, Option<CodexUsage>), CodexError> {
    let source = std::str::from_utf8(body).map_err(|error| {
        CodexError::InvalidResponse(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            error,
        )))
    })?;
    let mut events = Vec::new();
    let mut deltas = String::new();
    for line in source.lines() {
        let Some(data) = line.trim().strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data == "[DONE]" || data.is_empty() {
            continue;
        }
        let event: Value = serde_json::from_str(data).map_err(CodexError::InvalidResponse)?;
        if let Some(delta) = event.get("delta").and_then(Value::as_str) {
            deltas.push_str(delta);
        }
        events.push(event);
    }
    let raw = events
        .iter()
        .rev()
        .find(|event| event.get("type").and_then(Value::as_str) == Some("response.completed"))
        .and_then(|event| event.get("response"))
        .cloned()
        .ok_or_else(|| {
            CodexError::InvalidRequest("Codex stream ended without response.completed".into())
        })?;
    let has_calls = raw
        .get("output")
        .and_then(Value::as_array)
        .is_some_and(|items| items.iter().any(|item| item["type"] == "function_call"));
    let text = response_text(&raw)
        .or_else(|| (!deltas.is_empty()).then_some(deltas))
        .or_else(|| has_calls.then(String::new))
        .ok_or(CodexError::MissingText)?;
    let usage = response_usage(&raw);
    Ok((raw, text, usage))
}

/// Opens the ChatGPT authorization page and waits for the loopback callback.
pub async fn login_with_browser() -> Result<CodexOAuthTokens, CodexError> {
    let listener = TcpListener::bind(CALLBACK_ADDR)
        .await
        .map_err(CodexError::BindCallback)?;
    let verifier = format!("{}abcdefghijk", Uuid::new_v4().simple());
    let challenge = base64_url(&Sha256::digest(verifier.as_bytes()));
    let state = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let redirect_uri = format!("http://localhost:1455{CALLBACK_PATH}");
    let authorize_url = reqwest::Url::parse_with_params(
        &format!("{ISSUER}/oauth/authorize"),
        [
            ("response_type", "code"),
            ("client_id", CLIENT_ID),
            ("redirect_uri", redirect_uri.as_str()),
            ("scope", "openid profile email offline_access"),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("id_token_add_organizations", "true"),
            ("codex_cli_simplified_flow", "true"),
            ("state", state.as_str()),
            ("originator", "kakune"),
        ],
    )
    .map_err(|error| CodexError::AuthorizeUrl(error.to_string()))?;
    println!("Open this URL to authorize ChatGPT Plus/Pro:\n{authorize_url}");
    let _ = webbrowser::open(authorize_url.as_str());
    let callback = timeout(
        Duration::from_secs(300),
        receive_callback(&listener, &state),
    )
    .await
    .map_err(|_| CodexError::CallbackTimeout)??;
    exchange_code(
        &Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(CodexError::HttpClient)?,
        &callback,
        &redirect_uri,
        &verifier,
    )
    .await
}

async fn receive_callback(listener: &TcpListener, state: &str) -> Result<String, CodexError> {
    let (mut stream, peer) = listener
        .accept()
        .await
        .map_err(CodexError::AcceptCallback)?;
    if !is_loopback(peer) {
        return Err(CodexError::InvalidCallback(
            "callback peer is not loopback".to_string(),
        ));
    }
    let mut bytes = vec![0; 16 * 1024];
    let size = stream
        .read(&mut bytes)
        .await
        .map_err(CodexError::ReadCallback)?;
    let request = std::str::from_utf8(&bytes[..size])
        .map_err(|_| CodexError::InvalidCallback("callback is not UTF-8".to_string()))?;
    let target = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| {
            CodexError::InvalidCallback("callback request line is invalid".to_string())
        })?;
    let url = reqwest::Url::parse(&format!("http://localhost{target}"))
        .map_err(|error| CodexError::AuthorizeUrl(error.to_string()))?;
    let result = if url.path() != CALLBACK_PATH {
        Err(CodexError::InvalidCallback(
            "unexpected callback path".to_string(),
        ))
    } else if let Some(error) = url.query_pairs().find(|(key, _)| key == "error") {
        Err(CodexError::InvalidCallback(format!(
            "authorization failed: {}",
            error.1
        )))
    } else if url
        .query_pairs()
        .find(|(key, value)| key == "state" && value == state)
        .is_none()
    {
        Err(CodexError::InvalidCallback(
            "callback state did not match".to_string(),
        ))
    } else {
        url.query_pairs()
            .find(|(key, _)| key == "code")
            .map(|(_, value)| value.into_owned())
            .ok_or_else(|| {
                CodexError::InvalidCallback("callback did not include a code".to_string())
            })
    };
    let page = if result.is_ok() {
        "Authorization completed. You can return to Kakune."
    } else {
        "Authorization failed. You can return to Kakune."
    };
    stream
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{page}",
                page.len()
            )
            .as_bytes(),
        )
        .await
        .map_err(CodexError::WriteCallback)?;
    result
}

async fn exchange_code(
    client: &Client,
    code: &str,
    redirect_uri: &str,
    verifier: &str,
) -> Result<CodexOAuthTokens, CodexError> {
    let response = client
        .post(format!("{ISSUER}/oauth/token"))
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code),
            ("redirect_uri", redirect_uri),
            ("client_id", CLIENT_ID),
            ("code_verifier", verifier),
        ])
        .send()
        .await?;
    parse_token_response(response, None).await
}

async fn refresh_tokens(client: &Client, refresh: &str) -> Result<CodexOAuthTokens, CodexError> {
    let response = client
        .post(format!("{ISSUER}/oauth/token"))
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh),
            ("client_id", CLIENT_ID),
        ])
        .send()
        .await?;
    parse_token_response(response, Some(refresh)).await
}

async fn parse_token_response(
    response: reqwest::Response,
    previous_refresh: Option<&str>,
) -> Result<CodexOAuthTokens, CodexError> {
    let status = response.status();
    let body = read_response_body(response, &CodexCancellation::new(), 64 * 1024).await?;
    if !status.is_success() {
        return Err(CodexError::Response {
            status,
            body: bounded_message(&body),
        });
    }
    let mut tokens: TokenResponse =
        serde_json::from_slice(&body).map_err(CodexError::InvalidTokenResponse)?;
    if tokens.refresh_token.is_empty() {
        tokens.refresh_token = previous_refresh.unwrap_or_default().to_string();
    }
    if tokens.access_token.is_empty() || tokens.refresh_token.is_empty() {
        return Err(CodexError::InvalidTokenResponse(serde_json::Error::io(
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "token response omitted access or refresh token",
            ),
        )));
    }
    let account_id = tokens
        .id_token
        .as_deref()
        .and_then(account_id)
        .or_else(|| account_id(&tokens.access_token));
    Ok(CodexOAuthTokens {
        refresh: tokens.refresh_token,
        access: tokens.access_token,
        expires_at_ms: now_millis()
            .saturating_add(tokens.expires_in.unwrap_or(3600).saturating_mul(1000)),
        account_id,
    })
}

async fn read_response_body(
    response: reqwest::Response,
    cancellation: &CodexCancellation,
    limit: usize,
) -> Result<Vec<u8>, CodexError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(CodexError::ResponseTooLarge { limit });
    }
    let mut stream = response.bytes_stream();
    let mut body = Vec::new();
    loop {
        let chunk = tokio::select! {
            chunk = stream.next() => chunk,
            _ = cancellation.cancelled() => return Err(CodexError::Cancelled),
        };
        let Some(chunk) = chunk else {
            return Ok(body);
        };
        let chunk = chunk?;
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(CodexError::ResponseTooLarge { limit });
        }
        body.extend_from_slice(&chunk);
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: String,
    id_token: Option<String>,
    expires_in: Option<u64>,
}

fn response_text(value: &Value) -> Option<String> {
    value
        .get("output_text")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            let texts = value
                .get("output")?
                .as_array()?
                .iter()
                .flat_map(|item| {
                    item.get("content")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                })
                .filter_map(|content| content.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>();
            (!texts.is_empty()).then(|| texts.join("\n"))
        })
}

fn response_usage(value: &Value) -> Option<CodexUsage> {
    let usage = value.get("usage")?;
    let input_tokens = usage.get("input_tokens").and_then(Value::as_u64);
    let cached_input_tokens = usage
        .get("input_tokens_details")
        .and_then(|details| details.get("cached_tokens"))
        .and_then(Value::as_u64);
    let output_tokens = usage.get("output_tokens").and_then(Value::as_u64);
    (input_tokens.is_some() || cached_input_tokens.is_some() || output_tokens.is_some()).then_some(
        CodexUsage {
            input_tokens,
            cached_input_tokens,
            output_tokens,
        },
    )
}

fn account_id(token: &str) -> Option<String> {
    jwt_claim(token, "chatgpt_account_id")
        .or_else(|| nested_jwt_claim(token, "https://api.openai.com/auth", "chatgpt_account_id"))
        .or_else(|| {
            jwt_payload(token)?
                .get("organizations")?
                .as_array()?
                .first()?
                .get("id")?
                .as_str()
                .map(str::to_string)
        })
}

fn jwt_claim(token: &str, claim: &str) -> Option<String> {
    jwt_payload(token)?.get(claim)?.as_str().map(str::to_string)
}
fn nested_jwt_claim(token: &str, namespace: &str, claim: &str) -> Option<String> {
    jwt_payload(token)?
        .get(namespace)?
        .get(claim)?
        .as_str()
        .map(str::to_string)
}
fn jwt_payload(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    serde_json::from_slice(&base64_url_decode(payload)?).ok()
}
fn base64_url(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut output = String::new();
    for chunk in input.chunks(3) {
        let value = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        output.push(TABLE[((value >> 18) & 63) as usize] as char);
        output.push(TABLE[((value >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            output.push(TABLE[((value >> 6) & 63) as usize] as char);
        }
        if chunk.len() > 2 {
            output.push(TABLE[(value & 63) as usize] as char);
        }
    }
    output
}
fn base64_url_decode(input: &str) -> Option<Vec<u8>> {
    let mut value = 0u32;
    let mut bits = 0u8;
    let mut output = Vec::new();
    for byte in input.bytes() {
        let part = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        };
        value = (value << 6) | u32::from(part);
        bits += 6;
        while bits >= 8 {
            bits -= 8;
            output.push((value >> bits) as u8);
            value &= (1 << bits) - 1;
        }
    }
    Some(output)
}
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
fn is_loopback(peer: SocketAddr) -> bool {
    peer.ip().is_loopback()
}
fn bounded_message(bytes: &[u8]) -> String {
    String::from_utf8_lossy(&bytes[..bytes.len().min(4096)]).into_owned()
}

#[derive(Debug, Error)]
pub enum CodexError {
    #[error("invalid Codex request: {0}")]
    InvalidRequest(String),
    #[error("stored Codex OAuth tokens are invalid: {0}")]
    InvalidStoredTokens(#[source] serde_json::Error),
    #[error("cannot encode Codex OAuth tokens: {0}")]
    EncodeTokens(#[source] serde_json::Error),
    #[error("cannot create Codex HTTP client: {0}")]
    HttpClient(#[source] reqwest::Error),
    #[error("Codex OAuth/Responses request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Codex service returned {status}: {body}")]
    Response { status: StatusCode, body: String },
    #[error("Codex response is invalid JSON: {0}")]
    InvalidResponse(#[source] serde_json::Error),
    #[error("Codex token response is invalid: {0}")]
    InvalidTokenResponse(#[source] serde_json::Error),
    #[error("Codex response did not include text output")]
    MissingText,
    #[error("Codex response exceeds the {limit}-byte limit")]
    ResponseTooLarge { limit: usize },
    #[error("Codex request timed out after {0:?}")]
    Timeout(Duration),
    #[error("Codex request was cancelled")]
    Cancelled,
    #[error("cannot bind OAuth callback listener: {0}")]
    BindCallback(#[source] std::io::Error),
    #[error("cannot accept OAuth callback: {0}")]
    AcceptCallback(#[source] std::io::Error),
    #[error("cannot read OAuth callback: {0}")]
    ReadCallback(#[source] std::io::Error),
    #[error("cannot write OAuth callback: {0}")]
    WriteCallback(#[source] std::io::Error),
    #[error("OAuth callback was invalid: {0}")]
    InvalidCallback(String),
    #[error("cannot construct OAuth authorization URL: {0}")]
    AuthorizeUrl(String),
    #[error("OAuth callback timed out after five minutes")]
    CallbackTimeout,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_stream_is_not_a_successful_response() {
        let body = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\ndata: [DONE]\n";
        assert!(parse_stream_response(body).is_err());
        let failed =
            b"data: {\"type\":\"response.failed\",\"response\":{\"output_text\":\"partial\"}}\n";
        assert!(parse_stream_response(failed).is_err());
    }

    #[test]
    fn completed_tool_call_does_not_require_text() {
        let body = b"data: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"function_call\",\"name\":\"submit_result\",\"call_id\":\"c1\",\"arguments\":\"{}\"}]}}\n";
        let (raw, text, _) = parse_stream_response(body).unwrap();
        assert!(text.is_empty());
        assert_eq!(raw["output"][0]["call_id"], "c1");
    }

    #[tokio::test]
    async fn refresh_response_can_keep_the_previous_refresh_token() {
        let response = reqwest::Response::from(axum::http::Response::new(
            br#"{"access_token":"new-access","expires_in":3600}"#.to_vec(),
        ));
        let tokens = parse_token_response(response, Some("old-refresh"))
            .await
            .unwrap();
        assert_eq!(tokens.refresh, "old-refresh");
        assert_eq!(tokens.access, "new-access");
    }

    #[test]
    fn extracts_responses_text_and_usage() {
        let response = json!({"output":[{"content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":12,"input_tokens_details":{"cached_tokens":8},"output_tokens":3}});
        assert_eq!(response_text(&response).as_deref(), Some("hello"));
        assert_eq!(
            response_usage(&response)
                .expect("usage")
                .cached_input_tokens,
            Some(8)
        );
    }

    #[test]
    fn extracts_text_from_responses_sse() {
        let body = b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"content\":[{\"type\":\"output_text\",\"text\":\"hello\"}]}]}}\n\ndata: [DONE]\n";
        let (_, text, _) = parse_stream_response(body).expect("SSE response parses");
        assert_eq!(text, "hello");
    }

    #[test]
    fn token_debug_and_secret_encoding_do_not_expose_credentials() {
        let tokens = CodexOAuthTokens {
            refresh: "refresh-token-value".to_string(),
            access: "access-token-value".to_string(),
            expires_at_ms: 1,
            account_id: None,
        };
        let debug = format!("{tokens:?}");
        assert!(!debug.contains("refresh-token-value"));
        assert!(!debug.contains("access-token-value"));
        assert_eq!(
            CodexOAuthTokens::from_secret(&tokens.to_secret().expect("encode"))
                .expect("decode")
                .access,
            "access-token-value"
        );
    }

    #[tokio::test]
    #[ignore = "requires an explicitly configured ChatGPT OAuth token payload"]
    async fn live_responses_request_requires_explicit_credential() {
        let tokens = CodexOAuthTokens::from_secret(
            &std::env::var("KAKUNE_CODEX_OAUTH_TOKENS")
                .expect("set KAKUNE_CODEX_OAUTH_TOKENS to run this live test"),
        )
        .expect("OAuth token payload should be valid");
        let model = std::env::var("KAKUNE_CODEX_MODEL").unwrap_or_else(|_| "gpt-5".to_string());
        let result = CodexClient::new(model, Duration::from_secs(60), tokens)
            .expect("client should be valid")
            .run(
                "Reply with the word Kakune.".to_string(),
                &CodexCancellation::new(),
            )
            .await
            .expect("Codex should accept the documented responses request");
        assert!(!result.text.trim().is_empty());
    }
}
