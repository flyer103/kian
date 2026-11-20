//! The macOS bridge ABI client (CHANNEL-IMESSAGE §3.2, `protocol: 1`).
//!
//! The bridge is a separate daemon on the Messages Mac; this module is
//! the adapter's only knowledge of it: three routes (`GET /v1/health`,
//! `GET /v1/updates`, `POST /v1/send`), one bearer token, one versioned
//! JSON shape. Nothing here reaches Messages, `chat.db` or Apple: a
//! different bridge implementation (a hosted Mac, a VM) only has to
//! honour the ABI.
//!
//! Error text never carries the token: the token rides the
//! `Authorization` header, and every mapped error is built from the
//! response body or a fixed reason string — never from request
//! headers.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The ABI version this adapter speaks.
pub const PROTOCOL: u32 = 1;

/// The bridge's documented long-poll cap (`wait_ms ≤ 25000`).
pub const MAX_WAIT_MS: u64 = 25_000;

/// One message as the bridge reduces it (CHANNEL-IMESSAGE §3.2): the
/// bridge-owned shape, not the raw `imsg` object. Fields the phase-1
/// contract does not name are deliberately absent so the bridge's
/// third-party schema never becomes durable session data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BridgeMessage {
    /// `message.ROWID` — the physical scan position (per database
    /// generation; never a kian identity).
    pub rowid: u64,
    /// Apple's message GUID — the idempotency key.
    pub guid: String,
    #[serde(default)]
    pub chat_id: Option<i64>,
    #[serde(default)]
    pub chat_guid: Option<String>,
    #[serde(default)]
    pub chat_identifier: Option<String>,
    #[serde(default)]
    pub sender: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub created_at_ms: Option<u64>,
    #[serde(default)]
    pub service: Option<String>,
    #[serde(default)]
    pub is_from_me: bool,
    #[serde(default)]
    pub is_group: bool,
    #[serde(default)]
    pub is_reaction: bool,
}

/// One `/v1/updates` page.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UpdatesBatch {
    #[serde(default = "default_protocol")]
    pub protocol: u32,
    #[serde(default)]
    pub generation: String,
    #[serde(default)]
    pub messages: Vec<BridgeMessage>,
    /// The exclusive cursor to pass next.
    #[serde(default)]
    pub next_rowid: u64,
    #[serde(default)]
    pub has_more: bool,
    /// The requested cursor predates what the bridge can replay.
    #[serde(default)]
    pub gap: bool,
}

fn default_protocol() -> u32 {
    PROTOCOL
}

/// One `/v1/health` snapshot (the operator/diagnostic surface; the
/// receive loop does not need it).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Health {
    #[serde(default = "default_protocol")]
    pub protocol: u32,
    #[serde(default)]
    pub bridge_version: String,
    #[serde(default)]
    pub instance_id: String,
    #[serde(default)]
    pub imsg: ImsgInfo,
    #[serde(default)]
    pub database: DatabaseInfo,
    #[serde(default)]
    pub automation: String,
    #[serde(default)]
    pub watch: WatchInfo,
    #[serde(default)]
    pub counters: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
pub struct ImsgInfo {
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub rpc: bool,
    #[serde(default)]
    pub private_api: bool,
}

#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
pub struct DatabaseInfo {
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub generation: String,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
pub struct WatchInfo {
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub last_rowid: u64,
    #[serde(default)]
    pub buffered: u64,
    #[serde(default)]
    pub participants: Vec<String>,
}

/// One `/v1/send` request.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SendRequest {
    pub protocol: u32,
    /// The delivery idempotency key (deterministic per shaped
    /// delivery; CHANNEL-IMESSAGE §3.2).
    pub client_id: String,
    /// The allowlist key the adapter already validated.
    pub to: String,
    /// The inbound conversation, when known (thread/service
    /// continuity).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_guid: Option<String>,
    pub text: String,
    pub service: String,
}

impl SendRequest {
    #[must_use]
    pub fn new(
        client_id: impl Into<String>,
        to: impl Into<String>,
        chat_guid: Option<String>,
        text: impl Into<String>,
        service: impl Into<String>,
    ) -> Self {
        Self {
            protocol: PROTOCOL,
            client_id: client_id.into(),
            to: to.into(),
            chat_guid,
            text: text.into(),
            service: service.into(),
        }
    }
}

/// One `/v1/send` success.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SendOk {
    #[serde(default)]
    pub message_id: String,
    #[serde(default)]
    pub rowid: Option<u64>,
    #[serde(default)]
    pub guid: Option<String>,
    #[serde(default)]
    pub service: Option<String>,
    /// The transport accepted the message and an outgoing row was
    /// observed. `false` = accepted without local verification.
    #[serde(default)]
    pub verified: bool,
    /// A repeated `client_id` was served from the bridge's observed
    /// cache (the retry dedupe, CHANNEL-IMESSAGE §3.2).
    #[serde(default)]
    pub deduplicated: bool,
}

/// A `/v1/send` refusal body (`ok: false`).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SendErr {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub detail: String,
    #[serde(default)]
    pub retry_safe: bool,
}

/// Why a bridge call failed — pre-aligned with the seam's closed
/// vocabulary so classification never parses strings.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BridgeError {
    #[error(
        "imessage bridge unauthorized (check KIAN_IMESSAGE_BRIDGE_TOKEN against the bridge token_file)"
    )]
    Unauthorized,
    #[error("imessage bridge unreachable: {0}")]
    Unreachable(String),
    #[error("imessage bridge request timed out")]
    Timeout,
    #[error("imessage bridge database unreadable: {0}")]
    DatabaseUnreadable(String),
    /// A send refusal with the bridge's delivery disposition.
    #[error("imessage bridge refused the send ({kind}): {detail}")]
    SendRejected {
        kind: String,
        detail: String,
        retry_safe: bool,
    },
    #[error("imessage bridge response malformed: {0}")]
    Malformed(String),
}

/// The bridge ABI client. Cheap to clone (a `reqwest::Client` inside);
/// the token is never rendered by `Debug` because the struct has no
/// `Debug` derive (a leak would need an explicit field read).
#[derive(Clone)]
pub struct BridgeClient {
    http: reqwest::Client,
    base: String,
    token: String,
}

impl BridgeClient {
    /// Builds the client over an `http(s)://host[:port]` base.
    pub fn new(base: impl Into<String>, token: impl Into<String>) -> Result<Self, BridgeError> {
        let base = base.into().trim().trim_end_matches('/').to_string();
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| BridgeError::Unreachable(e.to_string()))?;
        Ok(Self {
            http,
            base,
            token: token.into(),
        })
    }

    /// `GET /v1/updates` — the exclusive-cursor long poll.
    pub async fn updates(
        &self,
        since_rowid: u64,
        limit: u32,
        wait_ms: u64,
        timeout: Duration,
    ) -> Result<UpdatesBatch, BridgeError> {
        let url = format!(
            "{}/v1/updates?since_rowid={since_rowid}&limit={limit}&wait_ms={wait_ms}",
            self.base
        );
        let response = self
            .http
            .get(url)
            .bearer_auth(&self.token)
            .timeout(timeout)
            .send()
            .await
            .map_err(map_reqwest)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(BridgeError::Unauthorized);
        }
        if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(BridgeError::DatabaseUnreadable(
                error_detail(response).await,
            ));
        }
        if !response.status().is_success() {
            return Err(BridgeError::Unreachable(format!(
                "bridge answered HTTP {}",
                response.status()
            )));
        }
        let batch: UpdatesBatch = response
            .json()
            .await
            .map_err(|e| BridgeError::Malformed(e.to_string()))?;
        if batch.protocol != PROTOCOL {
            return Err(BridgeError::Malformed(format!(
                "bridge protocol {} (this adapter speaks {PROTOCOL})",
                batch.protocol
            )));
        }
        Ok(batch)
    }

    /// `POST /v1/send` — one text message.
    pub async fn send(
        &self,
        request: &SendRequest,
        timeout: Duration,
    ) -> Result<SendOk, BridgeError> {
        let url = format!("{}/v1/send", self.base);
        let response = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .json(request)
            .timeout(timeout)
            .send()
            .await
            .map_err(map_reqwest)?;
        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            return Err(BridgeError::Unauthorized);
        }
        if status.is_success() {
            return response
                .json::<SendOk>()
                .await
                .map_err(|e| BridgeError::Malformed(e.to_string()));
        }
        if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            // The poisoned-child refusal has no send disposition body;
            // it is a plain unavailability (retry-safe by construction).
            return Err(BridgeError::Unreachable(format!(
                "bridge unavailable (HTTP {status})"
            )));
        }
        match response.json::<SendErr>().await {
            Ok(body) if !body.kind.is_empty() => Err(BridgeError::SendRejected {
                kind: body.kind,
                detail: body.detail,
                retry_safe: body.retry_safe,
            }),
            _ => Err(BridgeError::Malformed(format!(
                "send refusal HTTP {status} without a disposition body"
            ))),
        }
    }

    /// `GET /v1/health` — the preflight surface (`kian imessage check`).
    pub async fn health(&self, timeout: Duration) -> Result<Health, BridgeError> {
        let url = format!("{}/v1/health", self.base);
        let response = self
            .http
            .get(url)
            .bearer_auth(&self.token)
            .timeout(timeout)
            .send()
            .await
            .map_err(map_reqwest)?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(BridgeError::Unauthorized);
        }
        if !response.status().is_success() {
            return Err(BridgeError::Unreachable(format!(
                "bridge health answered HTTP {}",
                response.status()
            )));
        }
        let health: Health = response
            .json()
            .await
            .map_err(|e| BridgeError::Malformed(e.to_string()))?;
        if health.protocol != PROTOCOL {
            return Err(BridgeError::Malformed(format!(
                "bridge protocol {} (this adapter speaks {PROTOCOL})",
                health.protocol
            )));
        }
        Ok(health)
    }
}

/// Reads a refusal body's `detail` without ever rendering request
/// headers (the body is bridge-authored and redacted per §3.2).
async fn error_detail(response: reqwest::Response) -> String {
    match response.json::<SendErr>().await {
        Ok(body) if !body.detail.is_empty() => body.detail,
        _ => "bridge reports the Messages database is unreadable".to_string(),
    }
}

fn map_reqwest(error: reqwest::Error) -> BridgeError {
    if error.is_timeout() {
        BridgeError::Timeout
    } else {
        BridgeError::Unreachable(error.without_url().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_request_serializes_the_contract_shape() {
        let request = SendRequest::new("cid-1", "+14155551212", None, "hi", "auto");
        let json = serde_json::to_value(&request).expect("serialize");
        assert_eq!(json["protocol"], 1);
        assert_eq!(json["client_id"], "cid-1");
        assert_eq!(json["to"], "+14155551212");
        assert_eq!(json["service"], "auto");
        assert!(json.get("chat_guid").is_none(), "omitted when unknown");
    }

    #[test]
    fn updates_batch_accepts_the_documented_page_shape() {
        let raw = serde_json::json!({
            "protocol": 1,
            "generation": "gen-1",
            "messages": [{
                "rowid": 48211,
                "guid": "8DF1",
                "chat_id": 7,
                "chat_guid": "iMessage;-;+14155551212",
                "sender": "+14155551212",
                "text": "hi",
                "created_at_ms": 1794718315000u64,
                "service": "iMessage",
                "is_from_me": false,
                "is_group": false,
                "is_reaction": false
            }],
            "next_rowid": 48211,
            "has_more": false,
            "gap": false
        });
        let batch: UpdatesBatch = serde_json::from_value(raw).expect("parse");
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(batch.messages[0].guid, "8DF1");
        assert_eq!(batch.next_rowid, 48211);
    }

    #[test]
    fn send_refusal_carries_a_disposition() {
        let raw = serde_json::json!({
            "protocol": 1,
            "ok": false,
            "kind": "may_have_completed",
            "detail": "post-dispatch verification ambiguous",
            "retry_safe": false
        });
        let err: SendErr = serde_json::from_value(raw).expect("parse");
        assert_eq!(err.kind, "may_have_completed");
        assert!(!err.retry_safe);
    }

    #[test]
    fn health_defaults_keep_the_preflight_robust() {
        let health: Health = serde_json::from_value(serde_json::json!({
            "protocol": 1,
            "database": {"ready": true, "generation": "g"},
            "watch": {"participants": ["+1"]}
        }))
        .expect("parse");
        assert!(health.database.ready);
        assert_eq!(health.watch.participants, vec!["+1".to_string()]);
        assert!(!health.imsg.rpc);
    }
}
