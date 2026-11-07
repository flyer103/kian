//! Typed surface v1 (card C46) — one typed method per frozen C13/C14
//! wire method, mirroring the server's dispatch exactly:
//!
//! - control plane (`POST /rpc`): `create_session`, `submit`, `steer`,
//!   `queue`, `interrupt`, `projection`;
//! - ops plane (`POST /ops/rpc`): `wake_now`, `recovery_budget_reset`,
//!   `mock_channel_send`.
//!
//! The SSE `events` helper lives in [`crate::sse`]. Every method here
//! round-trips against a scripted server with golden request bytes in
//! `tests/fixtures/` — the C8 serialization-freeze discipline applied
//! to the client.

use serde::Deserialize;
use serde_json::{Value, json};

use crate::error::ClientError;
use crate::transport::KianClient;

/// `create_session` → `{"session": "<uuid>"}`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CreateSessionResult {
    pub session: String,
}

/// `submit`/`steer`/`queue` → `{"deduplicated": bool}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct InputAck {
    pub deduplicated: bool,
}

/// `interrupt` → `{"outcome": "<server-worded outcome>"}`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct InterruptResult {
    pub outcome: String,
}

/// `projection` → `{"state": <projection JSON>}`. The state is the
/// server's projection object; the SDK keeps it as parsed JSON because
/// the projection shape belongs to the server's version, not the wire
/// method.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ProjectionResult {
    pub state: Value,
}

/// `wake_now` outcome: a drain was spawned, or the wake folded onto an
/// in-flight drain's pendingWake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeOutcome {
    Started,
    Folded,
}

/// `recovery_budget_reset` → where the `recovery/budget-reset` event
/// landed (`{"seq", "epoch", "ts"}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct ResetReceipt {
    pub seq: u64,
    pub epoch: u64,
    pub ts: u64,
}

/// `mock_channel_send` → `{"event_id", "channel", "topic"}` (C27 demo /
/// S4 injection).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct MockSendReceipt {
    pub event_id: String,
    pub channel: String,
    pub topic: Option<String>,
}

impl KianClient {
    /// Control plane: create a session (`POST /rpc`, no params).
    pub async fn create_session(&self) -> Result<CreateSessionResult, ClientError> {
        let result = self.rpc("create_session", json!({})).await?;
        Self::parse(result)
    }

    /// Control plane: submit a work input for a session.
    pub async fn submit(
        &self,
        session: &str,
        content: Value,
        idempotency_key: Option<&str>,
    ) -> Result<InputAck, ClientError> {
        self.input_method("submit", session, content, idempotency_key)
            .await
    }

    /// Control plane: steer a running session (injected at the next
    /// provider-turn safe boundary).
    pub async fn steer(
        &self,
        session: &str,
        content: Value,
        idempotency_key: Option<&str>,
    ) -> Result<InputAck, ClientError> {
        self.input_method("steer", session, content, idempotency_key)
            .await
    }

    /// Control plane: queue an input for a busy session (promoted at
    /// the safe boundary, never interrupts the current turn).
    pub async fn queue(
        &self,
        session: &str,
        content: Value,
        idempotency_key: Option<&str>,
    ) -> Result<InputAck, ClientError> {
        self.input_method("queue", session, content, idempotency_key)
            .await
    }

    async fn input_method(
        &self,
        method: &str,
        session: &str,
        content: Value,
        idempotency_key: Option<&str>,
    ) -> Result<InputAck, ClientError> {
        let mut params = json!({"session": session, "content": content});
        if let Some(key) = idempotency_key {
            params["idempotency_key"] = json!(key);
        }
        let result = self.rpc(method, params).await?;
        Self::parse(result)
    }

    /// Control plane: request an interrupt; the outcome string is the
    /// server's worded receipt. The intent is durable (C68/ADR-014):
    /// whenever work may still be in flight the server also admits a
    /// control-envelope inbox row that the owning drain consumes into a
    /// `control/interrupt` event — the interrupt survives a drain crash
    /// and crosses replicas. No new wire method.
    pub async fn interrupt(&self, session: &str) -> Result<InterruptResult, ClientError> {
        let result = self.rpc("interrupt", json!({"session": session})).await?;
        Self::parse(result)
    }

    /// Control plane: read the session's projection.
    pub async fn projection(&self, session: &str) -> Result<ProjectionResult, ClientError> {
        let result = self.rpc("projection", json!({"session": session})).await?;
        Self::parse(result)
    }

    /// Ops plane: trigger a wake for the session (idempotent; folds
    /// onto pendingWake when a drain is in flight).
    pub async fn wake_now(&self, session: &str) -> Result<WakeOutcome, ClientError> {
        let result = self.ops("wake_now", json!({"session": session})).await?;
        let outcome = result
            .get("outcome")
            .and_then(Value::as_str)
            .ok_or_else(|| ClientError::Malformed("wake_now result missing `outcome`".into()))?;
        match outcome {
            "started" => Ok(WakeOutcome::Started),
            "folded" => Ok(WakeOutcome::Folded),
            other => Err(ClientError::Malformed(format!(
                "wake_now outcome `{other}` is neither started nor folded"
            ))),
        }
    }

    /// Ops plane: append a `recovery/budget-reset` event (an explicit
    /// event — there is no silent counter wipe).
    pub async fn recovery_budget_reset(
        &self,
        session: &str,
        reason: Option<&str>,
    ) -> Result<ResetReceipt, ClientError> {
        let mut params = json!({"session": session});
        if let Some(r) = reason {
            params["reason"] = json!(r);
        }
        let result = self.ops("recovery_budget_reset", params).await?;
        Self::parse(result)
    }

    /// Ops plane: inject one demo event onto a configured mock channel
    /// (C27); the subscription engine routes it per the configured
    /// rules.
    pub async fn mock_channel_send(
        &self,
        channel: &str,
        topic: Option<&str>,
        payload: Value,
    ) -> Result<MockSendReceipt, ClientError> {
        let mut params = json!({"channel": channel, "payload": payload});
        if let Some(t) = topic {
            params["topic"] = json!(t);
        }
        let result = self.ops("mock_channel_send", params).await?;
        Self::parse(result)
    }

    fn parse<T: serde::de::DeserializeOwned>(result: Value) -> Result<T, ClientError> {
        serde_json::from_value(result)
            .map_err(|e| ClientError::Malformed(format!("result shape: {e}")))
    }

    /// Derived event id (C69) — pure, no I/O: the deterministic id kian
    /// assigns to a session event. Rule (mirroring
    /// `kian_core::events::event_id` byte-for-byte, see the crate's
    /// `derived` module):
    ///
    /// ```text
    /// event_id = "evt_" + uuid5(namespace = <session uuid>,
    ///                           name    = "kian:event:<seq>")
    /// ```
    ///
    /// RFC 4122 v5 (SHA-1 over `namespace || name`, version/variant
    /// bits forced). `session` is the session uuid string (hyphenated
    /// or bare 32-hex, any case); an unparseable session is an honest
    /// [`ClientError::Malformed`]. Tooling can reference an event
    /// before fetching it — `kian session export --with-event-ids`
    /// stamps the same value.
    pub fn event_id(session: &str, seq: u64) -> Result<String, ClientError> {
        crate::derived::event_id(session, seq)
            .ok_or_else(|| ClientError::Malformed(format!("`{session}` is not a session uuid")))
    }
}

#[cfg(test)]
mod tests {
    use super::KianClient;

    /// C69 golden vectors: the SDK's dependency-free uuid-v5 mirror must
    /// equal the core rule's literals (generated independently of the
    /// Rust `uuid` crate — Python `uuid.uuid5`), pinning the two
    /// implementations to the same bytes.
    #[test]
    fn event_id_matches_the_core_golden_vectors() {
        let ns = "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b";
        let cases: &[(u64, &str)] = &[
            (1, "evt_ec671a02-5108-5e7a-811a-3517bea635e6"),
            (7, "evt_42124c15-3948-56f1-9606-517dc61d2829"),
            (42, "evt_b1a35135-d4f4-50cd-b457-e8b96fb9e5dd"),
        ];
        for (seq, expected) in cases {
            assert_eq!(
                &KianClient::event_id(ns, *seq).unwrap(),
                expected,
                "seq {seq}"
            );
        }
        // Distinct per session at the same seq.
        assert_ne!(
            KianClient::event_id(ns, 7).unwrap(),
            KianClient::event_id("00000000-0000-0000-0000-000000000001", 7).unwrap()
        );
        // Bare-hex and hyphenated spellings of one session agree.
        assert_eq!(
            KianClient::event_id(ns, 7).unwrap(),
            KianClient::event_id("0f9c2a113b4d4e5f8a6b7c8d9e0f1a2b", 7).unwrap()
        );
        // Determinism.
        assert_eq!(
            KianClient::event_id(ns, 7).unwrap(),
            KianClient::event_id(ns, 7).unwrap()
        );
    }

    /// A malformed session is an honest error, never a garbage id.
    #[test]
    fn event_id_rejects_malformed_sessions() {
        assert!(KianClient::event_id("not-a-uuid", 1).is_err());
        assert!(KianClient::event_id("", 1).is_err());
    }
}
