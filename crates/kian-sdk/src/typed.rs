//! Typed surface v1 — one typed method per frozen
//! wire method, mirroring the server's dispatch exactly:
//!
//! - control plane (`POST /rpc`): `create_session`, `submit`, `steer`,
//!   `queue`, `interrupt`, `raise_budget`, `projection`;
//! - ops plane (`POST /ops/rpc`): `wake_now`, `recovery_budget_reset`,
//!   `mock_channel_send`, `release_worker`.
//!
//! The SSE `events` helper lives in [`crate::sse`]. Every method here
//! round-trips against a scripted server with golden request bytes in
//! `tests/fixtures/` — the serialization-freeze discipline applied
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

/// `confirm` → `{"outcome": "accepted" | "not_pending" | "expired"}`
/// (CONFIRMATION-MODEL §9).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ConfirmResult {
    pub outcome: String,
}

/// `raise_budget` → `{"outcome": "accepted" | "deduplicated" | "terminal"}` (ADR-054).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RaiseBudgetResult {
    pub outcome: String,
}

/// `pending` → one entry per unanswered ask (CONFIRMATION-MODEL §9).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PendingResult {
    pub pending: Vec<PendingAsk>,
}

/// One pending ask as the `pending` read reports it.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PendingAsk {
    pub call_id: String,
    pub tool: String,
    pub arguments: Value,
    pub requested_at_ms: u64,
    /// TTL overlay at read time — never a stored state.
    pub expired: bool,
}

/// `projection` → `{"state": <projection JSON>}`. The state is the
/// server's projection object; the SDK keeps it as parsed JSON because
/// the projection shape belongs to the server's version, not the wire
/// method. `version` is present on read-model servers (READ-MODEL
/// §3) and absent on older ones — additive.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ProjectionResult {
    pub state: Value,
    /// Last committed seq the state was derived from (READ-MODEL §3;
    /// `None` on pre-read-model servers).
    #[serde(default)]
    pub version: Option<u64>,
}

/// `projection` with `since` (READ-MODEL §3) → the versioned read:
/// `since == version` answers `{"version":..,"unchanged":true}`
/// with **no** state; any other (older) `since` answers the full
/// `{"version":..,"state":..}`. `since > version` is an RPC error
/// (`-32602`, the client is ahead or pointed at another store).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ProjectionVersioned {
    /// The committed prefix the payload was derived from.
    pub version: u64,
    /// `true` iff nothing changed since the caller's `since` — no
    /// state is carried then.
    #[serde(default)]
    pub unchanged: bool,
    /// `Some(state)` iff `unchanged` is false.
    #[serde(default)]
    pub state: Option<Value>,
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

/// `release_worker` → which bound thread was closed and where (ADR-035).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ReleaseWorkerReceipt {
    pub worker: String,
    /// `"released"` (this call closed the thread) or `"already_released"`.
    pub outcome: String,
    pub session: String,
    pub thread_id: String,
    /// The terminal end write's seq.
    pub seq: u64,
}

/// `mock_channel_send` → `{"event_id", "channel", "topic"}` (demo /
/// injection).
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
    /// server's worded receipt. The intent is durable (ADR-014):
    /// whenever work may still be in flight the server also admits a
    /// control-envelope inbox row that the owning drain consumes into a
    /// `control/interrupt` event — the interrupt survives a drain crash
    /// and crosses replicas. No new wire method.
    pub async fn interrupt(&self, session: &str) -> Result<InterruptResult, ClientError> {
        let result = self.rpc("interrupt", json!({"session": session})).await?;
        Self::parse(result)
    }

    /// Control plane: answer one pending ask (CONFIRMATION-MODEL §5).
    /// `decision` is the closed `approve`/`reject` vocabulary; the
    /// outcome is the server's honest receipt (`accepted` /
    /// `not_pending` / `expired`).
    pub async fn confirm(
        &self,
        session: &str,
        call_id: &str,
        decision: &str,
        reason: Option<&str>,
        requested_by: Option<&str>,
    ) -> Result<ConfirmResult, ClientError> {
        let mut params = json!({
            "session": session,
            "call_id": call_id,
            "decision": decision,
        });
        if let Some(r) = reason {
            params["reason"] = json!(r);
        }
        if let Some(who) = requested_by {
            params["requested_by"] = json!(who);
        }
        let result = self.rpc("confirm", params).await?;
        Self::parse(result)
    }

    /// Control plane: raise a session's lifetime budget (ADR-054). It
    /// admits ONE durable reserved control row (idempotent) that the
    /// owning drain consumes into `budget/raised`; the outcome is the
    /// server's honest receipt (`accepted` / `deduplicated` /
    /// `terminal`). At least one amount must be positive.
    pub async fn raise_budget(
        &self,
        session: &str,
        tokens: Option<u64>,
        wall_clock_ms: Option<u64>,
        reason: Option<&str>,
        requested_by: Option<&str>,
        idempotency_key: Option<&str>,
    ) -> Result<RaiseBudgetResult, ClientError> {
        let mut params = json!({"session": session});
        if let Some(tokens) = tokens {
            params["tokens"] = json!(tokens);
        }
        if let Some(ms) = wall_clock_ms {
            params["wall_clock_ms"] = json!(ms);
        }
        if let Some(reason) = reason {
            params["reason"] = json!(reason);
        }
        if let Some(who) = requested_by {
            params["requested_by"] = json!(who);
        }
        if let Some(key) = idempotency_key {
            params["idempotency_key"] = json!(key);
        }
        let result = self.rpc("raise_budget", params).await?;
        Self::parse(result)
    }

    /// Control plane: the session's pending asks (a pure read with the
    /// TTL overlay).
    pub async fn pending(&self, session: &str) -> Result<PendingResult, ClientError> {
        let result = self.rpc("pending", json!({"session": session})).await?;
        Self::parse(result)
    }

    /// Control plane: read the session's projection.
    pub async fn projection(&self, session: &str) -> Result<ProjectionResult, ClientError> {
        let result = self.rpc("projection", json!({"session": session})).await?;
        Self::parse(result)
    }

    /// Control plane: the versioned projection read (READ-MODEL §3) —
    /// presents the caller's last version; an unchanged answer costs
    /// one version check and carries no state.
    pub async fn projection_since(
        &self,
        session: &str,
        since: u64,
    ) -> Result<ProjectionVersioned, ClientError> {
        let result = self
            .rpc("projection", json!({"session": session, "since": since}))
            .await?;
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

    /// Ops plane: close the live thread bound to `worker` and free its
    /// 1:1 slot (ADR-035). A refused release is the server's honest
    /// `-32002`/`-32008` with the reason in the message.
    pub async fn release_worker(
        &self,
        worker: &str,
        reason: Option<&str>,
    ) -> Result<ReleaseWorkerReceipt, ClientError> {
        let mut params = json!({"worker": worker});
        if let Some(r) = reason {
            params["reason"] = json!(r);
        }
        let result = self.ops("release_worker", params).await?;
        Self::parse(result)
    }

    /// Ops plane: inject one demo event onto a configured mock channel
    /// the subscription engine routes it per the configured
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

    /// Derived event id — pure, no I/O: the deterministic id kian
    /// assigns to a session event. Rule (mirroring
    /// `kian_core::events::event_id` byte-for-byte, see the crate's
    /// `derived` module):
    ///
    /// ```text
    /// event_id = "evt_" + uuid5(namespace = <session uuid>,
    ///                           name = "kian:event:<seq>")
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

    /// golden vectors: the SDK's dependency-free uuid-v5 mirror must
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
