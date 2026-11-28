//! The `InboxWriter` port — the only thing the bundle may ask of the
//! server (layered iron rule, AGENTS.md constraint 9).
//!
//! The port exposes two capabilities through three methods, all
//! control-plane-shaped:
//!
//! - `admit` — make an input row durable. The row exists *before* any
//!   wake may fire (constraint 6, invariant 1). Idempotent by
//!   `(session, idempotency_key)` — first write wins.
//! - `admit_channel` — the same write under the channel-scoped guard
//!   (ADR-081): the key is `chan:<channel>:<event_id>` and a key already
//!   consumed in *any* session suppresses the insert (the Router route's
//!   semantics; a plain writer may keep `admit`'s per-session dedupe).
//! - `wake` — the advisory liveness nudge. Wakes may fold, be lost or
//!   race; the startup inbox rebuild re-derives them, so no work is lost
//!   when a wake disappears.
//!
//! Assembly (kian-cli) implements this trait over the real
//! engine. The bundle crate itself holds no execution symbols —
//! `tests/iron_rule.rs` enforces that textually. The single type this
//! file takes from `kian-orchestrator` is `WakeOutcome`, a pure data
//! enum; that is the whole "types only via a narrow port" dependency.

use futures::future::BoxFuture;
use kian_core::model::SessionId;
use kian_orchestrator::WakeOutcome;
use serde_json::Value;

/// Result of a durable inbox write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmitReceipt {
    /// True when this call deduplicated against an already-committed row
    /// with the same `(session, idempotency_key)` — the original row
    /// stands, exactly one row exists. On [`InboxWriter::admit_channel`]
    /// it also covers the channel guard: a consumed row with that key in
    /// any session suppressed the insert (ADR-081).
    pub deduplicated: bool,
}

/// Failures of an [`InboxWriter::admit`]. The store's typed errors are
/// mapped at the assembly boundary; the bundle only needs "the row is
/// not durable" — on this error the wake is *never* issued (the same
/// discipline the engine's own admit path follows).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InboxWriteError {
    #[error("inbox write failed: {0}")]
    Write(String),
}

/// The bundle's whole request surface toward the server: write the
/// inbox, then wake. Object-safe so plugins/assembly can hand the
/// bundle an `Arc<dyn InboxWriter>`.
///
/// Implementations must preserve the ordering obligation: `admit`'s
/// returned future resolves only after the row is durably committed,
/// and `wake` is called strictly after that resolution (invariant 1 —
/// observable ordering, exercised by the integration tests).
pub trait InboxWriter: Send + Sync {
    /// Durably admits one input for `session`. The payload is a normal
    /// message payload (verbatim-passed JSON; the channel provenance
    /// wrapper rides inside its `content` — see [`crate::provenance`]).
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>>;

    /// The channel-scoped variant of [`InboxWriter::admit`] (ADR-081):
    /// `idempotency_key` is the event's channel key
    /// ([`crate::intake::channel_idempotency_key`]), and the write is
    /// additionally suppressed when a row with that key is already
    /// **consumed in any session** — the Router route's cross-incarnation
    /// guard. Defaults to `admit` (per-session dedupe only), so a writer
    /// without a scoped implementation keeps today's semantics.
    fn admit_channel(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: String,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        self.admit(session, payload, Some(idempotency_key))
    }

    /// Wakes the session. Advisory by design (constraint 6): a folded or
    /// lost wake never loses work — the row is already durable and the
    /// startup rebuild re-derives pending wakes.
    fn wake(&self, session: SessionId) -> WakeOutcome;
}

#[cfg(test)]
mod tests {
    use super::{AdmitReceipt, InboxWriteError};

    #[test]
    fn admit_receipt_and_error_are_plain_data() {
        let r = AdmitReceipt { deduplicated: true };
        assert!(r.deduplicated);
        let e = InboxWriteError::Write("boom".into());
        assert_eq!(e.to_string(), "inbox write failed: boom");
        assert_eq!(e, InboxWriteError::Write("boom".into()));
    }
}
