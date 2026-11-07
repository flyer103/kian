//! The drain-context port the bridge drives against.
//!
//! [`DshCtx`] mirrors exactly the surface `DrainCtx` offers a
//! [`LoopDriver`](kian_core::seams::LoopDriver) — minus everything the
//! bridge must not reach (no store handles, no end write: the end
//! write is the engine's alone, constraint 4). The port lives in this
//! crate and the impl over the real `DrainCtx` lives in assembly
//! (kian-cli) over a local newtype — the orphan rule permits nothing
//! else, and the split is what keeps `cargo tree -p kian-dsh` free of
//! kian-store/kian-orchestrator edges (narrow waist, DESIGN §7).

use futures::future::BoxFuture;
use kian_core::events::{Event, EventKind};
use kian_core::model::{Epoch, InputRow, SessionId};
use serde_json::Value;
use thiserror::Error;

/// Failures of a [`DshCtx`] implementation, surfaced to the driver as
/// honest reasons — never best-effort swallowing.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DshCtxError {
    /// The underlying drain operation failed (store/fence error, …).
    #[error("drain context failure: {0}")]
    Failure(String),
    /// An input take was attempted while a provider turn is in flight
    /// (the safe-boundary refusal, invariant 3).
    #[error("drain context: not at a safe boundary")]
    NotAtSafeBoundary,
}

/// The bridge's view of one drain: the fenced append path, the inbox
/// safe-boundary take, and the read-only stream — nothing else.
///
/// All futures borrow `self`; the driver owns its `Arc<dyn DshCtx>`
/// for the lifetime of an episode, so borrowing futures are enough.
pub trait DshCtx: Send + Sync + 'static {
    /// The session this drain owns.
    fn session(&self) -> SessionId;
    /// The drain's fencing epoch (audit only — fencing itself stays in
    /// the append path).
    fn epoch(&self) -> Epoch;
    /// Whether a provider turn is in flight on the kian side (the
    /// promote-boundary flag the engine's `DrainCtx` tracks).
    fn in_turn(&self) -> bool;
    /// The session's full committed stream (the projection source,
    /// constraint 1).
    fn events(&self) -> BoxFuture<'_, Result<Vec<Event>, DshCtxError>>;
    /// One fenced append carrying the drain's epoch.
    fn append(&self, kind: EventKind, bytes: Value) -> BoxFuture<'_, Result<Event, DshCtxError>>;
    /// Appends `turn/start` with `bytes` and closes the promote
    /// boundary.
    fn begin_turn(&self, bytes: Value) -> BoxFuture<'_, Result<Event, DshCtxError>>;
    /// Appends `turn/end` with `bytes` and reopens the promote
    /// boundary.
    fn end_turn(&self, bytes: Value) -> BoxFuture<'_, Result<Event, DshCtxError>>;
    /// Takes every eligible input at this safe boundary (appending the
    /// consumption events and binding the rows); refuses mid-turn.
    fn take_eligible_inputs(&self) -> BoxFuture<'_, Result<Vec<InputRow>, DshCtxError>>;
    /// Pure read: does the session hold eligible-but-unconsumed input?
    fn has_eligible_inputs(&self) -> BoxFuture<'_, Result<bool, DshCtxError>>;
    /// Whether an interrupt was requested for this drain. The mounted
    /// assembly cannot see the engine's interrupt flag from `DrainCtx`
    /// (v1: it reports `false`); the bridge forwards the contract's
    /// `interrupt` control frame whenever a context reports it, and
    /// the engine still honors its own flag at outcome boundaries.
    fn interrupt_requested(&self) -> bool {
        false
    }
}
