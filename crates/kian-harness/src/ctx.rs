//! The drain-context port a bridge drives against (HARNESS-BRIDGE §4).
//!
//! [`BridgeCtx`] mirrors exactly the surface `DrainCtx` offers a
//! [`LoopDriver`](kian_core::seams::LoopDriver) — minus everything a bridge must not reach (no
//! store handles, no end write: the end write is the engine's alone). The port lives here and
//! the impl over the real `DrainCtx` lives in assembly (`kian-cli`) over a local newtype.

use futures::future::BoxFuture;
use kian_core::events::{Event, EventKind};
use kian_core::model::{Epoch, InputRow, SessionId};
use serde_json::Value;
use thiserror::Error;

/// Failures of a [`BridgeCtx`] implementation, surfaced to the driver as honest reasons —
/// never best-effort swallowing.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BridgeCtxError {
    /// The underlying drain operation failed (store/fence error, …).
    #[error("drain context failure: {0}")]
    Failure(String),
    /// An input take was attempted while a provider turn is in flight.
    #[error("drain context: not at a safe boundary")]
    NotAtSafeBoundary,
}

/// The bridge's view of one drain: the fenced append path, the inbox safe-boundary take and the
/// read-only stream — nothing else.
pub trait BridgeCtx: Send + Sync + 'static {
    /// The session this drain owns.
    fn session(&self) -> SessionId;
    /// The drain's fencing epoch (audit only — fencing itself stays in the append path).
    fn epoch(&self) -> Epoch;
    /// Whether a provider turn is in flight on the kian side.
    fn in_turn(&self) -> bool;
    /// The session's full committed stream (the projection source).
    fn events(&self) -> BoxFuture<'_, Result<Vec<Event>, BridgeCtxError>>;
    /// One fenced append carrying the drain's epoch.
    fn append(&self, kind: EventKind, bytes: Value)
    -> BoxFuture<'_, Result<Event, BridgeCtxError>>;
    /// Appends `turn/start` with `bytes` and closes the promote boundary.
    fn begin_turn(&self, bytes: Value) -> BoxFuture<'_, Result<Event, BridgeCtxError>>;
    /// Appends `turn/end` with `bytes` and reopens the promote boundary.
    fn end_turn(&self, bytes: Value) -> BoxFuture<'_, Result<Event, BridgeCtxError>>;
    /// Takes every eligible input at this safe boundary (appending the consumption events and
    /// binding the rows); refuses mid-turn.
    fn take_eligible_inputs(&self) -> BoxFuture<'_, Result<Vec<InputRow>, BridgeCtxError>>;
    /// Pure read: does the session hold eligible-but-unconsumed input?
    fn has_eligible_inputs(&self) -> BoxFuture<'_, Result<bool, BridgeCtxError>>;
    /// Whether an interrupt was requested for this drain. Assembly reports it when the engine
    /// exposes the flag; a bridge forwards the control frame whenever a context reports it.
    fn interrupt_requested(&self) -> bool {
        false
    }
}
