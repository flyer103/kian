//! `kian-harness` — the shared skeleton for external-harness bridges
//! ([HARNESS-BRIDGE](../../design/HARNESS-BRIDGE.md), [ADR-037](../../design/decisions/ADR-037-multi-harness-bridge.md)).
//!
//! A bridge runs an external harness as a kian [`LoopDriver`](kian_core::seams::LoopDriver):
//! the harness keeps its prompts, tools, sandbox and model choice; kian keeps the durable
//! session, control, recovery and audit. Per-harness crates (`kian-claude`, `kian-codex`)
//! contribute a protocol adapter and a spawn recipe; this crate owns the state machine:
//! episode spawn/park, safe boundaries, takeover de-duplication, the step budget, honest
//! outcomes, the approval policy and the `tool/post` port.
//!
//! # Narrow waist
//!
//! The only kian dependency is `kian-core`. The drain surface is [`BridgeCtx`] — a crate-local
//! port mirroring what `DrainCtx` offers a loop; assembly (`kian-cli`) implements it over the
//! real `DrainCtx` (the orphan rule is why the port lives here and the impl lives there).
//!
//! # Honesty
//!
//! A child that dies mid-turn leaves the honest failure trace (an open turn with no
//! `assistant/message`); the driver reports `SoftFailed`, never `completed`. A protocol drift is
//! a named `SoftFailed`; a helper event a harness can grow without semantic loss is absorbed with
//! a counter, never silently dropped when it could carry model-visible content. Phase 1 has no
//! cross-process harness resume: a takeover ends the session honestly as `interrupted`
//! (HARNESS-BRIDGE §4.6).

pub mod approval;
pub mod ctx;
pub mod driver;
pub mod event;
pub mod fixture;
pub mod peer;
pub mod testing;

pub use approval::{ApprovalPolicy, Decision, Rule};
pub use ctx::{BridgeCtx, BridgeCtxError};
pub use driver::{
    BridgeDriver, BridgeDriverConfig, SessionFactory, ToolPostPort, ToolPostView, ToolPreCall,
    ToolPrePort, ToolPreVerdict,
};
pub use event::{
    ApprovalAnswer, ApprovalRequest, Capabilities, ContractError, Control, HarnessSession, Mapped,
    SessionEvent, SteerMode,
};
