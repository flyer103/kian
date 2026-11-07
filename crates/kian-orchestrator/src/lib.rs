//! `kian-orchestrator` — execution model, recovery subsystem, control-plane
//! implementation.
//!
//! DESIGN §7 assigns this crate the key interfaces `admit`, `wake`, `claim`,
//! `drain_loop`. Per the layered iron rule (AGENTS.md constraint 9) the
//! control plane here only writes the inbox and reads projections — it
//! never touches execution; execution only appends events and never mutates
//! projections directly.
//!
//! This crate is inside the narrow waist (constraint 10): its internal
//! dependencies are exactly `kian-core` + `kian-store`. Provider, loop,
//! endpoint and plugin implementations stay outside its dependency tree
//! and are consumed through the seam traits in `kian_core::seams`.
//!
//! Modules:
//! - [`engine`] — Admit → Wake → Drain → Promote (DESIGN §2.3)
//! - [`wake`] — the foldable/losable pendingWake registry
//! - [`metrics`] — orchestrator counters: state transitions, drain
//!   depth/duration, pendingWake traffic
//! - [`recovery`] — three-layer recovery (DESIGN §2.4)
pub mod engine;
pub mod metrics;
pub mod read;
pub mod recovery;
pub mod wake;

pub use engine::{
    Clock, DrainCtx, DrainHandle, Engine, EngineConfig, EngineError, InterruptOutcome, ManualClock,
    SystemClock, WakeOutcome,
};
pub use metrics::{Metrics, MetricsSnapshot};
pub use read::{EventTail, VersionedProjection};
pub use recovery::{
    BudgetConfig, BudgetDenied, BudgetSnapshot, ClassifyCtx, CompactOutcome, ErrorClass,
    EscalationCause, GuardDecision, OneShotGuard, PolicyConfig, PolicyError, PolicyTable,
    RecoveryBudget, RecoveryEnv, RecoveryError, RecoveryReactor, RecoveryTerminal, classify,
    run_with_recovery,
};
// Re-exported for convenience: the recovery policy seam types the
// subsystem consumes (definitions live in kian-core, DESIGN §6.2).
pub use kian_core::seams::{PolicyRule, RecoveryAction};
pub use wake::WakeRegistry;
