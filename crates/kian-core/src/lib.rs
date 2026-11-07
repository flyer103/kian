//! `kian-core` — concept model, session state machine, six-invariant
//! assertions.
//!
//! DESIGN §7 assigns this crate: "Concept model, session state machine,
//! six-invariant assertions" with key interfaces `SessionId`, `EventKind`,
//! `Projection`. It is the narrow waist of the system (AGENTS.md constraint
//! 10): no provider, channel, daemon or super-agent specific code may live
//! here.
//!
//! The event stream is the sole truth (constraint 1): all session state is a
//! projection of events and projections are disposable and rebuildable.
//!
//! Modules:
//! - [`model`] — concept model (DESIGN §2.1)
//! - [`events`] — event classification and envelope, schema v0 (DESIGN §3.1, §3.3)
//! - [`state`] — session state machine and pure projection (DESIGN §2.2)
//! - [`context`] — model-context projection `derive_model_context` (DESIGN §3.2, constraints 2/3)
//! - [`control`] — reserved control-envelope convention (ADR-014, durable interrupt intent)
//! - [`memory`] — the `memory/note` fold: fail-closed payload parsing, the
//!   latest-per-subject projection and range verification (MEMORY-MODEL §3)
//! - [`invariants`] — the six semantic invariants + NoHalfTerminated as assertion predicates
//! - [`seams`] — seam trait definitions (DESIGN §6.2); impl crates re-export

pub mod context;
pub mod control;
pub mod events;
pub mod invariants;
pub mod memory;
pub mod model;
pub mod seams;
pub mod secrets;
pub mod state;
