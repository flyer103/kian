//! `kian-loop` — LoopDriver seam, built-in ReAct loop.
//!
//! DESIGN §7 assigns this crate the key interface `LoopDriver`. The agent
//! loop is just a control flow inside a drain (AGENTS.md preamble): this
//! crate implements the [`LoopDriver`] seam and the built-in async ReAct
//! loop; it is a passenger inside the orchestrator's drain and never
//! drives engine semantics itself.
//!
//! Termination is honest (AGENTS.md constraint 7): an empty model response
//! is a soft failure routed to the recovery policy, never `completed`.
//!
//! Modules:
//! - [`react`] — the built-in ReAct LoopDriver
//! - [`tools`] — the built-in local fs/exec `ToolProvider` + `ExecBackend`
//!   local exec runs via `spawn_blocking` so a sleeping tool
//!   cannot stall concurrent drains
//! - [`evidence`] — the evidence gate v1: bounded, declarative,
//!   pure predicates over the event stream; wired into [`react`]'s stop
//!   path, default-off

pub mod evidence;
pub mod react;
pub mod tools;

#[cfg(test)]
mod evidence_gate_tests;
#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod test_support;

pub use evidence::{
    BUILTIN_GATE_NAME, CheckOutcome, CheckSpec, EvidenceGateConfig, EvidenceGateCounters,
    GateVerdict, VerdictCounters, evaluate_evidence_gate, verdict_payload,
};
pub use kian_core::seams::{LoopDriver, LoopOutcome};
pub use react::{
    BudgetScope, CLASS_EMPTY_RESPONSE, DEFAULT_MAX_IMAGES_PER_TURN, DEFAULT_MAX_ITERATIONS,
    DEFAULT_MAX_VISION_BYTES_PER_TURN, LoopError, ReActConfig, ReActLoop, VisionBudget,
    to_chat_message,
};
pub use tools::{
    DEFAULT_EXEC_TIMEOUT_MS, FilteredTools, LocalExecBackend, LocalTools, unmatched_tool_names,
};
