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
//! - [`react`] — the built-in ReAct LoopDriver (card C11)
//! - [`tools`] — the built-in local fs/exec `ToolProvider` + `ExecBackend`
//!   (card C11); local exec runs via `spawn_blocking` so a sleeping tool
//!   cannot stall concurrent drains

pub mod react;
pub mod tools;

#[cfg(test)]
mod integration_tests;
#[cfg(test)]
mod test_support;

pub use kian_core::seams::{LoopDriver, LoopOutcome};
pub use react::{
    CLASS_EMPTY_RESPONSE, DEFAULT_MAX_ITERATIONS, LoopError, ReActConfig, ReActLoop,
    to_chat_message,
};
pub use tools::{DEFAULT_EXEC_TIMEOUT_MS, LocalExecBackend, LocalTools};
