//! `kian-claude` — the Claude Code bridge adapter (HARNESS-BRIDGE §5.1, phase 1).
//!
//! Spawns `claude --print --input-format stream-json --output-format stream-json --verbose`
//! and speaks its newline-delimited JSON. The harness keeps its prompts, tools, sandbox and model;
//! kian keeps the durable session, control, audit and honest outcomes.
//!
//! **Projection-only**: Claude Code exposes no assembled model request, so this adapter emits no
//! `step/request` and claims no cache accounting or byte-request replay.
//!
//! # Protocol pins (verify at each live smoke; HARNESS-BRIDGE §11)
//!
//! - input: `{"type":"user","message":{"role":"user","content":[{"type":"text","text":…}]}}`
//! - interrupt: `{"type":"control_request","request_id":…,"request":{"subtype":"interrupt"}}`
//! - approval answer:
//!   `{"type":"control_response","response":{"subtype":"success","request_id":…,"response":{"behavior":"allow"|"deny","message":…}}}`
//! - output: `system/init`, `assistant`, `user` (tool results), `result`, `control_request`
//!   (`can_use_tool`), `control_response`, `stream_event`
//!
//! An unrecognised top-level message type is a contract error, never a guess.

pub mod mapping;
pub mod session;

pub use mapping::{ClaudeConfig, ClaudeMapper};
pub use session::{ClaudeAdapter, ClaudeSession};
