//! The bridge event vocabulary (HARNESS-BRIDGE §4.1–§4.3).
//!
//! Adapters map native harness events onto the **existing** kian kinds; the shared types here
//! are the only surface between an adapter and the driver.

use futures::future::BoxFuture;
use kian_core::events::EventKind;
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

/// One kian event a native harness event maps to, or an explicit absorbed marker.
///
/// `Absorbed` exists so an envelope the bridge deliberately does not persist (a harness
/// episode boundary, a stream delta) is *reported*, never silently dropped.
#[derive(Debug, Clone, PartialEq)]
pub enum Mapped {
    Event { kind: EventKind, payload: Value },
    Absorbed { kind: &'static str },
}

/// One kian→harness control frame: the translation of the orchestrator's input surfaces.
#[derive(Debug, Clone, PartialEq)]
pub enum Control {
    /// A promoted input row — the plain message content, exactly what the inbox row carried.
    Input { content: Value },
    /// A steer: the synthetic (strippable) content; the harness injects it into its next
    /// admitted request, and the mapped echo carries `synthetic:true`.
    Steer { content: Value },
    /// Stop the current turn; the cancelled turn settles without an `assistant/message`.
    Interrupt,
}

/// The harness asks kian to decide a gated tool call (HARNESS-BRIDGE §4.4).
#[derive(Debug, Clone, PartialEq)]
pub struct ApprovalRequest {
    /// The harness-side request id (echoed in the answer).
    pub id: String,
    /// The harness's own call id (Claude `tool_use` id, Codex `itemId`), when the protocol
    /// exposes one — the `tool/pre` correlation key.
    pub call_id: Option<String>,
    /// The harness's own tool name.
    pub tool: String,
    /// The tool input, kept only for matching (never logged, never persisted).
    pub input: Value,
}

/// The kian decision, delivered back to the harness and recorded as `tool/call.permission`
/// provenance by the adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalAnswer {
    pub id: String,
    pub allow: bool,
    /// `"configured"` (the declarative policy) or `"hook"` (a `tool/pre` veto).
    pub policy: &'static str,
    /// The rule that decided: a configured pattern, `"default"`, or `"<plugin>:tool/pre"`.
    pub matched: String,
    /// The hook's denial reason (a hook deny only).
    pub reason: Option<String>,
}

/// What one poll of a live harness session produced.
#[derive(Debug, Clone, PartialEq)]
pub enum SessionEvent {
    /// One mapped event or an absorbed marker.
    Mapped(Mapped),
    /// A gated tool the harness will not run until kian answers.
    Approval(ApprovalRequest),
    /// The child exited (cleanly or not); the driver decides the honest outcome from turn state.
    Exit { code: Option<i32> },
}

/// A harness protocol failure: drift, malformed input, an unsupported frame.
///
/// The driver turns every variant into a named `SoftFailed`; none is ever a fabricated success.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ContractError {
    /// A frame the adapter cannot classify without guessing.
    #[error("harness protocol drift: {0}")]
    Drift(String),
    /// The child closed its stdout or died before a required handshake.
    #[error("harness protocol: child exited: {0}")]
    Exited(String),
    /// Pipe/process I/O failed (the child is unusable).
    #[error("harness protocol: i/o failure: {0}")]
    Io(String),
    /// A line exceeded the bounded read size (HARNESS-BRIDGE §8).
    #[error("harness protocol: line exceeds {limit} bytes")]
    LineTooLong { limit: usize },
}

/// What one harness's protocol can do (HARNESS-BRIDGE §3). Drift-tested against the normative
/// capability matrix; a capability the harness lacks is never configured as required.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Capabilities {
    /// Stable harness name (`"claude-code"`, `"codex"`); also the audit label.
    pub name: &'static str,
    /// Whether the assembled model request is observable (`step/request`).
    pub request_bytes: bool,
    /// How a steer can reach the harness.
    pub steer: SteerMode,
    /// Whether an in-flight turn can be cancelled.
    pub interrupt: bool,
    /// Whether kian answers a pre-execution tool gate.
    pub approval_gate: bool,
    /// Whether the harness conversation can be resumed after process death (phase 2).
    pub resume: bool,
}

/// Steer delivery semantics (matrix row `steer`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SteerMode {
    /// No steering.
    None,
    /// The harness accepts a steer only as the next user message.
    NextTurn,
    /// The harness accepts a steer while a turn is in flight.
    InTurn,
}

/// One live harness session: the adapter's protocol state plus its child process.
///
/// Dropping the session kills the child (the adapter's peer owns the process).
pub trait HarnessSession: Send {
    /// The protocol's declared capabilities.
    fn capabilities(&self) -> Capabilities;
    /// The next event, or an honest error.
    fn next(&mut self) -> BoxFuture<'_, Result<SessionEvent, ContractError>>;
    /// Deliver one control frame.
    fn send(&mut self, control: Control) -> BoxFuture<'_, Result<(), ContractError>>;
    /// Answer a gated tool the harness is waiting on.
    fn answer(&mut self, answer: ApprovalAnswer) -> BoxFuture<'_, Result<(), ContractError>>;
    /// The harness's continuation handle (HARNESS-BRIDGE §4.6, ADR-047): Claude's `session_id`
    /// from `system/init`, Codex's thread id. The driver persists it on the turn's `turn/end`.
    fn resume_handle(&self) -> Option<String> {
        None
    }
}
