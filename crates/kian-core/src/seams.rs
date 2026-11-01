//! Seam trait definitions (DESIGN §6.2 "Seam three roles").
//!
//! The Phase-1 seam traits — `ModelProvider`, `LoopDriver`, `ToolProvider`,
//! `ExecBackend`, `RecoveryPolicySource` — are *definitions only*: no impls
//! here; implementation crates (kian-llm, kian-loop, …) implement and
//! re-export them. Living in `kian-core` keeps the orchestrator dependency
//! tree clean (constraint 10).

use std::collections::BTreeMap;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::SlotName;

// ---------------------------------------------------------------------------
// ModelProvider
// ---------------------------------------------------------------------------

/// A model request: slot + opaque provider-shaped body. The body is built
/// and serialized by the provider implementation (its serialization format
/// is frozen stable content, C8); the core stays provider-agnostic.
///
/// Persist-what-you-send (A2): the *caller* persists the request bytes as a
/// `step/request` event before invoking [`ModelProvider::complete`] — the
/// ordering obligation lives at the call site, not inside the provider.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelRequest {
    pub slot: SlotName,
    pub body: Value,
}

/// One streamed response chunk; the delta shape is provider-defined and
/// opaque to the core.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelChunk {
    pub delta: Value,
}

/// Provider-side failures. Variants pre-align with the pure error
/// classifier (C10) so classification never needs to parse error strings.
#[derive(Debug, Clone, PartialEq, Eq, Hash, thiserror::Error)]
pub enum ProviderError {
    #[error("rate limited")]
    RateLimited,
    #[error("context overflow")]
    ContextOverflow,
    #[error("provider unavailable: {0}")]
    Unavailable(String),
    #[error("transport error: {0}")]
    Transport(String),
    /// A soft failure (constraint 7): never mappable onto `completed`.
    #[error("empty response")]
    EmptyResponse,
    #[error("invalid response: {0}")]
    InvalidResponse(String),
    #[error("{0}")]
    Other(String),
}

/// Seam (DESIGN §6.2): model completion as a stream of chunks. Built-in
/// provider: openai-compat (kian-llm, C8); any third-party may replace it.
pub trait ModelProvider: Send + Sync {
    fn complete(&self, req: ModelRequest) -> BoxStream<'static, Result<ModelChunk, ProviderError>>;
}

// ---------------------------------------------------------------------------
// ToolProvider
// ---------------------------------------------------------------------------

/// A model-visible tool schema. Serialized into the request prefix; the
/// serialization format is frozen stable content (constraint 3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolSchema {
    pub name: String,
    pub description: String,
    /// JSON-Schema-shaped parameter description (opaque to the core).
    pub parameters: Value,
}

/// A tool invocation requested by the model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

/// A tool invocation result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    pub call_id: String,
    pub output: Value,
    /// Distinguishes tool-level errors from successful outputs honestly
    /// (constraint 7).
    pub is_error: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ToolError {
    #[error("invalid tool call: {0}")]
    InvalidCall(String),
    #[error("tool execution failed: {0}")]
    Execution(String),
    #[error("tool timed out")]
    Timeout,
}

/// Seam (DESIGN §6.2): model-visible tools. Built-ins: fs/exec/kiand
/// (kian-loop, C11). Side-effecting invocations must be *logged before
/// execution* (invariant 1) — the caller appends the `tool/call` event
/// before `invoke` runs.
pub trait ToolProvider: Send + Sync {
    fn schemas(&self) -> Vec<ToolSchema>;
    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>>;
}

// ---------------------------------------------------------------------------
// ExecBackend
// ---------------------------------------------------------------------------

/// A command execution request (argv vector, never a shell string).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecCommand {
    pub argv: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub cwd: Option<String>,
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecResult {
    /// `None` when the process was killed / did not exit normally.
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExecError {
    #[error("failed to spawn: {0}")]
    Spawn(String),
    #[error("execution timed out")]
    Timeout,
    #[error("io error: {0}")]
    Io(String),
}

/// Seam (DESIGN §6.2): environment command execution. Built-ins: local
/// (Phase 1) and remote endpoint via KiandProto (Phase 2).
pub trait ExecBackend: Send + Sync {
    fn exec(&self, cmd: ExecCommand) -> BoxFuture<'static, Result<ExecResult, ExecError>>;
}

// ---------------------------------------------------------------------------
// LoopDriver
// ---------------------------------------------------------------------------

/// What a loop iteration reports back to the drain. A soft failure is
/// routed to the recovery policy and is *never* settled as `completed`
/// (constraint 7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoopOutcome {
    /// One provider turn finished (events were appended during it).
    TurnDone,
    /// No eligible input remains; the drain may settle and release.
    Settled,
    /// A stop condition ended the loop intentionally.
    Stopped { reason: String },
    /// Soft failure (e.g. empty model response) — consult the recovery
    /// policy table.
    SoftFailed { reason: String },
}

/// Seam (DESIGN §6.2): the agent loop as control flow inside a drain.
/// Built-in: the async ReAct loop (kian-loop, C11). The context type is
/// left abstract on purpose — it is the drain handle surface (inbox take,
/// event append, …) that the orchestrator provides; the core does not model
/// execution internals (layered iron rule).
pub trait LoopDriver: Send + Sync {
    type Ctx: Send;
    fn drive(&self, ctx: Self::Ctx) -> BoxStream<'static, LoopOutcome>;
}

// ---------------------------------------------------------------------------
// RecoveryPolicySource
// ---------------------------------------------------------------------------

/// One declarative policy rule (A4): a classifier class name → action.
/// `class` strings follow the pure classifier's names (C10:
/// `provider_down`, `rate_limited`, `context_overflow`, …).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyRule {
    pub class: String,
    pub action: RecoveryAction,
}

/// Recovery actions (DESIGN §2.4). Every action is one-shot-guarded and
/// budgeted with refund semantics; none may loop unboundedly (constraint 8).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RecoveryAction {
    BackoffRetry { delay_ms: u64 },
    CompressThenRetry,
    Failover,
    InterruptAsk,
    Fail,
}

/// Seam (DESIGN §6.2): supplies the declarative recovery table. Built-in:
/// config file (`kian.yaml`, C10/C13). Disposition stays owned by the
/// caller — this source is pure data.
pub trait RecoveryPolicySource: Send + Sync {
    fn table(&self) -> Vec<PolicyRule>;
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        ExecCommand, ExecResult, LoopOutcome, ModelChunk, ModelRequest, PolicyRule, RecoveryAction,
        ToolCall, ToolResult, ToolSchema,
    };
    use crate::model::SlotName;

    /// All Phase-1 seams are object-safe so the orchestrator can hold
    /// plugin-registered `Arc<dyn _>` impls (DESIGN §6.2, §7).
    #[test]
    fn seam_traits_are_object_safe() {
        fn assert_object_safe(
            _: &dyn super::ModelProvider,
            _: &dyn super::ToolProvider,
            _: &dyn super::ExecBackend,
            _: &dyn super::RecoveryPolicySource,
            _: &dyn super::LoopDriver<Ctx = ()>,
        ) {
        }
        let _ = assert_object_safe;
    }

    /// The seam support types round-trip through JSON; policy rules come
    /// from declarative config (C10) and must be serde-shaped.
    #[test]
    fn seam_support_types_round_trip() {
        let req = ModelRequest {
            slot: SlotName("main".into()),
            body: json!({"messages": []}),
        };
        let chunk = ModelChunk {
            delta: json!({"text": "hi"}),
        };
        let schema = ToolSchema {
            name: "fs.read".into(),
            description: "read a file".into(),
            parameters: json!({"type": "object"}),
        };
        let call = ToolCall {
            id: "call-1".into(),
            name: "fs.read".into(),
            arguments: json!({"path": "/tmp/x"}),
        };
        let result = ToolResult {
            call_id: "call-1".into(),
            output: json!({"content": "…"}),
            is_error: false,
        };
        let cmd = ExecCommand {
            argv: vec!["rg".into(), "-n".into(), "pat".into()],
            env: [("HOME".to_string(), "/tmp".to_string())]
                .into_iter()
                .collect(),
            cwd: Some("/tmp".into()),
            timeout_ms: Some(5_000),
        };
        let exec = ExecResult {
            exit_code: Some(0),
            stdout: "match".into(),
            stderr: String::new(),
            timed_out: false,
        };
        let rule = PolicyRule {
            class: "rate_limited".into(),
            action: RecoveryAction::BackoffRetry { delay_ms: 500 },
        };

        fn rt<T>(val: &T)
        where
            T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
        {
            let ser = serde_json::to_value(val).unwrap();
            let back: T = serde_json::from_value(ser).unwrap();
            assert_eq!(&back, val);
        }
        rt(&req);
        rt(&chunk);
        rt(&schema);
        rt(&call);
        rt(&result);
        rt(&cmd);
        rt(&exec);
        rt(&rule);

        let outcome = LoopOutcome::SoftFailed {
            reason: "empty model response".into(),
        };
        // LoopOutcome uses default serde variant naming (it is a seam type,
        // not part of the frozen event schema).
        assert_eq!(
            serde_json::to_value(&outcome).unwrap(),
            json!({"SoftFailed": {"reason": "empty model response"}})
        );
    }
}
