//! Seam trait definitions (DESIGN §6.2 "Seam three roles").
//!
//! The built-in seam traits — `ModelProvider`, `LoopDriver`, `ToolProvider`,
//! `ExecBackend`, `RecoveryPolicySource` — plus the bundle additions
//! `ChannelAdapter` and `WatchdogSource` — are *definitions only*:
//! no impls here; implementation crates (kian-llm, kian-loop,
//! kian-superagent, kian-exec, …) implement and re-export them. Living in
//! `kian-core` keeps the orchestrator dependency tree clean (constraint
//! 10).

use std::collections::BTreeMap;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::SlotName;
use crate::secrets::Redaction;

// ---------------------------------------------------------------------------
// ModelProvider
// ---------------------------------------------------------------------------

/// A model request: slot + opaque provider-shaped body. The body is built
/// and serialized by the provider implementation (its serialization format
/// is frozen stable content); the core stays provider-agnostic.
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
///
/// a chunk may instead (or additionally) carry the provider's
/// `usage` object **verbatim** — a usage-only frame has `delta: null`
/// and `usage: Some(…)`; consumers must skip null deltas when
/// assembling content. The field is additive (serde default, omitted
/// when `None`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelChunk {
    pub delta: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Value>,
}

impl ModelChunk {
    /// A delta-bearing chunk with no usage report.
    #[must_use]
    pub fn new(delta: Value) -> Self {
        Self { delta, usage: None }
    }

    /// A chunk carrying a (possibly null) delta plus the provider's
    /// usage object verbatim.
    #[must_use]
    pub fn with_usage(delta: Value, usage: Value) -> Self {
        Self {
            delta,
            usage: Some(usage),
        }
    }
}

/// Provider-side failures. Variants pre-align with the pure error
/// classifier so classification never needs to parse error strings.
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
/// provider: openai-compat (kian-llm); any third-party may replace it.
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
    /// redaction provenance: what the exec redactor scrubbed from
    /// this result (aggregate counts, never secret values). Additive
    /// seam field — legacy JSON parses with an empty vec, an empty vec
    /// serializes without the key, and the report never enters the
    /// model-visible `output`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub redactions: Vec<Redaction>,
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
/// (kian-loop). Side-effecting invocations must be *logged before
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
    /// redaction provenance, produced by the redaction pass that
    /// scrubbed `stdout`/`stderr` (the report travels with the result so
    /// tool layers can attach it to the `tool/result` event payload).
    /// Additive: legacy JSON parses with an empty vec; empty serializes
    /// without the key.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub redactions: Vec<Redaction>,
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
/// (built-) and remote endpoint via KiandProto (bundle).
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
    /// The session-lifetime token budget refused the next provider
    /// request (ADR-054): the drain ends `awaiting_budget` (resumable,
    /// running -> idle) instead of spending past the limit. `budget` is
    /// the projection the gate evaluated (`limit`, `raised`, `spent`,
    /// `estimate`), carried for the end-of-drain audit line.
    AwaitingBudget { budget: Value },
}

/// Stop reason prefix a resident loop (the Router, the watchdog) emits
/// when its **per-episode** iteration budget is exhausted. The
/// `RouterDriver` recognizes it and parks the resident instead of ending
/// the session (ADR-024); it is an in-process contract, not an event or a
/// wire value.
pub const EPISODE_BUDGET_STOP_REASON_PREFIX: &str = "episode iteration budget exhausted";
/// Stop reason a resident loop (the Router) emits when its rotation
/// threshold is met at a park boundary: the engine ends the session
/// `rotated` (ADR-040 phase 2). The **hold owner** ends its own session
/// — a parked resident holds the drain, so an outside closer cannot
/// claim an idle session. In-process contract only, not an event or wire
/// value.
pub const ROTATION_STOP_REASON: &str = "resident rotation requested";

/// Seam (DESIGN §6.2): the agent loop as control flow inside a drain.
/// Built-in: the async ReAct loop (kian-loop). The context type is
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
/// `class` strings follow the pure classifier's names (
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
/// config file (`kian.yaml`). Disposition stays owned by the
/// caller — this source is pure data.
pub trait RecoveryPolicySource: Send + Sync {
    fn table(&self) -> Vec<PolicyRule>;
}

// ---------------------------------------------------------------------------
// ChannelAdapter (bundle — definition only; impls live in the
// super-agent bundle, never in core)
// ---------------------------------------------------------------------------

/// One inbound channel event, surfaced by a [`ChannelAdapter`]. The
/// payload is verbatim channel JSON (it rides inside the admitted input's
/// `content`, so the frozen message-payload contract is untouched).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChannelEvent {
    /// Channel name (e.g. `mock-im`, `lark`, `github`).
    pub channel: String,
    /// Adapter-side unique id — the idempotency key for admission
    /// (one inbox row per event, never two).
    pub event_id: String,
    /// Optional routing topic (subscription rules may match on it).
    pub topic: Option<String>,
    /// Optional sender provenance (adapter-defined identity — the id,
    /// name, or object the channel reports). Opaque to the core; the
    /// bundle's admission-class vocabulary keys on it.
    /// `#[serde(default)]`: old JSON parses unchanged with `None`.
    #[serde(default)]
    pub sender: Option<Value>,
    /// Optional group/chat provenance (adapter-defined — the chat or
    /// group the event arrived in, when distinct from the routing
    /// topic). Opaque to the core.
    /// `#[serde(default)]`: old JSON parses unchanged with `None`.
    #[serde(default)]
    pub group: Option<String>,
    /// Verbatim event payload, opaque to the core.
    pub payload: Value,
    /// Observed at, unix epoch milliseconds.
    pub ts_ms: u64,
}

/// One outbound channel delivery, handed to a [`ChannelAdapter`]. Output
/// policies (word limits, structured sections) are applied by the caller
/// *before* the send; the adapter delivers what it is given and returns
/// an honest [`SendReceipt`] or error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChannelDelivery {
    pub channel: String,
    /// Channel-defined delivery target (chat id, issue ref, …) — opaque
    /// to everything upstream of the adapter.
    pub target: Value,
    /// The message content to deliver, already policy-shaped.
    pub content: Value,
}

/// Proof that a delivery landed: the adapter's own id for the sent
/// message plus the delivery timestamp. Recorded in the audit stream via
/// the caller's `channel/delivered` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendReceipt {
    pub message_id: String,
    pub ts_ms: u64,
}

/// Adapter-side failures, pre-aligned with the pure error classifier's
/// vocabulary so classification never parses error strings.
#[derive(Debug, Clone, PartialEq, Eq, Hash, thiserror::Error)]
pub enum ChannelError {
    #[error("channel unavailable: {0}")]
    Unavailable(String),
    #[error("delivery failed: {0}")]
    DeliveryFailed(String),
    #[error("delivery timed out")]
    Timeout,
}

/// Seam (DESIGN §6.2, bundle): an inbound/outbound channel (IM, issue
/// tracker, …). Definition only in core — the first impl is the
/// production-quality in-memory mock channel (kian-superagent); real
/// lark/GitHub adapters are ecosystem. Subscribe streams must be
/// independently cloneable consumers of the adapter's own choosing.
pub trait ChannelAdapter: Send + Sync {
    /// Inbound events. Each event must be surfaced exactly once per
    /// subscriber; admission dedupes on `event_id` as the durable guard.
    fn subscribe(&self) -> BoxStream<'static, ChannelEvent>;
    /// Outbound delivery. Resolves only after the channel accepted the
    /// message — an honest error never masquerades as a receipt.
    fn send(&self, out: ChannelDelivery) -> BoxFuture<'static, Result<SendReceipt, ChannelError>>;
}

// ---------------------------------------------------------------------------
// WatchdogSource (bundle — definition only)
// ---------------------------------------------------------------------------

/// Endpoint health as the watchdog observes it. Edge-triggered: a source
/// reports *changes*, not heartbeats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EndpointHealth {
    Online,
    Offline,
}

/// One edge-triggered health transition for a registered endpoint. A
/// dead daemon cannot report its own death — the authoritative v1 view
/// is the server's heartbeat-absence observation (DESIGN §4.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthEvent {
    pub endpoint_id: String,
    pub status: EndpointHealth,
    pub ts_ms: u64,
    pub detail: Option<String>,
}

/// Seam (DESIGN §6.2, bundle): an edge-triggered health monitor.
/// Definition only in core — the v1 impl is the server-side heartbeat
/// watchdog over the endpoint registry (kian-exec). The stream
/// carries state changes only (no flapping, no periodic noise); health
/// events reach sessions exclusively via Admit — the watchdog never
/// writes session streams itself (layered iron rule).
pub trait WatchdogSource: Send + Sync {
    fn health(&self) -> BoxStream<'static, HealthEvent>;
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        ExecCommand, ExecResult, LoopOutcome, ModelChunk, ModelRequest, PolicyRule, RecoveryAction,
        ToolCall, ToolResult, ToolSchema,
    };
    use crate::model::SlotName;

    /// All built-in seams are object-safe so the orchestrator can hold
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

    /// The bundle seams are object-safe for the same reason the
    /// built-in seams are: assembly holds `Arc<dyn ChannelAdapter>` /
    /// `Arc<dyn WatchdogSource>` registered by plugins.
    #[test]
    fn bundle_seam_traits_are_object_safe() {
        fn assert_object_safe(_: &dyn super::ChannelAdapter, _: &dyn super::WatchdogSource) {}
        let _ = assert_object_safe;
    }

    /// The bundle seam support types round-trip through JSON; the
    /// health status serializes as the lowercase tabled form.
    #[test]
    fn bundle_seam_support_types_round_trip() {
        use super::{
            ChannelDelivery, ChannelError, ChannelEvent, EndpointHealth, HealthEvent, SendReceipt,
        };

        let event = ChannelEvent {
            channel: "mock-im".into(),
            event_id: "evt-7".into(),
            topic: Some("worker-1".into()),
            sender: Some(json!({"id": "u-42", "name": "yifei"})),
            group: Some("chat-9".into()),
            payload: json!({"text": "@kian deploy"}),
            ts_ms: 1_762_000_000_000,
        };
        let delivery = ChannelDelivery {
            channel: "mock-im".into(),
            target: json!({"chat_id": 42}),
            content: json!({"text": "done"}),
        };
        let receipt = SendReceipt {
            message_id: "msg-9".into(),
            ts_ms: 1_762_000_000_001,
        };
        let health = HealthEvent {
            endpoint_id: "ep-laptop".into(),
            status: EndpointHealth::Offline,
            ts_ms: 1_762_000_000_002,
            detail: Some("heartbeat stale".into()),
        };

        fn rt<T>(val: &T)
        where
            T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
        {
            let ser = serde_json::to_value(val).unwrap();
            let back: T = serde_json::from_value(ser).unwrap();
            assert_eq!(&back, val);
        }
        rt(&event);
        rt(&delivery);
        rt(&receipt);
        rt(&health);

        // Tabled serde forms: lowercase status words.
        assert_eq!(
            serde_json::to_value(EndpointHealth::Online).unwrap(),
            json!("online")
        );
        assert_eq!(
            serde_json::to_value(EndpointHealth::Offline).unwrap(),
            json!("offline")
        );
        // Errors stay display-stable (classification reads kinds, not
        // strings, but the messages are human-facing).
        assert_eq!(ChannelError::Timeout.to_string(), "delivery timed out");
    }

    /// Old `ChannelEvent` JSON (legacy, no `sender`/`group`) parses
    /// unchanged with `None` defaults for the new fields, and the new
    /// fields serialize as explicit nulls — additive, never breaking.
    #[test]
    fn old_channel_event_json_parses_with_default_provenance() {
        use super::ChannelEvent;

        let old = json!({
            "channel": "mock-im",
            "event_id": "evt-legacy",
            "topic": null,
            "payload": {"text": "hi"},
            "ts_ms": 1_762_000_000_000u64,
        });
        let event: ChannelEvent = serde_json::from_value(old).unwrap();
        assert_eq!(event.channel, "mock-im");
        assert_eq!(event.event_id, "evt-legacy");
        assert_eq!(event.topic, None);
        assert_eq!(event.sender, None);
        assert_eq!(event.group, None);

        // The new fields serialize as nulls (serde default visibility):
        // a round trip preserves equality either way.
        let ser = serde_json::to_value(&event).unwrap();
        assert_eq!(ser["sender"], serde_json::Value::Null);
        assert_eq!(ser["group"], serde_json::Value::Null);
        let back: ChannelEvent = serde_json::from_value(ser).unwrap();
        assert_eq!(back, event);
    }

    /// `ModelChunk.usage` is additive — legacy JSON (no `usage`
    /// key) parses with `None`, `None` serializes without the key, and
    /// a usage-carrying chunk round-trips with `delta: null`.
    #[test]
    fn model_chunk_usage_is_additive() {
        let plain = ModelChunk::new(json!({"content": "hi"}));
        let ser = serde_json::to_value(&plain).unwrap();
        assert_eq!(ser, json!({"delta": {"content": "hi"}}));
        let legacy: ModelChunk =
            serde_json::from_value(json!({"delta": {"content": "hi"}})).unwrap();
        assert_eq!(legacy, plain);
        assert_eq!(legacy.usage, None);

        let with = ModelChunk::with_usage(serde_json::Value::Null, json!({"prompt_tokens": 3}));
        let ser = serde_json::to_value(&with).unwrap();
        assert_eq!(ser, json!({"delta": null, "usage": {"prompt_tokens": 3}}));
        let back: ModelChunk = serde_json::from_value(ser).unwrap();
        assert_eq!(back, with);
    }

    /// The seam support types round-trip through JSON; policy rules come
    /// from declarative config and must be serde-shaped.
    #[test]
    fn seam_support_types_round_trip() {
        let req = ModelRequest {
            slot: SlotName("main".into()),
            body: json!({"messages": []}),
        };
        let chunk = ModelChunk::new(json!({"text": "hi"}));
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
            redactions: Vec::new(),
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
            redactions: Vec::new(),
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
