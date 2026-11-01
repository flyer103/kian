//! Built-in ReAct loop (card C11).
//!
//! Per iteration: assemble context via `derive_model_context`, call the
//! resolved model slot, persist request/response events before they fly,
//! dispatch tool calls (side effects logged before execution), respect
//! promote boundaries and the steer allowance, and terminate honestly.
//! Local process execution runs via `spawn_blocking` so a sleeping tool
//! cannot stall concurrent drains.
//!
//! The loop is a **passenger inside the drain** (layered iron rule): it
//! only ever appends events through [`DrainCtx`] and reads projections;
//! admission, claims, interrupts, the end write and episode supervision
//! belong to the engine (C9).
//!
//! ## Event payload conventions (frozen, additions-only discipline)
//!
//! - `turn/start` / `turn/end`: `{"iteration": <n>, ...}` — the iteration
//!   counter, derived from the stream (number of prior `turn/start`s),
//!   never from resident state.
//! - `step/request`: `{"slot", "provider", "model", "iteration", "body"}`
//!   — the A2 audit copy of the request, appended **before** the provider
//!   is called (the provider-level `PersistSink`, C8, additionally covers
//!   fallback-request bytes at the HTTP edge).
//! - `assistant/message`: content `{"text": str, "tool_calls"?: [...]}`.
//! - `tool/call` (logged before execution): content
//!   `{"id", "name", "arguments"}` with role `assistant` (C4 fixture
//!   convention).
//! - `tool/result`: content `{"call_id", "output", "is_error"}` with role
//!   `tool`.
//!
//! ## Honest termination (constraint 7)
//!
//! An empty model response is a soft failure: the loop consults the
//! declarative [`RecoveryPolicySource`] table for the class
//! [`CLASS_EMPTY_RESPONSE`] and reports [`LoopOutcome::SoftFailed`] — the
//! engine settles it as `failed`, never `completed`. The loop itself never
//! retries (constraint 8: recovery disposition is one-shot-guarded and
//! owned by the caller, C10).

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt};
use kian_core::context::{Message, derive_model_context};
use kian_core::events::{Event, EventKind};
use kian_core::model::{AgentDef, SlotName};
use kian_core::seams::{
    LoopDriver, LoopOutcome, ModelRequest, ProviderError, RecoveryAction, RecoveryPolicySource,
    ToolCall, ToolProvider, ToolResult,
};
use kian_llm::provider::{ChatMessage, build_chat_body};
use kian_llm::slots::{SessionSlots, SlotBinding, SlotError, SlotResolver};
use kian_llm::turn::{ProviderRegistry, SlotEventSink, complete_turn};
use kian_orchestrator::{DrainCtx, EngineError};
use serde_json::{Value, json};

/// Recovery-policy class for an empty model response (constraint 7).
pub const CLASS_EMPTY_RESPONSE: &str = "empty_response";

/// Iteration budget default: stops honest loops that never converge.
pub const DEFAULT_MAX_ITERATIONS: usize = 32;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Tuning for the built-in ReAct loop.
#[derive(Debug, Clone)]
pub struct ReActConfig {
    /// The model slot the loop drives (resolved via the session's slot
    /// bindings, ADR-006).
    pub slot: SlotName,
    /// Maximum provider turns per session (counted from the event stream,
    /// so the budget survives episodes and restarts).
    pub max_iterations: usize,
}

impl Default for ReActConfig {
    fn default() -> Self {
        Self {
            slot: SlotName("main".into()),
            max_iterations: DEFAULT_MAX_ITERATIONS,
        }
    }
}

struct Inner {
    registry: Arc<ProviderRegistry>,
    tools: Arc<dyn ToolProvider>,
    policy: Arc<dyn RecoveryPolicySource>,
    agent: AgentDef,
    config: ReActConfig,
}

/// The built-in async ReAct [`LoopDriver`] (DESIGN §6.2). Cheap to clone.
#[derive(Clone)]
pub struct ReActLoop(Arc<Inner>);

impl ReActLoop {
    /// Assembles the loop from an agent definition, a provider registry
    /// (slot targets resolve against it), the tool provider, and the
    /// declarative recovery-policy source consulted on soft failures.
    pub fn new(
        agent: AgentDef,
        registry: Arc<ProviderRegistry>,
        tools: Arc<dyn ToolProvider>,
        policy: Arc<dyn RecoveryPolicySource>,
    ) -> Self {
        Self::with_config(agent, registry, tools, policy, ReActConfig::default())
    }

    /// [`ReActLoop::new`] with explicit loop tuning.
    pub fn with_config(
        agent: AgentDef,
        registry: Arc<ProviderRegistry>,
        tools: Arc<dyn ToolProvider>,
        policy: Arc<dyn RecoveryPolicySource>,
        config: ReActConfig,
    ) -> Self {
        Self(Arc::new(Inner {
            registry,
            tools,
            policy,
            agent,
            config,
        }))
    }

    #[must_use]
    pub fn config(&self) -> &ReActConfig {
        &self.0.config
    }
}

// ---------------------------------------------------------------------------
// LoopDriver
// ---------------------------------------------------------------------------

/// unfold state: one step == one full provider turn (with its tool
/// dispatches). `done` ends the stream after the terminal outcome was
/// yielded.
struct LoopState {
    ctx: DrainCtx,
    done: bool,
}

impl LoopDriver for ReActLoop {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        let inner = Arc::clone(&self.0);
        futures::stream::unfold(LoopState { ctx, done: false }, move |mut st| {
            let inner = Arc::clone(&inner);
            async move {
                if st.done {
                    return None;
                }
                let outcome = match inner.iteration(&st.ctx).await {
                    Flow::TurnDone => LoopOutcome::TurnDone,
                    Flow::Settled => LoopOutcome::Settled,
                    Flow::Stopped(reason) => LoopOutcome::Stopped { reason },
                    Flow::Soft(reason) => LoopOutcome::SoftFailed { reason },
                };
                if !matches!(outcome, LoopOutcome::TurnDone) {
                    st.done = true;
                }
                Some((outcome, st))
            }
        })
        .boxed()
    }
}

/// What one iteration concluded.
enum Flow {
    /// A provider turn ran (with or without tool calls); keep looping.
    TurnDone,
    /// No eligible input remains and the model gave a final answer.
    Settled,
    /// An intentional stop condition fired.
    Stopped(String),
    /// Soft failure — routed to the recovery policy; never `completed`.
    Soft(String),
}

enum TurnOutcome {
    FinalAnswer,
    ToolCalls(usize),
}

enum TurnError {
    /// Soft failure — reported to the engine as `LoopOutcome::SoftFailed`.
    Soft(String),
    /// Hard error (store/append failure): honest `failed`, never retried
    /// by the loop.
    Fatal(LoopError),
}

/// Loop-level failures. Every one degrades honestly to a soft report —
/// the loop has no code path that could masquerade an error as
/// completion.
#[derive(Debug, thiserror::Error)]
pub enum LoopError {
    #[error("engine: {0}")]
    Engine(#[from] EngineError),
    #[error("context projection: {0}")]
    Context(#[from] kian_core::context::ContextError),
    #[error("slots: {0}")]
    Slot(#[from] SlotError),
}

impl Inner {
    /// One ReAct iteration.
    async fn iteration(&self, ctx: &DrainCtx) -> Flow {
        match self.run_iteration(ctx).await {
            Ok(flow) => flow,
            Err(e) => Flow::Soft(format!("loop error: {e}")),
        }
    }

    async fn run_iteration(&self, ctx: &DrainCtx) -> Result<Flow, LoopError> {
        // 1. Promote newly eligible inputs at this safe boundary (we are
        //    between provider turns here; DrainCtx refuses otherwise —
        //    steer never interleaves an in-flight turn).
        let _ = ctx.take_eligible_inputs().await?;

        // 2. Rebuild the truth and derive the model context (A2/A3).
        let mut events = ctx.events().await?;
        let binding = self.binding_for(ctx, &mut events).await?;
        let messages = derive_model_context(&events)?;

        // 3. Iteration counter — derived from the stream, so the budget
        //    survives episodes and restarts (constraint 1 in spirit).
        let iteration = events
            .iter()
            .filter(|ev| matches!(ev.kind, EventKind::TurnStart))
            .count();
        if iteration >= self.config.max_iterations {
            return Ok(Flow::Stopped(format!(
                "iteration budget exhausted after {iteration} turns"
            )));
        }

        // 4. The provider turn. begin/end bracket every outcome (including
        //    soft failures) so turn events stay balanced in the stream.
        ctx.begin_turn_with(json!({"iteration": iteration})).await?;
        let flow = match self
            .provider_turn(ctx, &binding, &messages, iteration)
            .await
        {
            Ok(TurnOutcome::ToolCalls(n)) => {
                ctx.end_turn_with(json!({"iteration": iteration, "tool_calls": n}))
                    .await?;
                return Ok(Flow::TurnDone);
            }
            Ok(TurnOutcome::FinalAnswer) => {
                // Final answer + empty inbox = honest settle. New inputs
                // (a folded steer) keep the loop going instead.
                if ctx.has_eligible_inputs().await? {
                    Flow::TurnDone
                } else {
                    Flow::Settled
                }
            }
            Err(TurnError::Soft(reason)) => {
                let _ = ctx
                    .end_turn_with(json!({"iteration": iteration, "outcome": "soft_failure"}))
                    .await;
                return Ok(Flow::Soft(reason));
            }
            Err(TurnError::Fatal(e)) => {
                let _ = ctx
                    .end_turn_with(json!({"iteration": iteration, "outcome": "error"}))
                    .await;
                return Err(e);
            }
        };
        ctx.end_turn_with(json!({"iteration": iteration, "outcome": "final_answer"}))
            .await?;
        Ok(flow)
    }

    /// Resolves the session's slot binding, appending `slot/binding`
    /// events at first use (the binding is then fixed for the session's
    /// lifetime and rebuildable by replay, ADR-006).
    async fn binding_for(
        &self,
        ctx: &DrainCtx,
        events: &mut Vec<Event>,
    ) -> Result<SlotBinding, LoopError> {
        let session = ctx.session();
        if let Ok(slots) = SessionSlots::replay(session, events)
            && slots.binding(&self.config.slot).is_ok()
        {
            return Ok(slots.binding(&self.config.slot)?.clone());
        }
        let fresh = SlotResolver::from_agent_def(&self.agent).bind_session(session);
        for payload in fresh.binding_payloads() {
            ctx.append(EventKind::SlotBinding, payload).await?;
        }
        // Refresh the projection source so the appended bindings are part
        // of the truth this drain derives from.
        *events = ctx.events().await?;
        Ok(fresh.binding(&self.config.slot)?.clone())
    }

    /// Runs one provider turn: persist the A2 audit copy, call the slot
    /// (with C8 fallback traversal), assemble the streamed response, then
    /// either dispatch tool calls (log-before-execute) or return a final
    /// answer.
    async fn provider_turn(
        &self,
        ctx: &DrainCtx,
        binding: &SlotBinding,
        messages: &[Message],
        iteration: usize,
    ) -> Result<TurnOutcome, TurnError> {
        let target = binding.target.clone();

        // Deterministic tool order (the request body freezes the tools
        // array order — C8 serialization contract).
        let mut schemas = self.tools.schemas();
        schemas.sort_by(|a, b| a.name.cmp(&b.name));
        let chat: Vec<ChatMessage> = messages.iter().map(to_chat_message).collect();

        // A2: the audit copy of the exact request is durable BEFORE the
        // provider is called.
        let body = build_chat_body(&target.model, &chat, &schemas);
        ctx.append(
            EventKind::StepRequest,
            json!({
                "slot": self.config.slot.0,
                "provider": target.provider,
                "model": target.model,
                "iteration": iteration,
                "body": body,
            }),
        )
        .await
        .map_err(TurnError::fatal)?;

        // complete_turn rebuilds the body per (fallback) target so the
        // model name always matches the provider actually called.
        let slot = self.config.slot.clone();
        let build_chat = chat.clone();
        let build_schemas = schemas.clone();
        let build = Arc::new(move |t: &kian_core::model::SlotTarget| ModelRequest {
            slot: slot.clone(),
            body: build_chat_body(&t.model, &build_chat, &build_schemas),
        });
        let mut stream = complete_turn(
            Arc::clone(&self.registry),
            self.config.slot.clone(),
            binding.clone(),
            build,
            Arc::new(CtxSlotSink { ctx: ctx.clone() }),
        );

        // Assemble the streamed deltas.
        let mut text = String::new();
        let mut calls: BTreeMap<u64, CallAcc> = BTreeMap::new();
        let mut stream_error: Option<ProviderError> = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => assemble_delta(&chunk.delta, &mut text, &mut calls),
                Err(e) => {
                    stream_error = Some(e);
                    break;
                }
            }
        }
        if let Some(e) = stream_error {
            let class = error_class(&e);
            let action = consult_policy(self.policy.as_ref(), class);
            let note = action_note(action);
            return Err(TurnError::Soft(format!(
                "provider error ({class}): {e}{note}"
            )));
        }
        if text.is_empty() && calls.is_empty() {
            // Constraint 7: an empty model response is a soft failure
            // routed to the recovery policy — never a completion.
            let action = consult_policy(self.policy.as_ref(), CLASS_EMPTY_RESPONSE);
            let note = action_note(action);
            return Err(TurnError::Soft(format!("empty model response{note}")));
        }

        let tool_calls: Vec<ToolCall> = calls.into_values().map(CallAcc::finish).collect();
        let mut content = json!({"text": text});
        if !tool_calls.is_empty() {
            content["tool_calls"] = Value::Array(
                tool_calls
                    .iter()
                    .map(|c| json!({"id": c.id, "name": c.name, "arguments": c.arguments}))
                    .collect(),
            );
        }
        ctx.append(
            EventKind::AssistantMessage,
            json!({"role": "assistant", "content": content}),
        )
        .await
        .map_err(TurnError::fatal)?;

        if tool_calls.is_empty() {
            return Ok(TurnOutcome::FinalAnswer);
        }

        let n = tool_calls.len();
        // Log-before-execute (invariant 1): each `tool/call` event is
        // durable before `invoke` runs.
        for call in tool_calls {
            ctx.append(
                EventKind::ToolCall,
                json!({
                    "role": "assistant",
                    "content": {"id": call.id, "name": call.name, "arguments": call.arguments}
                }),
            )
            .await
            .map_err(TurnError::fatal)?;
            let result = match self.tools.invoke(call.clone()).await {
                Ok(r) => r,
                Err(e) => {
                    // Tool-level failure is an honest error result, not a
                    // loop abort: the model sees the failure and reacts.
                    ToolResult {
                        call_id: call.id.clone(),
                        output: json!({"error": e.to_string()}),
                        is_error: true,
                    }
                }
            };
            ctx.append(
                EventKind::ToolResult,
                json!({
                    "role": "tool",
                    "content": {
                        "call_id": result.call_id,
                        "output": result.output,
                        "is_error": result.is_error
                    }
                }),
            )
            .await
            .map_err(TurnError::fatal)?;
        }
        Ok(TurnOutcome::ToolCalls(n))
    }
}

// ---------------------------------------------------------------------------
// Support types
// ---------------------------------------------------------------------------

impl TurnError {
    fn fatal(e: EngineError) -> Self {
        Self::Fatal(e.into())
    }
}

/// Persists `slot/fallback` payloads through the drain ctx (one durable
/// event per fallback switch, before the fallback provider is called —
/// C8 ordering).
struct CtxSlotSink {
    ctx: DrainCtx,
}

impl SlotEventSink for CtxSlotSink {
    fn persist_fallback(
        &self,
        payload: Value,
    ) -> futures::future::BoxFuture<'static, Result<(), String>> {
        let ctx = self.ctx.clone();
        Box::pin(async move {
            ctx.append(EventKind::SlotFallback, payload)
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
    }
}

/// Accumulates one streamed tool call (openai streaming shape).
struct CallAcc {
    id: String,
    name: String,
    arguments: String,
}

impl CallAcc {
    fn finish(self) -> ToolCall {
        let arguments = serde_json::from_str::<Value>(&self.arguments)
            .unwrap_or_else(|_| Value::String(self.arguments.clone()));
        ToolCall {
            id: self.id,
            name: self.name,
            arguments,
        }
    }
}

/// Folds one streamed delta into the assembled turn. Content fragments
/// concatenate; tool-call fragments accumulate by stream index.
fn assemble_delta(delta: &Value, text: &mut String, calls: &mut BTreeMap<u64, CallAcc>) {
    if let Some(fragment) = delta.get("content").and_then(Value::as_str) {
        text.push_str(fragment);
    }
    if let Some(list) = delta.get("tool_calls").and_then(Value::as_array) {
        for entry in list {
            let index = entry.get("index").and_then(Value::as_u64).unwrap_or(0);
            let slot = calls.entry(index).or_insert_with(|| CallAcc {
                id: String::new(),
                name: String::new(),
                arguments: String::new(),
            });
            if let Some(id) = entry.get("id").and_then(Value::as_str) {
                slot.id = id.to_string();
            }
            if let Some(name) = entry.pointer("/function/name").and_then(Value::as_str) {
                slot.name.push_str(name);
            }
            if let Some(args) = entry.pointer("/function/arguments").and_then(Value::as_str) {
                slot.arguments.push_str(args);
            }
        }
    }
}

/// Converts a projected [`Message`] into the request-body chat shape.
/// String content passes through; structured content (tool transcripts)
/// is serialized compactly — deterministic under the frozen C8
/// serialization (lexicographic keys).
///
/// Public (card C14): `kian session replay` re-derives the model context
/// from the event stream and must rebuild the *exact* chat shape the
/// loop sent, so the A2 byte-identity check compares like with like.
/// This conversion is part of the frozen request-serialization contract
/// (constraint 3) — changing it invalidates every replay comparison and
/// every provider prefix cache.
pub fn to_chat_message(m: &Message) -> ChatMessage {
    let content = match &m.content {
        Value::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    };
    ChatMessage {
        role: m.role.clone(),
        content,
    }
}

/// Maps a provider error onto the classifier's class names (C10
/// vocabulary) for the policy lookup.
fn error_class(e: &ProviderError) -> &'static str {
    match e {
        ProviderError::RateLimited => "rate_limited",
        ProviderError::ContextOverflow => "context_overflow",
        ProviderError::Unavailable(_) | ProviderError::Transport(_) => "provider_down",
        ProviderError::EmptyResponse => CLASS_EMPTY_RESPONSE,
        ProviderError::InvalidResponse(_) => "validation_failed",
        ProviderError::Other(_) => "internal",
    }
}

/// Consults the declarative policy table for `class`. The loop never
/// *execs* the action (disposition is owned by the caller, A4); the match
/// is reported in the soft-failure reason for observability.
fn consult_policy(policy: &dyn RecoveryPolicySource, class: &str) -> Option<RecoveryAction> {
    policy
        .table()
        .into_iter()
        .find(|rule| rule.class == class)
        .map(|rule| rule.action)
}

fn action_note(action: Option<RecoveryAction>) -> String {
    match action {
        Some(a) => format!("; recovery policy action: {a:?}"),
        None => "; no matching recovery policy rule".into(),
    }
}

#[cfg(test)]
mod tests {
    use crate::react::{
        CLASS_EMPTY_RESPONSE, ReActConfig, ReActLoop, assemble_delta, consult_policy,
    };
    use crate::test_support::{NoopPolicy, NoopTools, agent_def};

    // -- pure-unit sections ---------------------------------------------------

    #[test]
    fn delta_assembly_concatenates_content_fragments() {
        let mut text = String::new();
        let mut calls = Default::default();
        for fragment in ["Hello", ", ", "kian"] {
            assemble_delta(
                &serde_json::json!({"content": fragment}),
                &mut text,
                &mut calls,
            );
        }
        assert_eq!(text, "Hello, kian");
        assert!(calls.is_empty());
    }

    #[test]
    fn delta_assembly_accumulates_tool_call_fragments_by_index() {
        let mut text = String::new();
        let mut calls = Default::default();
        assemble_delta(
            &serde_json::json!({"tool_calls": [
                {"index": 0, "id": "call-1", "function": {"name": "fs.read"}}
            ]}),
            &mut text,
            &mut calls,
        );
        assemble_delta(
            &serde_json::json!({"tool_calls": [
                {"index": 0, "function": {"arguments": "{\"pa"}}
            ]}),
            &mut text,
            &mut calls,
        );
        assemble_delta(
            &serde_json::json!({"tool_calls": [
                {"index": 0, "function": {"arguments": "th\": \"a.txt\"}"}}
            ]}),
            &mut text,
            &mut calls,
        );
        let finished: Vec<_> = calls.into_values().map(|c| c.finish()).collect();
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].id, "call-1");
        assert_eq!(finished[0].name, "fs.read");
        assert_eq!(finished[0].arguments, serde_json::json!({"path": "a.txt"}));
    }

    #[test]
    fn policy_consult_matches_rule_by_class() {
        use kian_core::seams::{PolicyRule, RecoveryAction, RecoveryPolicySource};

        struct Table(Vec<PolicyRule>);
        impl RecoveryPolicySource for Table {
            fn table(&self) -> Vec<PolicyRule> {
                self.0.clone()
            }
        }
        let table = Table(vec![
            PolicyRule {
                class: "rate_limited".into(),
                action: RecoveryAction::BackoffRetry { delay_ms: 100 },
            },
            PolicyRule {
                class: CLASS_EMPTY_RESPONSE.into(),
                action: RecoveryAction::InterruptAsk,
            },
        ]);
        assert_eq!(
            consult_policy(&table, "rate_limited"),
            Some(RecoveryAction::BackoffRetry { delay_ms: 100 })
        );
        assert_eq!(
            consult_policy(&table, CLASS_EMPTY_RESPONSE),
            Some(RecoveryAction::InterruptAsk)
        );
        assert_eq!(consult_policy(&table, "no_such_class"), None);
    }

    // -- engine integration sections live in integration_tests.rs -------------

    #[test]
    fn react_config_defaults_are_sane() {
        let cfg = ReActConfig::default();
        assert_eq!(cfg.slot.0, "main");
        assert!(cfg.max_iterations >= 1);
        // constructor smoke: config accessor round-trips
        let react = ReActLoop::new(
            agent_def(),
            std::sync::Arc::new(kian_llm::ProviderRegistry::new()),
            std::sync::Arc::new(NoopTools),
            std::sync::Arc::new(NoopPolicy),
        );
        assert_eq!(react.config().slot.0, "main");
    }
}
