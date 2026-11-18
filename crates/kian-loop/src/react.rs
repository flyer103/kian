//! Built-in ReAct loop.
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
//! belong to the engine.
//!
//! ## Event payload conventions (frozen, additions-only discipline)
//!
//! - `turn/start` / `turn/end`: `{"iteration": <n>,...}` — the iteration
//!   counter, derived from the stream (number of prior `turn/start`s),
//!   never from resident state.
//! - `step/request`: `{"slot", "provider", "model", "iteration", "body"}`
//!   — the A2 audit copy of the request, appended **before** the provider
//!   is called (the provider-level `PersistSink`, additionally covers
//!   fallback-request bytes at the HTTP edge).
//! - `assistant/message`: content `{"text": str, "tool_calls"?: [...]}`.
//! - `tool/call` (logged before execution): content
//!   `{"id", "name", "arguments"}` with role `assistant` (fixture
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
//! owned by the caller).
//!
//! ## Evidence gate on the stop path
//!
//! When mounted via [`ReActLoop::with_evidence_gate`], every terminal stop
//! flow (`Settled` — the final-answer "done" claim — and `Stopped`) passes
//! through [`crate::evidence`] before it is reported: pass appends
//! `evidence/verdict{passed:true}` and lets the stop proceed; fail appends
//! the failed verdict, buys at most `max_retries` extra provider turns,
//! then reports a soft failure the engine settles as `failed` — never
//! `completed` with a failed gate. The gate makes no provider call and
//! never mutates context. Default-off (built-in behavior unchanged).

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt};
use kian_core::context::{Message, derive_model_context};
use kian_core::events::{Event, EventKind};
use kian_core::model::{AgentDef, Permission, SlotName};
use kian_core::seams::{
    LoopDriver, LoopOutcome, ModelRequest, ProviderError, RecoveryAction, RecoveryPolicySource,
    ToolCall, ToolProvider, ToolResult,
};
use kian_llm::provider::{ChatMessage, WireFunctionCall, WireToolCall, build_chat_body};
use kian_llm::slots::{SessionSlots, SlotBinding, SlotError, SlotResolver};
use kian_llm::turn::{ProviderRegistry, SlotEventSink, complete_turn};
use kian_orchestrator::{DrainCtx, EngineError};
use serde_json::{Value, json};

use crate::evidence::{
    BUILTIN_GATE_NAME, EvidenceGateConfig, evaluate_evidence_gate, verdict_payload,
};

/// Recovery-policy class for an empty model response (constraint 7).
pub const CLASS_EMPTY_RESPONSE: &str = "empty_response";

/// Iteration budget default: stops honest loops that never converge.
pub const DEFAULT_MAX_ITERATIONS: usize = 32;

/// How a loop's iteration budget is scoped (ADR-024).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BudgetScope {
    /// The budget counts every `turn/start` ever committed on the session
    /// (the legacy behavior; correct for short-lived worker/plain
    /// sessions).
    #[default]
    Lifetime,
    /// The budget counts the current episode's turns only (resident loops:
    /// the Router and the watchdog). Exhaustion parks the resident; it
    /// never ends the session.
    PerEpisode,
}

impl BudgetScope {
    /// The budget counter this scope compares against the limit.
    #[must_use]
    fn used(self, lifetime_iteration: usize, episode_iterations: usize) -> usize {
        match self {
            Self::Lifetime => lifetime_iteration,
            Self::PerEpisode => episode_iterations,
        }
    }
}

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
    /// so the budget survives episodes and restarts — the legacy scope).
    /// With [`BudgetScope::PerEpisode`] (resident loops, ADR-024) the same
    /// value bounds one episode instead.
    pub max_iterations: usize,
    /// Whether the budget is lifetime-scoped (default) or per episode.
    pub budget_scope: BudgetScope,
    /// Per-call tool permission table (ADR-015): tool name →
    /// `allow`/`deny`. **Absent = empty = allow-all**, and with an empty
    /// table the `tool/call` payload stays byte-identical to the legacy
    /// shape (no provenance key at all). A non-empty table decides every
    /// call — provenance rides the payload on allow, not only on deny.
    pub permissions: BTreeMap<String, Permission>,
}

impl Default for ReActConfig {
    fn default() -> Self {
        Self {
            slot: SlotName("main".into()),
            max_iterations: DEFAULT_MAX_ITERATIONS,
            budget_scope: BudgetScope::Lifetime,
            permissions: BTreeMap::new(),
        }
    }
}

struct Inner {
    registry: Arc<ProviderRegistry>,
    tools: Arc<dyn ToolProvider>,
    policy: Arc<dyn RecoveryPolicySource>,
    agent: AgentDef,
    config: ReActConfig,
    /// evidence gate on the stop path; default (disabled) leaves
    /// built-in behavior untouched.
    gate: EvidenceGateConfig,
    /// The injected hook ports (PLUGIN-SPEC §4 rows (b)–(f)); `None`
    /// mounts no consultation and is byte-identical to the pre-plugin
    /// loop. The loop never builds a bus — assembly injects it.
    hooks: Option<Arc<dyn kian_plugin::hooks::HookPorts>>,
    /// The §9.2 session-consistency guard, called once per drive (one
    /// drain begin = one claim boundary): record the set digest at
    /// first sight, recompute and compare after. `None` (every legacy
    /// constructor) is a no-op — byte-identical (§13.3). This is not
    /// an eighth interception port (§4's seven are frozen); it is the
    /// guard's own seam.
    set_guard: Option<Arc<dyn kian_plugin::hooks::SessionSetGuard>>,
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

    /// [`ReActLoop::new`] with explicit loop tuning. The evidence gate is
    /// disabled (built-in behavior).
    pub fn with_config(
        agent: AgentDef,
        registry: Arc<ProviderRegistry>,
        tools: Arc<dyn ToolProvider>,
        policy: Arc<dyn RecoveryPolicySource>,
        config: ReActConfig,
    ) -> Self {
        Self::with_evidence_gate(
            agent,
            registry,
            tools,
            policy,
            config,
            EvidenceGateConfig::default(),
        )
    }

    /// [`ReActLoop::with_config`] with an evidence gate mounted on
    /// the stop path. A disabled [`EvidenceGateConfig`] behaves exactly
    /// like [`ReActLoop::with_config`].
    pub fn with_evidence_gate(
        agent: AgentDef,
        registry: Arc<ProviderRegistry>,
        tools: Arc<dyn ToolProvider>,
        policy: Arc<dyn RecoveryPolicySource>,
        config: ReActConfig,
        gate: EvidenceGateConfig,
    ) -> Self {
        Self::with_hooks(agent, registry, tools, policy, config, gate, None)
    }

    /// [`ReActLoop::with_evidence_gate`] plus the injected hook ports
    /// (PLUGIN-SPEC §4): the bus instance assembly owns, consulted at
    /// this loop's five ReAct sites — `turn/request`, `turn/response`,
    /// `tool/pre`, `tool/post`, `recover/decided`. `None` (every
    /// legacy constructor) mounts no consultation at all.
    pub fn with_hooks(
        agent: AgentDef,
        registry: Arc<ProviderRegistry>,
        tools: Arc<dyn ToolProvider>,
        policy: Arc<dyn RecoveryPolicySource>,
        config: ReActConfig,
        gate: EvidenceGateConfig,
        hooks: Option<Arc<dyn kian_plugin::hooks::HookPorts>>,
    ) -> Self {
        Self::with_hooks_and_guard(agent, registry, tools, policy, config, gate, hooks, None)
    }

    /// [`ReActLoop::with_hooks`] plus the §9.2 session-set guard — the
    /// assembly mounts both from the same bus instance. The guard runs
    /// once per drive (the drain/claim boundary), before the first
    /// iteration; the frame carries this drain's session and epoch with
    /// the honest floor binding (the loop holds no worker binding,
    /// §7.3 — an unbound session resolves the server scope, §8.2).
    /// Eight named arguments, same shape as the sibling constructors —
    /// a bundle struct would only rename the same list.
    #[allow(clippy::too_many_arguments)]
    pub fn with_hooks_and_guard(
        agent: AgentDef,
        registry: Arc<ProviderRegistry>,
        tools: Arc<dyn ToolProvider>,
        policy: Arc<dyn RecoveryPolicySource>,
        config: ReActConfig,
        gate: EvidenceGateConfig,
        hooks: Option<Arc<dyn kian_plugin::hooks::HookPorts>>,
        set_guard: Option<Arc<dyn kian_plugin::hooks::SessionSetGuard>>,
    ) -> Self {
        Self(Arc::new(Inner {
            registry,
            tools,
            policy,
            agent,
            config,
            gate,
            hooks,
            set_guard,
        }))
    }

    #[must_use]
    pub fn config(&self) -> &ReActConfig {
        &self.0.config
    }

    /// The mounted evidence-gate config. The shared verdict counters
    /// inside are the metrics surface for `GET /metrics` (wired by the
    /// assembly).
    #[must_use]
    pub fn evidence_gate(&self) -> &EvidenceGateConfig {
        &self.0.gate
    }
}

// ---------------------------------------------------------------------------
// LoopDriver
// ---------------------------------------------------------------------------

/// unfold state: one step == one full provider turn (with its tool
/// dispatches). `done` ends the stream after the terminal outcome was
/// yielded; `begun` fires the drain-boundary §9.2 guard once, on the
/// first poll.
struct LoopState {
    ctx: DrainCtx,
    done: bool,
    begun: bool,
    /// Per-episode turn count for [`BudgetScope::PerEpisode`]; reset per
    /// drive stream (ADR-024).
    drain_iterations: usize,
}

impl LoopDriver for ReActLoop {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        let inner = Arc::clone(&self.0);
        futures::stream::unfold(
            LoopState {
                ctx,
                done: false,
                begun: false,
                drain_iterations: 0,
            },
            move |mut st| {
                let inner = Arc::clone(&inner);
                async move {
                    if st.done {
                        return None;
                    }
                    // §9.2 steps 1–4 at the drain/claim boundary: the
                    // guard records the set digest at first sight and
                    // re-checks it on every claim this process serves.
                    // A no-op without a mounted guard. The frame's
                    // `worker` is the session's durable `exec_worker`
                    // binding (§7.3/§8.2) derived from its own stream —
                    // a bound worker session records its narrowed set's
                    // digest, never the server scope's. Fired on the
                    // first poll (the drain's first step) because the
                    // stream read is async.
                    if !st.begun {
                        st.begun = true;
                        if let Some(guard) = inner.set_guard.as_ref() {
                            let worker = match st.ctx.events().await {
                                Ok(events) => kian_plugin::hooks::durable_worker_binding(
                                    &events,
                                    &st.ctx.session(),
                                )
                                .unwrap_or_default(),
                                // An unreadable stream cannot prove a
                                // binding; the honest floor is §7.3's
                                // empty string, never a guess.
                                Err(_) => String::new(),
                            };
                            guard.session_begin(kian_plugin::hooks::FrameInfo {
                                session: st.ctx.session(),
                                worker,
                                seq: 0,
                                epoch: Some(st.ctx.epoch().0),
                            });
                        }
                    }
                    let outcome = match inner.iteration(&st.ctx, &mut st.drain_iterations).await {
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
            },
        )
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

/// One provider turn's terminal state plus the audit facts the
/// `turn/end` payload needs: the last non-null provider usage
/// observed across the whole turn — including an attempt that emitted
/// usage before failing — travels with every outcome variant.
struct TurnResult {
    result: Result<TurnOutcome, TurnError>,
    usage: Option<Value>,
}

enum TurnError {
    /// Soft failure — reported to the engine as `LoopOutcome::SoftFailed`.
    /// Carries the structured recovery audit for `turn/end`
    /// (`{"class", "action"}`).
    Soft { reason: String, audit: Value },
    /// Hard error (store/append failure): honest `failed`, never retried
    /// by the loop. Also carries the recovery audit.
    Fatal { error: LoopError, audit: Value },
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
    async fn iteration(&self, ctx: &DrainCtx, drain_iterations: &mut usize) -> Flow {
        let flow = match self.run_iteration(ctx, drain_iterations).await {
            Ok(flow) => flow,
            Err(e) => return Flow::Soft(format!("loop error: {e}")),
        };
        // terminal stop flows pass through the evidence gate before
        // they are reported (a disabled gate is a no-op pass-through).
        match flow {
            Flow::Settled | Flow::Stopped(_) => self.apply_evidence_gate(ctx, flow).await,
            other => other,
        }
    }

    /// The evidence gate on the loop's stop path. Evaluates the
    /// configured checks as pure predicates over the session's own event
    /// stream — never a provider call, never a context mutation — and
    /// appends one `evidence/verdict` event:
    ///
    /// - pass → the stop proceeds; the verdict is the last event before
    ///   the engine's end write;
    /// - fail → the loop continues (`Flow::TurnDone`) while
    ///   `attempt < max_retries`, else reports a soft failure — the
    ///   engine settles it `failed`, never `completed` (constraint 7).
    ///
    /// The gate only reports; disposition stays with the engine.
    async fn apply_evidence_gate(&self, ctx: &DrainCtx, flow: Flow) -> Flow {
        if !self.gate.enabled {
            return flow;
        }
        let events = match ctx.events().await {
            Ok(events) => events,
            Err(e) => {
                return Flow::Soft(format!("evidence gate: stream read failed: {e}"));
            }
        };
        let verdict = evaluate_evidence_gate(BUILTIN_GATE_NAME, &self.gate.checks, &events);
        let payload = verdict_payload(BUILTIN_GATE_NAME, &verdict);
        if let Err(e) = ctx.append(EventKind::EvidenceVerdict, payload).await {
            return Flow::Soft(format!("evidence gate: verdict append failed: {e}"));
        }
        if verdict.passed {
            self.gate.counters.record_passed();
            flow
        } else {
            self.gate.counters.record_failed();
            if verdict.attempt < self.gate.max_retries {
                Flow::TurnDone
            } else {
                Flow::Soft(format!(
                    "evidence gate `{BUILTIN_GATE_NAME}` failed after {} attempt(s)",
                    verdict.attempt + 1
                ))
            }
        }
    }

    // -- hook-port dispatch (PLUGIN-SPEC §4 rows (b)–(f)) ------------------
    //
    // Every helper is a no-op without an injected bus, and every view
    // is built from the exact facts the site already holds — no extra
    // store reads, no re-serialization of model-visible bytes (the
    // digest fields describe the persisted bytes; nothing is rebuilt).

    /// The frame identity of a drain-phase dispatch (§7.3): this
    /// drain's session and fencing epoch, `seq` is the last append
    /// this drain observed, and `worker` is the session's durable
    /// `exec_worker` binding derived from its own stream — the loop
    /// holds no resident binding, so a bound worker session narrows
    /// its plugin set (§8.2) while an unbound session keeps the honest
    /// floor (§7.3's empty string). The events read is the engine's
    /// cached one, and the model-visible request bytes are untouched —
    /// frame identity is `#[serde(skip)]`d metadata (§12.1).
    async fn hook_frame(ctx: &DrainCtx, seq: u64) -> Option<kian_plugin::hooks::FrameInfo> {
        let worker = match ctx.events().await {
            Ok(events) => kian_plugin::hooks::durable_worker_binding(&events, &ctx.session())
                .unwrap_or_default(),
            // An unreadable stream cannot prove a binding; the honest
            // floor is §7.3's empty string, never a guess.
            Err(_) => String::new(),
        };
        Some(kian_plugin::hooks::FrameInfo {
            session: ctx.session(),
            worker,
            seq,
            epoch: Some(ctx.epoch().0),
        })
    }

    /// Row (b) — `turn/request` (§4.2): observe-only, exactly once per
    /// provider turn, on the persisted primary body, immediately after
    /// the `step/request` append returns `Ok`.
    // The view's normative fields arrive as separate borrowed parts of
    // the turn's state; a tuple struct would only rename the same list.
    #[allow(clippy::too_many_arguments)]
    async fn observe_request(
        &self,
        ctx: &DrainCtx,
        binding: &SlotBinding,
        chat_len: usize,
        schemas: &[kian_core::seams::ToolSchema],
        iteration: usize,
        body: &Value,
        step_request_seq: u64,
    ) {
        use kian_plugin::hooks::{RequestMeta, serialized, sha256_hex};
        let Some(hooks) = self.hooks.as_ref() else {
            return;
        };
        let bytes = serialized(body);
        let meta = RequestMeta {
            slot: self.config.slot.0.clone(),
            provider: binding.target.provider.clone(),
            model: binding.target.model.clone(),
            iteration: iteration as u64,
            request_bytes: bytes.len() as u64,
            request_sha256: sha256_hex(&bytes),
            message_count: chat_len as u64,
            tool_names: schemas.iter().map(|s| s.name.clone()).collect(),
            tool_schema_sha256: sha256_hex(&serialized(&Value::Array(
                schemas
                    .iter()
                    .map(|s| serde_json::to_value(s).unwrap_or(Value::Null))
                    .collect(),
            ))),
            timing: "pre_send".to_string(),
            step_request_seq,
            frame: Self::hook_frame(ctx, step_request_seq).await,
        };
        hooks.request(&meta).await;
    }

    /// Row (c) — `turn/response` (§4.3): observe-only, once per settled
    /// provider turn, never per chunk. `error_audit` is the
    /// `turn/end`-vocabulary `{class, action}` object the same path
    /// already builds, when the outcome is a failure.
    #[allow(clippy::too_many_arguments)] // settled-turn state, borrowed in parts
    async fn observe_response(
        &self,
        ctx: &DrainCtx,
        iteration: usize,
        seq: u64,
        outcome: &str,
        tool_calls: u64,
        text_bytes: u64,
        reasoning_bytes: u64,
        usage: Option<&Value>,
        error_audit: Option<&Value>,
    ) {
        use kian_plugin::hooks::{ResponseView, TurnErrorAudit};
        let Some(hooks) = self.hooks.as_ref() else {
            return;
        };
        let error = error_audit.and_then(|audit| {
            Some(TurnErrorAudit {
                class: audit.get("class")?.as_str()?.to_string(),
                action: audit.get("action")?.as_str()?.to_string(),
            })
        });
        let view = ResponseView {
            iteration: iteration as u64,
            outcome: outcome.to_string(),
            tool_calls,
            text_bytes,
            reasoning_bytes,
            usage: usage.cloned().unwrap_or(Value::Null),
            error,
            timing: "settled".to_string(),
            frame: Self::hook_frame(ctx, seq).await,
        };
        hooks.response(&view).await;
    }

    /// Row (d) — `tool/pre` (§4.4 step 2): consulted after the
    /// declarative gate has decided and before the `tool/call` append /
    /// `invoke`. Only a verdict from a `veto:tool` holder (or its
    /// fail-closed default) denies; otherwise the declarative gate's
    /// decision stands.
    async fn consult_tool_pre(
        &self,
        ctx: &DrainCtx,
        call: &ToolCall,
        iteration: usize,
        seq: u64,
        decision: Option<PermissionDecision>,
    ) -> kian_plugin::hooks::ToolVerdict {
        use kian_plugin::hooks::{ToolCallView, bounded_value, serialized, sha256_hex};
        let Some(hooks) = self.hooks.as_ref() else {
            return kian_plugin::hooks::ToolVerdict::Allow;
        };
        let bytes = serialized(&call.arguments);
        let (arguments, arguments_truncated) = bounded_value(&call.arguments);
        let view = ToolCallView {
            call_id: call.id.clone(),
            name: call.name.clone(),
            iteration: iteration as u64,
            arguments,
            arguments_bytes: bytes.len() as u64,
            arguments_sha256: sha256_hex(&bytes),
            arguments_truncated,
            permission: permission_view(call, decision),
            surface: "react".to_string(),
            frame: Self::hook_frame(ctx, seq).await,
        };
        hooks.tool_pre(&view).await
    }

    /// Row (d′) — `tool/post` (§4.4 step 5): observe-only after the
    /// `tool/result` append, on the executed, declared-denied and
    /// hook-denied arms alike (the uniform surface).
    async fn observe_tool_post(&self, ctx: &DrainCtx, result: &ToolResult, name: &str, seq: u64) {
        use kian_plugin::hooks::{RedactionReport, ToolPostView, serialized, sha256_hex};
        let Some(hooks) = self.hooks.as_ref() else {
            return;
        };
        let bytes = serialized(&result.output);
        let view = ToolPostView {
            call_id: result.call_id.clone(),
            name: name.to_string(),
            is_error: result.is_error,
            output_bytes: bytes.len() as u64,
            output_sha256: sha256_hex(&bytes),
            redactions: result
                .redactions
                .iter()
                .map(|r| RedactionReport {
                    kind: serde_json::to_value(r.kind)
                        .ok()
                        .and_then(|v| v.as_str().map(str::to_string))
                        .unwrap_or_default(),
                    name: r.name.clone(),
                    count: r.count,
                })
                .collect(),
            frame: Self::hook_frame(ctx, seq).await,
        };
        hooks.tool_post(&view).await;
    }

    /// Row (f) — `recover/decided` (§4.5): observe-only at each
    /// class → action resolution this loop performs; the audit object
    /// is the one the `turn/end` payload already carries.
    async fn observe_recover(&self, ctx: &DrainCtx, iteration: usize, seq: u64, audit: &Value) {
        let Some(hooks) = self.hooks.as_ref() else {
            return;
        };
        let view = kian_plugin::hooks::RecoveryView {
            error_class: audit
                .get("class")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            action: audit
                .get("action")
                .and_then(Value::as_str)
                .unwrap_or("none")
                .to_string(),
            iteration: iteration as u64,
            frame: Self::hook_frame(ctx, seq).await,
        };
        hooks.recover(&view).await;
    }

    /// The fatal-turn observation pair (rows (c) + (f)): an
    /// append-failure turn settles as `error` and its recovery audit
    /// observes like any other resolution.
    #[allow(clippy::too_many_arguments)] // fatal-turn state, borrowed in parts
    async fn observe_fatal(
        &self,
        ctx: &DrainCtx,
        iteration: usize,
        seq: u64,
        audit: &Value,
        text_bytes: u64,
        reasoning_bytes: u64,
        usage: Option<&Value>,
    ) {
        self.observe_recover(ctx, iteration, seq, audit).await;
        self.observe_response(
            ctx,
            iteration,
            seq,
            "error",
            0,
            text_bytes,
            reasoning_bytes,
            usage,
            Some(audit),
        )
        .await;
    }

    async fn run_iteration(
        &self,
        ctx: &DrainCtx,
        drain_iterations: &mut usize,
    ) -> Result<Flow, LoopError> {
        // 1. Promote newly eligible inputs at this safe boundary (we are
        //    between provider turns here; DrainCtx refuses otherwise —
        //    steer never interleaves an in-flight turn).
        let taken = ctx.take_eligible_inputs().await?;

        // 2. Rebuild the truth and derive the model context (A2/A3).
        let mut events = ctx.events().await?;
        let binding = self.binding_for(ctx, &mut events).await?;
        let messages = derive_model_context(&events)?;

        // (ADR-025) A resident episode with no eligible input and a context
        // that ends on an assistant message has nothing to judge: settle
        // (the RouterDriver parks) without spending a provider call. A
        // tool-result continuation ends on `tool` and still proceeds; a new
        // input is `taken` and still proceeds. Non-resident loops are
        // untouched.
        if resident_empty_batch(
            self.config.budget_scope,
            taken.is_empty(),
            messages.last().map(|m| m.role.as_str()),
        ) {
            tracing::debug!(
                session = %ctx.session().0,
                "resident empty batch: settling without a provider call (ADR-025)"
            );
            return Ok(Flow::Settled);
        }

        // 3. Iteration counter — derived from the stream, so the budget
        //    survives episodes and restarts (constraint 1 in spirit).
        let iteration = events
            .iter()
            .filter(|ev| matches!(ev.kind, EventKind::TurnStart))
            .count();
        let used = self.config.budget_scope.used(iteration, *drain_iterations);
        if used >= self.config.max_iterations {
            let reason = match self.config.budget_scope {
                BudgetScope::Lifetime => {
                    format!("iteration budget exhausted after {iteration} turns")
                }
                BudgetScope::PerEpisode => format!(
                    "{} after {used} turns",
                    kian_core::seams::EPISODE_BUDGET_STOP_REASON_PREFIX
                ),
            };
            return Ok(Flow::Stopped(reason));
        }
        if self.config.budget_scope == BudgetScope::PerEpisode {
            *drain_iterations += 1;
        }

        // 4. The provider turn. begin/end bracket every outcome (including
        //    soft failures) so turn events stay balanced in the stream.
        ctx.begin_turn_with(json!({"iteration": iteration})).await?;
        let turn = self
            .provider_turn(ctx, &binding, &messages, iteration)
            .await;
        let usage = turn.usage.as_ref();
        let flow = match turn.result {
            Ok(TurnOutcome::ToolCalls(n)) => {
                let mut payload = json!({"iteration": iteration, "tool_calls": n});
                if let Some(u) = usage {
                    payload["usage"] = u.clone();
                }
                ctx.end_turn_with(payload).await?;
                return Ok(Flow::TurnDone);
            }
            // Final answer + empty inbox = honest settle. New inputs
            // (a folded steer) keep the loop going instead.
            Ok(TurnOutcome::FinalAnswer) => {
                if ctx.has_eligible_inputs().await? {
                    Flow::TurnDone
                } else {
                    Flow::Settled
                }
            }
            Err(TurnError::Soft { reason, audit }) => {
                let mut payload = json!({
                    "iteration": iteration,
                    "outcome": "soft_failure",
                    "error": audit,
                });
                if let Some(u) = usage {
                    payload["usage"] = u.clone();
                }
                let _ = ctx.end_turn_with(payload).await;
                return Ok(Flow::Soft(reason));
            }
            Err(TurnError::Fatal { error, audit }) => {
                let mut payload = json!({
                    "iteration": iteration,
                    "outcome": "error",
                    "error": audit,
                });
                if let Some(u) = usage {
                    payload["usage"] = u.clone();
                }
                let _ = ctx.end_turn_with(payload).await;
                return Err(error);
            }
        };
        let mut payload = json!({"iteration": iteration, "outcome": "final_answer"});
        if let Some(u) = usage {
            payload["usage"] = u.clone();
        }
        ctx.end_turn_with(payload).await?;
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
    /// (with fallback traversal), assemble the streamed response, then
    /// either dispatch tool calls (log-before-execute) or return a final
    /// answer. The returned [`TurnResult`] carries the last non-null
    /// provider usage observed across the turn, on every path.
    async fn provider_turn(
        &self,
        ctx: &DrainCtx,
        binding: &SlotBinding,
        messages: &[Message],
        iteration: usize,
    ) -> TurnResult {
        let target = binding.target.clone();

        // Deterministic tool order (the request body freezes the tools
        // array order — serialization contract).
        let mut schemas = self.tools.schemas();
        schemas.sort_by(|a, b| a.name.cmp(&b.name));
        let chat: Vec<ChatMessage> = messages.iter().filter_map(to_chat_message).collect();

        // A2: the audit copy of the exact request is durable BEFORE the
        // provider is called.
        let body = build_chat_body(&target.model, &chat, &schemas);
        let step_request = ctx
            .append(
                EventKind::StepRequest,
                json!({
                    "slot": self.config.slot.0,
                    "provider": target.provider,
                    "model": target.model,
                    "iteration": iteration,
                    "body": body,
                }),
            )
            .await;
        let step_request_seq = match step_request {
            Ok(ev) => ev.seq,
            Err(e) => {
                let fatal = TurnError::fatal(e, self.policy.as_ref());
                // Rows (c)+(f): the turn settles `error` with its audit.
                if let TurnError::Fatal { audit, .. } = &fatal {
                    self.observe_fatal(ctx, iteration, 0, audit, 0, 0, None)
                        .await;
                }
                return TurnResult {
                    result: Err(fatal),
                    usage: None,
                };
            }
        };
        // Port (b) — `turn/request` (§4.2): fires immediately after the
        // persisted primary body exists, exactly once per provider turn
        // (never per fallback attempt — `complete_turn`'s rebuilt bodies
        // are provider-layer internals, not second `step/request`s).
        self.observe_request(
            ctx,
            binding,
            chat.len(),
            &schemas,
            iteration,
            &body,
            step_request_seq,
        )
        .await;

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

        // Assemble the streamed deltas. Usage-only chunks carry a
        // Null delta — they never assemble content, but the last
        // non-null usage is captured across the whole turn, including an
        // attempt that emitted usage before failing.
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut calls: BTreeMap<u64, CallAcc> = BTreeMap::new();
        let mut usage: Option<Value> = None;
        let mut stream_error: Option<ProviderError> = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => {
                    if let Some(u) = chunk.usage.as_ref().filter(|u| !u.is_null()) {
                        usage = Some(u.clone()); // last wins
                    }
                    if !chunk.delta.is_null() {
                        assemble_delta(&chunk.delta, &mut text, &mut reasoning, &mut calls);
                    }
                }
                Err(e) => {
                    stream_error = Some(e);
                    break;
                }
            }
        }
        // Byte counts for the §3.2.4 view fields — captured before the
        // content assembly moves the strings.
        let text_bytes = text.len() as u64;
        let reasoning_bytes = reasoning.len() as u64;
        if let Some(e) = stream_error {
            let class = error_class(&e);
            let action = consult_policy(self.policy.as_ref(), class);
            let note = action_note(action);
            let audit = recovery_audit(self.policy.as_ref(), provider_error_class(&e));
            // Rows (f)+(c): the resolution observes, the settled turn
            // reports `soft_failure` with the same audit object.
            self.observe_recover(ctx, iteration, step_request_seq, &audit)
                .await;
            self.observe_response(
                ctx,
                iteration,
                step_request_seq,
                "soft_failure",
                0,
                text_bytes,
                reasoning_bytes,
                usage.as_ref(),
                Some(&audit),
            )
            .await;
            return TurnResult {
                result: Err(TurnError::Soft {
                    reason: format!("provider error ({class}): {e}{note}"),
                    audit,
                }),
                usage,
            };
        }
        if text.is_empty() && calls.is_empty() {
            // Constraint 7: an empty model response is a soft failure
            // routed to the recovery policy — never a completion.
            let action = consult_policy(self.policy.as_ref(), CLASS_EMPTY_RESPONSE);
            let note = action_note(action);
            let audit = recovery_audit(
                self.policy.as_ref(),
                kian_orchestrator::recovery::ErrorClass::EmptyResponse,
            );
            self.observe_recover(ctx, iteration, step_request_seq, &audit)
                .await;
            self.observe_response(
                ctx,
                iteration,
                step_request_seq,
                "soft_failure",
                0,
                0,
                0,
                usage.as_ref(),
                Some(&audit),
            )
            .await;
            return TurnResult {
                result: Err(TurnError::Soft {
                    reason: format!("empty model response{note}"),
                    audit,
                }),
                usage,
            };
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
        if !reasoning.is_empty() {
            // ADR-022: thinking-mode reasoning is context — persisted
            // verbatim and replayed by `assistant_message` below.
            content["reasoning_content"] = Value::String(reasoning);
        }
        if let Err(e) = ctx
            .append(
                EventKind::AssistantMessage,
                json!({"role": "assistant", "content": content}),
            )
            .await
        {
            let fatal = TurnError::fatal(e, self.policy.as_ref());
            if let TurnError::Fatal { audit, .. } = &fatal {
                self.observe_fatal(
                    ctx,
                    iteration,
                    step_request_seq,
                    audit,
                    text_bytes,
                    reasoning_bytes,
                    usage.as_ref(),
                )
                .await;
            }
            return TurnResult {
                result: Err(fatal),
                usage,
            };
        }

        if tool_calls.is_empty() {
            // Row (c) — the settled final-answer turn (§4.3).
            self.observe_response(
                ctx,
                iteration,
                step_request_seq,
                "final_answer",
                0,
                text_bytes,
                reasoning_bytes,
                usage.as_ref(),
                None,
            )
            .await;
            return TurnResult {
                result: Ok(TurnOutcome::FinalAnswer),
                usage,
            };
        }

        let n = tool_calls.len();
        // Log-before-execute (invariant 1): each `tool/call` event is
        // durable before `invoke` runs — for BOTH arms. The deny arm
        // logs the call (with its deny provenance) and then never
        // invokes; the honest audit record of the attempted call exists
        // either way.
        for call in tool_calls {
            // (ADR-015): evaluate the per-call permission once,
            // before the `tool/call` append. `None` = no table
            // configured → the legacy byte-identical payload, invoke
            // as today.
            let decision = evaluate_permission(&self.config.permissions, &call.name);
            // Port (d) — `tool/pre` (§4.4 step 2): consulted
            // **unconditionally**, after the declarative table is
            // evaluated and before the `tool/call` append / `invoke` —
            // a policy-denied call is still a call the port is defined
            // over, and the payload already carries the declarative
            // verdict (`permission.evaluated/policy`). A hook denial
            // wins over the declarative one by the existing
            // first-deny order — end-to-end: the hook-denied log AND
            // the honest-error result both name the plugin (§4.4
            // steps 3–4); an abstaining hook leaves the declarative
            // decision standing.
            let hook_verdict = self
                .consult_tool_pre(ctx, &call, iteration, step_request_seq, decision)
                .await;
            let appended = match hook_verdict {
                kian_plugin::hooks::ToolVerdict::Deny {
                    ref plugin,
                    ref reason,
                } => {
                    // §4.4 step 3: the hook denial is logged before any
                    // effect, with `policy:"hook"` provenance naming the
                    // denying plugin (§16.1's payload value — P4 owns
                    // the EVENT-MODEL row).
                    ctx.append(
                        EventKind::ToolCall,
                        tool_call_payload_hook_denied(&call, plugin, reason),
                    )
                    .await
                }
                _ => {
                    ctx.append(EventKind::ToolCall, tool_call_payload(&call, decision))
                        .await
                }
            };
            let call_seq = match appended {
                Ok(ev) => ev.seq,
                Err(e) => {
                    let fatal = TurnError::fatal(e, self.policy.as_ref());
                    if let TurnError::Fatal { audit, .. } = &fatal {
                        self.observe_fatal(
                            ctx,
                            iteration,
                            step_request_seq,
                            audit,
                            text_bytes,
                            reasoning_bytes,
                            usage.as_ref(),
                        )
                        .await;
                    }
                    return TurnResult {
                        result: Err(fatal),
                        usage,
                    };
                }
            };
            // §4.4 step 4: a hook-denied call never reaches
            // `self.tools.invoke`; the model receives the honest-error
            // `tool/result` naming the plugin and its reason. The hook
            // denial also wins the RESULT on the coincident arm (a
            // call the declarative table denies AND a `veto:tool`
            // plugin denies): first-deny order means the plugin's
            // denial is the one the pair records end-to-end — the
            // `tool/call` above (`policy:"hook"`, the plugin's
            // reason) and this `tool/result` name the same denier,
            // never one each.
            if let kian_plugin::hooks::ToolVerdict::Deny {
                ref plugin,
                ref reason,
            } = hook_verdict
            {
                let denied = ToolResult {
                    call_id: call.id.clone(),
                    output: json!({
                        "error": format!("denied by plugin {plugin}: {reason}"),
                        "policy": "hook"
                    }),
                    is_error: true,
                    redactions: Vec::new(),
                };
                if let Err(e) = ctx
                    .append(EventKind::ToolResult, tool_result_payload(&denied))
                    .await
                {
                    let fatal = TurnError::fatal(e, self.policy.as_ref());
                    if let TurnError::Fatal { audit, .. } = &fatal {
                        self.observe_fatal(
                            ctx,
                            iteration,
                            call_seq,
                            audit,
                            text_bytes,
                            reasoning_bytes,
                            usage.as_ref(),
                        )
                        .await;
                    }
                    return TurnResult {
                        result: Err(fatal),
                        usage,
                    };
                }
                self.observe_tool_post(ctx, &denied, &call.name, call_seq)
                    .await;
                continue;
            }
            // Declarative deny (the table alone denied the call): no
            // tool side effect either. The model sees an honest error
            // result next turn and reacts — a denied call is a tool
            // failure, never a loop abort.
            if let Some(denied) = declared_denial(&call, decision) {
                if let Err(e) = ctx
                    .append(EventKind::ToolResult, tool_result_payload(&denied))
                    .await
                {
                    let fatal = TurnError::fatal(e, self.policy.as_ref());
                    if let TurnError::Fatal { audit, .. } = &fatal {
                        self.observe_fatal(
                            ctx,
                            iteration,
                            call_seq,
                            audit,
                            text_bytes,
                            reasoning_bytes,
                            usage.as_ref(),
                        )
                        .await;
                    }
                    return TurnResult {
                        result: Err(fatal),
                        usage,
                    };
                }
                // Port (d′) — `tool/post` fires on the declared-denial
                // arm too (§4.4 step 5: the uniform surface).
                self.observe_tool_post(ctx, &denied, &call.name, call_seq)
                    .await;
                continue;
            }
            let result = match self.tools.invoke(call.clone()).await {
                Ok(r) => r,
                Err(e) => {
                    // Tool-level failure is an honest error result, not a
                    // loop abort: the model sees the failure and reacts.
                    ToolResult {
                        call_id: call.id.clone(),
                        output: json!({"error": e.to_string()}),
                        is_error: true,
                        redactions: Vec::new(),
                    }
                }
            };
            // redaction provenance rides the payload OUTSIDE
            // `content` — the model-visible wire shape (and therefore
            // request bytes and prefix cache) never sees it; the key is
            // omitted entirely when nothing was redacted.
            let result_seq = match ctx
                .append(EventKind::ToolResult, tool_result_payload(&result))
                .await
            {
                Ok(ev) => ev.seq,
                Err(e) => {
                    let fatal = TurnError::fatal(e, self.policy.as_ref());
                    if let TurnError::Fatal { audit, .. } = &fatal {
                        self.observe_fatal(
                            ctx,
                            iteration,
                            call_seq,
                            audit,
                            text_bytes,
                            reasoning_bytes,
                            usage.as_ref(),
                        )
                        .await;
                    }
                    return TurnResult {
                        result: Err(fatal),
                        usage,
                    };
                }
            };
            // Port (d′) — `tool/post` after the executed call's
            // `tool/result` append (§4.4 step 5).
            self.observe_tool_post(ctx, &result, &call.name, result_seq)
                .await;
        }
        // Row (c) — the settled tool-calls turn (§4.3): dispatched once
        // per settled provider turn, after its tool dispatches.
        self.observe_response(
            ctx,
            iteration,
            step_request_seq,
            "tool_calls",
            n as u64,
            text_bytes,
            reasoning_bytes,
            usage.as_ref(),
            None,
        )
        .await;
        TurnResult {
            result: Ok(TurnOutcome::ToolCalls(n)),
            usage,
        }
    }
}

// ---------------------------------------------------------------------------
// Support types
// ---------------------------------------------------------------------------

impl TurnError {
    /// A hard (store/append) failure with its recovery audit. The
    /// class follows the existing pure classifier's vocabulary
    /// (`Engine` → `store_failure`; context/slot errors are outside it
    /// and classify as `unknown`), the action comes from the table.
    fn fatal(e: EngineError, policy: &dyn RecoveryPolicySource) -> Self {
        let error = LoopError::Engine(e);
        let audit = recovery_audit(policy, loop_error_class(&error));
        Self::Fatal { error, audit }
    }
}

/// The class of a loop-level hard error, aligned with the existing pure
/// classifier (`recovery::classify`): engine failures are store
/// failures; context-projection and slot-resolution errors sit outside
/// its vocabulary and classify as `unknown` — the same result
/// `classify(RecoveryError::Unknown(_))` yields.
fn loop_error_class(e: &LoopError) -> kian_orchestrator::recovery::ErrorClass {
    match e {
        LoopError::Engine(_) => kian_orchestrator::recovery::ErrorClass::StoreFailure,
        LoopError::Context(_) | LoopError::Slot(_) => {
            kian_orchestrator::recovery::ErrorClass::Unknown
        }
    }
}

/// The class of a provider error via the existing pure classifier
/// `classify(RecoveryError::Provider(e))`.
fn provider_error_class(e: &ProviderError) -> kian_orchestrator::recovery::ErrorClass {
    kian_orchestrator::recovery::classify(
        &kian_orchestrator::recovery::RecoveryError::Provider(e.clone()),
        &kian_orchestrator::recovery::ClassifyCtx { attempt: 0 },
    )
}

/// The `turn/end` `error` audit object: the pure classifier's class
/// name plus the declarative policy's disposition name — `"none"` when
/// no rule covers the class (the honest no-rule spelling).
fn recovery_audit(
    policy: &dyn RecoveryPolicySource,
    class: kian_orchestrator::recovery::ErrorClass,
) -> Value {
    let action = consult_policy(policy, class.name());
    json!({
        "class": class.name(),
        "action": action_name(action.as_ref()),
    })
}

/// The disposition-name spelling of a policy action (the `ActionDef`
/// names of `kian.yaml`), or `"none"` when no rule matched.
fn action_name(action: Option<&RecoveryAction>) -> &'static str {
    match action {
        None => "none",
        Some(RecoveryAction::BackoffRetry { .. }) => "backoff_retry",
        Some(RecoveryAction::CompressThenRetry) => "compress_then_retry",
        Some(RecoveryAction::Failover) => "failover",
        Some(RecoveryAction::InterruptAsk) => "interrupt_ask",
        Some(RecoveryAction::Fail) => "fail",
    }
}

/// Persists `slot/fallback` payloads through the drain ctx (one durable
/// event per fallback switch, before the fallback provider is called —
/// ordering).
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
fn assemble_delta(
    delta: &Value,
    text: &mut String,
    reasoning: &mut String,
    calls: &mut BTreeMap<u64, CallAcc>,
) {
    if let Some(fragment) = delta.get("content").and_then(Value::as_str) {
        text.push_str(fragment);
    }
    if let Some(fragment) = delta.get("reasoning_content").and_then(Value::as_str) {
        reasoning.push_str(fragment);
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

/// Converts a projected [`Message`] into the request-body chat shape
/// (dialect v2). Plain string content passes through unchanged
/// (byte-identical to dialect v1); structured content is translated to
/// the strict openai wire shape — anything else is serialized compactly,
/// deterministic under the frozen serialization (lexicographic keys).
///
/// - `role:"tool"` `{call_id, output, is_error}` → `{role:"tool",
///   tool_call_id, content: <output as a JSON string>}` (the `is_error`
///   flag stays in the event stream; the wire carries the output only,
///   the shape verified live against the real DeepSeek API).
/// - assistant `{"text", "tool_calls": [...]}` → `{role:"assistant",
///   content: <text or null>, tool_calls: [{id, type:"function",
///   function:{name, arguments: <JSON string>}}]}`.
/// - assistant `{"id", "name", "arguments"}` (the per-call `tool/call`
///   transcript duplicate) → **dropped**: the enclosing assistant turn's
///   `tool_calls` already carries the same call id, and strict endpoints
///   reject the duplicate. Returns `None`.
///
/// Public: `kian session replay` re-derives the model context
/// from the event stream and must rebuild the *exact* chat shape the
/// loop sent, so the A2 byte-identity check compares like with like.
/// This conversion is part of the frozen request-serialization contract
/// (constraint 3) — changing it invalidates every replay comparison and
/// every provider prefix cache.
pub fn to_chat_message(m: &Message) -> Option<ChatMessage> {
    match m.role.as_str() {
        "tool" => Some(tool_result_message(&m.content)),
        "assistant" => assistant_message(&m.content),
        _ => Some(string_or_compact(&m.role, &m.content)),
    }
}

/// The permission decision for one tool call (ADR-015). Evaluated
/// once per call, before the `tool/call` append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PermissionDecision {
    /// Explicitly allowed by the configured table.
    AllowConfigured,
    /// Not named by the (non-empty) table — the default allow.
    AllowDefault,
    /// Explicitly denied by the configured table: no invocation.
    Deny,
}

/// The deny `reason` string on the provenance object (closed).
const PERMISSION_DENY_REASON: &str = "tool_denied_by_policy";

/// Evaluates the per-call permission table (ADR-015):
///
/// - empty table → `None`: no table is configured, the `tool/call`
///   payload stays byte-identical to the legacy shape (constraint 3 —
///   cache-byte stability) and the call invokes as today;
/// - non-empty table → every call gets a decision: an explicit entry is
///   `configured` (allow or deny), an unnamed tool falls through to the
///   **default allow** — provenance is recorded on the action either way
///   (provenance-on-the-action), not only on deny.
fn evaluate_permission(
    permissions: &BTreeMap<String, Permission>,
    tool: &str,
) -> Option<PermissionDecision> {
    if permissions.is_empty() {
        return None;
    }
    match permissions.get(tool) {
        Some(Permission::Allow) => Some(PermissionDecision::AllowConfigured),
        Some(Permission::Deny) => Some(PermissionDecision::Deny),
        None => Some(PermissionDecision::AllowDefault),
    }
}

/// The declared-permission deny arm's honest-error result (ADR-015):
/// `Some` only when the declarative table denied this call — the
/// payload shape the model reacts to, unchanged from the pre-plugin
/// loop.
fn declared_denial(call: &ToolCall, decision: Option<PermissionDecision>) -> Option<ToolResult> {
    matches!(decision, Some(PermissionDecision::Deny)).then(|| ToolResult {
        call_id: call.id.clone(),
        output: json!({
            "error": format!("permission denied by policy: {}", call.name),
            "policy": "deny"
        }),
        is_error: true,
        redactions: Vec::new(),
    })
}

/// The `tool/call` event payload: the frozen `{role, content}` shape
/// plus, only when a permission table is configured, the additive
/// top-level `permission` provenance key **outside `content`** — the
/// projection reads `role`/`content` only, so request bytes are
/// identical with and without it.
fn tool_call_payload(call: &ToolCall, decision: Option<PermissionDecision>) -> Value {
    let mut payload = json!({
        "role": "assistant",
        "content": {"id": call.id, "name": call.name, "arguments": call.arguments}
    });
    if let Some(decision) = decision {
        payload["permission"] = match decision {
            PermissionDecision::AllowConfigured => json!({
                "evaluated": "allow", "policy": "configured", "matched": call.name
            }),
            PermissionDecision::AllowDefault => json!({
                "evaluated": "allow", "policy": "default", "matched": call.name
            }),
            PermissionDecision::Deny => json!({
                "evaluated": "deny",
                "policy": "configured",
                "matched": call.name,
                "reason": PERMISSION_DENY_REASON
            }),
        };
    }
    payload
}

/// The `tool/call` payload of a **hook-denied** call (§4.4 step 3 +
/// §16.1): log-before-execute holds for hook denials too — the call is
/// logged, with `permission.policy = "hook"` and `matched` naming the
/// denying `<plugin>:<hook>`, before the honest-error result is
/// appended and **before any effect** (the invoke never runs). The
/// `policy: "hook"` payload value is the one EVENT-MODEL amendment of
/// this round (P4's card owns the doc + goldens).
fn tool_call_payload_hook_denied(call: &ToolCall, plugin: &str, reason: &str) -> Value {
    let mut payload = json!({
        "role": "assistant",
        "content": {"id": call.id, "name": call.name, "arguments": call.arguments}
    });
    payload["permission"] = json!({
        "evaluated": "deny",
        "policy": "hook",
        "matched": format!("{plugin}:tool/pre"),
        "reason": reason,
    });
    payload
}

/// The declarative gate's decision rendered as the §3.2.5
/// `permission` view object (ADR-015 provenance shape; `reason` rides
/// only a deny). `None` (no table configured) renders the default-allow
/// row — the decision the call actually runs under.
fn permission_view(
    call: &ToolCall,
    decision: Option<PermissionDecision>,
) -> kian_plugin::hooks::PermissionView {
    match decision {
        Some(PermissionDecision::AllowConfigured) => kian_plugin::hooks::PermissionView {
            evaluated: "allow".to_string(),
            policy: "configured".to_string(),
            matched: call.name.clone(),
            reason: None,
        },
        Some(PermissionDecision::Deny) => kian_plugin::hooks::PermissionView {
            evaluated: "deny".to_string(),
            policy: "configured".to_string(),
            matched: call.name.clone(),
            reason: Some(PERMISSION_DENY_REASON.to_string()),
        },
        Some(PermissionDecision::AllowDefault) | None => kian_plugin::hooks::PermissionView {
            evaluated: "allow".to_string(),
            policy: "default".to_string(),
            matched: call.name.clone(),
            reason: None,
        },
    }
}

/// The `tool/result` event payload: `{role, content}` plus, only when
/// the exec redactor scrubbed something, the provenance report as
/// an additive top-level `redactions` key **outside `content`** — the
/// model-visible wire shape (`to_chat_message` reads `content` only) is
/// byte-identical either way, so request bytes and the provider prefix
/// cache never see the report.
fn tool_result_payload(result: &ToolResult) -> Value {
    let mut payload = json!({
        "role": "tool",
        "content": {
            "call_id": result.call_id,
            "output": result.output,
            "is_error": result.is_error
        }
    });
    if !result.redactions.is_empty() {
        // A failed serialization degrades to omitting the key (the
        // report is provenance, never correctness-critical).
        if let Ok(report) = serde_json::to_value(&result.redactions) {
            payload["redactions"] = report;
        }
    }
    payload
}

/// `{role, content}` from a plain string, or the compact serialization
/// of any other JSON shape (the v1 fallback, unchanged).
fn string_or_compact(role: &str, content: &Value) -> ChatMessage {
    let text = match content {
        Value::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    };
    ChatMessage {
        role: role.to_owned(),
        content: Some(text),
        tool_call_id: None,
        tool_calls: None,
        reasoning_content: None,
    }
}

/// A `tool/result` payload `{call_id, output, is_error}` → the strict
/// wire shape. A foreign shape still maps to a `role:"tool"` message
/// (with an empty `tool_call_id`) so nothing is silently swallowed.
fn tool_result_message(content: &Value) -> ChatMessage {
    if let Some(obj) = content.as_object()
        && let Some(call_id) = obj.get("call_id").and_then(Value::as_str)
    {
        let output = obj.get("output").cloned().unwrap_or(Value::Null);
        return ChatMessage {
            role: "tool".into(),
            content: Some(serde_json::to_string(&output).unwrap_or_default()),
            tool_call_id: Some(call_id.to_owned()),
            tool_calls: None,
            reasoning_content: None,
        };
    }
    ChatMessage {
        role: "tool".into(),
        content: Some(serde_json::to_string(content).unwrap_or_default()),
        tool_call_id: Some(String::new()),
        tool_calls: None,
        reasoning_content: None,
    }
}

/// The replayed reasoning of an assistant payload, if present
/// (ADR-022). Absent/blank/non-string ⇒ `None` (legacy bytes unchanged).
fn reasoning_field(obj: &serde_json::Map<String, Value>) -> Option<String> {
    obj.get("reasoning_content")
        .and_then(Value::as_str)
        .filter(|r| !r.is_empty())
        .map(str::to_owned)
}

/// An assistant payload: a tool-call turn, its per-call transcript
/// duplicate (`None` — dropped from the wire), a bare `{"text": …}`, or
/// the compact fallback.
fn assistant_message(content: &Value) -> Option<ChatMessage> {
    let Some(obj) = content.as_object() else {
        return Some(string_or_compact("assistant", content));
    };
    if obj.contains_key("tool_calls") {
        let calls: Option<Vec<WireToolCall>> =
            obj.get("tool_calls").and_then(Value::as_array).map(|list| {
                list.iter()
                    .filter_map(|tc| {
                        let id = tc.get("id").and_then(Value::as_str)?;
                        let name = tc.get("name").and_then(Value::as_str)?;
                        let arguments = tc.get("arguments").cloned().unwrap_or(Value::Null);
                        Some(WireToolCall {
                            id: id.to_owned(),
                            r#type: "function".into(),
                            function: WireFunctionCall {
                                name: name.to_owned(),
                                arguments: serde_json::to_string(&arguments).unwrap_or_default(),
                            },
                        })
                    })
                    .collect()
            });
        return Some(ChatMessage {
            role: "assistant".into(),
            // Empty text serializes as `content: null` (the strict shape
            // verified live: a purely tool-calling assistant turn).
            content: obj
                .get("text")
                .and_then(Value::as_str)
                .filter(|t| !t.is_empty())
                .map(str::to_owned),
            tool_call_id: None,
            tool_calls: calls,
            reasoning_content: reasoning_field(obj),
        });
    }
    // The per-call `tool/call` transcript duplicate: the enclosing
    // assistant turn's tool_calls already carries this call id.
    if obj.contains_key("id") && obj.contains_key("name") && obj.contains_key("arguments") {
        return None;
    }
    if let Some(text) = obj.get("text").and_then(Value::as_str) {
        return Some(ChatMessage {
            role: "assistant".into(),
            content: Some(text.to_owned()),
            tool_call_id: None,
            tool_calls: None,
            reasoning_content: reasoning_field(obj),
        });
    }
    Some(string_or_compact("assistant", content))
}

/// (ADR-025) Whether a resident iteration has no work to judge: the loop is
/// episode-scoped, `take_eligible_inputs` returned nothing, and the context
/// ends on an assistant message (the empty-batch shape). A tool-result
/// continuation (last role `tool`) or any newly taken input is real work.
fn resident_empty_batch(scope: BudgetScope, taken_empty: bool, last_role: Option<&str>) -> bool {
    scope == BudgetScope::PerEpisode && taken_empty && last_role == Some("assistant")
}

/// Maps a provider error onto the classifier's class names (
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
        BudgetScope, CLASS_EMPTY_RESPONSE, ReActConfig, ReActLoop, assemble_delta, consult_policy,
        to_chat_message, tool_result_payload,
    };
    use crate::test_support::{NoopPolicy, NoopTools, agent_def};
    use kian_core::context::{Message, derive_model_context};
    use kian_core::events::{Event, EventKind};
    use kian_core::model::SessionId;
    use kian_core::seams::{ToolCall, ToolResult};
    use kian_core::secrets::{Redaction, RedactionKind};
    use kian_llm::provider::build_chat_body;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn msg(role: &str, content: serde_json::Value) -> Message {
        Message {
            role: role.into(),
            content,
            synthetic: false,
        }
    }

    /// the `tool/result` payload carries the redaction report as
    /// an additive top-level key OUTSIDE `content`, and omits it when
    /// nothing was redacted — the un-redacted payload is byte-identical
    /// to the legacy literal.
    #[test]
    fn tool_result_payload_carries_redactions_outside_content() {
        let plain = ToolResult {
            call_id: "call-1".into(),
            output: json!({"exit_code": 0, "stdout": "ok", "stderr": "", "timed_out": false}),
            is_error: false,
            redactions: Vec::new(),
        };
        let payload = tool_result_payload(&plain);
        assert_eq!(
            payload,
            json!({
                "role": "tool",
                "content": {
                    "call_id": "call-1",
                    "output": {"exit_code": 0, "stdout": "ok", "stderr": "", "timed_out": false},
                    "is_error": false
                }
            }),
            "no redactions → no key, bytes unchanged"
        );

        let redacted = ToolResult {
            call_id: "call-2".into(),
            output: json!({"exit_code": 0, "stdout": "[REDACTED:token]", "stderr": "", "timed_out": false}),
            is_error: false,
            redactions: vec![Redaction {
                kind: RedactionKind::TokenPattern,
                name: "ghp_".into(),
                count: 1,
            }],
        };
        let payload = tool_result_payload(&redacted);
        assert_eq!(
            payload["redactions"],
            json!([{"kind": "token_pattern", "name": "ghp_", "count": 1}])
        );
        // The report sits outside `content`; content itself is exactly
        // the four legacy fields.
        assert!(payload["content"].get("redactions").is_none());
        assert_eq!(payload["content"]["call_id"], json!("call-2"));
    }

    /// (constraint 3): the additive `redactions` payload key never
    /// reaches the model context — the projection reads `role`/`content`
    /// only, so request bytes are identical with and without the key.
    #[test]
    fn redactions_payload_key_never_enters_the_model_context() {
        let session = SessionId::new();
        let bytes = tool_result_payload(&ToolResult {
            call_id: "call-9".into(),
            output: json!({"content": "scrubbed"}),
            is_error: false,
            redactions: vec![Redaction {
                kind: RedactionKind::EnvLine,
                name: "MY_SERVICE_TOKEN".into(),
                count: 2,
            }],
        });
        let with = Event {
            seq: 2,
            session,
            epoch: kian_core::model::Epoch(1),
            ts: 1,
            kind: EventKind::ToolResult,
            bytes: bytes.clone(),
            version: 0,
        };
        let mut without_bytes = bytes.clone();
        without_bytes.as_object_mut().unwrap().remove("redactions");
        let without = Event {
            bytes: without_bytes,
            ..with.clone()
        };
        let base = Event {
            seq: 1,
            session,
            epoch: kian_core::model::Epoch(0),
            ts: 0,
            kind: EventKind::SessionCreated,
            bytes: json!({}),
            version: 0,
        };
        let ctx_with = derive_model_context(&[base.clone(), with]).unwrap();
        let ctx_without = derive_model_context(&[base, without]).unwrap();
        assert_eq!(ctx_with, ctx_without);
        assert_eq!(ctx_with.len(), 1);
        // And the projected message is the frozen wire shape.
        let chat = to_chat_message(&ctx_with[0]).unwrap();
        assert_eq!(chat.role, "tool");
        assert_eq!(chat.tool_call_id.as_deref(), Some("call-9"));
        assert_eq!(chat.content.as_deref(), Some("{\"content\":\"scrubbed\"}"));
    }

    /// Dialect v2: a projected tool round trip maps to the
    /// strict openai wire shape, and the per-call `tool/call` transcript
    /// duplicate is dropped (the assistant turn's tool_calls already
    /// carries the id).
    #[test]
    fn tool_round_trip_projects_to_the_strict_wire_shape() {
        let assistant = msg(
            "assistant",
            json!({
                "text": "",
                "tool_calls": [
                    {"id": "call-1", "name": "fs_read", "arguments": {"path": "a.txt"}}
                ]
            }),
        );
        let chat = to_chat_message(&assistant).unwrap();
        assert_eq!(chat.role, "assistant");
        assert_eq!(chat.content, None, "empty text serializes as null");
        let tc = &chat.tool_calls.as_ref().unwrap()[0];
        assert_eq!(tc.id, "call-1");
        assert_eq!(tc.r#type, "function");
        assert_eq!(tc.function.name, "fs_read");
        assert_eq!(tc.function.arguments, "{\"path\":\"a.txt\"}");

        // Non-empty text rides along as string content.
        let with_text = msg(
            "assistant",
            json!({"text": "checking", "tool_calls": [
                {"id": "call-2", "name": "exec", "arguments": {"argv": ["true"]}}
            ]}),
        );
        let chat = to_chat_message(&with_text).unwrap();
        assert_eq!(chat.content.as_deref(), Some("checking"));

        // The tool/call transcript duplicate: dropped.
        let dup = msg(
            "assistant",
            json!({"id": "call-1", "name": "fs_read", "arguments": {"path": "a.txt"}}),
        );
        assert!(to_chat_message(&dup).is_none());

        // The tool result: role tool + tool_call_id + serialized output.
        let result = msg(
            "tool",
            json!({"call_id": "call-1", "output": {"content": "x"}, "is_error": false}),
        );
        let chat = to_chat_message(&result).unwrap();
        assert_eq!(chat.role, "tool");
        assert_eq!(chat.tool_call_id.as_deref(), Some("call-1"));
        assert_eq!(chat.content.as_deref(), Some("{\"content\":\"x\"}"));
        assert!(chat.tool_calls.is_none());
    }

    /// Dialect v2: plain messages keep the exact v1 `{content, role}`
    /// shape (no new keys), and bare `{"text": …}` assistant content
    /// unwraps to the string.
    #[test]
    fn plain_messages_keep_the_v1_shape() {
        let user = msg("user", json!("hello"));
        let chat = to_chat_message(&user).unwrap();
        assert_eq!(chat.role, "user");
        assert_eq!(chat.content.as_deref(), Some("hello"));
        assert!(chat.tool_call_id.is_none() && chat.tool_calls.is_none());

        let bare = msg("assistant", json!({"text": "hi"}));
        assert_eq!(
            to_chat_message(&bare).unwrap().content.as_deref(),
            Some("hi")
        );

        // A foreign tool shape still maps (empty tool_call_id) — nothing
        // is silently swallowed.
        let foreign = msg("tool", json!({"odd": true}));
        let chat = to_chat_message(&foreign).unwrap();
        assert_eq!(chat.tool_call_id.as_deref(), Some(""));
    }

    // -- pure-unit sections ---------------------------------------------------

    /// (ADR-015): with no permissions table configured, evaluation
    /// returns `None` — the `tool/call` payload stays byte-identical to
    /// the legacy shape (cache-byte stability). A non-empty table
    /// decides every call: an explicit entry is `configured`, an absent
    /// entry falls through to the default allow.
    #[test]
    fn permission_evaluation_covers_absent_default_and_configured() {
        use crate::react::{PermissionDecision, evaluate_permission};
        use kian_core::model::Permission;

        let empty: BTreeMap<String, Permission> = BTreeMap::new();
        assert_eq!(evaluate_permission(&empty, "fs_write"), None);

        let table: BTreeMap<String, Permission> = BTreeMap::from([
            ("fs_read".to_string(), Permission::Allow),
            ("fs_write".to_string(), Permission::Deny),
        ]);
        assert_eq!(
            evaluate_permission(&table, "fs_read"),
            Some(PermissionDecision::AllowConfigured)
        );
        assert_eq!(
            evaluate_permission(&table, "fs_write"),
            Some(PermissionDecision::Deny)
        );
        // A tool the table does not name: the default allow, provenance
        // still recorded (provenance-on-the-action).
        assert_eq!(
            evaluate_permission(&table, "exec"),
            Some(PermissionDecision::AllowDefault)
        );
    }

    /// the allow/deny provenance shapes on `tool/call` — additive
    /// top-level `permission` key outside `content`, `content` exactly
    /// the legacy `{id, name, arguments}` triple.
    #[test]
    fn tool_call_payload_carries_permission_provenance_outside_content() {
        use crate::react::{evaluate_permission, tool_call_payload};
        use kian_core::model::Permission;

        let call = ToolCall {
            id: "call-7".into(),
            name: "fs_write".into(),
            arguments: json!({"path": "a.txt"}),
        };

        // No table → byte-identical legacy payload (no key at all).
        let empty: BTreeMap<String, Permission> = BTreeMap::new();
        let legacy = tool_call_payload(&call, evaluate_permission(&empty, &call.name));
        assert_eq!(
            legacy,
            json!({
                "role": "assistant",
                "content": {"id": "call-7", "name": "fs_write", "arguments": {"path": "a.txt"}}
            })
        );

        // Configured allow.
        let table: BTreeMap<String, Permission> =
            BTreeMap::from([("fs_read".to_string(), Permission::Allow)]);
        let allowed = tool_call_payload(&call, evaluate_permission(&table, &call.name)); // default arm
        assert_eq!(
            allowed["permission"],
            json!({"evaluated": "allow", "policy": "default", "matched": "fs_write"})
        );
        assert_eq!(allowed["content"], legacy["content"]);

        // Configured deny.
        let table: BTreeMap<String, Permission> =
            BTreeMap::from([("fs_write".to_string(), Permission::Deny)]);
        let denied = tool_call_payload(&call, evaluate_permission(&table, &call.name));
        assert_eq!(
            denied["permission"],
            json!({
                "evaluated": "deny",
                "policy": "configured",
                "matched": "fs_write",
                "reason": "tool_denied_by_policy"
            })
        );
        assert_eq!(denied["content"], legacy["content"]);
    }

    /// P4 (PLUGIN-SPEC §4.4 + §16.1): the one EVENT-MODEL amendment of
    /// the hook round — the additive payload VALUE `policy:"hook"` on
    /// the existing `permission` key (never a new key, never a new
    /// kind). This is the golden pair, frozen to the exact serialized
    /// bytes: (a) the hook-denied `tool/call` payload (log-before-
    /// execute provenance naming the denying `<plugin>:<hook>`) and
    /// (b) the honest-error `tool/result` the model reacts to. Object
    /// keys serialize in canonical (sorted) order — `serde_json`
    /// without `preserve_order` — so the strings below are the durable
    /// audit bytes, not just structural equality.
    #[test]
    fn hook_denied_permission_payload_goldens_are_frozen_bytes() {
        use crate::react::{evaluate_permission, tool_call_payload, tool_call_payload_hook_denied};
        use kian_core::model::Permission;

        let call = ToolCall {
            id: "call-7".into(),
            name: "exec".into(),
            arguments: json!({"cmd": ["ls"]}),
        };

        // (a) tool/call, hook-denied arm (§4.4 step 3).
        let hook_denied =
            tool_call_payload_hook_denied(&call, "approval-gate", "exec not on the allowlist");
        assert_eq!(
            serde_json::to_string(&hook_denied).unwrap(),
            "{\"content\":{\"arguments\":{\"cmd\":[\"ls\"]},\"id\":\"call-7\",\"name\":\"exec\"},\
             \"permission\":{\"evaluated\":\"deny\",\"matched\":\"approval-gate:tool/pre\",\
             \"policy\":\"hook\",\"reason\":\"exec not on the allowlist\"},\"role\":\"assistant\"}"
        );

        // (b) tool/result, the honest error of the same denial
        // (§4.4 step 4) — exactly the bytes the loop appends on the
        // deny arm (no redactions key: the report is empty there).
        let denied_result = ToolResult {
            call_id: call.id.clone(),
            output: json!({
                "error": "denied by plugin approval-gate: exec not on the allowlist",
                "policy": "hook"
            }),
            is_error: true,
            redactions: Vec::new(),
        };
        assert_eq!(
            serde_json::to_string(&tool_result_payload(&denied_result)).unwrap(),
            "{\"content\":{\"call_id\":\"call-7\",\"is_error\":true,\
             \"output\":{\"error\":\"denied by plugin approval-gate: exec not on the allowlist\",\
             \"policy\":\"hook\"}},\"role\":\"tool\"}"
        );

        // Frozen regression — the amendment touches no other payload
        // shape: every pre-existing `permission` arm (and the legacy
        // key-less arm) serializes to exactly its pre-amendment bytes.
        let empty: BTreeMap<String, Permission> = BTreeMap::new();
        assert_eq!(
            serde_json::to_string(&tool_call_payload(
                &call,
                evaluate_permission(&empty, &call.name)
            ))
            .unwrap(),
            "{\"content\":{\"arguments\":{\"cmd\":[\"ls\"]},\"id\":\"call-7\",\"name\":\"exec\"},\
             \"role\":\"assistant\"}"
        );
        let default_table = BTreeMap::from([("fs_read".to_string(), Permission::Allow)]);
        assert_eq!(
            serde_json::to_string(&tool_call_payload(
                &call,
                evaluate_permission(&default_table, &call.name)
            ))
            .unwrap(),
            "{\"content\":{\"arguments\":{\"cmd\":[\"ls\"]},\"id\":\"call-7\",\"name\":\"exec\"},\
             \"permission\":{\"evaluated\":\"allow\",\"matched\":\"exec\",\"policy\":\"default\"},\
             \"role\":\"assistant\"}"
        );
        let allow_table = BTreeMap::from([("exec".to_string(), Permission::Allow)]);
        assert_eq!(
            serde_json::to_string(&tool_call_payload(
                &call,
                evaluate_permission(&allow_table, &call.name)
            ))
            .unwrap(),
            "{\"content\":{\"arguments\":{\"cmd\":[\"ls\"]},\"id\":\"call-7\",\"name\":\"exec\"},\
             \"permission\":{\"evaluated\":\"allow\",\"matched\":\"exec\",\"policy\":\"configured\"},\
             \"role\":\"assistant\"}"
        );
        let deny_table = BTreeMap::from([("exec".to_string(), Permission::Deny)]);
        assert_eq!(
            serde_json::to_string(&tool_call_payload(
                &call,
                evaluate_permission(&deny_table, &call.name)
            ))
            .unwrap(),
            "{\"content\":{\"arguments\":{\"cmd\":[\"ls\"]},\"id\":\"call-7\",\"name\":\"exec\"},\
             \"permission\":{\"evaluated\":\"deny\",\"matched\":\"exec\",\"policy\":\"configured\",\
             \"reason\":\"tool_denied_by_policy\"},\"role\":\"assistant\"}"
        );
    }

    /// (constraint 3): the additive `permission` payload key never
    /// reaches the model context — the projection reads `role`/`content`
    /// only, so the projected message (and therefore request bytes) is
    /// identical with and without the key.
    #[test]
    fn permission_payload_key_never_enters_the_model_context() {
        use crate::react::{PermissionDecision, tool_call_payload};
        use kian_core::seams::ToolCall;

        let session = SessionId::new();
        let call = ToolCall {
            id: "call-1".into(),
            name: "fs_read".into(),
            arguments: json!({"path": "a.txt"}),
        };
        let with_bytes = tool_call_payload(&call, Some(PermissionDecision::Deny));
        let mut without_bytes = with_bytes.clone();
        without_bytes.as_object_mut().unwrap().remove("permission");
        let with = Event {
            seq: 2,
            session,
            epoch: kian_core::model::Epoch(1),
            ts: 1,
            kind: EventKind::ToolCall,
            bytes: with_bytes,
            version: 0,
        };
        let without = Event {
            bytes: without_bytes,
            ..with.clone()
        };
        let base = Event {
            seq: 1,
            session,
            epoch: kian_core::model::Epoch(0),
            ts: 0,
            kind: EventKind::SessionCreated,
            bytes: json!({}),
            version: 0,
        };
        let ctx_with = derive_model_context(&[base.clone(), with]).unwrap();
        let ctx_without = derive_model_context(&[base, without]).unwrap();
        assert_eq!(ctx_with, ctx_without);
        assert_eq!(ctx_with.len(), 1);
        // The projected message is the frozen wire shape (the per-call
        // transcript duplicate, dropped from the chat body downstream).
        assert_eq!(ctx_with[0].role, "assistant");
        assert_eq!(
            ctx_with[0].content,
            json!({"id": "call-1", "name": "fs_read", "arguments": {"path": "a.txt"}})
        );
        assert!(!ctx_with[0].synthetic);
    }

    /// ADR-024: the budget scope selects which counter the limit applies
    /// to — the lifetime `turn/start` count (worker/plain) or the current
    /// episode's count (resident loops).
    /// (ADR-025) Only a resident episode ending on an assistant message with
    /// nothing taken is skipped; tool-result continuations, new inputs and
    /// lifetime loops are real work.
    #[test]
    fn resident_empty_batch_is_skipped_only_for_resident_assistant_tails() {
        use super::resident_empty_batch;
        assert!(resident_empty_batch(
            BudgetScope::PerEpisode,
            true,
            Some("assistant")
        ));
        assert!(!resident_empty_batch(
            BudgetScope::PerEpisode,
            false,
            Some("assistant")
        ));
        assert!(!resident_empty_batch(
            BudgetScope::PerEpisode,
            true,
            Some("tool")
        ));
        assert!(!resident_empty_batch(
            BudgetScope::PerEpisode,
            true,
            Some("user")
        ));
        assert!(!resident_empty_batch(
            BudgetScope::Lifetime,
            true,
            Some("assistant")
        ));
    }

    #[test]
    fn budget_scope_selects_lifetime_or_episode_counter() {
        assert_eq!(BudgetScope::Lifetime.used(9, 0), 9);
        assert_eq!(BudgetScope::Lifetime.used(0, 9), 0);
        assert_eq!(BudgetScope::PerEpisode.used(9, 2), 2);
        assert_eq!(BudgetScope::PerEpisode.used(0, 0), 0);
    }

    #[test]
    fn delta_assembly_concatenates_content_fragments() {
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut calls = Default::default();
        for fragment in ["Hello", ", ", "kian"] {
            assemble_delta(
                &serde_json::json!({"content": fragment}),
                &mut text,
                &mut reasoning,
                &mut calls,
            );
        }
        assert_eq!(text, "Hello, kian");
        assert!(calls.is_empty());
    }

    /// ADR-022: reasoning fragments concatenate in arrival order, persist
    /// inside the assistant payload, and replay as the top-level wire
    /// field; a legacy payload without the key stays byte-identical.
    #[test]
    fn reasoning_fragments_are_persisted_and_replayed() {
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut calls = Default::default();
        assemble_delta(
            &json!({"reasoning_content": "think "}),
            &mut text,
            &mut reasoning,
            &mut calls,
        );
        assemble_delta(
            &json!({"reasoning_content": "harder"}),
            &mut text,
            &mut reasoning,
            &mut calls,
        );
        assemble_delta(
            &json!({"content": "answer"}),
            &mut text,
            &mut reasoning,
            &mut calls,
        );
        assert_eq!(reasoning, "think harder");
        assert_eq!(text, "answer");

        let with = to_chat_message(&msg(
            "assistant",
            json!({"text": text, "reasoning_content": reasoning}),
        ))
        .unwrap();
        assert_eq!(with.reasoning_content.as_deref(), Some("think harder"));
        let body = build_chat_body("m", &[with], &[]);
        assert_eq!(body["messages"][0]["reasoning_content"], "think harder");

        let legacy = to_chat_message(&msg("assistant", json!({"text": "answer"}))).unwrap();
        assert!(legacy.reasoning_content.is_none());
        let body = build_chat_body("m", &[legacy], &[]);
        assert!(body["messages"][0].get("reasoning_content").is_none());
    }

    #[test]
    fn delta_assembly_accumulates_tool_call_fragments_by_index() {
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut calls = Default::default();
        assemble_delta(
            &serde_json::json!({"tool_calls": [
                {"index": 0, "id": "call-1", "function": {"name": "fs.read"}}
            ]}),
            &mut text,
            &mut reasoning,
            &mut calls,
        );
        assemble_delta(
            &serde_json::json!({"tool_calls": [
                {"index": 0, "function": {"arguments": "{\"pa"}}
            ]}),
            &mut text,
            &mut reasoning,
            &mut calls,
        );
        assemble_delta(
            &serde_json::json!({"tool_calls": [
                {"index": 0, "function": {"arguments": "th\": \"a.txt\"}"}}
            ]}),
            &mut text,
            &mut reasoning,
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

    /// the recovery audit carries the pure classifier's class name
    /// plus the policy disposition name, `"none"` when no rule covers
    /// the class; provider classes flow through the existing pure
    /// classifier and loop-level hard errors map onto its vocabulary.
    #[test]
    fn recovery_audit_carries_class_and_policy_action() {
        use kian_core::seams::{PolicyRule, RecoveryPolicySource};
        use kian_orchestrator::recovery::ErrorClass;
        use serde_json::json;

        struct Table(Vec<PolicyRule>);
        impl RecoveryPolicySource for Table {
            fn table(&self) -> Vec<PolicyRule> {
                self.0.clone()
            }
        }
        let with_rule = Table(vec![PolicyRule {
            class: "store_failure".into(),
            action: kian_core::seams::RecoveryAction::Fail,
        }]);
        assert_eq!(
            super::recovery_audit(&with_rule, ErrorClass::StoreFailure),
            json!({"class": "store_failure", "action": "fail"})
        );
        assert_eq!(
            super::recovery_audit(&with_rule, ErrorClass::EmptyResponse),
            json!({"class": "empty_response", "action": "none"}),
            "no covering rule → the honest none spelling"
        );

        // Provider classes route through the pure classifier.
        assert_eq!(
            super::provider_error_class(&kian_core::seams::ProviderError::RateLimited),
            ErrorClass::RateLimited
        );
        assert_eq!(
            super::provider_error_class(&kian_core::seams::ProviderError::Unavailable(
                "upstream 503".into()
            )),
            ErrorClass::ProviderDown
        );

        // Loop-level hard errors stay inside the classifier vocabulary.
        assert_eq!(
            super::loop_error_class(&super::LoopError::Engine(
                kian_orchestrator::EngineError::NotAtSafeBoundary {
                    session: kian_core::model::SessionId::new(),
                }
            )),
            ErrorClass::StoreFailure
        );
        assert_eq!(
            super::loop_error_class(&super::LoopError::Slot(
                kian_llm::slots::SlotError::UnknownSlot("aux".into())
            )),
            ErrorClass::Unknown
        );
    }

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
