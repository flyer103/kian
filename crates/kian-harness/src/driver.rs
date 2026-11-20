//! The shared bridge driver: one `LoopDriver` for every external harness
//! (HARNESS-BRIDGE §4).
//!
//! The driver owns the episode shape (spawn/park, safe boundaries, takeover de-duplication, the
//! step budget, honest outcomes, the approval policy, the `tool/post` port). Per-harness crates
//! supply a [`SessionFactory`] and a [`HarnessSession`]; they never touch the store, the engine or
//! the end write.
//!
//! # Episode lifecycle
//!
//! `drive` is called once per drain episode. A fresh episode spawns a child; a later drive of the
//! same session continues a parked child (the session table). Phase 1 has no cross-process resume:
//! a takeover spawns a fresh child, and with no fresh input the session ends honestly
//! `interrupted` (HARNESS-BRIDGE §4.6).
//!
//! # Turn ownership
//!
//! The driver owns `turn/start`: at a safe boundary it takes eligible inputs, delivers them, then
//! appends `turn/start` with the derived iteration. The adapter owns `turn/end` (it alone knows
//! the harness outcome); the driver stamps the iteration and yields `TurnDone`, which is the
//! engine's supervision point.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_core::seams::{LoopDriver, LoopOutcome};
use serde_json::{Value, json};

use crate::approval::{ApprovalPolicy, Decision};
use crate::ctx::{BridgeCtx, BridgeCtxError};
use crate::event::{
    ApprovalAnswer, Capabilities, ContractError, Control, HarnessSession, Mapped, SessionEvent,
};

/// How often an idle drain re-checks the inbox while waiting for the peer's next line. Constraint
/// 6: wakes may fold, be lost or race, so the drain never depends on one to make progress.
pub const IDLE_INPUT_RECHECK: Duration = Duration::from_millis(50);

/// Default open-step inactivity bound (HARNESS-BRIDGE §4.5, ADR-044): how long an open harness
/// step may produce no progress (no protocol event, no completed control write) before the child
/// is declared unresponsive. Generous on purpose — the bound ends a dead bridge, it does not
/// police a slow one; operators on longer silent tools raise it. `0` disables the bound.
pub const DEFAULT_STEP_TIMEOUT_MS: u64 = 1_800_000;

/// Driver configuration (assembly-owned).
#[derive(Debug, Clone)]
pub struct BridgeDriverConfig {
    /// The declarative approval policy (HARNESS-BRIDGE §6.3).
    pub policy: ApprovalPolicy,
    /// Mapped turns per session (default 32, the dsh budget).
    pub max_steps: u64,
    /// Open-step inactivity bound in milliseconds (HARNESS-BRIDGE §4.5, ADR-044). `0` disables.
    pub step_timeout_ms: u64,
}

impl Default for BridgeDriverConfig {
    fn default() -> Self {
        Self {
            policy: ApprovalPolicy::deny_all(),
            max_steps: 32,
            step_timeout_ms: DEFAULT_STEP_TIMEOUT_MS,
        }
    }
}

/// Spawns one harness session (adapter-owned) and declares its capabilities.
pub trait SessionFactory: Send + Sync {
    /// The harness's declared capabilities (drift-tested against HARNESS-BRIDGE §3).
    fn capabilities(&self) -> Capabilities;
    /// Spawns and performs the protocol handshake within the adapter's own deadline.
    fn spawn(
        &self,
        session: SessionId,
    ) -> BoxFuture<'static, Result<Box<dyn HarnessSession>, String>>;
}

/// What the `tool/post` port observes: the mapped result's shape, no output content.
#[derive(Debug, Clone)]
pub struct ToolPostView {
    pub call_id: String,
    pub name: String,
    pub is_error: bool,
    pub output: Value,
}

/// The `tool/post` port (§3.2.6 shape, observe-only). Assembly adapts it to the hook bus.
pub trait ToolPostPort: Send + Sync {
    fn tool_post(&self, view: ToolPostView) -> BoxFuture<'static, ()>;
}

/// The `tool/pre` consultation for a harness-gated tool (HARNESS-BRIDGE §4.4, ADR-045).
///
/// The driver evaluates its declarative policy first and passes that decision along; the port
/// adapts the call to the plugin bus and returns its verdict. A deny can only narrow the
/// declarative gate, never widen it.
#[derive(Debug, Clone)]
pub struct ToolPreCall {
    pub session: SessionId,
    pub call_id: String,
    pub name: String,
    pub iteration: u64,
    pub arguments: Value,
    /// The declarative decision the hook may veto (never widen).
    pub config_allow: bool,
    pub config_matched: String,
}

/// The plugin bus's verdict, reduced to what the driver needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolPreVerdict {
    /// No valid denial: the declarative decision stands.
    Allow,
    /// A `veto:tool` denial (or the bus's fail-closed default).
    Deny { plugin: String, reason: String },
}

/// The `tool/pre` port (assembly adapts it to `kian-plugin`'s bus).
pub trait ToolPrePort: Send + Sync {
    fn tool_pre(&self, call: ToolPreCall) -> BoxFuture<'static, ToolPreVerdict>;
}

/// The bridge `LoopDriver`.
pub struct BridgeDriver {
    inner: Arc<Inner>,
}

struct Inner {
    factory: Arc<dyn SessionFactory>,
    config: BridgeDriverConfig,
    table: Mutex<HashMap<SessionId, Option<LiveSession>>>,
    tool_post: Option<Arc<dyn ToolPostPort>>,
    tool_pre: Option<Arc<dyn ToolPrePort>>,
}

impl BridgeDriver {
    #[must_use]
    pub fn new(factory: Arc<dyn SessionFactory>, config: BridgeDriverConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                factory,
                config,
                table: Mutex::new(HashMap::new()),
                tool_post: None,
                tool_pre: None,
            }),
        }
    }

    /// Mounts the `tool/post` port. Call before serving (no session is parked at construction).
    #[must_use]
    pub fn with_tool_post_port(mut self, port: Arc<dyn ToolPostPort>) -> Self {
        let inner = Arc::get_mut(&mut self.inner).expect("unique at construction");
        inner.tool_post = Some(port);
        self
    }

    /// Mounts the `tool/pre` port (HARNESS-BRIDGE §4.4). Call before serving.
    #[must_use]
    pub fn with_tool_pre_port(mut self, port: Arc<dyn ToolPrePort>) -> Self {
        let inner = Arc::get_mut(&mut self.inner).expect("unique at construction");
        inner.tool_pre = Some(port);
        self
    }

    /// The harness capabilities this driver was built for.
    #[must_use]
    pub fn capabilities(&self) -> Capabilities {
        self.inner.factory.capabilities()
    }

    fn take(&self, session: SessionId) -> Option<LiveSession> {
        self.inner
            .table
            .lock()
            .expect("session table lock")
            .get_mut(&session)
            .and_then(Option::take)
    }

    fn park(&self, session: SessionId, live: LiveSession) {
        self.inner
            .table
            .lock()
            .expect("session table lock")
            .insert(session, Some(live));
    }
}

/// One live bridge: the child plus the per-episode derived state.
struct LiveSession {
    session: Box<dyn HarnessSession>,
    dedup: Dedup,
    delivered: BTreeSet<String>,
    turn_open: bool,
    turns_emitted: u64,
    turn_iteration: u64,
    /// Delivered inputs that still need a turn opened for them (fresh promotions delivered
    /// before `run`, or rows taken at a safe boundary).
    pending_inputs: usize,
    interrupt_sent: bool,
    tool_names: BTreeMap<String, String>,
}

impl LiveSession {
    fn new(session: Box<dyn HarnessSession>, events: &[Event]) -> Self {
        Self {
            session,
            dedup: Dedup::build(events),
            delivered: BTreeSet::new(),
            turn_open: false,
            turns_emitted: 0,
            turn_iteration: 0,
            pending_inputs: 0,
            interrupt_sent: false,
            tool_names: BTreeMap::new(),
        }
    }
}

/// Multiset of the persisted stream's bridge-projectable events (the dsh rule): a mapped event
/// that matches a remaining entry was already persisted and appends nothing.
#[derive(Debug, Default)]
struct Dedup {
    entries: VecDeque<(EventKind, Value)>,
}

impl Dedup {
    fn build(events: &[Event]) -> Self {
        let mut dedup = Self::default();
        for ev in events {
            if is_bridge_kind(ev.kind) {
                dedup.entries.push_back((ev.kind, ev.bytes.clone()));
            }
        }
        dedup
    }

    fn take(&mut self, kind: EventKind, payload: &Value) -> bool {
        if let Some(pos) = self
            .entries
            .iter()
            .position(|(k, p)| *k == kind && p == payload)
        {
            self.entries.remove(pos);
            true
        } else {
            false
        }
    }

    fn add(&mut self, kind: EventKind, payload: Value) {
        self.entries.push_back((kind, payload));
    }
}

fn is_bridge_kind(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::InputAdmitted
            | EventKind::AssistantMessage
            | EventKind::ToolCall
            | EventKind::ToolResult
            | EventKind::TurnStart
            | EventKind::TurnEnd
    )
}

/// Unfold state: `Next` keeps the live session between outcomes; `Done` ends the stream.
enum St {
    Start(Arc<dyn BridgeCtx>),
    Next {
        ctx: Arc<dyn BridgeCtx>,
        parked: Parked,
        steps_in_stream: u64,
    },
    Done,
}

/// A session parked between outcomes: a dropped stream returns it to the driver's table so the
/// next drive of the same hold continues the same child.
struct Parked {
    driver: BridgeDriver,
    session: SessionId,
    live: Mutex<Option<LiveSession>>,
}

impl Parked {
    fn take(&self) -> Option<LiveSession> {
        self.live.lock().expect("parked session").take()
    }
}

impl Drop for Parked {
    fn drop(&mut self) {
        if let Some(live) = self.live.lock().expect("parked session").take() {
            self.driver.park(self.session, live);
        }
    }
}

impl LoopDriver for BridgeDriver {
    type Ctx = Arc<dyn BridgeCtx>;

    fn drive(&self, ctx: Arc<dyn BridgeCtx>) -> BoxStream<'static, LoopOutcome> {
        let driver = self.clone_ref();
        futures::stream::unfold(St::Start(ctx), move |state| {
            let driver = driver.clone_ref();
            async move {
                match driver.step(state).await {
                    Step::Yield(outcome, next) => Some((outcome, next)),
                    Step::End => None,
                }
            }
        })
        .boxed()
    }
}

impl BridgeDriver {
    fn clone_ref(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    /// The configured open-step inactivity bound; `None` when disabled (`0`).
    fn step_bound(&self) -> Option<Duration> {
        (self.inner.config.step_timeout_ms > 0)
            .then(|| Duration::from_millis(self.inner.config.step_timeout_ms))
    }

    async fn step(&self, state: St) -> Step {
        match state {
            St::Done => Step::End,
            St::Start(ctx) => {
                let (outcome, next) = self.episode(ctx).await;
                Step::Yield(outcome, next)
            }
            St::Next {
                ctx,
                parked,
                steps_in_stream,
            } => match parked.take() {
                Some(live) => {
                    let (outcome, next) = self.run(ctx, live, steps_in_stream).await;
                    Step::Yield(outcome, next)
                }
                None => Step::End,
            },
        }
    }

    /// Continues a parked session or spawns a fresh child, then runs.
    async fn episode(&self, ctx: Arc<dyn BridgeCtx>) -> (LoopOutcome, St) {
        let bound = self.step_bound();
        let session = ctx.session();
        if let Some(live) = self.take(session) {
            let steps = count_steps(&ctx).await;
            return self.run(ctx, live, steps).await;
        }
        let events = match ctx.events().await {
            Ok(events) => events,
            Err(e) => return (soft("stream read", e), St::Done),
        };
        let steps_in_stream = events
            .iter()
            .filter(|ev| matches!(ev.kind, EventKind::TurnStart))
            .count() as u64;
        if steps_in_stream >= self.inner.config.max_steps {
            return (
                LoopOutcome::Stopped {
                    reason: format!("harness step budget exhausted after {steps_in_stream} steps"),
                },
                St::Done,
            );
        }
        let mut live = match self.inner.factory.spawn(session).await {
            Ok(session) => LiveSession::new(session, &events),
            Err(reason) => return (LoopOutcome::SoftFailed { reason }, St::Done),
        };
        // The engine promotes the batch into `input/admitted` before `drive`; deliver those
        // (the drain's own take at the first safe boundary would find an empty inbox).
        for payload in fresh_promotions(&events) {
            if let Err(reason) = deliver(&mut live, &payload, bound).await {
                return (LoopOutcome::SoftFailed { reason }, St::Done);
            }
            live.pending_inputs += 1;
        }
        self.run(ctx, live, steps_in_stream).await
    }

    /// Runs until one outcome is due.
    async fn run(
        &self,
        ctx: Arc<dyn BridgeCtx>,
        mut live: LiveSession,
        steps_in_stream: u64,
    ) -> (LoopOutcome, St) {
        let bound = self.step_bound();
        loop {
            if !live.turn_open {
                if ctx.interrupt_requested() && !live.interrupt_sent {
                    live.interrupt_sent = true;
                    match within_step_bound(
                        bound,
                        "interrupt control write",
                        live.session.send(Control::Interrupt),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            return (
                                LoopOutcome::SoftFailed {
                                    reason: format!("bridge interrupt: {e}"),
                                },
                                St::Done,
                            );
                        }
                        Err(reason) => return (LoopOutcome::SoftFailed { reason }, St::Done),
                    }
                }
                let rows = match ctx.take_eligible_inputs().await {
                    Ok(rows) => rows,
                    Err(e) => return (soft("input take", e), St::Done),
                };
                for row in rows {
                    let payload = consumption_payload(&row.payload);
                    live.dedup.add(EventKind::InputAdmitted, payload.clone());
                    if let Err(reason) = deliver(&mut live, &payload, bound).await {
                        return (LoopOutcome::SoftFailed { reason }, St::Done);
                    }
                    live.pending_inputs += 1;
                }
                if live.pending_inputs > 0 {
                    let iteration = steps_in_stream + live.turns_emitted;
                    if iteration >= self.inner.config.max_steps {
                        return (
                            LoopOutcome::Stopped {
                                reason: format!(
                                    "harness step budget exhausted after {iteration} steps"
                                ),
                            },
                            St::Done,
                        );
                    }
                    if let Err(e) = ctx.begin_turn(json!({"iteration": iteration})).await {
                        return (soft("turn begin", e), St::Done);
                    }
                    live.pending_inputs = 0;
                    live.turn_open = true;
                    live.turn_iteration = iteration;
                    live.turns_emitted += 1;
                } else if live.turns_emitted == 0 && steps_in_stream > 0 {
                    // A takeover with no undelivered input: phase 1 has no cross-process harness
                    // resume, so the dead turn cannot be re-entered — end honestly.
                    return (
                        LoopOutcome::SoftFailed {
                            reason: "harness turn lost on takeover: phase 1 has no cross-process \
                                     harness resume and no undelivered input to re-run"
                                .to_owned(),
                        },
                        St::Done,
                    );
                } else {
                    // kian's loop semantics (`Flow::Settled`): the exchange is over. Dropping the
                    // child ends the harness conversation; a later input starts a new one.
                    return (LoopOutcome::Settled, St::Done);
                }
            }

            let event = if live.turn_open {
                match within_step_bound(bound, "step (no protocol event)", live.session.next())
                    .await
                {
                    Ok(event) => event,
                    Err(reason) => return (LoopOutcome::SoftFailed { reason }, St::Done),
                }
            } else {
                // Folded/lost wake safety: at an idle safe boundary the child may legitimately
                // stay silent until we deliver the next input, so re-check on a bounded tick.
                tokio::select! {
                    event = live.session.next() => event,
                    _ = tokio::time::sleep(IDLE_INPUT_RECHECK) => continue,
                }
            };

            match event {
                Ok(SessionEvent::Mapped(Mapped::Absorbed { .. })) => {}
                Ok(SessionEvent::Mapped(Mapped::Event { kind, payload })) => {
                    if kind == EventKind::TurnStart {
                        return (
                            LoopOutcome::SoftFailed {
                                reason: "bridge contract: an adapter must not emit turn/start \
                                         (the driver owns turn boundaries)"
                                    .to_owned(),
                            },
                            St::Done,
                        );
                    }
                    if kind == EventKind::TurnEnd {
                        let payload = turn_end_payload(&payload, live.turn_iteration);
                        live.turn_open = false;
                        if let Err(e) = ctx.end_turn(payload).await {
                            return (soft("turn end", e), St::Done);
                        }
                        let session = ctx.session();
                        return (
                            LoopOutcome::TurnDone,
                            St::Next {
                                ctx,
                                parked: Parked {
                                    driver: self.clone_ref(),
                                    session,
                                    live: Mutex::new(Some(live)),
                                },
                                steps_in_stream,
                            },
                        );
                    }
                    if kind == EventKind::ToolCall
                        && let (Some(id), Some(name)) = (
                            payload.pointer("/content/id").and_then(Value::as_str),
                            payload.pointer("/content/name").and_then(Value::as_str),
                        )
                    {
                        live.tool_names.insert(id.to_string(), name.to_string());
                    }
                    if live.dedup.take(kind, &payload) {
                        continue;
                    }
                    if let Err(e) = ctx.append(kind, payload.clone()).await {
                        return (soft("append", e), St::Done);
                    }
                    if kind == EventKind::ToolResult
                        && let Some(port) = self.inner.tool_post.clone()
                        && let Some(content) = payload.get("content")
                    {
                        let call_id = content
                            .get("call_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned();
                        match live.tool_names.get(&call_id) {
                            Some(name) => {
                                port.tool_post(ToolPostView {
                                    call_id,
                                    name: name.clone(),
                                    is_error: content
                                        .get("is_error")
                                        .and_then(Value::as_bool)
                                        .unwrap_or(false),
                                    output: content.get("output").cloned().unwrap_or(Value::Null),
                                })
                                .await;
                            }
                            None => {
                                tracing::warn!(
                                    target: "kian::harness",
                                    call_id = %call_id,
                                    "tool/post skipped: no matching mapped tool/call"
                                );
                            }
                        }
                    }
                }
                Ok(SessionEvent::Approval(request)) => {
                    let (decision, matched) = self
                        .inner
                        .config
                        .policy
                        .decide(&request.tool, &request.input);
                    let mut allow = decision == Decision::Allow;
                    let mut policy = "configured";
                    let mut matched = matched;
                    let mut reason = None;
                    if let Some(port) = self.inner.tool_pre.clone() {
                        let verdict = port
                            .tool_pre(ToolPreCall {
                                session: ctx.session(),
                                call_id: request
                                    .call_id
                                    .clone()
                                    .unwrap_or_else(|| request.id.clone()),
                                name: request.tool.clone(),
                                iteration: live.turn_iteration,
                                arguments: request.input.clone(),
                                config_allow: allow,
                                config_matched: matched.clone(),
                            })
                            .await;
                        if let ToolPreVerdict::Deny {
                            plugin,
                            reason: hook_reason,
                        } = verdict
                        {
                            allow = false;
                            policy = "hook";
                            matched = format!("{plugin}:tool/pre");
                            reason = Some(hook_reason);
                        }
                    }
                    let answer = ApprovalAnswer {
                        id: request.id.clone(),
                        allow,
                        policy,
                        matched,
                        reason,
                    };
                    match within_step_bound(bound, "approval answer", live.session.answer(answer))
                        .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            return (
                                LoopOutcome::SoftFailed {
                                    reason: format!("bridge approval answer: {e}"),
                                },
                                St::Done,
                            );
                        }
                        Err(reason) => return (LoopOutcome::SoftFailed { reason }, St::Done),
                    }
                }
                Ok(SessionEvent::Exit { code }) => {
                    if live.turn_open {
                        return (
                            LoopOutcome::SoftFailed {
                                reason: format!(
                                    "harness child exited mid-turn (code {code:?}) — interrupted, \
                                     never completed"
                                ),
                            },
                            St::Done,
                        );
                    }
                    return (LoopOutcome::Settled, St::Done);
                }
                Err(e) => return (contract_error(e), St::Done),
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum Step {
    Yield(LoopOutcome, St),
    End,
}

/// Delivers one admitted payload as the harness's control frame.
async fn deliver(
    live: &mut LiveSession,
    payload: &Value,
    bound: Option<Duration>,
) -> Result<(), String> {
    let key = serde_json::to_string(payload).unwrap_or_default();
    if live.delivered.contains(&key) {
        return Ok(());
    }
    let content = payload.get("content").cloned().unwrap_or(Value::Null);
    let control = if payload.get("synthetic").and_then(Value::as_bool) == Some(true) {
        Control::Steer { content }
    } else {
        Control::Input { content }
    };
    match within_step_bound(
        bound,
        "input delivery (control write)",
        live.session.send(control),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(format!("bridge control write: {e}")),
        Err(reason) => return Err(reason),
    }
    live.delivered.insert(key);
    Ok(())
}

/// Bounds one wait on the harness child by the configured open-step inactivity bound (ADR-044).
///
/// `step_timeout_ms = 0` disables the bound (the pre-ADR wait). The `Err` is the driver's honest
/// `SoftFailed` reason: the child is unresponsive, never "completed".
async fn within_step_bound<T>(
    bound: Option<Duration>,
    what: &'static str,
    fut: impl std::future::Future<Output = T>,
) -> Result<T, String> {
    let Some(bound) = bound else {
        return Ok(fut.await);
    };
    match tokio::time::timeout(bound, fut).await {
        Ok(value) => Ok(value),
        Err(_) => Err(format!(
            "harness {what}: no progress within the step bound ({} ms, HARNESS-BRIDGE §4.5, \
             ADR-044) — the child is unresponsive; ending honestly",
            bound.as_millis()
        )),
    }
}

/// The engine's row→message convention (mirrors the orchestrator's consumption plan).
fn consumption_payload(row_payload: &Value) -> Value {
    if let Some(obj) = row_payload.as_object() {
        if obj.get("synthetic").and_then(Value::as_bool) == Some(true)
            && let Some(content) = obj.get("content")
        {
            return json!({"role": "user", "content": content, "synthetic": true});
        }
        if obj.len() == 2
            && let (Some(content), Some(_spill)) = (obj.get("content"), obj.get("spill"))
        {
            return json!({"role": "user", "content": content});
        }
    }
    json!({"role": "user", "content": row_payload})
}

/// Stamps the driver-owned iteration onto the adapter's `turn/end` payload.
fn turn_end_payload(payload: &Value, iteration: u64) -> Value {
    let mut payload = payload.clone();
    if let Some(obj) = payload.as_object_mut() {
        obj.insert("iteration".to_owned(), json!(iteration));
        payload
    } else {
        json!({"iteration": iteration})
    }
}

/// The `input/admitted` events appended after the last `drain/claimed` — this drain's promoted
/// batch (the engine promotes after appending its claim and before `drive`).
fn fresh_promotions(events: &[Event]) -> Vec<Value> {
    let last_claim = events
        .iter()
        .rposition(|ev| matches!(ev.kind, EventKind::DrainClaimed))
        .map(|idx| idx + 1)
        .unwrap_or(0);
    events[last_claim..]
        .iter()
        .filter(|ev| ev.kind == EventKind::InputAdmitted)
        .map(|ev| ev.bytes.clone())
        .collect()
}

/// Turns already persisted in the stream (the budget counter's cross-episode piece).
async fn count_steps(ctx: &Arc<dyn BridgeCtx>) -> u64 {
    ctx.events()
        .await
        .map(|events| {
            events
                .iter()
                .filter(|ev| matches!(ev.kind, EventKind::TurnStart))
                .count() as u64
        })
        .unwrap_or(0)
}

fn soft(what: &str, e: BridgeCtxError) -> LoopOutcome {
    LoopOutcome::SoftFailed {
        reason: format!("bridge {what}: {e}"),
    }
}

fn contract_error(e: ContractError) -> LoopOutcome {
    LoopOutcome::SoftFailed {
        reason: format!("bridge contract: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ADR-044: the bound passes progress through, is disabled by `0`, and names itself in the
    /// honest failure when the child goes silent.
    #[tokio::test]
    async fn within_step_bound_passes_progress_through_and_names_the_bound() {
        assert_eq!(
            within_step_bound(Some(Duration::from_millis(500)), "x", async { 7 }).await,
            Ok(7)
        );
        assert_eq!(
            within_step_bound(None, "x", async { 8 }).await,
            Ok(8),
            "`0` disables the bound"
        );
        let err = within_step_bound(
            Some(Duration::from_millis(30)),
            "control write",
            std::future::pending::<()>(),
        )
        .await
        .expect_err("silence must be bounded");
        assert!(err.contains("control write"), "{err}");
        assert!(err.contains("step bound"), "{err}");
        assert!(err.contains("ADR-044"), "{err}");
    }
}
