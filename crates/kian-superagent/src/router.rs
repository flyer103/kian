//! The Router profile (card C26): the resident Router session, its
//! judge prompt, dispatch tools, and the residency hold.
//!
//! ## Resident Router session
//!
//! The Router is a *resident* session: a configured [`SessionId`]
//! bootstrapped through the C23 admit path (the first execution hold
//! appends `session/created` — no server API is widened for it). It
//! runs the built-in driver wrapped by [`RouterDriver`], which owns
//! two residency rules:
//!
//! 1. **Park while workers run.** After a provider turn that leaves
//!    delegated threads without a consumed completion report, the
//!    driver parks *before* the next provider turn (no judge turn is
//!    spent re-asking an unchanged context) and resumes exactly when a
//!    new input row lands in the Router's inbox (the worker's
//!    completion report, admitted durably before its wake — invariant
//!    1). This is the design point C25 routed here: a terminal Router
//!    session freezes, so the hold is kept alive while workers run.
//! 2. **Never settle on an idle final answer.** A `Settled` outcome
//!    with the inbox drained is swallowed and the driver parks for the
//!    next admitted batch instead of propagating it — the session
//!    stays resident. Intentional stops propagate honestly; soft
//!    failures propagate honestly (the session records `failed` and
//!    returns to idle, resumable by the next admit).
//!
//! Parking is sliced by a tick; each tick yields `TurnDone` so episode
//! supervision observes interrupts promptly. Parking never makes a
//! provider call and never writes an event.
//!
//! ## Dispatch tools
//!
//! [`RouterTools`] is the Router's whole model-visible toolset:
//! `spawn_worker` (C25 mechanics + the Router-side `thread/started`),
//! `reply` (ImPolicy-shaped) and `deliver_structured`
//! (StructuredPolicy-checked). Workers never hold these
//! ([`crate::templates`] enforces the structural diff); the Router
//! aggregates and replies, workers act and report.
//!
//! ## Ports (layered iron rule)
//!
//! Every stream write from a tool goes through the [`StreamPort`]
//! installed per hold by [`RouterDriver`]; a stale port's writes are
//! fenced out by the store (NoSplitBrainWrite), so the ambient slot is
//! safe. Inbox writes go through [`InboxWriter`]; [`RouterInbox`] is
//! the wrapper assembly should mount so residency parking observes
//! admits. No server symbol is named here (`tests/iron_rule.rs`).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use kian_core::events::{Event, EventKind};
use kian_core::model::{AgentDef, SessionId};
use kian_core::seams::{
    ChannelAdapter, ChannelDelivery, LoopDriver, LoopOutcome, ToolCall, ToolError, ToolProvider,
    ToolResult, ToolSchema,
};
use kian_orchestrator::WakeOutcome;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::inbox::{AdmitReceipt, InboxWriteError, InboxWriter};
use crate::policy::{
    ImPolicy, StructuredPolicy, im_delivered_payload, structured_delivered_payload,
};
use crate::templates::SPAWN_WORKER_TOOL;
use crate::worker::{
    PortFor, StreamPort, WorkerSpawner, parse_thread_report, thread_started_payload,
};

/// The `reply` tool name (Router-only).
pub const REPLY_TOOL: &str = "reply";

/// The `deliver_structured` tool name (Router-only).
pub const DELIVER_STRUCTURED_TOOL: &str = "deliver_structured";

/// Idempotency key of the Router's one bootstrap input.
pub const ROUTER_BOOTSTRAP_KEY: &str = "router:bootstrap";

/// Default judge-prompt template. `{inputs}` is the pending-input count
/// of the batch being judged.
pub const JUDGE_PROMPT_TEMPLATE: &str = "You are the Router of a super-agent. Judge every pending \
input as ONE batch (a single hold sweep merges them); never judge inputs one at a time. For each \
batch decide: dispatch work to a worker with spawn_worker, reply directly with reply (plain text, \
the channel enforces the word limit), or deliver finished structured work with \
deliver_structured. Worker completion reports arrive as later inputs — aggregate them before \
replying. Pending inputs in this batch: {inputs}";

/// Parking tick default: how often a parked Router re-checks its inbox
/// signal (and lets supervision observe an interrupt).
pub const DEFAULT_PARK_TICK_MS: u64 = 50;

// ---------------------------------------------------------------------------
// Judge prompt
// ---------------------------------------------------------------------------

/// The Router's judge prompt template (declarative, part of the
/// profile; rendered at bootstrap).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JudgePrompt {
    pub template: String,
}

impl Default for JudgePrompt {
    fn default() -> Self {
        Self {
            template: JUDGE_PROMPT_TEMPLATE.to_string(),
        }
    }
}

impl JudgePrompt {
    /// Renders the prompt for a batch of `pending` inputs.
    #[must_use]
    pub fn render(&self, pending: usize) -> String {
        self.template.replace("{inputs}", &pending.to_string())
    }
}

// ---------------------------------------------------------------------------
// RouterProfile
// ---------------------------------------------------------------------------

/// The resident Router profile: configured session id, the Router
/// [`AgentDef`] (cheap fast `router` slot, dispatch + channel toolset),
/// the judge prompt template, and the §4.2 output-policy templates the
/// dispatch tools enforce.
#[derive(Debug, Clone, PartialEq)]
pub struct RouterProfile {
    /// The configured resident Router session id (bootstrap admits to
    /// it; the first hold creates the session).
    pub session: SessionId,
    /// The Router agent definition ([`crate::templates::router_template`]).
    pub agent: AgentDef,
    pub judge: JudgePrompt,
    pub im: ImPolicy,
    pub structured: StructuredPolicy,
}

impl RouterProfile {
    /// A profile over `agent` with the default judge prompt and §4.2
    /// policy templates.
    #[must_use]
    pub fn new(session: SessionId, agent: AgentDef) -> Self {
        Self {
            session,
            agent,
            judge: JudgePrompt::default(),
            im: ImPolicy::default(),
            structured: StructuredPolicy::default(),
        }
    }

    /// Bootstraps the resident session via the C23 admit path: one
    /// durable judge-prompt input to the configured (possibly
    /// not-yet-existing) session id — the first execution hold appends
    /// `session/created`. Idempotent per [`ROUTER_BOOTSTRAP_KEY`], so
    /// assembly may call it at every startup.
    pub async fn bootstrap(
        &self,
        inbox: &dyn InboxWriter,
    ) -> Result<AdmitReceipt, InboxWriteError> {
        inbox
            .admit(
                self.session,
                json!(self.judge.render(0)),
                Some(ROUTER_BOOTSTRAP_KEY.to_string()),
            )
            .await
    }
}

// ---------------------------------------------------------------------------
// Channel registry
// ---------------------------------------------------------------------------

/// Name → adapter lookup for the Router's channel-facing tools.
/// Assembly builds it from config (C27); the tools send through it and
/// never construct adapters themselves.
#[derive(Clone, Default)]
pub struct ChannelRegistry {
    adapters: BTreeMap<String, Arc<dyn ChannelAdapter>>,
}

impl ChannelRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers (or replaces) the adapter for `name`.
    pub fn register(&mut self, name: impl Into<String>, adapter: Arc<dyn ChannelAdapter>) {
        self.adapters.insert(name.into(), adapter);
    }

    /// The adapter registered for `name`.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Arc<dyn ChannelAdapter>> {
        self.adapters.get(name).cloned()
    }

    /// Sorted registered channel names.
    #[must_use]
    pub fn names(&self) -> Vec<String> {
        self.adapters.keys().cloned().collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.adapters.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.adapters.is_empty()
    }
}

impl std::fmt::Debug for ChannelRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelRegistry")
            .field("channels", &self.names())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// RouterHold — the ambient per-hold stream port for the tools
// ---------------------------------------------------------------------------

/// The ambient slot holding the current hold's [`StreamPort`].
/// [`RouterDriver`] installs one port per execution hold; the dispatch
/// tools read it to append lineage and delivery events to the Router's
/// own stream mid-turn. A port left over from a dead hold is harmless:
/// its writes carry the dead epoch and the store fences them out
/// (NoSplitBrainWrite).
#[derive(Default)]
pub struct RouterHold {
    port: RwLock<Option<Arc<dyn StreamPort>>>,
}

impl std::fmt::Debug for RouterHold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterHold")
            .field("port_installed", &self.port.read().is_ok())
            .finish()
    }
}

impl RouterHold {
    /// A fresh shared hold slot.
    #[must_use]
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Installs the port for the current execution hold.
    pub fn install(&self, port: Arc<dyn StreamPort>) {
        *self.port.write().expect("router hold lock") = Some(port);
    }

    /// The current hold's port, if one was installed.
    #[must_use]
    pub fn port(&self) -> Option<Arc<dyn StreamPort>> {
        self.port.read().expect("router hold lock").clone()
    }
}

// ---------------------------------------------------------------------------
// RouterTools — the dispatch + channel toolset
// ---------------------------------------------------------------------------

/// The Router's whole model-visible toolset: `spawn_worker`, `reply`,
/// `deliver_structured`. Constructed from the profile, the worker
/// spawner, the channel registry, and the shared hold slot.
pub struct RouterTools {
    spawner: Arc<WorkerSpawner>,
    channels: ChannelRegistry,
    im: ImPolicy,
    structured: StructuredPolicy,
    hold: Arc<RouterHold>,
}

impl RouterTools {
    #[must_use]
    pub fn new(
        spawner: Arc<WorkerSpawner>,
        channels: ChannelRegistry,
        profile: &RouterProfile,
        hold: Arc<RouterHold>,
    ) -> Self {
        Self {
            spawner,
            channels,
            im: profile.im.clone(),
            structured: profile.structured.clone(),
            hold,
        }
    }

    /// The tool names this provider exposes (the Router-only surface).
    #[must_use]
    pub fn tool_names(&self) -> Vec<String> {
        self.schemas().iter().map(|s| s.name.clone()).collect()
    }

    fn port(&self) -> Result<Arc<dyn StreamPort>, ToolError> {
        self.hold.port().ok_or_else(|| {
            ToolError::Execution("router stream port not installed for this hold".into())
        })
    }

    async fn spawn_worker(&self, call: ToolCall) -> Result<ToolResult, ToolError> {
        // The port is checked before any side effect: a spawn whose
        // lineage event cannot be recorded must not happen at all.
        let port = self.port()?;
        let task = call
            .arguments
            .get("task")
            .cloned()
            .ok_or_else(|| ToolError::InvalidCall("spawn_worker requires `task`".into()))?;
        let correlation = call.arguments.get("correlation").cloned();
        let spawned = self
            .spawner
            .spawn(task, correlation)
            .await
            .map_err(|e| ToolError::Execution(e.to_string()))?;
        // Router-side lineage at dispatch: identical bytes to the
        // worker-side event (the delegation pair is discoverable by
        // scanning both streams).
        port.emit(
            EventKind::ThreadStarted,
            thread_started_payload(
                &spawned.thread,
                spawned.worker_session,
                &spawned.task_message,
            ),
        )
        .await
        .map_err(|e| ToolError::Execution(format!("thread/started write failed: {e}")))?;
        Ok(ToolResult {
            call_id: call.id,
            output: json!({
                "thread_id": spawned.thread.id.to_string(),
                "worker": spawned.worker_session.0.to_string(),
                "status": "dispatched",
            }),
            is_error: false,
        })
    }

    async fn reply(&self, call: ToolCall) -> Result<ToolResult, ToolError> {
        let port = self.port()?;
        let args = &call.arguments;
        let channel = str_arg(args, "channel", "reply")?;
        let text = str_arg(args, "text", "reply")?;
        let target = args.get("target").cloned().unwrap_or(Value::Null);
        let adapter = self
            .channels
            .get(channel)
            .ok_or_else(|| ToolError::InvalidCall(format!("unknown channel `{channel}`")))?;

        // §4.2 IM policy: shape the text BEFORE the send (policy lives
        // in the bundle, not the adapter).
        let enforced = self.im.enforce(text);
        let delivery = ChannelDelivery {
            channel: channel.to_string(),
            target: target.clone(),
            content: json!({"text": enforced.text}),
        };
        let receipt = adapter
            .send(delivery)
            .await
            .map_err(|e| ToolError::Execution(e.to_string()))?;
        // channel/delivered lands only after the send succeeded.
        port.emit(
            EventKind::ChannelDelivered,
            im_delivered_payload(channel, &target, &enforced),
        )
        .await
        .map_err(|e| ToolError::Execution(format!("channel/delivered write failed: {e}")))?;
        Ok(ToolResult {
            call_id: call.id,
            output: json!({
                "message_id": receipt.message_id,
                "words": enforced.words,
                "truncated": enforced.truncated,
            }),
            is_error: false,
        })
    }

    async fn deliver_structured(&self, call: ToolCall) -> Result<ToolResult, ToolError> {
        let port = self.port()?;
        let args = &call.arguments;
        let channel = str_arg(args, "channel", "deliver_structured")?;
        let target = args
            .get("target")
            .cloned()
            .ok_or_else(|| ToolError::InvalidCall("deliver_structured requires `target`".into()))?;
        let payload = args.get("payload").cloned().ok_or_else(|| {
            ToolError::InvalidCall("deliver_structured requires `payload`".into())
        })?;
        let adapter = self
            .channels
            .get(channel)
            .ok_or_else(|| ToolError::InvalidCall(format!("unknown channel `{channel}`")))?;

        // §4.2 structured contract: a payload missing a section is
        // rejected honestly — error to the loop, never silently sent,
        // no delivery, no channel/delivered.
        self.structured
            .validate(&payload)
            .map_err(|e| ToolError::InvalidCall(e.to_string()))?;

        let delivery = ChannelDelivery {
            channel: channel.to_string(),
            target: target.clone(),
            content: payload,
        };
        let receipt = adapter
            .send(delivery)
            .await
            .map_err(|e| ToolError::Execution(e.to_string()))?;
        port.emit(
            EventKind::ChannelDelivered,
            structured_delivered_payload(channel, &target),
        )
        .await
        .map_err(|e| ToolError::Execution(format!("channel/delivered write failed: {e}")))?;
        Ok(ToolResult {
            call_id: call.id,
            output: json!({"message_id": receipt.message_id, "sections_delivered": true}),
            is_error: false,
        })
    }
}

/// A required string tool argument.
fn str_arg<'a>(args: &'a Value, key: &str, tool: &str) -> Result<&'a str, ToolError> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::InvalidCall(format!("{tool} requires a string `{key}`")))
}

impl ToolProvider for RouterTools {
    fn schemas(&self) -> Vec<ToolSchema> {
        vec![
            ToolSchema {
                name: SPAWN_WORKER_TOOL.to_string(),
                description: "Dispatch a task to a fresh worker session; returns the thread id. "
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "task": {"type": "string", "description": "The actual work, as user content"},
                        "correlation": {"description": "Optional correlation token (e.g. event id)"}
                    },
                    "required": ["task"],
                }),
            },
            ToolSchema {
                name: REPLY_TOOL.to_string(),
                description: "Reply plain text on a channel (word-limit shaped by policy).".into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "channel": {"type": "string"},
                        "text": {"type": "string"},
                        "target": {"description": "Channel-defined delivery target"}
                    },
                    "required": ["channel", "text"],
                }),
            },
            ToolSchema {
                name: DELIVER_STRUCTURED_TOOL.to_string(),
                description: "Deliver a structured report (required sections: background, \
changes, verification)."
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "channel": {"type": "string"},
                        "target": {"description": "Channel-defined delivery target"},
                        "payload": {"type": "object", "description": "Must carry every required section"}
                    },
                    "required": ["channel", "target", "payload"],
                }),
            },
        ]
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        // The boxed future must not borrow the provider (the loop moves
        // the call in and awaits later); cloning shares the same
        // spawner, registry, policies and hold slot.
        let this = self.clone();
        Box::pin(async move {
            match call.name.as_str() {
                s if s == SPAWN_WORKER_TOOL => this.spawn_worker(call).await,
                s if s == REPLY_TOOL => this.reply(call).await,
                s if s == DELIVER_STRUCTURED_TOOL => this.deliver_structured(call).await,
                other => Err(ToolError::InvalidCall(format!(
                    "unknown router tool `{other}`"
                ))),
            }
        })
    }
}

/// `ToolProvider` takes `&self` and hands out boxed futures that must
/// not borrow the provider; the clone shares the same spawner,
/// registry, policies and hold slot, so one provider instance serves a
/// whole resident Router.
impl Clone for RouterTools {
    fn clone(&self) -> Self {
        Self {
            spawner: Arc::clone(&self.spawner),
            channels: self.channels.clone(),
            im: self.im.clone(),
            structured: self.structured.clone(),
            hold: Arc::clone(&self.hold),
        }
    }
}

// ---------------------------------------------------------------------------
// RouterInbox — the residency signal
// ---------------------------------------------------------------------------

/// [`InboxWriter`] wrapper assembly mounts in front of the port
/// implementation: admits to the Router session are counted so
/// [`RouterDriver`] can park until the *next* durable row (a worker
/// completion report, a channel event, a steer) lands. The row is
/// durable before the counter advances (the port contract resolves the
/// admit future only on commit), so a parked Router that observes the
/// advance will find the row eligible on its next inner pass.
pub struct RouterInbox {
    inner: Arc<dyn InboxWriter>,
    router: SessionId,
    admits: Arc<AtomicU64>,
}

impl RouterInbox {
    /// Wraps `inner`; admits to `router` advance the counter.
    #[must_use]
    pub fn new(inner: Arc<dyn InboxWriter>, router: SessionId) -> Arc<Self> {
        Arc::new(Self {
            inner,
            router,
            admits: Arc::new(AtomicU64::new(0)),
        })
    }

    /// New durable rows admitted to the Router session so far
    /// (deduplicated re-admits do not advance it — the original row is
    /// the signal).
    #[must_use]
    pub fn admit_count(&self) -> u64 {
        self.admits.load(Ordering::Acquire)
    }
}

impl InboxWriter for RouterInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        let is_router = session == self.router;
        let counter = Arc::clone(&self.admits);
        let fut = self.inner.admit(session, payload, idempotency_key);
        Box::pin(async move {
            let receipt = fut.await?;
            if is_router && !receipt.deduplicated {
                counter.fetch_add(1, Ordering::Release);
            }
            Ok(receipt)
        })
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.inner.wake(session)
    }
}

// ---------------------------------------------------------------------------
// Outstanding-thread projection (pure)
// ---------------------------------------------------------------------------

/// Thread ids the Router dispatched (its own `thread/started` events)
/// that have no consumed completion report yet (an `input/admitted`
/// carrying the thread-report wrapper). A pure stream projection —
/// constraint 1: no resident state.
#[must_use]
pub fn outstanding_threads(events: &[Event]) -> Vec<Uuid> {
    let mut started: BTreeSet<Uuid> = BTreeSet::new();
    for ev in events {
        if ev.kind == EventKind::ThreadStarted
            && let Some(id) = event_thread_id(ev)
        {
            started.insert(id);
        }
    }
    for ev in events {
        if ev.kind != EventKind::InputAdmitted {
            continue;
        }
        if let Some(report) = ev.bytes.get("content").and_then(parse_thread_report) {
            started.remove(&report.thread_id);
        }
    }
    started.into_iter().collect()
}

/// `bytes.thread_id` as a uuid, if present and well-formed.
fn event_thread_id(ev: &Event) -> Option<Uuid> {
    ev.bytes
        .get("thread_id")
        .and_then(Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

// ---------------------------------------------------------------------------
// RouterDriver — residency decorator over the built-in loop
// ---------------------------------------------------------------------------

/// A decorator over any inner [`LoopDriver`] that makes it the resident
/// Router loop:
///
/// - installs the per-hold [`StreamPort`] into the shared
///   [`RouterHold`] (the dispatch tools' write surface) before the
///   first outcome;
/// - parks between provider turns while delegated threads are
///   outstanding and no new input has landed (no judge turn is spent
///   on an unchanged context; the hold stays alive while workers run);
/// - parks instead of propagating `Settled` (a resident session never
///   freezes on an idle final answer — a later batch re-drives the
///   inner loop);
/// - propagates `Stopped` (intentional stop) and `SoftFailed` (honest
///   failure) unchanged.
pub struct RouterDriver<D: LoopDriver> {
    inner: Arc<DriverCore<D>>,
}

struct DriverCore<D: LoopDriver> {
    driver: Arc<D>,
    hold: Arc<RouterHold>,
    port_for: PortFor<D::Ctx>,
    inbox: Arc<RouterInbox>,
    park_tick: Duration,
}

impl<D: LoopDriver> Clone for RouterDriver<D> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<D: LoopDriver> RouterDriver<D> {
    /// Wraps `driver`. `inbox` must be the mounted [`RouterInbox`] (the
    /// residency signal); `port_for` builds the per-hold port
    /// (assembly's job, the same shape as the worker wrapper).
    pub fn new(
        driver: Arc<D>,
        hold: Arc<RouterHold>,
        port_for: PortFor<D::Ctx>,
        inbox: Arc<RouterInbox>,
    ) -> Self {
        Self::with_park_tick(
            driver,
            hold,
            port_for,
            inbox,
            Duration::from_millis(DEFAULT_PARK_TICK_MS),
        )
    }

    /// [`RouterDriver::new`] with an explicit park tick (tests).
    #[must_use]
    pub fn with_park_tick(
        driver: Arc<D>,
        hold: Arc<RouterHold>,
        port_for: PortFor<D::Ctx>,
        inbox: Arc<RouterInbox>,
        park_tick: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(DriverCore {
                driver,
                hold,
                port_for,
                inbox,
                park_tick,
            }),
        }
    }
}

/// The decorator's phases. `First` installs the port and starts the
/// inner loop unconditionally (a fresh hold must consume whatever is
/// already eligible — takeover included); `Inner` delegates; `Park`
/// waits for the next durable Router admit, ticking out `TurnDone`s so
/// supervision observes interrupts.
enum Phase<Ctx> {
    First(Ctx),
    Inner {
        ctx: Ctx,
        stream: BoxStream<'static, LoopOutcome>,
        /// Inbox counter at the moment the inner loop last (re)started.
        seen: u64,
    },
    Park {
        ctx: Ctx,
        seen: u64,
    },
    Done,
}

impl<D> LoopDriver for RouterDriver<D>
where
    D: LoopDriver + 'static,
    D::Ctx: Clone + Send + Sync + 'static,
{
    type Ctx = D::Ctx;

    fn drive(&self, ctx: D::Ctx) -> BoxStream<'static, LoopOutcome> {
        let core = Arc::clone(&self.inner);
        // One port per execution hold; the tools share it through the
        // hold slot. A fresh hold always replaces the previous port
        // (its writes would be fenced out anyway — see RouterHold).
        let port = (core.port_for)(&ctx);
        core.hold.install(port);
        futures::stream::unfold(Phase::First(ctx), move |phase| {
            let core = Arc::clone(&core);
            async move {
                let mut phase = phase;
                loop {
                    match phase {
                        Phase::Done => return None,
                        Phase::First(ctx) => {
                            // A fresh hold always runs the inner loop
                            // first: its opening pass takes whatever is
                            // already eligible (a report admitted while
                            // the Router was idle, takeover included) —
                            // parking before that would strand the row.
                            let seen = core.inbox.admit_count();
                            let stream = core.driver.drive(ctx.clone());
                            phase = Phase::Inner { ctx, stream, seen };
                        }
                        Phase::Inner {
                            ctx,
                            mut stream,
                            seen,
                        } => {
                            return match stream.next().await {
                                // Keep iterating — but if delegated
                                // threads are outstanding and nothing
                                // new landed, park before the next
                                // provider turn (their reports wake us;
                                // no judge turn is spent on unchanged
                                // context).
                                Some(LoopOutcome::TurnDone) => {
                                    // Park iff delegated threads are
                                    // outstanding AND no durable row
                                    // landed since the inner loop (re)
                                    // started (`seen` is that baseline;
                                    // a later row is pending work the
                                    // next pass takes).
                                    let proceed = core.inbox.admit_count() != seen
                                        || !has_outstanding_threads(&core).await;
                                    let next = if proceed {
                                        Phase::Inner { ctx, stream, seen }
                                    } else {
                                        Phase::Park { ctx, seen }
                                    };
                                    Some((LoopOutcome::TurnDone, next))
                                }
                                // Resident session: an idle final
                                // answer parks for the next batch
                                // instead of freezing the session.
                                Some(LoopOutcome::Settled) => {
                                    Some((LoopOutcome::TurnDone, Phase::Park { ctx, seen }))
                                }
                                // Honest terminals propagate unchanged:
                                // intentional stops and soft failures
                                // stay visible in the stream.
                                Some(outcome) => Some((outcome, Phase::Done)),
                                None => Some((
                                    LoopOutcome::SoftFailed {
                                        reason: "router loop ended without a terminal outcome"
                                            .into(),
                                    },
                                    Phase::Done,
                                )),
                            };
                        }
                        Phase::Park { ctx, seen } => {
                            let now = core.inbox.admit_count();
                            if now != seen {
                                // A durable row landed while parked:
                                // re-drive the inner loop; its opening
                                // pass takes the eligible inputs.
                                let stream = core.driver.drive(ctx.clone());
                                phase = Phase::Inner {
                                    ctx,
                                    stream,
                                    seen: now,
                                };
                                continue;
                            }
                            // Tick: give supervision a safe boundary (it
                            // observes interrupts at outcomes) and keep
                            // parking. No provider call, no event write.
                            tokio::time::sleep(core.park_tick).await;
                            return Some((LoopOutcome::TurnDone, Phase::Park { ctx, seen }));
                        }
                    }
                }
            }
        })
        .boxed()
    }
}

/// True when the Router's own stream shows delegated threads without a
/// consumed completion report ([`outstanding_threads`]). On any port or
/// read failure returns `false` (keep looping) — a parking decision
/// must never strand the loop on a read failure.
async fn has_outstanding_threads<D: LoopDriver>(core: &DriverCore<D>) -> bool {
    let Some(port) = core.hold.port() else {
        return false;
    };
    match port.read_stream().await {
        Ok(events) => !outstanding_threads(&events).is_empty(),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use futures::future::BoxFuture;
    use kian_core::events::{Event, EventKind};
    use kian_core::model::{Epoch, SessionId};
    use kian_core::seams::{ChannelAdapter, ToolCall, ToolProvider};
    use serde_json::json;

    use super::{
        ChannelRegistry, DELIVER_STRUCTURED_TOOL, JUDGE_PROMPT_TEMPLATE, JudgePrompt, REPLY_TOOL,
        ROUTER_BOOTSTRAP_KEY, RouterHold, RouterInbox, RouterProfile, RouterTools,
        outstanding_threads,
    };
    use crate::inbox::{AdmitReceipt, InboxWriteError, InboxWriter};
    use crate::mock::MockChannel;
    use crate::policy::{ImPolicy, StructuredPolicy};
    use crate::templates::{
        ROUTER_SLOT, SPAWN_WORKER_TOOL, SuperAgentProfile, router_template, worker_template,
    };
    use crate::worker::{StreamPort, StreamPortError, WorkerSpawner};
    use kian_orchestrator::WakeOutcome;

    fn sid(n: u128) -> SessionId {
        SessionId(uuid::Uuid::from_u128(n))
    }

    fn tid(n: u128) -> uuid::Uuid {
        uuid::Uuid::from_u128(n)
    }

    fn event(session: SessionId, seq: u64, kind: EventKind, bytes: serde_json::Value) -> Event {
        Event {
            seq,
            session,
            epoch: Epoch(2),
            ts: 1_000 + seq,
            kind,
            bytes,
            version: 0,
        }
    }

    #[test]
    fn judge_prompt_renders_the_pending_count() {
        let prompt = JudgePrompt::default();
        assert!(prompt.template.contains("{inputs}"));
        assert_eq!(JUDGE_PROMPT_TEMPLATE, prompt.template);
        let rendered = prompt.render(3);
        assert!(rendered.contains("Pending inputs in this batch: 3"));
        assert!(!rendered.contains("{inputs}"));
        let custom = JudgePrompt {
            template: "judge {inputs} inputs".into(),
        };
        assert_eq!(custom.render(0), "judge 0 inputs");
    }

    #[test]
    fn profile_carries_the_router_slot_and_policy_templates() {
        let session = sid(1);
        let profile = RouterProfile::new(session, router_template("mock", "mock-router"));
        assert_eq!(profile.session, session);
        assert!(
            profile
                .agent
                .model_slots
                .contains_key(&kian_core::model::SlotName(ROUTER_SLOT.into()))
        );
        assert_eq!(profile.im, ImPolicy::default());
        assert_eq!(profile.structured, StructuredPolicy::default());
    }

    /// A recording port: the tools' emit surface without any server.
    struct RecordingPort {
        session: SessionId,
        emitted: Mutex<Vec<(EventKind, serde_json::Value)>>,
    }

    impl RecordingPort {
        fn shared(session: SessionId) -> (Arc<Self>, Arc<dyn StreamPort>) {
            let port = Arc::new(Self {
                session,
                emitted: Mutex::new(Vec::new()),
            });
            let dyn_port: Arc<dyn StreamPort> = Arc::clone(&port) as Arc<dyn StreamPort>;
            (port, dyn_port)
        }
    }

    impl StreamPort for RecordingPort {
        fn session(&self) -> SessionId {
            self.session
        }
        fn emit(
            &self,
            kind: EventKind,
            bytes: serde_json::Value,
        ) -> BoxFuture<'static, Result<(), StreamPortError>> {
            self.emitted.lock().unwrap().push((kind, bytes));
            Box::pin(async { Ok(()) })
        }
        fn read_stream(&self) -> BoxFuture<'static, Result<Vec<Event>, StreamPortError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    /// A scripted inbox (admits recorded, wakes recorded).
    struct ScriptedInbox {
        admits: Mutex<Vec<(SessionId, Option<String>)>>,
        dedup: bool,
    }

    impl InboxWriter for ScriptedInbox {
        fn admit(
            &self,
            session: SessionId,
            _payload: serde_json::Value,
            idempotency_key: Option<String>,
        ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
            self.admits.lock().unwrap().push((session, idempotency_key));
            let dedup = self.dedup;
            Box::pin(async move {
                Ok(AdmitReceipt {
                    deduplicated: dedup,
                })
            })
        }
        fn wake(&self, _session: SessionId) -> WakeOutcome {
            WakeOutcome::Folded
        }
    }

    fn tools_for_test() -> (
        Arc<RouterTools>,
        Arc<RouterHold>,
        Arc<MockChannel>,
        Arc<ScriptedInbox>,
    ) {
        let router = sid(10);
        let inbox: Arc<ScriptedInbox> = Arc::new(ScriptedInbox {
            admits: Mutex::new(Vec::new()),
            dedup: false,
        });
        let spawner = Arc::new(WorkerSpawner::new(
            router,
            &worker_template("mock", "mock-worker", 7),
            inbox.clone() as Arc<dyn InboxWriter>,
        ));
        let mock = Arc::new(MockChannel::new("mock-im"));
        let mut channels = ChannelRegistry::new();
        channels.register("mock-im", Arc::clone(&mock) as Arc<dyn ChannelAdapter>);
        let hold = RouterHold::shared();
        let profile = RouterProfile::new(router, router_template("mock", "mock-router"));
        (
            Arc::new(RouterTools::new(spawner, channels, &profile, hold.clone())),
            hold,
            mock,
            inbox,
        )
    }

    fn call(name: &str, args: serde_json::Value) -> ToolCall {
        ToolCall {
            id: "call-1".into(),
            name: name.into(),
            arguments: args,
        }
    }

    #[tokio::test]
    async fn bootstrap_admits_the_judge_prompt_once() {
        let inbox = Arc::new(ScriptedInbox {
            admits: Mutex::new(Vec::new()),
            dedup: false,
        });
        let router = sid(10);
        let profile = RouterProfile::new(router, router_template("mock", "mock-router"));
        profile
            .bootstrap(&*inbox as &dyn InboxWriter)
            .await
            .unwrap();
        let admits = inbox.admits.lock().unwrap().clone();
        assert_eq!(admits.len(), 1);
        assert_eq!(admits[0].0, router);
        assert_eq!(admits[0].1.as_deref(), Some(ROUTER_BOOTSTRAP_KEY));
    }

    #[test]
    fn channel_registry_resolves_by_name() {
        let mut channels = ChannelRegistry::new();
        assert!(channels.is_empty());
        channels.register("mock-im", Arc::new(MockChannel::new("mock-im")));
        channels.register("ci", Arc::new(MockChannel::new("ci")));
        assert!(channels.get("mock-im").is_some());
        assert!(channels.get("nope").is_none());
        assert_eq!(
            channels.names(),
            vec!["ci".to_string(), "mock-im".to_string()]
        );
        assert_eq!(channels.len(), 2);
    }

    #[test]
    fn hold_installs_and_reads_the_current_port() {
        let hold = RouterHold::shared();
        assert!(hold.port().is_none());
        let (_rec, dyn_port) = RecordingPort::shared(sid(10));
        hold.install(dyn_port);
        assert!(hold.port().is_some());
    }

    #[tokio::test]
    async fn reply_enforces_the_word_limit_and_appends_delivered() {
        let (tools, hold, mock, _inbox) = tools_for_test();
        let (rec, dyn_port) = RecordingPort::shared(sid(10));
        hold.install(dyn_port);

        let long_text: String = (1..=120)
            .map(|i| format!("w{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        let result = tools
            .invoke(call(
                REPLY_TOOL,
                json!({"channel": "mock-im", "text": long_text, "target": {"chat_id": 7}}),
            ))
            .await
            .unwrap();
        assert!(!result.is_error);
        assert_eq!(result.output["words"], json!(100));
        assert_eq!(result.output["truncated"], json!(true));

        // The delivery on the channel is the truncated text.
        let deliveries = mock.deliveries();
        assert_eq!(deliveries.len(), 1);
        let delivered = deliveries[0].delivery.content["text"].as_str().unwrap();
        assert_eq!(delivered.split_whitespace().count(), 100);
        assert!(delivered.ends_with('…'));

        // channel/delivered was appended after the send, C18 schema.
        let emitted = rec.emitted.lock().unwrap().clone();
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].0, EventKind::ChannelDelivered);
        assert_eq!(emitted[0].1["policy"], json!("im"));
        assert_eq!(emitted[0].1["words"], json!(100));
        assert_eq!(emitted[0].1["truncated"], json!(true));
        assert_eq!(emitted[0].1["channel"], json!("mock-im"));
    }

    #[tokio::test]
    async fn reply_without_a_port_fails_honestly_and_sends_nothing() {
        let (tools, hold, mock, _inbox) = tools_for_test();
        assert!(hold.port().is_none());
        let err = tools
            .invoke(call(
                REPLY_TOOL,
                json!({"channel": "mock-im", "text": "hi"}),
            ))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("port not installed"));
        // The port check precedes the send: nothing was delivered.
        assert!(mock.deliveries().is_empty());
    }

    #[tokio::test]
    async fn unknown_tool_and_missing_arguments_are_invalid_calls() {
        let (tools, hold, _mock, _inbox) = tools_for_test();
        let (_rec, dyn_port) = RecordingPort::shared(sid(10));
        hold.install(dyn_port);
        assert!(tools.invoke(call("nope", json!({}))).await.is_err());
        assert!(
            tools
                .invoke(call(REPLY_TOOL, json!({"text": "no channel"})))
                .await
                .is_err()
        );
        assert!(
            tools
                .invoke(call(REPLY_TOOL, json!({"channel": "nope", "text": "x"})))
                .await
                .is_err()
        );
        assert!(
            tools
                .invoke(call(SPAWN_WORKER_TOOL, json!({"correlation": "c"})))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn structured_delivery_missing_section_is_rejected_never_sent() {
        let (tools, hold, mock, _inbox) = tools_for_test();
        let (_rec, dyn_port) = RecordingPort::shared(sid(10));
        hold.install(dyn_port);
        let err = tools
            .invoke(call(
                DELIVER_STRUCTURED_TOOL,
                json!({
                    "channel": "mock-im",
                    "target": {"chat_id": 1},
                    "payload": {"background": "b", "changes": "c"},
                }),
            ))
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("missing required section `verification`")
        );
        assert!(
            mock.deliveries().is_empty(),
            "rejected payload is never sent"
        );
    }

    #[tokio::test]
    async fn router_inbox_counts_only_new_router_admits() {
        let inner = Arc::new(ScriptedInbox {
            admits: Mutex::new(Vec::new()),
            dedup: false,
        });
        let router = sid(10);
        let inbox = RouterInbox::new(inner.clone() as Arc<dyn InboxWriter>, router);
        assert_eq!(inbox.admit_count(), 0);
        inbox
            .admit(router, json!("a"), Some("k1".into()))
            .await
            .unwrap();
        inbox.admit(sid(20), json!("worker"), None).await.unwrap();
        assert_eq!(inbox.admit_count(), 1, "only router-session admits count");
        inbox.wake(router);
        assert_eq!(inner.admits.lock().unwrap().len(), 2);

        // A deduplicated re-admit does not advance the counter.
        let dedup_inner = Arc::new(ScriptedInbox {
            admits: Mutex::new(Vec::new()),
            dedup: true,
        });
        let inbox = RouterInbox::new(dedup_inner as Arc<dyn InboxWriter>, router);
        inbox
            .admit(router, json!("again"), Some("k1".into()))
            .await
            .unwrap();
        assert_eq!(inbox.admit_count(), 0);
    }

    #[test]
    fn outstanding_threads_projects_from_the_stream_alone() {
        let router = sid(10);
        let worker = sid(20);
        let started = |t: uuid::Uuid, w: SessionId| {
            event(
                router,
                1,
                EventKind::ThreadStarted,
                json!({
                    "thread_id": t.to_string(),
                    "parent": router.0.to_string(),
                    "worker": w.0.to_string(),
                    "pinned_version": 7,
                    "task": {},
                }),
            )
        };
        let report_input = |t: uuid::Uuid| {
            event(
                router,
                2,
                EventKind::InputAdmitted,
                json!({
                    "role": "user",
                    "content": {
                        "thread_id": t.to_string(),
                        "status": "completed",
                        "summary": {"text": "done"},
                    }
                }),
            )
        };
        // Two dispatched, one reported: exactly the other is outstanding.
        let events = vec![
            started(tid(0xaa), worker),
            started(tid(0xbb), sid(21)),
            report_input(tid(0xaa)),
        ];
        assert_eq!(outstanding_threads(&events), vec![tid(0xbb)]);
        // All reported: nothing outstanding.
        let mut all = events.clone();
        all.push(report_input(tid(0xbb)));
        assert!(outstanding_threads(&all).is_empty());
        // A failed report still resolves the thread.
        let failed = event(
            router,
            3,
            EventKind::InputAdmitted,
            json!({
                "role": "user",
                "content": {"thread_id": tid(0xbb).to_string(), "status": "failed", "summary": {}}
            }),
        );
        assert!(outstanding_threads(&[events[1].clone(), failed]).is_empty());
        // Plain user inputs resolve nothing.
        let plain = event(
            router,
            4,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "hello"}),
        );
        assert_eq!(
            outstanding_threads(&[events[1].clone(), plain]),
            vec![tid(0xbb)]
        );
    }

    #[test]
    fn toolset_diff_router_holds_channel_tools_worker_none() {
        // The structural diff asserted by the card: the Router's
        // toolset is exactly the dispatch surface; the worker template
        // holds none of it and the profile rejects a smuggle attempt.
        let (tools, _hold, _mock, _inbox) = tools_for_test();
        let names = tools.tool_names();
        for expected in [SPAWN_WORKER_TOOL, REPLY_TOOL, DELIVER_STRUCTURED_TOOL] {
            assert!(names.contains(&expected.to_string()), "missing {expected}");
        }
        assert_eq!(
            names.len(),
            3,
            "the dispatch surface is exactly three tools"
        );

        let worker = worker_template("mock", "mock-worker", 7);
        let router = router_template("mock", "mock-router");
        for tool in &names {
            assert!(
                !worker.tools.contains(tool),
                "worker must not hold `{tool}`"
            );
            assert!(router.tools.contains(tool), "router must hold `{tool}`");
        }
        SuperAgentProfile::new(router.clone(), worker.clone()).expect("pair validates");
        let mut smuggler = worker;
        smuggler.tools.push(REPLY_TOOL.into());
        assert!(SuperAgentProfile::new(router, smuggler).is_err());
    }
}
