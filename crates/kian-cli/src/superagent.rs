//! Super-agent assembly (card C27): the wiring that mounts the
//! `kian-superagent` bundle and `kian-exec` server half onto the real
//! engine, behind the same fail-closed discipline as the Phase-1
//! assembly.
//!
//! This module is assembly-layer code — the peer of [`crate::server`],
//! not of the HTTP handler modules. It names the engine, the store and
//! the drivers on purpose (that is what assembly is for); the layered
//! iron rule still holds at its edges: HTTP handlers reach any of this
//! only through the [`crate::api::ControlPlane`] / [`crate::ops::OpsPlane`]
//! traits, and the bundle reaches the server only through its
//! [`InboxWriter`] port (implemented here over the engine — the C23
//! cycle breaker is a late-bound engine slot).
//!
//! Composition (one engine, one driver — the C26 pattern):
//!
//! - the configured resident Router session runs under
//!   [`RouterDriver`] (port install + residency parking over a ReAct
//!   loop whose tools are the bundle's dispatch/channel tools);
//! - sessions with Thread lineage (a first input carrying the
//!   delegation wrapper, or an existing `thread/started`) run under
//!   [`WorkerLoopDriver`] (lineage + evidence gate + completion
//!   report);
//! - every other session is a plain Phase-1 session and runs the
//!   default agent's loop unchanged.
//!
//! Startup order is load-bearing (the card's acceptance): inbox
//! rebuild → watchdog/subscription start → listener accepts traffic.
//! [`startup_order_log`] records the sequence so the integration test
//! can assert it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_core::seams::{
    ChannelAdapter, LoopDriver, LoopOutcome, ToolCall, ToolError, ToolProvider, ToolResult,
    ToolSchema, WatchdogSource,
};
use kian_exec::watchdog::{Clock as ExecClock, SystemClock as ExecSystemClock};
use kian_exec::{EndpointRegistryRow, ExecStore, HeartbeatWatchdog, WatchdogConfig};
use kian_loop::evidence::EvidenceGateConfig;
use kian_loop::react::ReActLoop;
use kian_orchestrator::{DrainCtx, Engine, WakeOutcome};
use kian_store::EventStore;
use kian_superagent::{
    AdmitReceipt, InboxWriteError, InboxWriter, LineagePlan, MockChannel, PassthroughRegistry,
    PortFor, RouterDriver, RouterInbox, RouterMetrics, RouterProfile, StreamPort, StreamPortError,
    SubscriptionService, WorkerLoopDriver, lineage_plan, worker_sessions,
};

use crate::config::EndpointConfig;

// ---------------------------------------------------------------------------
// Startup order log (per-assembly; the C27 acceptance assertion)
// ---------------------------------------------------------------------------

/// The recorded startup sequence of one assembly. Purely observable
/// state for `tests/superagent_serve.rs`; production code never reads
/// it beyond recording.
pub type StartupOrder = Arc<Mutex<Vec<&'static str>>>;

/// A fresh, empty order log for one `Server::from_config` run.
pub fn new_startup_order() -> StartupOrder {
    Arc::new(Mutex::new(Vec::new()))
}

fn record(order: &StartupOrder, step: &'static str) {
    order.lock().expect("startup order lock").push(step);
}

/// Records that `startup_rebuild` returned (called by
/// `Server::from_config` between the rebuild and any service start).
pub fn record_rebuild_done(order: &StartupOrder) {
    record(order, "startup_rebuild_done");
}

// ---------------------------------------------------------------------------
// The InboxWriter port over the real engine (the C23 cycle breaker)
// ---------------------------------------------------------------------------

/// [`InboxWriter`] implemented over the engine's own admit + wake: the
/// row is durable when `admit` resolves (the engine resolves its admit
/// future only on commit — invariant 1), and the wake is strictly
/// after.
///
/// The engine is installed **after** the port exists (the engine needs
/// a driver, the driver's tools need this port): a late-bound slot,
/// filled exactly once by [`crate::server::Server::from_config`] before
/// the first wake can fire.
pub struct EngineInbox {
    engine: OnceLock<Arc<Engine<ServeLoopDriver>>>,
}

impl EngineInbox {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            engine: OnceLock::new(),
        })
    }

    /// Installs the engine. Fails (returns the engine back) if one is
    /// already installed — exactly one engine per port, ever.
    pub fn install(
        &self,
        engine: Arc<Engine<ServeLoopDriver>>,
    ) -> Result<(), Arc<Engine<ServeLoopDriver>>> {
        self.engine.set(engine)
    }
}

impl Default for EngineInbox {
    fn default() -> Self {
        Self {
            engine: OnceLock::new(),
        }
    }
}

impl InboxWriter for EngineInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        let engine = self
            .engine
            .get()
            .expect("engine installed before the first admit")
            .clone();
        Box::pin(async move {
            let outcome = engine
                .admit(session, payload, idempotency_key)
                .await
                .map_err(|e| InboxWriteError::Write(e.to_string()))?;
            Ok(AdmitReceipt {
                deduplicated: outcome.deduplicated,
            })
        })
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.engine
            .get()
            .expect("engine installed before the first wake")
            .wake(session)
    }
}

// ---------------------------------------------------------------------------
// The per-hold StreamPort over DrainCtx (the C25/C26 shape)
// ---------------------------------------------------------------------------

/// Assembly's [`StreamPort`]: one per execution hold, built from the
/// hold's [`DrainCtx`]. Writes carry the hold's epoch, so a stale
/// port's writes are fenced out by the store (NoSplitBrainWrite).
struct DrainCtxPort(DrainCtx);

impl StreamPort for DrainCtxPort {
    fn session(&self) -> SessionId {
        self.0.session()
    }

    fn emit(
        &self,
        kind: EventKind,
        bytes: serde_json::Value,
    ) -> BoxFuture<'static, Result<(), StreamPortError>> {
        let ctx = self.0.clone();
        Box::pin(async move {
            ctx.append(kind, bytes)
                .await
                .map(|_| ())
                .map_err(|e| StreamPortError(e.to_string()))
        })
    }

    fn read_stream(&self) -> BoxFuture<'static, Result<Vec<Event>, StreamPortError>> {
        let ctx = self.0.clone();
        Box::pin(async move {
            ctx.events()
                .await
                .map_err(|e| StreamPortError(e.to_string()))
        })
    }
}

/// The per-hold [`StreamPort`] factory (public: the plugins build the
/// Router/Worker decorators with it).
pub fn port_for_ctx() -> PortFor<DrainCtx> {
    Arc::new(|ctx: &DrainCtx| Arc::new(DrainCtxPort(ctx.clone())) as Arc<dyn StreamPort>)
}

// ---------------------------------------------------------------------------
// The RecallPort over the store's public scan (the C34 read surface)
// ---------------------------------------------------------------------------

/// Assembly's [`kian_superagent::RecallPort`]: cross-session reads
/// served over the store's existing public scan API — reads only, no
/// execution surface, no writes. Holds the disposable spill index (a
/// location cache rebuilt at startup; the spilled raws themselves live
/// only in the streams — constraint 1).
pub struct StoreRecall {
    store: EventStore,
    spill: Mutex<kian_superagent::SpillIndex>,
}

impl StoreRecall {
    /// A fresh port over `store` with an empty spill index (the
    /// startup rebuild fills it).
    pub fn new(store: EventStore) -> Arc<Self> {
        Arc::new(Self {
            store,
            spill: Mutex::new(kian_superagent::SpillIndex::new()),
        })
    }

    /// Rebuilds the spill index at startup: scan the Router stream,
    /// then every worker stream it names (the [`worker_sessions`]
    /// derivation — the same universe `PassthroughRegistry::rebuild`
    /// scans), and fold every `input/spilled` event into the index.
    /// Idempotent and disposable: drop the index and run it again over
    /// the same log and the same map comes back.
    ///
    /// v1 boundary (C34): the scanned sessions are the bundle's whole
    /// channel-fed universe. `input/spilled` events are written only by
    /// the drain's consumption of a spill envelope row, and those rows
    /// are admitted only by the subscription service — to the Router
    /// or to spawned workers. A spill on a session unreachable from
    /// the Router stream therefore cannot exist today; if a future
    /// route widens admission, this scan list widens with it.
    ///
    /// kian-62: `chain` is the re-arm chain — ended frozen predecessors
    /// first, then the live resident session (see
    /// [`RouterProfile::re_arm`]); predecessor streams still carry
    /// spill envelopes the index must serve.
    pub async fn rebuild_spill_index(&self, chain: &[SessionId]) -> Result<(), String> {
        let mut events = Vec::new();
        for session in chain {
            let part = self.store.rebuild(*session).await.map_err(|e| {
                format!(
                    "spill index rebuild (resident {session}): {e}",
                    session = session.0
                )
            })?;
            events.extend(part);
        }
        for worker in worker_sessions(&events) {
            let worker_events = self
                .store
                .rebuild(worker)
                .await
                .map_err(|e| format!("spill index rebuild (worker): {e}"))?;
            events.extend(worker_events);
        }
        let index = kian_superagent::SpillIndex::rebuild(&events);
        tracing::info!(
            spills = index.len(),
            "superagent: spill index rebuilt from streams"
        );
        *self.spill.lock().expect("spill index lock") = index;
        Ok(())
    }
}

impl kian_superagent::RecallPort for StoreRecall {
    fn read_events(
        &self,
        session: SessionId,
        since_seq: u64,
        until_seq: u64,
    ) -> BoxFuture<'static, Result<Vec<Event>, kian_superagent::RecallPortError>> {
        let store = self.store.clone();
        Box::pin(async move {
            if since_seq >= until_seq {
                // An empty window is an honest empty result, not an
                // error — the port refuses nothing read-only.
                return Ok(Vec::new());
            }
            store
                .scan(session, since_seq..until_seq)
                .await
                .map_err(|e| kian_superagent::RecallPortError::Read(e.to_string()))
        })
    }

    fn list_threads(
        &self,
        parent: SessionId,
    ) -> BoxFuture<'static, Result<Vec<(String, SessionId)>, kian_superagent::RecallPortError>>
    {
        let store = self.store.clone();
        Box::pin(async move {
            let events = store
                .rebuild(parent)
                .await
                .map_err(|e| kian_superagent::RecallPortError::Read(e.to_string()))?;
            Ok(kian_superagent::thread_bindings(&events))
        })
    }

    fn spill_location(
        &self,
        event_id: &str,
    ) -> BoxFuture<'static, Result<Option<(SessionId, u64)>, kian_superagent::RecallPortError>>
    {
        let located = self
            .spill
            .lock()
            .expect("spill index lock")
            .lookup(event_id);
        Box::pin(async move { Ok(located) })
    }
}

// ---------------------------------------------------------------------------
// The assembled driver
// ---------------------------------------------------------------------------

/// The driver every assembled engine runs: the Phase-1 plain loop, or
/// the super-agent three-way router (Phase-1 behavior with the bundle
/// mounted). Both variants share `Ctx = DrainCtx`.
pub enum ServeLoopDriver {
    /// Phase-1: one loop, the default agent (unchanged behavior).
    Plain(Arc<ReActLoop>),
    /// Super-agent: resident Router / spawned workers / plain sessions.
    SuperAgent(Arc<SuperAgentDriver>),
}

// ---------------------------------------------------------------------------
// C54: the S6 dsh bridge mount — the DshCtx port over the real DrainCtx
// ---------------------------------------------------------------------------

/// Assembly's [`kian_dsh::DshCtx`] over the engine's `DrainCtx`: the
/// fenced append path, the safe-boundary input take, and the read-only
/// stream — the bridge reaches nothing else (no store, no end write).
/// A newtype because the orphan rule puts the port impl in the crate
/// that owns neither side's foreign halves — this one owns the
/// wrapper.
pub struct DrainDshCtx(DrainCtx);

impl kian_dsh::DshCtx for DrainDshCtx {
    fn session(&self) -> SessionId {
        self.0.session()
    }

    fn epoch(&self) -> kian_core::model::Epoch {
        self.0.epoch()
    }

    fn in_turn(&self) -> bool {
        self.0.in_turn()
    }

    fn events(&self) -> BoxFuture<'_, Result<Vec<Event>, kian_dsh::DshCtxError>> {
        let fut = self.0.events();
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_dsh::DshCtxError::Failure(e.to_string()))
        })
    }

    fn append(
        &self,
        kind: EventKind,
        bytes: serde_json::Value,
    ) -> BoxFuture<'_, Result<Event, kian_dsh::DshCtxError>> {
        let fut = self.0.append(kind, bytes);
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_dsh::DshCtxError::Failure(e.to_string()))
        })
    }

    fn begin_turn(
        &self,
        bytes: serde_json::Value,
    ) -> BoxFuture<'_, Result<Event, kian_dsh::DshCtxError>> {
        let fut = self.0.begin_turn_with(bytes);
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_dsh::DshCtxError::Failure(e.to_string()))
        })
    }

    fn end_turn(
        &self,
        bytes: serde_json::Value,
    ) -> BoxFuture<'_, Result<Event, kian_dsh::DshCtxError>> {
        let fut = self.0.end_turn_with(bytes);
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_dsh::DshCtxError::Failure(e.to_string()))
        })
    }

    fn take_eligible_inputs(
        &self,
    ) -> BoxFuture<'_, Result<Vec<kian_core::model::InputRow>, kian_dsh::DshCtxError>> {
        let fut = self.0.take_eligible_inputs();
        Box::pin(async move {
            fut.await.map_err(|e| match e {
                kian_orchestrator::EngineError::NotAtSafeBoundary { .. } => {
                    kian_dsh::DshCtxError::NotAtSafeBoundary
                }
                other => kian_dsh::DshCtxError::Failure(other.to_string()),
            })
        })
    }

    fn has_eligible_inputs(&self) -> BoxFuture<'_, Result<bool, kian_dsh::DshCtxError>> {
        let fut = self.0.has_eligible_inputs();
        Box::pin(async move {
            fut.await
                .map_err(|e| kian_dsh::DshCtxError::Failure(e.to_string()))
        })
    }

    // interrupt_requested: the engine's interrupt flag is not visible
    // from DrainCtx; v1 relies on the engine honoring it at outcome
    // boundaries (the mounted bridge never forwards an `interrupt`
    // control frame mid-episode). The contract frame itself is
    // exercised by the kian-dsh driver suite.
}

/// The worker template's inner loop, config-gated (C54): the built-in
/// ReAct loop (default), or the S6 dsh bridge when
/// `superagent.harness.dsh` is on. The decorator (`WorkerLoopDriver`)
/// and the plain/router sides are unchanged either way.
pub enum WorkerInnerDriver {
    React(Arc<ReActLoop>),
    Dsh(Arc<kian_dsh::DshDriver>),
}

impl LoopDriver for WorkerInnerDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        match self {
            Self::React(loop_) => loop_.drive(ctx),
            Self::Dsh(driver) => {
                driver.drive(Arc::new(DrainDshCtx(ctx)) as Arc<dyn kian_dsh::DshCtx>)
            }
        }
    }
}

impl LoopDriver for ServeLoopDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        match self {
            Self::Plain(loop_) => loop_.drive(ctx),
            Self::SuperAgent(driver) => driver.drive(ctx),
        }
    }
}

/// One engine, one driver, three routings:
///
/// - the configured Router session → [`RouterDriver`] (residency);
/// - a session with Thread lineage → [`WorkerLoopDriver`] (lineage,
///   evidence gate, completion report);
/// - anything else → the default agent's loop (Phase-1 semantics for
///   plain sessions).
///
/// Worker lineage is decided from the stream itself (constraint 1: no
/// resident routing state), so a restarted server classifies a
/// takeover hold identically.
pub struct SuperAgentDriver {
    router: SessionId,
    worker_version: u64,
    router_side: RouterDriver<ReActLoop>,
    worker_side: WorkerLoopDriver<crate::superagent::WorkerInnerDriver>,
    default_side: Arc<ReActLoop>,
    /// The resident watchdog session (C40), when configured: routed to
    /// its own driver (the same residency shape as the Router's) so
    /// each admitted tick is one judge turn.
    watchdog: Option<SessionId>,
    watchdog_side: Option<RouterDriver<ReActLoop>>,
}

impl SuperAgentDriver {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        router: SessionId,
        worker_version: u64,
        router_side: RouterDriver<ReActLoop>,
        worker_side: WorkerLoopDriver<crate::superagent::WorkerInnerDriver>,
        default_side: Arc<ReActLoop>,
    ) -> Self {
        Self {
            router,
            worker_version,
            router_side,
            worker_side,
            default_side,
            watchdog: None,
            watchdog_side: None,
        }
    }

    /// Attaches the resident watchdog session and its driver (C40).
    /// Additive: unconfigured (the default) keeps the pre-C40 routing
    /// exactly.
    #[must_use]
    pub fn with_watchdog(mut self, session: SessionId, side: RouterDriver<ReActLoop>) -> Self {
        self.watchdog = Some(session);
        self.watchdog_side = Some(side);
        self
    }
}

impl LoopDriver for SuperAgentDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        if ctx.session() == self.router {
            return self.router_side.drive(ctx);
        }
        if let (Some(watchdog), Some(side)) = (self.watchdog, &self.watchdog_side)
            && ctx.session() == watchdog
        {
            return side.drive(ctx);
        }
        // Classify by stream content before the first provider turn:
        // worker streams carry the delegation wrapper (or already carry
        // thread/started); plain sessions see the default agent.
        let worker = self.worker_side.clone();
        let default = Arc::clone(&self.default_side);
        let router = self.router;
        let worker_version = self.worker_version;
        let session = ctx.session();
        futures::stream::once(async move {
            let events = ctx.events().await.unwrap_or_default();
            match lineage_plan(&events, session, router, worker_version) {
                LineagePlan::PassThrough => default.drive(ctx).boxed(),
                LineagePlan::Emit { .. } | LineagePlan::Resume { .. } => worker.drive(ctx).boxed(),
            }
        })
        .flatten()
        .boxed()
    }
}

// ---------------------------------------------------------------------------
// Tool composition
// ---------------------------------------------------------------------------

/// A [`ToolProvider`] over an ordered list of providers: schemas are
/// the union with **first-wins** deduplication by tool name, and
/// invocations route to the first provider owning the name.
///
/// Assembly uses the ordering `[remote-exec…, local-tools]` for worker
/// agents, so the remote `exec` tool (same schema as the local built-in,
/// C20) shadows the local one when endpoints are configured — one
/// uniform `exec` surface for the model either way.
pub struct CombinedTools {
    providers: Vec<Arc<dyn ToolProvider>>,
}

impl CombinedTools {
    pub fn new(providers: Vec<Arc<dyn ToolProvider>>) -> Self {
        Self { providers }
    }
}

impl ToolProvider for CombinedTools {
    fn schemas(&self) -> Vec<ToolSchema> {
        let mut seen = BTreeSet::new();
        let mut out = Vec::new();
        for provider in &self.providers {
            for schema in provider.schemas() {
                if seen.insert(schema.name.clone()) {
                    out.push(schema);
                }
            }
        }
        out
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        // First provider whose schema set contains the call's name.
        for provider in &self.providers {
            if provider
                .schemas()
                .iter()
                .any(|schema| schema.name == call.name)
            {
                return provider.invoke(call);
            }
        }
        Box::pin(async move {
            Err(ToolError::InvalidCall(format!(
                "no provider serves tool `{}`",
                call.name
            )))
        })
    }
}

// ---------------------------------------------------------------------------
// Subscription metrics (the consumer loop is assembly-owned so the
// outcomes are observable)
// ---------------------------------------------------------------------------

/// Counters for the subscription engine (routed/dropped/deduplicated).
/// Plain atomics — cheap to read from `/metrics`.
#[derive(Debug, Default)]
pub struct SubscriptionMetrics {
    routed: std::sync::atomic::AtomicU64,
    routed_deduplicated: std::sync::atomic::AtomicU64,
    dropped_no_rule: std::sync::atomic::AtomicU64,
    /// C36 admission-class `ignore` drops (the DroppedNoRule shape).
    dropped_ignored: std::sync::atomic::AtomicU64,
    /// C36 `explicit-only` drops without an explicit-request signal.
    dropped_not_explicit: std::sync::atomic::AtomicU64,
    failures: std::sync::atomic::AtomicU64,
    /// C57: per-code intake-reason counters — one tick per decision,
    /// keyed by the closed [`kian_superagent::IntakeReason`] set's
    /// stable code. Every code is materialized in the snapshot, so
    /// the series shape is fixed (the C41 histogram discipline).
    by_reason: ReasonCounters,
}

/// The closed per-code counter set (all 7 codes, zero-initialized).
#[derive(Debug)]
struct ReasonCounters {
    counts: std::collections::BTreeMap<&'static str, std::sync::atomic::AtomicU64>,
}

impl ReasonCounters {
    fn new() -> Self {
        Self {
            counts: kian_superagent::IntakeReason::ALL
                .iter()
                .map(|reason| (reason.code(), std::sync::atomic::AtomicU64::new(0)))
                .collect(),
        }
    }

    fn bump(&self, reason: kian_superagent::IntakeReason) {
        if let Some(counter) = self.counts.get(reason.code()) {
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> serde_json::Value {
        let mut object = serde_json::Map::new();
        for (code, counter) in &self.counts {
            object.insert(
                (*code).to_string(),
                serde_json::json!(counter.load(std::sync::atomic::Ordering::Relaxed)),
            );
        }
        serde_json::Value::Object(object)
    }
}

impl Default for ReasonCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionMetrics {
    fn bump(counter: &std::sync::atomic::AtomicU64) {
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Records one handled event's outcome: the legacy shape counters
    /// plus exactly one per-code tick per decision (C57 — the codes
    /// come from the same mapping the audit lines stamp, so the two
    /// surfaces cannot drift).
    pub fn record_outcome(
        &self,
        outcome: &kian_superagent::RoutingOutcome,
        router: kian_core::model::SessionId,
    ) {
        match outcome {
            kian_superagent::RoutingOutcome::Routed { deduplicated, .. } => {
                Self::bump(&self.routed);
                if *deduplicated {
                    Self::bump(&self.routed_deduplicated);
                }
            }
            // C38 fan-out: one routed counter tick per admitted
            // subscriber, dedup per row.
            kian_superagent::RoutingOutcome::FannedOut { admitted } => {
                for admission in admitted {
                    Self::bump(&self.routed);
                    if admission.deduplicated {
                        Self::bump(&self.routed_deduplicated);
                    }
                }
            }
            kian_superagent::RoutingOutcome::DroppedNoRule => {
                Self::bump(&self.dropped_no_rule);
            }
            // C36 fast-path drops: audit line by the service, counter
            // here (the DroppedNoRule shape).
            kian_superagent::RoutingOutcome::DroppedIgnored => {
                Self::bump(&self.dropped_ignored);
            }
            kian_superagent::RoutingOutcome::DroppedNotExplicit => {
                Self::bump(&self.dropped_not_explicit);
            }
        }
        for reason in kian_superagent::outcome_reasons(outcome, router) {
            self.by_reason.bump(reason);
        }
    }

    pub fn snapshot(&self) -> serde_json::Value {
        use std::sync::atomic::Ordering;
        serde_json::json!({
            "routed": self.routed.load(Ordering::Relaxed),
            "routed_deduplicated": self.routed_deduplicated.load(Ordering::Relaxed),
            "dropped_no_rule": self.dropped_no_rule.load(Ordering::Relaxed),
            "dropped_ignored": self.dropped_ignored.load(Ordering::Relaxed),
            "dropped_not_explicit": self.dropped_not_explicit.load(Ordering::Relaxed),
            "failures": self.failures.load(Ordering::Relaxed),
            "by_reason": self.by_reason.snapshot(),
        })
    }
}

/// Spawns one consumer per adapter, feeding the subscription service
/// and recording outcomes (the assembly-owned variant of
/// `SubscriptionService::spawn` — same semantics, observable).
pub fn spawn_subscription_consumers(
    service: Arc<SubscriptionService>,
    adapters: Vec<Arc<dyn ChannelAdapter>>,
    metrics: Arc<SubscriptionMetrics>,
) -> Vec<tokio::task::JoinHandle<()>> {
    adapters
        .into_iter()
        .map(|adapter| {
            let service = Arc::clone(&service);
            let metrics = Arc::clone(&metrics);
            let mut stream = adapter.subscribe();
            tokio::spawn(async move {
                while let Some(event) = stream.next().await {
                    match service.handle_event(event).await {
                        Ok(outcome) => {
                            // Legacy shape counters plus the C57
                            // per-code ticks (once per decision, the
                            // audit lines' own mapping).
                            metrics.record_outcome(&outcome, service.router_session());
                        }
                        Err(_) => {
                            SubscriptionMetrics::bump(&metrics.failures);
                        }
                    }
                }
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Watchdog wiring: health edges → admitted inputs for affected sessions
// ---------------------------------------------------------------------------

/// The endpoint-health input payload admitted to a session holding
/// queued commands on the endpoint that went offline. A normal message
/// payload whose content embeds a `kian.endpoint_health` marker — the
/// same content-embedding discipline as channel provenance (the frozen
/// C4 payload contract is untouched).
pub fn endpoint_health_payload(
    endpoint_id: &str,
    status: &str,
    detail: Option<&str>,
    queued: usize,
    ts_ms: u64,
) -> serde_json::Value {
    serde_json::json!({
        "kian": {
            "type": "endpoint_health",
            "endpoint": endpoint_id,
            "status": status,
            "detail": detail,
            "queued_commands": queued,
            "ts_ms": ts_ms,
        },
        "text": format!(
            "execution endpoint `{endpoint_id}` is {status}; {queued} queued command(s) affected"
        ),
    })
}

/// Sessions whose streams carry a `tool/call` with one of the given
/// call ids — the sessions holding the queued commands. Derived purely
/// from the event log (constraint 1) via the store's pool; the `kind`
/// column and the envelope's call id do the matching.
async fn sessions_for_tool_calls(
    store: &EventStore,
    tool_call_refs: &BTreeSet<String>,
) -> Result<BTreeSet<SessionId>, String> {
    let mut sessions = BTreeSet::new();
    for call_id in tool_call_refs {
        // C51: same lookup, per dialect (sqlite json_extract vs pg
        // jsonb path) — derived purely from the event log either way.
        let rows: Vec<(String,)> = match store {
            EventStore::Sqlite(pool) => sqlx::query_as(
                "SELECT DISTINCT session FROM events \
                 WHERE kind = 'tool/call' \
                   AND json_extract(envelope, '$.bytes.content.id') = ?",
            )
            .bind(call_id)
            .fetch_all(pool)
            .await
            .map_err(|e| format!("endpoint-health session lookup: {e}"))?,
            EventStore::Postgres(pool) => sqlx::query_as(
                "SELECT DISTINCT session FROM events \
                 WHERE kind = 'tool/call' \
                   AND envelope::jsonb -> 'bytes' -> 'content' ->> 'id' = $1::text",
            )
            .bind(call_id)
            .fetch_all(pool)
            .await
            .map_err(|e| format!("endpoint-health session lookup: {e}"))?,
        };
        for (session,) in rows {
            if let Ok(uuid) = uuid::Uuid::parse_str(&session) {
                sessions.insert(SessionId(uuid));
            }
        }
    }
    Ok(sessions)
}

/// Spawns the watchdog bridge: subscribes to the [`WatchdogSource`]
/// health stream to learn which endpoints are (still) offline, then
/// **periodically re-examines** every offline endpoint for sessions
/// holding queued commands — one durable health input row per affected
/// session, admitted and woken.
///
/// Why the tick beside the edges: an endpoint that is offline *before*
/// a command is enqueued (registered-but-stale at startup, or the
/// offline edge arriving between polls) would otherwise never be
/// re-examined — the edge fired once and the queue row landed after
/// it. The re-examination is idempotent per offline episode: the admit
/// key embeds the episode's `ts_ms`, so repeated sweeps deduplicate at
/// the inbox row. Online edges drop the endpoint from the sweep set
/// (reconnects re-drive the queue on their own, C20 semantics).
pub fn spawn_watchdog_bridge(
    watchdog: Arc<HeartbeatWatchdog>,
    exec_store: ExecStore,
    event_store: EventStore,
    inbox: Arc<dyn InboxWriter>,
) -> tokio::task::JoinHandle<()> {
    use kian_core::seams::EndpointHealth;
    use std::collections::BTreeMap;
    tokio::spawn(async move {
        let mut health = watchdog.health();
        // endpoint → (offline since ts_ms, detail) — the live sweep set.
        let mut offline: BTreeMap<String, (u64, Option<String>)> = BTreeMap::new();
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                event = health.next() => {
                    let Some(event) = event else { break };
                    match event.status {
                        EndpointHealth::Offline => {
                            tracing::info!(
                                endpoint = %event.endpoint_id,
                                "watchdog: endpoint offline"
                            );
                            offline.insert(
                                event.endpoint_id.clone(),
                                (event.ts_ms, event.detail.clone()),
                            );
                        }
                        EndpointHealth::Online => {
                            tracing::info!(
                                endpoint = %event.endpoint_id,
                                "watchdog: endpoint back online; queue re-drives itself"
                            );
                            offline.remove(&event.endpoint_id);
                        }
                    }
                }
                _ = ticker.tick() => {
                    for (endpoint, (ts_ms, detail)) in offline.clone() {
                        sweep_offline_endpoint(
                            &exec_store,
                            &event_store,
                            inbox.as_ref(),
                            &endpoint,
                            ts_ms,
                            detail.as_deref(),
                        )
                        .await;
                    }
                }
            }
        }
    })
}

/// One idempotent sweep of a currently-offline endpoint: sessions
/// holding queued commands on it get one durable health input row.
async fn sweep_offline_endpoint(
    exec_store: &ExecStore,
    event_store: &EventStore,
    inbox: &dyn InboxWriter,
    endpoint_id: &str,
    offline_since_ms: u64,
    detail: Option<&str>,
) {
    let queued = match exec_store.drivable(endpoint_id).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(error = %e, endpoint = %endpoint_id, "watchdog bridge: cannot read queued commands");
            return;
        }
    };
    let refs: BTreeSet<String> = queued
        .iter()
        .filter_map(|row| row.tool_call_ref.clone())
        .collect();
    if refs.is_empty() {
        return;
    }
    let sessions = match sessions_for_tool_calls(event_store, &refs).await {
        Ok(sessions) => sessions,
        Err(e) => {
            tracing::warn!(error = %e, "watchdog bridge: session lookup failed");
            return;
        }
    };
    for session in sessions {
        let payload =
            endpoint_health_payload(endpoint_id, "offline", detail, refs.len(), offline_since_ms);
        let key = format!("endpoint-health:{endpoint_id}:offline:{offline_since_ms}");
        match inbox.admit(session, payload, Some(key)).await {
            Ok(receipt) => {
                inbox.wake(session);
                if !receipt.deduplicated {
                    tracing::info!(
                        session = %session.0,
                        endpoint = %endpoint_id,
                        queued = refs.len(),
                        "watchdog bridge: health input admitted"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    session = %session.0,
                    "watchdog bridge: health input admit failed (wake skipped)"
                );
            }
        }
    }
}

/// Registers the configured endpoints in the durable registry so a
/// never-connected endpoint is observable (registered-but-stale is an
/// actionable offline edge — C21 semantics).
async fn seed_endpoint_registry(
    exec_store: &ExecStore,
    endpoints: &[EndpointConfig],
) -> Result<(), String> {
    for ep in endpoints {
        exec_store
            .upsert_endpoint(&ep.id, &ep.addr, 0, "offline")
            .await
            .map_err(|e| format!("seed endpoint registry `{}`: {e}", ep.id))?;
    }
    Ok(())
}

/// Reads the registry back (used by assembly logging/tests).
pub async fn endpoint_registry(exec_store: &ExecStore) -> Result<Vec<EndpointRegistryRow>, String> {
    exec_store
        .endpoints()
        .await
        .map_err(|e| format!("endpoint registry read: {e}"))
}

// ---------------------------------------------------------------------------
// The assembled runtime
// ---------------------------------------------------------------------------

/// The running super-agent services the [`crate::server::Server`]
/// holds: everything with a background task or a metrics surface. The
/// server's `ControlPlane`/`OpsPlane` impls reach the bundle only
/// through this handle (mock-channel injection needs the channel map;
/// `/metrics` needs the counters).
pub struct SuperAgentRuntime {
    /// The configured resident Router session.
    pub router: SessionId,
    /// Channel name → mock adapter (the S4 injection surface; v1 only
    /// mock channels exist).
    pub channels: BTreeMap<String, Arc<MockChannel>>,
    /// The worker's evidence gate (shared counters = the verdict
    /// metrics surface).
    pub worker_gate: EvidenceGateConfig,
    /// Subscription outcome counters.
    pub subscription_metrics: Arc<SubscriptionMetrics>,
    /// Router judge-turn batching metrics (C41: batch size, wall
    /// duration, amortized per-decision latency — recorded by
    /// `RouterDriver`, read-only here).
    pub router_metrics: Arc<RouterMetrics>,
    /// The heartbeat watchdog (when endpoints are configured).
    pub watchdog: Option<Arc<HeartbeatWatchdog>>,
    /// The remote-exec services (when endpoints are configured).
    pub remote: Option<Arc<crate::plugins::RemoteExecServices>>,
    /// The completion bridge's counters (C37; present when the async
    /// pair is mounted).
    pub async_metrics: Option<Arc<crate::async_bridge::AsyncBridgeMetrics>>,
    /// The cross-session read port (C34): `read_events` /
    /// `list_threads` / `spill_location` over the store's public scan,
    /// with the startup-rebuilt spill index. Read-only by
    /// construction; the recall toolpack (C35) mounts on it.
    pub recall: Arc<StoreRecall>,
    /// GitHub channels by configured name (C48/C49): the webhook-fed
    /// adapters (at most one — the config validation enforces it).
    /// The `WebhookPlane` hand-off in `server.rs` reads this map.
    pub github_channels: BTreeMap<String, Arc<kian_superagent::GitHubChannel>>,
    /// Consumer + bridge tasks; kept alive for the server's lifetime.
    pub tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl SuperAgentRuntime {
    /// The mock channel registered under `name`.
    pub fn mock_channel(&self, name: &str) -> Option<Arc<MockChannel>> {
        self.channels.get(name).cloned()
    }

    /// The `/metrics` superagent fragment: evidence-verdict,
    /// subscription, channel-policy (a pure projection of the Router
    /// stream — workers never deliver; policies are Router-side),
    /// router batching (C41: the §7.2 series the resident Router's
    /// driver records) and watchdog counters.
    pub async fn metrics_report(&self, store: &EventStore) -> serde_json::Value {
        let verdicts = self.worker_gate.counters.snapshot();
        let subscriptions = self.subscription_metrics.snapshot();
        let router_batching = self.router_metrics.snapshot();

        // Channel-policy counts derive from the Router's committed
        // stream: `channel/delivered` events carry policy/words/truncated.
        let mut delivered = 0u64;
        let mut im = 0u64;
        let mut structured = 0u64;
        let mut truncated = 0u64;
        if let Ok(events) = store.rebuild(self.router).await {
            for event in &events {
                if event.kind != EventKind::ChannelDelivered {
                    continue;
                }
                delivered += 1;
                match event.bytes.get("policy").and_then(|p| p.as_str()) {
                    Some("im") => im += 1,
                    Some("structured") => structured += 1,
                    _ => {}
                }
                if event.bytes.get("truncated") == Some(&serde_json::json!(true)) {
                    truncated += 1;
                }
            }
        }

        let watchdog = self.watchdog.as_ref().map(|watchdog| {
            let metrics = watchdog.metrics();
            serde_json::json!({
                "offline_transitions": metrics.offline_transitions(),
                "online_transitions": metrics.online_transitions(),
                "mean_detection_latency_ms": metrics.mean_detection_latency_ms(),
            })
        });

        serde_json::json!({
            "router_session": self.router.0,
            "evidence_verdicts": {
                "passed": verdicts.passed,
                "failed": verdicts.failed,
            },
            "subscriptions": subscriptions,
            "router_batching": router_batching,
            "channel_policy": {
                "delivered": delivered,
                "im": im,
                "structured": structured,
                "truncated": truncated,
            },
            "watchdog": watchdog,
            "async_bridge": self.async_metrics.as_ref().map(|m| m.snapshot()),
        })
    }
}

/// Everything the assembled server needs from the activated
/// `superagent` plugin before the engine exists.
pub struct SuperAgentServices {
    /// The assembled three-way driver.
    pub driver: Arc<SuperAgentDriver>,
    /// The late-bound engine slot (server installs the engine).
    pub inbox: Arc<EngineInbox>,
    /// The mounted router inbox (residency signal; also the spawner's
    /// and the worker wrapper's port).
    pub router_inbox: Arc<RouterInbox>,
    /// The resident Router profile (bootstrap is called post-rebuild).
    pub profile: RouterProfile,
    /// The subscription service (registry installed post-rebuild).
    pub subscriptions: Arc<SubscriptionService>,
    /// Mock channels by configured name (injection surface).
    pub mock_channels: BTreeMap<String, Arc<MockChannel>>,
    /// Lark channels by configured name (C44) — the real Feishu
    /// adapters, registered alongside the mocks.
    pub lark_channels: BTreeMap<String, Arc<kian_superagent::LarkChannel>>,
    /// GitHub channels by configured name (C48/C49) — the webhook-fed
    /// adapters. The C49 receiver plane (server.rs) hands verified
    /// deliveries to the single channel through this map.
    pub github_channels: BTreeMap<String, Arc<kian_superagent::GitHubChannel>>,
    /// Adapters in config order (consumer spawn order).
    pub adapters: Vec<Arc<dyn ChannelAdapter>>,
    /// The worker's evidence gate config (shared counters).
    pub worker_gate: EvidenceGateConfig,
    /// Subscription outcome counters.
    pub subscription_metrics: Arc<SubscriptionMetrics>,
    /// Router judge-turn batching metrics (C41) — the handle the
    /// driver records into and `/metrics` snapshots.
    pub router_metrics: Arc<RouterMetrics>,
    /// Pre-opened exec services when endpoints are configured.
    pub remote: Option<Arc<crate::plugins::RemoteExecServices>>,
    /// The async-jobs engine (C37), present when `superagent.async_tools`
    /// is on. The startup sweep re-drives and redelivers after the
    /// inbox rebuild.
    pub async_jobs: Option<Arc<kian_exec::AsyncJobs>>,
    /// The completion bridge's counters (C37), for `/metrics`.
    pub async_metrics: Option<Arc<crate::async_bridge::AsyncBridgeMetrics>>,
    /// The cross-session read port (C34): reads over the store's
    /// public scan plus the disposable spill index, rebuilt by
    /// `start_services` after the inbox rebuild.
    pub recall: Arc<StoreRecall>,
    /// The timer input sources (C40): their run tasks spawn in
    /// `start_services` after the subscription consumers (a timer with
    /// no consumer would emit ticks into the void).
    pub timers: Vec<Arc<kian_superagent::TimerChannel>>,
    /// The resident watchdog profile (C40), when configured: bootstrapped
    /// (idempotent prompt admit + wake) in `start_services`.
    pub watchdog: Option<kian_superagent::WatchdogProfile>,
}

/// Post-`startup_rebuild` services start (the card's load-bearing
/// order): registry rebuild → subscription consumers → watchdog +
/// bridge → Router bootstrap. Called by [`crate::server::Server::from_config`]
/// **after** the inbox rebuild returned; every step lands in `order`.
pub async fn start_services(
    services: Arc<SuperAgentServices>,
    engine: Arc<Engine<ServeLoopDriver>>,
    store: EventStore,
    order: StartupOrder,
) -> Result<SuperAgentRuntime, String> {
    let router = services.profile.session;

    // 1. Rebuild the passthrough registry from event streams (the
    //    NoLostWork analog for subscriptions; nothing authoritative
    //    survives only in memory). kian-62: the scan covers the whole
    //    re-arm chain — ended predecessors still carry thread/started
    //    lineage and worker references the registry must serve.
    let resident_chain: Vec<SessionId> = services
        .profile
        .re_arm
        .iter()
        .map(|ended| ended.session)
        .chain(std::iter::once(router))
        .collect();
    let mut events = Vec::new();
    for session in &resident_chain {
        let part = store
            .rebuild(*session)
            .await
            .map_err(|e| format!("passthrough registry rebuild (resident {}): {e}", session.0))?;
        events.extend(part);
    }
    for worker in worker_sessions(&events) {
        let worker_events = store
            .rebuild(worker)
            .await
            .map_err(|e| format!("passthrough registry rebuild (worker): {e}"))?;
        events.extend(worker_events);
    }
    let registry = PassthroughRegistry::rebuild(&events);
    tracing::info!(
        topics = registry.len(),
        chain = resident_chain.len(),
        "superagent: passthrough registry rebuilt from streams"
    );
    services.subscriptions.install_registry(registry);

    // 1b. Rebuild the disposable spill index (C34) over the same
    //     scanned universe — a pure location projection, reads only.
    services
        .recall
        .rebuild_spill_index(&resident_chain)
        .await
        .map_err(|e| format!("spill index rebuild: {e}"))?;
    record(&order, "spill_index_rebuilt");

    // 2. Subscription consumers (AFTER the rebuild — no event is
    //    routed against an empty registry).
    let consumers = spawn_subscription_consumers(
        Arc::clone(&services.subscriptions),
        services.adapters.clone(),
        Arc::clone(&services.subscription_metrics),
    );
    record(&order, "subscriptions_started");

    // 2b. C40: the timers' run tasks spawn in step 4b below —
    //     strictly after the consumers exist (a tick emitted before
    //     any subscriber is a tick nobody hears).

    // 3. Watchdog + bridge (needs the engine-backed inbox).
    let mut watchdog = None;
    let mut bridge = Vec::new();
    if let Some(remote) = &services.remote {
        seed_endpoint_registry(remote.backend.store(), &remote.endpoints).await?;
        let offline_after = remote
            .endpoints
            .iter()
            .map(|ep| ep.offline_after_ms)
            .min()
            .unwrap_or(30_000);
        // Detection bound = offline_after + poll_interval; keep the
        // poll a fraction of the bound, capped at 1s.
        let poll = (offline_after / 4).clamp(50, 1_000);
        let config = WatchdogConfig {
            offline_after: Duration::from_millis(offline_after),
            poll_interval: Duration::from_millis(poll),
        };
        let clock: Arc<dyn ExecClock> = Arc::new(ExecSystemClock);
        let wd = HeartbeatWatchdog::start(remote.backend.store().clone(), config, clock);
        bridge.push(spawn_watchdog_bridge(
            Arc::clone(&wd),
            remote.backend.store().clone(),
            store.clone(),
            Arc::clone(&services.router_inbox) as Arc<dyn InboxWriter>,
        ));
        watchdog = Some(wd);
        record(&order, "watchdog_started");
    }

    // 3b. C37: the async bridge's startup sweep, strictly after the
    //     inbox rebuild (admitted completions must never race an empty
    //     wake registry): re-offer settled rows (a crash between settle
    //     and deliver loses nothing — the admit key dedupes), then
    //     re-drive pending/inflight jobs (observers re-attach through
    //     the C20 queue; one row, one dispatch, ever).
    if let Some(jobs) = &services.async_jobs {
        jobs.redeliver_settled().await;
        jobs.redrive_pending().await;
        record(&order, "async_jobs_redriven");
    }

    // 4. Bootstrap the resident Router (idempotent admit + wake; the
    //    first hold appends `session/created`).
    services
        .profile
        .bootstrap(services.router_inbox.as_ref())
        .await
        .map_err(|e| format!("router bootstrap: {e}"))?;
    engine.wake(router);
    record(&order, "router_bootstrapped");

    let mut tasks = consumers;

    // 4b. C40: bootstrap the resident watchdog session (idempotent
    //     prompt admit + wake; the first hold appends
    //     `session/created`), then start the timers' run tasks —
    //     after this point every emitted tick reaches a live consumer
    //     and a bootstrapped judge.
    if let Some(watchdog_profile) = &services.watchdog {
        watchdog_profile
            .bootstrap(services.router_inbox.as_ref())
            .await
            .map_err(|e| format!("watchdog bootstrap: {e}"))?;
        engine.wake(watchdog_profile.session);
        record(&order, "watchdog_bootstrapped");
    }
    for timer in &services.timers {
        let poll = (timer.config().interval_ms / 20).clamp(10, 1_000);
        tasks.push(timer.spawn(Duration::from_millis(poll)));
    }
    if !services.timers.is_empty() {
        record(&order, "timers_started");
    }

    tasks.extend(bridge);
    Ok(SuperAgentRuntime {
        router,
        channels: services.mock_channels.clone(),
        worker_gate: clone_gate(&services.worker_gate),
        subscription_metrics: Arc::clone(&services.subscription_metrics),
        router_metrics: Arc::clone(&services.router_metrics),
        watchdog,
        remote: services.remote.clone(),
        async_metrics: services.async_metrics.clone(),
        recall: Arc::clone(&services.recall),
        github_channels: services.github_channels.clone(),
        tasks,
    })
}

/// Clones the gate config while sharing its counters (the metrics
/// surface is the same handle the worker wrapper records into).
pub fn clone_gate(gate: &EvidenceGateConfig) -> EvidenceGateConfig {
    EvidenceGateConfig {
        enabled: gate.enabled,
        checks: gate.checks.clone(),
        max_retries: gate.max_retries,
        counters: Arc::clone(&gate.counters),
    }
}

// ---------------------------------------------------------------------------
// Unit tests (pure pieces only — the wiring is exercised by
// tests/superagent_serve.rs over the real engine)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn combined_tools_dedupe_first_wins_and_route_by_name() {
        use kian_core::seams::ToolSchema;
        use std::sync::atomic::{AtomicU64, Ordering};

        struct FixedTools {
            names: Vec<&'static str>,
            hits: Arc<AtomicU64>,
        }
        impl ToolProvider for FixedTools {
            fn schemas(&self) -> Vec<ToolSchema> {
                self.names
                    .iter()
                    .map(|n| ToolSchema {
                        name: (*n).into(),
                        description: String::new(),
                        parameters: serde_json::json!({"type": "object"}),
                    })
                    .collect()
            }
            fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
                self.hits.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    Ok(ToolResult {
                        call_id: call.id,
                        output: serde_json::json!({"from": call.name}),
                        is_error: false,
                    })
                })
            }
        }

        let first_hits = Arc::new(AtomicU64::new(0));
        let second_hits = Arc::new(AtomicU64::new(0));
        let first = Arc::new(FixedTools {
            names: vec!["exec", "only_first"],
            hits: Arc::clone(&first_hits),
        });
        let second = Arc::new(FixedTools {
            names: vec!["exec", "fs.read"],
            hits: Arc::clone(&second_hits),
        });
        let combined = CombinedTools::new(vec![
            Arc::clone(&first) as Arc<dyn ToolProvider>,
            Arc::clone(&second) as Arc<dyn ToolProvider>,
        ]);

        // Schema union: one `exec`, plus each side's unique names.
        let mut names: Vec<String> = combined.schemas().iter().map(|s| s.name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["exec", "fs.read", "only_first"]);

        // `exec` routes to the FIRST provider (remote shadows local).
        let result = combined
            .invoke(ToolCall {
                id: "c1".into(),
                name: "exec".into(),
                arguments: serde_json::json!({}),
            })
            .await
            .expect("exec routes");
        assert_eq!(first_hits.load(Ordering::SeqCst), 1);
        assert_eq!(second_hits.load(Ordering::SeqCst), 0);
        assert_eq!(result.output["from"], "exec");

        // A name neither serves is an honest error.
        let err = combined
            .invoke(ToolCall {
                id: "c2".into(),
                name: "nope".into(),
                arguments: serde_json::json!({}),
            })
            .await
            .expect_err("unknown tool");
        assert!(err.to_string().contains("no provider serves tool `nope`"));
    }

    #[test]
    fn endpoint_health_payload_embeds_the_marker() {
        let payload = endpoint_health_payload("ep1", "offline", Some("stale"), 2, 42);
        assert_eq!(payload["kian"]["type"], "endpoint_health");
        assert_eq!(payload["kian"]["endpoint"], "ep1");
        assert_eq!(payload["kian"]["status"], "offline");
        assert_eq!(payload["kian"]["queued_commands"], 2);
        assert!(payload["text"].as_str().unwrap().contains("ep1"));
    }

    #[tokio::test]
    async fn engine_inbox_requires_install_before_use() {
        use kian_superagent::InboxWriter as _;
        let inbox = EngineInbox::new();
        let session = SessionId::new();
        // Before install: the admit future panics on use (documented);
        // wake would too. Use catch_unwind to assert the contract
        // without poisoning the test thread's output.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            futures::executor::block_on(inbox.admit(session, serde_json::json!(1), None))
        }));
        assert!(result.is_err(), "admit before install must panic");
    }

    /// C36: the consumer loop counts admission-class drops in the
    /// `DroppedNoRule` shape — `ignore` bumps `dropped_ignored`,
    /// `explicit-only` without a signal bumps `dropped_not_explicit`,
    /// and nothing is routed.
    #[tokio::test]
    async fn admission_class_drops_bump_their_counters() {
        use kian_superagent::{
            AdmissionClass, AdmissionClassRule, AdmissionClassTable, Route, RuleMatcher,
            SubscriptionRule, SubscriptionRuleSet,
        };

        struct CountingInbox;
        impl InboxWriter for CountingInbox {
            fn admit(
                &self,
                _session: SessionId,
                _payload: serde_json::Value,
                _key: Option<String>,
            ) -> futures::future::BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>>
            {
                Box::pin(async {
                    Ok(AdmitReceipt {
                        deduplicated: false,
                    })
                })
            }
            fn wake(&self, _session: SessionId) -> WakeOutcome {
                WakeOutcome::Folded
            }
        }

        let channel = Arc::new(kian_superagent::MockChannel::new("mock-im"));
        let rules = SubscriptionRuleSet::new(vec![SubscriptionRule {
            channel: "mock-im".into(),
            matcher: RuleMatcher::Always,
            route: Route::Router,
        }]);
        let classes = AdmissionClassTable::new(vec![AdmissionClassRule {
            channel: "mock-im".into(),
            topic: None,
            sender: None,
            class: AdmissionClass::ExplicitOnly,
        }]);
        let service = Arc::new(kian_superagent::SubscriptionService::with_classes(
            SessionId::new(),
            rules,
            classes,
            Arc::new(CountingInbox) as Arc<dyn InboxWriter>,
        ));
        let metrics = Arc::new(SubscriptionMetrics::default());
        let tasks = super::spawn_subscription_consumers(
            Arc::clone(&service),
            vec![Arc::clone(&channel) as Arc<dyn ChannelAdapter>],
            Arc::clone(&metrics),
        );

        // A non-mention event drops (explicit-only without a signal);
        // a mention is admitted.
        channel.inject(None, serde_json::json!({"text": "just chatting"}));
        channel.inject(None, serde_json::json!({"text": "hey @kian deploy"}));
        for _ in 0..200 {
            let snap = metrics.snapshot();
            if snap["dropped_not_explicit"] == 1 && snap["routed"] == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let snap = metrics.snapshot();
        assert_eq!(snap["dropped_not_explicit"], 1);
        assert_eq!(snap["routed"], 1);
        assert_eq!(snap["dropped_ignored"], 0);

        // Now an Ignore channel: the ignore counter bumps instead.
        let ignored_channel = Arc::new(kian_superagent::MockChannel::new("mock-im"));
        let ignored_service = Arc::new(kian_superagent::SubscriptionService::with_classes(
            SessionId::new(),
            SubscriptionRuleSet::new(vec![]),
            kian_superagent::AdmissionClassTable::new(vec![kian_superagent::AdmissionClassRule {
                channel: "mock-im".into(),
                topic: None,
                sender: None,
                class: kian_superagent::AdmissionClass::Ignore,
            }]),
            Arc::new(CountingInbox) as Arc<dyn InboxWriter>,
        ));
        let ignored_metrics = Arc::new(SubscriptionMetrics::default());
        let ignored_tasks = super::spawn_subscription_consumers(
            Arc::clone(&ignored_service),
            vec![Arc::clone(&ignored_channel) as Arc<dyn ChannelAdapter>],
            Arc::clone(&ignored_metrics),
        );
        ignored_channel.inject(None, serde_json::json!({"text": "noise"}));
        for _ in 0..200 {
            if ignored_metrics.snapshot()["dropped_ignored"] == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(ignored_metrics.snapshot()["dropped_ignored"], 1);
        assert_eq!(ignored_metrics.snapshot()["routed"], 0);
        for task in tasks.into_iter().chain(ignored_tasks) {
            task.abort();
        }
    }

    /// C57: per-code counters increment exactly once per decision,
    /// keyed by the closed `IntakeReason` set, and the snapshot
    /// materializes every code (stable series shape). The legacy
    /// shape counters keep their semantics beside them.
    #[test]
    fn by_reason_counters_tick_once_per_decision() {
        use kian_superagent::FanoutAdmission;

        let router = SessionId::new();
        let worker_a = SessionId::new();
        let worker_b = SessionId::new();
        let metrics = SubscriptionMetrics::default();

        metrics.record_outcome(
            &kian_superagent::RoutingOutcome::Routed {
                session: router,
                deduplicated: false,
            },
            router,
        );
        metrics.record_outcome(
            &kian_superagent::RoutingOutcome::Routed {
                session: worker_a,
                deduplicated: true,
            },
            router,
        );
        metrics.record_outcome(
            &kian_superagent::RoutingOutcome::FannedOut {
                admitted: vec![
                    FanoutAdmission {
                        session: worker_a,
                        deduplicated: false,
                    },
                    FanoutAdmission {
                        session: worker_b,
                        deduplicated: true,
                    },
                ],
            },
            router,
        );
        metrics.record_outcome(&kian_superagent::RoutingOutcome::DroppedNoRule, router);
        metrics.record_outcome(&kian_superagent::RoutingOutcome::DroppedIgnored, router);
        metrics.record_outcome(&kian_superagent::RoutingOutcome::DroppedNotExplicit, router);

        let snap = metrics.snapshot();
        let by_reason = &snap["by_reason"];
        // Exactly once per decision: 1 router admit, 1 passthrough
        // admit, 2 dedups, 1 per drop shape.
        assert_eq!(by_reason["admitted_router"], serde_json::json!(1));
        assert_eq!(by_reason["admitted_passthrough"], serde_json::json!(1));
        assert_eq!(by_reason["deduplicated"], serde_json::json!(2));
        assert_eq!(by_reason["no_rule"], serde_json::json!(1));
        assert_eq!(by_reason["class_ignored"], serde_json::json!(1));
        assert_eq!(by_reason["not_explicit"], serde_json::json!(1));
        // Reserved for C59: defined and materialized, never produced.
        assert_eq!(by_reason["released_to_eval"], serde_json::json!(0));
        // Every code of the closed set is materialized — the snapshot
        // shape is fixed regardless of what has happened.
        assert_eq!(
            by_reason.as_object().expect("by_reason object").len(),
            kian_superagent::IntakeReason::ALL.len()
        );
        // The legacy shape counters keep their own semantics.
        assert_eq!(snap["routed"], serde_json::json!(4));
        assert_eq!(snap["routed_deduplicated"], serde_json::json!(2));
        assert_eq!(snap["dropped_no_rule"], serde_json::json!(1));
        assert_eq!(snap["dropped_ignored"], serde_json::json!(1));
        assert_eq!(snap["dropped_not_explicit"], serde_json::json!(1));
        assert_eq!(snap["failures"], serde_json::json!(0));
    }
}
