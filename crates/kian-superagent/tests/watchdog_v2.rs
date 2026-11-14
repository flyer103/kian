//! Integration tests: Watchdog Session v2 — the
//! model-in-the-loop progress judge over a real sqlite `EventStore`
//! and a real `Engine`. ScriptedProvider on all three sides (Router,
//! Worker, Watchdog), `TimerChannel` driven by a `ManualTurnClock` as
//! the tick source, `Route::Session` routing the ticks: no
//! credentials, no network egress, no wall-clock cadence asserts.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use kian_core::events::{Event, EventKind};
use kian_core::model::{InputRow, SessionId};
use kian_core::seams::{
    ChannelAdapter, ChannelEvent, LoopDriver, LoopOutcome, ModelChunk, ModelProvider, ModelRequest,
    PolicyRule, RecoveryPolicySource, ToolCall, ToolError, ToolProvider, ToolResult, ToolSchema,
};
use kian_llm::turn::ProviderRegistry;
use kian_loop::{EvidenceGateConfig, ReActConfig, ReActLoop};
use kian_orchestrator::{Clock, DrainCtx, Engine, EngineConfig, SystemClock};
use kian_store::EventStore;
use kian_superagent::TurnClock;
use kian_superagent::rules::{Route, RuleMatcher, SubscriptionRule};
use kian_superagent::{
    AdmitReceipt, ChannelRegistry, InboxWriteError, InboxWriter, MockChannel, PortFor, RecallPort,
    RecallPortError, RouterDriver, RouterHold, RouterInbox, RouterProfile, RouterTools, SpillIndex,
    StreamPort, StreamPortError, SubscriptionRuleSet, TimerChannel, TimerConfig, WATCHDOG_TOOLS,
    WatchdogProfile, WatchdogTools, WorkerLoopConfig, WorkerLoopDriver, WorkerSpawner,
    channel_input_payload, thread_bindings,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

async fn temp_store() -> EventStore {
    let path = std::env::temp_dir().join(format!("kian-superagent-c40-{}.sqlite", Uuid::new_v4()));
    let store = EventStore::open(&path).await.expect("open store");
    std::mem::forget(path);
    store
}

async fn wait_for_events(
    store: &EventStore,
    session: SessionId,
    desc: &str,
    pred: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    for _ in 0..1_200 {
        let events = store.rebuild(session).await.expect("rebuild");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

fn count_kind(events: &[Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind).count()
}

// ---------------------------------------------------------------------------
// ScriptedProvider (all three sides — no credentials, no egress)
// ---------------------------------------------------------------------------

type ScriptedChunks = Vec<Result<ModelChunk, kian_core::seams::ProviderError>>;
type DynamicTurn = Arc<dyn Fn(&ModelRequest) -> ScriptedChunks + Send + Sync>;

enum ScriptedTurn {
    Fixed(ScriptedChunks),
    Dynamic(DynamicTurn),
}

struct ScriptedProvider {
    turns: Mutex<VecDeque<ScriptedTurn>>,
    call_count: std::sync::atomic::AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ScriptedProvider {
    fn new(turns: Vec<ScriptedChunks>) -> Self {
        Self {
            turns: Mutex::new(
                turns
                    .into_iter()
                    .map(ScriptedTurn::Fixed)
                    .collect::<VecDeque<_>>(),
            ),
            call_count: std::sync::atomic::AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn push_dynamic(&self, turn: DynamicTurn) {
        self.turns
            .lock()
            .unwrap()
            .push_back(ScriptedTurn::Dynamic(turn));
    }

    fn request_count(&self) -> usize {
        self.call_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn request_bodies(&self) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.body.clone())
            .collect()
    }
}

impl ModelProvider for ScriptedProvider {
    fn complete(
        &self,
        req: ModelRequest,
    ) -> BoxStream<'static, Result<ModelChunk, kian_core::seams::ProviderError>> {
        self.requests.lock().unwrap().push(req.clone());
        let _ = self
            .call_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let turn = match self.turns.lock().unwrap().pop_front() {
            Some(ScriptedTurn::Fixed(chunks)) => chunks,
            Some(ScriptedTurn::Dynamic(build)) => build(&req),
            None => Vec::new(),
        };
        futures::stream::iter(turn).boxed()
    }
}

fn text_chunk(text: &str) -> Result<ModelChunk, kian_core::seams::ProviderError> {
    Ok(ModelChunk::new(json!({"content": text})))
}

fn tool_call_chunk(
    id: &str,
    name: &str,
    args: Value,
) -> Result<ModelChunk, kian_core::seams::ProviderError> {
    let arguments = serde_json::to_string(&args).unwrap();
    Ok(ModelChunk::new(json!({"tool_calls": [{
        "index": 0,
        "id": id,
        "function": {"name": name, "arguments": arguments}
    }]})))
}

/// The first thread id found in any prior tool result of the request
/// body (`threads[0].thread_id` from `list_threads`, `thread_id` from
/// `read_progress`) — the watchdog's dynamic turns read the
/// runtime-minted thread id this way. Dialect v2: a
/// `role:"tool"` message's content is the serialized tool output
/// itself; the legacy v1 shape wrapped it as `{"call_id","output"}` —
/// both accepted here.
fn thread_id_from_request(req: &ModelRequest) -> Option<String> {
    let messages = req.body.get("messages")?.as_array()?;
    for message in messages {
        if message.get("role").and_then(Value::as_str) != Some("tool") {
            continue;
        }
        let Ok(content) = serde_json::from_str::<Value>(
            message
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or_default(),
        ) else {
            continue;
        };
        let candidates = [
            content.pointer("/thread_id"),
            content.pointer("/output/thread_id"),
            content.pointer("/threads/0/thread_id"),
            content.pointer("/output/threads/0/thread_id"),
        ];
        if let Some(id) = candidates
            .into_iter()
            .flatten()
            .find_map(|v| v.as_str().map(str::to_string))
        {
            return Some(id);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tools / policy fixtures
// ---------------------------------------------------------------------------

/// The worker's tool surface in these tests: an `exec` tool gated by a
/// flag — the drain parks mid-turn inside it until the test releases,
/// which is exactly what a stuck worker looks like (thread outstanding,
/// stale last assistant message, no completion).
struct GatedTools {
    released: Arc<std::sync::atomic::AtomicBool>,
}

impl GatedTools {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            released: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }
}

impl ToolProvider for GatedTools {
    fn schemas(&self) -> Vec<ToolSchema> {
        vec![ToolSchema {
            name: "exec".into(),
            description: "gated test exec".into(),
            parameters: json!({"type": "object", "properties": {}}),
        }]
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        let flag = Arc::clone(&self.released);
        Box::pin(async move {
            while !flag.load(std::sync::atomic::Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok(ToolResult {
                call_id: call.id,
                output: json!({"ok": true, "stdout": "released"}),
                is_error: false,
                redactions: Vec::new(),
            })
        })
    }
}

struct NoopPolicy;
impl RecoveryPolicySource for NoopPolicy {
    fn table(&self) -> Vec<PolicyRule> {
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// Assembly surface: InboxWriter + StreamPort + RecallPort
// ---------------------------------------------------------------------------

struct LateEngineInbox {
    store: EventStore,
    clock: Arc<dyn Clock>,
    engine: OnceLock<Arc<Engine<HarnessDriver>>>,
}

impl InboxWriter for LateEngineInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        let store = self.store.clone();
        let now = self.clock.now_ms();
        Box::pin(async move {
            let outcome = store
                .admit(InputRow {
                    id: Uuid::new_v4(),
                    session,
                    payload,
                    admitted_at: now,
                    eligible_at: now,
                    idempotency_key,
                })
                .await
                .map_err(|e| InboxWriteError::Write(e.to_string()))?;
            Ok(AdmitReceipt {
                deduplicated: outcome.deduplicated,
            })
        })
    }

    fn wake(&self, session: SessionId) -> kian_orchestrator::WakeOutcome {
        self.engine
            .get()
            .expect("engine installed before first wake")
            .wake(session)
    }
}

struct CtxPort(DrainCtx);

impl StreamPort for CtxPort {
    fn session(&self) -> SessionId {
        self.0.session()
    }

    fn emit(
        &self,
        kind: EventKind,
        bytes: Value,
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

fn port_for_ctx() -> PortFor<DrainCtx> {
    Arc::new(|ctx: &DrainCtx| Arc::new(CtxPort(ctx.clone())) as Arc<dyn StreamPort>)
}

/// The test's `RecallPort` over the store's public scan (the kian-cli
/// `StoreRecall` shape).
struct StoreRecallPort {
    store: EventStore,
    /// Constructed for shape parity with the kian-cli recall port; this
    /// suite never reads the index (pre-existing at: clippy 1.98
    /// `dead_code`, fixed lint-only).
    #[expect(dead_code)]
    spill: Mutex<SpillIndex>,
}

impl StoreRecallPort {
    fn new(store: EventStore) -> Arc<Self> {
        Arc::new(Self {
            store,
            spill: Mutex::new(SpillIndex::new()),
        })
    }
}

impl RecallPort for StoreRecallPort {
    fn read_events(
        &self,
        session: SessionId,
        since_seq: u64,
        until_seq: u64,
    ) -> BoxFuture<'static, Result<Vec<Event>, RecallPortError>> {
        let store = self.store.clone();
        Box::pin(async move {
            if since_seq >= until_seq {
                return Ok(Vec::new());
            }
            store
                .scan(session, since_seq..until_seq)
                .await
                .map_err(|e| RecallPortError::Read(e.to_string()))
        })
    }

    fn list_threads(
        &self,
        parent: SessionId,
    ) -> BoxFuture<'static, Result<Vec<(String, SessionId)>, RecallPortError>> {
        let store = self.store.clone();
        Box::pin(async move {
            let events = store
                .rebuild(parent)
                .await
                .map_err(|e| RecallPortError::Read(e.to_string()))?;
            Ok(thread_bindings(&events))
        })
    }

    fn spill_location(
        &self,
        _event_id: &str,
    ) -> BoxFuture<'static, Result<Option<(SessionId, u64)>, RecallPortError>> {
        Box::pin(async { Ok(None) })
    }
}

// ---------------------------------------------------------------------------
// The assembled driver (router + watchdog + worker)
// ---------------------------------------------------------------------------

struct HarnessDriver {
    router: SessionId,
    watchdog: SessionId,
    router_side: RouterDriver<ReActLoop>,
    watchdog_side: RouterDriver<ReActLoop>,
    worker_side: WorkerLoopDriver<ReActLoop>,
}

impl LoopDriver for HarnessDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        if ctx.session() == self.router {
            self.router_side.drive(ctx)
        } else if ctx.session() == self.watchdog {
            // The watchdog is a resident session exactly like the
            // Router: park between judge turns, one drain per tick.
            self.watchdog_side.drive(ctx)
        } else {
            self.worker_side.drive(ctx)
        }
    }
}

struct Harness {
    store: EventStore,
    engine: Arc<Engine<HarnessDriver>>,
    router: SessionId,
    watchdog: SessionId,
    /// Kept for debug symmetry with worker/watchdog scripts; no test in
    /// this suite reads it (pre-existing at: clippy 1.98 `dead_code`,
    /// fixed lint-only).
    #[expect(dead_code)]
    router_script: Arc<ScriptedProvider>,
    worker_script: Arc<ScriptedProvider>,
    watchdog_script: Arc<ScriptedProvider>,
    subscriptions: Arc<kian_superagent::SubscriptionService>,
    timer: Arc<TimerChannel>,
    timer_clock: Arc<kian_superagent::ManualTurnClock>,
    gate: Arc<GatedTools>,
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

const TIMER_INTERVAL_MS: u64 = 5_000;

async fn build_harness(
    router_turns: Vec<ScriptedChunks>,
    worker_turns: Vec<ScriptedChunks>,
    watchdog_turns: Vec<ScriptedChunks>,
) -> Harness {
    let store = temp_store().await;
    let router = SessionId::new();
    let watchdog = SessionId::new();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    let router_agent = kian_superagent::router_template("mock", "mock-router");
    let worker_agent = kian_superagent::worker_template("mock", "mock-worker", 7);
    let watchdog_agent = kian_superagent::watchdog_template("mock", "mock-watchdog", 1);

    let router_script = Arc::new(ScriptedProvider::new(router_turns));
    let worker_script = Arc::new(ScriptedProvider::new(worker_turns));
    let watchdog_script = Arc::new(ScriptedProvider::new(watchdog_turns));

    let mut router_registry = ProviderRegistry::new();
    router_registry.register("mock", Arc::clone(&router_script) as Arc<dyn ModelProvider>);
    let mut worker_registry = ProviderRegistry::new();
    worker_registry.register("mock", Arc::clone(&worker_script) as Arc<dyn ModelProvider>);
    let mut watchdog_registry = ProviderRegistry::new();
    watchdog_registry.register(
        "mock",
        Arc::clone(&watchdog_script) as Arc<dyn ModelProvider>,
    );

    let late = Arc::new(LateEngineInbox {
        store: store.clone(),
        clock: clock.clone(),
        engine: OnceLock::new(),
    });
    let router_inbox = RouterInbox::new(late.clone() as Arc<dyn InboxWriter>, router);
    let watchdog_inbox = RouterInbox::new(late.clone() as Arc<dyn InboxWriter>, watchdog);

    // Router dispatch tools over the mock IM channel.
    let spawner = Arc::new(WorkerSpawner::new(
        router,
        &worker_agent,
        router_inbox.clone() as Arc<dyn InboxWriter>,
    ));
    let mock = Arc::new(MockChannel::new("mock-im"));
    let mut channels = ChannelRegistry::new();
    channels.register("mock-im", Arc::clone(&mock) as Arc<dyn ChannelAdapter>);
    let hold = RouterHold::shared();
    let profile = RouterProfile::new(router, router_agent.clone());
    let dispatch_tools = Arc::new(RouterTools::new(
        spawner,
        channels.clone(),
        &profile,
        hold.clone(),
    ));

    // The watchdog's read/intervene surface over the same recall port
    // assembly exposes, the engine inbox, the shared channel registry
    // (escalation) and its own hold.
    let recall = StoreRecallPort::new(store.clone());
    let watchdog_hold = RouterHold::shared();
    let watchdog_clock = Arc::new(kian_superagent::ManualTurnClock::new(1_000_000));
    let watchdog_tools = Arc::new(WatchdogTools::new(
        recall.clone() as Arc<dyn RecallPort>,
        router,
        watchdog_inbox.clone() as Arc<dyn InboxWriter>,
        channels,
        kian_superagent::ImPolicy::default(),
        watchdog_hold.clone(),
        watchdog_clock,
    ));

    let router_loop = Arc::new(ReActLoop::with_config(
        router_agent,
        Arc::new(router_registry),
        dispatch_tools,
        Arc::new(NoopPolicy),
        ReActConfig {
            slot: kian_core::model::SlotName("router".into()),
            max_iterations: 10_000,
            budget_scope: kian_loop::BudgetScope::Lifetime,
            permissions: std::collections::BTreeMap::new(),
        },
    ));
    let gate = GatedTools::new();
    let worker_loop = Arc::new(ReActLoop::with_config(
        worker_agent,
        Arc::new(worker_registry),
        gate.clone(),
        Arc::new(NoopPolicy),
        ReActConfig::default(),
    ));
    let watchdog_loop = Arc::new(ReActLoop::with_config(
        watchdog_agent,
        Arc::new(watchdog_registry),
        watchdog_tools,
        Arc::new(NoopPolicy),
        ReActConfig {
            slot: kian_core::model::SlotName("main".into()),
            max_iterations: 10_000,
            budget_scope: kian_loop::BudgetScope::Lifetime,
            permissions: std::collections::BTreeMap::new(),
        },
    ));

    let router_side = RouterDriver::with_park_tick(
        router_loop,
        hold,
        port_for_ctx(),
        router_inbox.clone(),
        Duration::from_millis(10),
    );
    let watchdog_side = RouterDriver::with_park_tick(
        watchdog_loop,
        watchdog_hold,
        port_for_ctx(),
        watchdog_inbox.clone(),
        Duration::from_millis(10),
    );
    let worker_side = WorkerLoopDriver::new(
        worker_loop,
        router_inbox.clone() as Arc<dyn InboxWriter>,
        WorkerLoopConfig {
            router,
            worker_version: 7,
            gate: EvidenceGateConfig::default(),
        },
        port_for_ctx(),
    );

    let engine = Engine::with_clock(
        store.clone(),
        Arc::new(HarnessDriver {
            router,
            watchdog,
            router_side,
            watchdog_side,
            worker_side,
        }),
        EngineConfig::default(),
        clock,
    );
    assert!(late.engine.set(Arc::clone(&engine)).is_ok());

    // The timer input source: a channel like any other, routed to the
    // fixed watchdog session by an ordinary subscription rule.
    let timer_clock = Arc::new(kian_superagent::ManualTurnClock::new(0));
    let timer = Arc::new(TimerChannel::new(
        "timer",
        TimerConfig::new(TIMER_INTERVAL_MS),
        timer_clock.clone() as Arc<dyn TurnClock>,
    ));
    let rules = SubscriptionRuleSet::new(vec![SubscriptionRule {
        channel: "timer".into(),
        matcher: RuleMatcher::Topic("watchdog".into()),
        route: Route::Session(watchdog),
    }]);
    let subscriptions = Arc::new(kian_superagent::SubscriptionService::new(
        router,
        rules,
        router_inbox.clone() as Arc<dyn InboxWriter>,
    ));
    let mut tasks = subscriptions.spawn(vec![timer.clone() as Arc<dyn ChannelAdapter>]);
    tasks.push(timer.spawn(Duration::from_millis(2)));
    // Let the timer's run loop anchor its start before time moves.
    tokio::time::sleep(Duration::from_millis(20)).await;

    Harness {
        store,
        engine,
        router,
        watchdog,
        router_script,
        worker_script,
        watchdog_script,
        subscriptions,
        timer,
        timer_clock,
        gate,
        _tasks: tasks,
    }
}

fn trigger_event(event_id: &str, text: &str) -> ChannelEvent {
    ChannelEvent {
        channel: "mock-im".into(),
        event_id: event_id.into(),
        topic: None,
        sender: None,
        group: None,
        payload: json!({"text": text}),
        ts_ms: 42,
    }
}

/// Fires tick `n` by advancing the manual clock past its deadline.
fn fire_tick(h: &Harness, n: u64) {
    h.timer_clock
        .advance(n * TIMER_INTERVAL_MS + 1 - h.timer_clock.now_ms());
    assert!(h.timer_clock.now_ms() > n * TIMER_INTERVAL_MS);
}

/// Adapts the engine to the `InboxWriter` port for the bootstrap test
/// (engine.admit is row-then-wake — the same ordering the port holds).
struct EngineAsInbox(Arc<Engine<HarnessDriver>>);

impl InboxWriter for EngineAsInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        let engine = Arc::clone(&self.0);
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

    fn wake(&self, session: SessionId) -> kian_orchestrator::WakeOutcome {
        self.0.wake(session)
    }
}

/// Pushes the watchdog's judge turns: list_threads → read_progress →
/// intervene (each reading the runtime-minted thread id out of the
/// request body) → a final text judgment.
fn script_watchdog_intervention(h: &Harness, nudge: &str) {
    let script = Arc::clone(&h.watchdog_script);
    script.push_dynamic(Arc::new(|_req: &ModelRequest| {
        vec![tool_call_chunk("call-list", "list_threads", json!({}))]
    }));
    script.push_dynamic(Arc::new(|req: &ModelRequest| {
        let thread_id =
            thread_id_from_request(req).expect("the list_threads result carries the thread id");
        vec![tool_call_chunk(
            "call-read",
            "read_progress",
            json!({"thread_id": thread_id}),
        )]
    }));
    let nudge = nudge.to_string();
    script.push_dynamic(Arc::new(move |req: &ModelRequest| {
        let thread_id =
            thread_id_from_request(req).expect("the read_progress result carries the thread id");
        vec![tool_call_chunk(
            "call-intervene",
            "intervene",
            json!({"thread_id": thread_id, "text": nudge}),
        )]
    }));
    script.push_dynamic(Arc::new(|_req: &ModelRequest| {
        vec![text_chunk("thread was stuck; nudged the worker")]
    }));
}

/// Pushes the watchdog's healthy no-op judge turns.
fn script_watchdog_noop(h: &Harness) {
    let script = Arc::clone(&h.watchdog_script);
    script.push_dynamic(Arc::new(|_req: &ModelRequest| {
        vec![tool_call_chunk("call-list", "list_threads", json!({}))]
    }));
    script.push_dynamic(Arc::new(|req: &ModelRequest| {
        let thread_id =
            thread_id_from_request(req).expect("the list_threads result carries the thread id");
        vec![tool_call_chunk(
            "call-read",
            "read_progress",
            json!({"thread_id": thread_id}),
        )]
    }));
    script.push_dynamic(Arc::new(|_req: &ModelRequest| {
        vec![text_chunk("all threads healthy — no intervention")]
    }));
}

// ---------------------------------------------------------------------------
// Named acceptance tests
// ---------------------------------------------------------------------------

/// The stuck-worker arc, end to end: a dispatched thread with no
/// completion and a stale last message → tick → the watchdog judges
/// (ScriptedProvider) → `intervene` admits a steer row (durable before
/// the wake, invariant 1) → the worker's next drain consumes it at the
/// provider-turn safe boundary and completes honestly.
#[tokio::test]
async fn stuck_worker_tick_judges_intervenes_and_the_worker_consumes_the_steer() {
    let nudge = "watchdog nudge: please report your status now";
    let h = build_harness(
        vec![vec![tool_call_chunk(
            "call-spawn",
            "spawn_worker",
            json!({"task": "chase the flaky test"}),
        )]],
        vec![
            // Turn 1: an assistant message (the stale progress), then a
            // gated exec call — the drain parks inside it (stuck).
            vec![
                text_chunk("digging through the logs, one moment"),
                tool_call_chunk("call-exec", "exec", json!({})),
            ],
            // Turn 2 (after the gate releases): the final answer.
            vec![text_chunk("resumed after the nudge — done")],
        ],
        vec![],
    )
    .await;
    script_watchdog_intervention(&h, nudge);

    // Hop 1 — the trigger: the Router dispatches the worker.
    h.engine
        .admit(
            h.router,
            channel_input_payload(&trigger_event("evt-trigger", "please chase the flaky test")),
            Some("evt-trigger".into()),
        )
        .await
        .expect("admit trigger");

    let router_events = wait_for_events(&h.store, h.router, "thread/started", |events| {
        events.iter().any(|e| e.kind == EventKind::ThreadStarted)
    })
    .await;
    let started = router_events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .unwrap();
    let worker =
        SessionId(Uuid::parse_str(started.bytes["worker"].as_str().unwrap()).expect("worker uuid"));
    let thread_id = started.bytes["thread_id"].as_str().unwrap().to_string();

    // The stuck state: an assistant message exists, the gated exec has
    // not returned, no completion. Wait until the stale message is
    // committed (the drain is parked inside the tool).
    let worker_events = wait_for_events(&h.store, worker, "stale assistant message", |events| {
        events.iter().any(|e| e.kind == EventKind::AssistantMessage)
    })
    .await;
    assert!(
        !worker_events
            .iter()
            .any(|e| e.kind == EventKind::ThreadCompleted),
        "the stuck thread has no completion"
    );

    // Hop 2 — the tick: the timer fires, the tick routes to the
    // watchdog session, and the judge arc runs.
    fire_tick(&h, 1);
    let watchdog_events = wait_for_events(
        &h.store,
        h.watchdog,
        "watchdog intervenes and judges",
        |events| {
            // Three tool-call turns (list, read, intervene) plus the
            // final judgment text.
            events.iter().any(|e| {
                e.kind == EventKind::ToolResult
                    && e.bytes["content"]["call_id"] == json!("call-intervene")
                    && e.bytes["content"]["is_error"] == json!(false)
            }) && events.iter().any(|e| {
                e.kind == EventKind::AssistantMessage
                    && e.bytes["content"]["text"] == json!("thread was stuck; nudged the worker")
            })
        },
    )
    .await;

    // The tick is audited in the watchdog's own stream,
    // provenance-wrapped (the timer is a channel like any other).
    let tick_input = watchdog_events
        .iter()
        .find(|e| e.kind == EventKind::InputAdmitted)
        .expect("the tick was admitted");
    assert_eq!(
        tick_input.bytes["content"]["kian"]["type"],
        json!("channel_event")
    );
    assert_eq!(
        tick_input.bytes["content"]["kian"]["event_id"],
        json!("tick-1")
    );
    assert_eq!(
        tick_input.bytes["content"]["kian"]["channel"],
        json!("timer")
    );

    // The read tools saw the stuck state the judgment was made on.
    let read_result = watchdog_events
        .iter()
        .find(|e| {
            e.kind == EventKind::ToolResult && e.bytes["content"]["call_id"] == json!("call-read")
        })
        .expect("read_progress result");
    let progress = &read_result.bytes["content"]["output"];
    assert_eq!(progress["status"], json!("outstanding"));
    assert_eq!(progress["thread_id"], json!(thread_id));
    assert_eq!(
        progress["last_assistant"]["text"],
        json!("digging through the logs, one moment")
    );

    // The steer row is durable (admitted, keyed by the tool call) —
    // re-handling the same tick id (a restart's re-emission) dedupes
    // against the committed row and never re-intervenes.
    let again = h
        .subscriptions
        .handle_event(h.timer.tick_event(1))
        .await
        .expect("re-handle tick-1");
    assert!(matches!(
        again,
        kian_superagent::RoutingOutcome::Routed {
            session,
            deduplicated: true
        } if session == h.watchdog
    ));

    // Hop 3 — release the gate: the worker's next provider turn
    // consumes the steer (promoted at the safe boundary, synthetic
    // marker intact) and completes honestly.
    h.gate
        .released
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let worker_events = wait_for_events(
        &h.store,
        worker,
        "steer consumed + thread completed",
        |events| {
            events.iter().any(|e| e.kind == EventKind::ThreadCompleted)
                && events.iter().any(|e| {
                    e.kind == EventKind::InputAdmitted
                        && e.bytes.get("content").and_then(|c| c.as_str()) == Some(nudge)
                        && e.bytes.get("synthetic") == Some(&json!(true))
                })
        },
    )
    .await;

    // The promoted steer is model-visible in a persisted request body
    // (persist-what-you-send): the post-gate request carries it.
    let bodies = h.worker_script.request_bodies();
    assert!(h.worker_script.request_count() >= 2);
    assert!(
        bodies
            .iter()
            .any(|b| serde_json::to_string(b).unwrap().contains(nudge)),
        "the nudge rode a provider request"
    );

    // The completion report reached the Router (the honest end).
    let router_events = h.store.rebuild(h.router).await.unwrap();
    assert!(router_events.iter().any(|e| {
        e.kind == EventKind::InputAdmitted
            && e.bytes["content"]["thread_id"] == json!(thread_id)
            && e.bytes["content"]["status"] == json!("completed")
    }));
    assert_eq!(
        worker_events
            .iter()
            .find(|e| e.kind == EventKind::ThreadCompleted)
            .unwrap()
            .bytes["status"],
        json!("completed")
    );

    // A2 replay: the watchdog's own events re-derive the same model
    // context from either read path — the projection is pure.
    let rebuilt = h.store.rebuild(h.watchdog).await.unwrap();
    let scanned = h.store.scan(h.watchdog, 0..u64::MAX).await.unwrap();
    let ctx_a = kian_core::context::derive_model_context(&rebuilt).unwrap();
    let ctx_b = kian_core::context::derive_model_context(&scanned).unwrap();
    assert_eq!(
        kian_core::context::context_to_bytes(&ctx_a),
        kian_core::context::context_to_bytes(&ctx_b),
        "replay derives identical context bytes"
    );
}

/// A healthy worker earns no intervention: the tick judges, the no-op
/// is the outcome, and the no-op itself is auditable in the watchdog
/// stream — no steer row, no channel delivery, nothing hidden.
#[tokio::test]
async fn healthy_worker_tick_is_an_auditable_no_op() {
    let h = build_harness(
        vec![vec![tool_call_chunk(
            "call-spawn",
            "spawn_worker",
            json!({"task": "quick task"}),
        )]],
        vec![vec![text_chunk("done in one turn")]],
        vec![],
    )
    .await;
    script_watchdog_noop(&h);

    h.engine
        .admit(
            h.router,
            channel_input_payload(&trigger_event("evt-trigger", "please do the quick task")),
            Some("evt-trigger".into()),
        )
        .await
        .expect("admit trigger");

    // The worker completes and reports; wait for the consumed report.
    let router_events =
        wait_for_events(&h.store, h.router, "completion report consumed", |events| {
            events.iter().any(|e| {
                e.kind == EventKind::InputAdmitted
                    && e.bytes["content"].get("thread_id").is_some()
                    && e.bytes["content"]["status"] == json!("completed")
            })
        })
        .await;
    let started = router_events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .unwrap();
    let worker = SessionId(Uuid::parse_str(started.bytes["worker"].as_str().unwrap()).unwrap());

    // The tick judges.
    fire_tick(&h, 1);
    let watchdog_events =
        wait_for_events(&h.store, h.watchdog, "healthy no-op judgment", |events| {
            events.iter().any(|e| {
                e.kind == EventKind::AssistantMessage
                    && e.bytes["content"]["text"] == json!("all threads healthy — no intervention")
            })
        })
        .await;
    // The read surface saw health: status completed.
    let read_result = watchdog_events
        .iter()
        .find(|e| {
            e.kind == EventKind::ToolResult && e.bytes["content"]["call_id"] == json!("call-read")
        })
        .expect("read_progress result");
    assert_eq!(
        read_result.bytes["content"]["output"]["status"],
        json!("completed")
    );

    // The no-op is the honest outcome: no intervene tool call, no
    // channel delivery, and the worker's stream gained nothing.
    assert!(
        !watchdog_events
            .iter()
            .any(|e| e.kind == EventKind::ToolResult
                && e.bytes["content"]["call_id"] == json!("call-intervene")),
        "no intervention for a healthy thread"
    );
    assert!(
        !watchdog_events
            .iter()
            .any(|e| e.kind == EventKind::ChannelDelivered),
        "no escalation for a healthy thread"
    );
    let worker_events = h.store.rebuild(worker).await.unwrap();
    assert_eq!(
        count_kind(&worker_events, EventKind::InputAdmitted),
        1,
        "the worker consumed only its delegation — no steer row"
    );
    // And the no-op is auditable: the tick input, both read results
    // and the final judgment are all in the watchdog's own stream.
    assert_eq!(count_kind(&watchdog_events, EventKind::InputAdmitted), 1);
    assert_eq!(count_kind(&watchdog_events, EventKind::ToolResult), 2);
    assert_eq!(
        count_kind(&watchdog_events, EventKind::AssistantMessage),
        3,
        "two tool-call turns plus the final no-op judgment"
    );
}

/// The watchdog profile bootstraps its judge prompt idempotently, and
/// the bootstrapped session still judges ticks (the production shape:
/// `start_services` bootstraps, ticks arrive later).
#[tokio::test]
async fn bootstrapped_watchdog_still_judges_ticks() {
    let h = build_harness(
        vec![],
        vec![],
        vec![vec![text_chunk("understood my charge")]],
    )
    .await;
    let profile = WatchdogProfile::new(h.watchdog, {
        let mut agent = kian_superagent::watchdog_template("mock", "mock-watchdog", 1);
        agent.tools = WATCHDOG_TOOLS.iter().map(|t| t.to_string()).collect();
        agent
    });
    let inbox = Arc::new(EngineAsInbox(Arc::clone(&h.engine)));
    let receipt = profile
        .bootstrap(inbox.as_ref() as &dyn InboxWriter)
        .await
        .expect("bootstrap");
    assert!(!receipt.deduplicated);
    // The second bootstrap (a restart's call) dedupes — idempotent.
    let again = profile
        .bootstrap(inbox.as_ref() as &dyn InboxWriter)
        .await
        .expect("bootstrap again");
    assert!(again.deduplicated);

    wait_for_events(
        &h.store,
        h.watchdog,
        "bootstrap prompt consumed",
        |events| events.iter().any(|e| e.kind == EventKind::AssistantMessage),
    )
    .await;
    assert_eq!(h.watchdog_script.request_count(), 1);
}
