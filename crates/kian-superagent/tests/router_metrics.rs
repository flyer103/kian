//! Integration tests: §7.2-shaped batching metrics over the
//! Router drain — per judge turn, inputs consumed (batch size), turn
//! wall duration (per-request) and duration/inputs (per-decision
//! amortized latency). Recorded by `RouterDriver` through the
//! bundle-owned `RouterMetrics` + `ManualTurnClock` (deterministic
//! timing: the clock moves only when the test advances it, and a gated
//! scripted provider pins the turn's start/end around the advance).
//! ScriptedProvider both sides, no credentials, no network egress.

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
use kian_loop::{ReActConfig, ReActLoop};
use kian_orchestrator::{Clock, DrainCtx, Engine, EngineConfig, SystemClock, WakeOutcome};
use kian_store::EventStore;
use kian_superagent::{
    AdmitReceipt, ChannelRegistry, InboxWriteError, InboxWriter, ManualTurnClock, MockChannel,
    RouterDriver, RouterHold, RouterInbox, RouterMetrics, RouterProfile, RouterTools,
    RouterTurnRecorder, StreamPort, StreamPortError, WorkerSpawner, channel_input_payload,
    router_template, worker_template,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

async fn temp_store() -> EventStore {
    let path = std::env::temp_dir().join(format!("kian-superagent-c41-{}.sqlite", Uuid::new_v4()));
    let store = EventStore::open(&path).await.expect("open store");
    std::mem::forget(path);
    store
}

/// Polls a session's committed stream until `pred` holds (bounded,
/// generous — never tight enough to flake).
async fn wait_for_events(
    store: &EventStore,
    session: SessionId,
    desc: &str,
    pred: impl Fn(&[Event]) -> bool,
) {
    for _ in 0..800 {
        let events = store.rebuild(session).await.expect("rebuild");
        if pred(&events) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

fn count_kind(events: &[Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind).count()
}

// ---------------------------------------------------------------------------
// A counting, optionally gated scripted provider (the Router side)
// ---------------------------------------------------------------------------

type ScriptedChunks = Vec<Result<ModelChunk, kian_core::seams::ProviderError>>;

/// A `ModelProvider` that replays scripted turns in order, counts
/// requests, and — for one chosen call — parks on a oneshot gate until
/// the test releases it (pinning the judge turn's open interval around
/// a manual clock advance).
struct CountingProvider {
    turns: Mutex<VecDeque<ScriptedChunks>>,
    call_count: std::sync::atomic::AtomicUsize,
    gate_on_call: usize,
    gate_rx: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl CountingProvider {
    fn with_gate(
        turns: Vec<ScriptedChunks>,
        gate_on_call: usize,
        gate: Option<tokio::sync::oneshot::Receiver<()>>,
    ) -> Self {
        Self {
            turns: Mutex::new(turns.into()),
            call_count: std::sync::atomic::AtomicUsize::new(0),
            gate_on_call,
            gate_rx: Mutex::new(gate),
        }
    }

    fn request_count(&self) -> usize {
        self.call_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Polls until the provider has served `n` requests.
    async fn wait_for_calls(&self, n: usize) {
        for _ in 0..800 {
            if self.request_count() >= n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("provider never reached {n} calls");
    }
}

impl ModelProvider for CountingProvider {
    fn complete(
        &self,
        _req: ModelRequest,
    ) -> BoxStream<'static, Result<ModelChunk, kian_core::seams::ProviderError>> {
        use std::sync::atomic::Ordering;
        let n = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
        let turn = self.turns.lock().unwrap().pop_front().unwrap_or_default();
        let gate = if n == self.gate_on_call {
            self.gate_rx.lock().unwrap().take()
        } else {
            None
        };
        match gate {
            Some(rx) => futures::stream::once(async move {
                // A dropped sender counts as released (teardown safety).
                let _ = rx.await;
                turn
            })
            .flat_map(futures::stream::iter)
            .boxed(),
            None => futures::stream::iter(turn).boxed(),
        }
    }
}

/// The worker side never fires in these tests; one plain scripted turn.
struct PlainProvider {
    turns: Mutex<VecDeque<ScriptedChunks>>,
}

impl ModelProvider for PlainProvider {
    fn complete(
        &self,
        _req: ModelRequest,
    ) -> BoxStream<'static, Result<ModelChunk, kian_core::seams::ProviderError>> {
        let turn = self.turns.lock().unwrap().pop_front().unwrap_or_default();
        futures::stream::iter(turn).boxed()
    }
}

fn text_chunk(text: &str) -> Result<ModelChunk, kian_core::seams::ProviderError> {
    Ok(ModelChunk::new(json!({"content": text})))
}

/// The worker side's inert tool provider.
struct FakeTools;

impl ToolProvider for FakeTools {
    fn schemas(&self) -> Vec<ToolSchema> {
        [ToolSchema {
            name: "fs.read".into(),
            description: "test".into(),
            parameters: json!({"type": "object", "properties": {}}),
        }]
        .into_iter()
        .collect()
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        Box::pin(async move {
            Ok(ToolResult {
                call_id: call.id,
                output: json!({"ok": true}),
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
// Assembly surface: InboxWriter + StreamPort over the real engine
// ---------------------------------------------------------------------------

/// The cycle breaker: admits are durable store writes; wakes reach
/// the engine through a late-bound slot.
struct LateEngineInbox {
    store: EventStore,
    clock: Arc<dyn Clock>,
    engine: OnceLock<Arc<Engine<SuperAgentDriver>>>,
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

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.engine
            .get()
            .expect("engine installed before first wake")
            .wake(session)
    }
}

/// Assembly's `StreamPort` over the execution-hold handle (shape).
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

fn port_for_ctx() -> kian_superagent::PortFor<DrainCtx> {
    Arc::new(|ctx: &DrainCtx| Arc::new(CtxPort(ctx.clone())) as Arc<dyn StreamPort>)
}

// ---------------------------------------------------------------------------
// The assembled resident-Router driver
// ---------------------------------------------------------------------------

struct SuperAgentDriver {
    router: SessionId,
    router_side: RouterDriver<ReActLoop>,
    other_side: ReActLoop,
}

impl LoopDriver for SuperAgentDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        if ctx.session() == self.router {
            self.router_side.drive(ctx)
        } else {
            self.other_side.drive(ctx)
        }
    }
}

struct Harness {
    store: EventStore,
    engine: Arc<Engine<SuperAgentDriver>>,
    router: SessionId,
    script: Arc<CountingProvider>,
    router_inbox: Arc<RouterInbox>,
    metrics: Option<Arc<RouterMetrics>>,
    clock: Arc<ManualTurnClock>,
    gate_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

/// The manual clock's frozen epoch; all durations are deltas from it.
const T0: u64 = 1_000_000;

#[allow(clippy::too_many_arguments)]
async fn build_harness(
    router_turns: Vec<ScriptedChunks>,
    metrics_on: bool,
    gate_on_call: usize,
    gate_rx: Option<tokio::sync::oneshot::Receiver<()>>,
    gate_tx: Option<tokio::sync::oneshot::Sender<()>>,
) -> Harness {
    let store = temp_store().await;
    let router = SessionId::new();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    let router_agent = router_template("mock", "mock-router");
    let worker_agent = worker_template("mock", "mock-worker", 7);
    let profile = RouterProfile::new(router, router_agent.clone());

    let script = Arc::new(CountingProvider::with_gate(
        router_turns,
        gate_on_call,
        gate_rx,
    ));
    let worker_script = Arc::new(PlainProvider {
        turns: Mutex::new(vec![vec![text_chunk("unused worker turn")]].into()),
    });

    let mut router_registry = ProviderRegistry::new();
    router_registry.register("mock", Arc::clone(&script) as Arc<dyn ModelProvider>);
    let mut worker_registry = ProviderRegistry::new();
    worker_registry.register("mock", Arc::clone(&worker_script) as Arc<dyn ModelProvider>);

    let late = Arc::new(LateEngineInbox {
        store: store.clone(),
        clock: clock.clone(),
        engine: OnceLock::new(),
    });
    let router_inbox = RouterInbox::new(
        late.clone() as Arc<dyn InboxWriter>,
        kian_superagent::ResidentBinding::new(router),
    );

    let spawner = Arc::new(WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_agent,
        router_inbox.clone() as Arc<dyn InboxWriter>,
    ));
    let mock = Arc::new(MockChannel::new("mock-im"));
    let mut channels = ChannelRegistry::new();
    channels.register("mock-im", Arc::clone(&mock) as Arc<dyn ChannelAdapter>);
    let hold = RouterHold::shared();
    let tools = Arc::new(RouterTools::new(
        spawner,
        channels,
        &profile,
        hold.clone(),
        kian_superagent::ResidentBinding::new(profile.session),
    ));

    let router_loop = Arc::new(ReActLoop::with_config(
        router_agent,
        Arc::new(router_registry),
        tools,
        Arc::new(NoopPolicy),
        ReActConfig {
            slot: kian_core::model::SlotName("router".into()),
            max_iterations: 10_000,
            budget_scope: kian_loop::BudgetScope::Lifetime,
            vision: kian_loop::VisionBudget::default(),
            max_request_bytes: Default::default(),
            permissions: std::collections::BTreeMap::new(),
        },
    ));
    let other_loop = Arc::new(ReActLoop::with_config(
        worker_agent,
        Arc::new(worker_registry),
        Arc::new(FakeTools),
        Arc::new(NoopPolicy),
        ReActConfig::default(),
    ));

    let metrics = RouterMetrics::shared();
    let turn_clock = Arc::new(ManualTurnClock::new(T0));
    let router_side = if metrics_on {
        RouterDriver::with_park_tick_and_metrics(
            router_loop,
            hold,
            port_for_ctx(),
            router_inbox.clone(),
            Duration::from_millis(10),
            Some(RouterTurnRecorder::new(
                Arc::clone(&metrics),
                Arc::clone(&turn_clock) as Arc<dyn kian_superagent::TurnClock>,
            )),
        )
    } else {
        RouterDriver::with_park_tick(
            router_loop,
            hold,
            port_for_ctx(),
            router_inbox.clone(),
            Duration::from_millis(10),
        )
    };

    let engine = Engine::with_clock(
        store.clone(),
        Arc::new(SuperAgentDriver {
            router,
            router_side,
            other_side: ReActLoop::clone(&other_loop),
        }),
        EngineConfig::default(),
        clock,
    );
    assert!(late.engine.set(Arc::clone(&engine)).is_ok());

    Harness {
        store,
        engine,
        router,
        script,
        router_inbox,
        metrics: metrics_on.then_some(metrics),
        clock: turn_clock,
        gate_tx,
    }
}

fn channel_event_input(event_id: &str, text: &str) -> Value {
    channel_input_payload(&ChannelEvent {
        channel: "mock-im".into(),
        event_id: event_id.into(),
        topic: None,
        sender: None,
        group: None,
        payload: json!({"text": text}),
        ts_ms: 42,
    })
}

/// Admits `ids` as durable rows with ONE residency signal (the whole
/// cohort commits before the Router's admit counter moves — a parked
/// Router can never observe a torn cohort), then wakes the Router once.
async fn admit_and_wake(h: &Harness, ids: &[&str]) {
    let rows = ids
        .iter()
        .map(|id| {
            (
                channel_event_input(id, &format!("event {id}")),
                Some((*id).into()),
            )
        })
        .collect();
    h.router_inbox
        .admit_cohort(h.router, rows)
        .await
        .expect("admit cohort");
    h.engine.wake(h.router);
}

/// Polls until the metrics have recorded `n` judge turns.
async fn wait_for_turns(metrics: &RouterMetrics, n: u64) {
    for _ in 0..800 {
        if metrics.judge_turns() >= n {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("metrics never reached {n} judge turns");
}

// ---------------------------------------------------------------------------
// Named acceptance tests
// ---------------------------------------------------------------------------

/// N events admitted together → ONE judge turn records batch=N with
/// duration/N amortized — ManualClock-deterministic: the turn's start
/// (t0) precedes the gated provider call, the clock advances by exactly
/// 3000 ms while gated, and the record closes after the release.
#[tokio::test]
async fn n_events_admitted_together_one_judge_turn_batch_n_amortized_deterministic() {
    let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
    let mut h = build_harness(
        vec![
            vec![text_chunk("bootstrap acknowledged")],
            vec![text_chunk("batch judged")],
        ],
        true,
        2,
        Some(gate_rx),
        Some(gate_tx),
    )
    .await;

    // Bootstrap judge turn (ungated, frozen clock ⇒ duration 0).
    let profile = RouterProfile::new(h.router, router_template("mock", "mock-router"));
    profile
        .bootstrap(h.router_inbox.clone().as_ref())
        .await
        .unwrap();
    h.engine.wake(h.router);
    wait_for_events(&h.store, h.router, "bootstrap judge turn", |events| {
        count_kind(events, EventKind::TurnStart) == 1
            && count_kind(events, EventKind::AssistantMessage) == 1
    })
    .await;
    let metrics = h.metrics.as_ref().expect("metrics mounted");
    wait_for_turns(metrics, 1).await;

    // Three channel events admitted TOGETHER, one wake: the parked
    // Router re-drives and its opening sweep consumes all three — the
    // gated provider call pins the turn open while we advance time.
    admit_and_wake(&h, &["evt-1", "evt-2", "evt-3"]).await;
    h.script.wait_for_calls(2).await;

    // The clock moves only here, inside the open turn's interval.
    h.clock.advance(3_000);
    h.gate_tx.take().expect("gate sender").send(()).unwrap();

    wait_for_events(&h.store, h.router, "batch judge turn settled", |events| {
        count_kind(events, EventKind::InputAdmitted) == 4
            && count_kind(events, EventKind::AssistantMessage) == 2
    })
    .await;
    wait_for_turns(metrics, 2).await;

    let snap = metrics.snapshot();
    assert_eq!(snap["judge_turns"], json!(2), "one turn per sweep");
    assert_eq!(snap["inputs_consumed"], json!(4));
    assert_eq!(
        snap["last_turn"]["batch"],
        json!(3),
        "the batch turn recorded batch=N: {snap}"
    );
    assert_eq!(
        snap["last_turn"]["duration_ms"],
        json!(3_000),
        "exactly the manual advance: {snap}"
    );
    assert_eq!(snap["last_turn"]["amortized_ms"], json!(1_000.0));
    // §7.2 aggregates: total duration only moved inside the batch turn.
    assert_eq!(snap["turn_duration_ms_total"], json!(3_000));
    assert_eq!(snap["mean_turn_duration_ms"], json!(1_500.0));
    assert_eq!(snap["amortized_ms_per_input"], json!(750.0));
    // Histogram: bootstrap lands in "1", the batch of 3 in "4".
    assert_eq!(snap["batch_histogram"]["1"], json!(1));
    assert_eq!(snap["batch_histogram"]["4"], json!(1));
    assert_eq!(snap["batch_histogram"]["2"], json!(0));

    // Exactly two provider requests: bootstrap judge + batch judge —
    // recording did not add provider behavior. Structural:
    // the cohort admit advances the Router's residency counter in one
    // atomic jump after every row commits, so the parked Router cannot
    // observe a torn cohort and fold a second batch into the same
    // judge turn; and a no-work re-drive parks before opening a turn.
    assert_eq!(h.script.request_count(), 2);
}

/// N events across separate drains → N judge turns of batch 1.
#[tokio::test]
async fn n_events_across_separate_drains_record_n_turns_of_batch_one() {
    let h = build_harness(
        vec![
            vec![text_chunk("bootstrap acknowledged")],
            vec![text_chunk("one")],
            vec![text_chunk("two")],
            vec![text_chunk("three")],
        ],
        true,
        usize::MAX,
        None,
        None,
    )
    .await;

    let profile = RouterProfile::new(h.router, router_template("mock", "mock-router"));
    profile
        .bootstrap(h.router_inbox.clone().as_ref())
        .await
        .unwrap();
    h.engine.wake(h.router);
    wait_for_events(&h.store, h.router, "bootstrap judge turn", |events| {
        count_kind(events, EventKind::AssistantMessage) == 1
    })
    .await;
    let metrics = h.metrics.as_ref().expect("metrics mounted");
    wait_for_turns(metrics, 1).await;

    // One event per drain, each fully judged before the next admits.
    for (i, id) in ["evt-1", "evt-2", "evt-3"].iter().enumerate() {
        admit_and_wake(&h, &[id]).await;
        let expected = i + 2;
        wait_for_events(&h.store, h.router, "one event judged", |events| {
            count_kind(events, EventKind::InputAdmitted) == expected
                && count_kind(events, EventKind::AssistantMessage) == expected
        })
        .await;
        wait_for_turns(metrics, expected as u64).await;
    }

    let snap = metrics.snapshot();
    assert_eq!(snap["judge_turns"], json!(4), "bootstrap + three drains");
    assert_eq!(snap["inputs_consumed"], json!(4));
    assert_eq!(snap["batch_histogram"]["1"], json!(4));
    assert_eq!(snap["batch_histogram"]["2"], json!(0));
    assert_eq!(snap["last_turn"]["batch"], json!(1));
    // One provider request per judge turn — batches never merged.
    assert_eq!(h.script.request_count(), 4);
}

/// Zero provider behavior change: the identical scripted workload runs
/// the same provider request count and the same committed stream shape
/// with metrics off and with metrics on (which also records it).
#[tokio::test]
async fn metrics_on_and_off_produce_identical_provider_behavior() {
    fn turns() -> Vec<ScriptedChunks> {
        vec![
            vec![text_chunk("bootstrap acknowledged")],
            vec![text_chunk("one")],
            vec![text_chunk("two")],
        ]
    }

    // Metrics OFF: the exact legacy wiring (plain with_park_tick).
    let off = build_harness(turns(), false, usize::MAX, None, None).await;
    let profile = RouterProfile::new(off.router, router_template("mock", "mock-router"));
    profile
        .bootstrap(off.router_inbox.clone().as_ref())
        .await
        .unwrap();
    off.engine.wake(off.router);
    wait_for_events(&off.store, off.router, "bootstrap", |events| {
        count_kind(events, EventKind::AssistantMessage) == 1
    })
    .await;
    for (i, id) in ["evt-1", "evt-2"].iter().enumerate() {
        admit_and_wake(&off, &[id]).await;
        let expected = i + 2;
        wait_for_events(&off.store, off.router, "one event judged", |events| {
            count_kind(events, EventKind::InputAdmitted) == expected
                && count_kind(events, EventKind::AssistantMessage) == expected
        })
        .await;
    }
    let off_events = off.store.rebuild(off.router).await.unwrap();

    // Metrics ON: same scripts, same admission sequence.
    let on = build_harness(turns(), true, usize::MAX, None, None).await;
    let profile = RouterProfile::new(on.router, router_template("mock", "mock-router"));
    profile
        .bootstrap(on.router_inbox.clone().as_ref())
        .await
        .unwrap();
    on.engine.wake(on.router);
    wait_for_events(&on.store, on.router, "bootstrap", |events| {
        count_kind(events, EventKind::AssistantMessage) == 1
    })
    .await;
    for (i, id) in ["evt-1", "evt-2"].iter().enumerate() {
        admit_and_wake(&on, &[id]).await;
        let expected = i + 2;
        wait_for_events(&on.store, on.router, "one event judged", |events| {
            count_kind(events, EventKind::InputAdmitted) == expected
                && count_kind(events, EventKind::AssistantMessage) == expected
        })
        .await;
    }
    let on_events = on.store.rebuild(on.router).await.unwrap();

    // Identical provider request counts and stream shapes.
    assert_eq!(off.script.request_count(), on.script.request_count());
    assert_eq!(on.script.request_count(), 3);
    let shape = |events: &[Event]| events.iter().map(|e| e.kind).collect::<Vec<_>>();
    assert_eq!(shape(&off_events), shape(&on_events));

    // And the metrics-on run recorded exactly what happened: three
    // turns of batch 1 each — observability, not behavior.
    let metrics = on.metrics.as_ref().expect("metrics mounted");
    wait_for_turns(metrics, 3).await;
    let snap = metrics.snapshot();
    assert_eq!(snap["judge_turns"], json!(3));
    assert_eq!(snap["inputs_consumed"], json!(3));
    assert_eq!(snap["batch_histogram"]["1"], json!(3));
}
