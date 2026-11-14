//! Integration tests: the recall toolpack over a real
//! sqlite `EventStore` and a real `Engine` — the Chain of Recall.
//! ScriptedProvider on both sides, MockChannel as the IM stand-in: no
//! credentials, no network egress (nothing for a system proxy to
//! intercept).
//!
//! The assembly surface is exactly what kian-cli mounts: the
//! `RecallPort` over the store's public scan (with the disposable
//! spill index), `RecallTools` beside the Router dispatch tools, the
//! resident Router under `RouterDriver`, spawned workers under
//! `WorkerLoopDriver`. The built-in loop is unmodified — recall
//! results land as ordinary tail `tool/result` events.

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
use kian_orchestrator::{Clock, DrainCtx, Engine, EngineConfig, SystemClock, WakeOutcome};
use kian_store::EventStore;
use kian_superagent::{
    AdmitReceipt, ChannelRegistry, InboxWriteError, InboxWriter, MockChannel, PortFor, RecallPort,
    RecallPortError, RecallToolConfig, RecallTools, RouterDriver, RouterHold, RouterInbox,
    RouterProfile, RouterTools, ShapingConfig, SpillIndex, StreamPort, StreamPortError,
    WorkerLoopConfig, WorkerLoopDriver, WorkerSpawner, channel_input_payload, shape_channel_input,
    thread_bindings, worker_sessions,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

async fn temp_store() -> EventStore {
    let path = std::env::temp_dir().join(format!("kian-superagent-c35-{}.sqlite", Uuid::new_v4()));
    let store = EventStore::open(&path).await.expect("open store");
    std::mem::forget(path);
    store
}

/// Polls a session's committed stream until `pred` holds (bounded,
/// generous — never tight enough to flake). Returns the stream.
async fn wait_for_events(
    store: &EventStore,
    session: SessionId,
    desc: &str,
    pred: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    for _ in 0..800 {
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
// ScriptedProvider (both sides — no credentials, no egress)
// ---------------------------------------------------------------------------

type ScriptedChunks = Vec<Result<ModelChunk, kian_core::seams::ProviderError>>;
type DynamicTurn = Arc<dyn Fn(&ModelRequest) -> ScriptedChunks + Send + Sync>;

/// One scripted provider turn: fixed chunks, or a closure that builds
/// the turn from the request body it is answering (the two-hop test
/// extracts the runtime-minted thread id this way).
enum ScriptedTurn {
    Fixed(ScriptedChunks),
    Dynamic(DynamicTurn),
}

/// A `ModelProvider` that replays scripted turns in order, records
/// every request body, and supports request-derived turns.
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

    /// Appends one dynamic turn (built from the request body).
    fn push_dynamic(&self, turn: DynamicTurn) {
        self.turns
            .lock()
            .unwrap()
            .push_back(ScriptedTurn::Dynamic(turn));
    }

    fn request_count(&self) -> usize {
        self.call_count.load(std::sync::atomic::Ordering::SeqCst)
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

/// Extracts the `output.thread_id` of a prior tool result from the
/// request body. Dialect v2: a `role:"tool"` message's content
/// is the serialized tool output itself (with the wire `tool_call_id`
/// beside it), so the thread id sits at `/thread_id`; the legacy v1
/// shape wrapped it as `{"call_id","output"}` — both accepted here.
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
        let tid = content
            .pointer("/thread_id")
            .or_else(|| content.pointer("/output/thread_id"))
            .and_then(Value::as_str);
        if let Some(tid) = tid {
            return Some(tid.to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tools / policy fixtures
// ---------------------------------------------------------------------------

/// The worker side's tool provider (the full worker surface, every
/// call succeeding — stream shape is what matters here).
struct FakeTools;

impl ToolProvider for FakeTools {
    fn schemas(&self) -> Vec<ToolSchema> {
        ["fs_read", "fs_write", "exec"]
            .iter()
            .map(|name| ToolSchema {
                name: (*name).into(),
                description: format!("test {name}"),
                parameters: json!({"type": "object", "properties": {}}),
            })
            .collect()
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        Box::pin(async move {
            Ok(ToolResult {
                call_id: call.id,
                output: json!({"ok": true, "stdout": "hi"}),
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

/// First-wins tool composition (the kian-cli assembly shape, locally).
struct Combined(Vec<Arc<dyn ToolProvider>>);

impl ToolProvider for Combined {
    fn schemas(&self) -> Vec<ToolSchema> {
        let mut seen = std::collections::BTreeSet::new();
        let mut out = Vec::new();
        for provider in &self.0 {
            for schema in provider.schemas() {
                if seen.insert(schema.name.clone()) {
                    out.push(schema);
                }
            }
        }
        out
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        for provider in &self.0 {
            if provider.schemas().iter().any(|s| s.name == call.name) {
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
// Assembly surface: InboxWriter + StreamPort + RecallPort
// ---------------------------------------------------------------------------

/// The cycle breaker: admits are durable store writes; wakes reach the
/// engine through a late-bound slot.
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

/// Assembly's `StreamPort` over the execution-hold handle.
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

/// The test's `RecallPort`: reads over the store's public scan plus
/// the disposable spill index — exactly the kian-cli `StoreRecall`
/// shape. `rebuild` repopulates the index the way startup does.
struct StoreRecallPort {
    store: EventStore,
    spill: Mutex<SpillIndex>,
}

impl StoreRecallPort {
    fn new(store: EventStore) -> Arc<Self> {
        Arc::new(Self {
            store,
            spill: Mutex::new(SpillIndex::new()),
        })
    }

    /// The startup-rebuild shape: scan the Router stream and every
    /// worker it names, fold the `input/spilled` events into the index.
    async fn rebuild(&self, router: SessionId) {
        let mut events = self.store.rebuild(router).await.expect("router rebuild");
        for worker in worker_sessions(&events) {
            events.extend(self.store.rebuild(worker).await.expect("worker rebuild"));
        }
        *self.spill.lock().unwrap() = SpillIndex::rebuild(&events);
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
        event_id: &str,
    ) -> BoxFuture<'static, Result<Option<(SessionId, u64)>, RecallPortError>> {
        let located = self.spill.lock().unwrap().lookup(event_id);
        Box::pin(async move { Ok(located) })
    }
}

// ---------------------------------------------------------------------------
// The assembled super-agent driver (router side + worker side)
// ---------------------------------------------------------------------------

struct SuperAgentDriver {
    router: SessionId,
    router_side: RouterDriver<ReActLoop>,
    worker_side: WorkerLoopDriver<ReActLoop>,
}

impl LoopDriver for SuperAgentDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        if ctx.session() == self.router {
            self.router_side.drive(ctx)
        } else {
            self.worker_side.drive(ctx)
        }
    }
}

struct Harness {
    store: EventStore,
    engine: Arc<Engine<SuperAgentDriver>>,
    router: SessionId,
    router_script: Arc<ScriptedProvider>,
    worker_script: Arc<ScriptedProvider>,
    router_inbox: Arc<RouterInbox>,
    recall: Arc<StoreRecallPort>,
}

/// The shaping used by the spill tests: a tiny admission cap so
/// every fixture raw is over-cap (Inline view + spilled verbatim raw).
fn spill_shaping() -> ShapingConfig {
    ShapingConfig {
        inline_cap_bytes: Some(32),
        first_chars: 24,
        neighbor_fields: vec!["reply_to".into()],
    }
}

async fn build_harness(
    router_turns: Vec<ScriptedChunks>,
    worker_turns: Vec<ScriptedChunks>,
    shaping: ShapingConfig,
    recall_config: RecallToolConfig,
) -> Harness {
    let store = temp_store().await;
    let router = SessionId::new();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    let router_agent = kian_superagent::router_template("mock", "mock-router");
    let worker_agent = kian_superagent::worker_template("mock", "mock-worker", 7);
    let profile = RouterProfile::new(router, router_agent.clone());

    let router_script = Arc::new(ScriptedProvider::new(router_turns));
    let worker_script = Arc::new(ScriptedProvider::new(worker_turns));

    let mut router_registry = ProviderRegistry::new();
    router_registry.register("mock", Arc::clone(&router_script) as Arc<dyn ModelProvider>);
    let mut worker_registry = ProviderRegistry::new();
    worker_registry.register("mock", Arc::clone(&worker_script) as Arc<dyn ModelProvider>);

    let late = Arc::new(LateEngineInbox {
        store: store.clone(),
        clock: clock.clone(),
        engine: OnceLock::new(),
    });
    let router_inbox = RouterInbox::new(late.clone() as Arc<dyn InboxWriter>, router);

    let spawner = Arc::new(WorkerSpawner::new(
        router,
        &worker_agent,
        router_inbox.clone() as Arc<dyn InboxWriter>,
    ));
    let mock = Arc::new(MockChannel::new("mock-im"));
    let mut channels = ChannelRegistry::new();
    channels.register("mock-im", Arc::clone(&mock) as Arc<dyn ChannelAdapter>);
    let hold = RouterHold::shared();
    let dispatch_tools = Arc::new(RouterTools::new(spawner, channels, &profile, hold.clone()));

    // The recall toolpack beside the dispatch tools, over the read
    // port — exactly how assembly mounts it (default on for Router).
    let recall = StoreRecallPort::new(store.clone());
    let recall_tools = Arc::new(RecallTools::new(
        recall.clone() as Arc<dyn RecallPort>,
        router,
        shaping,
        recall_config,
    ));
    let router_tools = Arc::new(Combined(vec![
        dispatch_tools as Arc<dyn ToolProvider>,
        recall_tools as Arc<dyn ToolProvider>,
    ]));

    let router_loop = Arc::new(ReActLoop::with_config(
        router_agent,
        Arc::new(router_registry),
        router_tools,
        Arc::new(NoopPolicy),
        ReActConfig {
            slot: kian_core::model::SlotName("router".into()),
            max_iterations: 10_000,
            budget_scope: kian_loop::BudgetScope::Lifetime,
            permissions: std::collections::BTreeMap::new(),
        },
    ));
    let worker_loop = Arc::new(ReActLoop::with_config(
        worker_agent,
        Arc::new(worker_registry),
        Arc::new(FakeTools),
        Arc::new(NoopPolicy),
        ReActConfig::default(),
    ));

    let router_side = RouterDriver::with_park_tick(
        router_loop,
        hold,
        port_for_ctx(),
        router_inbox.clone(),
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
        Arc::new(SuperAgentDriver {
            router,
            router_side,
            worker_side,
        }),
        EngineConfig::default(),
        clock,
    );
    assert!(late.engine.set(Arc::clone(&engine)).is_ok());

    Harness {
        store,
        engine,
        router,
        router_script,
        worker_script,
        router_inbox,
        recall,
    }
}

/// One over-cap channel event shaped at admission (the envelope:
/// Inline view in `content`, verbatim raw in the `spill` sidecar).
fn over_cap_event(event_id: &str, topic: Option<&str>, raw: Value) -> ChannelEvent {
    ChannelEvent {
        channel: "mock-im".into(),
        event_id: event_id.into(),
        topic: topic.map(str::to_string),
        sender: None,
        group: None,
        payload: raw,
        ts_ms: 42,
    }
}

/// The serialized bytes of one event (the prefix-stability currency).
fn event_bytes(events: &[Event]) -> Vec<(u64, EventKind, String)> {
    events
        .iter()
        .map(|ev| {
            (
                ev.seq,
                ev.kind,
                serde_json::to_string(&ev.bytes).expect("serialize event bytes"),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Named acceptance tests
// ---------------------------------------------------------------------------

/// Over-cap event + `recall_event` round-trip: the verbatim raw comes
/// back as an ordinary `tool/result` appended at the tail, after the
/// admitted/spilled pair — and the prior stream is byte-identical
/// (prefix-bytes assertion, discipline).
#[tokio::test]
async fn over_cap_event_recall_event_round_trip_returns_verbatim_raw_as_a_tail_tool_result() {
    let raw = json!({
        "text": "FULL_OVER_CAP_RAW_abcdefghijklmnopqrstuvwxyz_MUST_STAY_VERBATIM_IN_THE_SPILL",
        "reply_to": "msg-parent-7",
    });
    let event = over_cap_event("evt-over-cap", Some("room-9"), raw.clone());
    let envelope = shape_channel_input(&event, &spill_shaping());

    // built-in: admit the envelope (no wake yet), then one judge turn —
    // the drain consumes the pair and the Router parks.
    let h = build_harness(
        vec![
            vec![text_chunk("noted the inline view")],
            vec![tool_call_chunk(
                "call-recall",
                "recall_event",
                json!({"event_id": "evt-over-cap"}),
            )],
            vec![text_chunk("chain answered")],
        ],
        vec![vec![text_chunk("unused worker turn")]],
        spill_shaping(),
        RecallToolConfig::default(),
    )
    .await;
    h.engine
        .admit(h.router, envelope, Some("evt-over-cap".into()))
        .await
        .expect("admit envelope");

    let before = wait_for_events(&h.store, h.router, "pair + first judge turn", |events| {
        count_kind(events, EventKind::InputAdmitted) == 1
            && count_kind(events, EventKind::InputSpilled) == 1
            && count_kind(events, EventKind::AssistantMessage) == 1
    })
    .await;
    let before_bytes = event_bytes(&before);

    // The committed pair: Inline view in context, verbatim raw out of
    // context (the shape — re-verified here because the round trip
    // below must return exactly these raw bytes).
    let spilled = before
        .iter()
        .find(|e| e.kind == EventKind::InputSpilled)
        .expect("input/spilled");
    assert_eq!(spilled.bytes["raw"], raw, "the spilled raw is verbatim");
    let admitted = before
        .iter()
        .find(|e| e.kind == EventKind::InputAdmitted)
        .expect("input/admitted");
    assert!(admitted.bytes["content"]["kian"].get("raw").is_none());

    // bundle: a follow-up input drives the recall turn. The spill
    // index is deliberately EMPTY (fresh spills are not in it — the
    // startup rebuild predates them): the tool finds the raw through
    // the scan fallback over the stream, the sole truth. The admit
    // goes through the mounted Router inbox — the residency counter
    // that re-drives a parked Router.
    h.router_inbox
        .admit(
            h.router,
            json!("what was in that big event?"),
            Some("follow-up".into()),
        )
        .await
        .expect("admit follow-up");

    let after = wait_for_events(&h.store, h.router, "recall tool result + final", |events| {
        count_kind(events, EventKind::ToolResult) == 1
            && count_kind(events, EventKind::AssistantMessage) == 3
    })
    .await;

    // The prefix-bytes assertion: every event that existed before the
    // recall turn is byte-identical after it (constraint 3 — nothing
    // rewrites, reorders or reformats prior context).
    let after_bytes = event_bytes(&after);
    assert_eq!(
        after_bytes[..before_bytes.len()],
        before_bytes[..],
        "the pre-recall stream must be a byte-identical prefix"
    );

    // The tool/result is a tail event after the pair and carries the
    // verbatim raw (the round trip: what admission shaped away, recall
    // brings back byte-for-byte).
    let pair_end = after
        .iter()
        .position(|e| e.kind == EventKind::InputSpilled)
        .unwrap();
    let result_pos = after
        .iter()
        .position(|e| e.kind == EventKind::ToolResult)
        .expect("tool/result");
    assert!(result_pos > pair_end, "the result lands after the pair");

    let result = &after[result_pos];
    assert_eq!(result.bytes["content"]["is_error"], json!(false));
    let output = &result.bytes["content"]["output"];
    assert_eq!(output["event_id"], json!("evt-over-cap"));
    assert_eq!(output["channel"], json!("mock-im"));
    assert_eq!(output["topic"], json!("room-9"));
    assert_eq!(output["raw"], raw, "the verbatim raw round-trips");
    assert_eq!(
        output["next_hops"]["neighbors"],
        json!({"tool": "recall_neighbors", "event_id": "evt-over-cap"})
    );
}

/// The two-hop chain, end to end with ScriptedProvider: a channel
/// trigger makes the Router dispatch a worker; the worker completes
/// and reports; the Router's next judge turn recalls the thread and
/// sees the worker's final summary, with the worker session as the
/// next hop.
#[tokio::test]
async fn two_hop_chain_trigger_to_recall_thread_reaches_the_worker_summary() {
    let h = build_harness(
        vec![vec![tool_call_chunk(
            "call-spawn",
            "spawn_worker",
            json!({"task": "summarize the flaky test"}),
        )]],
        vec![vec![text_chunk(
            "the flaky test was a race; fixed with a bounded retry",
        )]],
        ShapingConfig::default(),
        RecallToolConfig::default(),
    )
    .await;

    // No hook needed: the ordering is structural. The Router parks
    // after the spawn turn (an outstanding thread, no new input); the
    // worker's completion report is the admit that advances the
    // residency counter, and `thread/completed` is committed *before*
    // that admit — so by the time the recall turn's request exists,
    // the thread view is complete. The recall turn itself is built
    // from the request body: the thread id is minted at runtime, so
    // the turn reads it out of the spawn tool's prior result.
    let router_script = Arc::clone(&h.router_script);
    router_script.push_dynamic(Arc::new(|req: &ModelRequest| {
        let thread_id =
            thread_id_from_request(req).expect("the spawn tool result carries the thread id");
        vec![tool_call_chunk(
            "call-recall-thread",
            "recall_thread",
            json!({"thread_id": thread_id}),
        )]
    }));
    router_script.push_dynamic(Arc::new(|_req: &ModelRequest| {
        vec![text_chunk("recall chain complete")]
    }));

    // Hop 1 — the trigger: one channel event wakes the resident
    // Router, whose judge turn dispatches the worker.
    let trigger = ChannelEvent {
        channel: "mock-im".into(),
        event_id: "evt-trigger".into(),
        topic: None,
        sender: None,
        group: None,
        payload: json!({"text": "the flaky test again, please summarize"}),
        ts_ms: 42,
    };
    h.engine
        .admit(
            h.router,
            channel_input_payload(&trigger),
            Some("evt-trigger".into()),
        )
        .await
        .expect("admit trigger");

    // Hop 2 — recall_thread returns the worker's summary.
    let events = wait_for_events(
        &h.store,
        h.router,
        "spawn + recall tool results",
        |events| {
            count_kind(events, EventKind::ToolResult) == 2
                && count_kind(events, EventKind::AssistantMessage) == 3
        },
    )
    .await;

    let started = events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .expect("thread/started on the router stream");
    let worker =
        SessionId(Uuid::parse_str(started.bytes["worker"].as_str().unwrap()).expect("worker uuid"));
    let thread_id = started.bytes["thread_id"].as_str().unwrap().to_string();

    let recall_result = events
        .iter()
        .find(|e| {
            e.kind == EventKind::ToolResult
                && e.bytes["content"]["call_id"] == json!("call-recall-thread")
        })
        .expect("recall_thread tool/result");
    assert_eq!(recall_result.bytes["content"]["is_error"], json!(false));
    let output = &recall_result.bytes["content"]["output"];
    assert_eq!(output["thread_id"], json!(thread_id));
    assert_eq!(output["status"], json!("completed"));
    assert_eq!(
        output["summary"],
        json!({"text": "the flaky test was a race; fixed with a bounded retry"}),
        "the worker's final summary is the recall answer"
    );
    assert!(
        serde_json::to_string(&output["task"])
            .unwrap()
            .contains("summarize the flaky test"),
        "the delegated task rides the thread view"
    );
    assert_eq!(
        output["next_hops"]["worker_session"],
        json!(worker.0.to_string()),
        "the worker session is the next hop"
    );

    // The worker's own stream audited the completion the recall read.
    let worker_events = h.store.rebuild(worker).await.unwrap();
    assert!(
        worker_events
            .iter()
            .any(|e| e.kind == EventKind::ThreadCompleted)
    );
    assert_eq!(h.worker_script.request_count(), 1);
}

/// Recall of an unknown id is an honest error result — the model sees
/// the failure and reacts; never silence, never a fabricated hit.
#[tokio::test]
async fn recall_of_an_unknown_id_is_an_honest_error_result_never_silence() {
    let h = build_harness(
        vec![
            vec![tool_call_chunk(
                "call-ghost",
                "recall_event",
                json!({"event_id": "evt-no-such-raw"}),
            )],
            vec![text_chunk("that event never spilled; moving on")],
        ],
        vec![vec![text_chunk("unused worker turn")]],
        ShapingConfig::default(),
        RecallToolConfig::default(),
    )
    .await;

    let trigger = ChannelEvent {
        channel: "mock-im".into(),
        event_id: "evt-plain".into(),
        topic: None,
        sender: None,
        group: None,
        payload: json!({"text": "recall something that never spilled"}),
        ts_ms: 42,
    };
    h.engine
        .admit(
            h.router,
            channel_input_payload(&trigger),
            Some("evt-plain".into()),
        )
        .await
        .expect("admit trigger");

    let events = wait_for_events(
        &h.store,
        h.router,
        "honest error result + final",
        |events| {
            count_kind(events, EventKind::ToolResult) == 1
                && count_kind(events, EventKind::AssistantMessage) == 2
        },
    )
    .await;

    let result = events
        .iter()
        .find(|e| e.kind == EventKind::ToolResult)
        .expect("a result exists — the failure is never silent");
    assert_eq!(result.bytes["content"]["is_error"], json!(true));
    let err = result.bytes["content"]["output"]["error"].as_str().unwrap();
    assert!(
        err.contains("unknown event id `evt-no-such-raw`"),
        "error was: {err}"
    );
    // Honesty, not abort: the loop continued to a final answer.
    assert_eq!(h.router_script.request_count(), 2);
}

/// Recall outputs are capped: a raw larger than the toolpack's output
/// cap surfaces as the Inline view (excerpt + continuation), never as
/// an unbounded re-injection. The cap applies to tool outputs too.
#[tokio::test]
async fn recall_outputs_are_capped_no_unbounded_raw_reinjection() {
    let marker = "HUGE_RAW_0123456789abcdefghijklmnopqrstuvwxyz_".repeat(8);
    let raw = json!({"text": marker, "reply_to": "msg-7"});
    let serialized = serde_json::to_string(&raw).unwrap();
    let event = over_cap_event("evt-huge", Some("room-9"), raw.clone());

    let h = build_harness(
        vec![vec![text_chunk("consumed the pair")]],
        vec![vec![text_chunk("unused worker turn")]],
        spill_shaping(),
        RecallToolConfig {
            output_cap_bytes: 64,
            neighbor_window: 3,
        },
    )
    .await;
    h.engine
        .admit(
            h.router,
            shape_channel_input(&event, &spill_shaping()),
            Some("evt-huge".into()),
        )
        .await
        .expect("admit envelope");
    wait_for_events(&h.store, h.router, "pair committed", |events| {
        count_kind(events, EventKind::InputSpilled) == 1
    })
    .await;

    // Direct invocation over the read port (the loop path is covered
    // by the round-trip test; the cap is a property of the output).
    let tools = RecallTools::new(
        h.recall.clone() as Arc<dyn RecallPort>,
        h.router,
        spill_shaping(),
        RecallToolConfig {
            output_cap_bytes: 64,
            neighbor_window: 3,
        },
    );
    let result = tools
        .invoke(ToolCall {
            id: "c1".into(),
            name: "recall_event".into(),
            arguments: json!({"event_id": "evt-huge"}),
        })
        .await
        .expect("recall_event succeeds");
    assert!(!result.is_error);

    let output = &result.output;
    assert!(
        output.get("raw").is_none(),
        "an over-cap raw must not ride verbatim: {output}"
    );
    assert_eq!(output["spill"], json!(true));
    assert_eq!(output["continuation"], json!({"event_id": "evt-huge"}));
    let excerpt = output["inline"]["excerpt"].as_str().unwrap();
    assert!(serialized.starts_with(excerpt));
    let output_len = serde_json::to_string(output).unwrap().len();
    assert!(
        output_len < serialized.len(),
        "the capped output ({output_len}) must be smaller than the raw ({})",
        serialized.len()
    );
    assert!(!serde_json::to_string(output).unwrap().contains(&marker));
}

/// The locality view: adjacent channel events on the same topic within
/// the window, each Inline-shaped with its event id as the next hop —
/// and events of other topics stay out. This test also exercises the
/// INDEXED location path (the spill index rebuilt the way startup
/// does); the round-trip test above exercises the scan fallback.
#[tokio::test]
async fn recall_neighbors_returns_the_same_topic_locality_view() {
    let raw_a = json!({"text": "neighbor A about the flaky test"});
    let raw_b = json!({"text": "neighbor B about the flaky test", "pad": "0123456789abcdef"});
    let raw_other = json!({"text": "different topic entirely"});

    let h = build_harness(
        vec![vec![text_chunk("consumed the batch")]],
        vec![vec![text_chunk("unused worker turn")]],
        spill_shaping(),
        // A window wide enough to span the whole four-row sweep: the
        // consumption order of one batch is not the admit order, so
        // the assertion below compares the neighbor SET, not order.
        RecallToolConfig {
            output_cap_bytes: 8_192,
            neighbor_window: 16,
        },
    )
    .await;
    // One sweep consumes the whole batch: three same-topic over-cap
    // events (A, anchor, B in unknown sweep order) plus one
    // other-topic event.
    let rows = [
        over_cap_event("evt-a", Some("room-9"), raw_a.clone()),
        over_cap_event(
            "evt-anchor",
            Some("room-9"),
            json!({
                "text": "the anchor event of room-9", "reply_to": "msg-1",
            }),
        ),
        over_cap_event("evt-b", Some("room-9"), raw_b.clone()),
        over_cap_event("evt-other", Some("room-11"), raw_other.clone()),
    ];
    for ev in &rows {
        let payload = shape_channel_input(ev, &spill_shaping());
        h.engine
            .admit(h.router, payload, Some(ev.event_id.clone()))
            .await
            .expect("admit");
    }
    wait_for_events(&h.store, h.router, "all four pairs committed", |events| {
        count_kind(events, EventKind::InputSpilled) == 4
    })
    .await;
    // The startup-rebuild shape: the index now knows every location.
    h.recall.rebuild(h.router).await;

    let tools = RecallTools::new(
        h.recall.clone() as Arc<dyn RecallPort>,
        h.router,
        spill_shaping(),
        RecallToolConfig {
            output_cap_bytes: 8_192,
            neighbor_window: 16,
        },
    );
    let result = tools
        .invoke(ToolCall {
            id: "c1".into(),
            name: "recall_neighbors".into(),
            arguments: json!({"event_id": "evt-anchor"}),
        })
        .await
        .expect("recall_neighbors succeeds");
    assert!(!result.is_error);

    let output = &result.output;
    assert_eq!(output["event_id"], json!("evt-anchor"));
    assert_eq!(output["topic"], json!("room-9"));
    let neighbors = output["neighbors"].as_array().unwrap();
    let mut ids: Vec<&str> = neighbors
        .iter()
        .map(|n| n["event_id"].as_str().unwrap())
        .collect();
    ids.sort_unstable();
    // Same topic both sides, other topic excluded, anchor never lists
    // its own sibling admission.
    assert_eq!(ids, vec!["evt-a", "evt-b"]);
    // Each entry is Inline-shaped and carries its verbatim raw only
    // under the output cap.
    let by_id = |id: &str| {
        neighbors
            .iter()
            .find(|n| n["event_id"] == json!(id))
            .unwrap()
            .clone()
    };
    assert_eq!(by_id("evt-a")["view"], raw_a);
    assert_eq!(by_id("evt-b")["view"], raw_b);
    let mut hop_ids = output["next_hops"]["event_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    hop_ids.sort_unstable();
    assert_eq!(
        serde_json::json!({"tool": "recall_event", "event_ids": hop_ids}),
        output["next_hops"],
        "every neighbor id is a recall_event next hop"
    );
    assert!(!serde_json::to_string(output).unwrap().contains("evt-other"));
}

/// Tool schemas are stable-ordered prompt-prefix content (constraint
/// 3's gate): the serialization is a golden that does not change
/// across this change, and repeated calls yield identical bytes.
#[test]
fn recall_tool_schemas_are_stable_ordered_golden() {
    let tools = RecallTools::new(
        Arc::new(EmptyRecallPort) as Arc<dyn RecallPort>,
        SessionId::new(),
        ShapingConfig::default(),
        RecallToolConfig::default(),
    );
    let first = serde_json::to_string(&tools.schemas()).unwrap();
    let second = serde_json::to_string(&tools.schemas()).unwrap();
    assert_eq!(first, second, "repeated serialization is byte-identical");

    let names: Vec<String> = tools.schemas().iter().map(|s| s.name.clone()).collect();
    assert_eq!(
        names,
        vec!["recall_event", "recall_neighbors", "recall_thread"],
        "stable sorted order"
    );

    // The golden: any change here is a deliberate prefix-cache
    // invalidation (constraint 3) and must be flagged for review.
    let expected = concat!(
        r#"[{"name":"recall_event","description":"Recall one spilled channel event: its "#,
        r#"Inline/Spill-shaped raw (verbatim up to the output cap) plus next-hop ids (locality, "#,
        r#"topic).","parameters":{"properties":{"event_id":{"description":"The channel event id "#,
        r#"to recall (the continuation handle)","type":"string"}},"required":["event_id"],"#,
        r#""type":"object"}},{"name":"recall_neighbors","description":"Recall the locality view "#,
        r#"of one channel event: adjacent channel events on the same topic within a window, each "#,
        r#"Inline-shaped and capped, with their event ids as next hops.","parameters":"#,
        r#"{"properties":{"event_id":{"description":"The anchor channel event id","type":"string"}},"#,
        r#""required":["event_id"],"type":"object"}},{"name":"recall_thread","description":"Recall "#,
        r#"one delegated thread: its task, status (completed | failed | interrupted | outstanding) "#,
        r#"and final summary, with the worker session as the next hop.","parameters":"#,
        r#"{"properties":{"thread_id":{"description":"The thread id returned by spawn_worker","#,
        r#""type":"string"}},"required":["thread_id"],"type":"object"}}]"#,
    );
    assert_eq!(first, expected, "schema serialization golden changed");
}

/// A read port over nothing (the schema golden needs no store).
struct EmptyRecallPort;

impl RecallPort for EmptyRecallPort {
    fn read_events(
        &self,
        _session: SessionId,
        _since_seq: u64,
        _until_seq: u64,
    ) -> BoxFuture<'static, Result<Vec<Event>, RecallPortError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn list_threads(
        &self,
        _parent: SessionId,
    ) -> BoxFuture<'static, Result<Vec<(String, SessionId)>, RecallPortError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn spill_location(
        &self,
        _event_id: &str,
    ) -> BoxFuture<'static, Result<Option<(SessionId, u64)>, RecallPortError>> {
        Box::pin(async { Ok(None) })
    }
}
