//! Integration tests: the resident Router profile over a
//! real sqlite `EventStore` and a real `Engine` — batching, channel
//! output policies, subscription passthrough, and the structural
//! toolset diff. ScriptedProvider both sides, MockChannel as the IM
//! stand-in: no credentials, no network egress (nothing for a system
//! proxy to intercept).

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
    AdmitReceipt, ChannelRegistry, InboxWriteError, InboxWriter, MockChannel, RouterDriver,
    RouterHold, RouterInbox, RouterProfile, RouterTools, RuleMatcher, ShapingConfig, StreamPort,
    StreamPortError, SubscriptionRule, SubscriptionRuleSet, SubscriptionService, WorkerSpawner,
    channel_input_payload, router_template, worker_template,
};
use kian_superagent::{CorrelateBy, Route};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

async fn temp_store() -> EventStore {
    let path = std::env::temp_dir().join(format!("kian-superagent-c26-{}.sqlite", Uuid::new_v4()));
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

fn position_of(events: &[Event], pred: impl Fn(&Event) -> bool) -> Option<usize> {
    events.iter().position(pred)
}

// ---------------------------------------------------------------------------
// ScriptedProvider (both sides — no credentials, no egress)
// ---------------------------------------------------------------------------

type ScriptedChunks = Vec<Result<ModelChunk, kian_core::seams::ProviderError>>;

/// A `ModelProvider` that replays scripted turns in order and counts
/// requests (the single-judge-turn assertion).
struct ScriptedProvider {
    turns: Mutex<VecDeque<ScriptedChunks>>,
    call_count: std::sync::atomic::AtomicUsize,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ScriptedProvider {
    fn new(turns: Vec<ScriptedChunks>) -> Self {
        Self {
            turns: Mutex::new(turns.into()),
            call_count: std::sync::atomic::AtomicUsize::new(0),
            requests: Mutex::new(Vec::new()),
        }
    }

    fn request_count(&self) -> usize {
        self.call_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn requests(&self) -> Vec<ModelRequest> {
        self.requests.lock().unwrap().clone()
    }
}

impl ModelProvider for ScriptedProvider {
    fn complete(
        &self,
        req: ModelRequest,
    ) -> BoxStream<'static, Result<ModelChunk, kian_core::seams::ProviderError>> {
        self.requests.lock().unwrap().push(req);
        let n = self
            .call_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let _ = n;
        let turn = self.turns.lock().unwrap().pop_front().unwrap_or_default();
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

fn words(n: usize) -> String {
    (1..=n)
        .map(|i| format!("w{i}"))
        .collect::<Vec<_>>()
        .join(" ")
}

// ---------------------------------------------------------------------------
// Tools / policy fixtures
// ---------------------------------------------------------------------------

/// The worker side's tool provider (the full worker surface, every call
/// succeeding — stream shape is what matters here).
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

// ---------------------------------------------------------------------------
// Assembly surface: InboxWriter + StreamPort over the real engine
// ---------------------------------------------------------------------------

/// The cycle breaker (pattern): admits are durable store writes;
/// wakes reach the engine through a late-bound slot.
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

/// One engine, one driver: the resident Router session routes to the
/// `RouterDriver` (port install + residency parking over the built-in
/// ReAct loop with the dispatch tools); every other session routes to
/// the plain worker-side ReAct loop.
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
    router_script: Arc<ScriptedProvider>,
    worker_script: Arc<ScriptedProvider>,
    router_inbox: Arc<RouterInbox>,
    mock: Arc<MockChannel>,
}

async fn build_harness(
    router_turns: Vec<ScriptedChunks>,
    worker_turns: Vec<ScriptedChunks>,
) -> Harness {
    let store = temp_store().await;
    let router = SessionId::new();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);

    let router_agent = router_template("mock", "mock-router");
    let worker_agent = worker_template("mock", "mock-worker", 7);
    let profile = RouterProfile::new(router, router_agent.clone());

    let router_script = Arc::new(ScriptedProvider::new(router_turns));
    let worker_script = Arc::new(ScriptedProvider::new(worker_turns));

    let mut router_registry = ProviderRegistry::new();
    router_registry.register("mock", Arc::clone(&router_script) as Arc<dyn ModelProvider>);
    let mut worker_registry = ProviderRegistry::new();
    worker_registry.register("mock", Arc::clone(&worker_script) as Arc<dyn ModelProvider>);

    // The mounted inbox: admits counted for residency parking.
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
    let tools = Arc::new(RouterTools::new(spawner, channels, &profile, hold.clone()));

    let router_loop = Arc::new(ReActLoop::with_config(
        router_agent,
        Arc::new(router_registry),
        tools,
        Arc::new(NoopPolicy),
        ReActConfig {
            slot: kian_core::model::SlotName("router".into()),
            max_iterations: 10_000,
            budget_scope: kian_loop::BudgetScope::Lifetime,
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

    let router_side = RouterDriver::with_park_tick(
        router_loop,
        hold,
        port_for_ctx(),
        router_inbox.clone(),
        Duration::from_millis(10),
    );

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
        router_script,
        worker_script,
        router_inbox,
        mock,
    }
}

/// One channel-event input payload (the provenance wrapper).
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

/// Admits `n` channel events together as ONE residency signal (the
/// whole cohort commits before the Router's admit counter moves — a
/// parked Router can never observe a torn cohort;), no wakes,
/// then wakes the Router once — exactly "N channel events admitted
/// together".
async fn admit_batch_together(h: &Harness, ids: &[&str]) {
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

// ---------------------------------------------------------------------------
// Named acceptance tests
// ---------------------------------------------------------------------------

/// Engine-native batching (article §7.2 for free): N channel events
/// admitted together are consumed in **one** Router drain sweep with a
/// single judge turn over all N — and the resident Router survives its
/// first settle to judge the second batch (never freezes).
#[tokio::test]
async fn batch_admits_consumed_in_one_sweep_single_judge_turn() {
    let h = build_harness(
        vec![
            vec![text_chunk("bootstrap acknowledged")],
            vec![text_chunk("batch judged")],
        ],
        vec![vec![text_chunk("unused worker turn")]],
    )
    .await;

    // Bootstrap via the admit path (durable row, then one wake).
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

    // Three channel events admitted together, one wake.
    admit_batch_together(&h, &["evt-1", "evt-2", "evt-3"]).await;
    wait_for_events(&h.store, h.router, "second (batch) judge turn", |events| {
        count_kind(events, EventKind::InputAdmitted) == 4
            && count_kind(events, EventKind::TurnStart) == 2
            && count_kind(events, EventKind::AssistantMessage) == 2
    })
    .await;

    let events = h.store.rebuild(h.router).await.unwrap();

    // Single judge turn over all N: exactly two provider requests total
    // (bootstrap judge + batch judge) — one per sweep.
    assert_eq!(h.router_script.request_count(), 2);

    // The batch assertion: the three channel `input/admitted` events
    // are contiguous, land after the bootstrap turn, and all precede
    // the batch's single `turn/start`.
    let first_turn_end = position_of(&events, |e| e.kind == EventKind::TurnEnd).unwrap();
    let batch_positions: Vec<usize> = events
        .iter()
        .enumerate()
        .filter(|(i, e)| {
            e.kind == EventKind::InputAdmitted
                && *i > first_turn_end
                && e.bytes["content"]["kian"]["type"] == json!("channel_event")
        })
        .map(|(i, _)| i)
        .collect();
    assert_eq!(batch_positions.len(), 3, "all three events in one sweep");
    assert!(
        batch_positions[0] > first_turn_end,
        "batch lands after the bootstrap turn"
    );
    assert_eq!(
        batch_positions[2] - batch_positions[0],
        2,
        "the sweep's consumption events are contiguous"
    );
    let batch_turn_start = events
        .iter()
        .enumerate()
        .filter(|(i, e)| e.kind == EventKind::TurnStart && *i > batch_positions[2])
        .map(|(i, _)| i)
        .next()
        .expect("a turn/start after the batch");
    assert_eq!(count_kind(&events, EventKind::TurnStart), 2);

    // The single judge turn saw ALL N events in its request body
    // (the A2 audit copy on the stream).
    let request = &events[batch_turn_start..]
        .iter()
        .find(|e| e.kind == EventKind::StepRequest)
        .expect("step/request of the batch turn");
    let body = serde_json::to_string(&request.bytes["body"]).unwrap();
    for id in ["evt-1", "evt-2", "evt-3"] {
        assert!(
            body.contains(id),
            "the judge turn request must see `{id}`; body: {body}"
        );
    }
    assert_eq!(body.matches("channel_event").count(), 3);

    // Residency: the Router never settles — after the second judge
    // turn it parks, hold alive, no end write.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let events = h.store.rebuild(h.router).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
            .count(),
        0,
        "the resident Router never freezes on an idle final answer"
    );
    assert!(h.engine.is_draining(&h.router), "the hold stays alive");
    assert_eq!(
        h.engine.projection(h.router).await.unwrap(),
        kian_core::state::SessionState::Running
    );
}

/// Over-length reply: the §4.2 IM policy truncates at 100 words with
/// an ellipsis; `channel/delivered{words:100, truncated:true}` lands
/// after the send.
#[tokio::test]
async fn over_limit_reply_delivered_truncated_with_channel_delivered() {
    let over_limit = words(120);
    let h = build_harness(
        vec![
            vec![tool_call_chunk(
                "call-1",
                "reply",
                json!({"channel": "mock-im", "text": over_limit, "target": {"chat_id": 7}}),
            )],
            vec![text_chunk("replied")],
        ],
        vec![vec![text_chunk("unused worker turn")]],
    )
    .await;

    h.engine
        .admit(
            h.router,
            channel_event_input("evt-long", "please summarize"),
            Some("evt-long".into()),
        )
        .await
        .unwrap();

    wait_for_events(&h.store, h.router, "reply turn + final", |events| {
        count_kind(events, EventKind::TurnStart) == 2
            && count_kind(events, EventKind::AssistantMessage) == 2
    })
    .await;

    // The delivered text is truncated to ≤100 words with an ellipsis.
    let deliveries = h.mock.deliveries();
    assert_eq!(deliveries.len(), 1);
    let delivered = deliveries[0].delivery.content["text"].as_str().unwrap();
    assert_eq!(delivered.split_whitespace().count(), 100);
    assert!(delivered.ends_with('…'));
    assert!(!delivered.contains("w120"));

    // channel/delivered after the send, schema, honest truncation.
    let events = h.store.rebuild(h.router).await.unwrap();
    let delivered_ev = events
        .iter()
        .find(|e| e.kind == EventKind::ChannelDelivered)
        .expect("channel/delivered");
    assert_eq!(delivered_ev.bytes["channel"], json!("mock-im"));
    assert_eq!(delivered_ev.bytes["policy"], json!("im"));
    assert_eq!(delivered_ev.bytes["words"], json!(100));
    assert_eq!(delivered_ev.bytes["truncated"], json!(true));
    assert_eq!(delivered_ev.bytes["target"], json!({"chat_id": 7}));
    // The delivery event trails the successful send: a tool/result for
    // the reply call exists and is not an error.
    let tool_results: Vec<&Event> = events
        .iter()
        .filter(|e| e.kind == EventKind::ToolResult)
        .collect();
    assert_eq!(tool_results.len(), 1);
    assert_eq!(tool_results[0].bytes["content"]["is_error"], json!(false));
    assert_eq!(
        tool_results[0].bytes["content"]["output"]["words"],
        json!(100)
    );
}

/// Under-limit replies pass verbatim: no truncation, honest counts.
#[tokio::test]
async fn under_limit_reply_delivered_untruncated() {
    let short = "done: the flaky test was a race, fixed with a bounded retry.";
    let h = build_harness(
        vec![
            vec![tool_call_chunk(
                "call-1",
                "reply",
                json!({"channel": "mock-im", "text": short}),
            )],
            vec![text_chunk("replied")],
        ],
        vec![vec![text_chunk("unused worker turn")]],
    )
    .await;

    h.engine
        .admit(h.router, channel_event_input("evt-short", "status?"), None)
        .await
        .unwrap();

    wait_for_events(&h.store, h.router, "reply turn + final", |events| {
        count_kind(events, EventKind::TurnStart) == 2
            && count_kind(events, EventKind::AssistantMessage) == 2
    })
    .await;

    let deliveries = h.mock.deliveries();
    assert_eq!(deliveries.len(), 1);
    let delivered = deliveries[0].delivery.content["text"].as_str().unwrap();
    assert_eq!(delivered, short, "under-limit passes verbatim");

    let events = h.store.rebuild(h.router).await.unwrap();
    let delivered_ev = events
        .iter()
        .find(|e| e.kind == EventKind::ChannelDelivered)
        .expect("channel/delivered");
    let word_count = short.split_whitespace().count() as u64;
    assert_eq!(delivered_ev.bytes["words"], json!(word_count));
    assert_eq!(delivered_ev.bytes["truncated"], json!(false));
}

/// Honesty: a structured payload missing a section is rejected — the
/// tool call fails into the loop, nothing is delivered, and no
/// `channel/delivered` event exists.
#[tokio::test]
async fn structured_payload_missing_section_rejected_no_delivery_no_event() {
    let h = build_harness(
        vec![
            vec![tool_call_chunk(
                "call-1",
                "deliver_structured",
                json!({
                    "channel": "mock-im",
                    "target": {"chat_id": 7},
                    "payload": {"background": "b", "changes": "c"},
                }),
            )],
            vec![text_chunk("cannot deliver an incomplete report")],
        ],
        vec![vec![text_chunk("unused worker turn")]],
    )
    .await;

    h.engine
        .admit(h.router, channel_event_input("evt-bad", "report?"), None)
        .await
        .unwrap();

    wait_for_events(&h.store, h.router, "failed tool turn + final", |events| {
        count_kind(events, EventKind::TurnStart) == 2
            && count_kind(events, EventKind::ToolResult) == 1
    })
    .await;

    // No delivery, no channel/delivered — the rejection is honest.
    assert!(h.mock.deliveries().is_empty());
    let events = h.store.rebuild(h.router).await.unwrap();
    assert_eq!(count_kind(&events, EventKind::ChannelDelivered), 0);

    // The error surfaced to the loop names the missing section.
    let tool_result = events
        .iter()
        .find(|e| e.kind == EventKind::ToolResult)
        .unwrap();
    assert_eq!(tool_result.bytes["content"]["is_error"], json!(true));
    let err = tool_result.bytes["content"]["output"]["error"]
        .as_str()
        .unwrap();
    assert!(
        err.contains("missing required section `verification`"),
        "error was: {err}"
    );
}

/// The structured happy path: a complete payload is delivered and
/// `channel/delivered{policy:"structured"}` lands after the send
/// (no word count — the §2 `words?` schema).
#[tokio::test]
async fn structured_delivery_happy_path_appends_channel_delivered() {
    let payload = json!({
        "background": "the release job failed on a flaky test",
        "changes": "retried with backoff",
        "verification": "cargo test --workspace green twice",
    });
    let h = build_harness(
        vec![
            vec![tool_call_chunk(
                "call-1",
                "deliver_structured",
                json!({
                    "channel": "mock-im",
                    "target": {"issue": 42},
                    "payload": payload.clone(),
                }),
            )],
            vec![text_chunk("delivered")],
        ],
        vec![vec![text_chunk("unused worker turn")]],
    )
    .await;

    h.engine
        .admit(h.router, channel_event_input("evt-good", "report?"), None)
        .await
        .unwrap();

    wait_for_events(&h.store, h.router, "delivery turn + final", |events| {
        count_kind(events, EventKind::TurnStart) == 2
            && count_kind(events, EventKind::ChannelDelivered) == 1
    })
    .await;

    let deliveries = h.mock.deliveries();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].delivery.content, payload);
    assert_eq!(deliveries[0].delivery.target, json!({"issue": 42}));

    let events = h.store.rebuild(h.router).await.unwrap();
    let delivered_ev = events
        .iter()
        .find(|e| e.kind == EventKind::ChannelDelivered)
        .unwrap();
    assert_eq!(delivered_ev.bytes["policy"], json!("structured"));
    assert_eq!(delivered_ev.bytes["truncated"], json!(false));
    assert!(
        delivered_ev.bytes.get("words").is_none(),
        "`words` is omitted for structured deliveries"
    );
}

/// Subscription passthrough: a CI-style event of a worker's own topic
/// flows straight into that worker's inbox — the Router stream carries
/// ZERO events (no judgment, not even a session creation).
#[tokio::test]
async fn passthrough_event_reaches_worker_inbox_zero_router_events() {
    let h = build_harness(
        vec![vec![text_chunk("router never sees it")]],
        vec![vec![text_chunk("worker noted the ci event")]],
    )
    .await;

    let worker = SessionId::new();
    // A dedicated `ci` channel: the passthrough rule matches on the
    // channel name, so the injected event must carry `ci`.
    let ci = Arc::new(MockChannel::new("ci"));
    let rules = SubscriptionRuleSet::new(vec![
        SubscriptionRule {
            channel: "ci".into(),
            matcher: RuleMatcher::Topic("pr-9".into()),
            route: Route::Passthrough {
                correlate_by: CorrelateBy::Topic,
            },
        },
        SubscriptionRule {
            channel: "mock-im".into(),
            matcher: RuleMatcher::Always,
            route: Route::Router,
        },
    ]);
    let service = Arc::new(SubscriptionService::new(
        h.router,
        rules,
        h.router_inbox.clone() as Arc<dyn InboxWriter>,
    ));
    service.register_topic("pr-9", worker);
    let _consumers = service.spawn(vec![Arc::clone(&ci) as Arc<dyn ChannelAdapter>]);

    let injected = ci.inject(Some("pr-9"), json!({"build": "ok", "sha": "abc"}));

    // The worker's inbox row is consumed on its first hold; the input
    // carries the channel-event wrapper with the event id verbatim.
    wait_for_events(&h.store, worker, "worker consumes the ci event", |events| {
        events.iter().any(|e| {
            e.kind == EventKind::InputAdmitted
                && e.bytes["content"]["kian"]["event_id"] == json!(injected.event_id)
        })
    })
    .await;
    // The worker settles completed on its single scripted turn.
    wait_for_events(&h.store, worker, "worker settles", |events| {
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    })
    .await;

    // The worker judged its own passthrough event with exactly one
    // turn (and the Router made zero provider calls).
    let worker_events = h.store.rebuild(worker).await.unwrap();
    assert_eq!(
        worker_events
            .iter()
            .filter(|e| e.kind == EventKind::TurnStart)
            .count(),
        1
    );
    assert_eq!(h.worker_script.request_count(), 1);
    assert!(matches!(
        worker_events.last().unwrap().kind,
        EventKind::SessionEnded {
            reason: kian_core::events::EndReason::Completed
        }
    ));

    // Zero Router events: no admit, no wake, no session creation —
    // passthrough never touches Router judgment.
    let router_events = h.store.rebuild(h.router).await.unwrap();
    assert!(
        router_events.is_empty(),
        "router stream must carry zero events, got {router_events:#?}"
    );
    assert_eq!(h.router_script.request_count(), 0);
}

/// the admission wrapper spills exactly once. The committed pair
/// carries only the Inline view into context and the verbatim raw into
/// its non-context-bearing sibling; a duplicate event id adds no row.
#[tokio::test]
async fn over_cap_admission_commits_one_pair_and_router_sees_only_the_excerpt() {
    let h = build_harness(
        vec![vec![text_chunk("judged the inline view")]],
        vec![vec![text_chunk("unused worker turn")]],
    )
    .await;
    let rules = SubscriptionRuleSet::new(vec![SubscriptionRule {
        channel: "mock-im".into(),
        matcher: RuleMatcher::Always,
        route: Route::Router,
    }]);
    let service = SubscriptionService::new(
        h.router,
        rules,
        h.router_inbox.clone() as Arc<dyn InboxWriter>,
    )
    .with_shaping(ShapingConfig {
        inline_cap_bytes: Some(32),
        first_chars: 24,
        neighbor_fields: vec!["reply_to".into()],
    });

    let full_text = "FULL_OVER_CAP_RAW_abcdefghijklmnopqrstuvwxyz_MUST_NOT_REACH_THE_ROUTER";
    let raw = json!({"text": full_text, "reply_to": "msg-parent-7"});
    let event = ChannelEvent {
        channel: "mock-im".into(),
        event_id: "evt-over-cap".into(),
        topic: Some("room-9".into()),
        sender: None,
        group: None,
        payload: raw.clone(),
        ts_ms: 42,
    };
    let first = service.handle_event(event.clone()).await.unwrap();
    assert!(matches!(
        first,
        kian_superagent::RoutingOutcome::Routed {
            deduplicated: false,
            ..
        }
    ));
    // Shaped-once means idempotent admission, not a second shaping pass
    // over anything committed: the same event id dedupes to the one pair.
    let duplicate = service.handle_event(event).await.unwrap();
    assert!(matches!(
        duplicate,
        kian_superagent::RoutingOutcome::Routed {
            deduplicated: true,
            ..
        }
    ));

    let events = wait_for_events(&h.store, h.router, "spill pair and judge turn", |events| {
        count_kind(events, EventKind::InputSpilled) == 1
            && count_kind(events, EventKind::AssistantMessage) == 1
    })
    .await;
    assert_eq!(count_kind(&events, EventKind::InputAdmitted), 1);
    assert_eq!(count_kind(&events, EventKind::InputSpilled), 1);

    let serialized = serde_json::to_string(&raw).unwrap();
    let excerpt: String = serialized.chars().take(24).collect();
    let admitted = events
        .iter()
        .find(|e| e.kind == EventKind::InputAdmitted)
        .unwrap();
    let kian = &admitted.bytes["content"]["kian"];
    assert_eq!(kian["inline"]["excerpt"], json!(excerpt));
    assert_eq!(kian["inline"]["neighbors"]["topic"], json!("room-9"));
    assert_eq!(
        kian["inline"]["neighbors"]["event_id"],
        json!("evt-over-cap")
    );
    assert_eq!(
        kian["inline"]["neighbors"]["reply_to"],
        json!("msg-parent-7")
    );
    assert_eq!(kian["spill"], json!(true));
    assert_eq!(kian["continuation"]["event_id"], json!("evt-over-cap"));
    assert!(
        kian.get("raw").is_none(),
        "the raw left model-visible content"
    );

    let spilled = events
        .iter()
        .find(|e| e.kind == EventKind::InputSpilled)
        .unwrap();
    assert_eq!(
        spilled.bytes,
        json!({
            "event_id": "evt-over-cap",
            "channel": "mock-im",
            "topic": "room-9",
            "raw": raw,
        })
    );

    // The provider capture is the actual model-bound request, not merely
    // a projection assertion: it contains the excerpt and never the raw.
    let requests = h.router_script.requests();
    assert_eq!(requests.len(), 1, "the duplicate adds no judge turn");
    let body = serde_json::to_string(&requests[0].body).unwrap();
    let content = requests[0].body["messages"][0]["content"]
        .as_str()
        .expect("channel input message content");
    let model_content: Value = serde_json::from_str(content).expect("model content is JSON");
    assert_eq!(model_content["kian"]["inline"]["excerpt"], json!(excerpt));
    assert!(!body.contains(full_text), "full raw leaked: {body}");
    assert!(!body.contains("MUST_NOT_REACH_THE_ROUTER"));
}
