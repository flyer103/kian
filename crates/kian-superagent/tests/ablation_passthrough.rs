//! Integration test: the §7.1 ablation benchmark, with and
//! without passthrough, on kian's own bundle.
//!
//! One scripted workload — a work request (mock-IM) plus a train of
//! same-topic CI-style events — runs twice under **identical**
//! `ScriptedProvider`s (the same dynamic scripts; the provider's
//! response is a pure function of the request body it answers):
//!
//! - **passthrough on** — rules route `ci/pr-42` through the
//!   passthrough registry to the worker the Router dispatched for the
//!   work request; the train never reaches the Router.
//! - **passthrough off** — every event reaches the Router; the train is
//!   consumed by Router judge turns.
//!
//! What §7.1 measured, in kian's native units: Router judge turns
//! (`turn/start` count on the Router stream — the "Router decisions"
//! axis) and the read-token proxy (summed `step/request` body bytes on
//! the Router stream — the -style cache-read axis). The assertions
//! are **directionality, not the article's numbers** (different
//! workload, different model): with passthrough, turns ≤ without and
//! bytes ≤ without, at equal task completion (both runs end with the
//! worker's `thread/completed{completed}` and the Router's
//! `channel/delivered` — the honesty anchor: the ablation never trades
//! away completion).
//!
//! Determinism: `ManualClock` for the engine, scripted providers with
//! fixed dynamic logic, fixed event ids and fixed injection order, zero
//! wall-clock asserts. The one deliberate asymmetry — without
//! passthrough the CI train can never reach the worker, so the test
//! admits an explicit finish row to the worker after the Router has
//! consumed the whole train (with passthrough the worker finishes by
//! observing the train itself, the behavior under test) — is recorded
//! in the README benchmark note.
//!
//! Fully in-process over a real sqlite `EventStore` and a real
//! `Engine`: no credentials, no network egress (nothing for a system
//! proxy to intercept; `NO_PROXY` is still forced per AGENTS.md).

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use kian_core::events::{Event, EventKind};
use kian_core::model::{InputRow, SessionId, SlotName};
use kian_core::seams::{
    ChannelAdapter, ChannelEvent, LoopDriver, LoopOutcome, ModelChunk, ModelProvider, ModelRequest,
    RecoveryPolicySource, ToolCall, ToolError, ToolProvider, ToolResult, ToolSchema,
};
use kian_llm::turn::ProviderRegistry;
use kian_loop::{EvidenceGateConfig, ReActConfig, ReActLoop};
use kian_orchestrator::{Clock, DrainCtx, Engine, EngineConfig, ManualClock, WakeOutcome};
use kian_store::EventStore;
use kian_superagent::{
    AdmitReceipt, ChannelRegistry, InboxWriteError, InboxWriter, MockChannel, Route, RouterDriver,
    RouterHold, RouterInbox, RouterProfile, RouterTools, RuleMatcher, SubscriptionRule,
    SubscriptionRuleSet, SubscriptionService, WorkerLoopConfig, WorkerLoopDriver, WorkerSpawner,
    router_template, worker_template,
};
use serde_json::{Value, json};
use uuid::Uuid;

/// Forces the no-proxy environment for localhost (must run before any
/// HTTP client is built; AGENTS.md macOS proxy gotcha).
fn force_no_proxy() {
    unsafe {
        std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
        std::env::set_var("no_proxy", "127.0.0.1,localhost");
    }
}

// ---------------------------------------------------------------------------
// The workload script (fixed ids, fixed order, fixed timestamps)
// ---------------------------------------------------------------------------

/// The CI topic the train runs on (and the passthrough rule binds).
const CI_TOPIC: &str = "pr-42";
/// Quote-free marker inside the work request's payload text.
const WORK_MARKER: &str = "ABLATION_WORK_REQUEST_PR42";
/// Quote-free marker of the explicit finish row (passthrough-off only).
const FINISH_MARKER: &str = "ABLATION_WORKER_FINISH_NOW";

/// The quote-free per-stage marker the worker looks for.
fn ci_marker(stage: usize) -> String {
    format!("ABLATION_CI_STAGE_{stage}_OBSERVED")
}

/// The one work request of the workload (mock-IM, Router-routed in both
/// configurations).
fn work_request(seed: u64) -> ChannelEvent {
    ChannelEvent {
        channel: "mock-im".into(),
        event_id: format!("ablation-work-{seed}"),
        topic: None,
        sender: None,
        group: None,
        payload: json!({
            "text": format!(
                "@kian {WORK_MARKER} please drive the {CI_TOPIC} CI watch to completion"
            )
        }),
        ts_ms: 1_000 + seed,
    }
}

/// One CI-style train event (same topic, distinct stage marker).
fn train_event(seed: u64, stage: usize, total: usize) -> ChannelEvent {
    ChannelEvent {
        channel: "ci".into(),
        event_id: format!("ablation-ci-{seed}-{stage}"),
        topic: Some(CI_TOPIC.into()),
        sender: None,
        group: None,
        payload: json!({
            "stage": stage,
            "log": format!("{} of {total}", ci_marker(stage)),
        }),
        ts_ms: 2_000 + seed * 1_000 + stage as u64,
    }
}

// ---------------------------------------------------------------------------
// ScriptedProvider — identical dynamic logic in both configurations
// ---------------------------------------------------------------------------

type ScriptedChunks = Vec<Result<ModelChunk, kian_core::seams::ProviderError>>;
type DynamicTurn = Arc<dyn Fn(&ModelRequest) -> ScriptedChunks + Send + Sync>;

/// A `ModelProvider` whose every turn is built by one dynamic closure
/// from the request body it answers — a pure function of the committed
/// context, so both configurations run byte-identical decision logic.
struct ScriptedProvider {
    turn: DynamicTurn,
    calls: AtomicUsize,
}

impl ModelProvider for ScriptedProvider {
    fn complete(
        &self,
        req: ModelRequest,
    ) -> BoxStream<'static, Result<ModelChunk, kian_core::seams::ProviderError>> {
        let _ = self.calls.fetch_add(1, Ordering::SeqCst);
        let turn = (self.turn)(&req);
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

/// Serializes the request body once (the search surface).
fn body_text(req: &ModelRequest) -> String {
    serde_json::to_string(&req.body).unwrap_or_default()
}

/// The Router's judge logic (identical in both runs): dispatch the work
/// request to a worker, note any train batch the rules routed here,
/// reply on the channel when the worker's completion report arrives.
///
/// Every discriminator is quote-free, so it survives the chat body's
/// string-embedded JSON regardless of escaping depth.
fn router_turn(req: &ModelRequest) -> ScriptedChunks {
    let body = body_text(req);
    if body.contains("message_id") {
        // The reply tool already fired: settle this batch with text.
        vec![text_chunk("summary delivered")]
    } else if body.contains("completed") && body.contains("thread_id") {
        // The worker's completion report is in context: deliver the
        // summary on the IM channel (channel/delivered follows).
        vec![tool_call_chunk(
            "call-reply",
            "reply",
            json!({
                "channel": "mock-im",
                "text": format!("{CI_TOPIC} CI digest: every stage observed green"),
            }),
        )]
    } else if body.contains(WORK_MARKER) && !body.contains("thread_id") {
        // The work request, not yet dispatched.
        vec![tool_call_chunk(
            "call-spawn",
            "spawn_worker",
            json!({
                "task": format!("watch the {CI_TOPIC} CI train and summarize once every stage has reported"),
                "correlation": "ablation-work",
            }),
        )]
    } else {
        // A train batch the rules routed to the Router (passthrough
        // off) or the post-spawn pass: note it and settle.
        vec![text_chunk("noted")]
    }
}

/// The worker's logic (identical in both runs): keep reading until the
/// whole CI train is in its context (the passthrough behavior under
/// test) — then answer and complete. With passthrough off the train can
/// never arrive; the explicit finish row ends the watch instead.
fn worker_turn(train_len: usize) -> DynamicTurn {
    let markers: Vec<String> = (1..=train_len).map(ci_marker).collect();
    Arc::new(move |req: &ModelRequest| {
        let body = body_text(req);
        let all_seen = markers.iter().all(|m| body.contains(m.as_str()));
        if all_seen || body.contains(FINISH_MARKER) {
            return vec![text_chunk(
                "CI train fully observed: every stage green; work complete",
            )];
        }
        // The watch keeps reading; the loop's `max_iterations` is the
        // honest bail-out if a marker were ever lost.
        vec![tool_call_chunk(
            "call-read",
            "fs_read",
            json!({"path": "stage.log"}),
        )]
    })
}

// ---------------------------------------------------------------------------
// Fixtures (the harness shape, ManualClock'd)
// ---------------------------------------------------------------------------

/// The worker's tool surface (every call succeeds — the loop shape is
/// what matters here, not tool output).
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
    fn table(&self) -> Vec<kian_core::seams::PolicyRule> {
        Vec::new()
    }
}

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

impl kian_superagent::worker::StreamPort for CtxPort {
    fn session(&self) -> SessionId {
        self.0.session()
    }

    fn emit(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> BoxFuture<'static, Result<(), kian_superagent::worker::StreamPortError>> {
        let ctx = self.0.clone();
        Box::pin(async move {
            ctx.append(kind, bytes)
                .await
                .map(|_| ())
                .map_err(|e| kian_superagent::worker::StreamPortError(e.to_string()))
        })
    }

    fn read_stream(
        &self,
    ) -> BoxFuture<'static, Result<Vec<Event>, kian_superagent::worker::StreamPortError>> {
        let ctx = self.0.clone();
        Box::pin(async move {
            ctx.events()
                .await
                .map_err(|e| kian_superagent::worker::StreamPortError(e.to_string()))
        })
    }
}

use kian_superagent::worker::PortFor;

fn port_for_ctx() -> PortFor<DrainCtx> {
    Arc::new(|ctx: &DrainCtx| {
        Arc::new(CtxPort(ctx.clone())) as Arc<dyn kian_superagent::worker::StreamPort>
    })
}

/// The assembled three-way driver: Router side + worker side over one
/// engine (the kian-cli assembly shape, locally).
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

// ---------------------------------------------------------------------------
// The measured run
// ---------------------------------------------------------------------------

/// What one configuration's run measured (kian's native ablation axes
/// plus the honesty anchors).
#[derive(Debug, Clone, PartialEq, Eq)]
struct AblationRun {
    passthrough: bool,
    /// `turn/start` count on the Router stream (Router decisions).
    router_turns: usize,
    /// Summed `step/request` body bytes on the Router stream (the
    /// -style cache-read proxy).
    router_request_bytes: usize,
    /// `input/admitted` count on the Router stream (workload sanity).
    router_inputs: usize,
    /// The worker's stream ends `thread/completed{completed}`.
    worker_completed: bool,
    /// The Router's stream carries `channel/delivered`.
    router_delivered: bool,
}

/// The rules of one configuration. Passthrough on routes the CI topic
/// through the registry (bound at runtime to the dispatched worker, the
/// documented spawner hook); passthrough off sends every event to the
/// Router. The mock-IM rule is identical in both.
fn rules(passthrough: bool) -> SubscriptionRuleSet {
    let ci = if passthrough {
        SubscriptionRule {
            channel: "ci".into(),
            matcher: RuleMatcher::Topic(CI_TOPIC.into()),
            route: Route::Passthrough {
                correlate_by: kian_superagent::CorrelateBy::Topic,
            },
        }
    } else {
        SubscriptionRule {
            channel: "ci".into(),
            matcher: RuleMatcher::Always,
            route: Route::Router,
        }
    };
    SubscriptionRuleSet::new(vec![
        ci,
        SubscriptionRule {
            channel: "mock-im".into(),
            matcher: RuleMatcher::Always,
            route: Route::Router,
        },
    ])
}

async fn temp_store(tag: &str) -> EventStore {
    let path = std::env::temp_dir().join(format!("kian-c42-{tag}-{}.sqlite", Uuid::new_v4()));
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
    for _ in 0..1_000 {
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

/// Runs the full scripted workload once under one configuration and
/// returns the measured axes plus the completion anchors.
///
/// Fixed order: work request → (Router dispatches the worker) → topic
/// binding (passthrough on only) → the CI train → (passthrough off
/// only: explicit worker finish after the Router consumed the train) →
/// wait for `thread/completed{completed}` and `channel/delivered`.
async fn run_configuration(passthrough: bool, train_len: usize, seed: u64) -> AblationRun {
    force_no_proxy();
    let store = temp_store(if passthrough { "on" } else { "off" }).await;
    let router = SessionId::new();
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_000_000));

    let router_agent = router_template("mock", "mock-router");
    let worker_agent = worker_template("mock", "mock-worker", 7);
    let profile = RouterProfile::new(router, router_agent.clone());

    let router_script = Arc::new(ScriptedProvider {
        turn: Arc::new(router_turn),
        calls: AtomicUsize::new(0),
    });
    let worker_script = Arc::new(ScriptedProvider {
        turn: worker_turn(train_len),
        calls: AtomicUsize::new(0),
    });

    let mut router_registry = ProviderRegistry::new();
    router_registry.register("mock", Arc::clone(&router_script) as Arc<dyn ModelProvider>);
    let mut worker_registry = ProviderRegistry::new();
    worker_registry.register("mock", Arc::clone(&worker_script) as Arc<dyn ModelProvider>);

    let late = Arc::new(LateEngineInbox {
        store: store.clone(),
        clock: clock.clone(),
        engine: OnceLock::new(),
    });
    // The mounted router inbox: the subscription service, the spawner
    // and the worker wrapper all admit through it (the assembly shape),
    // so the residency counter observes every Router-bound row.
    let router_inbox = RouterInbox::new(late.clone() as Arc<dyn InboxWriter>, router);

    let spawner = Arc::new(WorkerSpawner::new(
        router,
        &worker_agent,
        router_inbox.clone() as Arc<dyn InboxWriter>,
    ));
    let mock_im = Arc::new(MockChannel::new("mock-im"));
    let ci_channel = Arc::new(MockChannel::new("ci"));
    let mut channels = ChannelRegistry::new();
    channels.register("mock-im", Arc::clone(&mock_im) as Arc<dyn ChannelAdapter>);
    let hold = RouterHold::shared();
    let dispatch_tools = Arc::new(RouterTools::new(spawner, channels, &profile, hold.clone()));

    let router_loop = Arc::new(ReActLoop::with_config(
        router_agent,
        Arc::new(router_registry),
        dispatch_tools,
        Arc::new(NoopPolicy),
        ReActConfig {
            slot: SlotName("router".into()),
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
        ReActConfig {
            slot: SlotName("main".into()),
            // The watch loop spins (tool-call turns) until the train is
            // fully observed — comfortably above DEFAULT_MAX_ITERATIONS
            // (32) and far below the defensive cap.
            max_iterations: 20_000,
            budget_scope: kian_loop::BudgetScope::Lifetime,
            permissions: std::collections::BTreeMap::new(),
        },
    ));

    let router_side = RouterDriver::with_park_tick(
        router_loop,
        hold,
        port_for_ctx(),
        router_inbox.clone(),
        Duration::from_millis(5),
    );
    let worker_side = WorkerLoopDriver::new(
        worker_loop,
        router_inbox.clone() as Arc<dyn InboxWriter>,
        WorkerLoopConfig {
            router,
            worker_version: 7,
            gate: EvidenceGateConfig::default(),
            multiagent: None,
            board_gating: kian_superagent::BoardGating::default(),
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

    // The subscription pipeline over both channels (in-process, no
    // server, no credentials).
    let service = Arc::new(SubscriptionService::new(
        router,
        rules(passthrough),
        router_inbox.clone() as Arc<dyn InboxWriter>,
    ));
    let _consumers = service.spawn(vec![
        Arc::clone(&mock_im) as Arc<dyn ChannelAdapter>,
        Arc::clone(&ci_channel) as Arc<dyn ChannelAdapter>,
    ]);

    // 1. The work request: the Router judges it and dispatches the
    //    worker for the CI watch.
    mock_im.send_event(work_request(seed));
    let events = wait_for_events(&store, router, "router dispatches the worker", |events| {
        events.iter().any(|e| e.kind == EventKind::ThreadStarted)
    })
    .await;
    let started = events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .expect("thread/started observed");
    let worker =
        SessionId(Uuid::parse_str(started.bytes["worker"].as_str().unwrap()).expect("worker uuid"));

    // 2. Bind the topic (passthrough on only): the runtime registration
    //    the worker spawner performs in assembly (the registry's
    //    documented runtime hook; restarts re-derive it from streams).
    if passthrough {
        service.register_topic(CI_TOPIC, worker);
        assert_eq!(
            service.registry_snapshot().resolve(CI_TOPIC),
            std::collections::BTreeSet::from([worker]),
            "the topic is bound before the train is injected"
        );
    }

    // 3. The CI train, fixed order: with passthrough every event admits
    //    straight into the worker's inbox (no Router judgment); without
    //    it every event reaches the Router.
    for stage in 1..=train_len {
        ci_channel.send_event(train_event(seed, stage, train_len));
    }

    // 4. Passthrough off only: the train can never reach the worker, so
    //    the watch ends by an explicit finish row — admitted strictly
    //    after the Router consumed the whole train, so the worker's
    //    completion is never batched into a train turn.
    if !passthrough {
        wait_for_events(
            &store,
            router,
            "router consumed the whole train",
            |events| count_kind(events, EventKind::InputAdmitted) == 1 + train_len,
        )
        .await;
        engine
            .admit(
                worker,
                json!(FINISH_MARKER),
                Some(format!("ablation-finish-{seed}")),
            )
            .await
            .expect("admit finish row");
    }

    // 5. Honesty anchors: the worker completes `completed`, the Router
    //    delivers on the channel.
    let worker_events = wait_for_events(
        &store,
        worker,
        "worker thread/completed{completed}",
        |events| {
            events.iter().any(|e| {
                e.kind == EventKind::ThreadCompleted && e.bytes["status"] == json!("completed")
            })
        },
    )
    .await;
    let worker_completed = worker_events
        .iter()
        .any(|e| e.kind == EventKind::ThreadCompleted && e.bytes["status"] == json!("completed"));

    // Wait for the judge turn that follows the delivery (the reply tool
    // call's follow-up) so the byte axis covers the full workload —
    // deterministic in both configurations.
    let router_events = wait_for_events(
        &store,
        router,
        "channel/delivered + the closing judge turn",
        |events| {
            let delivered = events
                .iter()
                .position(|e| e.kind == EventKind::ChannelDelivered);
            let last_assistant = events
                .iter()
                .rposition(|e| e.kind == EventKind::AssistantMessage);
            match (delivered, last_assistant) {
                (Some(d), Some(a)) => a > d,
                _ => false,
            }
        },
    )
    .await;

    // 6. The axes: Router judge turns and summed step/request body
    //    bytes, both read from the Router's committed stream.
    let router_turns = count_kind(&router_events, EventKind::TurnStart);
    let router_request_bytes = router_events
        .iter()
        .filter(|e| e.kind == EventKind::StepRequest)
        .map(|e| {
            serde_json::to_string(&e.bytes["body"])
                .expect("serialize body")
                .len()
        })
        .sum();
    let router_inputs = count_kind(&router_events, EventKind::InputAdmitted);
    let router_delivered = router_events
        .iter()
        .any(|e| e.kind == EventKind::ChannelDelivered);

    AblationRun {
        passthrough,
        router_turns,
        router_request_bytes,
        router_inputs,
        worker_completed,
        router_delivered,
    }
}

// ---------------------------------------------------------------------------
// Named acceptance tests
// ---------------------------------------------------------------------------

const TRAIN_LEN: usize = 6;
const SEED: u64 = 1;

/// The ablation axis 1 — Router judge turns: with passthrough the train
/// never spends a Router decision; without it, every train event is
/// consumed by a judge turn. Directionality, not the article's numbers.
#[tokio::test]
async fn ablation_passthrough_reduces_router_turns() {
    let on = run_configuration(true, TRAIN_LEN, SEED).await;
    let off = run_configuration(false, TRAIN_LEN, SEED).await;
    // Workload sanity: the off-run's Router really judged the train.
    assert_eq!(
        on.router_inputs, 2,
        "on: exactly the work request + the completion report"
    );
    assert_eq!(
        off.router_inputs,
        2 + TRAIN_LEN,
        "off: the whole train reached the Router"
    );
    assert!(
        on.router_turns <= off.router_turns,
        "passthrough turns {} > without {}",
        on.router_turns,
        off.router_turns
    );
}

/// The ablation axis 2 — the read-token proxy (summed `step/request`
/// body bytes on the Router stream): with passthrough the train bytes
/// never enter a Router request; without it they ride every judge turn
/// after their consumption (append-only context).
#[tokio::test]
async fn ablation_passthrough_reduces_router_request_bytes() {
    let on = run_configuration(true, TRAIN_LEN, SEED).await;
    let off = run_configuration(false, TRAIN_LEN, SEED).await;
    assert_eq!(on.router_inputs, 2);
    assert_eq!(off.router_inputs, 2 + TRAIN_LEN);
    assert!(
        on.router_request_bytes <= off.router_request_bytes,
        "passthrough bytes {} > without {}",
        on.router_request_bytes,
        off.router_request_bytes
    );
}

/// The honesty anchor: the ablation never trades away completion. Both
/// configurations run the same workload to the same end state — the
/// worker's `thread/completed{completed}` and the Router's
/// `channel/delivered`.
#[tokio::test]
async fn ablation_both_configurations_complete() {
    let on = run_configuration(true, TRAIN_LEN, SEED).await;
    let off = run_configuration(false, TRAIN_LEN, SEED).await;
    for run in [&on, &off] {
        let label = if run.passthrough { "on" } else { "off" };
        assert!(run.worker_completed, "{label}: worker completed");
        assert!(run.router_delivered, "{label}: router delivered");
    }
}

/// The heavier local variant (`#[ignore]`-gated: larger event trains,
/// repeated seeds). Run with:
///
/// ```bash
/// cargo test -p kian-superagent --test ablation_passthrough -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "heavier ablation variant for local runs (see README benchmark note)"]
async fn ablation_heavy_variant_local_only() {
    for seed in [11u64, 12, 13] {
        let on = run_configuration(true, 40, seed).await;
        let off = run_configuration(false, 40, seed).await;
        assert!(
            on.worker_completed && on.router_delivered,
            "seed {seed}: on completes"
        );
        assert!(
            off.worker_completed && off.router_delivered,
            "seed {seed}: off completes"
        );
        assert!(
            on.router_turns <= off.router_turns,
            "seed {seed}: turns {} > {}",
            on.router_turns,
            off.router_turns
        );
        assert!(
            on.router_request_bytes <= off.router_request_bytes,
            "seed {seed}: bytes {} > {}",
            on.router_request_bytes,
            off.router_request_bytes
        );
        println!(
            "seed {seed}: passthrough on  — turns {}, request bytes {}, router inputs {}",
            on.router_turns, on.router_request_bytes, on.router_inputs
        );
        println!(
            "seed {seed}: passthrough off — turns {}, request bytes {}, router inputs {}",
            off.router_turns, off.router_request_bytes, off.router_inputs
        );
    }
}
