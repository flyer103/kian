//! Integration tests: Thread delegation over a real sqlite
//! `EventStore` and a real `Engine`, with **ScriptedProvider on both
//! sides** (Router and Worker) — no credentials, no network egress at
//! all (in-process engines + mock providers), so there is nothing for a
//! system proxy to intercept.
//!
//! The assembly surface is exactly what will build: the
//! `InboxWriter` port over the engine, the `StreamPort` over the
//! execution-hold handle, one engine whose driver routes the resident
//! Router session to the router loop and every other session to the
//! worker wrapper.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt, once};
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, InputRow, SessionId};
use kian_core::seams::{
    LoopDriver, LoopOutcome, ModelChunk, ModelProvider, ModelRequest, PolicyRule,
    RecoveryPolicySource, ToolCall, ToolError, ToolProvider, ToolResult, ToolSchema,
};
use kian_llm::turn::ProviderRegistry;
use kian_loop::{CheckSpec, EvidenceGateConfig, ReActConfig, ReActLoop};
use kian_orchestrator::{Clock, DrainCtx, Engine, EngineConfig, ManualClock, WakeOutcome};
use kian_store::EventStore;
use kian_superagent::worker::{BindingAdmissionPort, SpawnError};
use kian_superagent::{
    AdmitReceipt, ChannelRegistry, DelegationPair, InboxWriteError, InboxWriter, MemberState,
    MemberView, PortFor, RouterHold, RouterProfile, RouterTools, SPAWN_WORKER_TOOL,
    SessionBootstrap, SpawnRequest, SpawnSessionWriter, SpawnedWorker, StreamPort, StreamPortError,
    SuperAgentProfile, WorkerDirectoryError, WorkerDirectoryPort, WorkerLoopConfig,
    WorkerLoopDriver, WorkerSpawner, WorkerStreamsError, WorkerStreamsPort, delegation_pairs,
    parse_thread_report, router_template, thread_liveness, thread_started_payload, worker_template,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

async fn temp_store() -> EventStore {
    let path = std::env::temp_dir().join(format!("kian-superagent-c25-{}.sqlite", Uuid::new_v4()));
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
    for _ in 0..600 {
        let events = store.rebuild(session).await.expect("rebuild");
        if pred(&events) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

// ---------------------------------------------------------------------------
// ScriptedProvider (both sides — the stand-in, no credentials)
// ---------------------------------------------------------------------------

type ScriptedChunks = Vec<Result<ModelChunk, kian_core::seams::ProviderError>>;
type RequestHook = Arc<dyn Fn(usize) -> BoxFuture<'static, ()> + Send + Sync>;

/// A `ModelProvider` that replays scripted turns in order and records
/// every request body. The optional per-request hook runs at first poll
/// of each turn (the fault-injection point — the takeover test hangs a
/// turn from it).
struct ScriptedProvider {
    turns: Mutex<VecDeque<ScriptedChunks>>,
    hook: Mutex<Option<RequestHook>>,
    call_count: std::sync::atomic::AtomicUsize,
}

impl ScriptedProvider {
    fn new(turns: Vec<ScriptedChunks>) -> Self {
        Self {
            turns: Mutex::new(turns.into()),
            hook: Mutex::new(None),
            call_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Installs a hook awaited (with the 1-based call number) when a
    /// request arrives.
    fn on_request<F, Fut>(&self, hook: F)
    where
        F: Fn(usize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        *self.hook.lock().unwrap() = Some(Arc::new(move |n| Box::pin(hook(n))));
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
        let _ = req;
        let n = self
            .call_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let hook = self.hook.lock().unwrap().clone();
        let turn = self.turns.lock().unwrap().pop_front().unwrap_or_default();
        let state = (hook, n, turn.into_iter(), false);
        futures::stream::unfold(state, |(hook, n, mut iter, mut hook_done)| async move {
            if !hook_done {
                hook_done = true;
                if let Some(hook) = &hook {
                    hook(n).await;
                }
            }
            iter.next().map(|item| (item, (hook, n, iter, hook_done)))
        })
        .boxed()
    }
}

fn text_chunk(text: &str) -> Result<ModelChunk, kian_core::seams::ProviderError> {
    Ok(ModelChunk::new(json!({"content": text})))
}

fn tool_call_chunk(id: &str, name: &str) -> Result<ModelChunk, kian_core::seams::ProviderError> {
    Ok(ModelChunk::new(json!({"tool_calls": [{
        "index": 0,
        "id": id,
        "function": {"name": name, "arguments": "{\"argv\":[\"echo\",\"hi\"]}"}
    }]})))
}

// ---------------------------------------------------------------------------
// Tools / policy
// ---------------------------------------------------------------------------

/// The worker's tool provider: the full worker toolset surface, every
/// invocation succeeding (LocalTools/RemoteExecTool behavior is
/// exercised in their own crates; here the stream shape is what the
/// evidence gate reads).
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

/// The cycle breaker: the driver graph needs an inbox before the engine
/// exists, so the engine installs itself after construction. `admit` is
/// the same durable store write the engine performs; `wake` reaches the
/// engine through the late-bound slot.
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

/// Assembly's `StreamPort` over the execution-hold handle: every emit
/// carries the hold's fencing epoch (the handle guarantees that); the
/// stream read is the committed truth. A local newtype — the orphan
/// rule keeps a direct impl in assembly's crate (exactly how will
/// build it).
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
            ctx.events_vec()
                .await
                .map_err(|e| StreamPortError(e.to_string()))
        })
    }
}

/// The port constructor assembly hands the wrapper: one `CtxPort` per
/// execution hold, built from the hold handle.
fn port_for_ctx() -> PortFor<DrainCtx> {
    Arc::new(|ctx: &DrainCtx| Arc::new(CtxPort(ctx.clone())) as Arc<dyn StreamPort>)
}

/// Wraps the inbox and proves invariant 1 for the completion report: at
/// the moment the Router is woken, the just-admitted report row is
/// already durable (an eligible row with a `thread-report:` key
/// exists). The probe runs on its own thread with a small runtime (a
/// sync trait method cannot await on this thread's runtime) — the
/// `OrderingInbox` pattern.
struct DurabilityProbeInbox {
    inner: Arc<dyn InboxWriter>,
    store: EventStore,
    router: SessionId,
    wake_saw_durable_report: Mutex<Vec<bool>>,
}

impl InboxWriter for DurabilityProbeInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        self.inner.admit(session, payload, idempotency_key)
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        if session != self.router {
            return self.inner.wake(session);
        }
        // Check-then-wake: nothing has consumed the row yet because the
        // wake that would spawn the consuming hold happens below.
        let store = self.store.clone();
        let router = self.router;
        let durable = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("probe runtime");
            rt.block_on(async move {
                store
                    .eligible_inputs(router, now_ms())
                    .await
                    .unwrap_or_default()
                    .iter()
                    .any(|row| {
                        row.idempotency_key
                            .as_deref()
                            .is_some_and(|k| k.starts_with("thread-report:"))
                    })
            })
        })
        .join()
        .unwrap_or(false);
        self.wake_saw_durable_report.lock().unwrap().push(durable);
        self.inner.wake(session)
    }
}

// ---------------------------------------------------------------------------
// The Router-side dispatch driver (the spawn_worker stand-)
// ---------------------------------------------------------------------------

/// The Router's loop: on first drive (per *stream*, idempotent — a
/// restarted engine must not re-spawn) it dispatches one worker via
/// [`WorkerSpawner`], appends `thread/started` to the Router's own
/// stream at dispatch (what 's `spawn_worker` tool will do from
/// inside the Router's hold), then delegates to the inner ReAct loop so
/// the Router's own scripted model turn runs through the real loop
/// machinery.
struct RouterDispatchDriver {
    inner: ReActLoop,
    spawner: Arc<WorkerSpawner>,
    task: Value,
    spawned: Arc<Mutex<Option<SpawnedWorker>>>,
}

impl LoopDriver for RouterDispatchDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        let spawner = Arc::clone(&self.spawner);
        let task = self.task.clone();
        let spawned_slot = Arc::clone(&self.spawned);
        let prelude_ctx = ctx.clone();
        once(async move {
            // Idempotent by stream scan: a takeover engine must not
            // spawn a second worker for a Router that already
            // dispatched.
            let events = prelude_ctx.events().await.expect("router stream read");
            if !events.iter().any(|e| e.kind == EventKind::ThreadStarted) {
                let spawned = spawner.spawn(task, None).await.expect("spawn");
                *spawned_slot.lock().unwrap() = Some(spawned.clone());
                prelude_ctx
                    .append(
                        EventKind::ThreadStarted,
                        thread_started_payload(
                            &spawned.thread,
                            spawned.worker_session,
                            &spawned.task_message,
                        ),
                    )
                    .await
                    .expect("router thread/started");
            }
            LoopOutcome::TurnDone
        })
        .chain(self.inner.drive(ctx))
        .boxed()
    }
}

// ---------------------------------------------------------------------------
// The assembled super-agent driver
// ---------------------------------------------------------------------------

/// One engine, one driver: the resident Router session routes to the
/// router wrapper (which passes it through — no lineage), every other
/// session routes to the worker wrapper. Both wrappers share the same
/// inbox port and config, exactly how assembles it.
struct SuperAgentDriver {
    router: SessionId,
    router_side: WorkerLoopDriver<RouterDispatchDriver>,
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
    worker_script: Arc<ScriptedProvider>,
    router_script: Arc<ScriptedProvider>,
    spawned: Arc<Mutex<Option<SpawnedWorker>>>,
    probe: Arc<DurabilityProbeInbox>,
}

fn worker_gate() -> EvidenceGateConfig {
    EvidenceGateConfig {
        enabled: true,
        checks: vec![
            CheckSpec::FinalMessagePresent,
            CheckSpec::ToolResultSinceLastUserInput,
        ],
        max_retries: 1,
        ..EvidenceGateConfig::default()
    }
}

/// What varies per harness: the scripted turns on both sides, the
/// worker gate, and the shared Router identity/dispatch record (the
/// Router id is minted once per *store*, not per engine — a takeover
/// engine must drive the same resident Router).
struct HarnessSpec {
    worker_turns: Vec<ScriptedChunks>,
    router_turns: Vec<ScriptedChunks>,
    gate: EvidenceGateConfig,
    spawned: Arc<Mutex<Option<SpawnedWorker>>>,
    router: SessionId,
}

fn build_harness(
    store: EventStore,
    clock: Arc<dyn Clock>,
    engine_config: EngineConfig,
    spec: HarnessSpec,
) -> Harness {
    let HarnessSpec {
        worker_turns,
        router_turns,
        gate,
        spawned,
        router,
    } = spec;
    let worker_agent = worker_template("mock", "mock-worker", 7);
    let router_agent = router_template("mock", "mock-router");
    SuperAgentProfile::new(router_agent.clone(), worker_agent.clone())
        .expect("structural profile rules hold");

    let worker_script = Arc::new(ScriptedProvider::new(worker_turns));
    let router_script = Arc::new(ScriptedProvider::new(router_turns));

    let mut worker_registry = ProviderRegistry::new();
    worker_registry.register("mock", Arc::clone(&worker_script) as Arc<dyn ModelProvider>);
    let mut router_registry = ProviderRegistry::new();
    router_registry.register("mock", Arc::clone(&router_script) as Arc<dyn ModelProvider>);

    let worker_loop = Arc::new(ReActLoop::with_config(
        worker_agent,
        Arc::new(worker_registry),
        Arc::new(FakeTools),
        Arc::new(NoopPolicy),
        ReActConfig::default(),
    ));
    let router_loop = Arc::new(ReActLoop::with_config(
        router_agent,
        Arc::new(router_registry),
        Arc::new(FakeTools),
        Arc::new(NoopPolicy),
        ReActConfig {
            slot: kian_core::model::SlotName("router".into()),
            max_iterations: 16,
            budget_scope: kian_loop::BudgetScope::Lifetime,
            vision: kian_loop::VisionBudget::default(),
            max_request_bytes: Default::default(),
            permissions: std::collections::BTreeMap::new(),
        },
    ));

    let late = Arc::new(LateEngineInbox {
        store: store.clone(),
        clock: clock.clone(),
        engine: OnceLock::new(),
    });
    let probe = Arc::new(DurabilityProbeInbox {
        inner: Arc::clone(&late) as Arc<dyn InboxWriter>,
        store: store.clone(),
        router,
        wake_saw_durable_report: Mutex::new(Vec::new()),
    });

    let spawner = Arc::new(WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_template("mock", "mock-worker", 7),
        probe.clone() as Arc<dyn InboxWriter>,
    ));
    let router_dispatch = Arc::new(RouterDispatchDriver {
        inner: (*router_loop).clone(),
        spawner,
        task: json!("build the thing"),
        spawned: Arc::clone(&spawned),
    });

    let wrapper_config = WorkerLoopConfig {
        router: kian_superagent::ResidentBinding::new(router),
        worker_version: 7,
        gate,
        multiagent: None,
        board_gating: kian_superagent::BoardGating::default(),
    };
    let router_side = WorkerLoopDriver::new(
        Arc::clone(&router_dispatch) as Arc<RouterDispatchDriver>,
        probe.clone() as Arc<dyn InboxWriter>,
        wrapper_config.clone(),
        port_for_ctx(),
    );
    let worker_side = WorkerLoopDriver::new(
        worker_loop,
        probe.clone() as Arc<dyn InboxWriter>,
        wrapper_config,
        port_for_ctx(),
    );

    let engine = Engine::with_clock(
        store.clone(),
        Arc::new(SuperAgentDriver {
            router,
            router_side,
            worker_side,
        }),
        engine_config,
        clock,
    );
    // Install the engine into the late-bound inbox (before any wake can
    // fire — nothing drains yet).
    assert!(late.engine.set(Arc::clone(&engine)).is_ok());

    Harness {
        store,
        engine,
        router,
        worker_script,
        router_script,
        spawned,
        probe,
    }
}

async fn default_harness(
    worker_turns: Vec<ScriptedChunks>,
    router_turns: Vec<ScriptedChunks>,
    gate: EvidenceGateConfig,
) -> Harness {
    build_harness(
        temp_store().await,
        Arc::new(kian_orchestrator::SystemClock),
        EngineConfig::default(),
        HarnessSpec {
            worker_turns,
            router_turns,
            gate,
            spawned: Arc::new(Mutex::new(None)),
            router: SessionId::new(),
        },
    )
}

// ---------------------------------------------------------------------------
// Stream helpers
// ---------------------------------------------------------------------------

fn count_kind(events: &[Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind).count()
}

fn position_of(events: &[Event], pred: impl Fn(&Event) -> bool) -> Option<usize> {
    events.iter().position(pred)
}

async fn wait_spawned(slot: &Arc<Mutex<Option<SpawnedWorker>>>) -> SpawnedWorker {
    for _ in 0..600 {
        if let Some(spawned) = slot.lock().unwrap().clone() {
            return spawned;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("router never dispatched a worker");
}

/// Holds the Router's first provider turn until the spawned worker's
/// stream is terminal — making the "report lands while the Router's
/// hold is in flight" ordering deterministic (the folded-wake path the
/// engine is designed for: the report row arrives, the wake folds onto
/// the running hold, the Router's next episode consumes it).
fn hold_router_turn_on(
    script: &ScriptedProvider,
    store: EventStore,
    slot: &Arc<Mutex<Option<SpawnedWorker>>>,
) {
    let store = store.clone();
    let slot = Arc::clone(slot);
    script.on_request(move |n| {
        let store = store.clone();
        let slot = Arc::clone(&slot);
        async move {
            if n != 1 {
                return;
            }
            for _ in 0..600 {
                let spawned = slot.lock().unwrap().clone();
                if let Some(spawned) = spawned
                    && let Ok(events) = store.rebuild(spawned.worker_session).await
                    && events
                        .iter()
                        .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            panic!("worker never settled while the router waited");
        }
    });
}

fn hold_router_turn_until_worker_settles(h: &Harness) {
    hold_router_turn_on(&h.router_script, h.store.clone(), &h.spawned);
}

// ---------------------------------------------------------------------------
// Named acceptance tests
// ---------------------------------------------------------------------------

/// Full lifecycle: Router dispatch → the `thread/started` pair on both
/// streams with identical thread ids → worker completes (tool call +
/// final answer) → gate verdict → `thread/completed` → the report row
/// lands in the Router inbox **durable before any Router wake** → the
/// Router's next hold consumes it.
#[tokio::test]
async fn full_lifecycle_delegation_verdict_completion_report_consumed() {
    let h = default_harness(
        vec![
            vec![tool_call_chunk("call-1", "exec")],
            vec![text_chunk("task complete")],
        ],
        vec![vec![text_chunk("dispatched")], vec![text_chunk("noted")]],
        worker_gate(),
    )
    .await;
    hold_router_turn_until_worker_settles(&h);

    h.engine
        .admit(h.router, json!("please build the thing"), None)
        .await
        .unwrap();

    let spawned = wait_spawned(&h.spawned).await;
    let worker = spawned.worker_session;
    let thread_id = spawned.thread.id;

    // Worker terminal + Router consumed the report (its second input).
    wait_for_events(&h.store, worker, "worker settles", |events| {
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    })
    .await;
    wait_for_events(&h.store, h.router, "router consumes the report", |events| {
        count_kind(events, EventKind::InputAdmitted) == 2
            && events
                .iter()
                .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    })
    .await;

    let worker_events = h.store.rebuild(worker).await.unwrap();
    let router_events = h.store.rebuild(h.router).await.unwrap();

    // The thread/started pair: both streams, identical thread id, the
    // worker field names the worker, the parent names the Router.
    let router_started = router_events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .expect("router-side thread/started");
    let worker_started = worker_events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .expect("worker-side thread/started");
    assert_eq!(
        router_started.bytes["thread_id"],
        worker_started.bytes["thread_id"]
    );
    assert_eq!(
        router_started.bytes["thread_id"],
        json!(thread_id.to_string())
    );
    assert_eq!(router_started.bytes["worker"], json!(worker.0.to_string()));
    assert_eq!(
        router_started.bytes["parent"],
        json!(h.router.0.to_string())
    );
    assert_eq!(router_started.bytes["pinned_version"], json!(7));
    assert_eq!(count_kind(&worker_events, EventKind::ThreadStarted), 1);

    // The worker's first input is the delegation wrapper, model-visible
    // and never rewritten: content embeds thread id + task verbatim.
    let first_input = worker_events
        .iter()
        .find(|e| e.kind == EventKind::InputAdmitted)
        .unwrap();
    assert_eq!(
        first_input.bytes["content"]["thread_id"],
        json!(thread_id.to_string())
    );
    assert_eq!(
        first_input.bytes["content"]["task"],
        json!("build the thing")
    );

    // The evidence verdict passed and precedes thread/completed, which
    // precedes the end write (the ordering contract).
    let verdict_pos = position_of(&worker_events, |e| e.kind == EventKind::EvidenceVerdict)
        .expect("evidence/verdict");
    let completed_pos = position_of(&worker_events, |e| e.kind == EventKind::ThreadCompleted)
        .expect("thread/completed");
    let ended_pos = position_of(&worker_events, |e| {
        matches!(e.kind, EventKind::SessionEnded { .. })
    })
    .unwrap();
    assert!(verdict_pos < completed_pos && completed_pos < ended_pos);
    let verdict = &worker_events[verdict_pos];
    assert_eq!(verdict.bytes["gate"], json!("worker"));
    assert_eq!(verdict.bytes["passed"], json!(true));
    assert_eq!(verdict.bytes["attempt"], json!(0));

    // thread/completed: honest completed status, summary = the last
    // assistant message.
    let completed = &worker_events[completed_pos];
    assert_eq!(completed.bytes["thread_id"], json!(thread_id.to_string()));
    assert_eq!(completed.bytes["status"], json!("completed"));
    assert_eq!(completed.bytes["summary"]["text"], json!("task complete"));

    // Both sessions settle: worker completed, router completed.
    assert!(matches!(
        worker_events.last().unwrap().kind,
        EventKind::SessionEnded {
            reason: kian_core::events::EndReason::Completed
        }
    ));
    assert!(matches!(
        router_events.last().unwrap().kind,
        EventKind::SessionEnded {
            reason: kian_core::events::EndReason::Completed
        }
    ));

    // The Router consumed the completion report as its second input.
    let report_input = router_events
        .iter()
        .filter(|e| e.kind == EventKind::InputAdmitted)
        .nth(1)
        .expect("report input");
    let report = parse_thread_report(&report_input.bytes["content"]).expect("report parses");
    assert_eq!(report.thread_id, thread_id);
    assert_eq!(report.status, "completed");
    assert_eq!(report.summary["text"], json!("task complete"));

    // Invariant 1, observable: at the moment the Router was woken for
    // the report, the row was already durable.
    assert_eq!(
        h.probe.wake_saw_durable_report.lock().unwrap().clone(),
        vec![true],
        "the report row must be durable before the Router wake fires"
    );

    // Pass-through neutrality: the Router stream (no lineage) never
    // carries worker-wrapper events.
    assert_eq!(count_kind(&router_events, EventKind::ThreadCompleted), 0);
    assert_eq!(count_kind(&router_events, EventKind::EvidenceVerdict), 0);

    // The delegation causal pair is discoverable by scanning both
    // streams (the audit helper).
    assert_eq!(
        delegation_pairs(&router_events, &worker_events),
        vec![DelegationPair {
            thread_id,
            parent: h.router,
            worker,
            task: router_started.bytes["task"].clone(),
        }]
    );

    // The gate made no model call beyond the loop's own two turns.
    assert_eq!(h.worker_script.request_count(), 2);
}

/// Worker killed mid-task: the hold dies between the tool turn and the
/// final answer, the lease expires, a fresh engine takes over — **no
/// duplicate `thread/started`** (idempotent lineage by stream scan) and
/// the task still completes through the takeover re-drive.
#[tokio::test]
async fn takeover_midtask_no_duplicate_thread_started_task_completes() {
    let store = temp_store().await;
    let clock = Arc::new(ManualClock::new(1_000_000));
    let spawned_slot: Arc<Mutex<Option<SpawnedWorker>>> = Arc::new(Mutex::new(None));
    let crash_now = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let router = SessionId::new();

    // Engine A on its own runtime: the worker finishes its tool turn,
    // then hangs in the second provider turn until the crash flag drops
    // the runtime (the zombie pattern).
    let spawned_a = Arc::clone(&spawned_slot);
    let clock_a: Arc<ManualClock> = Arc::clone(&clock);
    let store_a = store.clone();
    let flag = Arc::clone(&crash_now);
    let zombie = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async move {
            let h = build_harness(
                store_a,
                clock_a,
                EngineConfig {
                    holder: "doomed".into(),
                    lease_ttl_ms: 100,
                    heartbeat_interval_ms: 10,
                },
                HarnessSpec {
                    worker_turns: vec![
                        vec![tool_call_chunk("call-1", "exec")],
                        vec![text_chunk("never delivered")],
                    ],
                    router_turns: vec![vec![text_chunk("dispatched")]],
                    gate: worker_gate(),
                    spawned: spawned_a,
                    router,
                },
            );
            // Hang the second worker provider turn (request #2) — the
            // fault-injection point — and park the Router's own turn
            // (its hold dies with the runtime anyway).
            h.worker_script.on_request(|n| async move {
                if n == 2 {
                    std::future::pending::<()>().await;
                }
            });
            h.router_script.on_request(|_| std::future::pending::<()>());
            h.engine
                .admit(h.router, json!("please build the thing"), None)
                .await
                .unwrap();
            while !flag.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }); // runtime dropped: the worker's hold dies mid-turn
    });

    // Wait until the worker is mid second turn (two turn/starts, the
    // tool result of turn 1 committed), then crash engine A.
    let spawned = wait_spawned(&spawned_slot).await;
    let worker = spawned.worker_session;
    wait_for_events(&store, worker, "worker mid second turn", |events| {
        count_kind(events, EventKind::TurnStart) == 2
            && count_kind(events, EventKind::ToolResult) == 1
    })
    .await;
    crash_now.store(true, std::sync::atomic::Ordering::Relaxed);
    zombie.join().expect("zombie thread finished");

    // Engine B: the lease is still fresh (the frozen clock heartbeat);
    // past the TTL the recovery sweep takes over with a higher epoch.
    let h2 = build_harness(
        store.clone(),
        Arc::clone(&clock) as Arc<dyn Clock>,
        EngineConfig {
            holder: "recoverer".into(),
            lease_ttl_ms: 100,
            heartbeat_interval_ms: 10,
        },
        HarnessSpec {
            worker_turns: vec![vec![text_chunk("resumed and done")]],
            router_turns: vec![vec![text_chunk("resumed")], vec![text_chunk("noted")]],
            gate: worker_gate(),
            spawned: Arc::new(Mutex::new(None)),
            router,
        },
    );
    // The Router's takeover turn waits for the worker to finish, so the
    // report lands while the Router's hold is in flight (the folded-wake
    // path; see `hold_router_turn_until_worker_settles`). The worker
    // identity comes from engine A's dispatch (engine B never re-spawns).
    hold_router_turn_on(&h2.router_script, h2.store.clone(), &spawned_slot);
    clock.advance(200);
    let woken = h2.engine.recovery_sweep().await.unwrap();
    assert!(
        woken.contains(&worker),
        "expired running worker woken (woken: {woken:?})"
    );

    wait_for_events(&h2.store, worker, "takeover completes the task", |events| {
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    })
    .await;
    wait_for_events(
        &h2.store,
        h2.router,
        "router consumes the report",
        |events| count_kind(events, EventKind::InputAdmitted) == 2,
    )
    .await;

    let worker_events = h2.store.rebuild(worker).await.unwrap();

    // Idempotent lineage: exactly one thread/started, one verdict, one
    // thread/completed, one end write — and honest completion.
    assert_eq!(count_kind(&worker_events, EventKind::ThreadStarted), 1);
    assert_eq!(count_kind(&worker_events, EventKind::EvidenceVerdict), 1);
    assert_eq!(count_kind(&worker_events, EventKind::ThreadCompleted), 1);
    assert!(matches!(
        worker_events.last().unwrap().kind,
        EventKind::SessionEnded {
            reason: kian_core::events::EndReason::Completed
        }
    ));
    let completed = worker_events
        .iter()
        .find(|e| e.kind == EventKind::ThreadCompleted)
        .unwrap();
    assert_eq!(completed.bytes["status"], json!("completed"));
    assert_eq!(
        completed.bytes["summary"]["text"],
        json!("resumed and done")
    );

    // Two claims (A's + B's takeover), strictly increasing epochs.
    let claims: Vec<kian_core::model::Epoch> = worker_events
        .iter()
        .filter(|e| e.kind == EventKind::DrainClaimed)
        .map(|e| e.epoch)
        .collect();
    assert_eq!(claims.len(), 2, "A's claim + B's takeover claim");
    assert!(claims[1] > claims[0]);

    // The Router (driven by engine B) did not re-spawn: still exactly
    // one thread/started on its stream either.
    let router_events = h2.store.rebuild(h2.router).await.unwrap();
    assert_eq!(count_kind(&router_events, EventKind::ThreadStarted), 1);
    let report_input = router_events
        .iter()
        .filter(|e| e.kind == EventKind::InputAdmitted)
        .nth(1)
        .expect("report consumed by the router");
    let report = parse_thread_report(&report_input.bytes["content"]).unwrap();
    assert_eq!(report.thread_id, spawned.thread.id);
    assert_eq!(report.status, "completed");
}

/// A soft-failed worker (empty model response) reports
/// `status: "failed"` honestly: `thread/completed{failed}` + a failed
/// report to the Router, and the session settles `failed` — never
/// `completed`. No gate runs on the failure path.
#[tokio::test]
async fn failed_worker_reports_status_failed_honestly() {
    let h = default_harness(
        vec![vec![]], // empty response → soft failure
        vec![vec![text_chunk("dispatched")], vec![text_chunk("noted")]],
        worker_gate(),
    )
    .await;
    hold_router_turn_until_worker_settles(&h);

    h.engine
        .admit(h.router, json!("please build the thing"), None)
        .await
        .unwrap();

    let spawned = wait_spawned(&h.spawned).await;
    let worker = spawned.worker_session;
    wait_for_events(&h.store, worker, "worker fails honestly", |events| {
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    })
    .await;
    wait_for_events(
        &h.store,
        h.router,
        "router consumes the failed report",
        |events| count_kind(events, EventKind::InputAdmitted) == 2,
    )
    .await;

    let worker_events = h.store.rebuild(worker).await.unwrap();
    assert!(matches!(
        worker_events.last().unwrap().kind,
        EventKind::SessionEnded {
            reason: kian_core::events::EndReason::Failed
        }
    ));
    let completed = worker_events
        .iter()
        .find(|e| e.kind == EventKind::ThreadCompleted)
        .expect("thread/completed records the honest status");
    assert_eq!(completed.bytes["status"], json!("failed"));
    assert_eq!(
        completed.bytes["thread_id"],
        json!(spawned.thread.id.to_string())
    );
    // The failure summary is honest (the soft-failure reason; no
    // assistant message exists to quote).
    assert!(
        completed.bytes["summary"]["text"]
            .as_str()
            .is_some_and(|t| !t.is_empty())
    );
    // No gate on the failure path — no verdict event.
    assert_eq!(count_kind(&worker_events, EventKind::EvidenceVerdict), 0);

    let router_events = h.store.rebuild(h.router).await.unwrap();
    let report_input = router_events
        .iter()
        .filter(|e| e.kind == EventKind::InputAdmitted)
        .nth(1)
        .unwrap();
    let report = parse_thread_report(&report_input.bytes["content"]).unwrap();
    assert_eq!(report.thread_id, spawned.thread.id);
    assert_eq!(report.status, "failed");
}

/// A never-satisfiable gate is bounded (constraint 8): the wrapper buys
/// at most `max_retries` extra provider turns, records each failed
/// verdict in-stream, then settles `failed` — never `completed` with a
/// failed gate. The gate itself makes no model call.
#[tokio::test]
async fn gate_failure_is_bounded_retries_then_failed_never_completed() {
    // The model only ever talks (final answer, no tool result): the
    // `tool_result_since_last_user_input` check can never pass.
    let h = default_harness(
        vec![
            vec![text_chunk("all talk, no tools")],
            vec![text_chunk("still just talking")],
        ],
        vec![vec![text_chunk("dispatched")], vec![text_chunk("noted")]],
        worker_gate(),
    )
    .await;
    hold_router_turn_until_worker_settles(&h);

    h.engine
        .admit(h.router, json!("please build the thing"), None)
        .await
        .unwrap();

    let spawned = wait_spawned(&h.spawned).await;
    let worker = spawned.worker_session;
    wait_for_events(
        &h.store,
        worker,
        "bounded gate exhausts and fails",
        |events| {
            events
                .iter()
                .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
        },
    )
    .await;
    wait_for_events(&h.store, h.router, "router consumes the report", |events| {
        count_kind(events, EventKind::InputAdmitted) == 2
    })
    .await;

    let worker_events = h.store.rebuild(worker).await.unwrap();
    assert!(matches!(
        worker_events.last().unwrap().kind,
        EventKind::SessionEnded {
            reason: kian_core::events::EndReason::Failed
        }
    ));
    // Exactly 1 + max_retries verdicts (attempts 0 and 1), all failed,
    // before thread/completed{failed} and the end write.
    let verdicts: Vec<&Event> = worker_events
        .iter()
        .filter(|e| e.kind == EventKind::EvidenceVerdict)
        .collect();
    assert_eq!(verdicts.len(), 2);
    for (i, verdict) in verdicts.iter().enumerate() {
        assert_eq!(verdict.bytes["gate"], json!("worker"));
        assert_eq!(verdict.bytes["passed"], json!(false));
        assert_eq!(verdict.bytes["attempt"], json!(i));
    }
    let completed = worker_events
        .iter()
        .find(|e| e.kind == EventKind::ThreadCompleted)
        .unwrap();
    assert_eq!(completed.bytes["status"], json!("failed"));
    assert!(
        position_of(&worker_events, |e| e.kind == EventKind::ThreadCompleted).unwrap()
            > worker_events
                .iter()
                .position(|e| e.kind == EventKind::EvidenceVerdict)
                .unwrap()
    );
    // No gate model calls: exactly the two scripted loop turns.
    assert_eq!(h.worker_script.request_count(), 2);
}

/// Structural profile assertion: the worker template's toolset contains
/// no channel tools; the Router/Worker pair validates as a
/// `SuperAgentProfile` (the dispatch/channel surface is Router-only).
#[tokio::test]
async fn worker_toolset_contains_no_channel_tools() {
    use kian_superagent::{CHANNEL_TOOLS, SPAWN_WORKER_TOOL, SuperAgentProfile, is_channel_tool};

    let worker = worker_template("mock", "mock-worker", 7);
    let router = router_template("mock", "mock-router");

    for tool in &worker.tools {
        assert!(
            !is_channel_tool(tool),
            "worker toolset must not contain channel tool `{tool}`"
        );
    }
    for channel in CHANNEL_TOOLS {
        assert!(!worker.tools.contains(&channel.to_string()));
    }
    assert!(!worker.tools.contains(&SPAWN_WORKER_TOOL.to_string()));
    assert!(worker.tools.contains(&"exec".to_string()));
    assert!(router.tools.contains(&SPAWN_WORKER_TOOL.to_string()));

    let profile = SuperAgentProfile::new(router, worker).expect("profile validates");
    assert_eq!(profile.worker_pinned_version(), 7);

    let mut smuggler = profile.clone().worker;
    smuggler.tools.push("reply".into());
    assert!(SuperAgentProfile::new(profile.clone().router, smuggler).is_err());
}

/// The audit helper: the delegation causal pair is discoverable by
/// scanning both streams alone — including for an ungated (default-off
/// gate) worker, and the pair carries the delegated task verbatim.
#[tokio::test]
async fn delegation_pair_discoverable_by_scanning_both_streams() {
    // Gate disabled (default-off): the worker completes ungated, no
    // verdict events — built-in inner behavior unchanged.
    let h = default_harness(
        vec![vec![text_chunk("done without a gate")]],
        vec![vec![text_chunk("dispatched")], vec![text_chunk("noted")]],
        EvidenceGateConfig::default(),
    )
    .await;
    // The Router must still hold its turn when the worker's report lands:
    // a report into an already-ended Router is refused (ADR-083), so this
    // fixture keeps the resident live instead of relying on resurrection.
    hold_router_turn_until_worker_settles(&h);

    h.engine
        .admit(h.router, json!("please build the thing"), None)
        .await
        .unwrap();

    let spawned = wait_spawned(&h.spawned).await;
    let worker = spawned.worker_session;
    wait_for_events(&h.store, worker, "worker completes ungated", |events| {
        events
            .iter()
            .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
    })
    .await;

    let worker_events = h.store.rebuild(worker).await.unwrap();
    let router_events = h.store.rebuild(h.router).await.unwrap();

    // Default-off: no verdicts anywhere.
    assert_eq!(count_kind(&worker_events, EventKind::EvidenceVerdict), 0);
    assert!(
        matches!(
            worker_events.last().unwrap().kind,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Completed
            }
        ),
        "last worker event must be the completed end, got {:?}",
        worker_events.last().map(|event| (event.seq, &event.kind))
    );

    // The pair is derivable from the logs alone.
    let pairs = delegation_pairs(&router_events, &worker_events);
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].thread_id, spawned.thread.id);
    assert_eq!(pairs[0].parent, h.router);
    assert_eq!(pairs[0].worker, worker);
    // The delegated task rides verbatim inside the pair.
    assert_eq!(pairs[0].task["content"]["task"], json!("build the thing"));
}

struct AdmissionPort {
    session: SessionId,
    ts: u64,
    events: Mutex<Vec<Event>>,
    emitted: Mutex<Vec<Event>>,
}

impl AdmissionPort {
    fn shared(session: SessionId) -> (Arc<Self>, Arc<dyn StreamPort>) {
        Self::shared_at(session, now_ms())
    }

    fn shared_at(session: SessionId, ts: u64) -> (Arc<Self>, Arc<dyn StreamPort>) {
        let port = Arc::new(Self {
            session,
            ts,
            events: Mutex::new(Vec::new()),
            emitted: Mutex::new(Vec::new()),
        });
        let dynamic = Arc::clone(&port) as Arc<dyn StreamPort>;
        (port, dynamic)
    }
}

impl StreamPort for AdmissionPort {
    fn session(&self) -> SessionId {
        self.session
    }

    fn emit(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> BoxFuture<'static, Result<(), StreamPortError>> {
        let seq = self.events.lock().unwrap().len() as u64 + 1;
        let event = Event {
            seq,
            session: self.session,
            epoch: Epoch(0),
            ts: self.ts,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        };
        self.events.lock().unwrap().push(event.clone());
        self.emitted.lock().unwrap().push(event);
        Box::pin(async { Ok(()) })
    }

    fn read_stream(&self) -> BoxFuture<'static, Result<Vec<Event>, StreamPortError>> {
        let events = self.events.lock().unwrap().clone();
        Box::pin(async move { Ok(events) })
    }
}

#[derive(Default)]
struct AdmissionInbox {
    admits: Mutex<Vec<(SessionId, Value)>>,
}

impl InboxWriter for AdmissionInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        _idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        self.admits.lock().unwrap().push((session, payload));
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

struct EmptyWorkerStreams;

impl WorkerStreamsPort for EmptyWorkerStreams {
    fn read_stream(
        &self,
        _session: SessionId,
    ) -> BoxFuture<'static, Result<Vec<Event>, WorkerStreamsError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// Two concurrent Router spawns for one worker cross one serialized
/// admission point: exactly one lineage and one child row land, while the
/// loser receives the owner refusal.
#[tokio::test]
async fn concurrent_spawns_for_one_worker_commit_exactly_one_binding() {
    let router = SessionId::new();
    let (port, dynamic_port) = AdmissionPort::shared(router);
    let inbox = Arc::new(AdmissionInbox::default());
    let spawner = Arc::new(WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_template("mock", "mock-worker", 7),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    ));
    let hold = RouterHold::shared();
    hold.install(dynamic_port);
    let mut profile = RouterProfile::new(router, router_template("mock", "mock-router"));
    profile.worker_membership = true;
    let tools = Arc::new(
        RouterTools::new(
            spawner,
            ChannelRegistry::new(),
            &profile,
            hold,
            kian_superagent::ResidentBinding::new(router),
        )
        .with_worker_membership(
            vec!["builder-1".into()],
            std::collections::BTreeMap::from([("builder-1".into(), 60_000)]),
            None,
        )
        .with_worker_streams(Arc::new(EmptyWorkerStreams)),
    );
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let launch = |call_id: &'static str| {
        let tools = Arc::clone(&tools);
        let barrier = Arc::clone(&barrier);
        tokio::spawn(async move {
            barrier.wait().await;
            tools
                .invoke(ToolCall {
                    id: call_id.into(),
                    name: SPAWN_WORKER_TOOL.into(),
                    arguments: json!({
                        "task": format!("task-{call_id}"),
                        "exec_worker": "builder-1",
                    }),
                })
                .await
        })
    };
    let first = launch("call-a");
    let second = launch("call-b");
    barrier.wait().await;
    let (first, second) = tokio::join!(first, second);
    let first = first.unwrap();
    let second = second.unwrap();

    let successes = usize::from(first.is_ok()) + usize::from(second.is_ok());
    assert_eq!(successes, 1, "exactly one spawn wins");
    let refusal = first.err().or_else(|| second.err()).expect("loser refusal");
    assert!(refusal.to_string().contains("worker `builder-1` is busy"));
    assert_eq!(
        port.emitted
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.kind == EventKind::ThreadStarted)
            .count(),
        1
    );
    assert_eq!(inbox.admits.lock().unwrap().len(), 1);
}

#[derive(Default)]
struct ReservationState {
    workers: Mutex<std::collections::BTreeMap<SessionId, Vec<Event>>>,
    created: Mutex<Vec<SessionId>>,
}

struct EagerCreate(Arc<ReservationState>);

impl SpawnSessionWriter for EagerCreate {
    fn create_session(
        &self,
        session: SessionId,
        _assignee: Option<&str>,
    ) -> BoxFuture<'static, Result<(), InboxWriteError>> {
        self.0.created.lock().unwrap().push(session);
        self.0.workers.lock().unwrap().insert(
            session,
            vec![Event {
                seq: 1,
                session,
                epoch: Epoch(0),
                ts: 1,
                kind: EventKind::SessionCreated,
                bytes: json!({}),
                version: SCHEMA_VERSION,
            }],
        );
        Box::pin(async { Ok(()) })
    }
}

struct FailingInbox;

impl InboxWriter for FailingInbox {
    fn admit(
        &self,
        _session: SessionId,
        _payload: Value,
        _idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        Box::pin(async { Err(InboxWriteError::Write("injected admit failure".into())) })
    }

    fn wake(&self, _session: SessionId) -> WakeOutcome {
        WakeOutcome::Folded
    }
}

struct ReservationStreams(Arc<ReservationState>);

impl WorkerStreamsPort for ReservationStreams {
    fn read_stream(
        &self,
        session: SessionId,
    ) -> BoxFuture<'static, Result<Vec<Event>, WorkerStreamsError>> {
        let events = self
            .0
            .workers
            .lock()
            .unwrap()
            .get(&session)
            .cloned()
            .unwrap_or_default();
        Box::pin(async move { Ok(events) })
    }
}

struct ReservedDirectory;

impl WorkerDirectoryPort for ReservedDirectory {
    fn known_ids(&self) -> BoxFuture<'static, Result<Vec<String>, WorkerDirectoryError>> {
        Box::pin(async { Ok(vec!["builder-1".into()]) })
    }

    fn members(&self) -> BoxFuture<'static, Result<Vec<MemberView>, WorkerDirectoryError>> {
        Box::pin(async {
            Ok(vec![MemberView {
                id: "builder-1".into(),
                description: None,
                version: Some(3),
                capabilities: Vec::new(),
                declaration_stale: false,
                state: MemberState::Online,
                last_seen_ms: 1,
            }])
        })
    }

    fn member(
        &self,
        id: &str,
    ) -> BoxFuture<'static, Result<Option<MemberView>, WorkerDirectoryError>> {
        let id = id.to_string();
        Box::pin(async move {
            Ok((id == "builder-1").then_some(MemberView {
                id,
                description: None,
                version: Some(3),
                capabilities: Vec::new(),
                declaration_stale: false,
                state: MemberState::Online,
                last_seen_ms: 1,
            }))
        })
    }
}

/// Eager `session/created` success followed by child-admit failure remains
/// a never-started reservation: it is not listed as bound, holds the slot
/// only for its lease, and then expires.
#[tokio::test]
async fn create_success_then_admit_failure_is_only_a_bounded_reservation() {
    let router = SessionId::new();
    let (parent, dynamic_parent) = AdmissionPort::shared_at(router, 1);
    let state = Arc::new(ReservationState::default());
    let spawner = Arc::new(
        WorkerSpawner::new(
            kian_superagent::ResidentBinding::new(router),
            &worker_template("mock", "mock-worker", 7),
            Arc::new(FailingInbox),
        )
        .with_session_creation(Arc::new(EagerCreate(Arc::clone(&state)))),
    );
    let hold = RouterHold::shared();
    hold.install(dynamic_parent);
    let mut profile = RouterProfile::new(router, router_template("mock", "mock-router"));
    profile.worker_membership = true;
    let streams = Arc::new(ReservationStreams(Arc::clone(&state)));
    let tools = Arc::new(
        RouterTools::new(
            spawner,
            ChannelRegistry::new(),
            &profile,
            hold,
            kian_superagent::ResidentBinding::new(router),
        )
        .with_worker_membership(
            Vec::new(),
            std::collections::BTreeMap::from([("builder-1".into(), 60_000)]),
            Some(Arc::new(ReservedDirectory)),
        )
        .with_worker_streams(Arc::clone(&streams) as Arc<dyn WorkerStreamsPort>),
    );

    let error = tools
        .invoke(ToolCall {
            id: "call-failing-admit".into(),
            name: SPAWN_WORKER_TOOL.into(),
            arguments: json!({
                "task": "fails admission",
                "exec_worker": "builder-1",
            }),
        })
        .await
        .expect_err("admit failure propagates");
    assert!(error.to_string().contains("injected admit failure"));
    let child = *state
        .created
        .lock()
        .unwrap()
        .first()
        .expect("created child");
    let child_events = streams.read_stream(child).await.unwrap();
    assert_eq!(
        thread_liveness(&child_events),
        kian_superagent::ThreadLiveness::NeverStarted
    );

    let listed = tools
        .invoke(ToolCall {
            id: "call-list".into(),
            name: kian_superagent::LIST_WORKERS_TOOL.into(),
            arguments: json!({}),
        })
        .await
        .unwrap();
    assert!(listed.output["workers"][0].get("bound_session").is_none());

    let parent_events = parent.read_stream().await.unwrap();
    let owner =
        kian_superagent::busy_owner(&parent_events, streams.as_ref(), "builder-1", 1, 60_000)
            .await
            .unwrap();
    assert_eq!(owner.expect("reserved").worker_session, child);
    assert!(
        kian_superagent::busy_owner(
            &parent_events,
            streams.as_ref(),
            "builder-1",
            60_001,
            60_000,
        )
        .await
        .unwrap()
        .is_none(),
        "the never-started reservation expires at its lease"
    );
}

struct BindingCheck {
    worker_id: String,
    now_ms: u64,
    ttl_ms: u64,
}

#[derive(Default)]
struct AdmissionEffectState {
    binding: Mutex<Option<kian_superagent::BusyOwner>>,
    checks: Mutex<Vec<BindingCheck>>,
    creates: Mutex<Vec<SessionId>>,
    bootstraps: Mutex<Vec<SessionId>>,
    admits: Mutex<Vec<(SessionId, Value)>>,
    wakes: Mutex<Vec<SessionId>>,
}

struct PortBarriers {
    entered: tokio::sync::Barrier,
    release: tokio::sync::Barrier,
}

struct FakeBindingPort {
    state: Arc<AdmissionEffectState>,
    error: Option<WorkerStreamsError>,
    barriers: Option<Arc<PortBarriers>>,
    first_check: std::sync::atomic::AtomicBool,
}

impl FakeBindingPort {
    fn answering(state: Arc<AdmissionEffectState>) -> Arc<Self> {
        Arc::new(Self {
            state,
            error: None,
            barriers: None,
            first_check: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn failing(state: Arc<AdmissionEffectState>) -> Arc<Self> {
        Arc::new(Self {
            state,
            error: Some(WorkerStreamsError("injected binding read failure".into())),
            barriers: None,
            first_check: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn with_barriers(state: Arc<AdmissionEffectState>, barriers: Arc<PortBarriers>) -> Arc<Self> {
        Arc::new(Self {
            state,
            error: None,
            barriers: Some(barriers),
            first_check: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

impl BindingAdmissionPort for FakeBindingPort {
    fn check_binding(
        &self,
        worker_id: &str,
        _candidate_thread: Uuid,
        _candidate_session: SessionId,
        now_ms: u64,
        ttl_ms: u64,
    ) -> BoxFuture<'static, Result<Option<kian_superagent::BusyOwner>, WorkerStreamsError>> {
        let worker_id = worker_id.to_string();
        let state = Arc::clone(&self.state);
        let error = self.error.clone();
        let barriers = self.barriers.clone();
        let first = !self
            .first_check
            .swap(true, std::sync::atomic::Ordering::SeqCst);
        Box::pin(async move {
            if first && let Some(barriers) = barriers {
                barriers.entered.wait().await;
                barriers.release.wait().await;
            }
            state.checks.lock().unwrap().push(BindingCheck {
                worker_id,
                now_ms,
                ttl_ms,
            });
            if let Some(error) = error {
                return Err(error);
            }
            Ok(state.binding.lock().unwrap().clone())
        })
    }
}

struct RecordingCreate(Arc<AdmissionEffectState>);

impl SpawnSessionWriter for RecordingCreate {
    fn create_session(
        &self,
        session: SessionId,
        _assignee: Option<&str>,
    ) -> BoxFuture<'static, Result<(), InboxWriteError>> {
        self.0.creates.lock().unwrap().push(session);
        Box::pin(async { Ok(()) })
    }
}

struct RecordingBootstrap(Arc<AdmissionEffectState>);

impl SessionBootstrap for RecordingBootstrap {
    fn bootstrap(&self, session: SessionId) -> BoxFuture<'static, Result<(), String>> {
        self.0.bootstraps.lock().unwrap().push(session);
        Box::pin(async { Ok(()) })
    }
}

struct RecordingInbox {
    state: Arc<AdmissionEffectState>,
    bind_on_admit: bool,
}

impl InboxWriter for RecordingInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        _idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        if self.bind_on_admit {
            let thread_id = payload["thread_id"]
                .as_str()
                .and_then(|value| Uuid::parse_str(value).ok())
                .expect("delegation thread id");
            *self.state.binding.lock().unwrap() = Some(kian_superagent::BusyOwner {
                thread_id,
                worker_session: session,
                reason: "live worker session",
            });
        }
        self.state.admits.lock().unwrap().push((session, payload));
        Box::pin(async {
            Ok(AdmitReceipt {
                deduplicated: false,
            })
        })
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.state.wakes.lock().unwrap().push(session);
        WakeOutcome::Folded
    }
}

fn bound_request(task: &str) -> SpawnRequest {
    SpawnRequest {
        task: json!(task),
        exec_worker: Some("builder-1".into()),
        ..SpawnRequest::default()
    }
}

fn admission_spawner(
    router: SessionId,
    state: Arc<AdmissionEffectState>,
    port: Option<Arc<FakeBindingPort>>,
    bind_on_admit: bool,
    ttl_ms: u64,
) -> WorkerSpawner {
    let spawner = WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_template("mock", "mock-worker", 7),
        Arc::new(RecordingInbox {
            state: Arc::clone(&state),
            bind_on_admit,
        }) as Arc<dyn InboxWriter>,
    )
    .with_session_creation(Arc::new(RecordingCreate(Arc::clone(&state))));
    let spawner = match port {
        Some(port) => spawner.with_binding_admission(
            port,
            std::collections::BTreeMap::from([("builder-1".into(), ttl_ms)]),
        ),
        None => spawner,
    };
    spawner.install_bootstrap(Arc::new(RecordingBootstrap(state)));
    spawner
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_bound_admits_share_one_serialized_authority() {
    let state = Arc::new(AdmissionEffectState::default());
    let port_barriers = Arc::new(PortBarriers {
        entered: tokio::sync::Barrier::new(2),
        release: tokio::sync::Barrier::new(2),
    });
    let port = FakeBindingPort::with_barriers(Arc::clone(&state), Arc::clone(&port_barriers));
    let spawner = Arc::new(admission_spawner(
        SessionId::new(),
        Arc::clone(&state),
        Some(port),
        true,
        12_345,
    ));
    let first = spawner.prepare(&bound_request("first"));
    let first_session = first.worker_session;
    let first_thread = first.thread.id;
    let second = spawner.prepare(&bound_request("second"));
    let second_session = second.worker_session;
    let second_thread = second.thread.id;

    let first_task = tokio::spawn({
        let spawner = Arc::clone(&spawner);
        async move { spawner.admit(&first).await }
    });
    port_barriers.entered.wait().await;
    let second_started = Arc::new(tokio::sync::Notify::new());
    let second_task = tokio::spawn({
        let spawner = Arc::clone(&spawner);
        let second_started = Arc::clone(&second_started);
        async move {
            second_started.notify_one();
            spawner.admit(&second).await
        }
    });
    second_started.notified().await;
    port_barriers.release.wait().await;

    let first_result = first_task.await.unwrap();
    let second_result = second_task.await.unwrap();
    let (loser, winner_session, winner_thread) = match (first_result, second_result) {
        (Ok(()), Err(error)) => (error, first_session, first_thread),
        (Err(error), Ok(())) => (error, second_session, second_thread),
        (ours, theirs) => panic!(
            "exactly one admit must win, got first={:?} second={:?}",
            ours.as_ref().err().map(ToString::to_string),
            theirs.as_ref().err().map(ToString::to_string)
        ),
    };
    match loser {
        SpawnError::BindingConflict {
            worker,
            owner_session,
            owner_thread,
        } => {
            assert_eq!(worker, "builder-1");
            assert_eq!(owner_session, winner_session);
            assert_eq!(owner_thread, winner_thread);
        }
        other => panic!("expected binding conflict, got {other}"),
    }

    assert_eq!(
        state.creates.lock().unwrap().as_slice(),
        std::slice::from_ref(&winner_session)
    );
    assert_eq!(
        state.bootstraps.lock().unwrap().as_slice(),
        std::slice::from_ref(&winner_session)
    );
    assert_eq!(
        state
            .admits
            .lock()
            .unwrap()
            .iter()
            .map(|(session, _)| *session)
            .collect::<Vec<_>>()
            .as_slice(),
        std::slice::from_ref(&winner_session)
    );
    assert_eq!(
        state.wakes.lock().unwrap().as_slice(),
        std::slice::from_ref(&winner_session)
    );
    let checks = state.checks.lock().unwrap();
    assert_eq!(checks.len(), 2);
    assert!(checks.iter().all(|check| check.worker_id == "builder-1"));
    assert!(checks.iter().all(|check| check.ttl_ms == 12_345));
    assert!(checks.iter().all(|check| check.now_ms > 0));
}

#[tokio::test]
async fn binding_port_error_is_a_typed_side_effect_free_refusal() {
    let state = Arc::new(AdmissionEffectState::default());
    let spawner = admission_spawner(
        SessionId::new(),
        Arc::clone(&state),
        Some(FakeBindingPort::failing(Arc::clone(&state))),
        true,
        60_000,
    );
    let spawned = spawner.prepare(&bound_request("port failure"));
    let error = spawner.admit(&spawned).await.expect_err("fail closed");

    match error {
        SpawnError::BindingAdmission { worker, error } => {
            assert_eq!(worker, "builder-1");
            assert!(error.to_string().contains("injected binding read failure"));
        }
        other => panic!("expected binding admission error, got {other}"),
    }
    assert_eq!(state.checks.lock().unwrap().len(), 1);
    assert!(state.creates.lock().unwrap().is_empty());
    assert!(state.bootstraps.lock().unwrap().is_empty());
    assert!(state.admits.lock().unwrap().is_empty());
    assert!(state.wakes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unbound_spawns_and_spawners_without_a_port_keep_legacy_behavior() {
    let state = Arc::new(AdmissionEffectState::default());
    *state.binding.lock().unwrap() = Some(kian_superagent::BusyOwner {
        thread_id: Uuid::new_v4(),
        worker_session: SessionId::new(),
        reason: "live worker session",
    });
    let spawner = admission_spawner(
        SessionId::new(),
        Arc::clone(&state),
        Some(FakeBindingPort::answering(Arc::clone(&state))),
        false,
        60_000,
    );
    let unbound = spawner.prepare(&SpawnRequest {
        task: json!("unbound"),
        ..SpawnRequest::default()
    });
    assert_eq!(unbound.exec_worker, None);
    spawner.admit(&unbound).await.unwrap();
    assert!(state.checks.lock().unwrap().is_empty());

    let unwired_state = Arc::new(AdmissionEffectState::default());
    let unwired = admission_spawner(
        SessionId::new(),
        Arc::clone(&unwired_state),
        None,
        false,
        60_000,
    );
    for task in ["first", "second"] {
        let request = bound_request(task);
        let spawned = unwired.prepare(&request);
        assert_eq!(spawned.exec_worker.as_deref(), Some("builder-1"));
        unwired.admit(&spawned).await.unwrap();
    }
    assert_eq!(state.creates.lock().unwrap().len(), 1);
    assert_eq!(state.admits.lock().unwrap().len(), 1);
    assert_eq!(state.wakes.lock().unwrap().len(), 1);
    assert_eq!(unwired_state.creates.lock().unwrap().len(), 2);
    assert_eq!(unwired_state.admits.lock().unwrap().len(), 2);
    assert_eq!(unwired_state.wakes.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn bound_admission_conflicts_until_the_release_view_changes() {
    let state = Arc::new(AdmissionEffectState::default());
    let spawner = admission_spawner(
        SessionId::new(),
        Arc::clone(&state),
        Some(FakeBindingPort::answering(Arc::clone(&state))),
        true,
        60_000,
    );
    let first = spawner.prepare(&bound_request("first"));
    spawner.admit(&first).await.unwrap();

    let second = spawner.prepare(&bound_request("second"));
    let conflict = spawner.admit(&second).await.expect_err("still bound");
    match conflict {
        SpawnError::BindingConflict {
            worker,
            owner_session,
            owner_thread,
        } => {
            assert_eq!(worker, "builder-1");
            assert_eq!(owner_session, first.worker_session);
            assert_eq!(owner_thread, first.thread.id);
        }
        other => panic!("expected binding conflict, got {other}"),
    }

    *state.binding.lock().unwrap() = None;
    let third = spawner.prepare(&bound_request("third"));
    spawner.admit(&third).await.unwrap();
    assert_eq!(state.creates.lock().unwrap().len(), 2);
    assert_eq!(state.admits.lock().unwrap().len(), 2);
    assert_eq!(state.wakes.lock().unwrap().len(), 2);
}
