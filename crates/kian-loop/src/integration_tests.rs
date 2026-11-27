//! Engine integration tests for the built-in ReAct loop (
//! acceptance). Each test drives the *real* engine (sqlite store,
//! admit/wake/drain) with the loop mounted as its `LoopDriver`; the
//! provider and (where useful) the tools are mocks — the engine, store,
//! projections and event ordering are never mocked.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde_json::{Value, json};
use uuid::Uuid;

use kian_core::context::derive_model_context;
use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::{AgentDef, Budgets, Permission, SessionId, SlotName};
use kian_core::seams::{PolicyRule, RecoveryAction, ToolCall, ToolResult};
use kian_core::state::{SessionState, project};
use kian_llm::ProviderRegistry;
use kian_orchestrator::{ArtifactRuntime, BudgetOutcome, Engine, EngineConfig, ReadModelConfig};
use kian_store::EventStore;

use crate::react::{CLASS_EMPTY_RESPONSE, ReActConfig, ReActLoop};
use crate::test_support::{
    HookTools, NoopPolicy, NoopTools, RecordingPolicy, ScriptedProvider, agent_def, text_chunk,
    tool_call_chunk,
};
use crate::tools::LocalTools;

// -- helpers ---------------------------------------------------------------

fn temp_db_path() -> PathBuf {
    std::env::temp_dir().join(format!("kian-loop-test-{}.sqlite", Uuid::new_v4()))
}

fn temp_tools_root() -> PathBuf {
    std::env::temp_dir().join(format!("kian-loop-tools-{}", Uuid::new_v4()))
}

fn cleanup_db(path: &std::path::Path) {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

/// Polls `cond` every 10 ms until it holds or `timeout_ms` elapses.
async fn wait_for<F, Fut>(timeout_ms: u64, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if cond().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

struct Harness {
    engine: Arc<Engine<ReActLoop>>,
    store: EventStore,
    db_path: PathBuf,
}

impl Drop for Harness {
    fn drop(&mut self) {
        // Best-effort cleanup; sqlite files are in the temp dir.
        let _ = &self.db_path;
    }
}

/// Builds a full engine harness: sqlite store, mock provider registry,
/// given tools and policy.
async fn harness(
    turns: Vec<Vec<Result<kian_core::seams::ModelChunk, kian_core::seams::ProviderError>>>,
    tools: Arc<dyn kian_core::seams::ToolProvider>,
    policy: Arc<dyn kian_core::seams::RecoveryPolicySource>,
    config: ReActConfig,
) -> (Harness, Arc<ScriptedProvider>) {
    let db_path = temp_db_path();
    let store = EventStore::open(&db_path).await.expect("open store");
    let provider = Arc::new(ScriptedProvider::new(turns));
    let mut registry = ProviderRegistry::new();
    registry.register("mock", provider.clone());
    let react = ReActLoop::with_config(agent_def(), Arc::new(registry), tools, policy, config);
    let engine = Engine::new(store.clone(), Arc::new(react), EngineConfig::default());
    (
        Harness {
            engine,
            store,
            db_path,
        },
        provider,
    )
}

/// A harness bound to an explicit agent definition (the budget gate reads
/// the agent's declared budgets when the stream pins none).
async fn harness_with_agent(
    agent: AgentDef,
    turns: Vec<Vec<Result<kian_core::seams::ModelChunk, kian_core::seams::ProviderError>>>,
) -> (Harness, Arc<ScriptedProvider>) {
    let db_path = temp_db_path();
    let store = EventStore::open(&db_path).await.expect("open store");
    let provider = Arc::new(ScriptedProvider::new(turns));
    let mut registry = ProviderRegistry::new();
    registry.register("mock", provider.clone());
    let react = ReActLoop::with_config(
        agent,
        Arc::new(registry),
        Arc::new(crate::test_support::NoopTools),
        Arc::new(crate::test_support::NoopPolicy),
        ReActConfig::default(),
    );
    let engine = Engine::new(store.clone(), Arc::new(react), EngineConfig::default());
    (
        Harness {
            engine,
            store,
            db_path,
        },
        provider,
    )
}

async fn events_of(store: &EventStore, sid: SessionId) -> Vec<Event> {
    store.rebuild(sid).await.expect("rebuild")
}

async fn ended_reason(store: &EventStore, sid: SessionId) -> Option<EndReason> {
    let events = events_of(store, sid).await;
    events.last().and_then(|ev| match ev.kind {
        EventKind::SessionEnded { reason } => Some(reason),
        _ => None,
    })
}

fn kinds(events: &[Event]) -> Vec<&'static str> {
    events.iter().map(|e| e.kind.as_str()).collect()
}

async fn submit_and_wait(h: &Harness, sid: SessionId, payload: Value) -> EndReason {
    h.engine.admit(sid, payload, None).await.expect("admit");
    // admit already woke the drain; the extra wake must be harmless
    // (either a fold or a second no-op start).
    let _ = h.engine.wake(sid);
    assert!(
        wait_for(10_000, || async {
            matches!(
                ended_reason(&h.store, sid).await,
                Some(EndReason::Completed | EndReason::Failed | EndReason::Interrupted)
            )
        })
        .await,
        "session never reached a terminal end write"
    );
    ended_reason(&h.store, sid).await.expect("end write")
}

// -- acceptance: multi-turn ReAct to completion -----------------------------

/// Scripted mock provider drives a full multi-turn task (final → tool →
/// tool → final answer) with real built-in fs tools; every expected event
/// is present in order, the request audit copies match the provider's
/// recorded requests byte-for-byte, and the stream replays in a fresh
/// store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scripted_provider_drives_multi_turn_task_to_completion_replayable() {
    let root = temp_tools_root();
    let tools = Arc::new(LocalTools::new(&root));
    let turns = vec![
        // Turn 1: write a file.
        vec![
            text_chunk("checking the file"),
            tool_call_chunk(
                "call-1",
                "fs_write",
                json!({"path": "out.txt", "content": "42"}),
            ),
        ],
        // Turn 2: read it back.
        vec![tool_call_chunk(
            "call-2",
            "fs_read",
            json!({"path": "out.txt"}),
        )],
        // Turn 3: final answer.
        vec![text_chunk("the value is 42")],
    ];
    let (h, provider) = harness(
        turns,
        tools,
        Arc::new(crate::test_support::NoopPolicy),
        ReActConfig::default(),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");

    let reason = submit_and_wait(&h, sid, json!("what is the value?")).await;
    assert_eq!(reason, EndReason::Completed);

    let events = events_of(&h.store, sid).await;
    // The full honest event trail, in order.
    let expected = [
        "session/created",
        "drain/claimed",
        "input/admitted",
        "slot/binding",
        "turn/start",
        "step/request",
        "assistant/message",
        "tool/call",
        "tool/result",
        "turn/end",
        "turn/start",
        "step/request",
        "assistant/message",
        "tool/call",
        "tool/result",
        "turn/end",
        "turn/start",
        "step/request",
        "assistant/message",
        "turn/end",
        "session/ended",
    ];
    assert_eq!(kinds(&events), expected, "unexpected event trail");
    // Iteration counters ride the turn events (0-based, stream-derived).
    for (n, ev) in events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::TurnStart))
        .enumerate()
    {
        assert_eq!(ev.bytes.get("iteration"), Some(&json!(n)));
    }
    // The tool really ran against the built-in local fs provider.
    assert_eq!(
        std::fs::read_to_string(root.join("out.txt")).unwrap(),
        "42",
        "built-in fs tool did not execute"
    );

    // A2: every step/request audit copy equals the bytes the provider saw.
    let requests = provider.requests();
    let step_bodies: Vec<&Value> = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::StepRequest))
        .map(|e| e.bytes.get("body").expect("body present"))
        .collect();
    assert_eq!(step_bodies.len(), requests.len());
    for (audit, sent) in step_bodies.iter().zip(&requests) {
        assert_eq!(audit, &sent);
    }

    // Replay: a fresh store over the same sqlite file projects the same
    // terminal state and re-derives the identical model context.
    let fresh = EventStore::open(&h.db_path).await.expect("reopen");
    let replayed = fresh.rebuild(sid).await.expect("replay");
    assert_eq!(kinds(&replayed), expected);
    assert_eq!(project(&replayed).unwrap(), SessionState::Completed);
    let messages = derive_model_context(&replayed).expect("context replays");
    let roles: Vec<&str> = messages.iter().map(|m| m.role.as_str()).collect();
    // Context-bearing kinds: user input, then per tool-turn the assistant
    // message + its `tool/call` (role assistant) + the `tool/result`
    // (role tool), then the final assistant answer.
    assert_eq!(
        roles,
        [
            "user",
            "assistant",
            "assistant",
            "tool",
            "assistant",
            "assistant",
            "tool",
            "assistant"
        ]
    );
    fresh.close().await;
    let _ = std::fs::remove_dir_all(&root);
    cleanup_db(&h.db_path);
}

// -- acceptance: interrupt at each iteration stage --------------------------

#[derive(Clone, Copy, PartialEq, Debug)]
enum Stage {
    BeforeFirstProviderCall,
    AtFinalTurn,
}

/// Injects an engine interrupt at each provider-stage point; every stage
/// must settle with `end_reason=interrupted` — never `completed`, never
/// `failed` (constraint 7: interrupted ≠ failed ≠ completed). The
/// during-tool stage is covered by its own test below.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_at_each_iteration_stage_lands_interrupted_never_completed_or_failed() {
    for stage in [Stage::BeforeFirstProviderCall, Stage::AtFinalTurn] {
        let turns = vec![
            vec![tool_call_chunk("call-1", "hook", json!({}))],
            vec![text_chunk("done")],
        ];
        let (h, provider) = harness(
            turns,
            Arc::new(crate::test_support::NoopTools),
            Arc::new(crate::test_support::NoopPolicy),
            ReActConfig::default(),
        )
        .await;
        let engine = Arc::clone(&h.engine);
        let sid = h.engine.create_session().await.expect("create");
        let s = sid;

        match stage {
            Stage::BeforeFirstProviderCall => {
                let engine = Arc::clone(&engine);
                provider.on_request(move |_n| {
                    let engine = Arc::clone(&engine);
                    async move {
                        let _ = engine.interrupt(s).await;
                    }
                });
            }
            Stage::AtFinalTurn => {
                let engine = Arc::clone(&engine);
                provider.on_request(move |n| {
                    let engine = Arc::clone(&engine);
                    async move {
                        if n == 2 {
                            let _ = engine.interrupt(s).await;
                        }
                    }
                });
            }
        }

        let reason = submit_and_wait(&h, sid, json!("do it")).await;
        assert_eq!(
            reason,
            EndReason::Interrupted,
            "stage {stage:?} must end interrupted"
        );
        cleanup_db(&h.db_path);
    }
}

/// Interrupt injected from inside a running tool: honored at the next
/// safe boundary, `end_reason=interrupted` (never `completed`/`failed`),
/// with the in-flight tool's result still landing honestly first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_during_tool_execution_lands_interrupted() {
    let db_path = temp_db_path();
    let store = EventStore::open(&db_path).await.expect("store");

    let engine_holder: Arc<Mutex<Option<Arc<Engine<ReActLoop>>>>> = Arc::new(Mutex::new(None));
    let session_cell: Arc<Mutex<Option<SessionId>>> = Arc::new(Mutex::new(None));
    let tools = {
        let engine_cell = Arc::clone(&engine_holder);
        let session_cell = Arc::clone(&session_cell);
        HookTools::new(Arc::new(move |call| {
            let engine_cell = Arc::clone(&engine_cell);
            let session_cell = Arc::clone(&session_cell);
            Box::pin(async move {
                let engine = engine_cell.lock().unwrap().clone();
                let sid = *session_cell.lock().unwrap();
                if let (Some(engine), Some(sid)) = (engine, sid) {
                    // Deterministic: the interrupt lands before the tool
                    // returns, so the next safe-boundary check sees it.
                    let _ = engine.interrupt(sid).await;
                }
                ToolResult {
                    call_id: call.id,
                    output: json!({"ok": true}),
                    is_error: false,
                    redactions: Vec::new(),
                }
            })
        }))
    };

    let provider = Arc::new(ScriptedProvider::new(vec![
        vec![tool_call_chunk("call-1", "hook", json!({}))],
        vec![text_chunk("done")],
    ]));
    let mut registry = ProviderRegistry::new();
    registry.register("mock", provider);
    let react = ReActLoop::new(
        agent_def(),
        Arc::new(registry),
        Arc::new(tools),
        Arc::new(crate::test_support::NoopPolicy),
    );
    let engine = Engine::new(store.clone(), Arc::new(react), EngineConfig::default());
    *engine_holder.lock().unwrap() = Some(Arc::clone(&engine));
    let sid = engine.create_session().await.expect("create");
    *session_cell.lock().unwrap() = Some(sid);

    engine.admit(sid, json!("do it"), None).await.unwrap();
    engine.wake(sid);
    assert!(
        wait_for(10_000, || async {
            matches!(
                ended_reason(&store, sid).await,
                Some(EndReason::Interrupted | EndReason::Completed | EndReason::Failed)
            )
        })
        .await
    );
    let reason = ended_reason(&store, sid).await.unwrap();
    assert_eq!(reason, EndReason::Interrupted);
    // The tool result still landed honestly before the interrupt settle.
    let events = events_of(&store, sid).await;
    assert!(kinds(&events).contains(&"tool/result"));
    assert_eq!(kinds(&events).last(), Some(&"session/ended"));
    cleanup_db(&db_path);
}

// -- acceptance: empty response → soft failure via the policy table ---------

/// A provider turn that streams successfully but yields nothing is a soft
/// failure: the policy table is consulted, the turn/end records
/// `soft_failure`, and the session ends `failed` — never `completed`.
#[tokio::test]
async fn empty_response_is_soft_failure_consulting_policy_never_completed() {
    let turns = vec![vec![]]; // one successful, chunk-free turn
    let policy = Arc::new(RecordingPolicy::with_empty_response_rule());
    let (h, _p) = harness(
        turns,
        Arc::new(crate::test_support::NoopTools),
        policy.clone(),
        ReActConfig::default(),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("hello?")).await;
    assert_eq!(reason, EndReason::Failed, "soft failure must not complete");
    assert_ne!(reason, EndReason::Completed);
    assert!(policy.consulted.load(Ordering::SeqCst), "policy consulted");
    let events = events_of(&h.store, sid).await;
    let soft_end = events
        .iter()
        .find(|e| matches!(e.kind, EventKind::TurnEnd))
        .expect("turn/end present");
    assert_eq!(
        soft_end.bytes.get("outcome"),
        Some(&json!("soft_failure")),
        "the soft failure is observable on turn/end"
    );
    cleanup_db(&h.db_path);
}

/// A provider that surfaces `ProviderError::EmptyResponse` takes the same
/// soft-failure path (constraint 7 both ways).
#[tokio::test]
async fn provider_empty_response_error_is_soft_failure_consulting_policy() {
    let turns = vec![vec![Err(kian_core::seams::ProviderError::EmptyResponse)]];
    let policy = Arc::new(RecordingPolicy::with_empty_response_rule());
    let (h, _p) = harness(
        turns,
        Arc::new(crate::test_support::NoopTools),
        policy.clone(),
        ReActConfig::default(),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("hello?")).await;
    assert_eq!(reason, EndReason::Failed);
    assert!(policy.consulted.load(Ordering::SeqCst));
    cleanup_db(&h.db_path);
}

// -- acceptance: log-before-execute -----------------------------------------

/// At invoke time of every tool call, the matching `tool/call` event is
/// already durable in the stream — the log-before-execute ordering proof.
#[tokio::test]
async fn tool_call_event_is_durable_before_execution() {
    let turns = vec![
        vec![
            tool_call_chunk("call-1", "hook", json!({})),
            // A second call in the same turn proves per-call logging.
            {
                let mut c = tool_call_chunk("call-2", "hook", json!({}));
                if let Ok(chunk) = &mut c {
                    chunk.delta["tool_calls"][0]["index"] = json!(1);
                    chunk.delta["tool_calls"][0]["id"] = json!("call-2");
                }
                c
            },
        ],
        vec![text_chunk("done")],
    ];
    let db_path = temp_db_path();
    let store = EventStore::open(&db_path).await.expect("store");

    // The session id reaches the hook through a side channel (the tool
    // call itself carries no session identity — brain/hand separation).
    let session_cell: Arc<Mutex<Option<SessionId>>> = Arc::new(Mutex::new(None));
    let store_for_hook = store.clone();
    let checks: Arc<Mutex<Vec<bool>>> = Arc::new(Mutex::new(Vec::new()));

    let tools = {
        let cell = Arc::clone(&session_cell);
        let checks = Arc::clone(&checks);
        HookTools::new(Arc::new(move |call: ToolCall| {
            let store = store_for_hook.clone();
            let cell = Arc::clone(&cell);
            let checks = Arc::clone(&checks);
            Box::pin(async move {
                let sid = cell
                    .lock()
                    .unwrap()
                    .expect("session recorded before tools run");
                // At execution time: is THIS call's tool/call event durable?
                let events = store.rebuild(sid).await.expect("rebuild in hook");
                let found = events.iter().any(|ev| {
                    matches!(ev.kind, EventKind::ToolCall)
                        && ev.bytes.pointer("/content/id") == Some(&json!(call.id))
                });
                checks.lock().unwrap().push(found);
                ToolResult {
                    call_id: call.id,
                    output: json!({"ok": true}),
                    is_error: false,
                    redactions: Vec::new(),
                }
            })
        }))
    };

    let provider = Arc::new(ScriptedProvider::new(turns));
    let mut registry = ProviderRegistry::new();
    registry.register("mock", provider);
    let react = ReActLoop::new(
        agent_def(),
        Arc::new(registry),
        Arc::new(tools),
        Arc::new(crate::test_support::NoopPolicy),
    );
    let engine = Engine::new(store.clone(), Arc::new(react), EngineConfig::default());
    let sid = engine.create_session().await.expect("create");
    *session_cell.lock().unwrap() = Some(sid);
    engine.admit(sid, json!("run"), None).await.unwrap();
    engine.wake(sid);
    assert!(
        wait_for(10_000, || async {
            matches!(ended_reason(&store, sid).await, Some(EndReason::Completed))
        })
        .await
    );
    let recorded = checks.lock().unwrap().clone();
    assert!(!recorded.is_empty());
    assert!(
        recorded.iter().all(|found| *found),
        "every tool invocation saw its own durable tool/call event first: {recorded:?}"
    );
    // And the stream order agrees: no tool/result precedes its tool/call.
    let events = events_of(&store, sid).await;
    let call_seq: Vec<u64> = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::ToolCall))
        .map(|e| e.seq)
        .collect();
    let result_seq: Vec<u64> = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::ToolResult))
        .map(|e| e.seq)
        .collect();
    assert_eq!(call_seq.len(), result_seq.len());
    for (c, r) in call_seq.iter().zip(&result_seq) {
        assert!(c < r);
    }
    cleanup_db(&db_path);
}

// -- acceptance: liveness under a sleeping local tool -----------------------

/// A drain running the built-in `exec` tool against a sleeping process
/// must not stall a concurrent drain in another session (spawn_blocking
/// discipline). The provider keys its script by request content so the
/// two concurrent sessions get independent, race-free scripts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sleeping_builtin_exec_tool_does_not_stall_concurrent_drain() {
    use futures::StreamExt;
    use kian_core::seams::{ModelChunk, ModelProvider, ModelRequest};

    /// A: first request ("sleep please") asks for the sleeping exec tool,
    /// follow-up (tool result in context) answers. B: instant answer.
    struct ByContentProvider;
    impl ModelProvider for ByContentProvider {
        fn complete(
            &self,
            req: ModelRequest,
        ) -> futures::stream::BoxStream<'static, Result<ModelChunk, kian_core::seams::ProviderError>>
        {
            let body = serde_json::to_string(&req.body).unwrap_or_default();
            let turn: Vec<Result<ModelChunk, kian_core::seams::ProviderError>> =
                if body.contains("call-sleep") {
                    // A's follow-up: the tool result is already in context.
                    vec![text_chunk("slept")]
                } else if body.contains("sleep please") {
                    vec![tool_call_chunk(
                        "call-sleep",
                        "exec",
                        json!({"argv": ["sleep", "2"]}),
                    )]
                } else if body.contains("quick answer") {
                    vec![text_chunk("quick")]
                } else {
                    vec![text_chunk("done")]
                };
            futures::stream::iter(turn).boxed()
        }
    }

    let root = temp_tools_root();
    let tools = Arc::new(LocalTools::new(&root));
    let db_path = temp_db_path();
    let store = EventStore::open(&db_path).await.expect("store");

    let mut registry = ProviderRegistry::new();
    registry.register("mock", Arc::new(ByContentProvider));
    let react = ReActLoop::new(
        agent_def(),
        Arc::new(registry),
        tools,
        Arc::new(crate::test_support::NoopPolicy),
    );
    let engine = Engine::new(store.clone(), Arc::new(react), EngineConfig::default());

    let a = engine.create_session().await.expect("create a");
    let b = engine.create_session().await.expect("create b");
    engine.admit(b, json!("quick answer"), None).await.unwrap();
    engine.admit(a, json!("sleep please"), None).await.unwrap();

    let started = std::time::Instant::now();
    assert!(
        wait_for(1_000, || async {
            matches!(ended_reason(&store, b).await, Some(EndReason::Completed))
        })
        .await,
        "session B was stalled by session A's sleeping tool (finished nothing in 1s)"
    );
    let b_elapsed = started.elapsed();
    assert!(b_elapsed < Duration::from_millis(1_000));
    // A is still mid-sleep: the sleep is real and B beat it.
    assert!(
        engine.is_draining(&a),
        "session A should still be draining its 2s tool"
    );
    assert!(
        wait_for(15_000, || async {
            matches!(ended_reason(&store, a).await, Some(EndReason::Completed))
        })
        .await,
        "session A never completed after its sleep"
    );
    assert!(started.elapsed() >= Duration::from_secs(2));
    let _ = std::fs::remove_dir_all(&root);
    cleanup_db(&db_path);
}

// -- promote boundaries + steer allowance -----------------------------------

/// A steer admitted mid-turn lands at the next provider-turn boundary
/// (never inside the in-flight turn) and is consumed by the following
/// iteration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn steer_arriving_mid_turn_lands_at_next_boundary_and_is_consumed() {
    let db_path = temp_db_path();
    let store = EventStore::open(&db_path).await.expect("store");
    let engine_holder: Arc<Mutex<Option<Arc<Engine<ReActLoop>>>>> = Arc::new(Mutex::new(None));
    let steer_session: Arc<Mutex<Option<SessionId>>> = Arc::new(Mutex::new(None));
    let tools = {
        let engine_cell = Arc::clone(&engine_holder);
        let session_cell = Arc::clone(&steer_session);
        HookTools::new(Arc::new(move |call| {
            let engine_cell = Arc::clone(&engine_cell);
            let session_cell = Arc::clone(&session_cell);
            Box::pin(async move {
                let engine = engine_cell.lock().unwrap().clone();
                let sid = *session_cell.lock().unwrap();
                if let (Some(engine), Some(sid)) = (engine, sid) {
                    // Steer from inside the tool (mid-turn): must NOT
                    // interleave the in-flight provider turn.
                    let _ = engine
                        .steer(sid, json!("plan B"), None)
                        .await
                        .expect("steer admitted");
                }
                ToolResult {
                    call_id: call.id,
                    output: json!({"ok": true}),
                    is_error: false,
                    redactions: Vec::new(),
                }
            })
        }))
    };

    let provider = Arc::new(ScriptedProvider::new(vec![
        vec![tool_call_chunk("call-1", "hook", json!({}))],
        vec![text_chunk("done with plan B")],
    ]));
    let mut registry = ProviderRegistry::new();
    registry.register("mock", provider.clone());
    let react = ReActLoop::new(
        agent_def(),
        Arc::new(registry),
        Arc::new(tools),
        Arc::new(crate::test_support::NoopPolicy),
    );
    let engine = Engine::new(store.clone(), Arc::new(react), EngineConfig::default());
    *engine_holder.lock().unwrap() = Some(Arc::clone(&engine));

    let sid = engine.create_session().await.expect("create");
    *steer_session.lock().unwrap() = Some(sid);
    engine
        .admit(sid, json!("original task"), None)
        .await
        .unwrap();
    engine.wake(sid);

    assert!(
        wait_for(10_000, || async {
            matches!(ended_reason(&store, sid).await, Some(EndReason::Completed))
        })
        .await
    );

    let events = events_of(&store, sid).await;
    let k = kinds(&events);
    // The synthetic input lands strictly between the first turn/end and
    // the second turn's step/request (promote-boundary proof).
    let turn1_end = events
        .iter()
        .find(|e| matches!(e.kind, EventKind::TurnEnd))
        .map(|e| e.seq)
        .expect("turn 1 end");
    let steer_ev = events
        .iter()
        .find(|e| {
            matches!(e.kind, EventKind::InputAdmitted)
                && e.bytes.get("synthetic") == Some(&json!(true))
        })
        .expect("steer input admitted");
    let step2 = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::StepRequest))
        .nth(1)
        .map(|e| e.seq)
        .expect("second request");
    assert!(
        turn1_end < steer_ev.seq && steer_ev.seq < step2,
        "steer must land between turn/end and the next step/request"
    );
    assert_eq!(steer_ev.bytes.get("content"), Some(&json!("plan B")));
    // The second provider request actually saw the steer.
    let requests = provider.requests();
    let serialized = serde_json::to_string(&requests[1]).unwrap();
    assert!(
        serialized.contains("plan B"),
        "second request lacks the steer"
    );
    // And the transcript context carries the strippable synthetic marker.
    let messages = derive_model_context(&events).unwrap();
    assert!(
        messages
            .iter()
            .any(|m| m.synthetic && m.content == json!("plan B"))
    );
    assert_eq!(k.last(), Some(&"session/ended"));
    cleanup_db(&db_path);
}

// -- stop conditions ---------------------------------------------------------

/// The iteration budget is an intentional stop: exactly `max_iterations`
/// turns run, then the loop stops (the engine settles it `completed` as a
/// deliberate stop condition, with the budget visible in the stream).
#[tokio::test]
async fn iteration_budget_stops_the_loop_after_max_turns() {
    let endless: Vec<Vec<Result<kian_core::seams::ModelChunk, kian_core::seams::ProviderError>>> =
        (0..10)
            .map(|i| vec![tool_call_chunk(&format!("call-{i}"), "hook", json!({}))])
            .collect();
    let (tools, seen) = HookTools::ok(json!({"ok": true}));
    let (h, _p) = harness(
        endless,
        Arc::new(tools),
        Arc::new(crate::test_support::NoopPolicy),
        ReActConfig {
            slot: SlotName("main".into()),
            max_iterations: 2,
            budget_scope: crate::react::BudgetScope::Lifetime,
            vision: crate::react::VisionBudget::default(),
            permissions: std::collections::BTreeMap::new(),
        },
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("loop forever")).await;
    assert_eq!(reason, EndReason::Completed, "budget stop is intentional");
    let events = events_of(&h.store, sid).await;
    let turns = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::TurnStart))
        .count();
    assert_eq!(turns, 2, "exactly max_iterations turns may run");
    assert_eq!(seen.lock().unwrap().len(), 2);
    cleanup_db(&h.db_path);
}

// -- session token budget gate (ADR-055) ------------------------------------

/// A pinned session token budget refuses the next request BEFORE the
/// `step/request` is persisted: the drain ends the resumable
/// `awaiting_budget`, the provider is never called, and the gate's
/// projection rides `turn/end`. The pin is authoritative over the
/// agent's (absent) declaration.
#[tokio::test]
async fn token_budget_gate_ends_awaiting_budget_before_the_request() {
    let (h, provider) = harness_with_agent(agent_def(), vec![vec![text_chunk("unused")]]).await;
    let sid = h
        .engine
        .create_session_with(SessionId::new(), json!({"budgets": {"tokens": 1}}))
        .await
        .expect("create");
    h.engine
        .admit(sid, json!("hello"), None)
        .await
        .expect("admit");
    assert!(
        wait_for(10_000, || async {
            ended_reason(&h.store, sid).await == Some(EndReason::AwaitingBudget)
        })
        .await,
        "the token budget gate never ended the drain"
    );
    assert!(
        provider.requests().is_empty(),
        "a request flew past the budget gate"
    );
    let events = events_of(&h.store, sid).await;
    assert_eq!(
        kinds(&events),
        [
            "session/created",
            "drain/claimed",
            "input/admitted",
            "slot/binding",
            "turn/start",
            "turn/end",
            "session/ended",
        ],
        "a refused request must not persist a step/request"
    );
    let turn_end = events
        .iter()
        .find(|ev| ev.kind == EventKind::TurnEnd)
        .expect("turn/end");
    assert_eq!(turn_end.bytes["outcome"], json!("awaiting_budget"));
    assert!(
        turn_end.bytes["budget"]["estimate"].as_u64().unwrap_or(0) > 1,
        "turn/end carries the worst-case estimate the gate refused"
    );
    cleanup_db(&h.db_path);
}

/// A durable budget raise is the wake that resumes the `awaiting_budget`
/// settle: the next drain consumes `budget/raised`, the gate passes, and
/// exactly one request flies to completion.
#[tokio::test]
async fn a_durable_budget_raise_resumes_the_awaiting_budget_session() {
    let mut agent = agent_def();
    agent.budgets = Budgets {
        tokens: Some(1),
        wall_clock_ms: None,
    };
    let (h, provider) = harness_with_agent(agent, vec![vec![text_chunk("done")]]).await;
    let sid = h.engine.create_session().await.expect("create");
    h.engine
        .admit(sid, json!("hello"), None)
        .await
        .expect("admit");
    assert!(
        wait_for(10_000, || async {
            ended_reason(&h.store, sid).await == Some(EndReason::AwaitingBudget)
        })
        .await,
        "the agent-declared budget never gated the drain"
    );
    assert_eq!(
        h.engine
            .raise_budget(
                sid,
                Some(1_000_000),
                None,
                Some("resume".into()),
                Some("test".into()),
                None,
            )
            .await
            .expect("raise"),
        BudgetOutcome::Accepted
    );
    assert!(
        wait_for(10_000, || async {
            ended_reason(&h.store, sid).await == Some(EndReason::Completed)
        })
        .await,
        "the raise did not resume the awaiting_budget session"
    );
    assert_eq!(
        provider.requests().len(),
        1,
        "the resumed drain flies exactly one request"
    );
    let events = events_of(&h.store, sid).await;
    assert_eq!(
        events
            .iter()
            .filter(|ev| ev.kind == EventKind::BudgetRaised)
            .count(),
        1,
        "the raise is one durable budget/raised event"
    );
    assert!(
        kinds(&events).contains(&"assistant/message"),
        "the resumed request produced a real answer"
    );
    cleanup_db(&h.db_path);
}

/// A policy table with no matching rule is still consulted (observable)
/// and the soft failure stands on its own.
#[tokio::test]
async fn soft_failure_without_matching_rule_still_consults_the_table() {
    let turns = vec![vec![]];
    let policy = Arc::new(RecordingPolicy {
        rules: vec![PolicyRule {
            class: "rate_limited".into(),
            action: RecoveryAction::BackoffRetry { delay_ms: 1 },
        }],
        consulted: std::sync::atomic::AtomicBool::new(false),
    });
    let (h, _p) = harness(
        turns,
        Arc::new(crate::test_support::NoopTools),
        policy.clone(),
        ReActConfig::default(),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("hello?")).await;
    assert_eq!(reason, EndReason::Failed);
    assert!(policy.consulted.load(Ordering::SeqCst));
    assert_ne!(CLASS_EMPTY_RESPONSE, "rate_limited");
    cleanup_db(&h.db_path);
}

// -- acceptance: turn/end audit enrichment (usage + recovery) ------------

use crate::test_support::usage_chunk;
use kian_core::context::Message;

fn turn_end_payloads(events: &[Event]) -> Vec<Value> {
    events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::TurnEnd))
        .map(|e| e.bytes.clone())
        .collect()
}

/// Every `turn/end` variant carries the verbatim provider usage when
/// observed (final answer and tool-call turns alike), legacy keys stay
/// untouched, and the usage fact is drain-activity only: the projected
/// model context is byte-for-byte the legacy shape (A2/A3 untouched).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_end_carries_usage_for_tool_calls_and_final_answer() {
    let usage1 = json!({"prompt_tokens": 11, "completion_tokens": 4});
    let usage2 = json!({"prompt_tokens": 30, "completion_tokens": 9});
    let turns = vec![
        vec![
            text_chunk("checking"),
            tool_call_chunk("call-1", "fs_read", json!({"path": "out.txt"})),
            usage_chunk(usage1.clone()),
        ],
        vec![text_chunk("the value is 42"), usage_chunk(usage2.clone())],
    ];
    let (h, _p) = harness(
        turns,
        Arc::new(crate::test_support::NoopTools),
        Arc::new(crate::test_support::NoopPolicy),
        ReActConfig::default(),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("what is the value?")).await;
    assert_eq!(reason, EndReason::Completed);

    let events = events_of(&h.store, sid).await;
    let payloads = turn_end_payloads(&events);
    assert_eq!(payloads.len(), 2);

    // Tool-call turn: legacy keys unchanged + verbatim usage.
    assert_eq!(payloads[0]["iteration"], json!(0));
    assert_eq!(payloads[0]["tool_calls"], json!(1));
    assert_eq!(payloads[0]["usage"], usage1, "verbatim, not normalized");
    assert_eq!(payloads[0].as_object().unwrap().len(), 3);

    // Final-answer turn: same contract.
    assert_eq!(payloads[1]["iteration"], json!(1));
    assert_eq!(payloads[1]["outcome"], json!("final_answer"));
    assert_eq!(payloads[1]["usage"], usage2);
    assert_eq!(payloads[1].as_object().unwrap().len(), 3);

    // A2/context golden: the usage fact never enters the model context —
    // `derive_model_context` bytes are exactly the legacy shape (here:
    // the NoopTools error transcript, projected as before).
    let context = derive_model_context(&events).expect("context");
    assert_eq!(
        context,
        vec![
            Message {
                role: "user".into(),
                content: json!("what is the value?"),
                synthetic: false,
            },
            Message {
                role: "assistant".into(),
                content: json!({"text": "checking", "tool_calls": [
                    {"id": "call-1", "name": "fs_read", "arguments": {"path": "out.txt"}}
                ]}),
                synthetic: false,
            },
            Message {
                role: "assistant".into(),
                content: json!({"id": "call-1", "name": "fs_read", "arguments": {"path": "out.txt"}}),
                synthetic: false,
            },
            Message {
                role: "tool".into(),
                content: json!({"call_id": "call-1", "output": {"error": "invalid tool call: noop"}, "is_error": true}),
                synthetic: false,
            },
            Message {
                role: "assistant".into(),
                content: json!({"text": "the value is 42"}),
                synthetic: false,
            },
        ]
    );
    let context_bytes = kian_core::context::context_to_bytes(&context);
    assert!(
        serde_json::from_slice::<Value>(&context_bytes)
            .expect("context bytes are json")
            .to_string()
            .find("prompt_tokens")
            .is_none(),
        "usage must never leak into context bytes"
    );
    cleanup_db(&h.db_path);
}

/// A soft failure mid-stream (provider error) after a usage frame keeps
/// the partial usage on `turn/end` and records the structured recovery
/// audit: the pure classifier's class plus the declarative policy's
/// action.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_end_soft_failure_carries_partial_usage_and_classified_error() {
    let turns = vec![vec![
        usage_chunk(json!({"prompt_tokens": 7, "completion_tokens": 0})),
        Err(kian_core::seams::ProviderError::RateLimited),
    ]];
    let policy = Arc::new(RecordingPolicy {
        rules: vec![PolicyRule {
            class: "rate_limited".into(),
            action: RecoveryAction::BackoffRetry { delay_ms: 1 },
        }],
        consulted: std::sync::atomic::AtomicBool::new(false),
    });
    let (h, _p) = harness(
        turns,
        Arc::new(crate::test_support::NoopTools),
        policy,
        ReActConfig::default(),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("hello?")).await;
    assert_eq!(reason, EndReason::Failed);

    let events = events_of(&h.store, sid).await;
    let payloads = turn_end_payloads(&events);
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0]["iteration"], json!(0));
    assert_eq!(payloads[0]["outcome"], json!("soft_failure"));
    assert_eq!(
        payloads[0]["usage"],
        json!({"prompt_tokens": 7, "completion_tokens": 0}),
        "usage observed before the failure still lands on turn/end"
    );
    assert_eq!(
        payloads[0]["error"],
        json!({"class": "rate_limited", "action": "backoff_retry"}),
        "class from the pure classifier, action from the policy table"
    );
    cleanup_db(&h.db_path);
}

/// Empty response (no usage, no rule match): the audit carries the
/// `empty_response` class and action `"none"` — the honest no-rule
/// spelling — and no `usage` key appears.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_end_soft_failure_without_rule_records_none_action() {
    let turns = vec![vec![]];
    let policy = Arc::new(RecordingPolicy {
        rules: vec![PolicyRule {
            class: "rate_limited".into(),
            action: RecoveryAction::Fail,
        }],
        consulted: std::sync::atomic::AtomicBool::new(false),
    });
    let (h, _p) = harness(
        turns,
        Arc::new(crate::test_support::NoopTools),
        policy,
        ReActConfig::default(),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("hello?")).await;
    assert_eq!(reason, EndReason::Failed);

    let events = events_of(&h.store, sid).await;
    let payloads = turn_end_payloads(&events);
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0]["outcome"], json!("soft_failure"));
    assert!(
        payloads[0].get("usage").is_none(),
        "no usage observed → no usage key"
    );
    assert_eq!(
        payloads[0]["error"],
        json!({"class": "empty_response", "action": "none"})
    );
    cleanup_db(&h.db_path);
}

/// No usage observed → the success-path `turn/end` payloads serialize
/// byte-identically to the legacy shapes (additive keys only,
/// never a reordered or renamed legacy key).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turn_end_without_usage_is_byte_identical_to_the_legacy_shape() {
    let turns = vec![
        vec![tool_call_chunk("call-1", "fs_read", json!({"path": "x"}))],
        vec![text_chunk("done")],
    ];
    let (h, _p) = harness(
        turns,
        Arc::new(crate::test_support::NoopTools),
        Arc::new(crate::test_support::NoopPolicy),
        ReActConfig::default(),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("go")).await;
    assert_eq!(reason, EndReason::Completed);

    let events = events_of(&h.store, sid).await;
    let payloads = turn_end_payloads(&events);
    assert_eq!(payloads.len(), 2);
    assert_eq!(
        serde_json::to_string(&payloads[0]).unwrap(),
        r#"{"iteration":0,"tool_calls":1}"#
    );
    assert_eq!(
        serde_json::to_string(&payloads[1]).unwrap(),
        r#"{"iteration":1,"outcome":"final_answer"}"#
    );
    cleanup_db(&h.db_path);
}

// -- acceptance: per-call tool permission provenance --------------------

/// Extracts the `tool/call` payloads from a stream.
fn tool_call_payloads(events: &[Event]) -> Vec<Value> {
    events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::ToolCall))
        .map(|e| e.bytes.clone())
        .collect()
}

/// allow path: a non-empty permissions table decides every call —
/// an explicit `allow` is `policy: configured`, an unnamed tool falls
/// through to the `policy: default` allow — the tool is invoked as
/// today in both arms and provenance rides the payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn allow_path_invokes_tool_and_records_provenance_default_vs_configured() {
    let turns = vec![
        vec![
            tool_call_chunk("call-1", "fs_read", json!({"path": "a.txt"})),
            // Second call in the same turn rides stream index 1 (the
            // helper hardcodes index 0, which would merge the two).
            Ok(kian_core::seams::ModelChunk::new(json!({"tool_calls": [{
                "index": 1,
                "id": "call-2",
                "function": {"name": "exec", "arguments": "{\"argv\": [\"true\"]}"}
            }]}))),
        ],
        vec![text_chunk("done")],
    ];
    let (tools, seen) = crate::test_support::HookTools::ok(json!({"ok": true}));
    let config = ReActConfig {
        vision: crate::react::VisionBudget::default(),
        permissions: BTreeMap::from([("fs_read".to_string(), Permission::Allow)]),
        ..ReActConfig::default()
    };
    let (h, _p) = harness(
        turns,
        Arc::new(tools),
        Arc::new(crate::test_support::NoopPolicy),
        config,
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("go")).await;
    assert_eq!(reason, EndReason::Completed, "allow never aborts the loop");

    // Both calls invoked (the spy provider served them).
    let invoked = seen.lock().unwrap().len();
    assert_eq!(invoked, 2, "allow arms invoke as today");

    // Provenance: configured allow + default allow.
    let events = events_of(&h.store, sid).await;
    let calls = tool_call_payloads(&events);
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0]["permission"],
        json!({"evaluated": "allow", "policy": "configured", "matched": "fs_read"})
    );
    assert_eq!(
        calls[0]["content"],
        json!({"id": "call-1", "name": "fs_read", "arguments": {"path": "a.txt"}})
    );
    assert_eq!(
        calls[1]["permission"],
        json!({"evaluated": "allow", "policy": "default", "matched": "exec"})
    );
    cleanup_db(&h.db_path);
}

/// deny path (fail-closed): the denied tool is NEVER invoked (the
/// spy counts zero), the `tool/call` carries the deny provenance with
/// its reason, a synthetic honest error `tool/result` lands next to it,
/// and the loop continues — the model reacts and the session completes,
/// never aborts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deny_path_does_not_invoke_and_emits_honest_error_result() {
    let turns = vec![
        vec![tool_call_chunk(
            "call-1",
            "fs_write",
            json!({"path": "a.txt", "content": "x"}),
        )],
        vec![text_chunk("handled the denial, done")],
    ];
    let (tools, seen) = crate::test_support::HookTools::ok(json!({"ok": true}));
    let config = ReActConfig {
        vision: crate::react::VisionBudget::default(),
        permissions: BTreeMap::from([("fs_write".to_string(), Permission::Deny)]),
        ..ReActConfig::default()
    };
    let (h, _p) = harness(
        turns,
        Arc::new(tools),
        Arc::new(crate::test_support::NoopPolicy),
        config,
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("go")).await;
    assert_eq!(
        reason,
        EndReason::Completed,
        "a denied call is a tool failure the model reacts to, never a loop abort"
    );

    // Zero invocations — the side effect never happened.
    assert!(seen.lock().unwrap().is_empty(), "deny must not invoke");

    let events = events_of(&h.store, sid).await;
    // The deny provenance on the logged call (log-before-execute holds:
    // the attempted call is durable even though it never ran).
    let calls = tool_call_payloads(&events);
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0]["permission"],
        json!({
            "evaluated": "deny",
            "policy": "configured",
            "matched": "fs_write",
            "reason": "tool_denied_by_policy"
        })
    );
    assert_eq!(calls[0]["content"]["name"], json!("fs_write"));
    // The synthetic honest error result.
    let results: Vec<&Event> = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::ToolResult))
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].bytes,
        json!({
            "role": "tool",
            "content": {
                "call_id": "call-1",
                "output": {"error": "permission denied by policy: fs_write", "policy": "deny"},
                "is_error": true
            }
        })
    );
    // The model saw the honest error in its next-turn context
    // (request bytes changed ONLY through this new tool/result — the
    // provenance key itself never projects; asserted separately).
    let messages = derive_model_context(&events).unwrap();
    let tool_msg = messages.iter().find(|m| m.role == "tool").unwrap();
    assert_eq!(tool_msg.content["is_error"], json!(true));
    assert_eq!(
        tool_msg.content["output"]["error"],
        json!("permission denied by policy: fs_write")
    );
    cleanup_db(&h.db_path);
}

/// (constraint 3): with NO permissions table configured the
/// `tool/call` payload is byte-identical to the legacy literal — no
/// `permission` key at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn absent_permissions_keep_tool_call_payload_byte_identical_to_today() {
    let turns = vec![
        vec![tool_call_chunk(
            "call-1",
            "fs_read",
            json!({"path": "a.txt"}),
        )],
        vec![text_chunk("done")],
    ];
    let (tools, _seen) = crate::test_support::HookTools::ok(json!({"ok": true}));
    let (h, _p) = harness(
        turns,
        Arc::new(tools),
        Arc::new(crate::test_support::NoopPolicy),
        ReActConfig::default(),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("go")).await;
    assert_eq!(reason, EndReason::Completed);

    let events = events_of(&h.store, sid).await;
    let calls = tool_call_payloads(&events);
    assert_eq!(calls.len(), 1);
    assert_eq!(
        serde_json::to_string(&calls[0]).unwrap(),
        r#"{"content":{"arguments":{"path":"a.txt"},"id":"call-1","name":"fs_read"},"role":"assistant"}"#,
        "absent config → exactly the legacy payload, no permission key"
    );
    cleanup_db(&h.db_path);
}

// -- acceptance: the five ReAct hook ports (PLUGIN-SPEC §4 rows b-f) -------

use futures::FutureExt;
use kian_plugin::hooks::{HandlerId, HookBus, HookId, HookPayload, HookPorts, HookReply, Registry};

/// [`harness`] with hook ports injected (the bus over a private
/// registry, so tier-0 registrations are isolated per test).
async fn harness_hooks(
    turns: Vec<Vec<Result<kian_core::seams::ModelChunk, kian_core::seams::ProviderError>>>,
    tools: Arc<dyn kian_core::seams::ToolProvider>,
    policy: Arc<dyn kian_core::seams::RecoveryPolicySource>,
    config: ReActConfig,
    registry: Arc<Registry>,
) -> (Harness, Arc<ScriptedProvider>) {
    let db_path = temp_db_path();
    let store = EventStore::open(&db_path).await.expect("open store");
    let provider = Arc::new(ScriptedProvider::new(turns));
    let mut providers = ProviderRegistry::new();
    providers.register("mock", provider.clone());
    let react = ReActLoop::with_hooks(
        agent_def(),
        Arc::new(providers),
        tools,
        policy,
        config,
        crate::evidence::EvidenceGateConfig::default(),
        Some(Arc::new(HookBus::with_registry(registry)) as Arc<dyn HookPorts>),
    );
    let engine = Engine::new(store.clone(), Arc::new(react), EngineConfig::default());
    (
        Harness {
            engine,
            store,
            db_path,
        },
        provider,
    )
}

/// A recording tier-0 handler: appends `label` to `log` per dispatch
/// and answers `reply`.
fn recording_handler(
    registry: &Arc<Registry>,
    plugin: &'static str,
    hook: HookId,
    grants: &[&str],
    reply: HookReply,
    label: &'static str,
    log: Arc<Mutex<Vec<&'static str>>>,
) -> kian_plugin::Disposer {
    registry.register(
        HandlerId {
            plugin,
            hook,
            grants: kian_plugin::hooks::GrantSet::from_names(grants).unwrap(),
            order: 0,
        },
        Arc::new(move |_payload: &HookPayload| {
            let log = Arc::clone(&log);
            let reply = reply.clone();
            async move {
                log.lock().unwrap().push(label);
                reply
            }
            .boxed()
        }),
    )
}

/// The P2 acceptance: a tier-0 deny at `tool/pre` yields an
/// honest-error `tool/result`, the call is logged before any effect
/// with `policy:"hook"` provenance naming the denying plugin, and the
/// invoke never runs — while every observe port still sees its
/// dispatch exactly once per its own unit (§4.2/§4.3/§4.4 step 5).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tier0_deny_at_tool_pre_yields_honest_error_result() {
    let registry = Arc::new(Registry::new());
    let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));

    let _keep_gate = recording_handler(
        &registry,
        "approval-gate",
        HookId::ToolPre,
        &["observe:tool", "veto:tool"],
        HookReply::BailDeny {
            reason: "exec not on the allowlist".to_string(),
        },
        "tool_pre",
        Arc::clone(&log),
    );
    let _keep_req = recording_handler(
        &registry,
        "auditor",
        HookId::TurnRequest,
        &["observe:turn"],
        HookReply::Ack { note: None },
        "request",
        Arc::clone(&log),
    );
    let _keep_resp = recording_handler(
        &registry,
        "auditor",
        HookId::TurnResponse,
        &["observe:turn"],
        HookReply::Ack { note: None },
        "response",
        Arc::clone(&log),
    );
    let _keep_post = recording_handler(
        &registry,
        "auditor",
        HookId::ToolPost,
        &["observe:tool"],
        HookReply::Ack { note: None },
        "tool_post",
        Arc::clone(&log),
    );

    let turns = vec![
        vec![tool_call_chunk("call-1", "exec", json!({"cmd": ["ls"]}))],
        vec![text_chunk("done")],
    ];
    let (tools, seen) = crate::test_support::HookTools::ok(json!({"ok": true}));
    let (h, _p) = harness_hooks(
        turns,
        Arc::new(tools),
        Arc::new(crate::test_support::NoopPolicy),
        ReActConfig::default(),
        registry,
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("go")).await;
    assert_eq!(reason, EndReason::Completed);

    // The invoke never ran — only the honest-error result exists.
    assert!(
        seen.lock().unwrap().is_empty(),
        "a hook-denied call never reaches tools.invoke"
    );
    let events = events_of(&h.store, sid).await;
    let calls = tool_call_payloads(&events);
    assert_eq!(calls.len(), 1, "the denied call is still logged");
    assert_eq!(
        calls[0]["permission"],
        json!({
            "evaluated": "deny",
            "policy": "hook",
            "matched": "approval-gate:tool/pre",
            "reason": "exec not on the allowlist",
        }),
        "§4.4 step 3 + §16.1: hook provenance names the plugin:hook"
    );
    let results: Vec<Value> = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::ToolResult))
        .map(|e| e.bytes["content"].clone())
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0],
        json!({
            "call_id": "call-1",
            "output": {
                "error": "denied by plugin approval-gate: exec not on the allowlist",
                "policy": "hook",
            },
            "is_error": true,
        }),
        "§4.4 step 4: the model receives the honest-error tool/result"
    );

    // The ports fired in the §4.2/§4.3 cadence: one request and one
    // response per provider turn (two turns), one tool/pre and one
    // tool/post for the denied call.
    let fired = log.lock().unwrap().clone();
    assert_eq!(
        fired.iter().filter(|l| **l == "request").count(),
        2,
        "turn/request fires exactly once per provider turn"
    );
    assert_eq!(
        fired.iter().filter(|l| **l == "response").count(),
        2,
        "turn/response fires exactly once per settled provider turn"
    );
    assert_eq!(
        fired.iter().filter(|l| **l == "tool_pre").count(),
        1,
        "tool/pre consulted once for the call"
    );
    assert_eq!(
        fired.iter().filter(|l| **l == "tool_post").count(),
        1,
        "tool/post observes the hook-denied arm too (uniform surface)"
    );
    cleanup_db(&h.db_path);
}

/// Row (f): a soft failure's class → action resolution observes
/// `recover/decided` with the same `{class, action}` pair the
/// `turn/end` audit carries, and the session still ends honestly
/// `failed` (constraint 7 — the observation changes nothing).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recover_decided_observes_the_resolution_of_a_soft_failure() {
    let registry = Arc::new(Registry::new());
    let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    let _keep = recording_handler(
        &registry,
        "auditor",
        HookId::RecoverDecided,
        &["observe:recover"],
        HookReply::Ack { note: None },
        "recover",
        Arc::clone(&log),
    );

    // An empty scripted turn: the empty-response soft failure.
    let turns = vec![Vec::new()];
    let (tools, _seen) = crate::test_support::HookTools::ok(json!({"ok": true}));
    let policy = crate::test_support::RecordingPolicy::with_empty_response_rule();
    let (h, _p) = harness_hooks(
        turns,
        Arc::new(tools),
        Arc::new(policy),
        ReActConfig::default(),
        registry,
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("go")).await;
    assert_eq!(reason, EndReason::Failed, "honest failure, unchanged");

    assert_eq!(
        log.lock()
            .unwrap()
            .iter()
            .filter(|l| **l == "recover")
            .count(),
        1,
        "the empty_response resolution observed exactly once"
    );
    let events = events_of(&h.store, sid).await;
    let turn_end = events
        .iter()
        .find(|e| matches!(e.kind, EventKind::TurnEnd))
        .expect("turn/end exists");
    assert_eq!(
        turn_end.bytes["error"],
        json!({"class": "empty_response", "action": "interrupt_ask"}),
        "the audit object the view mirrors"
    );
    cleanup_db(&h.db_path);
}

/// §12.1 (M4-b): the persisted `step/request` bytes are identical with
/// a hook **mounted and observing** and with no hooks at all — the
/// interception ports are metadata-only, never a request-body input.
/// (§13.3's `plugin_absent_is_byte_identical` compares section-absent
/// builds; this compares the same build with a mounted, observing
/// subscriber set.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_bytes_identical_with_hook_mounted_observing_vs_absent() {
    async fn scenario(hooks: bool) -> (PathBuf, Vec<String>, Vec<SessionId>) {
        let turns = vec![
            vec![tool_call_chunk("call-1", "exec", json!({"cmd": ["ls"]}))],
            vec![text_chunk("done")],
        ];
        let registry = Arc::new(Registry::new());
        if hooks {
            let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
            for (hook, grants) in [
                (HookId::TurnRequest, &["observe:turn"][..]),
                (HookId::TurnResponse, &["observe:turn"][..]),
                (HookId::ToolPre, &["observe:tool"][..]),
                (HookId::ToolPost, &["observe:tool"][..]),
                (HookId::RecoverDecided, &["observe:recover"][..]),
            ] {
                let _keep = recording_handler(
                    &registry,
                    "auditor",
                    hook,
                    grants,
                    HookReply::Ack { note: None },
                    "observe",
                    Arc::clone(&log),
                );
            }
        }
        let (tools, _seen) = crate::test_support::HookTools::ok(json!({"ok": true}));
        let config = ReActConfig {
            vision: crate::react::VisionBudget::default(),
            permissions: BTreeMap::from([("fs_read".to_string(), Permission::Allow)]),
            ..ReActConfig::default()
        };
        let (h, _p) = if hooks {
            harness_hooks(
                turns,
                Arc::new(tools),
                Arc::new(crate::test_support::NoopPolicy),
                config,
                registry,
            )
            .await
        } else {
            harness(
                turns,
                Arc::new(tools),
                Arc::new(crate::test_support::NoopPolicy),
                config,
            )
            .await
        };
        let sid = h.engine.create_session().await.expect("create");
        let reason = submit_and_wait(&h, sid, json!("go")).await;
        assert_eq!(reason, EndReason::Completed);
        let events = events_of(&h.store, sid).await;
        let bodies: Vec<String> = events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::StepRequest))
            .map(|e| serde_json::to_string(&e.bytes).unwrap())
            .collect();
        let db_path = h.db_path.clone();
        // keep the harness's store alive until the bodies are read
        drop(h);
        (db_path, bodies, vec![sid])
    }

    let (_db_with, with_hooks, _) = scenario(true).await;
    let (_db_without, without, _) = scenario(false).await;
    assert_eq!(
        with_hooks, without,
        "§12.1: a mounted, observing subscriber set changes zero request bytes"
    );
}

/// §4.4 step 5 (M4-e) + §4.4 step 2 (M3): `tool/pre` is consulted and
/// `tool/post` observes on **every** arm — the executed call, the
/// declared-deny call (consulted unconditionally, its payload carrying
/// the declarative verdict) and the hook-denied call alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tool_pre_and_post_cover_executed_and_declared_denied_arms() {
    let registry = Arc::new(Registry::new());
    let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    // Observing-only subscribers on both tool ports; the abstaining
    // tool/pre leaves the declarative decision standing.
    let _keep_pre = recording_handler(
        &registry,
        "auditor",
        HookId::ToolPre,
        &["observe:tool"],
        HookReply::BailNone,
        "tool_pre",
        Arc::clone(&log),
    );
    let _keep_post = recording_handler(
        &registry,
        "auditor",
        HookId::ToolPost,
        &["observe:tool"],
        HookReply::Ack { note: None },
        "tool_post",
        Arc::clone(&log),
    );

    let turns = vec![
        // Turn 1: an executed call (fs_read allowed by the table).
        vec![tool_call_chunk(
            "call-ok",
            "fs_read",
            json!({"path": "a.txt"}),
        )],
        // Turn 2: a declared-deny call (fs_write denied by the table).
        vec![tool_call_chunk(
            "call-deny",
            "fs_write",
            json!({"path": "a.txt", "content": "x"}),
        )],
        vec![text_chunk("done")],
    ];
    let (tools, seen) = crate::test_support::HookTools::ok(json!({"ok": true}));
    let config = ReActConfig {
        vision: crate::react::VisionBudget::default(),
        permissions: BTreeMap::from([
            ("fs_read".to_string(), Permission::Allow),
            ("fs_write".to_string(), Permission::Deny),
        ]),
        ..ReActConfig::default()
    };
    let (h, _p) = harness_hooks(
        turns,
        Arc::new(tools),
        Arc::new(crate::test_support::NoopPolicy),
        config,
        registry,
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("go")).await;
    assert_eq!(reason, EndReason::Completed);

    // Exactly the allowed call executed.
    assert_eq!(seen.lock().unwrap().len(), 1, "only the allowed call ran");
    let fired = log.lock().unwrap().clone();
    assert_eq!(
        fired.iter().filter(|l| **l == "tool_pre").count(),
        2,
        "§4.4 step 2: tool/pre fires on the declared-deny arm too, not just would-execute"
    );
    assert_eq!(
        fired.iter().filter(|l| **l == "tool_post").count(),
        2,
        "§4.4 step 5: tool/post observes the executed and declared-denied arms alike"
    );

    // The declared-deny arm's logged call keeps its declarative
    // provenance (an abstaining hook changes nothing).
    let events = events_of(&h.store, sid).await;
    let denied: Vec<Value> = tool_call_payloads(&events)
        .into_iter()
        .filter(|c| c["content"]["id"] == json!("call-deny"))
        .collect();
    assert_eq!(denied.len(), 1);
    assert_eq!(
        denied[0]["permission"],
        json!({
            "evaluated": "deny",
            "policy": "configured",
            "matched": "fs_write",
            "reason": "tool_denied_by_policy"
        }),
        "the declared-deny provenance stands under an abstaining hook"
    );
    cleanup_db(&h.db_path);
}

/// §4.4 steps 3–4 (NEW-1): on the **coincident** arm — the declarative
/// table denies the call AND a `veto:tool` plugin denies it — the
/// audit pair names ONE denier: first-deny order makes the hook
/// denial win end-to-end, so the `tool/call` provenance
/// (`policy:"hook"`, the plugin's reason) and the adjacent
/// `tool/result` (the plugin's honest error, `policy:"hook"`) agree;
/// the declarative honest error never lands, the invoke never runs,
/// and the uniform surface still observes the arm via `tool/post`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coincident_declared_and_hook_deny_name_one_denier() {
    let registry = Arc::new(Registry::new());
    let log: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    // A DENYING tool/pre subscriber (veto:tool) — not the abstaining
    // one the M3 test uses.
    let _keep_pre = recording_handler(
        &registry,
        "approval-gate",
        HookId::ToolPre,
        &["observe:tool", "veto:tool"],
        HookReply::BailDeny {
            reason: "outside business hours".to_string(),
        },
        "tool_pre",
        Arc::clone(&log),
    );
    let _keep_post = recording_handler(
        &registry,
        "auditor",
        HookId::ToolPost,
        &["observe:tool"],
        HookReply::Ack { note: None },
        "tool_post",
        Arc::clone(&log),
    );

    // The declarative table denies the SAME call the plugin denies.
    let turns = vec![
        vec![tool_call_chunk(
            "call-both",
            "fs_write",
            json!({"path": "a.txt", "content": "x"}),
        )],
        vec![text_chunk("done")],
    ];
    let (tools, seen) = crate::test_support::HookTools::ok(json!({"ok": true}));
    let config = ReActConfig {
        vision: crate::react::VisionBudget::default(),
        permissions: BTreeMap::from([("fs_write".to_string(), Permission::Deny)]),
        ..ReActConfig::default()
    };
    let (h, _p) = harness_hooks(
        turns,
        Arc::new(tools),
        Arc::new(crate::test_support::NoopPolicy),
        config,
        registry,
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("go")).await;
    assert_eq!(reason, EndReason::Completed);

    // Never invoked, whatever denied it.
    assert!(
        seen.lock().unwrap().is_empty(),
        "the coincident-deny call never reaches tools.invoke"
    );

    // tool/call: the hook provenance — NOT the declarative one.
    let events = events_of(&h.store, sid).await;
    let calls: Vec<Value> = tool_call_payloads(&events)
        .into_iter()
        .filter(|c| c["content"]["id"] == json!("call-both"))
        .collect();
    assert_eq!(calls.len(), 1, "the attempted call is logged exactly once");
    assert_eq!(
        calls[0]["permission"],
        json!({
            "evaluated": "deny",
            "policy": "hook",
            "matched": "approval-gate:tool/pre",
            "reason": "outside business hours",
        }),
        "§4.4 step 3: the hook denial is logged with its own provenance"
    );

    // tool/result: the plugin's honest error — the SAME denier the
    // call names, never the declarative `permission denied by policy`.
    let results: Vec<Value> = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::ToolResult))
        .map(|e| e.bytes["content"].clone())
        .collect();
    assert_eq!(results.len(), 1, "exactly one honest-error result");
    assert_eq!(
        results[0],
        json!({
            "call_id": "call-both",
            "output": {
                "error": "denied by plugin approval-gate: outside business hours",
                "policy": "hook",
            },
            "is_error": true,
        }),
        "§4.4 step 4: the pair agrees — the hook denier, end-to-end"
    );

    // The uniform surface still observed the arm.
    let fired = log.lock().unwrap().clone();
    assert_eq!(
        fired.iter().filter(|l| **l == "tool_pre").count(),
        1,
        "tool/pre consulted once for the coincident-deny call"
    );
    assert_eq!(
        fired.iter().filter(|l| **l == "tool_post").count(),
        1,
        "tool/post observes the coincident-deny arm too (uniform surface)"
    );
    cleanup_db(&h.db_path);
}

// -- acceptance: production compaction (ADR-058) ---------------------------

/// Builds an agent definition with an explicit slot profile and budgets.
fn profiled_agent_def(
    profile: Option<kian_core::model::ModelProfile>,
    budgets: Budgets,
    tools: Vec<String>,
) -> AgentDef {
    AgentDef {
        id: "react-test".into(),
        version: 1,
        model_slots: BTreeMap::from([(
            SlotName("main".into()),
            kian_core::model::SlotSpec {
                provider: "mock".into(),
                model: "mock-model".into(),
                fallback: Vec::new(),
                profile,
            },
        )]),
        tools,
        budgets,
    }
}

fn windowed_profile(window: u64, max_output: u64) -> kian_core::model::ModelProfile {
    kian_core::model::ModelProfile {
        dialect: kian_core::model::ModelDialect::OpenAiChat,
        context_window: Some(window),
        max_output: Some(max_output),
        reasoning: false,
        thinking: None,
        effort: None,
        strict: false,
        cache: true,
        vision: false,
    }
}

fn fold_settings(keep_recent_turns: usize) -> kian_orchestrator::compaction::CompactionSettings {
    kian_orchestrator::compaction::CompactionSettings {
        keep_recent_turns,
        min_reclaim_tokens: 1,
        trigger_percent: 90,
    }
}

/// The policy table the compaction tests run under: each reactive class takes
/// the disposition the test is about (the default table folds a window overflow
/// and fails a byte refusal).
fn compaction_policy(
    context_overflow: kian_core::seams::RecoveryAction,
    request_too_large: kian_core::seams::RecoveryAction,
) -> Arc<dyn kian_core::seams::RecoveryPolicySource> {
    Arc::new(crate::test_support::TablePolicy(vec![
        kian_core::seams::PolicyRule {
            class: "context_overflow".into(),
            action: context_overflow,
        },
        kian_core::seams::PolicyRule {
            class: "request_too_large".into(),
            action: request_too_large,
        },
    ]))
}

/// Harness with an explicit agent definition and compaction tuning, under the
/// policy table a deployed runtime ships by default (an overflow folds, a byte
/// refusal fails).
async fn harness_compacting(
    agent: AgentDef,
    turns: Vec<ScriptedChunksArgs>,
    tools: Arc<dyn kian_core::seams::ToolProvider>,
    compaction: kian_orchestrator::compaction::CompactionSettings,
) -> (Harness, Arc<ScriptedProvider>) {
    harness_compacting_under(
        agent,
        turns,
        tools,
        compaction,
        compaction_policy(
            kian_core::seams::RecoveryAction::CompressThenRetry,
            kian_core::seams::RecoveryAction::Fail,
        ),
    )
    .await
}

/// The same harness with an explicit recovery policy table.
async fn harness_compacting_under(
    agent: AgentDef,
    turns: Vec<ScriptedChunksArgs>,
    tools: Arc<dyn kian_core::seams::ToolProvider>,
    compaction: kian_orchestrator::compaction::CompactionSettings,
    policy: Arc<dyn kian_core::seams::RecoveryPolicySource>,
) -> (Harness, Arc<ScriptedProvider>) {
    let db_path = temp_db_path();
    let store = EventStore::open(&db_path).await.expect("open store");
    let provider = Arc::new(ScriptedProvider::new(turns));
    let mut registry = ProviderRegistry::new();
    registry.register("mock", provider.clone());
    let react = ReActLoop::with_config(
        agent,
        Arc::new(registry),
        tools,
        policy,
        ReActConfig::default(),
    )
    .with_compaction(compaction);
    let engine = Engine::new(store.clone(), Arc::new(react), EngineConfig::default());
    (
        Harness {
            engine,
            store,
            db_path,
        },
        provider,
    )
}

type ScriptedChunksArgs =
    Vec<Result<kian_core::seams::ModelChunk, kian_core::seams::ProviderError>>;

fn step_purposes(events: &[Event]) -> Vec<Option<String>> {
    events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::StepRequest))
        .map(|e| {
            e.bytes
                .get("purpose")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect()
}

fn compact_payloads(events: &[Event]) -> Vec<Value> {
    events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::Compact))
        .map(|e| e.bytes.clone())
        .collect()
}

async fn wait_for_end(h: &Harness, sid: SessionId, want: EndReason) -> bool {
    wait_for(10_000, || async {
        ended_reason(&h.store, sid).await == Some(want)
    })
    .await
}

/// Preventive arm: a declared window under pressure folds old turns into a
/// persisted summary before the next request, the maintenance request is a
/// normal persisted step/request with purpose=compact, and the final answer
/// still settles honestly. Two folds are allowed per episode; the fold
/// rounds never rewrite history.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preventive_pressure_compacts_before_the_request_and_the_summary_is_persisted() {
    let agent = profiled_agent_def(
        Some(windowed_profile(300, 50)),
        Budgets {
            tokens: None,
            wall_clock_ms: None,
        },
        vec![],
    );
    let turns = vec![
        vec![tool_call_chunk("call-1", "missing_tool", json!({}))],
        vec![tool_call_chunk("call-2", "missing_tool", json!({}))],
        vec![text_chunk("summary A")],
        vec![text_chunk("summary B")],
        vec![text_chunk("done")],
    ];
    let (h, provider) = harness_compacting(
        agent,
        turns,
        Arc::new(crate::test_support::NoopTools),
        fold_settings(1),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("do the thing")).await;
    assert_eq!(reason, EndReason::Completed);

    let events = events_of(&h.store, sid).await;
    let compacts = compact_payloads(&events);
    assert_eq!(compacts.len(), 2, "two folds cap one episode");
    let started_at: Vec<u64> = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::TurnStart))
        .map(|e| e.seq)
        .collect();
    for payload in &compacts {
        let from = payload["from_seq"].as_u64().unwrap();
        let to = payload["to_seq"].as_u64().unwrap();
        assert!(from < to && to < payload["produced_by"]["step_seq"].as_u64().unwrap());
        assert!(
            !payload["summary"]["content"]["text"]
                .as_str()
                .unwrap()
                .is_empty()
        );
        assert!(!payload["reason"].as_str().unwrap().is_empty());
        assert!(payload["folded_messages"].as_u64().unwrap() > 0);
        assert!(payload["reclaimed_tokens"].as_u64().unwrap() > 0);
        assert!(started_at.contains(&to), "fold boundary is a turn start");
    }

    let purposes = step_purposes(&events);
    assert_eq!(
        purposes
            .iter()
            .filter(|p| p.as_deref() == Some("compact"))
            .count(),
        2,
        "both summary requests are persisted with purpose=compact"
    );
    assert_eq!(
        purposes.len(),
        provider.requests().len(),
        "A2: one audit copy per request"
    );

    // The compact turn is an honest turn of its own, marked by purpose.
    let compact_turns: Vec<&Event> = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::TurnEnd) && e.bytes["purpose"] == json!("compact"))
        .collect();
    assert_eq!(compact_turns.len(), 2);
    for ev in compact_turns {
        assert_eq!(ev.bytes["outcome"], json!("compacted"));
    }

    // The maintenance request used the frozen summarizer frame; the compact
    // events carry the only reference to the requests that produced them.
    let summary_requests: Vec<Value> = provider
        .requests()
        .into_iter()
        .filter(|body| {
            body["messages"][0]["content"]
                .as_str()
                .is_some_and(|text| text.contains("summarize conversation history"))
        })
        .collect();
    assert_eq!(summary_requests.len(), 2);

    // Context derivation projects the summary, never the folded raw turns.
    let messages = derive_model_context(&events).expect("context derives");
    let flat: String = messages
        .iter()
        .map(|m| m.content.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        flat.contains("summary B"),
        "the latest summary is in the context"
    );
    assert!(!flat.contains("do the thing"), "folded raw turns are gone");
    cleanup_db(&h.db_path);
}

/// Reactive arm: a provider context overflow folds the old turns and the
/// next iteration retries the smaller request; the failed request stays
/// accounted in the stream.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reactive_overflow_compacts_then_retries_the_smaller_request() {
    let agent = profiled_agent_def(
        None,
        Budgets {
            tokens: None,
            wall_clock_ms: None,
        },
        vec![],
    );
    let turns = vec![
        vec![tool_call_chunk("call-1", "missing_tool", json!({}))],
        vec![Err(kian_core::seams::ProviderError::ContextOverflow)],
        vec![text_chunk("folded summary")],
        vec![text_chunk("done")],
    ];
    let (h, provider) = harness_compacting(
        agent,
        turns,
        Arc::new(crate::test_support::NoopTools),
        fold_settings(1),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("do the thing")).await;
    assert_eq!(
        reason,
        EndReason::Completed,
        "an overflow is recovered, not failed"
    );

    let events = events_of(&h.store, sid).await;
    let compacts = compact_payloads(&events);
    assert_eq!(compacts.len(), 1);
    assert_eq!(
        compacts[0]["reason"],
        json!("provider reported context overflow"),
        "the reactive arm names its trigger"
    );
    let soft_ends: Vec<&Event> = events
        .iter()
        .filter(|e| {
            matches!(e.kind, EventKind::TurnEnd)
                && e.bytes["outcome"] == json!("soft_failure")
                && e.bytes["error"]["class"] == json!("context_overflow")
        })
        .collect();
    assert_eq!(
        soft_ends.len(),
        1,
        "the failed request is evidenced honestly"
    );

    let requests = provider.requests();
    assert_eq!(requests.len(), 4);
    assert!(
        requests[2]["messages"][0]["content"]
            .as_str()
            .is_some_and(|text| text.contains("summarize conversation history")),
        "the maintenance turn is the third request"
    );
    assert!(
        requests[3]["messages"]
            .to_string()
            .contains("folded summary"),
        "the retry carries the summary in its context"
    );
    cleanup_db(&h.db_path);
}

/// ADR-073: a byte refusal folds under the policy opt-in — the fold is keyed
/// on the resolved action, the retry carries the summary, and the refusal
/// itself stays evidenced as a soft failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reactive_request_too_large_folds_under_the_policy_opt_in() {
    let agent = profiled_agent_def(
        None,
        Budgets {
            tokens: None,
            wall_clock_ms: None,
        },
        vec![],
    );
    let turns = vec![
        vec![tool_call_chunk("call-1", "missing_tool", json!({}))],
        vec![Err(kian_core::seams::ProviderError::RequestTooLarge {
            size: 35_350_711,
            limit: Some(33_554_432),
            detail: "the serialized request is 35350711 bytes, over providers.vadis.max_request_bytes 33554432".into(),
        })],
        vec![text_chunk("folded summary")],
        vec![text_chunk("done")],
    ];
    let (h, provider) = harness_compacting_under(
        agent,
        turns,
        Arc::new(crate::test_support::NoopTools),
        fold_settings(1),
        compaction_policy(
            kian_core::seams::RecoveryAction::CompressThenRetry,
            kian_core::seams::RecoveryAction::CompressThenRetry,
        ),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("do the thing")).await;
    assert_eq!(
        reason,
        EndReason::Completed,
        "a byte refusal the policy opts into recovering is recovered"
    );

    let events = events_of(&h.store, sid).await;
    let compacts = compact_payloads(&events);
    assert_eq!(compacts.len(), 1);
    assert_eq!(
        compacts[0]["reason"],
        json!("provider refused the request as too large (ADR-068)"),
        "the fold names the byte verdict, not a window overflow"
    );
    let soft_ends: Vec<&Event> = events
        .iter()
        .filter(|e| {
            matches!(e.kind, EventKind::TurnEnd)
                && e.bytes["outcome"] == json!("soft_failure")
                && e.bytes["error"]["class"] == json!("request_too_large")
                && e.bytes["error"]["action"] == json!("compress_then_retry")
        })
        .collect();
    assert_eq!(
        soft_ends.len(),
        1,
        "the refused request is evidenced honestly"
    );

    let requests = provider.requests();
    assert_eq!(requests.len(), 4);
    assert!(
        requests[2]["messages"][0]["content"]
            .as_str()
            .is_some_and(|text| text.contains("summarize conversation history")),
        "the fold is an ordinary maintenance turn"
    );
    assert!(
        requests[3]["messages"]
            .to_string()
            .contains("folded summary"),
        "the retry carries the summary in its context"
    );
    cleanup_db(&h.db_path);
}

/// ADR-073: without the opt-in the byte refusal stays terminal — nothing
/// folds and the turn settles failed, exactly as ADR-068's default table says.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_too_large_without_the_opt_in_fails_without_a_fold() {
    let agent = profiled_agent_def(
        None,
        Budgets {
            tokens: None,
            wall_clock_ms: None,
        },
        vec![],
    );
    let turns = vec![
        vec![tool_call_chunk("call-1", "missing_tool", json!({}))],
        vec![Err(kian_core::seams::ProviderError::RequestTooLarge {
            size: 35_350_711,
            limit: Some(33_554_432),
            detail: "the serialized request is 35350711 bytes".into(),
        })],
    ];
    let (h, provider) = harness_compacting_under(
        agent,
        turns,
        Arc::new(crate::test_support::NoopTools),
        fold_settings(1),
        compaction_policy(
            kian_core::seams::RecoveryAction::CompressThenRetry,
            kian_core::seams::RecoveryAction::Fail,
        ),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("do the thing")).await;
    assert_eq!(reason, EndReason::Failed);

    let events = events_of(&h.store, sid).await;
    assert!(
        compact_payloads(&events).is_empty(),
        "the default disposition never folds"
    );
    let soft_ends: Vec<&Event> = events
        .iter()
        .filter(|e| {
            matches!(e.kind, EventKind::TurnEnd)
                && e.bytes["outcome"] == json!("soft_failure")
                && e.bytes["error"]["class"] == json!("request_too_large")
                && e.bytes["error"]["action"] == json!("fail")
        })
        .collect();
    assert_eq!(soft_ends.len(), 1);
    assert_eq!(
        provider.requests().len(),
        2,
        "no summarizer request is issued"
    );
    cleanup_db(&h.db_path);
}

/// ADR-073: the reactive arm honors the table for the window overflow too — an
/// explicit table that maps `context_overflow` to `fail` folds nothing, which
/// is the delta the action gate introduces over the old class-only check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn context_overflow_without_the_policy_opt_in_fails_without_a_fold() {
    let agent = profiled_agent_def(
        None,
        Budgets {
            tokens: None,
            wall_clock_ms: None,
        },
        vec![],
    );
    let turns = vec![
        vec![tool_call_chunk("call-1", "missing_tool", json!({}))],
        vec![Err(kian_core::seams::ProviderError::ContextOverflow)],
    ];
    let (h, provider) = harness_compacting_under(
        agent,
        turns,
        Arc::new(crate::test_support::NoopTools),
        fold_settings(1),
        compaction_policy(
            kian_core::seams::RecoveryAction::Fail,
            kian_core::seams::RecoveryAction::Fail,
        ),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let reason = submit_and_wait(&h, sid, json!("do the thing")).await;
    assert_eq!(reason, EndReason::Failed);

    let events = events_of(&h.store, sid).await;
    assert!(
        compact_payloads(&events).is_empty(),
        "a table that maps the class to fail is not overridden by the loop"
    );
    assert_eq!(
        provider.requests().len(),
        2,
        "no summarizer request is issued"
    );
    cleanup_db(&h.db_path);
}

/// The ordinary-turn gate reserves the maintenance share (ADR-058) and a
/// raise releases the resulting awaiting_budget settle; a session without a
/// declared window never compacts.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_reserve_parks_the_turn_and_a_budget_raise_resumes_it() {
    let agent = profiled_agent_def(
        None,
        Budgets {
            tokens: Some(3000),
            wall_clock_ms: None,
        },
        vec![],
    );
    let turns = vec![
        vec![
            tool_call_chunk("call-1", "missing_tool", json!({})),
            usage_chunk(json!({"prompt_tokens": 1500, "completion_tokens": 500})),
        ],
        vec![text_chunk("done")],
    ];
    let (h, provider) = harness_compacting(
        agent,
        turns,
        Arc::new(crate::test_support::NoopTools),
        fold_settings(4),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    h.engine
        .admit(sid, json!("x".repeat(3000)), None)
        .await
        .expect("admit");
    let _ = h.engine.wake(sid);
    assert!(
        wait_for_end(&h, sid, EndReason::AwaitingBudget).await,
        "the reserve blocks the ordinary turn"
    );

    let events = events_of(&h.store, sid).await;
    let gate = events
        .iter()
        .find(|e| {
            matches!(e.kind, EventKind::TurnEnd) && e.bytes["outcome"] == json!("awaiting_budget")
        })
        .expect("gate turn/end");
    assert_eq!(gate.bytes["budget"]["limit"], json!(3000));
    assert_eq!(gate.bytes["budget"]["spent"], json!(2000));
    assert_eq!(
        gate.bytes["budget"]["reserve"],
        json!(300),
        "10% of the allowance is kept for maintenance"
    );
    assert!(
        compact_payloads(&events).is_empty(),
        "no window, no preventive fold"
    );

    h.engine
        .raise_budget(
            sid,
            Some(3000),
            None,
            Some("reserve test".into()),
            None,
            None,
        )
        .await
        .expect("raise");
    assert!(
        wait_for_end(&h, sid, EndReason::Completed).await,
        "the raise releases the settle and the turn completes"
    );
    let events = events_of(&h.store, sid).await;
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::BudgetRaised))
            .count(),
        1,
        "the raise is a durable event"
    );
    assert_eq!(provider.requests().len(), 2);
    cleanup_db(&h.db_path);
}

// -- acceptance: vision content parts (ADR-063) -----------------------------

fn vision_profile() -> kian_core::model::ModelProfile {
    kian_core::model::ModelProfile {
        dialect: kian_core::model::ModelDialect::OpenAiChat,
        context_window: None,
        max_output: None,
        reasoning: false,
        thinking: None,
        effort: None,
        strict: false,
        cache: true,
        vision: true,
    }
}

/// Engine harness with an explicit artifact runtime so the loop can read
/// referenced image blobs (ADR-063) through `DrainCtx::artifact_bytes`.
async fn harness_with_artifacts(
    agent: AgentDef,
    turns: Vec<ScriptedChunksArgs>,
    artifacts: Option<ArtifactRuntime>,
) -> (Harness, Arc<ScriptedProvider>) {
    let db_path = temp_db_path();
    let store = EventStore::open(&db_path).await.expect("open store");
    let provider = Arc::new(ScriptedProvider::new(turns));
    let mut registry = ProviderRegistry::new();
    registry.register("mock", provider.clone());
    let react = ReActLoop::with_config(
        agent,
        Arc::new(registry),
        Arc::new(NoopTools),
        Arc::new(NoopPolicy),
        ReActConfig::default(),
    );
    let engine = Engine::new_with_read_model_and_artifacts(
        store.clone(),
        Arc::new(react),
        EngineConfig::default(),
        ReadModelConfig::default(),
        artifacts,
    );
    (
        Harness {
            engine,
            store,
            db_path,
        },
        provider,
    )
}

/// What a client submits: the payload IS the model content. The engine's
/// admission wraps it as `{"role":"user","content": payload}`, so this
/// object becomes one user message whose content is the ADR-063 envelope.
fn image_envelope(artifact: &str) -> Value {
    json!({
        "text": "what is on this invoice?",
        "images": [{"artifact": artifact, "media_type": "image/png"}]
    })
}

/// ADR-063 L2: a user message referencing an artifact image is rendered as
/// dialect content parts (text + image_url data URL) for a vision profile;
/// the bytes come from the artifact runtime, and the internal carrier field
/// never reaches the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vision_profile_sends_artifact_images_as_content_parts() {
    let dir = std::env::temp_dir().join(format!("kian-loop-vision-{}", Uuid::new_v4()));
    let blobs = kian_store::ArtifactStore::open(&dir).expect("open artifacts");
    let reference = blobs.put(b"hi", "image/png").expect("put blob");
    let artifacts = ArtifactRuntime::new(
        kian_store::ArtifactStore::open(&dir).expect("open artifacts"),
        1024,
    );
    let agent = profiled_agent_def(
        Some(vision_profile()),
        Budgets {
            tokens: None,
            wall_clock_ms: None,
        },
        vec![],
    );
    let (h, provider) = harness_with_artifacts(
        agent,
        vec![vec![text_chunk("the invoice total is 42")]],
        Some(artifacts),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    assert_eq!(
        submit_and_wait(&h, sid, image_envelope(&reference.id)).await,
        EndReason::Completed
    );

    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    let messages = requests[0]["messages"].as_array().expect("messages");
    let last = messages.last().expect("user message");
    assert_eq!(last["role"], json!("user"));
    assert_eq!(
        last["content"],
        json!([
            {"type": "text", "text": "what is on this invoice?"},
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,aGk="}}
        ]),
        "the artifact bytes fly as one dialect image part"
    );
    assert_eq!(
        last.get("images"),
        None,
        "the internal carrier never reaches the wire"
    );

    cleanup_db(&h.db_path);
    let _ = std::fs::remove_dir_all(&dir);
}

/// ADR-063 honesty: neither a missing blob nor a non-vision profile changes
/// the wire shape into content parts — the envelope's text still flies, and
/// a non-vision target keeps the legacy compact-JSON conversion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_blob_or_a_non_vision_profile_keeps_the_text_only_shape() {
    let agent = profiled_agent_def(
        Some(vision_profile()),
        Budgets {
            tokens: None,
            wall_clock_ms: None,
        },
        vec![],
    );
    let (h, provider) = harness_with_artifacts(agent, vec![vec![text_chunk("ok")]], None).await;
    let sid = h.engine.create_session().await.expect("create");
    let missing = "f".repeat(64);
    assert_eq!(
        submit_and_wait(&h, sid, image_envelope(&missing)).await,
        EndReason::Completed
    );
    let requests = provider.requests();
    let last = requests[0]["messages"]
        .as_array()
        .expect("messages")
        .last()
        .expect("user message")
        .clone();
    assert_eq!(
        last["content"],
        json!("what is on this invoice?"),
        "a reclaimed artifact degrades to the envelope text, never a failed turn"
    );
    cleanup_db(&h.db_path);

    let muted = kian_core::model::ModelProfile {
        vision: false,
        ..vision_profile()
    };
    let agent = profiled_agent_def(
        Some(muted),
        Budgets {
            tokens: None,
            wall_clock_ms: None,
        },
        vec![],
    );
    let (h, provider) = harness_with_artifacts(agent, vec![vec![text_chunk("ok")]], None).await;
    let sid = h.engine.create_session().await.expect("create");
    assert_eq!(
        submit_and_wait(&h, sid, image_envelope(&missing)).await,
        EndReason::Completed
    );
    let requests = provider.requests();
    let last = requests[0]["messages"]
        .as_array()
        .expect("messages")
        .last()
        .expect("user message")
        .clone();
    let content = last["content"]
        .as_str()
        .expect("a non-vision target keeps the compact-JSON string");
    assert!(
        content.contains("images") && content.contains(&missing),
        "the legacy fallback carries the envelope verbatim: {content}"
    );
    cleanup_db(&h.db_path);
}

/// ADR-066 L1: six references in one tail message under the default
/// budget render the newest four as content parts; the text part is still
/// exactly the wrapper bytes and the two dropped references cost pixels,
/// never text.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_per_turn_image_budget_renders_only_the_newest_references() {
    let dir = std::env::temp_dir().join(format!("kian-loop-vision-budget-{}", Uuid::new_v4()));
    let blobs = kian_store::ArtifactStore::open(&dir).expect("open artifacts");
    let artifacts = ArtifactRuntime::new(
        kian_store::ArtifactStore::open(&dir).expect("open artifacts"),
        1024,
    );
    let ids: Vec<String> = ["a", "b", "c", "d", "e", "f"]
        .iter()
        .map(|byte| {
            blobs
                .put(byte.as_bytes(), "image/jpeg")
                .expect("put blob")
                .id
        })
        .collect();
    let agent = profiled_agent_def(
        Some(vision_profile()),
        Budgets {
            tokens: None,
            wall_clock_ms: None,
        },
        vec![],
    );
    let (h, provider) = harness_with_artifacts(
        agent,
        vec![vec![text_chunk("a night sky")]],
        Some(artifacts),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let wrapper = json!({
        "kian": {
            "type": "channel_event",
            "channel": "imessage",
            "topic": "imessage",
            "event_id": "evt-10",
            "raw": {"text": "[image]"}
        }
    });
    let mut content = wrapper.clone();
    content["images"] = json!(
        ids.iter()
            .map(|id| json!({"artifact": id, "media_type": "image/jpeg"}))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        submit_and_wait(&h, sid, content).await,
        EndReason::Completed
    );

    let requests = provider.requests();
    let last = requests[0]["messages"]
        .as_array()
        .expect("messages")
        .last()
        .expect("user message")
        .clone();
    let parts = last["content"].as_array().expect("vision content parts");
    assert_eq!(
        parts.len(),
        5,
        "one text part plus the newest four references"
    );
    assert_eq!(
        parts[0],
        json!({"type": "text", "text": serde_json::to_string(&wrapper).unwrap()}),
        "the dropped references change no text byte"
    );
    assert_eq!(
        parts[1..],
        [
            json!({"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,Yw=="}}),
            json!({"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,ZA=="}}),
            json!({"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,ZQ=="}}),
            json!({"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,Zg=="}}),
        ],
        "the newest four win; the two older references stay text-only"
    );
    cleanup_db(&h.db_path);
    let _ = std::fs::remove_dir_all(&dir);
}

/// ADR-065 L2: a channel row carrying image references beside its wrapper
/// reaches a vision target as content parts, and the text part is exactly
/// the bytes the same row sends without the flag — the wrapper is not
/// reformatted by gaining references.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_channel_wrapper_with_image_references_fills_the_vision_parts() {
    let dir = std::env::temp_dir().join(format!("kian-loop-vision-channel-{}", Uuid::new_v4()));
    let blobs = kian_store::ArtifactStore::open(&dir).expect("open artifacts");
    let reference = blobs.put(b"hi", "image/jpeg").expect("put blob");
    let artifacts = ArtifactRuntime::new(
        kian_store::ArtifactStore::open(&dir).expect("open artifacts"),
        1024,
    );
    let agent = profiled_agent_def(
        Some(vision_profile()),
        Budgets {
            tokens: None,
            wall_clock_ms: None,
        },
        vec![],
    );
    let (h, provider) = harness_with_artifacts(
        agent,
        vec![vec![text_chunk("a night sky")]],
        Some(artifacts),
    )
    .await;
    let sid = h.engine.create_session().await.expect("create");
    let wrapper = json!({
        "kian": {
            "type": "channel_event",
            "channel": "imessage",
            "topic": "imessage",
            "event_id": "evt-9",
            "raw": {"text": "[image]"}
        }
    });
    let mut content = wrapper.clone();
    content["images"] = json!([{"artifact": reference.id, "media_type": "image/jpeg"}]);
    assert_eq!(
        submit_and_wait(&h, sid, content).await,
        EndReason::Completed
    );

    let requests = provider.requests();
    let last = requests[0]["messages"]
        .as_array()
        .expect("messages")
        .last()
        .expect("user message")
        .clone();
    assert_eq!(
        last["content"],
        json!([
            {"type": "text", "text": serde_json::to_string(&wrapper).unwrap()},
            {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64,aGk="}}
        ]),
        "the wrapper keeps its bytes and the pixels ride as a content part"
    );
    assert_eq!(
        last.get("images"),
        None,
        "the carrier never reaches the wire"
    );
    cleanup_db(&h.db_path);
    let _ = std::fs::remove_dir_all(&dir);
}
