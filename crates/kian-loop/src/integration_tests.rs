//! Engine integration tests for the built-in ReAct loop (card C11
//! acceptance). Each test drives the *real* C9 engine (sqlite store,
//! admit/wake/drain) with the loop mounted as its `LoopDriver`; the
//! provider and (where useful) the tools are mocks — the engine, store,
//! projections and event ordering are never mocked.

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
use kian_core::model::{SessionId, SlotName};
use kian_core::seams::{PolicyRule, RecoveryAction, ToolCall, ToolResult};
use kian_core::state::{SessionState, project};
use kian_llm::ProviderRegistry;
use kian_orchestrator::{Engine, EngineConfig};
use kian_store::EventStore;

use crate::react::{CLASS_EMPTY_RESPONSE, ReActConfig, ReActLoop};
use crate::test_support::{
    HookTools, RecordingPolicy, ScriptedProvider, agent_def, text_chunk, tool_call_chunk,
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
                "fs.write",
                json!({"path": "out.txt", "content": "42"}),
            ),
        ],
        // Turn 2: read it back.
        vec![tool_call_chunk(
            "call-2",
            "fs.read",
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
