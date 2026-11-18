//! (R17-C6) Acceptance probes for the P4 class-A audit key groups
//! (`design/MULTIAGENT-MODEL.md` §7.2, plan §0.2 pins P4-1/P4-2/P4-3).
//!
//! Two key groups on events that already exist, both outside
//! `content`, both gated by `superagent.board.gating` (pin P4-1:
//! `false` ⇒ no key, no behaviour — P3 bytes exactly):
//!
//! - the **parent's** `thread/started` carries `"spawn_uid"` +
//!   `"multiagent": {task_id, assignee, attempt, depends_on[], group?}`;
//! - the **child's** `thread/started` carries `"spawn_uid"` +
//!   `"multiagent": {task_id, assignee}` only (the derivable subset —
//!   `depends_on`/`group`/`attempt` are parent-side bookkeeping,
//!   absent, never `null`) — pin P4-2;
//! - the child's `thread/completed` carries `"spawn_uid"` +
//!   `"multiagent": {task_id, outcome, evidence_ref?, contract?}`,
//!   with `outcome` written by `outcome_for_completion` (pin P4-3:
//!   the six completion-borne values only) and `evidence_ref` a
//!   `{session, seq}` pointer at the committed verdict, never a copy.
//!
//! The delegation **wrapper** (`task`) stays byte-identical between
//! the two streams (pin P4-2): the top-level audit keys diverge by
//! design while the model-visible bytes do not.
//!
//! Every fixture (ids, templates, streams, fakes) is authored here.
//! No network, no daemon.

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use kian_core::context::derive_model_context;
use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::{Epoch, SessionId, Thread};
use kian_core::seams::{LoopDriver, LoopOutcome, ToolCall, ToolProvider};
use kian_loop::EvidenceGateConfig;
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    AdmitReceipt, BoardGating, InboxWriteError, InboxWriter, MultiagentLimits, RouterHold,
    RouterProfile, RouterTools, SPAWN_WORKER_TOOL, SpawnIdentity, SpawnRequest, StreamPort,
    StreamPortError, WorkerSpawner, audit, child_session_id, derive_outcome,
    outcome_for_completion, router_template,
    worker::{
        WORKER_GATE_NAME, spawn_identity_of, thread_completed_payload, thread_report_content_ident,
        thread_started_payload,
    },
    worker_template,
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn sid(n: u128) -> SessionId {
    SessionId(Uuid::from_u128(n))
}

fn ev(session: SessionId, seq: u64, kind: EventKind, bytes: Value) -> Event {
    Event {
        seq,
        session,
        epoch: Epoch(1),
        ts: 10_000 + seq,
        kind,
        bytes,
        version: 0,
    }
}

/// An inbox fake that records every admit (with payload + key) and wake.
struct CaptureInbox {
    admits: Mutex<Vec<(SessionId, Value, Option<String>)>>,
    wakes: Mutex<Vec<SessionId>>,
}

impl CaptureInbox {
    fn shared() -> Arc<Self> {
        Arc::new(Self {
            admits: Mutex::new(Vec::new()),
            wakes: Mutex::new(Vec::new()),
        })
    }
}

impl InboxWriter for CaptureInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        self.admits
            .lock()
            .unwrap()
            .push((session, payload, idempotency_key));
        Box::pin(async {
            Ok(AdmitReceipt {
                deduplicated: false,
            })
        })
    }
    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.wakes.lock().unwrap().push(session);
        WakeOutcome::Folded
    }
}

fn limits() -> MultiagentLimits {
    MultiagentLimits {
        max_children: 4,
        max_depth: 1,
        child_timeout_ms: 900_000,
    }
}

// ---------------------------------------------------------------------------
// The parent-side rig (RouterTools, roster mounted, one tool/call at
// seq 5 — the pin-P2 derivation anchor)
// ---------------------------------------------------------------------------

fn router_session() -> SessionId {
    sid(0xC6_0001)
}

/// The recording stream port the Router's own `thread/started` lands on.
struct RouterPort {
    session: SessionId,
    events: Mutex<Vec<Event>>,
    emitted: Mutex<Vec<(EventKind, Value)>>,
}

impl StreamPort for RouterPort {
    fn session(&self) -> SessionId {
        self.session
    }
    fn emit(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> BoxFuture<'static, Result<(), StreamPortError>> {
        self.emitted.lock().unwrap().push((kind, bytes));
        Box::pin(async { Ok(()) })
    }
    fn read_stream(&self) -> BoxFuture<'static, Result<Vec<Event>, StreamPortError>> {
        let events = self.events.lock().unwrap().clone();
        Box::pin(async move { Ok(events) })
    }
}

/// The child-streams fake (every child unknown ⇒ never started).
struct NullStreams;

impl kian_superagent::WorkerStreamsPort for NullStreams {
    fn read_stream(
        &self,
        _session: SessionId,
    ) -> BoxFuture<'static, Result<Vec<Event>, kian_superagent::WorkerStreamsError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct RouterRig {
    tools: Arc<RouterTools>,
    port: Arc<RouterPort>,
    inbox: Arc<CaptureInbox>,
}

fn router_tool_call(seq: u64, call_id: &str) -> Event {
    ev(
        router_session(),
        seq,
        EventKind::ToolCall,
        json!({
            "role": "assistant",
            "content": {"id": call_id, "name": SPAWN_WORKER_TOOL,
                        "arguments": {"task": "the work", "agent": "researcher",
                                      "task_key": "ingest-feed"}},
        }),
    )
}

/// One assembled parent surface: multiagent on, a one-entry roster, the
/// spawn arguments resolved through it, and the class-A gate set by
/// `gating` (pin P4-1 — `false` must be P3 bytes exactly).
fn router_rig(gating: bool, parent_events: Vec<Event>) -> RouterRig {
    let router = router_session();
    let inbox = CaptureInbox::shared();
    let spawner = Arc::new(WorkerSpawner::new(
        router,
        &worker_template("mock", "mock-worker", 7),
        inbox.clone(),
    ));
    let mut profile = RouterProfile::new(router, router_template("mock", "mock-router"));
    profile.multiagent = Some(limits());
    let hold = RouterHold::shared();
    let mut tools = RouterTools::new(
        spawner,
        kian_superagent::ChannelRegistry::new(),
        &profile,
        hold.clone(),
    );
    tools = tools.with_board_gating(BoardGating(gating));
    tools = tools.with_worker_streams(Arc::new(NullStreams));
    tools = tools.with_roster(kian_superagent::Roster {
        entries: vec![kian_superagent::RosterEntry {
            id: "researcher".to_string(),
            description: "The researcher does one thing well".to_string(),
            agent: "kian-research".to_string(),
            mandate_template: kian_superagent::MandateTemplate {
                scope: "advisory scope text".to_string(),
                strength: kian_superagent::MandateStrength::Inferred,
            },
            tools_allow: vec!["fs_read".to_string()],
            limits: kian_superagent::RosterLimits { max_children: 2 },
            output_schema: None,
            resolved_agent: Some(kian_superagent::ResolvedAgent {
                def_id: "kian-research".to_string(),
                version: 3,
            }),
        }],
    });
    let port = Arc::new(RouterPort {
        session: router,
        events: Mutex::new(parent_events),
        emitted: Mutex::new(Vec::new()),
    });
    let dyn_port: Arc<dyn StreamPort> = Arc::clone(&port) as Arc<dyn StreamPort>;
    hold.install(dyn_port);
    RouterRig {
        tools: Arc::new(tools),
        port,
        inbox,
    }
}

fn spawn_call(call_id: &str) -> ToolCall {
    ToolCall {
        id: call_id.into(),
        name: SPAWN_WORKER_TOOL.into(),
        arguments: json!({"task": "the work", "agent": "researcher", "task_key": "ingest-feed"}),
    }
}

/// The parent-side thread of the spawned child (the shape both sides'
/// payloads describe).
fn spawned_thread(identity: &SpawnIdentity) -> Thread {
    Thread {
        id: identity.thread_id,
        parent_session: router_session(),
        pinned_version: 7,
    }
}

fn sorted_keys(value: &Value) -> Vec<String> {
    let mut keys: Vec<String> = value.as_object().expect("object").keys().cloned().collect();
    keys.sort();
    keys
}

// ---------------------------------------------------------------------------
// The child-side rig (WorkerLoopDriver over a scripted inner loop)
// ---------------------------------------------------------------------------

/// The fake stream port the decorator reads and writes.
struct FakePort {
    session: SessionId,
    events: Arc<Mutex<Vec<Event>>>,
}

impl StreamPort for FakePort {
    fn session(&self) -> SessionId {
        self.session
    }
    fn emit(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> BoxFuture<'static, Result<(), StreamPortError>> {
        let mut events = self.events.lock().unwrap();
        let seq = u64::try_from(events.len()).expect("seq") + 1;
        events.push(ev(self.session, seq, kind, bytes));
        Box::pin(async { Ok(()) })
    }
    fn read_stream(&self) -> BoxFuture<'static, Result<Vec<Event>, StreamPortError>> {
        let events = self.events.lock().unwrap().clone();
        Box::pin(async move { Ok(events) })
    }
}

/// The scripted inner loop: one drive, one terminal outcome, done.
struct ScriptedInner {
    outcome: LoopOutcome,
}

impl LoopDriver for ScriptedInner {
    type Ctx = ();

    fn drive(&self, _ctx: ()) -> BoxStream<'static, LoopOutcome> {
        let outcome = self.outcome.clone();
        futures::stream::once(async move { outcome }).boxed()
    }
}

struct ChildJoin {
    driver: kian_superagent::WorkerLoopDriver<ScriptedInner>,
    inbox: Arc<CaptureInbox>,
    events: Arc<Mutex<Vec<Event>>>,
    child: SessionId,
}

/// One identity + wrapper the child stream is seeded with (the same
/// bytes `WorkerSpawner::prepare` produces on the multiagent path).
fn child_identity() -> SpawnIdentity {
    SpawnIdentity::derive_delegated(
        router_session(),
        5,
        "kian-research",
        3,
        Some("ingest-feed"),
        Some("researcher".to_string()),
    )
}

fn wrapper_message(identity: &SpawnIdentity) -> Value {
    let spawner = WorkerSpawner::new(
        router_session(),
        &worker_template("mock", "mock-worker", 7),
        CaptureInbox::shared(),
    );
    let spawned = spawner.prepare(&SpawnRequest {
        task: json!("the work"),
        correlation: None,
        speculative: false,
        mandate: None,
        exec_worker: None,
        identity: Some(identity.clone()),
        output_schema: None,
    });
    spawned.task_message
}

/// Assembles the child driver over a seeded stream. `extra` lands after
/// the wrapper input (e.g. a committed verdict the completion points
/// at); `gating` is the pin-P4-1 class-A switch.
fn child_join(gating: bool, inner: LoopOutcome, extra: Vec<Event>) -> ChildJoin {
    let identity = child_identity();
    let child = child_session_id(identity.thread_id);
    let inbox = CaptureInbox::shared();
    let message = wrapper_message(&identity);
    let mut events = vec![
        ev(child, 1, EventKind::InputAdmitted, message),
        ev(
            child,
            2,
            EventKind::DrainClaimed,
            json!({"holder": "h", "epoch": 1}),
        ),
    ];
    let base_seq = events.len() as u64;
    for (i, event) in extra.into_iter().enumerate() {
        events.push(ev(child, base_seq + i as u64 + 1, event.kind, event.bytes));
    }
    let state = Arc::new(Mutex::new(events));
    let port_session = child;
    let port_state = Arc::clone(&state);
    let port_for = Arc::new(move |_ctx: &()| {
        Arc::new(FakePort {
            session: port_session,
            events: Arc::clone(&port_state),
        }) as Arc<dyn StreamPort>
    });
    let config = kian_superagent::WorkerLoopConfig {
        router: router_session(),
        worker_version: 7,
        gate: EvidenceGateConfig::default(),
        multiagent: Some(limits()),
        board_gating: BoardGating(gating),
    };
    let driver = kian_superagent::WorkerLoopDriver::new(
        Arc::new(ScriptedInner { outcome: inner }),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        config,
        port_for,
    );
    ChildJoin {
        driver,
        inbox,
        events: state,
        child,
    }
}

async fn drive(join: &ChildJoin) -> Vec<LoopOutcome> {
    join.driver.drive(()).collect().await
}

fn thread_started_of(events: &[Event]) -> Value {
    events
        .iter()
        .find(|e| e.kind == EventKind::ThreadStarted)
        .expect("thread/started emitted")
        .bytes
        .clone()
}

fn thread_completed_of(events: &[Event]) -> Value {
    events
        .iter()
        .find(|e| e.kind == EventKind::ThreadCompleted)
        .expect("thread/completed emitted")
        .bytes
        .clone()
}

/// A committed worker-gate verdict event (the `evidence_ref` target).
fn worker_verdict(seq_hint: u64) -> Event {
    ev(
        sid(0xC6_00F0),
        seq_hint,
        EventKind::EvidenceVerdict,
        json!({
            "gate": WORKER_GATE_NAME,
            "passed": true,
            "checks": [{"name": "answer-present", "passed": true}],
            "attempt": 0,
        }),
    )
}

// ---------------------------------------------------------------------------
// 1. 🔴 Pin P4-1: gating OFF ⇒ P3 bytes exactly (anchors rule 4)
// ---------------------------------------------------------------------------

/// The round's byte gate: with `board.gating: false` the parent's
/// `thread/started`, the child's `thread/started` and
/// `thread/completed`, the completion report and the spawn tool result
/// are all byte-equal to the P3 goldens (the landed builders, no
/// class-A key anywhere).
#[tokio::test]
async fn gating_off_emits_p3_bytes_exactly() {
    // --- parent side ---
    let rig = router_rig(false, vec![router_tool_call(5, "call-1")]);
    let result = rig
        .tools
        .invoke(spawn_call("call-1"))
        .await
        .expect("spawn dispatches");
    let admits = rig.inbox.admits.lock().unwrap().clone();
    assert_eq!(admits.len(), 1);
    let identity = spawn_identity_of(&admits[0].1).expect("identity in wrapper");
    let worker = child_session_id(identity.thread_id);
    let golden_message = json!({"role": "user", "content": admits[0].1.clone()});
    let emitted = rig.port.emitted.lock().unwrap().clone();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].0, EventKind::ThreadStarted);
    assert_eq!(
        emitted[0].1,
        thread_started_payload(&spawned_thread(&identity), worker, &golden_message),
        "parent thread/started is the P3 golden"
    );
    // The tool result is the P3 ack: no class-A key.
    assert_eq!(result.output["status"], json!("dispatched"));
    assert_eq!(result.output["spawn_outcome"], json!("dispatched"));
    assert!(
        result.output.get("multiagent").is_none(),
        "ack carries no `multiagent`"
    );

    // --- child side ---
    let join = child_join(false, LoopOutcome::Settled, Vec::new());
    drive(&join).await;
    let events = join.events.lock().unwrap().clone();
    let started = thread_started_of(&events);
    let identity = child_identity();
    assert_eq!(
        started,
        thread_started_payload(
            &Thread {
                id: identity.thread_id,
                parent_session: router_session(),
                pinned_version: 7,
            },
            join.child,
            &wrapper_message(&identity),
        ),
        "child thread/started is the P3 golden"
    );
    let completed = thread_completed_of(&events);
    let summary = json!({"text": ""});
    assert_eq!(
        completed,
        thread_completed_payload(identity.thread_id, "completed", summary.clone()),
        "child thread/completed is the P3 golden"
    );
    let admits = join.inbox.admits.lock().unwrap().clone();
    assert_eq!(admits.len(), 1);
    assert_eq!(
        admits[0].1,
        thread_report_content_ident(&identity, "completed", summary, None),
        "completion report is the P3 golden"
    );
}

// ---------------------------------------------------------------------------
// 2. 🔴 Key-set equality: exactly the named keys and no others
// ---------------------------------------------------------------------------

/// With `board.gating: true` the parent's `thread/started` carries
/// exactly `{"spawn_uid","multiagent"}` added at top level, the group
/// carries exactly `{task_id, assignee, attempt, depends_on}` (`group`
/// omitted when unset), and the values are the spawn's own facts; the
/// attempt ordinal counts the stream's prior `thread/started` carrying
/// the same `task_id` (§1:177-179).
#[tokio::test]
async fn gating_on_emits_exactly_the_named_keys_and_no_others() {
    // --- parent side, fresh parent stream ⇒ attempt 1 ---
    let rig = router_rig(true, vec![router_tool_call(5, "call-1")]);
    rig.tools
        .invoke(spawn_call("call-1"))
        .await
        .expect("spawn dispatches");
    let admits = rig.inbox.admits.lock().unwrap().clone();
    let identity = spawn_identity_of(&admits[0].1).expect("identity");
    let emitted = rig.port.emitted.lock().unwrap().clone();
    let payload = &emitted[0].1;
    let worker = child_session_id(identity.thread_id);
    let base = thread_started_payload(
        &spawned_thread(&identity),
        worker,
        &json!({"role": "user", "content": admits[0].1.clone()}),
    );
    let mut expected_top = sorted_keys(&base);
    expected_top.push("spawn_uid".to_string());
    expected_top.push("multiagent".to_string());
    expected_top.sort();
    assert_eq!(
        sorted_keys(payload),
        expected_top,
        "exactly two top-level additions"
    );
    assert_eq!(payload["spawn_uid"], json!(identity.spawn_uid.to_string()));
    let group = &payload["multiagent"];
    assert_eq!(
        sorted_keys(group),
        vec![
            "assignee".to_string(),
            "attempt".to_string(),
            "depends_on".to_string(),
            "task_id".to_string()
        ],
        "group keys exactly (group omitted when unset)"
    );
    assert_eq!(group["task_id"], json!(identity.task_id_str()));
    assert_eq!(group["assignee"], json!("researcher"));
    assert_eq!(group["attempt"], json!(1));
    assert_eq!(group["depends_on"], json!([]));

    // --- parent side, a prior spawn of the same task_key ⇒ attempt 2 ---
    let prior_identity = SpawnIdentity::derive_delegated(
        router_session(),
        3,
        "kian-research",
        3,
        Some("ingest-feed"),
        Some("researcher".to_string()),
    );
    let prior_worker = child_session_id(prior_identity.thread_id);
    let prior_started = thread_started_payload(
        &spawned_thread(&prior_identity),
        prior_worker,
        &wrapper_message(&prior_identity),
    );
    let rig2 = router_rig(
        true,
        vec![
            router_tool_call(3, "call-0"),
            prior_started
                .clone()
                .into_thread_started(router_session(), 4),
            router_tool_call(5, "call-1"),
        ],
    );
    rig2.tools
        .invoke(spawn_call("call-1"))
        .await
        .expect("spawn dispatches");
    let emitted2 = rig2.port.emitted.lock().unwrap().clone();
    assert_eq!(
        emitted2[0].1["multiagent"]["attempt"],
        json!(2),
        "attempt = 1 + prior thread/started with the same task_id"
    );

    // --- child side ---
    let join = child_join(true, LoopOutcome::Settled, Vec::new());
    drive(&join).await;
    let events = join.events.lock().unwrap().clone();
    let started = thread_started_of(&events);
    let identity = child_identity();
    let mut expected_top = sorted_keys(&thread_started_payload(
        &Thread {
            id: identity.thread_id,
            parent_session: router_session(),
            pinned_version: 7,
        },
        join.child,
        &wrapper_message(&identity),
    ));
    expected_top.push("spawn_uid".to_string());
    expected_top.push("multiagent".to_string());
    expected_top.sort();
    assert_eq!(sorted_keys(&started), expected_top);
    assert_eq!(started["spawn_uid"], json!(identity.spawn_uid.to_string()));
    assert_eq!(
        sorted_keys(&started["multiagent"]),
        vec!["assignee".to_string(), "task_id".to_string()],
        "child group is exactly the derivable subset"
    );
    let completed = thread_completed_of(&events);
    let mut expected_completed = sorted_keys(&thread_completed_payload(
        identity.thread_id,
        "completed",
        json!({"text": ""}),
    ));
    expected_completed.push("spawn_uid".to_string());
    expected_completed.push("multiagent".to_string());
    expected_completed.sort();
    assert_eq!(sorted_keys(&completed), expected_completed);
    assert_eq!(
        sorted_keys(&completed["multiagent"]),
        vec!["outcome".to_string(), "task_id".to_string()],
        "completed group: outcome + task_id only when no verdict was committed"
    );
    assert_eq!(
        completed["multiagent"]["outcome"],
        json!("completed_unverified")
    );
}

/// A tiny helper turning a payload into a committed parent event (the
/// prior-spawn fixture above).
trait ThreadStartedEvent {
    fn into_thread_started(self, session: SessionId, seq: u64) -> Event;
}

impl ThreadStartedEvent for Value {
    fn into_thread_started(self, session: SessionId, seq: u64) -> Event {
        ev(session, seq, EventKind::ThreadStarted, self)
    }
}

// ---------------------------------------------------------------------------
// 3. 🔴 The wrapper stays byte-identical on both streams (pin P4-2)
// ---------------------------------------------------------------------------

/// The two `thread/started` events share the wrapper (`task`) exactly
/// while their top-level audit keys differ by design: the parent
/// carries the bookkeeping (`attempt`, `depends_on`, `group?`), the
/// child does not — and neither side's wrapper bytes changed.
#[tokio::test]
async fn the_delegation_wrapper_stays_byte_identical_on_both_streams() {
    let rig = router_rig(true, vec![router_tool_call(5, "call-1")]);
    rig.tools
        .invoke(spawn_call("call-1"))
        .await
        .expect("spawn dispatches");
    let parent_started = rig.port.emitted.lock().unwrap().clone()[0].1.clone();

    let join = child_join(true, LoopOutcome::Settled, Vec::new());
    drive(&join).await;
    let events = join.events.lock().unwrap().clone();
    let child_started = thread_started_of(&events);

    // The wrapper is THE same bytes on both streams.
    assert_eq!(parent_started["task"], child_started["task"]);
    // The frozen lineage fields agree...
    for key in ["thread_id", "parent", "pinned_version"] {
        assert_eq!(parent_started[key], child_started[key], "frozen {key}");
    }
    // ...the worker field names each side's own session (by design)...
    assert_eq!(
        parent_started["worker"],
        json!(child_session_id(child_identity().thread_id).0.to_string())
    );
    assert_eq!(child_started["worker"], json!(join.child.0.to_string()));
    // ...and the audit groups differ exactly by the parent-only keys.
    let parent_group_keys = sorted_keys(&parent_started["multiagent"]);
    let child_group_keys = sorted_keys(&child_started["multiagent"]);
    let parent_only: Vec<String> = parent_group_keys
        .iter()
        .filter(|k| !child_group_keys.contains(k))
        .cloned()
        .collect();
    assert_eq!(
        parent_only,
        vec!["attempt".to_string(), "depends_on".to_string()]
    );
}

/// The child event omits the parent-only keys — absent, never `null`.
#[tokio::test]
async fn the_child_event_omits_the_parent_only_keys() {
    let join = child_join(true, LoopOutcome::Settled, Vec::new());
    drive(&join).await;
    let started = thread_started_of(&join.events.lock().unwrap().clone());
    let group = &started["multiagent"];
    for key in ["depends_on", "group", "attempt"] {
        assert!(group.get(key).is_none(), "`{key}` absent on the child");
        assert!(group.get(key) != Some(&Value::Null), "`{key}` never null");
    }
}

// ---------------------------------------------------------------------------
// 4. 🔴 Legacy replay (§6.3:265-269) beside a keyed sibling
// ---------------------------------------------------------------------------

/// A pre-P4 stream still parses and replays byte-identically beside a
/// keyed sibling: the additive keys never disturb the legacy event's
/// serialized bytes, and both rows serialize deterministically.
#[test]
fn legacy_payloads_replay_byte_identically_beside_a_keyed_sibling() {
    let identity = child_identity();
    let thread = Thread {
        id: identity.thread_id,
        parent_session: router_session(),
        pinned_version: 7,
    };
    let message = wrapper_message(&identity);
    let worker = child_session_id(identity.thread_id);
    let legacy = ev(
        router_session(),
        1,
        EventKind::ThreadStarted,
        thread_started_payload(&thread, worker, &message),
    );
    let keyed = ev(
        router_session(),
        2,
        EventKind::ThreadStarted,
        audit::child_thread_started_payload(
            thread_started_payload(&thread, worker, &message),
            identity.spawn_uid,
            identity.task_id,
            identity.assignee.as_deref(),
        ),
    );

    // The legacy event round-trips byte-identically...
    let legacy_json = serde_json::to_string(&legacy).expect("serialize");
    let reparsed: Event = serde_json::from_str(&legacy_json).expect("parse");
    assert_eq!(
        serde_json::to_string(&reparsed).expect("re-serialize"),
        legacy_json,
        "legacy bytes are stable across a parse/replay cycle"
    );
    // ...alone and beside the keyed sibling (no cross-contamination).
    let pair = serde_json::to_string(&vec![legacy.clone(), keyed.clone()]).expect("serialize pair");
    let reparsed_pair: Vec<Event> = serde_json::from_str(&pair).expect("parse pair");
    assert_eq!(
        serde_json::to_string(&reparsed_pair[0]).expect("re-serialize"),
        legacy_json,
        "the legacy row is unchanged beside a keyed sibling"
    );
    assert_ne!(
        serde_json::to_string(&reparsed_pair[1]).expect("re-serialize"),
        legacy_json
    );

    // The inverse of omit-when-unset: stripping the two additive keys
    // off the keyed payload yields EXACTLY the legacy payload bytes —
    // the class-A keys are strictly additive over the frozen base,
    // never a reformat of it.
    let mut stripped = keyed.bytes.clone();
    stripped
        .as_object_mut()
        .expect("object")
        .remove("spawn_uid");
    stripped
        .as_object_mut()
        .expect("object")
        .remove("multiagent");
    assert_eq!(
        serde_json::to_string(&stripped).expect("serialize"),
        serde_json::to_string(&legacy.bytes).expect("serialize"),
        "stripping the additive keys restores the legacy payload exactly"
    );
}

// ---------------------------------------------------------------------------
// 5. 🔴 derive_model_context is unchanged (EVENT-MODEL §4.2:188-190)
// ---------------------------------------------------------------------------

/// The same stream with and without the class-A keys yields identical
/// model context: the keys ride outside `content`, and
/// `derive_model_context` — which reads `role`/`content`/`synthetic`
/// only — never puts them in a request.
#[test]
fn derive_model_context_is_unchanged_by_the_new_keys() {
    let identity = child_identity();
    let thread = Thread {
        id: identity.thread_id,
        parent_session: router_session(),
        pinned_version: 7,
    };
    let message = wrapper_message(&identity);
    let worker = child_session_id(identity.thread_id);
    let base_payload = thread_started_payload(&thread, worker, &message);
    let keyed_payload = audit::child_thread_started_payload(
        base_payload.clone(),
        identity.spawn_uid,
        identity.task_id,
        identity.assignee.as_deref(),
    );
    let keyed_completed = audit::thread_completed_payload_keyed(
        thread_completed_payload(identity.thread_id, "completed", json!({"text": "done"})),
        identity.spawn_uid,
        identity.task_id,
        Some(kian_superagent::TaskOutcome::CompletedUnverified),
        None,
        None,
    );
    let session = sid(0xC6_0042);
    let shared = [
        ev(session, 1, EventKind::SessionCreated, json!({})),
        ev(
            session,
            2,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "summarize the run"}),
        ),
    ];
    let unkeyed = shared
        .iter()
        .cloned()
        .chain([ev(
            session,
            3,
            EventKind::ThreadStarted,
            base_payload.clone(),
        )])
        .collect::<Vec<_>>();
    let keyed = shared
        .iter()
        .cloned()
        .chain([
            ev(session, 3, EventKind::ThreadStarted, keyed_payload),
            ev(session, 4, EventKind::ThreadCompleted, keyed_completed),
        ])
        .collect::<Vec<_>>();
    let plain = derive_model_context(&unkeyed).expect("context derives");
    let with_keys = derive_model_context(&keyed).expect("context derives");
    assert_eq!(
        kian_core::context::context_to_bytes(&plain),
        kian_core::context::context_to_bytes(&with_keys),
        "identical model-context bytes with and without the class-A keys"
    );
}

// ---------------------------------------------------------------------------
// 6. Pin P4-3: outcome_for_completion agrees with derive_outcome on all
//    nine values; the three no-carrier values are unwritable
// ---------------------------------------------------------------------------

fn worker_verdict_event(passed: bool) -> Event {
    ev(
        sid(0xC6_0100),
        1,
        EventKind::EvidenceVerdict,
        json!({"gate": WORKER_GATE_NAME, "passed": passed, "checks": [], "attempt": 0}),
    )
}

fn schema_verdict_event(passed: bool) -> Event {
    ev(
        sid(0xC6_0101),
        1,
        EventKind::EvidenceVerdict,
        json!({"gate": "output_schema", "passed": passed, "checks": [], "attempt": 1}),
    )
}

fn completed(status: &str) -> Event {
    ev(
        sid(0xC6_0102),
        2,
        EventKind::ThreadCompleted,
        json!({"thread_id": Uuid::nil().to_string(), "status": status,
               "summary": {"text": "s"}}),
    )
}

fn turn_end_error(class: &str) -> Event {
    ev(
        sid(0xC6_0103),
        1,
        EventKind::TurnEnd,
        json!({"iteration": 1, "error": {"class": class, "action": "none"}}),
    )
}

fn control_interrupt(requested_by: &str) -> Event {
    ev(
        sid(0xC6_0104),
        1,
        EventKind::ControlInterrupt,
        json!({"action": "interrupt", "reason": null, "requested_by": requested_by,
               "requested_at_ms": 1, "target_epoch": null}),
    )
}

fn session_ended(reason: EndReason) -> Event {
    ev(
        sid(0xC6_0105),
        2,
        EventKind::SessionEnded { reason },
        json!({}),
    )
}

/// For every one of the nine outcome values: a stream that derives it,
/// and the agreement between the committed-stream derivation
/// (`derive_outcome`, the join/board reader) and the at-write helper
/// (`outcome_for_completion`, the §6.2 table applied to the completion
/// being appended). The six completion-borne values agree; the three
/// no-carrier values (`stalled`, `interrupted`, `input_required`) are
/// **unwritable** — the helper returns `None` on their streams and
/// never returns them for any status.
#[test]
fn outcome_for_completion_agrees_with_derive_outcome_on_all_nine_values() {
    use kian_superagent::TaskOutcome;
    // (label, events BEFORE the completion, the completion status,
    //  events AFTER appending the completion, expected derived value)
    type Row = (
        &'static str,
        Vec<Event>,
        &'static str,
        Vec<Event>,
        TaskOutcome,
    );
    let rows: Vec<Row> = vec![
        (
            "completed_verified",
            vec![worker_verdict_event(true)],
            "completed",
            vec![worker_verdict_event(true), completed("completed")],
            TaskOutcome::CompletedVerified,
        ),
        (
            "completed_unverified",
            vec![],
            "completed",
            vec![completed("completed")],
            TaskOutcome::CompletedUnverified,
        ),
        (
            "invalid_output",
            vec![schema_verdict_event(false)],
            "failed",
            vec![schema_verdict_event(false), completed("failed")],
            TaskOutcome::InvalidOutput,
        ),
        (
            "empty_output",
            vec![turn_end_error("empty_response")],
            "failed",
            vec![turn_end_error("empty_response"), completed("failed")],
            TaskOutcome::EmptyOutput,
        ),
        (
            "budget_exhausted",
            vec![turn_end_error("budget_exhausted")],
            "failed",
            vec![turn_end_error("budget_exhausted"), completed("failed")],
            TaskOutcome::BudgetExhausted,
        ),
        (
            "failed",
            vec![turn_end_error("provider_down")],
            "failed",
            vec![turn_end_error("provider_down"), completed("failed")],
            TaskOutcome::Failed,
        ),
    ];
    for (label, before, status, after, expected) in rows {
        assert_eq!(
            derive_outcome(&after, None),
            Some(expected),
            "{label}: the committed stream derives it"
        );
        assert_eq!(
            outcome_for_completion(status, &before, None),
            Some(expected),
            "{label}: the at-write helper agrees"
        );
    }

    // The three no-carrier values: derived on demand, never writable.
    let stalled = vec![
        control_interrupt("child_timeout"),
        session_ended(EndReason::Interrupted),
    ];
    assert_eq!(derive_outcome(&stalled, None), Some(TaskOutcome::Stalled));
    for status in ["completed", "failed", "interrupted"] {
        let written = outcome_for_completion(status, &stalled, None);
        assert_ne!(written, Some(TaskOutcome::Stalled), "stalled is unwritable");
        assert_ne!(
            written,
            Some(TaskOutcome::Interrupted),
            "interrupted is unwritable"
        );
    }
    let interrupted = vec![
        control_interrupt("ops"),
        session_ended(EndReason::Interrupted),
    ];
    assert_eq!(
        derive_outcome(&interrupted, None),
        Some(TaskOutcome::Interrupted)
    );
    assert_eq!(
        outcome_for_completion("interrupted", &interrupted, None),
        None,
        "an interrupt completion carries no outcome key (interrupted is unwritable)"
    );
    // A completion beside the interrupt pair still derives
    // `interrupted` (the pair is the carrier, completion or not) —
    // which is precisely the unwritable value: the helper returns
    // `None`, so the audit key is omitted and the value stays
    // derived on demand.
    assert_eq!(
        derive_outcome(
            &interrupted
                .iter()
                .cloned()
                .chain([completed("interrupted")])
                .collect::<Vec<_>>(),
            None
        ),
        Some(TaskOutcome::Interrupted)
    );
    assert_eq!(
        outcome_for_completion("interrupted", &interrupted, None),
        None
    );
    // input_required: a live waiting child has no completion at all.
    let live = vec![turn_end_error("none")];
    let report = waiting_report();
    assert_eq!(
        derive_outcome(&live, Some(&report)),
        Some(TaskOutcome::InputRequired)
    );
    for status in ["completed", "failed", "interrupted"] {
        assert_ne!(
            outcome_for_completion(status, &live, Some(&report)),
            Some(TaskOutcome::InputRequired),
            "input_required is unwritable"
        );
    }
}

fn waiting_report() -> kian_superagent::ThreadReport {
    kian_superagent::ThreadReport {
        thread_id: Uuid::nil(),
        status: "completed".to_string(),
        state: Some(kian_superagent::ReportState::Waiting),
        summary: json!({"text": "waiting"}),
    }
}

// ---------------------------------------------------------------------------
// 7. evidence_ref: a {session, seq} pointer, never a copy (§7.2:771)
// ---------------------------------------------------------------------------

/// The child's `thread/completed` `evidence_ref` is exactly
/// `{"session": <child session>, "seq": <the committed verdict's seq>}`
/// — a durable pointer at the verdict, carrying none of its content.
#[tokio::test]
async fn evidence_ref_points_at_the_committed_verdict_seq() {
    let join = child_join(true, LoopOutcome::Settled, vec![worker_verdict(3)]);
    drive(&join).await;
    let events = join.events.lock().unwrap().clone();
    let completed = thread_completed_of(&events);
    let verdict_seq = events
        .iter()
        .find(|e| e.kind == EventKind::EvidenceVerdict)
        .expect("verdict committed")
        .seq;
    let group = &completed["multiagent"];
    assert_eq!(group["outcome"], json!("completed_verified"));
    let evidence_ref = group
        .get("evidence_ref")
        .expect("evidence_ref present when a verdict decided the outcome");
    assert_eq!(
        sorted_keys(evidence_ref),
        vec!["seq".to_string(), "session".to_string()],
        "the pointer is exactly {{session, seq}}"
    );
    assert_eq!(evidence_ref["session"], json!(join.child.0.to_string()));
    assert_eq!(evidence_ref["seq"], json!(verdict_seq));
    // Pointer, not copy: none of the verdict's content rides along.
    let serialized = evidence_ref.to_string();
    assert!(!serialized.contains("passed"), "no verdict content");
    assert!(!serialized.contains(WORKER_GATE_NAME), "no gate name");
}

// ---------------------------------------------------------------------------
// 8. anchors.md rule 5: no new event kind, route, table or migration
// ---------------------------------------------------------------------------

/// The exhaustive spelling match: adding or removing an `EventKind`
/// variant breaks this function's compile, and the pinned count pins
/// the catalog at 22 (EVENT-MODEL §3). The migration chain is pinned
/// at its landed seven entries (v0..=v6) — P4 adds payload keys only.
/// [anchors.md rule 5]
#[test]
fn no_new_event_kind_route_table_or_migration() {
    fn spelling(kind: EventKind) -> &'static str {
        use kian_core::events::EventKind::*;
        match kind {
            SessionCreated => "session/created",
            InputAdmitted => "input/admitted",
            DrainClaimed => "drain/claimed",
            TurnStart => "turn/start",
            TurnEnd => "turn/end",
            StepRequest => "step/request",
            AssistantMessage => "assistant/message",
            ToolCall => "tool/call",
            ToolResult => "tool/result",
            Compact => "compact",
            SlotBinding => "slot/binding",
            SlotFallback => "slot/fallback",
            SlotMigration => "slot/migration",
            RecoveryBudgetReset => "recovery/budget-reset",
            ThreadStarted => "thread/started",
            ThreadCompleted => "thread/completed",
            EvidenceVerdict => "evidence/verdict",
            ChannelDelivered => "channel/delivered",
            InputSpilled => "input/spilled",
            ControlInterrupt => "control/interrupt",
            MemoryNote => "memory/note",
            SessionEnded { .. } => "session/ended",
        }
    }
    use kian_core::events::EventKind::*;
    let all = [
        SessionCreated,
        InputAdmitted,
        DrainClaimed,
        TurnStart,
        TurnEnd,
        StepRequest,
        AssistantMessage,
        ToolCall,
        ToolResult,
        Compact,
        SlotBinding,
        SlotFallback,
        SlotMigration,
        RecoveryBudgetReset,
        ThreadStarted,
        ThreadCompleted,
        EvidenceVerdict,
        ChannelDelivered,
        InputSpilled,
        ControlInterrupt,
        MemoryNote,
        SessionEnded {
            reason: EndReason::Completed,
        },
    ];
    assert_eq!(all.len(), 22, "the kind catalog stays at 22");
    assert_eq!(spelling(all[0]), "session/created");

    let chain = kian_store::migrations::chain();
    assert_eq!(chain.len(), 7, "the migration chain stays at seven entries");
    for (index, migration) in chain.iter().enumerate() {
        assert_eq!(
            migration.version, index as u32,
            "versions stay v0..=v6, adjacent and unedited"
        );
    }
}
