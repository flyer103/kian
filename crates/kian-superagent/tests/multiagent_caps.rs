//! (R15-C3) Acceptance probes for the `spawn_worker` caps surface of
//! the multiagent delegation core (plan §4 C3): the seq-anchored
//! identity derivation (pin P2), the §5.4 validation order (every
//! refusal before every side effect), the `max_depth` / `max_children`
//! caps (§5.3), and the spawn ack (pin P4).
//!
//! Every fixture (ids, ports, inbox fake) is authored here — no
//! network, no daemon, no store. The feature-off leg is the
//! byte-identity control: without `multiagent` the tool result, the
//! wrapper and the `thread/started` payload are today's exact bytes.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use kian_core::events::{Event, EventKind};
use kian_core::model::{Epoch, SessionId, Thread};
use kian_core::seams::{ToolCall, ToolProvider};
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    AdmitReceipt, ChannelRegistry, DELIVER_STRUCTURED_TOOL, InboxWriteError, InboxWriter,
    MandatePolicy, MultiagentLimits, ProfileError, REPLY_TOOL, RouterHold, RouterProfile,
    RouterTools, SPAWN_WORKER_TOOL, SpawnIdentity, StreamPort, StreamPortError, SuperAgentProfile,
    TaskOutcome, WorkerSpawner, WorkerStreamsError, WorkerStreamsPort, delegation_content,
    is_channel_tool, router_template, spawn_uid, thread_started_payload, worker_template,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn sid(n: u128) -> SessionId {
    SessionId(Uuid::from_u128(n))
}

/// The resident root Router session every plain rig uses.
fn router_session() -> SessionId {
    sid(0xC3_0001)
}

fn ev(session: SessionId, seq: u64, kind: EventKind, bytes: Value) -> Event {
    Event {
        seq,
        session,
        epoch: Epoch(2),
        ts: 1_000 + seq,
        kind,
        bytes,
        version: 0,
    }
}

/// The loop's log-before-execute record of one `spawn_worker` call —
/// the pin-P2 derivation boundary (`bytes.content.id` names the call).
fn tool_call(session: SessionId, seq: u64, call_id: &str) -> Event {
    ev(
        session,
        seq,
        EventKind::ToolCall,
        json!({
            "role": "assistant",
            "content": {"id": call_id, "name": SPAWN_WORKER_TOOL, "arguments": {"task": "the work"}},
        }),
    )
}

/// One `thread/started` lineage event on `parent`'s stream for a
/// child `worker` (the exact payload shape `spawn_worker` emits).
fn child_started(parent: SessionId, seq: u64, worker: SessionId) -> Event {
    ev(
        parent,
        seq,
        EventKind::ThreadStarted,
        json!({
            "thread_id": Uuid::new_v4().to_string(),
            "parent": parent.0.to_string(),
            "worker": worker.0.to_string(),
            "pinned_version": 7,
            "task": {"role": "user", "content": {}},
        }),
    )
}

/// The worker template's stable id (`worker_template` hardcodes it) —
/// the `agent_def` the derived identity records.
const WORKER_TEMPLATE_ID: &str = "kian-worker";

fn default_limits() -> MultiagentLimits {
    MultiagentLimits {
        max_children: 4,
        max_depth: 1,
        child_timeout_ms: 900_000,
    }
}

/// An inbox fake that records every admit (payload + key) and wake.
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

/// The requesting session's own stream port: serves the committed
/// events, records every emit.
struct RecPort {
    session: SessionId,
    events: Mutex<Vec<Event>>,
    emitted: Mutex<Vec<(EventKind, Value)>>,
}

impl RecPort {
    fn shared(session: SessionId, events: Vec<Event>) -> (Arc<Self>, Arc<dyn StreamPort>) {
        let port = Arc::new(Self {
            session,
            events: Mutex::new(events),
            emitted: Mutex::new(Vec::new()),
        });
        let dyn_port: Arc<dyn StreamPort> = Arc::clone(&port) as Arc<dyn StreamPort>;
        (port, dyn_port)
    }
}

impl StreamPort for RecPort {
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

/// The child-streams port fake: a fixed session → stream map. An
/// unknown session reads as an empty stream (never started).
struct MapStreams {
    streams: Mutex<BTreeMap<String, Vec<Event>>>,
}

impl MapStreams {
    fn shared(entries: Vec<(SessionId, Vec<Event>)>) -> Arc<Self> {
        Arc::new(Self {
            streams: Mutex::new(
                entries
                    .into_iter()
                    .map(|(session, events)| (session.0.to_string(), events))
                    .collect(),
            ),
        })
    }
}

impl WorkerStreamsPort for MapStreams {
    fn read_stream(
        &self,
        session: SessionId,
    ) -> BoxFuture<'static, Result<Vec<Event>, WorkerStreamsError>> {
        let events = self
            .streams
            .lock()
            .unwrap()
            .get(&session.0.to_string())
            .cloned()
            .unwrap_or_default();
        Box::pin(async move { Ok(events) })
    }
}

/// One assembled tool surface over a recording hold. `streams` rides
/// the `with_worker_streams` seam exactly like assembly wiring.
struct Rig {
    tools: Arc<RouterTools>,
    port: Arc<RecPort>,
    inbox: Arc<CaptureInbox>,
}

fn rig_for(
    session: SessionId,
    limits: Option<MultiagentLimits>,
    port_events: Vec<Event>,
    child_streams: Vec<(SessionId, Vec<Event>)>,
    install_streams: bool,
    mandate: Option<MandatePolicy>,
) -> Rig {
    let inbox = CaptureInbox::shared();
    let spawner = Arc::new(WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(session),
        &worker_template("mock", "mock-worker", 7),
        inbox.clone(),
    ));
    let mut profile = RouterProfile::new(session, router_template("mock", "mock-router"));
    profile.multiagent = limits;
    profile.mandate = mandate;
    let hold = RouterHold::shared();
    let tools = RouterTools::new(
        spawner,
        ChannelRegistry::new(),
        &profile,
        hold.clone(),
        kian_superagent::ResidentBinding::new(profile.session),
    );
    let tools = if install_streams {
        tools.with_worker_streams(MapStreams::shared(child_streams))
    } else {
        tools
    };
    let (port, dyn_port) = RecPort::shared(session, port_events);
    hold.install(dyn_port);
    Rig {
        tools: Arc::new(tools),
        port,
        inbox,
    }
}

fn rig(
    limits: Option<MultiagentLimits>,
    port_events: Vec<Event>,
    child_streams: Vec<(SessionId, Vec<Event>)>,
) -> Rig {
    rig_for(
        router_session(),
        limits,
        port_events,
        child_streams,
        true,
        None,
    )
}

fn call(call_id: &str) -> ToolCall {
    ToolCall {
        id: call_id.into(),
        name: SPAWN_WORKER_TOOL.into(),
        arguments: json!({"task": "the work"}),
    }
}

// ---------------------------------------------------------------------------
// 1. The seq-anchored identity (pin P2, M4)
// ---------------------------------------------------------------------------

/// Two spawns in one turn (different committed `tool/call` events)
/// derive different `spawn_uid`s, each exactly
/// `uuid5(parent, "kian:spawn:<own seq>:0")`; re-invoking the same
/// committed call derives the identical uid and the identical child
/// (M4: a replay never mints a second child).
#[tokio::test]
async fn spawn_uid_is_derived_from_the_committed_tool_call_own_seq() {
    let router = router_session();
    let events = vec![
        tool_call(router, 5, "call-1"),
        tool_call(router, 7, "call-2"),
    ];
    let rig = rig(Some(default_limits()), events, vec![]);

    let first = rig.tools.invoke(call("call-1")).await.unwrap();
    let identity1 = SpawnIdentity::derive(router, 5, WORKER_TEMPLATE_ID, 7);
    assert_eq!(
        first.output["spawn_uid"],
        json!(identity1.spawn_uid.to_string()),
        "the uid is the pin-P1/P2 derivation off the call's OWN seq"
    );
    assert_eq!(
        first.output["thread_id"],
        json!(identity1.task_id.to_string())
    );

    // A different call in the same turn: a different seq, a
    // different uid, a different child.
    let second = rig.tools.invoke(call("call-2")).await.unwrap();
    let identity2 = SpawnIdentity::derive(router, 7, WORKER_TEMPLATE_ID, 7);
    assert_eq!(
        second.output["spawn_uid"],
        json!(identity2.spawn_uid.to_string())
    );
    assert_ne!(first.output["spawn_uid"], second.output["spawn_uid"]);
    assert_ne!(first.output["thread_id"], second.output["thread_id"]);
    assert_eq!(
        second.output["spawn_uid"],
        json!(spawn_uid(router, 7, 0).to_string()),
        "exactly uuid5(parent, \"kian:spawn:<seq>:0\")"
    );

    // M4: the same committed call re-invoked recomputes the identical
    // uid, thread and worker session — never a second child.
    let replay = rig.tools.invoke(call("call-1")).await.unwrap();
    assert_eq!(replay.output["spawn_uid"], first.output["spawn_uid"]);
    assert_eq!(replay.output["thread_id"], first.output["thread_id"]);
    assert_eq!(replay.output["worker"], first.output["worker"]);
}

// ---------------------------------------------------------------------------
// 2. The uncommitted-call refusal (pin P2, before every side effect)
// ---------------------------------------------------------------------------

/// A call with no committed `tool/call` event is refused with the
/// honest refusal — before any thread id is minted, before any
/// `thread/started`, before any inbox row.
#[tokio::test]
async fn a_call_without_a_committed_tool_call_event_is_refused_before_any_side_effect() {
    let router = router_session();
    // A stream committed for a DIFFERENT call id, and the empty stream.
    let cases: Vec<Vec<Event>> = vec![vec![tool_call(router, 5, "call-1")], Vec::new()];
    for events in cases {
        let rig = rig(Some(default_limits()), events, vec![]);
        let err = rig.tools.invoke(call("call-404")).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("the requesting tool/call is not committed in this stream"),
            "{err}"
        );
        assert!(rig.port.emitted.lock().unwrap().is_empty(), "no lineage");
        assert!(rig.inbox.admits.lock().unwrap().is_empty(), "no inbox row");
        assert!(rig.inbox.wakes.lock().unwrap().is_empty(), "no wake");
    }
}

// ---------------------------------------------------------------------------
// 3./4. max_children (§5.3:585, §8.3:864)
// ---------------------------------------------------------------------------

/// Four holding children: the fifth spawn refuses, naming the bound
/// and the honest current count (`4` holding, `4` max) — never a
/// silent drop.
#[tokio::test]
async fn max_children_refusal_names_the_bound_and_the_current_count() {
    let router = router_session();
    let children = [
        sid(0xC3_0101),
        sid(0xC3_0102),
        sid(0xC3_0103),
        sid(0xC3_0104),
    ];
    let mut events: Vec<Event> = children
        .iter()
        .enumerate()
        .map(|(i, child)| child_started(router, u64::try_from(i).unwrap() + 1, *child))
        .collect();
    events.push(tool_call(router, 9, "call-5"));
    // All four children are live (started, no terminal event).
    let child_streams = children
        .iter()
        .map(|child| {
            (
                *child,
                vec![
                    ev(*child, 1, EventKind::SessionCreated, json!({})),
                    ev(
                        *child,
                        2,
                        EventKind::InputAdmitted,
                        json!({"content": "hold"}),
                    ),
                ],
            )
        })
        .collect();
    let rig = rig(Some(default_limits()), events, child_streams);

    let err = rig.tools.invoke(call("call-5")).await.unwrap_err();
    let text = err.to_string();
    assert!(text.contains("max_children"), "{text}");
    assert!(
        text.contains("holds 4"),
        "{text} — the honest current count"
    );
    assert!(text.contains("max_children is 4"), "{text} — the bound");
    // Never a silent drop: nothing landed, nothing was minted.
    assert!(rig.port.emitted.lock().unwrap().is_empty(), "no lineage");
    assert!(rig.inbox.admits.lock().unwrap().is_empty(), "no inbox row");
}

/// A `thread/completed{completed}` child releases its slot
/// (§5.3:585): the same fifth spawn that was refused now dispatches.
#[tokio::test]
async fn a_released_child_frees_its_max_children_slot() {
    let router = router_session();
    let children = [
        sid(0xC3_0201),
        sid(0xC3_0202),
        sid(0xC3_0203),
        sid(0xC3_0204),
    ];
    let mut events: Vec<Event> = children
        .iter()
        .enumerate()
        .map(|(i, child)| child_started(router, u64::try_from(i).unwrap() + 1, *child))
        .collect();
    events.push(tool_call(router, 9, "call-5"));
    let child_streams = children
        .iter()
        .enumerate()
        .map(|(i, child)| {
            let mut stream = vec![
                ev(*child, 1, EventKind::SessionCreated, json!({})),
                ev(*child, 2, EventKind::InputAdmitted, json!({"content": "hold"})),
            ];
            if i == 1 {
                // One child completed: its slot is released.
                stream.push(ev(
                    *child,
                    3,
                    EventKind::ThreadCompleted,
                    json!({"thread_id": Uuid::new_v4().to_string(), "status": "completed", "summary": {}}),
                ));
            }
            (*child, stream)
        })
        .collect();
    let rig = rig(Some(default_limits()), events, child_streams);

    let result = rig.tools.invoke(call("call-5")).await.unwrap();
    assert_eq!(result.output["status"], json!("dispatched"));
    assert!(
        result.output["spawn_uid"].is_string(),
        "the freed slot dispatches with the derived identity"
    );
    assert_eq!(rig.port.emitted.lock().unwrap().len(), 1, "one lineage");
    assert_eq!(rig.inbox.admits.lock().unwrap().len(), 1, "one row");
}

// ---------------------------------------------------------------------------
// 5. max_depth (§5.3:586)
// ---------------------------------------------------------------------------

/// A child orchestrator at depth 1 with `max_depth: 1` refuses to
/// spawn, naming `max_depth` and the current hop count.
#[tokio::test]
async fn max_depth_refusal_names_the_bound() {
    let root = sid(0xC3_0300);
    let orchestrator = sid(0xC3_0301);
    // The orchestrator's own stream: its OWN lineage event (naming it
    // as the worker, parent = root) plus the requesting tool/call.
    let events = vec![
        child_started(root, 1, orchestrator),
        tool_call(orchestrator, 2, "call-1"),
    ];
    let rig = rig_for(
        orchestrator,
        Some(default_limits()),
        events,
        vec![],
        true,
        None,
    );

    let err = rig.tools.invoke(call("call-1")).await.unwrap_err();
    let text = err.to_string();
    assert!(text.contains("max_depth"), "{text}");
    assert!(text.contains("1 hop"), "{text} — the current hop count");
    assert!(text.contains("max_depth is 1"), "{text} — the bound");
    assert!(rig.port.emitted.lock().unwrap().is_empty(), "no lineage");
    assert!(rig.inbox.admits.lock().unwrap().is_empty(), "no inbox row");
}

// ---------------------------------------------------------------------------
// 6. §5.4:604-605 — every refusal path, zero side effects
// ---------------------------------------------------------------------------

/// One loop over all refusal paths: the uncommitted call, the absent
/// child-stream port (fail-closed), the depth bound and the children
/// bound — each leaves NO thread, NO row, NO lineage.
#[tokio::test]
async fn every_refusal_creates_no_thread_no_row_and_no_lineage() {
    let router = router_session();
    let children = [
        sid(0xC3_0401),
        sid(0xC3_0402),
        sid(0xC3_0403),
        sid(0xC3_0404),
    ];
    let at_cap: Vec<Event> = children
        .iter()
        .enumerate()
        .map(|(i, child)| child_started(router, u64::try_from(i).unwrap() + 1, *child))
        .chain(std::iter::once(tool_call(router, 9, "call-cap")))
        .collect();
    let live: Vec<(SessionId, Vec<Event>)> = children
        .iter()
        .map(|child| {
            (
                *child,
                vec![ev(*child, 1, EventKind::SessionCreated, json!({}))],
            )
        })
        .collect();
    let orchestrator = sid(0xC3_0410);
    let root = sid(0xC3_0411);

    struct Case(&'static str, Rig, &'static str, &'static str);
    let cases = vec![
        Case(
            "uncommitted tool/call",
            rig(
                Some(default_limits()),
                vec![tool_call(router, 5, "call-9")],
                vec![],
            ),
            "call-1",
            "not committed in this stream",
        ),
        Case(
            "child-stream port absent (fail-closed)",
            rig_for(
                router,
                Some(default_limits()),
                vec![tool_call(router, 5, "call-1")],
                vec![],
                false,
                None,
            ),
            "call-1",
            "child-stream port is not installed",
        ),
        Case(
            "max_depth bound",
            rig_for(
                orchestrator,
                Some(default_limits()),
                vec![
                    child_started(root, 1, orchestrator),
                    tool_call(orchestrator, 2, "call-1"),
                ],
                vec![],
                true,
                None,
            ),
            "call-1",
            "max_depth",
        ),
        Case(
            "max_children bound",
            rig(Some(default_limits()), at_cap, live),
            "call-cap",
            "max_children",
        ),
    ];
    for Case(name, rig, call_id, needle) in cases {
        let err = rig.tools.invoke(call(call_id)).await.unwrap_err();
        assert!(err.to_string().contains(needle), "{name}: {err}");
        assert!(
            rig.port.emitted.lock().unwrap().is_empty(),
            "{name}: no thread/started may be written"
        );
        assert!(
            rig.inbox.admits.lock().unwrap().is_empty(),
            "{name}: no inbox row may land"
        );
        assert!(
            rig.inbox.wakes.lock().unwrap().is_empty(),
            "{name}: no wake"
        );
    }
}

// ---------------------------------------------------------------------------
// 7. Feature off (M8 at the tool boundary)
// ---------------------------------------------------------------------------

/// Without `multiagent` the tool result, the wrapper and the
/// `thread/started` payload are exactly today's bytes: the three-key
/// ack, the frozen wrapper builder, the frozen lineage payload — no
/// identity key anywhere.
#[tokio::test]
async fn multiagent_off_keeps_todays_spawn_result_and_wrapper_bytes() {
    let router = router_session();
    // Feature off must not even need the child-streams port: today's
    // wiring installs nothing new.
    let rig = rig_for(
        router,
        None,
        vec![tool_call(router, 5, "call-1")],
        vec![],
        false,
        None,
    );

    let result = rig.tools.invoke(call("call-1")).await.unwrap();
    // Today's exact result shape: three keys, `status:"dispatched"`.
    assert_eq!(
        result.output,
        json!({
            "thread_id": result.output["thread_id"].clone(),
            "worker": result.output["worker"].clone(),
            "status": "dispatched",
        }),
        "the feature-off ack is exactly the landed three-key shape"
    );
    for key in ["spawn_uid", "task_id", "spawn_outcome"] {
        assert!(result.output.get(key).is_none(), "no `{key}` when off");
    }

    // One lineage event, exactly the frozen payload builder's bytes.
    let emitted = rig.port.emitted.lock().unwrap().clone();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].0, EventKind::ThreadStarted);
    let thread_id = Uuid::parse_str(result.output["thread_id"].as_str().unwrap()).unwrap();
    let worker = SessionId(Uuid::parse_str(result.output["worker"].as_str().unwrap()).unwrap());
    let admits = rig.inbox.admits.lock().unwrap().clone();
    assert_eq!(admits.len(), 1, "one task row");
    let (_, wrapper, key) = &admits[0];
    // The wrapper is exactly the frozen legacy builder over the
    // minted thread id — no identity key, no mandate, no marker.
    assert_eq!(
        *wrapper,
        delegation_content(thread_id, None, json!("the work"))
    );
    for key_name in ["spawn_uid", "task_id", "agent_def", "agent_def_version"] {
        assert!(
            wrapper.get(key_name).is_none(),
            "absent, never null: {key_name}"
        );
    }
    assert!(!wrapper.to_string().contains("\"spawn_uid\""));
    // Pin P3: the task-input key stays `thread:<thread_id>`.
    assert_eq!(key.as_deref(), Some(format!("thread:{thread_id}").as_str()));
    let expected_started = thread_started_payload(
        &Thread {
            id: thread_id,
            parent_session: router,
            pinned_version: 7,
        },
        worker,
        &json!({"role": "user", "content": wrapper.clone()}),
    );
    assert_eq!(emitted[0].1, expected_started, "the frozen payload shape");
}

// ---------------------------------------------------------------------------
// 8. The spawn ack (pin P4, §6.1.2)
// ---------------------------------------------------------------------------

/// The multiagent ack carries `spawn_uid`, `thread_id`, `task_id` and
/// keeps `status:"dispatched"` (pin P4); `spawn_outcome` is
/// `"dispatched"` and is never a child outcome (§6.1.2, F15).
#[tokio::test]
async fn spawn_ack_carries_spawn_uid_thread_id_task_id_and_dispatched() {
    let router = router_session();
    let rig = rig(
        Some(default_limits()),
        vec![tool_call(router, 5, "call-1")],
        vec![],
    );
    let result = rig.tools.invoke(call("call-1")).await.unwrap();
    let identity = SpawnIdentity::derive(router, 5, WORKER_TEMPLATE_ID, 7);

    assert_eq!(
        result.output["status"],
        json!("dispatched"),
        "P4: status stays"
    );
    assert_eq!(result.output["spawn_outcome"], json!("dispatched"));
    assert_eq!(
        result.output["spawn_uid"],
        json!(identity.spawn_uid.to_string())
    );
    assert_eq!(result.output["task_id"], json!(identity.task_id_str()));
    // Pin P7: task_id equals the derived thread id, and the ack's
    // thread_id IS the derived child's thread.
    assert_eq!(result.output["task_id"], result.output["thread_id"]);
    assert_eq!(
        result.output["thread_id"],
        json!(identity.task_id.to_string())
    );

    // The admitted wrapper carries the identity round-trippably
    // (C2's pin-P6 class-B keys) with the resolved template facts.
    let admits = rig.inbox.admits.lock().unwrap().clone();
    assert_eq!(admits.len(), 1);
    assert_eq!(
        admits[0].1["spawn_uid"],
        json!(identity.spawn_uid.to_string())
    );
    assert_eq!(admits[0].1["agent_def"], json!(WORKER_TEMPLATE_ID));
    assert_eq!(admits[0].1["agent_def_version"], json!(7));

    // §6.1.2 / F15: a spawn outcome is never a child outcome —
    // "dispatched" is not in the closed task-outcome vocabulary.
    assert!(
        TaskOutcome::ALL
            .iter()
            .all(|outcome| outcome.as_str() != "dispatched"),
        "the spawn outcome vocabulary is disjoint from the child outcomes"
    );
    assert_eq!(TaskOutcome::parse("dispatched"), None);
}

// ---------------------------------------------------------------------------
// 9. The child mandate's grantor (§5.1:561)
// ---------------------------------------------------------------------------

/// A mandated multiagent spawn stamps the mandate with the SPAWNING
/// session as `granted_by` (the non-channel rule of ADR-012/§5.1) —
/// in both the admitted wrapper and the `thread/started` payload.
#[tokio::test]
async fn the_child_mandate_is_granted_by_the_spawning_session() {
    let router = router_session();
    let mut policy = MandatePolicy::default();
    policy.scopes.insert(
        WORKER_TEMPLATE_ID.to_string(),
        "answer the ticket".to_string(),
    );
    let rig = rig_for(
        router,
        Some(default_limits()),
        vec![tool_call(router, 5, "call-1")],
        vec![],
        true,
        Some(policy),
    );
    let result = rig.tools.invoke(call("call-1")).await.unwrap();
    assert_eq!(result.output["status"], json!("dispatched"));

    let admits = rig.inbox.admits.lock().unwrap().clone();
    assert_eq!(admits.len(), 1);
    let wrapper = &admits[0].1;
    let mandate = wrapper
        .get("mandate")
        .expect("the mandate rides the wrapper");
    assert_eq!(
        mandate["granted_by"],
        json!(router.0.to_string()),
        "§5.1:561 — the spawning session grants the child mandate"
    );
    assert_eq!(mandate["scope"], json!("answer the ticket"));
    // The same mandate object rides the Router-side lineage event's
    // task payload, verbatim.
    let emitted = rig.port.emitted.lock().unwrap().clone();
    assert_eq!(emitted.len(), 1);
    assert_eq!(
        emitted[0].1["task"]["content"]["mandate"]["granted_by"],
        json!(router.0.to_string())
    );
}

// ---------------------------------------------------------------------------
// 10. §5.2's leaf column, pinned structurally
// ---------------------------------------------------------------------------

/// The assembled profiles keep the worker surface a leaf: no
/// `spawn_worker`, no channel tools (`reply`, `deliver_structured`,
/// `channel.*`), no control surface — asserted structurally over
/// `SuperAgentProfile::new`, which already refuses a worker with a
/// channel tool (this pins the P1 invariant; it adds no enforcement).
#[test]
fn the_worker_surface_denies_spawn_channel_reply_and_control_tools() {
    let router_agent = router_template("mock", "mock-router");
    let worker_agent = worker_template("mock", "mock-worker", 7);
    let profile = SuperAgentProfile::new(router_agent.clone(), worker_agent.clone())
        .expect("the stock templates assemble");

    let denied = [SPAWN_WORKER_TOOL, REPLY_TOOL, DELIVER_STRUCTURED_TOOL];
    for tool in &profile.worker.tools {
        assert!(
            !denied.contains(&tool.as_str()),
            "{tool} is a Router dispatch/channel tool — the worker surface is a leaf"
        );
        assert!(
            !tool.starts_with("channel."),
            "{tool} is a channel tool — the worker surface is a leaf"
        );
    }
    // The dispatch/channel column is exactly what `is_channel_tool`
    // classifies (the structural refusal's own vocabulary). The
    // read-only catalog (`list_workers`) is not a channel tool and is
    // simply absent from the worker template.
    for tool in denied {
        assert!(is_channel_tool(tool), "{tool} must classify as denied");
    }
    // The Router keeps the dispatch tool (it is the orchestrator).
    assert!(profile.router.tools.iter().any(|t| t == SPAWN_WORKER_TOOL));

    // And the structural refusal fires for a rogue worker carrying a
    // channel tool — `SuperAgentProfile::new` is the P1 enforcement.
    let mut rogue = worker_agent.clone();
    rogue.tools.push(REPLY_TOOL.to_string());
    assert!(matches!(
        SuperAgentProfile::new(router_agent, rogue),
        Err(ProfileError::WorkerHasChannelTool(_))
    ));
}
