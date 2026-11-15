//! (R17-C7) Acceptance probes for the P4 `depends_on`/`group`
//! arguments, readiness gating, and the attempt ordinal
//! (`design/MULTIAGENT-MODEL.md` §1:177-179, §4.4, §5.4, §7.3:788-809;
//! plan §0.2 pins P4-1/P4-4/P4-5).
//!
//! Readiness is a **pure function of the primary**
//! ([`kian_superagent::ready_deps`]): the parent's own committed
//! stream plus an explicit child-stream lookup — never a view (M7,
//! READ-MODEL R8), never live config. Evaluated at the spawn, before
//! `max_children`, only when `superagent.board.gating` is on (pin
//! P4-1: gating off ⇒ no `depends_on`/`group` argument at all, no
//! readiness evaluated — P3 bytes exactly).
//!
//! Every fixture (ids, templates, streams, fakes) is authored here.
//! No network, no daemon.

use futures::future::BoxFuture;
use kian_core::events::{Event, EventKind};
use kian_core::model::{Epoch, SessionId};
use kian_core::seams::{ToolCall, ToolProvider};
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    AdmitReceipt, BoardGating, DepReadiness, InboxWriteError, InboxWriter, MultiagentLimits,
    RouterHold, RouterProfile, RouterTools, SPAWN_WORKER_TOOL, StreamPort, StreamPortError,
    WorkerSpawner, audit, child_session_id, ready_deps, router_template, task_id_for_key,
    worker::WORKER_GATE_NAME, worker_template,
};
use serde_json::{Value, json};
use std::collections::HashMap;
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

/// An inbox fake that records every admit and wake.
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

/// The child-streams fake with scripted per-session streams: the
/// readiness walk's ONLY view of a dependency's child.
struct MapStreams {
    map: Mutex<HashMap<SessionId, Vec<Event>>>,
}

impl MapStreams {
    fn empty() -> Arc<Self> {
        Arc::new(Self {
            map: Mutex::new(HashMap::new()),
        })
    }
    fn with(entries: Vec<(SessionId, Vec<Event>)>) -> Arc<Self> {
        Arc::new(Self {
            map: Mutex::new(entries.into_iter().collect()),
        })
    }
}

impl kian_superagent::WorkerStreamsPort for MapStreams {
    fn read_stream(
        &self,
        session: SessionId,
    ) -> BoxFuture<'static, Result<Vec<Event>, kian_superagent::WorkerStreamsError>> {
        let events = self
            .map
            .lock()
            .unwrap()
            .get(&session)
            .cloned()
            .unwrap_or_default();
        Box::pin(async move { Ok(events) })
    }
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

fn router_session() -> SessionId {
    sid(0xC7_0001)
}

fn router_tool_call(seq: u64, call_id: &str) -> Event {
    ev(
        router_session(),
        seq,
        EventKind::ToolCall,
        json!({
            "role": "assistant",
            "content": {"id": call_id, "name": SPAWN_WORKER_TOOL,
                        "arguments": {"task": "the work"}},
        }),
    )
}

struct RouterRig {
    tools: Arc<RouterTools>,
    port: Arc<RouterPort>,
    inbox: Arc<CaptureInbox>,
}

/// One assembled parent surface: multiagent on, a one-entry roster,
/// and the pin-P4-1 class-A gate set by `gating`.
fn router_rig(gating: bool, parent_events: Vec<Event>, streams: Arc<MapStreams>) -> RouterRig {
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
    tools = tools.with_worker_streams(streams);
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

fn spawn_call(call_id: &str, arguments: Value) -> ToolCall {
    ToolCall {
        id: call_id.into(),
        name: SPAWN_WORKER_TOOL.into(),
        arguments,
    }
}

fn sorted_property_names(params: &Value) -> Vec<String> {
    let mut keys: Vec<String> = params["properties"]
        .as_object()
        .expect("properties object")
        .keys()
        .cloned()
        .collect();
    keys.sort();
    keys
}

// ---------------------------------------------------------------------------
// The dependency fixtures: one recorded spawn of task key `dep` whose
// child's stream decides readiness.
// ---------------------------------------------------------------------------

/// A parent-side `thread/started` for the task `key` names as
/// completed, carrying the P4 audit group (the recorded graph's edge
/// source) and the child worker session `worker`.
fn dep_started(key: &str, worker: SessionId, depends_on: &[&str], seq: u64) -> Event {
    let router = router_session();
    let payload = json!({
        "thread_id": Uuid::new_v4().to_string(),
        "worker": worker.0.to_string(),
        "parent": router.0.to_string(),
        "task": {"role": "user", "content": {"task_id":
            task_id_for_key(router, key).to_string()}},
        "multiagent": {
            "task_id": task_id_for_key(router, key).to_string(),
            "attempt": 1,
            "depends_on": depends_on,
        },
    });
    ev(router, seq, EventKind::ThreadStarted, payload)
}

/// A child stream that derives `completed_verified`: a passed
/// worker-gate verdict + a normal completion.
fn verified_child(session: SessionId) -> Vec<Event> {
    vec![
        ev(
            session,
            3,
            EventKind::EvidenceVerdict,
            json!({"gate": WORKER_GATE_NAME, "passed": true,
                   "checks": [{"name": "answer-present", "passed": true}], "attempt": 0}),
        ),
        ev(
            session,
            4,
            EventKind::ThreadCompleted,
            json!({"status": "completed", "summary": {"text": "done"}}),
        ),
    ]
}

/// A child stream that derives `completed_unverified`: a normal
/// completion, no verdict.
fn unverified_child(session: SessionId) -> Vec<Event> {
    vec![ev(
        session,
        4,
        EventKind::ThreadCompleted,
        json!({"status": "completed", "summary": {"text": "an answer, unverified"}}),
    )]
}

/// A child stream that derives `empty_output` (§6.2 row 3): a failed
/// completion whose last `turn/end` error class is `empty_response`.
fn empty_output_child(session: SessionId) -> Vec<Event> {
    vec![
        ev(
            session,
            3,
            EventKind::TurnEnd,
            json!({"error": {"class": "empty_response"}}),
        ),
        ev(
            session,
            4,
            EventKind::ThreadCompleted,
            json!({"status": "failed", "summary": {"text": ""}}),
        ),
    ]
}

/// `ready_deps` over a map-backed child lookup.
fn ready_over(
    parent_events: &[Event],
    own_key: &str,
    keys: &[String],
    accept_unverified: bool,
    children: Vec<(SessionId, Vec<Event>)>,
) -> DepReadiness {
    let map: HashMap<SessionId, Vec<Event>> = children.into_iter().collect();
    ready_deps(
        router_session(),
        parent_events,
        task_id_for_key(router_session(), own_key),
        Some(own_key),
        keys,
        accept_unverified,
        &|session| map.get(&session).cloned().unwrap_or_default(),
    )
}

fn keys(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| (*s).to_string()).collect()
}

// ---------------------------------------------------------------------------
// 1. 🔴 Pin P4-1: gating off ⇒ the P2 schema form, no readiness
// ---------------------------------------------------------------------------

/// With `board.gating: false` the `spawn_worker` schema keeps the P2
/// property set exactly (no `depends_on`, no `group`), a call passing
/// either argument is refused naming the switch, and **no readiness
/// is evaluated** — the refusal is the argument gate itself, and a
/// gated-off spawn that declares no P4 argument dispatches with no
/// readiness read at all (the C6 byte gate owns those bytes).
#[tokio::test]
async fn gating_off_keeps_the_schema_at_the_p2_form() {
    let rig = router_rig(
        false,
        vec![router_tool_call(5, "call-1")],
        MapStreams::empty(),
    );
    let schema = rig
        .tools
        .schemas()
        .into_iter()
        .find(|s| s.name == SPAWN_WORKER_TOOL)
        .expect("spawn schema");
    assert_eq!(
        sorted_property_names(&schema.parameters),
        vec![
            "agent".to_string(),
            "correlation".to_string(),
            "output_schema".to_string(),
            "speculative".to_string(),
            "task".to_string(),
            "task_key".to_string(),
        ],
        "the P2 property set exactly — no depends_on, no group (pin P4-1)"
    );
    assert_eq!(schema.parameters["required"], json!(["task"]));

    // The arguments do not exist while the gate is off: a call passing
    // one is refused naming `superagent.board.gating` — never silently
    // degraded, never evaluated.
    for bad in [
        json!({"task": "w", "agent": "researcher", "task_key": "b",
               "depends_on": ["a"]}),
        json!({"task": "w", "agent": "researcher", "task_key": "b",
               "group": "ingest"}),
    ] {
        let err = rig
            .tools
            .invoke(spawn_call("call-1", bad))
            .await
            .expect_err("P4 arguments are refused while gating is off");
        let text = err.to_string();
        assert!(
            text.contains("superagent.board.gating"),
            "the refusal names the switch: {text}"
        );
    }
    assert!(
        rig.port.emitted.lock().unwrap().is_empty(),
        "no thread/started on the refusal path"
    );
    assert!(
        rig.inbox.admits.lock().unwrap().is_empty(),
        "no row on the refusal path"
    );
}

// ---------------------------------------------------------------------------
// 2. 🔴 Pin P4-1/§7.3:808-809: gating on adds exactly two properties
// ---------------------------------------------------------------------------

/// With `board.gating: true` the schema grows by exactly
/// `depends_on` + `group` — property-set equality against the P2 set
/// plus the two, `required` untouched, and the two shapes are plain
/// (array of strings / string — no enum, no ids, M9).
#[test]
fn gating_on_adds_exactly_two_properties() {
    let tools = {
        let router = router_session();
        let inbox = CaptureInbox::shared();
        let spawner = Arc::new(WorkerSpawner::new(
            router,
            &worker_template("mock", "mock-worker", 7),
            inbox,
        ));
        let mut profile = RouterProfile::new(router, router_template("mock", "mock-router"));
        profile.multiagent = Some(limits());
        let hold = RouterHold::shared();
        let mut tools = RouterTools::new(
            spawner,
            kian_superagent::ChannelRegistry::new(),
            &profile,
            hold,
        );
        tools = tools.with_board_gating(BoardGating(true));
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
        tools
    };
    let schema = tools
        .schemas()
        .into_iter()
        .find(|s| s.name == SPAWN_WORKER_TOOL)
        .expect("spawn schema");
    let mut expected = vec![
        "agent".to_string(),
        "correlation".to_string(),
        "output_schema".to_string(),
        "speculative".to_string(),
        "task".to_string(),
        "task_key".to_string(),
    ];
    // stable ordering: the sorted set is the P2 set + the two, so the
    // delta is exactly {depends_on, group} in sorted position.
    let mut got = sorted_property_names(&schema.parameters);
    expected.push("depends_on".to_string());
    expected.push("group".to_string());
    expected.sort();
    assert_eq!(got, expected, "property-set equality, stable ordering");
    got.clear();
    assert_eq!(
        schema.parameters["required"],
        json!(["task"]),
        "required untouched"
    );
    assert_eq!(
        schema.parameters["properties"]["depends_on"]["type"],
        json!("array")
    );
    assert_eq!(
        schema.parameters["properties"]["depends_on"]["items"]["type"],
        json!("string")
    );
    assert_eq!(
        schema.parameters["properties"]["group"]["type"],
        json!("string")
    );
    assert!(
        schema.parameters["properties"]["group"]
            .get("enum")
            .is_none(),
        "no enum (M9)"
    );
}

// ---------------------------------------------------------------------------
// 3. 🔴 §4.4:512-516 — an unsatisfied dependency refuses the spawn
// ---------------------------------------------------------------------------

/// A dependency with no successful completion refuses the spawn
/// **naming it**, before any side effect: no `thread/started`, no
/// inbox row. An unspawned referent and an outstanding referent are
/// both refused the same honest way.
#[tokio::test]
async fn a_dependency_without_a_successful_completion_refuses_the_spawn_naming_it() {
    // (a) the referent was never spawned at all.
    let rig = router_rig(
        true,
        vec![router_tool_call(5, "call-1")],
        MapStreams::empty(),
    );
    let err = rig
        .tools
        .invoke(spawn_call(
            "call-1",
            json!({"task": "the work", "agent": "researcher", "task_key": "b",
                   "depends_on": ["a"]}),
        ))
        .await
        .expect_err("unmet dependency refuses the spawn");
    let text = err.to_string();
    assert!(
        text.contains("unmet task key(s): a"),
        "the refusal names the unmet key: {text}"
    );
    assert!(
        rig.port.emitted.lock().unwrap().is_empty(),
        "no thread/started (§4.4:512-516)"
    );
    assert!(
        rig.inbox.admits.lock().unwrap().is_empty(),
        "no row (§4.4:512-516)"
    );

    // (b) the referent was spawned but is still outstanding.
    let worker = sid(0xC7_00A1);
    let outstanding = dep_started("a", worker, &[], 4);
    let rig = router_rig(
        true,
        vec![
            router_tool_call(3, "call-0"),
            outstanding,
            router_tool_call(5, "call-1"),
        ],
        MapStreams::with(vec![(worker, vec![])]),
    );
    let err = rig
        .tools
        .invoke(spawn_call(
            "call-1",
            json!({"task": "the work", "agent": "researcher", "task_key": "b",
                   "depends_on": ["a"]}),
        ))
        .await
        .expect_err("outstanding dependency refuses the spawn");
    assert!(
        err.to_string().contains("unmet task key(s): a"),
        "names the key: {err}"
    );
    assert!(rig.port.emitted.lock().unwrap().is_empty());

    // (c) every unmet key is named, not just the first (§4.4).
    let rig = router_rig(
        true,
        vec![router_tool_call(5, "call-1")],
        MapStreams::empty(),
    );
    let err = rig
        .tools
        .invoke(spawn_call(
            "call-1",
            json!({"task": "w", "agent": "researcher", "task_key": "c",
                   "depends_on": ["x", "y"]}),
        ))
        .await
        .expect_err("refused");
    let text = err.to_string();
    assert!(
        text.contains('x') && text.contains('y'),
        "both keys named: {text}"
    );
}

// ---------------------------------------------------------------------------
// 4. 🔴 §4.4:513-516 / §6.1.1:635-638 — empty_output never releases
// ---------------------------------------------------------------------------

/// The M6 honesty test: a dependency whose derived outcome is
/// `empty_output` never releases a dependent — at **any** policy
/// setting (`accept_unverified` both false and true).
#[test]
fn empty_output_never_releases_a_dependent_at_any_policy_setting() {
    let worker = sid(0xC7_00E1);
    let parent = vec![
        router_tool_call(3, "call-0"),
        dep_started("a", worker, &[], 4),
    ];
    for accept_unverified in [false, true] {
        assert_eq!(
            ready_over(
                &parent,
                "b",
                &keys(&["a"]),
                accept_unverified,
                vec![(worker, empty_output_child(worker))]
            ),
            DepReadiness::Unmet(keys(&["a"])),
            "empty_output is never in the success set (accept_unverified={accept_unverified})"
        );
    }
}

/// The success-set parameter: `completed_unverified` releases only
/// when policy accepts unverified work.
#[test]
fn completed_unverified_releases_only_when_policy_accepts_unverified() {
    let worker = sid(0xC7_0051);
    let parent = vec![
        router_tool_call(3, "call-0"),
        dep_started("a", worker, &[], 4),
    ];
    let strict = ready_over(
        &parent,
        "b",
        &keys(&["a"]),
        false,
        vec![(worker, unverified_child(worker))],
    );
    assert_eq!(strict, DepReadiness::Unmet(keys(&["a"])), "strict refuses");
    let lenient = ready_over(
        &parent,
        "b",
        &keys(&["a"]),
        true,
        vec![(worker, unverified_child(worker))],
    );
    assert_eq!(lenient, DepReadiness::Ready, "policy accepts unverified");
    // and completed_verified releases at both settings.
    for accept_unverified in [false, true] {
        assert_eq!(
            ready_over(
                &parent,
                "b",
                &keys(&["a"]),
                accept_unverified,
                vec![(worker, verified_child(worker))]
            ),
            DepReadiness::Ready
        );
    }
}

/// §5.4:607-608: a not-yet-spawned referent is not-ready, not an
/// error — `Unmet`, distinct from `Cycle` and from `Ready`.
#[test]
fn an_unspawned_referent_is_not_ready_and_is_not_an_error() {
    assert_eq!(
        ready_over(
            &[router_tool_call(5, "c1")],
            "b",
            &keys(&["never-spawned"]),
            false,
            vec![]
        ),
        DepReadiness::Unmet(keys(&["never-spawned"])),
        "Unmet, never an error variant"
    );
}

/// §7.3:791-793: a self-reference and a cycle over the parent's
/// recorded graph are refused naming the cycle.
#[test]
fn a_self_reference_or_cycle_is_refused_naming_the_cycle() {
    // (a) self-reference: `x` depends on `x`.
    let parent = vec![router_tool_call(5, "c1")];
    assert_eq!(
        ready_over(&parent, "x", &keys(&["x"]), false, vec![]),
        DepReadiness::Cycle(keys(&["x"])),
        "the one-edge cycle names itself"
    );
    // (b) a two-node cycle over the recorded graph: recorded `y`
    // depends on `x`; spawning `x` with depends_on [y] closes it.
    let worker = sid(0xC7_0061);
    let parent = vec![
        router_tool_call(3, "call-0"),
        dep_started("y", worker, &["x"], 4),
        router_tool_call(5, "c1"),
    ];
    match ready_over(&parent, "x", &keys(&["y"]), false, vec![(worker, vec![])]) {
        DepReadiness::Cycle(path) => {
            assert!(
                path.contains(&"y".to_string()) && path.contains(&"x".to_string()),
                "the cycle names both members: {path:?}"
            );
        }
        other => panic!("expected a cycle refusal, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 5. §1:177-179 — the attempt ordinal on the parent's thread/started
// ---------------------------------------------------------------------------

/// `attempt` = 1 + the count of the parent stream's `thread/started`
/// carrying the same `task_id`: two spawns sharing a `task_key` read
/// attempts 1 and 2 (the pure helper and the emitted audit group
/// agree).
#[tokio::test]
async fn attempt_counts_thread_started_carrying_the_same_task_id() {
    // A prior attempt of the SAME task key is already committed on
    // the parent stream (with the audit group a gated spawn emits).
    let prior = {
        let router = router_session();
        let identity = kian_superagent::SpawnIdentity::derive_delegated(
            router,
            3,
            "kian-research",
            3,
            Some("ingest-feed"),
            Some("researcher".to_string()),
        );
        let worker = child_session_id(identity.thread_id);
        audit::parent_thread_started_payload(
            json!({
                "thread_id": identity.thread_id.to_string(),
                "worker": worker.0.to_string(),
                "parent": router.0.to_string(),
                "task": {"role": "user", "content": {}},
            }),
            &audit::ParentThreadAudit {
                spawn_uid: identity.spawn_uid,
                task_id: identity.task_id,
                assignee: identity.assignee.clone(),
                attempt: 1,
                depends_on: Vec::new(),
                group: None,
            },
        )
    };
    let router = router_session();
    let prior_event = ev(router, 4, EventKind::ThreadStarted, prior);
    // The pure half first (§1:177-179 as a function of events).
    let identity = kian_superagent::SpawnIdentity::derive_delegated(
        router,
        5,
        "kian-research",
        3,
        Some("ingest-feed"),
        Some("researcher".to_string()),
    );
    assert_eq!(
        audit::attempt_of(std::slice::from_ref(&prior_event), identity.task_id),
        2
    );
    // And the emitted half: the second spawn's audit group says 2 and
    // carries the declared P4 facts.
    let satisfied = sid(0xC7_0071);
    let rig = router_rig(
        true,
        vec![
            router_tool_call(3, "call-0"),
            dep_started("a", satisfied, &[], 4),
            prior_event,
            router_tool_call(7, "call-1"),
        ],
        MapStreams::with(vec![(satisfied, verified_child(satisfied))]),
    );
    rig.tools
        .invoke(spawn_call(
            "call-1",
            json!({"task": "the work", "agent": "researcher", "task_key": "ingest-feed",
                   "depends_on": ["a"], "group": "ingest"}),
        ))
        .await
        .expect("the satisfied dependency releases the spawn");
    let emitted = rig.port.emitted.lock().unwrap().clone();
    assert_eq!(emitted.len(), 1, "exactly one thread/started");
    let group = &emitted[0].1["multiagent"];
    assert_eq!(
        group["attempt"],
        json!(2),
        "1 + the prior same-task_id started"
    );
    assert_eq!(
        group["depends_on"],
        json!(["a"]),
        "the declared deps ride the audit group"
    );
    assert_eq!(
        group["group"],
        json!("ingest"),
        "the declared group rides the audit group"
    );
}

// ---------------------------------------------------------------------------
// 6. 🔴 M7 / READ-MODEL R8 — readiness reads the primary, never a view
// ---------------------------------------------------------------------------

/// With a deliberately stale `task_board` row claiming the dependency
/// is ready (outcome `completed_verified`, ready `true` — the exact
/// JSON a stale view would serve), the spawn still refuses: the
/// evaluation reads the parent's stream and the dependency child's
/// stream, and nothing else. The board is observability; a stale view
/// may never authorise a spawn (§4.2 surface B, READ-MODEL R8).
#[tokio::test]
async fn readiness_reads_the_primary_never_the_view() {
    let worker = sid(0xC7_0081);
    // The PRIMARY: task `a` was spawned and its child is outstanding
    // (empty child stream — no completion).
    let rig = router_rig(
        true,
        vec![
            router_tool_call(3, "call-0"),
            dep_started("a", worker, &[], 4),
            router_tool_call(5, "call-1"),
        ],
        MapStreams::with(vec![(worker, Vec::new())]),
    );
    // The STALE VIEW: a task_board row claiming `a` completed
    // verified and the dependent ready — planted exactly where a view
    // would live (a disposable derived document). Readiness must not
    // be able to see it.
    let stale_view = json!({
        "task_id": task_id_for_key(router_session(), "a").to_string(),
        "outcome": "completed_verified",
        "ready": true,
        "evidence_provenance": "recorded",
    });
    assert_eq!(stale_view["outcome"], json!("completed_verified"));
    let err = rig
        .tools
        .invoke(spawn_call(
            "call-1",
            json!({"task": "the work", "agent": "researcher", "task_key": "b",
                   "depends_on": ["a"]}),
        ))
        .await
        .expect_err("the stale view must not release the spawn");
    assert!(
        err.to_string().contains("unmet task key(s): a"),
        "the primary's verdict wins: {err}"
    );
    assert!(
        rig.port.emitted.lock().unwrap().is_empty(),
        "no thread, no row"
    );
}
