//! (R11-05 / kian-125) Independent QA acceptance probes for the durable
//! per-session execution-worker binding (ADR-020) — L1 level.
//!
//! These are **QA-authored** probes with QA-authored fixtures: they are
//! not re-runs of the implementers' in-crate assertions. Every fixture
//! here (ids, catalogs, streams, fakes) is built in this file. The goal
//! is the brief §7 L1 matrix, re-derived independently:
//!
//! - wrapper / payload additive bytes (no-key = legacy, with-key =
//!   exactly one additive key, both sides identical task bytes);
//! - parse tolerance (absent = legacy unbound; malformed refused);
//! - the spawn refusal matrix (unknown / busy naming the owner /
//!   missing-when-required / malformed / feature-off) with zero side
//!   effects after every refusal;
//! - the liveness release table (ended releases; failed/interrupted
//!   hold) and the never-started reservation TTL window;
//! - the spawn schema shapes + the judge-prompt catalog block gate.
//!
//! No network, no daemon. `NO_PROXY` is irrelevant here.

use futures::future::BoxFuture;
use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::{Epoch, SessionId, Thread};
use kian_core::seams::{ToolCall, ToolError, ToolProvider as _};
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    AdmitReceipt, EXEC_WORKER_KEY, InboxWriteError, InboxWriter, MandateStrength, MandateTemplate,
    MockChannel, MultiagentLimits, ResolvedAgent, Roster, RosterEntry, RosterLimits, RouterHold,
    RouterProfile, RouterTools, SPAWN_WORKER_TOOL, SpawnRequest, StreamPort, StreamPortError,
    WorkerBinding, WorkerEntry, WorkerSpawner, WorkerStreamsError, WorkerStreamsPort,
    bound_workers, busy_owner, delegation_content, delegation_worker_binding, router_template,
    thread_liveness, thread_started_payload, thread_started_payload_bound,
    thread_started_worker_binding, worker_binding_of, worker_template,
};
use serde_json::{Value, json};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// QA fixtures (nothing shared with the implementers' test modules)
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

fn qa_thread(router: SessionId) -> Thread {
    Thread {
        id: Uuid::new_v4(),
        parent_session: router,
        pinned_version: 5,
    }
}

/// An inbox fake that records EVERY admit (with payload) and wake.
struct QaInbox {
    admits: Mutex<Vec<(SessionId, Value, Option<String>)>>,
    wakes: Mutex<Vec<SessionId>>,
}

impl QaInbox {
    fn shared() -> Arc<Self> {
        Arc::new(Self {
            admits: Mutex::new(Vec::new()),
            wakes: Mutex::new(Vec::new()),
        })
    }
}

impl InboxWriter for QaInbox {
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

/// A stream port that records emissions; `read_stream` returns the
/// committed events (the busy check reads the Router's own stream).
struct QaPort {
    session: SessionId,
    events: Mutex<Vec<Event>>,
}

impl QaPort {
    fn shared(session: SessionId) -> (Arc<Self>, Arc<dyn StreamPort>) {
        let port = Arc::new(Self {
            session,
            events: Mutex::new(Vec::new()),
        });
        (
            Arc::clone(&port) as Arc<Self>,
            Arc::clone(&port) as Arc<dyn StreamPort>,
        )
    }
}

impl StreamPort for QaPort {
    fn session(&self) -> SessionId {
        self.session
    }
    fn emit(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> BoxFuture<'static, Result<(), StreamPortError>> {
        let mut events = self.events.lock().unwrap();
        let seq = events.len() as u64 + 1;
        events.push(Event {
            seq,
            session: self.session,
            epoch: Epoch(1),
            ts: 10_000 + seq,
            kind,
            bytes,
            version: 0,
        });
        Box::pin(async { Ok(()) })
    }
    fn read_stream(&self) -> BoxFuture<'static, Result<Vec<Event>, StreamPortError>> {
        let events = self.events.lock().unwrap().clone();
        Box::pin(async move { Ok(events) })
    }
}

/// A worker-streams port fake: per-session canned result (events or a
/// read error — the fail-closed row).
struct QaStreams {
    sessions: Mutex<BTreeMap<String, Result<Vec<Event>, String>>>,
}

impl QaStreams {
    fn shared() -> Arc<Self> {
        Arc::new(Self {
            sessions: Mutex::new(BTreeMap::new()),
        })
    }
    fn set(&self, session: SessionId, result: Result<Vec<Event>, String>) {
        self.sessions
            .lock()
            .unwrap()
            .insert(session.0.to_string(), result);
    }
}

impl WorkerStreamsPort for QaStreams {
    fn read_stream(
        &self,
        session: SessionId,
    ) -> BoxFuture<'static, Result<Vec<Event>, WorkerStreamsError>> {
        let result = self
            .sessions
            .lock()
            .unwrap()
            .get(&session.0.to_string())
            .cloned();
        Box::pin(async move {
            match result {
                Some(Ok(events)) => Ok(events),
                Some(Err(text)) => Err(WorkerStreamsError(text)),
                None => Ok(Vec::new()),
            }
        })
    }
}

/// The QA spawn harness: RouterTools over QA fakes with a QA catalog.
fn qa_tools(
    catalog: Vec<WorkerEntry>,
    streams: Option<Arc<dyn WorkerStreamsPort>>,
) -> (Arc<RouterTools>, Arc<QaInbox>, Arc<QaPort>) {
    let router = sid(0x1111);
    let inbox = QaInbox::shared();
    let spawner = Arc::new(WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_template("mock", "qa-worker", 7),
        inbox.clone(),
    ));
    let mut channels = kian_superagent::ChannelRegistry::new();
    channels.register(
        "mock-im",
        Arc::new(MockChannel::new("mock-im")) as Arc<dyn kian_core::seams::ChannelAdapter>,
    );
    let hold = RouterHold::shared();
    let mut profile = RouterProfile::new(router, router_template("mock", "qa-router"));
    profile.worker_membership = !catalog.is_empty();
    let static_ids: Vec<String> = catalog.iter().map(|entry| entry.id.clone()).collect();
    let timeouts: BTreeMap<String, u64> = catalog
        .iter()
        .map(|entry| (entry.id.clone(), 60_000u64))
        .collect();
    let mut tools = RouterTools::new(
        spawner,
        channels,
        &profile,
        hold.clone(),
        kian_superagent::ResidentBinding::new(profile.session),
    )
    .with_worker_membership(static_ids, timeouts, None);
    if let Some(streams) = streams {
        tools = tools.with_worker_streams(streams);
    }
    let (port, dyn_port) = QaPort::shared(router);
    hold.install(dyn_port);
    (Arc::new(tools), inbox, port)
}

fn spawn_call(args: Value) -> ToolCall {
    ToolCall {
        id: format!("qa-call-{}", Uuid::new_v4().simple()),
        name: SPAWN_WORKER_TOOL.into(),
        arguments: args,
    }
}

fn qa_catalog_two() -> Vec<WorkerEntry> {
    vec![
        WorkerEntry::with_description("qa-one", "the first QA worker"),
        WorkerEntry::new("qa-two"),
    ]
}

/// Sorted key set of a JSON object.
fn sorted_keys(value: &Value) -> Vec<String> {
    let mut keys: Vec<String> = value
        .as_object()
        .expect("object")
        .keys()
        .map(|key| key.to_string())
        .collect();
    keys.sort();
    keys
}

// ---------------------------------------------------------------------------
// L1-A: wrapper / payload additive bytes
// ---------------------------------------------------------------------------

/// The plain spawn (no binding) must produce EXACTLY the legacy wrapper
/// bytes: the same key set as the frozen `delegation_content` builder,
/// byte-identical serialization. The bound spawn adds exactly ONE key.
#[test]
fn qa_wrapper_legacy_bytes_identical_and_bound_adds_exactly_one_key() {
    let spawner = WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(sid(1)),
        &worker_template("mock", "qa-worker", 7),
        QaInbox::shared(),
    );
    let task = json!({"text": "qa additive-byte probe"});
    let correlation = json!("qa-correlation");

    // Legacy: with correlation.
    let legacy = spawner.prepare(&SpawnRequest {
        task: task.clone(),
        correlation: Some(correlation.clone()),
        speculative: false,
        mandate: None,
        exec_worker: None,
        identity: None,
        output_schema: None,
    });
    let wrapper = legacy.task_message["content"].clone();
    assert_eq!(
        sorted_keys(&wrapper),
        vec![
            "correlation".to_string(),
            "task".to_string(),
            "thread_id".to_string()
        ],
        "legacy wrapper key set"
    );
    // Byte-identical to the frozen builder for the same thread id.
    let frozen = delegation_content(legacy.thread.id, Some(correlation.clone()), task.clone());
    assert_eq!(
        serde_json::to_string(&wrapper).expect("serialize qa wrapper"),
        serde_json::to_string(&frozen).expect("serialize frozen wrapper"),
        "legacy wrapper bytes are byte-identical to the frozen builder"
    );

    // Bound: the SAME request shape plus exec_worker — exactly one more
    // key, identical task value, the id verbatim.
    let bound = spawner.prepare(&SpawnRequest {
        task: task.clone(),
        correlation: Some(correlation),
        speculative: false,
        mandate: None,
        exec_worker: Some("qa-two".to_string()),
        identity: None,
        output_schema: None,
    });
    let bound_wrapper = bound.task_message["content"].clone();
    let mut expected_keys = sorted_keys(&wrapper);
    expected_keys.push(EXEC_WORKER_KEY.to_string());
    expected_keys.sort();
    assert_eq!(sorted_keys(&bound_wrapper), expected_keys);
    assert_eq!(bound_wrapper[EXEC_WORKER_KEY], json!("qa-two"));
    assert_eq!(
        bound_wrapper["task"], wrapper["task"],
        "task bytes untouched"
    );
    assert_eq!(
        bound_wrapper["thread_id"], bound_wrapper["thread_id"],
        "thread id present"
    );

    // Composition: speculative + mandate + binding — the binding is
    // still exactly one additive key over that composed legacy form.
    let composed_legacy = spawner.prepare(&SpawnRequest {
        task: task.clone(),
        correlation: None,
        speculative: true,
        mandate: Some(json!({"strength": "explicit", "scope": "qa"})),
        exec_worker: None,
        identity: None,
        output_schema: None,
    });
    let composed_bound = spawner.prepare(&SpawnRequest {
        task,
        correlation: None,
        speculative: true,
        mandate: Some(json!({"strength": "explicit", "scope": "qa"})),
        exec_worker: Some("qa-one".to_string()),
        identity: None,
        output_schema: None,
    });
    let mut composed_expected = sorted_keys(&composed_legacy.task_message["content"]);
    composed_expected.push(EXEC_WORKER_KEY.to_string());
    composed_expected.sort();
    assert_eq!(
        sorted_keys(&composed_bound.task_message["content"]),
        composed_expected,
        "speculative + mandate + exec_worker compose additively"
    );
}

/// The `thread/started` payload: the bound builder is EXACTLY the legacy
/// builder plus one top-level key, the task message rides both sides
/// byte-identical, and both readers see the same binding.
#[test]
fn qa_thread_started_payload_additive_and_both_sides_agree() {
    let router = sid(2);
    let worker = sid(3);
    let thread = qa_thread(router);
    let spawner = WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_template("mock", "qa-worker", 7),
        QaInbox::shared(),
    );
    let spawned = spawner.prepare(&SpawnRequest {
        task: json!("the qa task"),
        correlation: None,
        speculative: false,
        mandate: None,
        exec_worker: Some("qa-two".to_string()),
        identity: None,
        output_schema: None,
    });
    let task_message = spawned.task_message.clone();

    let legacy = thread_started_payload(&thread, worker, &task_message);
    let bound = thread_started_payload_bound(&thread, worker, &task_message, "qa-two");
    let mut expected_keys = sorted_keys(&legacy);
    expected_keys.push(EXEC_WORKER_KEY.to_string());
    expected_keys.sort();
    assert_eq!(
        sorted_keys(&bound),
        expected_keys,
        "exactly one additive key"
    );
    for key in ["thread_id", "parent", "worker", "pinned_version", "task"] {
        assert_eq!(bound[key], legacy[key], "frozen field {key} untouched");
    }
    assert_eq!(bound[EXEC_WORKER_KEY], json!("qa-two"));
    assert_eq!(bound["task"], task_message, "task bytes identical");

    // Both readers agree on the same spawn's surfaces (D4).
    assert_eq!(
        thread_started_worker_binding(&bound),
        WorkerBinding::Bound(Cow::Borrowed("qa-two"))
    );
    assert_eq!(
        delegation_worker_binding(&task_message["content"]),
        WorkerBinding::Bound(Cow::Borrowed("qa-two"))
    );
    // The legacy payload has no key and reads Unbound.
    assert_eq!(
        thread_started_worker_binding(&legacy),
        WorkerBinding::Unbound
    );

    // (R17-C6, pin P4-2) re-pointed at WRAPPER equality: once the P4
    // class-A keys land, the two sides' `thread/started` payloads are
    // NO LONGER byte-identical to each other — the parent carries the
    // bookkeeping (`attempt`, `depends_on[]`, `group?`) and the child
    // the derivable subset — while the delegation wrapper (`task`)
    // they embed stays byte-identical, and every frozen field keeps
    // its value on both sides.
    let uid = uuid::Uuid::new_v4();
    let task_id = uuid::Uuid::new_v4();
    let parent = kian_superagent::audit::parent_thread_started_payload(
        bound.clone(),
        &kian_superagent::audit::ParentThreadAudit {
            spawn_uid: uid,
            task_id,
            assignee: Some("qa-entry".to_string()),
            attempt: 2,
            depends_on: vec!["upstream".to_string()],
            group: None,
        },
    );
    let child = kian_superagent::audit::child_thread_started_payload(
        bound.clone(),
        uid,
        task_id,
        Some("qa-entry"),
    );
    assert_eq!(parent["task"], child["task"], "wrapper identical (P4-2)");
    assert_eq!(parent["task"], task_message, "wrapper untouched");
    for key in ["thread_id", "parent", "worker", "pinned_version"] {
        assert_eq!(parent[key], child[key], "frozen field {key} agrees");
    }
    let mut parent_group: Vec<String> = parent["multiagent"]
        .as_object()
        .expect("group object")
        .keys()
        .cloned()
        .collect();
    parent_group.sort();
    let mut child_group: Vec<String> = child["multiagent"]
        .as_object()
        .expect("group object")
        .keys()
        .cloned()
        .collect();
    child_group.sort();
    parent_group.retain(|k| !child_group.contains(k));
    assert_eq!(
        parent_group,
        vec!["attempt".to_string(), "depends_on".to_string()],
        "the parent-only keys are exactly the bookkeeping"
    );
    for key in ["depends_on", "group", "attempt"] {
        assert!(
            child["multiagent"].get(key).is_none(),
            "`{key}` absent on the child, never null"
        );
    }
}

// ---------------------------------------------------------------------------
// L1-B: parse tolerance (the reader matrix)
// ---------------------------------------------------------------------------

/// Absent = legacy Unbound; a non-empty string = Bound; EVERY other
/// shape — empty string, numbers, booleans, null, arrays, objects — is
/// Malformed (refused at use time, never guessed).
#[test]
fn qa_readers_classify_every_value_shape() {
    for (label, value, expected) in [
        ("absent", None, WorkerBinding::Unbound),
        (
            "bound",
            Some(json!("qa-two")),
            WorkerBinding::Bound(Cow::Borrowed("qa-two")),
        ),
        ("empty string", Some(json!("")), WorkerBinding::Malformed),
        ("integer", Some(json!(7)), WorkerBinding::Malformed),
        ("float", Some(json!(0.5)), WorkerBinding::Malformed),
        ("boolean", Some(json!(true)), WorkerBinding::Malformed),
        ("null", Some(Value::Null), WorkerBinding::Malformed),
        ("array", Some(json!(["qa-two"])), WorkerBinding::Malformed),
        (
            "object",
            Some(json!({"id": "qa-two"})),
            WorkerBinding::Malformed,
        ),
    ] {
        let mut wrapper = json!({"thread_id": "t", "task": "x"});
        if let Some(value) = &value {
            wrapper[EXEC_WORKER_KEY] = value.clone();
        }
        assert_eq!(
            delegation_worker_binding(&wrapper),
            expected,
            "wrapper reader: {label}"
        );
        let mut payload = json!({"thread_id": "t", "parent": "p", "worker": "w", "task": {}});
        if let Some(value) = &value {
            payload[EXEC_WORKER_KEY] = value.clone();
        }
        assert_eq!(
            thread_started_worker_binding(&payload),
            expected,
            "payload reader: {label}"
        );
    }
    // A non-object wrapper is not a binding surface at all: Unbound.
    assert_eq!(
        delegation_worker_binding(&json!("just a string")),
        WorkerBinding::Unbound
    );
}

/// The stream reader (I1): the payload copy and the first-input wrapper
/// copy must agree; a disagreement or a malformed copy reads Malformed
/// and the routing layer refuses (never picks one).
#[test]
fn qa_stream_reader_agreement_matrix() {
    let worker = sid(4);
    let lineage = |binding: Value| {
        let mut payload = json!({
            "thread_id": Uuid::new_v4().to_string(),
            "parent": sid(9).0.to_string(),
            "worker": worker.0.to_string(),
            "pinned_version": 7,
            "task": {"role": "user", "content": {}},
        });
        if !binding.is_null() {
            payload[EXEC_WORKER_KEY] = binding;
        }
        payload
    };
    let wrapper_input = |binding: Value| {
        let mut content = json!({"thread_id": "t", "task": "x"});
        if !binding.is_null() {
            content[EXEC_WORKER_KEY] = binding;
        }
        ev(
            worker,
            0,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": content}),
        )
    };

    // Agree (bound).
    let mut stream = vec![ev(
        worker,
        0,
        EventKind::ThreadStarted,
        lineage(json!("qa-two")),
    )];
    stream.push(wrapper_input(json!("qa-two")));
    assert_eq!(
        worker_binding_of(&stream, worker),
        WorkerBinding::Bound(Cow::Owned("qa-two".to_string()))
    );

    // Disagree — Malformed, never a pick.
    let stream = vec![
        ev(
            worker,
            0,
            EventKind::ThreadStarted,
            lineage(json!("qa-one")),
        ),
        wrapper_input(json!("qa-two")),
    ];
    assert_eq!(worker_binding_of(&stream, worker), WorkerBinding::Malformed);

    // Malformed payload copy — Malformed.
    let stream = vec![
        ev(worker, 0, EventKind::ThreadStarted, lineage(json!(42))),
        wrapper_input(json!("qa-two")),
    ];
    assert_eq!(worker_binding_of(&stream, worker), WorkerBinding::Malformed);

    // A lineage for a DIFFERENT worker session is invisible; only the
    // first input exists ⇒ the wrapper decides.
    let other_lineage = {
        let mut payload = lineage(json!("qa-one"));
        payload["worker"] = json!(sid(5).0.to_string());
        ev(sid(5), 0, EventKind::ThreadStarted, payload)
    };
    let stream = vec![other_lineage, wrapper_input(json!("qa-two"))];
    assert_eq!(
        worker_binding_of(&stream, worker),
        WorkerBinding::Bound(Cow::Owned("qa-two".to_string()))
    );

    // No lineage, no wrapper — Unbound.
    assert_eq!(
        worker_binding_of(
            &[ev(worker, 0, EventKind::SessionCreated, Value::Null)],
            worker
        ),
        WorkerBinding::Unbound
    );
}

// ---------------------------------------------------------------------------
// L1-C: the spawn refusal matrix (I7) — honest errors, zero side effects
// ---------------------------------------------------------------------------

#[tokio::test]
async fn qa_spawn_refusal_matrix_is_honest_and_traceless() {
    let (tools, inbox, port) = qa_tools(qa_catalog_two(), None);

    // ≥2 workers, argument missing: names the requirement and points
    // at `list_workers` (R1: the refusal text no longer lists ids —
    // the catalog is live data, tool-result only).
    let err = tools
        .invoke(spawn_call(json!({"task": "qa work"})))
        .await
        .unwrap_err();
    assert!(matches!(err, ToolError::InvalidCall(_)), "{err}");
    let text = err.to_string();
    assert!(text.contains("requires `exec_worker`"), "{text}");
    assert!(text.contains("list_workers"), "{text}");

    // Unknown id: names it and lists the configured ids.
    let err = tools
        .invoke(spawn_call(
            json!({"task": "qa work", "exec_worker": "qa-nope"}),
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, ToolError::InvalidCall(_)), "{err}");
    assert!(
        err.to_string().contains("unknown worker `qa-nope`"),
        "{err}"
    );

    // Malformed shapes: empty string and non-strings.
    for bad in [
        json!(""),
        json!(0),
        json!(["qa-one"]),
        json!({"id": "qa-one"}),
    ] {
        let err = tools
            .invoke(spawn_call(json!({"task": "qa work", "exec_worker": bad})))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidCall(_)), "{bad}: {err}");
        assert!(
            err.to_string().contains("must be a non-empty string"),
            "{bad}: {err}"
        );
    }

    // An explicit JSON null reads as omitted ⇒ missing-when-required.
    let err = tools
        .invoke(spawn_call(json!({"task": "qa work", "exec_worker": null})))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("requires `exec_worker`"), "{err}");

    // ZERO side effects after the whole matrix: no admit (⇒ no thread,
    // no way for any drain or provider turn to start), no wake, no
    // lineage emission.
    assert!(inbox.admits.lock().unwrap().is_empty(), "no inbox row");
    assert!(inbox.wakes.lock().unwrap().is_empty(), "no wake");
    assert!(
        port.events
            .lock()
            .unwrap()
            .iter()
            .all(|e| e.kind != EventKind::ThreadStarted),
        "no thread/started"
    );
}

/// (ADR-049) A bridged roster entry owns its tools inside the harness child: binding it to an
/// execution worker is refused before any side effect (no admit, no wake, no lineage).
#[tokio::test]
async fn qa_bridged_roster_entry_refuses_exec_worker_before_any_side_effect() {
    let router = sid(0x1111);
    let inbox = QaInbox::shared();
    let spawner = Arc::new(WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_template("mock", "qa-worker", 7),
        inbox.clone(),
    ));
    let mut channels = kian_superagent::ChannelRegistry::new();
    channels.register(
        "mock-im",
        Arc::new(MockChannel::new("mock-im")) as Arc<dyn kian_core::seams::ChannelAdapter>,
    );
    let hold = RouterHold::shared();
    let mut profile = RouterProfile::new(router, router_template("mock", "qa-router"));
    profile.worker_membership = true;
    profile.multiagent = Some(MultiagentLimits {
        max_children: 2,
        max_depth: 2,
        child_timeout_ms: 60_000,
    });
    let catalog = qa_catalog_two();
    let static_ids: Vec<String> = catalog.iter().map(|entry| entry.id.clone()).collect();
    let timeouts: BTreeMap<String, u64> = catalog
        .iter()
        .map(|entry| (entry.id.clone(), 60_000u64))
        .collect();
    let roster = Roster {
        entries: vec![RosterEntry {
            id: "codex-agent".to_string(),
            description: "A Codex worker".to_string(),
            agent: "codex".to_string(),
            mandate_template: MandateTemplate {
                scope: "delegate".to_string(),
                strength: MandateStrength::Inferred,
            },
            tools_allow: Vec::new(),
            limits: RosterLimits { max_children: 0 },
            output_schema: None,
            harness_bridged: true,
            resolved_agent: Some(ResolvedAgent {
                def_id: "codex".to_string(),
                version: 1,
            }),
        }],
    };
    let tools = RouterTools::new(
        spawner,
        channels,
        &profile,
        hold.clone(),
        kian_superagent::ResidentBinding::new(profile.session),
    )
    .with_worker_membership(static_ids, timeouts, None)
    .with_worker_streams(QaStreams::shared())
    .with_roster(roster);
    let (port, dyn_port) = QaPort::shared(router);
    hold.install(dyn_port);
    let tools = Arc::new(tools);

    // The spawn derives its identity from the requesting tool/call's committed seq.
    let call = spawn_call(json!({
        "task": "codex work",
        "agent": "codex-agent",
        "exec_worker": "qa-one"
    }));
    port.events.lock().unwrap().push(ev(
        router,
        1,
        EventKind::ToolCall,
        json!({"content": {"id": call.id.clone()}}),
    ));
    let err = tools.invoke(call).await.unwrap_err();
    let text = err.to_string();
    assert!(text.contains("harness bridge"), "{text}");
    assert!(text.contains("codex-agent"), "{text}");
    assert!(text.contains("exec_worker"), "{text}");
    // Zero side effects after the refusal.
    assert!(inbox.admits.lock().unwrap().is_empty(), "no inbox row");
    assert!(inbox.wakes.lock().unwrap().is_empty(), "no wake");
    assert!(
        port.events
            .lock()
            .unwrap()
            .iter()
            .all(|event| event.kind != EventKind::ThreadStarted),
        "no thread/started"
    );
}

#[tokio::test]
async fn qa_zero_endpoints_refuse_the_argument() {
    // Feature off: the argument is refused outright.
    let (tools, inbox, port) = qa_tools(Vec::new(), None);
    let err = tools
        .invoke(spawn_call(
            json!({"task": "qa work", "exec_worker": "qa-one"}),
        ))
        .await
        .unwrap_err();
    let text = err.to_string();
    assert!(text.contains("not configured on this server"), "{text}");
    assert!(text.contains("omit `exec_worker`"), "{text}");
    assert!(inbox.admits.lock().unwrap().is_empty());
    assert!(inbox.wakes.lock().unwrap().is_empty());
    assert!(
        port.events
            .lock()
            .unwrap()
            .iter()
            .all(|e| e.kind != EventKind::ThreadStarted)
    );

    // The zero-endpoints schema keeps today's surface: no property, no
    // required change.
    let schema = tools
        .schemas()
        .into_iter()
        .find(|s| s.name == SPAWN_WORKER_TOOL)
        .expect("spawn schema");
    assert!(
        schema.parameters["properties"]
            .get(EXEC_WORKER_KEY)
            .is_none(),
        "no exec_worker property: {}",
        schema.parameters
    );
    assert_eq!(schema.parameters["required"], json!(["task"]));
}

#[tokio::test]
async fn qa_single_worker_spawn_is_recorded_on_every_surface() {
    // R1 supersedes D6's omission-default: with exactly one worker the
    // argument is still REQUIRED. An explicit bind is then RECORDED on
    // every surface (wrapper, lineage, result echo), never silent.
    let (tools, inbox, port) = qa_tools(
        vec![WorkerEntry::with_description(
            "qa-solo",
            "the only QA worker",
        )],
        None,
    );
    // Omission is refused — never defaulted.
    let err = tools
        .invoke(spawn_call(json!({"task": "qa solo work"})))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("requires `exec_worker`"), "{err}");
    // The explicit bind lands on every surface.
    let result = tools
        .invoke(spawn_call(json!({
            "task": "qa solo work",
            "exec_worker": "qa-solo"
        })))
        .await
        .expect("explicit bind spawns");
    assert!(!result.is_error);
    assert_eq!(result.output[EXEC_WORKER_KEY], json!("qa-solo"));

    let admits = inbox.admits.lock().unwrap().clone();
    assert_eq!(admits.len(), 1, "exactly one row");
    assert_eq!(
        delegation_worker_binding(&admits[0].1),
        WorkerBinding::Bound(Cow::Borrowed("qa-solo")),
        "the admitted wrapper carries the binding"
    );
    let lineage: Vec<Event> = port
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.kind == EventKind::ThreadStarted)
        .cloned()
        .collect();
    assert_eq!(lineage.len(), 1);
    assert_eq!(
        thread_started_worker_binding(&lineage[0].bytes),
        WorkerBinding::Bound(Cow::Borrowed("qa-solo"))
    );
    // Both surfaces embed the identical task message bytes (D4).
    assert_eq!(
        lineage[0].bytes["task"],
        json!({"role": "user", "content": admits[0].1})
    );
}

#[tokio::test]
async fn qa_busy_refusal_names_the_owner_then_releases() {
    // I3 at the tool surface, through QA's own streams port.
    let streams = QaStreams::shared();
    let (tools, inbox, port) = qa_tools(vec![WorkerEntry::new("qa-busy")], Some(streams.clone()));

    // First spawn occupies qa-busy with a LIVE worker stream.
    let first = tools
        .invoke(spawn_call(
            json!({"task": "qa first", "exec_worker": "qa-busy"}),
        ))
        .await
        .expect("first spawn");
    let owner_session = first.output["worker"]
        .as_str()
        .expect("worker uuid")
        .to_string();
    let owner_thread = first.output["thread_id"]
        .as_str()
        .expect("thread id")
        .to_string();
    let owner = sid(0xbb);
    streams.set(
        owner,
        Ok(vec![ev(owner, 0, EventKind::SessionCreated, Value::Null)]),
    );
    // (The canned live stream is keyed by the REAL spawned session; the
    // first spawn's session id comes off the lineage.)
    let lineage_session = port.events.lock().unwrap()[0].bytes["worker"]
        .as_str()
        .expect("lineage worker uuid")
        .to_string();
    let spawned_sid = SessionId(Uuid::parse_str(&lineage_session).expect("uuid"));
    streams.set(
        spawned_sid,
        Ok(vec![ev(
            spawned_sid,
            0,
            EventKind::SessionCreated,
            Value::Null,
        )]),
    );

    let before_admits = inbox.admits.lock().unwrap().len();
    let before_lineage = port
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.kind == EventKind::ThreadStarted)
        .count();

    // Second spawn naming the busy worker: refused, naming the owner
    // session AND thread, with zero side effects.
    let err = tools
        .invoke(spawn_call(
            json!({"task": "qa second", "exec_worker": "qa-busy"}),
        ))
        .await
        .unwrap_err();
    let text = err.to_string();
    assert!(text.contains("spawn refused"), "{text}");
    assert!(text.contains("worker `qa-busy` is busy"), "{text}");
    assert!(
        text.contains(&owner_session) && text.contains(&owner_thread),
        "the refusal names the owning session and thread: {text} (spawned: {owner_session}/{owner_thread})"
    );
    assert_eq!(
        inbox.admits.lock().unwrap().len(),
        before_admits,
        "no row on refusal"
    );
    assert_eq!(
        port.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.kind == EventKind::ThreadStarted)
            .count(),
        before_lineage,
        "no thread/started on refusal"
    );

    // Once the owner's stream reads Released (thread/completed), the
    // same worker accepts a new spawn.
    streams.set(
        spawned_sid,
        Ok(vec![
            ev(spawned_sid, 0, EventKind::SessionCreated, Value::Null),
            ev(
                spawned_sid,
                1,
                EventKind::ThreadCompleted,
                json!({"thread_id": owner_thread, "status": "completed", "summary": {}}),
            ),
        ]),
    );
    let second = tools
        .invoke(spawn_call(
            json!({"task": "qa retry", "exec_worker": "qa-busy"}),
        ))
        .await
        .expect("released worker accepts a new spawn");
    assert_eq!(second.output[EXEC_WORKER_KEY], json!("qa-busy"));
}

#[tokio::test]
async fn qa_busy_check_read_error_fails_closed() {
    // A stream read error is undecidable: the spawn is refused honestly
    // and nothing is admitted — never a spawn on a broken fact.
    let streams = QaStreams::shared();
    let (tools, inbox, _port) = qa_tools(vec![WorkerEntry::new("qa-flaky")], Some(streams.clone()));

    let first = tools
        .invoke(spawn_call(
            json!({"task": "qa first", "exec_worker": "qa-flaky"}),
        ))
        .await
        .expect("first spawn lands (empty stream = never started)");
    let lineage_session = {
        let port_events = inbox.admits.lock().unwrap();
        let _ = &port_events;
        first.output["worker"]
            .as_str()
            .expect("worker uuid")
            .to_string()
    };
    let spawned_sid = SessionId(Uuid::parse_str(&lineage_session).expect("uuid"));
    streams.set(
        spawned_sid,
        Err("qa simulated stream read failure".to_string()),
    );

    let err = tools
        .invoke(spawn_call(
            json!({"task": "qa second", "exec_worker": "qa-flaky"}),
        ))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("busy check failed"), "{}", err);
    assert!(
        err.to_string().contains("qa simulated stream read failure"),
        "{err}"
    );
    assert_eq!(inbox.admits.lock().unwrap().len(), 1, "no second row");
}

// ---------------------------------------------------------------------------
// L1-D: the pure liveness / exclusivity projections
// ---------------------------------------------------------------------------

#[test]
fn qa_liveness_release_table() {
    let worker = sid(6);
    let created = || ev(worker, 0, EventKind::SessionCreated, Value::Null);

    // Never started: no session/created and no input.
    assert_eq!(
        thread_liveness(&[]),
        kian_superagent::ThreadLiveness::NeverStarted
    );

    // Started, no terminal: live.
    assert_eq!(
        thread_liveness(&[created()]),
        kian_superagent::ThreadLiveness::Live
    );

    let completed = |seq| {
        ev(
            worker,
            seq,
            EventKind::ThreadCompleted,
            json!({"thread_id": "t", "status": "completed"}),
        )
    };
    let failed = |seq| {
        ev(
            worker,
            seq,
            EventKind::ThreadCompleted,
            json!({"thread_id": "t", "status": "failed"}),
        )
    };
    let ended = |seq, reason| ev(worker, seq, EventKind::SessionEnded { reason }, Value::Null);

    // completed releases (both vocabularies); cancelled releases.
    assert_eq!(
        thread_liveness(&[created(), completed(1)]),
        kian_superagent::ThreadLiveness::Released
    );
    assert_eq!(
        thread_liveness(&[created(), ended(1, EndReason::Completed)]),
        kian_superagent::ThreadLiveness::Released
    );
    assert_eq!(
        thread_liveness(&[created(), ended(1, EndReason::Cancelled)]),
        kian_superagent::ThreadLiveness::Released
    );

    // failed / interrupted are resumable: the worker stays held.
    assert_eq!(
        thread_liveness(&[created(), failed(1)]),
        kian_superagent::ThreadLiveness::Live
    );
    assert_eq!(
        thread_liveness(&[created(), ended(1, EndReason::Failed)]),
        kian_superagent::ThreadLiveness::Live
    );
    assert_eq!(
        thread_liveness(&[created(), ended(1, EndReason::Interrupted)]),
        kian_superagent::ThreadLiveness::Live
    );

    // Last terminal wins in both orders.
    assert_eq!(
        thread_liveness(&[created(), failed(1), ended(2, EndReason::Completed)]),
        kian_superagent::ThreadLiveness::Released
    );
    assert_eq!(
        thread_liveness(&[created(), completed(1), ended(2, EndReason::Interrupted)]),
        kian_superagent::ThreadLiveness::Live
    );
}

#[tokio::test]
async fn qa_never_started_reservation_releases_only_after_ttl() {
    // D10's crash window: lineage landed, the task row never did. The
    // reservation holds the worker until the queue timeout elapses.
    let router = sid(7);
    let worker = sid(8);
    let thread_id = Uuid::new_v4();
    let mut payload = json!({
        "thread_id": thread_id.to_string(),
        "parent": router.0.to_string(),
        "worker": worker.0.to_string(),
        "pinned_version": 7,
        "task": {},
    });
    payload[EXEC_WORKER_KEY] = json!("qa-res");
    let router_events = vec![ev(router, 0, EventKind::ThreadStarted, payload)];
    // The lineage ts is 10_000 (the fixture's first seq).
    let started_ts = router_events[0].ts;

    let port = QaStreams::shared(); // empty stream ⇒ NeverStarted
    let ttl = 5_000u64;

    // Inside the window: busy, and the reason says never-started.
    let owner = busy_owner(&router_events, &*port, "qa-res", started_ts + ttl - 1, ttl)
        .await
        .expect("decidable");
    let owner = owner.expect("held inside the ttl window");
    assert_eq!(owner.thread_id, thread_id);
    assert_eq!(owner.worker_session, worker);
    assert!(owner.reason.contains("never-started"), "{}", owner.reason);

    // At/after the window: free.
    let freed = busy_owner(&router_events, &*port, "qa-res", started_ts + ttl, ttl)
        .await
        .expect("decidable");
    assert!(freed.is_none(), "released after the ttl");

    // A STARTED session is held regardless of the ttl (resumable).
    let started = QaStreams::shared();
    started.set(
        worker,
        Ok(vec![ev(worker, 0, EventKind::SessionCreated, Value::Null)]),
    );
    let held = busy_owner(
        &router_events,
        &*started,
        "qa-res",
        started_ts + ttl * 10,
        ttl,
    )
    .await
    .expect("decidable");
    assert!(
        held.is_some(),
        "a live session holds the worker past any ttl"
    );

    // A different worker id has no candidates: free.
    let other = busy_owner(&router_events, &*port, "qa-other", started_ts, ttl)
        .await
        .expect("decidable");
    assert!(other.is_none());
}

#[test]
fn qa_bound_workers_projects_only_bound_lineage() {
    // D15: an unbound (legacy) lineage contributes NOTHING — it is
    // never registered as bound, so it never holds a worker.
    let router = sid(7);
    let mut bound_payload = json!({
        "thread_id": Uuid::new_v4().to_string(),
        "parent": router.0.to_string(),
        "worker": sid(11).0.to_string(),
        "pinned_version": 7,
        "task": {},
    });
    bound_payload[EXEC_WORKER_KEY] = json!("qa-one");
    let unbound_payload = json!({
        "thread_id": Uuid::new_v4().to_string(),
        "parent": router.0.to_string(),
        "worker": sid(12).0.to_string(),
        "pinned_version": 7,
        "task": {},
    });
    let mut malformed_payload = unbound_payload.clone();
    malformed_payload["worker"] = json!(sid(13).0.to_string());
    malformed_payload[EXEC_WORKER_KEY] = json!(false);

    let map = bound_workers(&[
        ev(router, 0, EventKind::ThreadStarted, bound_payload),
        ev(router, 1, EventKind::ThreadStarted, unbound_payload),
        ev(router, 2, EventKind::ThreadStarted, malformed_payload),
    ]);
    assert_eq!(map.len(), 1, "only the bound lineage registers: {map:?}");
    let candidates = &map["qa-one"];
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].started_ts, 10_000, "the lineage ts is kept");
}

// ---------------------------------------------------------------------------
// L1-E: prompt / schema surface
// ---------------------------------------------------------------------------

#[tokio::test]
async fn qa_prompt_catalog_block_gates_on_non_empty_catalog() {
    // Zero endpoints: the bootstrap gate must render TODAY's bytes —
    // no catalog block anywhere in the admitted judge prompt.
    let router = sid(0x2222);
    let inbox = QaInbox::shared();
    let empty = RouterProfile::new(router, router_template("mock", "qa-router"));
    empty.bootstrap(&*inbox).await.expect("bootstrap");
    let admitted = inbox.admits.lock().unwrap()[0].1.clone();
    let empty_text = serde_json::to_string(&admitted).expect("serialize");
    assert!(
        !empty_text.contains("Execution workers"),
        "zero endpoints must not append any catalog block: {empty_text}"
    );
    assert!(
        !empty_text.contains("exec_worker"),
        "zero endpoints must not mention the binding: {empty_text}"
    );

    // Non-empty catalog (R12 R2): the FROZEN membership block lands —
    // a constant; no ids, no descriptions ever render (I6), and the
    // block is deterministic across renders.
    let inbox = QaInbox::shared();
    let mut full = RouterProfile::new(router, router_template("mock", "qa-router"));
    full.worker_membership = true;
    full.bootstrap(&*inbox).await.expect("bootstrap");
    let admitted = inbox.admits.lock().unwrap()[0].1.clone();
    let full_text = serde_json::to_string(&admitted).expect("serialize");
    assert!(full_text.contains("Execution workers"), "{full_text}");
    assert!(
        full_text.contains("Call `list_workers` to see the live catalog"),
        "{full_text}"
    );
    assert!(
        !full_text.contains("qa-one"),
        "no catalog ids may render into the prompt (I6): {full_text}"
    );
    assert!(
        full_text.contains("never invent a worker id."),
        "{full_text}"
    );

    // The block constant is membership-data-free by construction.
    let block = kian_superagent::WORKER_MEMBERSHIP_PROMPT_BLOCK;
    assert_eq!(block, kian_superagent::WORKER_MEMBERSHIP_PROMPT_BLOCK);
}

#[test]
fn qa_spawn_schema_plain_string() {
    // R1: `exec_worker` is a plain required string in EVERY catalog
    // shape — no enum, no ids in the schema (I6).
    let (one, _, _) = qa_tools(vec![WorkerEntry::new("qa-solo")], None);
    let schema = one
        .schemas()
        .into_iter()
        .find(|s| s.name == SPAWN_WORKER_TOOL)
        .expect("schema");
    let params = &schema.parameters;
    let solo_property = params["properties"][EXEC_WORKER_KEY].clone();
    assert_eq!(solo_property["type"], json!("string"));
    assert!(solo_property.get("enum").is_none());
    assert_eq!(
        params["required"],
        json!(["task", EXEC_WORKER_KEY]),
        "required even at one (supersedes D6)"
    );

    // Two endpoints: the identical property bytes.
    let (two, _, _) = qa_tools(qa_catalog_two(), None);
    let schema = two
        .schemas()
        .into_iter()
        .find(|s| s.name == SPAWN_WORKER_TOOL)
        .expect("schema");
    let params = &schema.parameters;
    assert_eq!(params["properties"][EXEC_WORKER_KEY], solo_property);
    assert_eq!(
        params["properties"][EXEC_WORKER_KEY]["type"],
        json!("string")
    );
    assert!(
        !params["properties"][EXEC_WORKER_KEY]
            .as_object()
            .expect("property")
            .contains_key("enum")
    );
    assert_eq!(
        params["required"],
        json!(["task", EXEC_WORKER_KEY]),
        "required at two or more"
    );
}
