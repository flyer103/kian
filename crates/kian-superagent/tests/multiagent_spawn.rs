//! (R15-C2) Acceptance probes for the spawn side of the multiagent
//! delegation core (plan §4 C2): the derived identity on
//! [`SpawnRequest`], the wrapper's pin-P6 class-B task-identity keys,
//! and the pure report builder.
//!
//! Every fixture (ids, template, inbox fake) is authored here. The
//! legacy path is the byte-identity control: without an identity the
//! two random mints and every landed wrapper byte are unchanged, and
//! the four task-identity keys are absent (never `null`). No network,
//! no daemon.

use futures::future::BoxFuture;
use kian_core::model::SessionId;
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    AdmitReceipt, EXEC_WORKER_KEY, InboxWriteError, InboxWriter, SpawnIdentity, SpawnRequest,
    WorkerSpawner, child_session_id, child_thread_id, delegation_content,
    mandate_delegation_content, thread_report_content,
    worker::{SPECULATIVE_MARKER, spawn_identity_of, thread_report_content_ident},
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

/// A spawner over a fixed router session and template (`mock-worker`
/// v7) whose inbox records every admit.
fn fixture(router: SessionId) -> (WorkerSpawner, Arc<CaptureInbox>) {
    let inbox = CaptureInbox::shared();
    let spawner = WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_template("mock", "mock-worker", 7),
        inbox.clone(),
    );
    (spawner, inbox)
}

/// A request with everything except the identity set, so each probe
/// only names the leg under test.
fn base_request(identity: Option<SpawnIdentity>) -> SpawnRequest {
    SpawnRequest {
        task: json!("do the thing"),
        correlation: Some(json!("evt-9")),
        speculative: false,
        mandate: None,
        exec_worker: None,
        identity,
        output_schema: None,
    }
}

const IDENTITY_KEYS: [&str; 4] = ["spawn_uid", "task_id", "agent_def", "agent_def_version"];

// ---------------------------------------------------------------------------
// 1. Legacy byte identity (the control for every additive change)
// ---------------------------------------------------------------------------

/// Without an identity the wrapper is EXACTLY the frozen builder's
/// bytes over the minted thread id — across every composition (plain,
/// speculative, mandate, exec_worker, and all of them together), the
/// legacy path is untouched.
#[test]
fn legacy_spawn_wrapper_bytes_are_unchanged_without_an_identity() {
    let (spawner, _) = fixture(sid(0x21));
    let task = json!("do the thing");
    let correlation = Some(json!("evt-9"));
    let mandate = json!({"strength": "explicit", "scope": "qa"});

    // Plain legacy: exact JSON equality against the golden form.
    let plain = spawner.prepare(&base_request(None));
    assert_eq!(
        plain.task_message["content"],
        delegation_content(plain.thread.id, correlation.clone(), task.clone())
    );
    // Serialized bytes too (the golden-string discipline).
    assert_eq!(
        serde_json::to_string(&plain.task_message["content"]).unwrap(),
        serde_json::to_string(&delegation_content(
            plain.thread.id,
            correlation.clone(),
            task.clone()
        ))
        .unwrap()
    );

    // Speculative: exactly delegation_content + the marker.
    let mut req = base_request(None);
    req.speculative = true;
    let spec = spawner.prepare(&req);
    let mut expected = delegation_content(spec.thread.id, correlation.clone(), task.clone());
    expected[SPECULATIVE_MARKER] = Value::Bool(true);
    assert_eq!(spec.task_message["content"], expected);

    // Mandate: exactly mandate_delegation_content.
    let mut req = base_request(None);
    req.mandate = Some(mandate.clone());
    let mand = spawner.prepare(&req);
    assert_eq!(
        mand.task_message["content"],
        mandate_delegation_content(mand.thread.id, correlation.clone(), task.clone(), &mandate)
    );

    // Binding: exactly delegation_content + the exec_worker key.
    let mut req = base_request(None);
    req.exec_worker = Some("mac-mini".to_string());
    let bound = spawner.prepare(&req);
    let mut expected = delegation_content(bound.thread.id, correlation.clone(), task.clone());
    expected[EXEC_WORKER_KEY] = json!("mac-mini");
    assert_eq!(bound.task_message["content"], expected);

    // Fully composed legacy form: everything at once, still exactly
    // the composed builders' bytes.
    let composed = spawner.prepare(&SpawnRequest {
        task: task.clone(),
        correlation: correlation.clone(),
        speculative: true,
        mandate: Some(mandate.clone()),
        exec_worker: Some("mac-mini".to_string()),
        identity: None,
        output_schema: None,
    });
    let mut expected = mandate_delegation_content(composed.thread.id, correlation, task, &mandate);
    expected[SPECULATIVE_MARKER] = Value::Bool(true);
    expected[EXEC_WORKER_KEY] = json!("mac-mini");
    assert_eq!(composed.task_message["content"], expected);
}

// ---------------------------------------------------------------------------
// 2. Derived identity (pin P1)
// ---------------------------------------------------------------------------

/// Preparing the same identity twice derives the same thread id, the
/// same worker session, and the same wrapper — a replay never mints a
/// second child (M4). The ids are exactly the pin-P1 derivations.
#[test]
fn derived_spawn_mints_the_thread_and_session_from_the_spawn_uid() {
    let router = sid(0x22);
    let (spawner, _) = fixture(router);
    let identity = SpawnIdentity::derive(router, 12, "mock-worker", 7);

    let first = spawner.prepare(&base_request(Some(identity.clone())));
    let second = spawner.prepare(&base_request(Some(identity.clone())));

    // A replay recomputes the same child — identical everything.
    assert_eq!(first.thread.id, second.thread.id, "thread id is derived");
    assert_eq!(
        first.worker_session, second.worker_session,
        "worker session is derived"
    );
    assert_eq!(
        first.task_message, second.task_message,
        "wrapper is derived"
    );

    // Exactly the pin-P1 derivations, and task_id == thread id (P7).
    assert_eq!(first.thread.id, child_thread_id(router, identity.spawn_uid));
    assert_eq!(first.worker_session, child_session_id(first.thread.id));
    assert_eq!(first.thread.id, identity.task_id);
    assert_eq!(first.thread.parent_session, router);
    assert_eq!(first.thread.pinned_version, 7);

    // A different committed seq is a different spawn: different child.
    let other = SpawnIdentity::derive(router, 13, "mock-worker", 7);
    let third = spawner.prepare(&base_request(Some(other)));
    assert_ne!(third.thread.id, first.thread.id);
    assert_ne!(third.worker_session, first.worker_session);
}

// ---------------------------------------------------------------------------
// 3./4. The pin-P6 wrapper keys — present on new wrappers, absent on legacy
// ---------------------------------------------------------------------------

/// The multiagent wrapper carries exactly the four pin-P6 task
/// identity keys with the identity's values, and `assignee` is absent
/// (P2 — the roster does not exist before P2).
#[test]
fn multiagent_wrapper_carries_the_four_task_identity_keys() {
    let router = sid(0x23);
    let (spawner, _) = fixture(router);
    let identity = SpawnIdentity::derive(router, 4, "mock-worker", 7);
    let spawned = spawner.prepare(&base_request(Some(identity.clone())));
    let wrapper = &spawned.task_message["content"];

    assert_eq!(wrapper["spawn_uid"], json!(identity.spawn_uid.to_string()));
    assert_eq!(wrapper["task_id"], json!(identity.task_id_str()));
    assert_eq!(wrapper["agent_def"], json!("mock-worker"));
    assert_eq!(wrapper["agent_def_version"], json!(7));
    // The wrapper's own thread_id names the same derived thread (P7).
    assert_eq!(wrapper["thread_id"], json!(identity.task_id.to_string()));
    // P2's `assignee` is meaningless before the roster exists.
    assert!(wrapper.get("assignee").is_none(), "assignee is P2, absent");
    // Exactly four keys over the legacy form for the same thread id.
    let legacy = delegation_content(
        spawned.thread.id,
        Some(json!("evt-9")),
        json!("do the thing"),
    );
    assert_eq!(
        wrapper.as_object().map(serde_json::Map::len),
        Some(legacy.as_object().map(serde_json::Map::len).unwrap() + 4),
        "exactly the four identity keys are added"
    );
}

/// On the legacy path the four keys are ABSENT — never written, never
/// `null` — across every composition.
#[test]
fn task_identity_keys_are_absent_not_null_on_the_legacy_path() {
    let (spawner, _) = fixture(sid(0x24));
    let compositions = [
        base_request(None),
        SpawnRequest {
            speculative: true,
            ..base_request(None)
        },
        SpawnRequest {
            mandate: Some(json!({"strength": "explicit", "scope": "qa"})),
            ..base_request(None)
        },
        SpawnRequest {
            exec_worker: Some("mac-mini".to_string()),
            ..base_request(None)
        },
    ];
    for req in &compositions {
        let spawned = spawner.prepare(req);
        let wrapper = &spawned.task_message["content"];
        for key in IDENTITY_KEYS {
            assert!(
                wrapper.get(key).is_none(),
                "legacy wrapper must not carry `{key}` (absent, never null)"
            );
        }
        let serialized = wrapper.to_string();
        for key in IDENTITY_KEYS {
            assert!(
                !serialized.contains(&format!("\"{key}\"")),
                "legacy wrapper bytes must not mention `{key}`"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 5. The admit key (pin P3)
// ---------------------------------------------------------------------------

/// The task-input key is `thread:<thread_id>` on BOTH paths, so a
/// retried admit de-dupes either way: every retry carries the
/// identical key and the identical admitted wrapper bytes.
#[tokio::test]
async fn task_input_key_is_thread_scoped_on_both_paths() {
    let router = sid(0x25);
    let (spawner, inbox) = fixture(router);

    // Legacy path: admit twice, both carries fold onto one row key.
    let legacy = spawner.prepare(&base_request(None));
    spawner.admit(&legacy).await.expect("legacy admit");
    spawner.admit(&legacy).await.expect("legacy re-admit");
    {
        let admits = inbox.admits.lock().unwrap().clone();
        assert_eq!(admits.len(), 2, "both retries reached the inbox fake");
        for (session, payload, key) in &admits {
            assert_eq!(*session, legacy.worker_session);
            assert_eq!(payload, &legacy.task_message["content"]);
            assert_eq!(
                key.as_deref(),
                Some(format!("thread:{}", legacy.thread.id).as_str())
            );
        }
    }

    // Derived path: same thread-scoped key over the derived thread id.
    let identity = SpawnIdentity::derive(router, 9, "mock-worker", 7);
    let derived = spawner.prepare(&base_request(Some(identity.clone())));
    spawner.admit(&derived).await.expect("derived admit");
    spawner.admit(&derived).await.expect("derived re-admit");
    let admits = inbox.admits.lock().unwrap().clone();
    let derived_admits: Vec<_> = admits.iter().skip(2).collect();
    assert_eq!(derived_admits.len(), 2);
    for (session, payload, key) in derived_admits {
        assert_eq!(*session, derived.worker_session);
        assert_eq!(payload, &derived.task_message["content"]);
        // Pin P3: unchanged `thread:<thread_id>` key — the derived
        // thread id is the identity's task id (P7), so the row folds.
        assert_eq!(
            key.as_deref(),
            Some(format!("thread:{}", identity.task_id).as_str())
        );
        assert_eq!(
            key.as_deref(),
            Some(format!("thread:{}", derived.thread.id).as_str())
        );
        // The admitted wrapper carries the identity keys (the durable
        // row the child side later reads them back from).
        assert_eq!(payload["spawn_uid"], json!(identity.spawn_uid.to_string()));
    }
}

// ---------------------------------------------------------------------------
// 6./7. The report builder (pin P6 class-C)
// ---------------------------------------------------------------------------

/// The report content is a pure function of the identity and the
/// committed verdict: same inputs re-derive the same bytes, and the
/// bytes are exactly the legacy triple plus the additive keys.
#[test]
fn report_content_is_a_pure_function_of_the_identity_and_the_committed_verdict() {
    let identity = SpawnIdentity::derive(sid(0x26), 5, "mock-worker", 7);
    let contract = json!({"passed": false, "errors": ["has_artifact"]});
    let summary = json!({"text": "done", "tokens": 42});

    let a = thread_report_content_ident(
        &identity,
        "completed",
        summary.clone(),
        Some(contract.clone()),
    );
    let b = thread_report_content_ident(
        &identity,
        "completed",
        summary.clone(),
        Some(contract.clone()),
    );
    assert_eq!(a, b, "same events re-derive the same bytes");

    // Exactly the legacy triple plus the additive keys, in that
    // composition (the triple itself is untouched).
    let mut expected = thread_report_content(identity.task_id, "completed", summary.clone());
    expected["spawn_uid"] = json!(identity.spawn_uid.to_string());
    expected["task_id"] = json!(identity.task_id_str());
    expected["contract"] = contract;
    assert_eq!(a, expected);

    // The legacy triple is byte-identical inside the derived report.
    assert_eq!(a["thread_id"], json!(identity.task_id.to_string()));
    assert_eq!(a["status"], json!("completed"));
    assert_eq!(a["summary"], summary);

    // A failed verdict with no failed checks is the same pure shape.
    let passed = json!({"passed": true, "errors": []});
    assert_eq!(
        thread_report_content_ident(&identity, "completed", summary, Some(passed.clone()))["contract"],
        passed
    );
}

/// Without a committed gate verdict the `contract` key is absent —
/// never `null` — and the legacy builder itself is unchanged.
#[test]
fn report_content_omits_contract_when_no_gate_verdict_was_committed() {
    let identity = SpawnIdentity::derive(sid(0x27), 6, "mock-worker", 7);
    let summary = json!({"text": "no gate ran"});
    let report = thread_report_content_ident(&identity, "failed", summary.clone(), None);
    assert!(report.get("contract").is_none(), "absent, never null");
    assert!(!report.to_string().contains("\"contract\""));
    // The identity keys still ride.
    assert_eq!(report["spawn_uid"], json!(identity.spawn_uid.to_string()));
    assert_eq!(report["task_id"], json!(identity.task_id_str()));
    // The legacy builder is not modified: its bytes stay the frozen
    // golden triple.
    assert_eq!(
        thread_report_content(identity.task_id, "failed", summary),
        json!({
            "thread_id": identity.task_id.to_string(),
            "status": "failed",
            "summary": json!({"text": "no gate ran"}),
        })
    );
}

// ---------------------------------------------------------------------------
// 8. The wrapper reader
// ---------------------------------------------------------------------------

/// `spawn_identity_of` round-trips a multiagent wrapper back to the
/// identity and returns `None` for legacy wrappers (and partial ones).
#[test]
fn spawn_identity_of_round_trips_and_returns_none_for_legacy_wrappers() {
    let router = sid(0x28);
    let (spawner, _) = fixture(router);
    let identity = SpawnIdentity::derive(router, 8, "mock-worker", 7);

    let spawned = spawner.prepare(&base_request(Some(identity.clone())));
    assert_eq!(
        spawn_identity_of(&spawned.task_message["content"]),
        Some(identity),
        "round trips off the wrapper bytes alone"
    );

    // Legacy wrapper: the keys are absent, so no identity.
    let legacy = spawner.prepare(&base_request(None));
    assert_eq!(spawn_identity_of(&legacy.task_message["content"]), None);

    // Partial wrapper (identity keys stripped one at a time): never
    // guessed, never reconstructed.
    let wrapper = spawned.task_message["content"].clone();
    for key in IDENTITY_KEYS {
        let mut partial = wrapper.clone();
        partial.as_object_mut().unwrap().remove(key);
        assert_eq!(spawn_identity_of(&partial), None, "partial without `{key}`");
    }
}

// ---------------------------------------------------------------------------
// 9. (R17-C2) The three spawn inputs, the pinned resolution, the class-D
//    bytes — the P2 acceptance probes (plan §3 C2). A router-level rig
//    (multiagent on, roster mounted) exercises the §5.4 chain exactly
//    as the tool surface runs it.
// ---------------------------------------------------------------------------

use kian_core::events::{Event, EventKind};
use kian_core::model::Epoch;
use kian_core::seams::{ToolCall, ToolProvider};
use kian_superagent::{
    ChannelRegistry, MandateStrength, MandateTemplate, MultiagentLimits,
    ROSTER_INSTRUCTION_PROMPT_LINE, ResolvedAgent, Roster, RosterEntry, RosterLimits, RouterHold,
    RouterProfile, RouterTools, SPAWN_WORKER_TOOL, StreamPort, StreamPortError, WorkerStreamsError,
    WorkerStreamsPort, router_template, task_id_for_key, thread_started_payload,
};

/// The resident Router session every P2 rig uses.
fn p2_router() -> SessionId {
    sid(0xC2_0001)
}

fn p2_limits() -> MultiagentLimits {
    MultiagentLimits {
        max_children: 4,
        max_depth: 1,
        child_timeout_ms: 900_000,
    }
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

/// The committed `tool/call` the pin-P2 derivation anchors to.
fn p2_tool_call(session: SessionId, seq: u64, call_id: &str) -> Event {
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

/// One roster entry with its resolution stamped (pin P2-1 — the
/// conversion product the spawn side reads).
fn p2_entry(id: &str, def_id: &str, version: u64, schema: Option<Value>) -> RosterEntry {
    RosterEntry {
        id: id.to_string(),
        description: format!("The {id} does one thing well"),
        agent: def_id.to_string(),
        mandate_template: MandateTemplate {
            scope: "advisory scope text".to_string(),
            strength: MandateStrength::Inferred,
        },
        tools_allow: vec!["fs_read".to_string()],
        limits: RosterLimits { max_children: 2 },
        output_schema: schema,
        harness_bridged: false,
        resolved_agent: Some(ResolvedAgent {
            def_id: def_id.to_string(),
            version,
        }),
    }
}

/// The child-streams fake (every child unknown ⇒ never started).
struct P2Streams;

impl WorkerStreamsPort for P2Streams {
    fn read_stream(
        &self,
        _session: SessionId,
    ) -> futures::future::BoxFuture<'static, Result<Vec<Event>, WorkerStreamsError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// The recording stream port (multiagent_caps' RecPort shape).
struct P2Port {
    session: SessionId,
    events: Mutex<Vec<Event>>,
    emitted: Mutex<Vec<(EventKind, Value)>>,
}

impl StreamPort for P2Port {
    fn session(&self) -> SessionId {
        self.session
    }
    fn emit(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> futures::future::BoxFuture<'static, Result<(), StreamPortError>> {
        self.emitted.lock().unwrap().push((kind, bytes));
        Box::pin(async { Ok(()) })
    }
    fn read_stream(
        &self,
    ) -> futures::future::BoxFuture<'static, Result<Vec<Event>, StreamPortError>> {
        let events = self.events.lock().unwrap().clone();
        Box::pin(async move { Ok(events) })
    }
}

struct P2Rig {
    tools: Arc<RouterTools>,
    port: Arc<P2Port>,
    inbox: Arc<CaptureInbox>,
    profile: RouterProfile,
}

/// One assembled tool surface: multiagent on, an optional roster, the
/// child-stream port installed, and one committed `tool/call` per
/// requested id (seq 5 for the first, 7 for a second).
fn p2_rig(roster: Option<Roster>, limits: MultiagentLimits) -> P2Rig {
    p2_rig_with_events(roster, limits, vec![p2_tool_call(p2_router(), 5, "call-1")])
}

fn p2_rig_with_events(
    roster: Option<Roster>,
    limits: MultiagentLimits,
    events: Vec<Event>,
) -> P2Rig {
    let router = p2_router();
    let inbox = CaptureInbox::shared();
    let spawner = Arc::new(WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_template("mock", "mock-worker", 7),
        inbox.clone(),
    ));
    let mut profile = RouterProfile::new(router, router_template("mock", "mock-router"));
    profile.multiagent = Some(limits);
    let hold = RouterHold::shared();
    let mut tools = RouterTools::new(
        spawner,
        ChannelRegistry::new(),
        &profile,
        hold.clone(),
        kian_superagent::ResidentBinding::new(profile.session),
    );
    tools = tools.with_worker_streams(Arc::new(P2Streams));
    if let Some(roster) = roster {
        tools = tools.with_roster(roster);
    }
    let port = Arc::new(P2Port {
        session: router,
        events: Mutex::new(events),
        emitted: Mutex::new(Vec::new()),
    });
    let dyn_port: Arc<dyn StreamPort> = Arc::clone(&port) as Arc<dyn StreamPort>;
    hold.install(dyn_port);
    P2Rig {
        tools: Arc::new(tools),
        port,
        inbox,
        profile,
    }
}

fn p2_call(call_id: &str, arguments: Value) -> ToolCall {
    ToolCall {
        id: call_id.into(),
        name: SPAWN_WORKER_TOOL.into(),
        arguments,
    }
}

fn spawn_schema_of(tools: &Arc<RouterTools>) -> Value {
    tools
        .schemas()
        .into_iter()
        .find(|s| s.name == SPAWN_WORKER_TOOL)
        .expect("spawn_worker on the surface")
        .parameters
}

/// 🔴 The class-D schema growth (§7.3:800-806 / pin P2-6): a non-empty
/// roster adds EXACTLY the three properties `agent` / `task_key` /
/// `output_schema` — plain shapes, no enum, no ids — and `required`
/// stays `["task"]`. An absent roster keeps the pre-P2 property set
/// byte-identically. Ordering is deterministic (the serialized golden
/// is compared as bytes). The prompt half of the class-D change grows
/// by exactly one frozen line, rendered once.
#[test]
fn spawn_worker_schema_grows_by_exactly_three_properties_and_one_prompt_line() {
    let roster = || Roster {
        entries: vec![p2_entry("researcher", "research", 3, None)],
    };
    let on = p2_rig(Some(roster()), p2_limits());
    let off = p2_rig(None, p2_limits());

    let on_schema = spawn_schema_of(&on.tools);
    let off_schema = spawn_schema_of(&off.tools);

    // required stays ["task"] on both sides — all three are optional.
    assert_eq!(on_schema["required"], json!(["task"]));
    assert_eq!(off_schema["required"], json!(["task"]));

    // Exactly three properties added, each a plain shape (no enum).
    let keys = |schema: &Value| -> Vec<String> {
        let mut keys: Vec<String> = schema["properties"]
            .as_object()
            .expect("properties object")
            .keys()
            .cloned()
            .collect();
        keys.sort();
        keys
    };
    let mut expected = keys(&off_schema);
    for key in ["agent", "task_key", "output_schema"] {
        expected.push(key.to_string());
    }
    expected.sort();
    assert_eq!(keys(&on_schema), expected, "exactly the three additions");
    assert_eq!(on_schema["properties"]["agent"]["type"], json!("string"));
    assert_eq!(on_schema["properties"]["task_key"]["type"], json!("string"));
    assert_eq!(
        on_schema["properties"]["output_schema"]["type"],
        json!("object")
    );
    // No roster id leaks into the schema (M9).
    let serialized = on_schema.to_string();
    assert!(!serialized.contains("researcher"), "no ids in the schema");

    // The prompt half: one frozen line, appended once, only when on.
    let mut flagged = RouterProfile::new(p2_router(), router_template("mock", "mock-router"));
    flagged.roster_instruction = true;
    let rendered = flagged.judge.render(0, flagged.im.max_words);
    let expected_prompt = format!("{rendered}\n\n{ROSTER_INSTRUCTION_PROMPT_LINE}");
    let plain = RouterProfile::new(p2_router(), router_template("mock", "mock-router"));
    assert_eq!(
        plain.judge.render(0, plain.im.max_words),
        rendered,
        "flag off renders today's bytes"
    );
    assert_eq!(
        expected_prompt
            .matches(ROSTER_INSTRUCTION_PROMPT_LINE)
            .count(),
        1,
        "rendered exactly once"
    );
    assert!(expected_prompt.ends_with(ROSTER_INSTRUCTION_PROMPT_LINE));
    // The line carries no template placeholder and no roster data.
    assert!(!ROSTER_INSTRUCTION_PROMPT_LINE.contains('{'));
    assert!(!ROSTER_INSTRUCTION_PROMPT_LINE.contains("researcher"));
}

/// 🔴 The round's class-D gate (pin P2-6 / anchors rule 4): a
/// multiagent-only config (no roster) keeps the P1 bytes — the schema,
/// the rendered prompt, the wrapper, the `thread/started` payload and
/// the tool result all equal the P1 goldens, constructed here from the
/// landed builders and the pin-P1/P2 derivation.
#[tokio::test]
async fn no_roster_keeps_the_p1_schema_prompt_and_spawn_bytes_identical() {
    let router = p2_router();
    let rig = p2_rig(None, p2_limits());

    // Schema: the pre-P2 property set (no agent/task_key/output_schema).
    let schema = spawn_schema_of(&rig.tools);
    for key in ["agent", "task_key", "output_schema"] {
        assert!(schema["properties"].get(key).is_none(), "no `{key}`");
    }
    // Prompt: the default profile renders exactly the judge template.
    assert_eq!(
        rig.profile.judge.render(0, rig.profile.im.max_words),
        kian_superagent::JUDGE_PROMPT_TEMPLATE
            .replace("{inputs}", "0")
            .replace("{budget}", &rig.profile.im.max_words.to_string())
    );

    // Spawn bytes against the P1 golden.
    let result = rig
        .tools
        .invoke(p2_call("call-1", json!({"task": "the work"})))
        .await
        .expect("spawn dispatches");
    let identity = SpawnIdentity::derive(router, 5, "kian-worker", 7);
    let thread = kian_core::model::Thread {
        id: identity.thread_id,
        parent_session: router,
        pinned_version: 7,
    };
    let worker_session = child_session_id(identity.thread_id);
    let mut golden_wrapper = delegation_content(identity.thread_id, None, json!("the work"));
    golden_wrapper["spawn_uid"] = json!(identity.spawn_uid.to_string());
    golden_wrapper["task_id"] = json!(identity.task_id_str());
    golden_wrapper["agent_def"] = json!("kian-worker");
    golden_wrapper["agent_def_version"] = json!(7);
    let golden_message = json!({"role": "user", "content": golden_wrapper});

    // The wrapper (the admitted row) is the P1 golden.
    let admits = rig.inbox.admits.lock().unwrap().clone();
    assert_eq!(admits.len(), 1);
    assert_eq!(admits[0].1, golden_message["content"]);
    assert_eq!(admits[0].0, worker_session);

    // thread/started is the P1 payload over the derived child.
    let emitted = rig.port.emitted.lock().unwrap().clone();
    assert_eq!(emitted.len(), 1);
    assert_eq!(emitted[0].0, EventKind::ThreadStarted);
    // (R17-C6, pin P4-2) re-pointed at WRAPPER equality: this rig has
    // `board.gating` off (the pin-P4-1 byte control), so the whole
    // payload still equals the P3 golden — but the contract the two
    // sides must keep once the class-A keys land is that the WRAPPER
    // (`task`) is byte-identical between them, not the whole payload.
    assert_eq!(
        emitted[0].1["task"], golden_message,
        "the wrapper is byte-identical (pin P4-2)"
    );
    assert_eq!(
        emitted[0].1,
        thread_started_payload(&thread, worker_session, &golden_message)
    );

    // The ack is the P1 shape: no roster key anywhere.
    assert_eq!(result.output["status"], json!("dispatched"));
    assert_eq!(
        result.output["spawn_uid"],
        json!(identity.spawn_uid.to_string())
    );
    assert_eq!(result.output["task_id"], json!(identity.task_id_str()));
    assert_eq!(result.output["spawn_outcome"], json!("dispatched"));
    assert_eq!(
        result.output["thread_id"],
        json!(identity.thread_id.to_string())
    );
    for key in ["assignee", "output_schema", "agent"] {
        assert!(result.output.get(key).is_none(), "ack carries no `{key}`");
    }

    // And the pin P2-6 refusal clause: passing the roster arguments
    // while the roster is absent is refused naming the block — with
    // zero side effects.
    let rig2 = p2_rig(None, p2_limits());
    let err = rig2
        .tools
        .invoke(p2_call(
            "call-1",
            json!({"task": "the work", "agent": "researcher"}),
        ))
        .await
        .expect_err("refused");
    assert!(err.to_string().contains("superagent.roster"), "{}", err);
    assert!(rig2.port.emitted.lock().unwrap().is_empty());
    assert!(rig2.inbox.admits.lock().unwrap().is_empty());
}

/// The frozen instruction line is one constant rendered once —
/// byte-equal for two rosters of different sizes (no ids, no counts).
#[tokio::test]
async fn the_frozen_instruction_line_is_one_constant_rendered_once() {
    let one = Roster {
        entries: vec![p2_entry("researcher", "research", 3, None)],
    };
    let three = Roster {
        entries: vec![
            p2_entry("researcher", "research", 3, None),
            p2_entry("coder", "code", 4, None),
            p2_entry("writer", "prose", 2, None),
        ],
    };
    let mut payloads = Vec::new();
    for roster in [one, three] {
        let inbox = CaptureInbox::shared();
        let mut profile = RouterProfile::new(p2_router(), router_template("mock", "mock-router"));
        profile.roster_instruction = !roster.is_empty();
        profile.bootstrap(&*inbox).await.expect("bootstrap");
        let admits = inbox.admits.lock().unwrap().clone();
        assert_eq!(admits.len(), 1);
        payloads.push(admits[0].1.clone());
    }
    assert_eq!(payloads[0], payloads[1], "roster size changes no byte");
    let payload = payloads[0].as_str().expect("prompt payload");
    assert!(payload.ends_with(ROSTER_INSTRUCTION_PROMPT_LINE));
    assert_eq!(
        payload.matches(ROSTER_INSTRUCTION_PROMPT_LINE).count(),
        1,
        "rendered exactly once"
    );
}

/// §2.1.1:264-269 / pin P2-1: a spawn with `agent` records the
/// resolution — the wrapper carries the entry id as `assignee` and the
/// RESOLVED entry's AgentDef as `agent_def`/`agent_def_version` (not
/// the worker template). The ack stays the landed shape.
#[tokio::test]
async fn a_spawn_with_agent_records_assignee_agent_def_and_agent_def_version() {
    let router = p2_router();
    let rig = p2_rig(
        Some(Roster {
            entries: vec![
                p2_entry("researcher", "research", 3, None),
                p2_entry("coder", "code", 4, None),
            ],
        }),
        p2_limits(),
    );
    let result = rig
        .tools
        .invoke(p2_call(
            "call-1",
            json!({"task": "the work", "agent": "researcher"}),
        ))
        .await
        .expect("dispatches");
    assert_eq!(result.output["status"], json!("dispatched"));

    let admits = rig.inbox.admits.lock().unwrap().clone();
    assert_eq!(admits.len(), 1);
    let wrapper = &admits[0].1;
    assert_eq!(wrapper["assignee"], json!("researcher"));
    assert_eq!(wrapper["agent_def"], json!("research"));
    assert_eq!(wrapper["agent_def_version"], json!(3));
    // The thread id is the derivation, not the task id (no task_key).
    let identity = SpawnIdentity::derive_delegated(
        router,
        5,
        "research",
        3,
        None,
        Some("researcher".to_string()),
    );
    assert_eq!(wrapper["spawn_uid"], json!(identity.spawn_uid.to_string()));
    assert_eq!(wrapper["thread_id"], json!(identity.thread_id.to_string()));
    assert_eq!(wrapper["task_id"], json!(identity.task_id_str()));
    // The ack is unchanged: no assignee key on the result.
    for key in ["assignee", "output_schema"] {
        assert!(result.output.get(key).is_none(), "ack carries no `{key}`");
    }
    // The identity round-trips off the wrapper bytes alone (§2.1.1).
    assert_eq!(spawn_identity_of(wrapper), Some(identity));
}

/// 🔴 Pin P2-8: two spawns sharing a `task_key` are attempts of ONE
/// task (the same `task_id`) but DISTINCT children (distinct
/// `spawn_uid`, distinct thread ids, distinct worker sessions) — the
/// duplicate-child bug `prepare` would have if the thread id were
/// taken from `task_id` (`worker.rs:792` before the pin).
#[tokio::test]
async fn two_spawns_sharing_a_task_key_get_distinct_thread_ids_and_one_task_id() {
    let router = p2_router();
    let rig = p2_rig_with_events(
        Some(Roster {
            entries: vec![p2_entry("researcher", "research", 3, None)],
        }),
        p2_limits(),
        vec![
            p2_tool_call(router, 5, "call-1"),
            p2_tool_call(router, 7, "call-2"),
        ],
    );
    let args = json!({"task": "the work", "agent": "researcher", "task_key": "ingest-feed"});
    let first = rig
        .tools
        .invoke(p2_call("call-1", args.clone()))
        .await
        .expect("first dispatches");
    let second = rig
        .tools
        .invoke(p2_call("call-2", args))
        .await
        .expect("second dispatches");

    // ONE task id — exactly uuid5(parent, "kian:task:<key>").
    let expected_task = task_id_for_key(router, "ingest-feed");
    assert_eq!(first.output["task_id"], json!(expected_task.to_string()));
    assert_eq!(second.output["task_id"], json!(expected_task.to_string()));
    assert_eq!(first.output["task_id"], second.output["task_id"]);

    // DISTINCT children: thread ids, spawn uids and sessions differ.
    assert_ne!(first.output["thread_id"], second.output["thread_id"]);
    assert_ne!(first.output["spawn_uid"], second.output["spawn_uid"]);
    assert_ne!(first.output["worker"], second.output["worker"]);

    // The wrappers agree: same task_id, different thread_id — and the
    // reader recovers both facts from the bytes alone.
    let admits = rig.inbox.admits.lock().unwrap().clone();
    assert_eq!(admits.len(), 2);
    let idents: Vec<_> = admits
        .iter()
        .map(|(_, wrapper, _)| spawn_identity_of(wrapper).expect("identity"))
        .collect();
    assert_eq!(idents[0].task_id, expected_task);
    assert_eq!(idents[1].task_id, expected_task);
    assert_ne!(idents[0].thread_id, idents[1].thread_id);
    assert_ne!(idents[0].spawn_uid, idents[1].spawn_uid);
    for identity in &idents {
        assert_eq!(identity.assignee.as_deref(), Some("researcher"));
    }
}

/// 🔴 §5.4 / §8.3:862: an unknown `agent` is refused BEFORE any side
/// effect — no `thread/started`, no inbox row, no thread id minted —
/// and the error names the known ids.
#[tokio::test]
async fn an_unknown_agent_is_refused_before_any_side_effect() {
    let rig = p2_rig(
        Some(Roster {
            entries: vec![
                p2_entry("researcher", "research", 3, None),
                p2_entry("coder", "code", 4, None),
            ],
        }),
        p2_limits(),
    );
    let err = rig
        .tools
        .invoke(p2_call(
            "call-1",
            json!({"task": "the work", "agent": "ghost"}),
        ))
        .await
        .expect_err("unknown agent refuses");
    let text = err.to_string();
    assert!(text.contains("unknown roster id `ghost`"), "{text}");
    assert!(
        text.contains("researcher") && text.contains("coder"),
        "{text}"
    );
    assert!(rig.port.emitted.lock().unwrap().is_empty(), "no lineage");
    assert!(rig.inbox.admits.lock().unwrap().is_empty(), "no row");
}

/// 🔴 §5.4:604-607: `output_schema` shape validation precedes roster
/// resolution — a call with a malformed schema AND an unknown agent
/// reports the schema error (naming the keyword), never the agent.
#[tokio::test]
async fn output_schema_shape_validation_precedes_roster_resolution() {
    let rig = p2_rig(
        Some(Roster {
            entries: vec![p2_entry("researcher", "research", 3, None)],
        }),
        p2_limits(),
    );
    let err = rig
        .tools
        .invoke(p2_call(
            "call-1",
            json!({
                "task": "the work",
                "agent": "ghost",
                "output_schema": {"oneOf": [{"type": "string"}]},
            }),
        ))
        .await
        .expect_err("malformed schema refuses");
    let text = err.to_string();
    assert!(text.contains("oneOf"), "{text}");
    assert!(text.contains("outside the supported subset"), "{text}");
    assert!(!text.contains("unknown roster id"), "{text}");
    assert!(rig.port.emitted.lock().unwrap().is_empty());
    assert!(rig.inbox.admits.lock().unwrap().is_empty());
}

/// 🔴 §5.4:604-607 (the other order fact): roster resolution precedes
/// `max_children` — an unknown agent with a FULL cap (max_children 0)
/// reports the roster error, so the cap state is never disclosed
/// through it.
#[tokio::test]
async fn roster_resolution_precedes_max_children_so_the_cap_state_is_never_disclosed() {
    let zero_cap = MultiagentLimits {
        max_children: 0,
        max_depth: 1,
        child_timeout_ms: 900_000,
    };
    let rig = p2_rig(
        Some(Roster {
            entries: vec![p2_entry("researcher", "research", 3, None)],
        }),
        zero_cap,
    );
    let err = rig
        .tools
        .invoke(p2_call(
            "call-1",
            json!({"task": "the work", "agent": "ghost"}),
        ))
        .await
        .expect_err("refused");
    let text = err.to_string();
    assert!(text.contains("unknown roster id `ghost`"), "{text}");
    assert!(!text.contains("max_children"), "{text}");
    // The control: a KNOWN agent at the same full cap names the cap.
    let known = rig
        .tools
        .invoke(p2_call(
            "call-1",
            json!({"task": "the work", "agent": "researcher"}),
        ))
        .await
        .expect_err("cap refuses");
    assert!(known.to_string().contains("max_children"), "{}", known);
}

/// §3.2.1:438-441: the declared `output_schema` rides the wrapper
/// verbatim (the class-B copy — the child is "told about" it), the
/// resolved entry's default applies when the call declared none, and
/// when neither declared one the key is ABSENT — never `null`.
#[tokio::test]
async fn the_declared_output_schema_rides_the_wrapper_and_is_absent_when_undeclared() {
    let call_schema = json!({
        "type": "object",
        "required": ["summary"],
        "properties": {"summary": {"type": "string"}},
    });
    let entry_schema = json!({"type": "string"});

    // Declared on the call: the call's bytes ride verbatim.
    let rig = p2_rig(
        Some(Roster {
            entries: vec![p2_entry(
                "researcher",
                "research",
                3,
                Some(entry_schema.clone()),
            )],
        }),
        p2_limits(),
    );
    rig.tools
        .invoke(p2_call(
            "call-1",
            json!({"task": "the work", "agent": "researcher", "output_schema": call_schema.clone()}),
        ))
        .await
        .expect("dispatches");
    let wrapper = rig.inbox.admits.lock().unwrap()[0].1.clone();
    assert_eq!(
        wrapper["output_schema"], call_schema,
        "the call's schema wins"
    );

    // Undeclared on the call, default on the entry: the default rides.
    let rig = p2_rig(
        Some(Roster {
            entries: vec![p2_entry(
                "researcher",
                "research",
                3,
                Some(entry_schema.clone()),
            )],
        }),
        p2_limits(),
    );
    rig.tools
        .invoke(p2_call(
            "call-1",
            json!({"task": "the work", "agent": "researcher"}),
        ))
        .await
        .expect("dispatches");
    let wrapper = rig.inbox.admits.lock().unwrap()[0].1.clone();
    assert_eq!(
        wrapper["output_schema"], entry_schema,
        "the entry default applies"
    );

    // Declared nowhere: absent, never null — and the serialized bytes
    // do not mention the key.
    let rig = p2_rig(
        Some(Roster {
            entries: vec![p2_entry("researcher", "research", 3, None)],
        }),
        p2_limits(),
    );
    rig.tools
        .invoke(p2_call(
            "call-1",
            json!({"task": "the work", "agent": "researcher"}),
        ))
        .await
        .expect("dispatches");
    let wrapper = rig.inbox.admits.lock().unwrap()[0].1.clone();
    assert!(wrapper.get("output_schema").is_none(), "absent, never null");
    assert!(!wrapper.to_string().contains("\"output_schema\""));
}
