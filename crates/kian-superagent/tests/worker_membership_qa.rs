//! (R12-07 / kian-133) Independent QA acceptance probes for the Router
//! membership surface — L1, super-agent half.
//!
//! QA-authored probes with QA-authored fixtures only. Re-derived from
//! brief §7 L1 rows and the locked spec §5–§6 (R1/R2/R4/R5/R6):
//!
//! - the `spawn_worker` schema: plain-string required `exec_worker`
//!   (no enum, no ids), byte-identical across membership
//!   configurations, absent + refused when membership is off;
//! - the judge prompt: the frozen membership block is the ONLY
//!   membership-driven prefix delta — identical bytes across
//!   configurations, zero-worker = the legacy render with no block;
//! - `list_workers`: static schema, id-ascending determinism, live
//!   bound-session projection, read-only (zero admits/wakes/events);
//! - the spawn validation matrix against the RUNTIME directory:
//!   unknown refused with sorted known ids; offline-known allowed with
//!   the state echo + queue note; a busy OFFLINE worker still refuses
//!   (I8: busy is live-session based, not online-based) naming owner
//!   and thread with zero side effects;
//! - the policy-version gate is NOT re-tested here (R11 machinery,
//!   unchanged by design R3).
//!
//! No network, no daemon. Fake names and uuids only.

use futures::future::BoxFuture;
use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::{Epoch, SessionId};
use kian_core::seams::{ToolCall, ToolError, ToolProvider as _};
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    AdmitReceipt, EXEC_WORKER_KEY, InboxWriteError, InboxWriter, LIST_WORKERS_TOOL, MemberState,
    MemberView, MockChannel, RouterHold, RouterProfile, RouterTools, SPAWN_WORKER_TOOL,
    WORKER_MEMBERSHIP_PROMPT_BLOCK, WorkerDirectoryError, WorkerDirectoryPort, WorkerSpawner,
    WorkerStreamsError, WorkerStreamsPort, busy_owner, router_template, thread_liveness,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// QA fixtures (nothing shared with any implementer test module)
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
    fn shared(session: SessionId) -> (Arc<Self>, Arc<dyn kian_superagent::StreamPort>) {
        let port = Arc::new(Self {
            session,
            events: Mutex::new(Vec::new()),
        });
        (
            Arc::clone(&port) as Arc<Self>,
            Arc::clone(&port) as Arc<dyn kian_superagent::StreamPort>,
        )
    }
}

impl kian_superagent::StreamPort for QaPort {
    fn session(&self) -> SessionId {
        self.session
    }
    fn emit(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> BoxFuture<'static, Result<(), kian_superagent::StreamPortError>> {
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
    fn read_stream(
        &self,
    ) -> BoxFuture<'static, Result<Vec<Event>, kian_superagent::StreamPortError>> {
        let events = self.events.lock().unwrap().clone();
        Box::pin(async move { Ok(events) })
    }
}

/// A worker-streams port fake: per-session canned result.
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

/// A QA directory-port fake: a canned member list + a read counter
/// (read-only observable from the port's side).
struct QaDirectory {
    members: Mutex<Vec<MemberView>>,
    reads: Mutex<u64>,
    fail: bool,
}

impl QaDirectory {
    fn with(members: Vec<MemberView>) -> Arc<Self> {
        Arc::new(Self {
            members: Mutex::new(members),
            reads: Mutex::new(0),
            fail: false,
        })
    }
    fn failing() -> Arc<Self> {
        Arc::new(Self {
            members: Mutex::new(Vec::new()),
            reads: Mutex::new(0),
            fail: true,
        })
    }
    fn set(&self, members: Vec<MemberView>) {
        *self.members.lock().unwrap() = members;
    }
}

impl WorkerDirectoryPort for QaDirectory {
    fn known_ids(&self) -> BoxFuture<'static, Result<Vec<String>, WorkerDirectoryError>> {
        *self.reads.lock().unwrap() += 1;
        let result = if self.fail {
            Err(WorkerDirectoryError("qa directory read failed".into()))
        } else {
            Ok(self
                .members
                .lock()
                .unwrap()
                .iter()
                .map(|m| m.id.clone())
                .collect())
        };
        Box::pin(async move { result })
    }

    fn members(&self) -> BoxFuture<'static, Result<Vec<MemberView>, WorkerDirectoryError>> {
        *self.reads.lock().unwrap() += 1;
        let result = if self.fail {
            Err(WorkerDirectoryError("qa directory read failed".into()))
        } else {
            Ok(self.members.lock().unwrap().clone())
        };
        Box::pin(async move { result })
    }

    fn member(
        &self,
        id: &str,
    ) -> BoxFuture<'static, Result<Option<MemberView>, WorkerDirectoryError>> {
        *self.reads.lock().unwrap() += 1;
        let result = if self.fail {
            Err(WorkerDirectoryError("qa directory read failed".into()))
        } else {
            Ok(self
                .members
                .lock()
                .unwrap()
                .iter()
                .find(|m| m.id == id)
                .cloned())
        };
        Box::pin(async move { result })
    }
}

fn view(id: &str, state: MemberState) -> MemberView {
    MemberView {
        id: id.into(),
        description: Some(format!("the {id} QA worker")),
        version: Some(1),
        capabilities: Vec::new(),
        declaration_stale: false,
        state,
        last_seen_ms: 10_000,
    }
}

/// The QA spawn harness: RouterTools over QA fakes, with the runtime
/// membership installed exactly as the assembly does.
fn qa_tools(
    static_ids: &[&str],
    directory: Option<Arc<dyn WorkerDirectoryPort>>,
    streams: Option<Arc<dyn WorkerStreamsPort>>,
) -> (Arc<RouterTools>, Arc<QaInbox>, Arc<QaPort>) {
    let router = sid(0x5151);
    let inbox = QaInbox::shared();
    let spawner = Arc::new(WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &kian_superagent::worker_template("mock", "qa-worker", 7),
        inbox.clone(),
    ));
    let mut channels = kian_superagent::ChannelRegistry::new();
    channels.register(
        "mock-im",
        Arc::new(MockChannel::new("mock-im")) as Arc<dyn kian_core::seams::ChannelAdapter>,
    );
    let hold = RouterHold::shared();
    let mut profile = RouterProfile::new(router, router_template("mock", "qa-router"));
    profile.worker_membership = !static_ids.is_empty() || directory.is_some();
    let timeouts: BTreeMap<String, u64> = static_ids
        .iter()
        .map(|id| (id.to_string(), 60_000u64))
        .collect();
    let mut tools = RouterTools::new(
        spawner,
        channels,
        &profile,
        hold.clone(),
        kian_superagent::ResidentBinding::new(profile.session),
    )
    .with_worker_membership(
        static_ids.iter().map(|s| s.to_string()).collect(),
        timeouts,
        directory,
    );
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

fn list_call() -> ToolCall {
    ToolCall {
        id: format!("qa-call-{}", Uuid::new_v4().simple()),
        name: LIST_WORKERS_TOOL.into(),
        arguments: json!({}),
    }
}

fn spawn_parameters(tools: &RouterTools) -> Value {
    tools
        .schemas()
        .into_iter()
        .find(|schema| schema.name == SPAWN_WORKER_TOOL)
        .expect("spawn_worker mounted")
        .parameters
}

fn side_effect_counts(inbox: &QaInbox, port: &QaPort) -> (usize, usize, usize) {
    let threads = port
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.kind == EventKind::ThreadStarted)
        .count();
    (
        inbox.admits.lock().unwrap().len(),
        inbox.wakes.lock().unwrap().len(),
        threads,
    )
}

// ---------------------------------------------------------------------------
// L1: the spawn_worker schema (R1) — byte stability across membership
// ---------------------------------------------------------------------------

#[tokio::test]
async fn qa_spawn_schema_plain_string_and_stable_across_membership() {
    // Two DIFFERENT membership configurations (different static ids,
    // different live directory contents): the schema bytes must be
    // identical (I6) — plain string, required, constant description.
    let directory = QaDirectory::with(vec![view("qa-dyn", MemberState::Online)]);
    let (tools_a, _, _) = qa_tools(&["qa-alpha", "qa-beta"], Some(directory), None);
    let schema_a = spawn_parameters(&tools_a);

    let directory_b = QaDirectory::with(vec![
        view("qa-zeta", MemberState::Offline),
        view("qa-dyn2", MemberState::Online),
    ]);
    let (tools_b, _, _) = qa_tools(&["qa-solo"], Some(directory_b), None);
    let schema_b = spawn_parameters(&tools_b);
    assert_eq!(
        serde_json::to_string(&schema_a).unwrap(),
        serde_json::to_string(&schema_b).unwrap(),
        "schema bytes identical across membership configurations"
    );

    // The R1 shape: plain string, required, no enum anywhere, a
    // constant description pointing at list_workers — no ids in it.
    let exec = &schema_a["properties"]["exec_worker"];
    assert_eq!(exec["type"], json!("string"), "plain string (no enum)");
    assert!(exec.get("enum").is_none(), "no enum key");
    assert!(schema_a["properties"]["task"].get("enum").is_none());
    let description = exec["description"].as_str().expect("description");
    assert!(description.contains("list_workers"), "{description}");
    assert!(!description.contains("qa-"), "no ids: {description}");
    assert_eq!(
        schema_a["required"],
        json!(["task", "exec_worker"]),
        "required even with one worker (deviation 2)"
    );

    // Membership OFF (no static ids, no directory): byte-identical
    // legacy — the property is absent and `required` is ["task"].
    let (tools_off, inbox, port) = qa_tools(&[], None, None);
    let schema_off = spawn_parameters(&tools_off);
    assert!(schema_off["properties"].get("exec_worker").is_none());
    assert_eq!(schema_off["required"], json!(["task"]));
    // …and supplying the argument is refused honestly (step 2).
    let err = tools_off
        .invoke(spawn_call(
            json!({"task": "qa work", "exec_worker": "qa-alpha"}),
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, ToolError::InvalidCall(_)), "{err}");
    assert!(
        err.to_string().contains("not configured on this server"),
        "{}",
        err
    );
    assert_eq!(
        side_effect_counts(&inbox, &port),
        (0, 0, 0),
        "zero side effects"
    );
}

// ---------------------------------------------------------------------------
// L1: the judge prompt (R2) — the block is the only membership delta
// ---------------------------------------------------------------------------

/// The bootstrap payload (the persisted judge prompt) is identical for
/// every membership configuration; zero-worker renders the legacy
/// bytes; membership-on is exactly legacy + the frozen block.
#[tokio::test]
async fn qa_judge_prompt_block_is_the_only_membership_delta() {
    let router = sid(0x7a7a);
    let inbox = QaInbox::shared();

    let mut off = RouterProfile::new(router, router_template("mock", "qa-router"));
    off.worker_membership = false;
    off.bootstrap(&*inbox).await.unwrap();
    let payload_off = inbox.admits.lock().unwrap().last().unwrap().1.clone();
    let text_off = payload_off.as_str().expect("prompt payload is a string");

    let mut on_a = RouterProfile::new(router, router_template("mock", "qa-router"));
    on_a.worker_membership = true;
    on_a.bootstrap(&*inbox).await.unwrap();
    let text_on_a = inbox
        .admits
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .1
        .as_str()
        .unwrap()
        .to_string();

    // Second on-render with a different (simulated) membership
    // configuration — static ids / joins / leaves are not inputs to the
    // render (they are not even passed), so the bytes must be equal.
    let mut on_b = RouterProfile::new(router, router_template("mock", "qa-router"));
    on_b.worker_membership = true;
    on_b.bootstrap(&*inbox).await.unwrap();
    let text_on_b = inbox
        .admits
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .1
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(text_on_a, text_on_b, "identical bytes for every on-config");

    // on = off + "\n\n" + the frozen block — no other delta.
    let suffix = format!("\n\n{WORKER_MEMBERSHIP_PROMPT_BLOCK}");
    let stripped = text_on_a
        .strip_suffix(&suffix)
        .expect("on-render ends with the frozen membership block");
    assert_eq!(stripped, text_off, "zero-worker render is the legacy bytes");

    // The block itself: no ids, no counts, no timestamps, no state.
    for id in ["qa-alpha", "qa-dyn", "worker-1", "builder"] {
        assert!(
            !WORKER_MEMBERSHIP_PROMPT_BLOCK.contains(id),
            "block names {id}"
        );
    }
    assert!(
        !text_off.contains("Execution workers:"),
        "off render has no block"
    );
}

// ---------------------------------------------------------------------------
// L1: list_workers (R4) — static schema, determinism, read-only
// ---------------------------------------------------------------------------

#[tokio::test]
async fn qa_list_workers_deterministic_sorted_and_read_only() {
    // The static pin appears through the port seam (the real port
    // merges registry + directory + static pins), so the fake carries
    // it as a member view too.
    let mut zeta = view("qa-zeta", MemberState::Offline);
    zeta.capabilities = vec!["apple-calendar".into(), "apple-health".into()];
    let directory = QaDirectory::with(vec![
        zeta,
        view("qa-alpha", MemberState::Online),
        view("qa-mid", MemberState::Online),
        view("qa-static", MemberState::Offline),
    ]);
    let (tools, inbox, port) = qa_tools(&["qa-static"], Some(directory), None);

    // Static schema bytes (no arguments).
    let schema = tools
        .schemas()
        .into_iter()
        .find(|s| s.name == LIST_WORKERS_TOOL)
        .expect("list_workers mounted when membership is on");
    assert_eq!(
        schema.parameters,
        json!({"type": "object", "properties": {}, "required": []}),
        "static no-argument schema"
    );

    // Result rows: id-ascending, state + description carried,
    // bound fields absent without a live bound session.
    let result = tools.invoke(list_call()).await.expect("list_workers ok");
    let workers = result.output["workers"].as_array().expect("workers array");
    let ids: Vec<&str> = workers.iter().map(|w| w["id"].as_str().unwrap()).collect();
    assert_eq!(
        ids,
        vec!["qa-alpha", "qa-mid", "qa-static", "qa-zeta"],
        "sorted"
    );
    assert_eq!(workers[0]["state"], json!("online"));
    assert_eq!(workers[3]["state"], json!("offline"));
    assert!(workers[0]["description"].is_string());
    // The config-owned capability catalog is a result-only field: it
    // appears when declared and is absent (not null) otherwise.
    assert_eq!(
        workers[3]["capabilities"],
        json!(["apple-calendar", "apple-health"])
    );
    assert!(workers[0].get("capabilities").is_none());
    for w in workers {
        assert!(w.get("bound_session").is_none() || w["bound_session"].is_null());
    }

    // Read-only: no admit, no wake, no event, not even a lineage one.
    assert_eq!(side_effect_counts(&inbox, &port), (0, 0, 0));
    // Two calls, same order both times (determinism).
    let again = tools.invoke(list_call()).await.expect("second call");
    assert_eq!(again.output, result.output);
    assert_eq!(side_effect_counts(&inbox, &port), (0, 0, 0));

    // Membership off: the tool is absent and its invoke is refused.
    let (tools_off, _, _) = qa_tools(&[], None, None);
    assert!(
        !tools_off
            .schemas()
            .into_iter()
            .any(|s| s.name == LIST_WORKERS_TOOL),
        "absent when membership is off"
    );
    let err = tools_off.invoke(list_call()).await.unwrap_err();
    assert!(matches!(err, ToolError::InvalidCall(_)), "{err}");
    assert!(err.to_string().contains("unknown router tool"), "{}", err);
}

/// The bound-session projection: a live bound worker session renders
/// `bound_session`/`bound_thread`; a dead one does not.
#[tokio::test]
async fn qa_list_workers_bound_fields_follow_live_sessions() {
    let streams = QaStreams::shared();
    let directory = QaDirectory::with(vec![view("qa-live", MemberState::Online)]);
    let (tools, _inbox, port) = qa_tools(&[], Some(directory), Some(streams.clone()));

    // Occupy qa-live with a real spawn, then make its worker stream
    // LIVE (created, thread started, no end event).
    let _first = tools
        .invoke(spawn_call(
            json!({"task": "qa occupy", "exec_worker": "qa-live"}),
        ))
        .await
        .expect("first spawn");
    let (worker_uuid, thread_uuid) = {
        let lineage = port.events.lock().unwrap();
        let started = lineage
            .iter()
            .find(|e| e.kind == EventKind::ThreadStarted)
            .expect("thread/started lineage");
        let worker_uuid = started.bytes["worker"]
            .as_str()
            .expect("worker uuid")
            .to_string();
        let thread_uuid = started
            .bytes
            .get("thread_id")
            .and_then(|v| v.as_str())
            .or_else(|| started.bytes["thread"].as_str())
            .expect("thread id")
            .to_string();
        assert_eq!(started.bytes[EXEC_WORKER_KEY], json!("qa-live"));
        (worker_uuid, thread_uuid)
    };
    let worker_sid = SessionId(Uuid::parse_str(&worker_uuid).unwrap());
    streams.set(
        worker_sid,
        Ok(vec![
            ev(worker_sid, 0, EventKind::SessionCreated, Value::Null),
            ev(
                worker_sid,
                1,
                EventKind::ThreadStarted,
                json!({"thread_id": thread_uuid, "worker": worker_uuid, EXEC_WORKER_KEY: "qa-live"}),
            ),
            ev(
                worker_sid,
                2,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "qa occupy"}),
            ),
        ]),
    );

    let result = tools.invoke(list_call()).await.expect("list_workers");
    let row = &result.output["workers"][0];
    assert_eq!(row["id"], json!("qa-live"));
    let bound_session = row["bound_session"].as_str().expect("bound session set");
    let bound_thread = row["bound_thread"].as_str().expect("bound thread set");
    assert_eq!(bound_session, worker_uuid);
    assert_eq!(bound_thread, thread_uuid);

    // The session ends ⇒ the bound fields disappear (live-session
    // based, I8), while the member itself stays online.
    streams.set(
        worker_sid,
        Ok(vec![
            ev(worker_sid, 0, EventKind::SessionCreated, Value::Null),
            ev(
                worker_sid,
                1,
                EventKind::ThreadStarted,
                json!({"thread_id": thread_uuid, "worker": worker_uuid, EXEC_WORKER_KEY: "qa-live"}),
            ),
            ev(
                worker_sid,
                2,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "qa occupy"}),
            ),
            ev(
                worker_sid,
                3,
                EventKind::SessionEnded { reason: EndReason::Completed },
                Value::Null,
            ),
        ]),
    );
    let result = tools.invoke(list_call()).await.expect("list_workers again");
    let row = &result.output["workers"][0];
    assert!(row.get("bound_session").is_none() || row["bound_session"].is_null());
    assert_eq!(row["state"], json!("online"), "membership stays online");
}

// ---------------------------------------------------------------------------
// L1: the spawn validation matrix against the runtime directory (R5/R6)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn qa_spawn_matrix_runtime_directory_offline_known_allowed() {
    let directory = QaDirectory::with(vec![
        view("qa-dyn-on", MemberState::Online),
        view("qa-dyn-off", MemberState::Offline),
    ]);
    let (tools, inbox, port) = qa_tools(&["qa-static"], Some(directory), None);

    // Unknown id: honest refusal naming the sorted known ids.
    let err = tools
        .invoke(spawn_call(
            json!({"task": "qa work", "exec_worker": "qa-ghost"}),
        ))
        .await
        .unwrap_err();
    assert!(matches!(err, ToolError::InvalidCall(_)), "{err}");
    let text = err.to_string();
    assert!(text.contains("unknown worker `qa-ghost`"), "{text}");
    assert!(
        text.contains("qa-dyn-off, qa-dyn-on, qa-static"),
        "sorted known ids: {text}"
    );

    // Missing exec_worker: required (R1), points at list_workers.
    let err = tools
        .invoke(spawn_call(json!({"task": "qa work"})))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("requires `exec_worker`"),
        "{}",
        err
    );

    // Offline-known: ALLOWED; the result echoes the observed state and
    // the honest queue note (step 5) — no other refusal intervenes.
    let ok = tools
        .invoke(spawn_call(
            json!({"task": "qa parked", "exec_worker": "qa-dyn-off"}),
        ))
        .await
        .expect("offline-known spawn is allowed");
    assert_eq!(ok.output[EXEC_WORKER_KEY], json!("qa-dyn-off"));
    assert_eq!(ok.output["worker_state"], json!("offline"), "{}", ok.output);
    assert_eq!(ok.output["note"], json!("offline — commands will queue"));

    // Online member: state echoed, no note.
    let ok = tools
        .invoke(spawn_call(
            json!({"task": "qa live", "exec_worker": "qa-dyn-on"}),
        ))
        .await
        .expect("online spawn");
    assert_eq!(ok.output["worker_state"], json!("online"));
    assert!(ok.output.get("note").is_none());

    // Every refusal above left zero side effects (two successful spawns
    // aside — count only the deltas after them).
    let (admits, wakes, _) = side_effect_counts(&inbox, &port);
    assert_eq!(admits, 2, "exactly the two accepted spawns admitted");
    assert_eq!(wakes, 2);

    // Directory read failure: the spawn fails CLOSED (never a guess).
    let failing = QaDirectory::failing();
    let (tools, inbox, port) = qa_tools(&["qa-static"], Some(failing), None);
    let err = tools
        .invoke(spawn_call(
            json!({"task": "qa work", "exec_worker": "qa-static"}),
        ))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("refused"), "{}", err);
    assert_eq!(side_effect_counts(&inbox, &port), (0, 0, 0));
}

/// I8: busy is live-session based, NOT online-based — a worker whose
/// directory state is offline but whose bound session is live still
/// refuses the second spawn, naming owner and thread, with zero side
/// effects. (This is the red-witness anchor for the offline-releases-
/// the-binding mutation.)
#[tokio::test]
async fn qa_busy_offline_worker_still_refuses() {
    let streams = QaStreams::shared();
    let directory = QaDirectory::with(vec![view("qa-busy", MemberState::Offline)]);
    let (tools, inbox, port) = qa_tools(&[], Some(directory), Some(streams.clone()));

    // First spawn occupies qa-busy (allowed: offline-known).
    let first = tools
        .invoke(spawn_call(
            json!({"task": "qa occupy", "exec_worker": "qa-busy"}),
        ))
        .await
        .expect("offline-known spawn succeeds");
    assert_eq!(first.output["worker_state"], json!("offline"));
    let owner_session = first.output["worker"].as_str().unwrap().to_string();
    let owner_thread = first.output["thread_id"].as_str().unwrap().to_string();

    // The bound worker session is LIVE (created + started, no end).
    let worker_sid = SessionId(Uuid::parse_str(&owner_session).unwrap());
    streams.set(
        worker_sid,
        Ok(vec![
            ev(worker_sid, 0, EventKind::SessionCreated, Value::Null),
            ev(
                worker_sid,
                1,
                EventKind::ThreadStarted,
                json!({"thread_id": owner_thread, "worker": owner_session, EXEC_WORKER_KEY: "qa-busy"}),
            ),
            ev(
                worker_sid,
                2,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "qa occupy"}),
            ),
        ]),
    );

    let (before_admits, before_wakes, before_lineage) = side_effect_counts(&inbox, &port);

    // Second spawn naming the busy OFFLINE worker: refused.
    let err = tools
        .invoke(spawn_call(
            json!({"task": "qa second", "exec_worker": "qa-busy"}),
        ))
        .await
        .unwrap_err();
    let text = err.to_string();
    assert!(text.contains("is busy"), "names the busy state: {text}");
    assert!(
        text.contains(&owner_session),
        "names the owner session: {text}"
    );
    assert!(
        text.contains(&owner_thread),
        "names the owner thread: {text}"
    );

    let (admits, wakes, lineage) = side_effect_counts(&inbox, &port);
    assert_eq!(admits, before_admits, "no row on refusal");
    assert_eq!(wakes, before_wakes, "no wake on refusal");
    assert_eq!(lineage, before_lineage, "no thread/started on refusal");

    // Cross-check the pure seam: busy_owner still finds the owner for
    // this id even though the directory says offline (liveness is
    // stream-based).
    let port_events = port.events.lock().unwrap().clone();
    let owner = busy_owner(&port_events, &*streams, "qa-busy", 20_000, 60_000)
        .await
        .expect("stream read ok")
        .expect("busy_owner sees the live bound session");
    assert_eq!(owner.worker_session.0.to_string(), owner_session);
    assert!(matches!(
        thread_liveness(&streams.read_stream(worker_sid).await.unwrap()),
        kian_superagent::ThreadLiveness::Live
    ));
}

// ---------------------------------------------------------------------------
// L1: the membership projection is disposable — a directory that
// changes under a live spawn flow never rewrites a binding (I1 spot)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn qa_membership_change_never_rewrites_a_binding() {
    let streams = QaStreams::shared();
    let directory = QaDirectory::with(vec![view("qa-bound", MemberState::Online)]);
    let (tools, _inbox, port) = qa_tools(&[], Some(directory.clone()), Some(streams.clone()));

    let first = tools
        .invoke(spawn_call(
            json!({"task": "qa bind", "exec_worker": "qa-bound"}),
        ))
        .await
        .expect("spawn");
    let lineage_before: Vec<Event> = port
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.kind == EventKind::ThreadStarted)
        .cloned()
        .collect();
    assert_eq!(lineage_before.len(), 1);
    let bytes_before = lineage_before[0].bytes.clone();

    // Membership churn: the worker goes offline, another joins.
    directory.set(vec![
        view("qa-bound", MemberState::Offline),
        view("qa-new", MemberState::Online),
    ]);

    // The committed lineage bytes are untouched (historical context is
    // immutable — the binding never rewrites).
    let lineage_after: Vec<Event> = port
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.kind == EventKind::ThreadStarted)
        .cloned()
        .collect();
    assert_eq!(lineage_after.len(), 1, "no new lineage from churn");
    assert_eq!(lineage_after[0].bytes, bytes_before);
    let _ = first;
}
