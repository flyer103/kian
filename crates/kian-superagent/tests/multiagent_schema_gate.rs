//! (R17-C3, pin P2-4) Acceptance probes for the child-side
//! `output_schema` gate: the honest silence on a pass, the exactly-one
//! correction on miss #0, the committed verdict + raw-answer
//! preservation on miss #1, the empty-answer precedence, and the
//! feature-off byte identity (M8). Every fixture is authored here: a
//! scripted inner [`LoopDriver`] that appends its scripted assistant
//! message to the shared stream each drive, an in-memory
//! [`StreamPort`], and a recording inbox — no network, no daemon, no
//! store.

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use kian_core::events::{Event, EventKind};
use kian_core::model::{Epoch, SessionId, Thread};
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_loop::EvidenceGateConfig;
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    AdmitReceipt, InboxWriteError, InboxWriter, MultiagentLimits, PortFor, SCHEMA_GATE_NAME,
    SpawnIdentity, SpawnRequest, StreamPort, StreamPortError, WorkerLoopConfig, WorkerLoopDriver,
    WorkerSpawner, derive_outcome, schema_correction_content, schema_correction_key,
    thread_started_payload, worker_template,
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

fn sid(n: u128) -> SessionId {
    SessionId(Uuid::from_u128(n))
}

const AMBIENT_ROUTER: u128 = 0x999;

fn limits() -> MultiagentLimits {
    MultiagentLimits {
        max_children: 4,
        max_depth: 1,
        child_timeout_ms: 900_000,
    }
}

/// The declared subset schema every wrapper-carrying leg uses: an
/// object with a required string `summary`.
fn summary_schema() -> Value {
    json!({
        "type": "object",
        "required": ["summary"],
        "properties": {"summary": {"type": "string"}},
    })
}

// ---------------------------------------------------------------------------
// Fixtures: the child stream, the port, the scripted inner
// ---------------------------------------------------------------------------

/// One seeded roster child: the wrapper bytes come from the real
/// `WorkerSpawner::prepare` with an identity carrying `assignee` and
/// a declared `output_schema` (exactly the bytes C2's spawn path
/// writes), so the gate reads what production reads.
struct Child {
    parent: SessionId,
    session: SessionId,
    thread_id: Uuid,
    identity: SpawnIdentity,
    task_message: Value,
}

fn make_child(with_schema: bool) -> Child {
    let router = sid(AMBIENT_ROUTER);
    let parent = sid(0x200);
    let identity = SpawnIdentity::derive_delegated(
        parent,
        1,
        "research",
        5,
        Some("task-a"),
        Some("researcher".into()),
    );
    let spawner = WorkerSpawner::new(
        router,
        &worker_template("mock", "mock-worker", 7),
        noop_inbox(),
    );
    let spawned = spawner.prepare(&SpawnRequest {
        task: json!("summarize the findings"),
        identity: Some(identity.clone()),
        output_schema: with_schema.then(summary_schema),
        ..SpawnRequest::default()
    });
    Child {
        parent,
        session: spawned.worker_session,
        thread_id: spawned.thread.id,
        identity,
        task_message: spawned.task_message,
    }
}

fn ev(seq: u64, session: SessionId, kind: EventKind, bytes: Value) -> Event {
    Event {
        seq,
        session,
        epoch: Epoch(1),
        ts: 1_700_000_000_000 + seq,
        kind,
        bytes,
        version: 0,
    }
}

fn child_events(child: &Child) -> Vec<Event> {
    let started = thread_started_payload(
        &Thread {
            id: child.thread_id,
            parent_session: child.parent,
            pinned_version: 7,
        },
        child.session,
        &child.task_message,
    );
    vec![
        ev(1, child.session, EventKind::SessionCreated, json!({})),
        ev(
            2,
            child.session,
            EventKind::InputAdmitted,
            json!({"content": child.task_message["content"].clone()}),
        ),
        ev(3, child.session, EventKind::ThreadStarted, started),
    ]
}

/// The scripted inner loop: on each drive it appends its next
/// scripted assistant text to the shared stream (exactly where the
/// real loop writes it) and yields the terminal outcome.
struct ScriptedAnswers {
    session: SessionId,
    events: Arc<Mutex<Vec<Event>>>,
    answers: Mutex<Vec<&'static str>>,
}

impl LoopDriver for ScriptedAnswers {
    type Ctx = ();

    fn drive(&self, _ctx: ()) -> BoxStream<'static, LoopOutcome> {
        let text = self
            .answers
            .lock()
            .unwrap()
            .pop()
            .unwrap_or("no more scripted answers");
        {
            let mut events = self.events.lock().unwrap();
            let seq = events.last().map(|e| e.seq + 1).unwrap_or(1);
            events.push(ev(
                seq,
                self.session,
                EventKind::AssistantMessage,
                json!({"role": "assistant", "content": {"text": text}}),
            ));
        }
        futures::stream::once(async move { LoopOutcome::Settled }).boxed()
    }
}

struct CaptureInbox {
    admits: Mutex<Vec<(SessionId, Value, Option<String>)>>,
}

impl CaptureInbox {
    fn shared() -> Arc<Self> {
        Arc::new(Self {
            admits: Mutex::new(Vec::new()),
        })
    }

    fn admits(&self) -> Vec<(SessionId, Value, Option<String>)> {
        self.admits.lock().unwrap().clone()
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

    fn wake(&self, _session: SessionId) -> WakeOutcome {
        WakeOutcome::Folded
    }
}

struct NoopInbox;

impl InboxWriter for NoopInbox {
    fn admit(
        &self,
        _session: SessionId,
        _payload: Value,
        _idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
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

fn noop_inbox() -> Arc<dyn InboxWriter> {
    Arc::new(NoopInbox)
}

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
        let seq = events.last().map(|e| e.seq + 1).unwrap_or(1);
        events.push(Event {
            seq,
            session: self.session,
            epoch: Epoch(1),
            ts: 1_700_000_100_000,
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

/// One assembled child under test. `answers` is reversed into the
/// scripted queue (first element = the first drive's answer).
struct Rig {
    driver: WorkerLoopDriver<ScriptedAnswers>,
    inbox: Arc<CaptureInbox>,
    events: Arc<Mutex<Vec<Event>>>,
}

fn assemble(child: &Child, events: Vec<Event>, answers: &[&'static str]) -> Rig {
    let inbox = CaptureInbox::shared();
    let state = Arc::new(Mutex::new(events));
    let port_session = child.session;
    let port_state = Arc::clone(&state);
    let port_for: PortFor<()> = Arc::new(move |_ctx: &()| {
        Arc::new(FakePort {
            session: port_session,
            events: Arc::clone(&port_state),
        }) as Arc<dyn StreamPort>
    });
    let inner = Arc::new(ScriptedAnswers {
        session: child.session,
        events: Arc::clone(&state),
        answers: Mutex::new(answers.iter().rev().cloned().collect()),
    });
    let config = WorkerLoopConfig {
        router: sid(AMBIENT_ROUTER),
        worker_version: 7,
        gate: EvidenceGateConfig::default(),
        multiagent: Some(limits()),
    };
    let driver = WorkerLoopDriver::new(
        inner,
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        config,
        port_for,
    );
    Rig {
        driver,
        inbox,
        events: state,
    }
}

async fn drive(rig: &Rig) -> Vec<LoopOutcome> {
    rig.driver.drive(()).collect().await
}

fn committed(rig: &Rig) -> Vec<Event> {
    rig.events.lock().unwrap().clone()
}

fn schema_verdicts(events: &[Event]) -> Vec<&Event> {
    events
        .iter()
        .filter(|ev| {
            ev.kind == EventKind::EvidenceVerdict
                && ev.bytes.get("gate").and_then(Value::as_str) == Some(SCHEMA_GATE_NAME)
        })
        .collect()
}

fn correction_inputs(events: &[Event]) -> Vec<&Event> {
    events
        .iter()
        .filter(|ev| {
            ev.kind == EventKind::InputAdmitted
                && ev.bytes.get("synthetic") == Some(&json!(true))
                && ev.bytes.pointer("/content/kian_schema_check").is_some()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------

/// A valid answer passes the gate silently: no verdict, no correction,
/// the completion carries the answer, and the derived outcome stays
/// `completed_unverified` (§3.2.1:437-441 — a silence is honest).
#[tokio::test]
async fn a_valid_answer_passes_the_copy_gate_silently() {
    let child = make_child(true);
    let rig = assemble(
        &child,
        child_events(&child),
        &["{\"summary\": \"all sources cited\"}"],
    );
    let outcomes = drive(&rig).await;
    let events = committed(&rig);
    assert_eq!(outcomes, vec![LoopOutcome::Settled]);
    assert!(
        schema_verdicts(&events).is_empty(),
        "a pass commits no verdict"
    );
    assert!(
        correction_inputs(&events).is_empty(),
        "a pass admits no correction"
    );
    let completed = events
        .iter()
        .find(|ev| ev.kind == EventKind::ThreadCompleted)
        .expect("completion");
    assert_eq!(completed.bytes["status"], json!("completed"));
    assert_eq!(
        completed.bytes["summary"]["text"],
        json!("{\"summary\": \"all sources cited\"}")
    );
    assert_eq!(
        derive_outcome(&events, None),
        Some(kian_superagent::TaskOutcome::CompletedUnverified)
    );
}

/// Miss #0 admits exactly one correction input with the byte-stable
/// synthetic content and re-drives; the corrected answer completes the
/// thread with no verdict anywhere on the stream.
#[tokio::test]
async fn a_first_miss_admits_exactly_one_correction_and_then_completes() {
    let child = make_child(true);
    let rig = assemble(
        &child,
        child_events(&child),
        &["not json at all", "{\"summary\": \"fixed\"}"],
    );
    let outcomes = drive(&rig).await;
    let events = committed(&rig);
    // TurnDone (the continue signal) then the terminal Settled.
    assert_eq!(outcomes, vec![LoopOutcome::TurnDone, LoopOutcome::Settled]);
    let corrections = correction_inputs(&events);
    assert_eq!(corrections.len(), 1, "exactly one correction");
    // The correction's content is the pure derivation off the same
    // committed facts (byte-stable across a replay).
    let failed = kian_superagent::schema_gate_checks(&summary_schema(), "not json at all");
    assert_eq!(failed.len(), 1);
    assert!(!failed[0].passed);
    assert_eq!(
        corrections[0].bytes,
        schema_correction_content(0, &failed),
        "the correction content is the pure derivation"
    );
    // The idempotency key the append derives from (asserted through
    // the pure derivation — same committed wrapper, same attempt).
    assert_eq!(
        schema_correction_key(&child.identity.spawn_uid.to_string(), 0),
        format!("schema-correction:{}:0", child.identity.spawn_uid)
    );
    assert!(
        schema_verdicts(&events).is_empty(),
        "a corrected answer commits no verdict"
    );
    let completed = events
        .iter()
        .find(|ev| ev.kind == EventKind::ThreadCompleted)
        .expect("completion");
    assert_eq!(completed.bytes["status"], json!("completed"));
}

/// Miss #1 commits the verdict (`passed:false`, `attempt:1`) and ends
/// `thread/completed{failed}` with the RAW answer preserved as the
/// summary (§3.2.1:445-451) — and the derived outcome is
/// `invalid_output` (pin P2-5).
#[tokio::test]
async fn a_second_miss_commits_the_verdict_and_preserves_the_raw_answer() {
    let child = make_child(true);
    // The FINAL answer (the one that failed the gate the second
    // time) is the raw answer the report must preserve.
    let raw = "also not json, second attempt";
    let rig = assemble(
        &child,
        child_events(&child),
        &["still not json, sorry", raw],
    );
    let outcomes = drive(&rig).await;
    let events = committed(&rig);
    // TurnDone (the miss-#0 continue signal) then the schema gate's
    // terminal soft failure.
    assert_eq!(outcomes.len(), 2);
    assert_eq!(outcomes[0], LoopOutcome::TurnDone);
    assert!(matches!(outcomes[1], LoopOutcome::SoftFailed { .. }));
    assert_eq!(
        correction_inputs(&events).len(),
        1,
        "exactly one correction, then the verdict"
    );
    let verdicts = schema_verdicts(&events);
    assert_eq!(verdicts.len(), 1, "exactly one committed verdict");
    assert_eq!(verdicts[0].bytes["passed"], json!(false));
    assert_eq!(verdicts[0].bytes["attempt"], json!(1));
    assert_eq!(verdicts[0].bytes["checks"][0]["name"], json!("json_parse"));
    let completed = events
        .iter()
        .find(|ev| ev.kind == EventKind::ThreadCompleted)
        .expect("completion");
    assert_eq!(completed.bytes["status"], json!("failed"));
    assert_eq!(
        completed.bytes["summary"]["text"],
        json!(raw),
        "the RAW answer is preserved in the report content"
    );
    assert_eq!(
        derive_outcome(&events, None),
        Some(kian_superagent::TaskOutcome::InvalidOutput)
    );
    // The report the parent receives carries the same raw answer.
    let report = rig.inbox.admits();
    assert_eq!(report.len(), 1);
    assert_eq!(report[0].1["summary"]["text"], json!(raw));
}

/// M8: a wrapper WITHOUT `output_schema` produces no gate, no verdict,
/// no correction — the completion bytes are exactly today's shape
/// (nothing between the assistant message and `thread/completed`).
#[tokio::test]
async fn no_output_schema_means_no_gate_no_verdict_and_no_new_bytes() {
    let child = make_child(false);
    let events0 = child_events(&child);
    let rig = assemble(&child, events0.clone(), &["any prose at all"]);
    let outcomes = drive(&rig).await;
    let events = committed(&rig);
    assert_eq!(outcomes, vec![LoopOutcome::Settled]);
    assert!(schema_verdicts(&events).is_empty());
    assert!(correction_inputs(&events).is_empty());
    // The appended tail is exactly: assistant/message then
    // thread/completed then the report admit — no gate bytes at all.
    let kinds: Vec<EventKind> = events[events0.len()..].iter().map(|e| e.kind).collect();
    assert_eq!(
        kinds,
        vec![EventKind::AssistantMessage, EventKind::ThreadCompleted]
    );
    let completed = events
        .iter()
        .find(|ev| ev.kind == EventKind::ThreadCompleted)
        .expect("completion");
    assert_eq!(completed.bytes["status"], json!("completed"));
    assert_eq!(
        derive_outcome(&events, None),
        Some(kian_superagent::TaskOutcome::CompletedUnverified)
    );
}

/// §6.2 row 3 precedence: an empty final answer is `empty_output` and
/// never a schema miss — no verdict, no correction, even though the
/// wrapper declares a schema. The empty answer arrives as a
/// `turn/end{class: empty_response}` soft failure (the loop's only
/// shape for it).
#[tokio::test]
async fn an_empty_final_answer_is_empty_output_and_never_a_schema_miss() {
    // A soft-failed outcome drives the failure arm directly (the
    // empty response never produced an assistant message): the
    // empty answer arrives as a `turn/end{class: empty_response}`
    // soft failure — the loop's only shape for it.
    let child = make_child(true);
    let inbox = CaptureInbox::shared();
    let state = Arc::new(Mutex::new({
        let mut events = child_events(&child);
        let seq = events.last().map(|e| e.seq + 1).unwrap_or(1);
        events.push(ev(
            seq,
            child.session,
            EventKind::TurnEnd,
            json!({
                "iteration": 1,
                "outcome": "soft_failure",
                "error": {"class": "empty_response", "action": "none"},
            }),
        ));
        events
    }));
    struct SoftFail;
    impl LoopDriver for SoftFail {
        type Ctx = ();
        fn drive(&self, _ctx: ()) -> BoxStream<'static, LoopOutcome> {
            futures::stream::once(async move {
                LoopOutcome::SoftFailed {
                    reason: "empty model response".into(),
                }
            })
            .boxed()
        }
    }
    let port_session = child.session;
    let port_state = Arc::clone(&state);
    let port_for: PortFor<()> = Arc::new(move |_ctx: &()| {
        Arc::new(FakePort {
            session: port_session,
            events: Arc::clone(&port_state),
        }) as Arc<dyn StreamPort>
    });
    let driver = WorkerLoopDriver::new(
        Arc::new(SoftFail),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        WorkerLoopConfig {
            router: sid(AMBIENT_ROUTER),
            worker_version: 7,
            gate: EvidenceGateConfig::default(),
            multiagent: Some(limits()),
        },
        port_for,
    );
    let outcomes: Vec<LoopOutcome> = driver.drive(()).collect().await;
    let events = state.lock().unwrap().clone();
    assert_eq!(
        outcomes,
        vec![LoopOutcome::SoftFailed {
            reason: "empty model response".into()
        }]
    );
    assert!(schema_verdicts(&events).is_empty(), "never a schema miss");
    assert!(correction_inputs(&events).is_empty());
    assert_eq!(
        derive_outcome(&events, None),
        Some(kian_superagent::TaskOutcome::EmptyOutput)
    );
    let completed = events
        .iter()
        .find(|ev| ev.kind == EventKind::ThreadCompleted)
        .expect("completion");
    assert_eq!(completed.bytes["status"], json!("failed"));
}

/// §2.2.1's byte-stability discipline applied to the correction row:
/// the same committed events re-derive the same key and the same
/// content — a takeover or replay drive can never double-append or
/// drift (the counter and the content are pure functions of the
/// stream).
#[test]
fn the_correction_row_key_is_byte_stable_across_a_replay() {
    let child = make_child(true);
    let mut events = child_events(&child);
    // One committed miss: an assistant answer that fails the schema,
    // and the correction the gate appended for attempt 0.
    let seq = events.last().map(|e| e.seq + 1).unwrap_or(1);
    events.push(ev(
        seq,
        child.session,
        EventKind::AssistantMessage,
        json!({"role": "assistant", "content": {"text": "definitely not json"}}),
    ));
    let failed = kian_superagent::schema_gate_checks(&summary_schema(), "definitely not json");
    let seq = events.last().map(|e| e.seq + 1).unwrap_or(1);
    events.push(ev(
        seq,
        child.session,
        EventKind::InputAdmitted,
        schema_correction_content(0, &failed),
    ));
    // The replay: the same committed events plan the same next step.
    let plan = kian_superagent::schema_gate_plan(&events);
    match plan {
        kian_superagent::SchemaGatePlan::Miss { attempt, .. } => assert_eq!(attempt, 1),
        other => panic!("a second miss after one committed correction: {other:?}"),
    }
    // And the key derivation is a pure function of the wrapper + the
    // stream-derived attempt.
    let uid = child.identity.spawn_uid.to_string();
    assert_eq!(
        schema_correction_key(&uid, 0),
        schema_correction_key(&uid, 0)
    );
    assert_eq!(
        schema_correction_key(&uid, 0),
        format!("schema-correction:{uid}:0")
    );
    // A second committed correction would derive :1 — never again :0.
    assert_ne!(
        schema_correction_key(&uid, 1),
        schema_correction_key(&uid, 0)
    );
    // The content derivation is byte-equal for equal inputs.
    assert_eq!(
        schema_correction_content(0, &failed),
        schema_correction_content(0, &failed)
    );
    assert_ne!(
        schema_correction_content(1, &failed),
        schema_correction_content(0, &failed),
        "the attempt rides the content, so a replay can count them"
    );
}
