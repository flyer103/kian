//! (R15-C4) Acceptance probes for the join side of the multiagent
//! delegation core (plan §4 C4): the durable parent row, the §2.2.1
//! pair-of-writes reconciliation (F2), the join target from the
//! child's own stream (F17), and the derived task outcome.
//!
//! Every fixture is authored here: a scripted inner [`LoopDriver`]
//! yielding one terminal outcome, an in-memory [`StreamPort`] over a
//! seeded child stream, and a recording inbox that de-duplicates by
//! idempotency key exactly like the real store. The F2 test seeds the
//! crash window (`thread/completed` committed, no parent row) and is
//! RED on pre-C4 code, which returns before admitting. No network, no
//! daemon, no store.
//!
//! Log-asserting tests follow the tracing capture discipline (the
//! crate's `tracing-test-log-capture` lesson): this file is its own
//! test binary, and one process-wide global subscriber is installed
//! first thing through a shared buffer every such test reads.

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::{Epoch, SessionId, Thread};
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_loop::EvidenceGateConfig;
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    AdmitReceipt, InboxWriteError, InboxWriter, MultiagentLimits, PortFor, ReportState,
    SpawnIdentity, SpawnRequest, StreamPort, StreamPortError, TaskOutcome, ThreadReport,
    WorkerLoopConfig, WorkerLoopDriver, WorkerSpawner, child_session_id, derive_outcome,
    thread_completed_payload, thread_report_content, thread_started_payload, worker_template,
};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixtures: ids, child streams
// ---------------------------------------------------------------------------

fn sid(n: u128) -> SessionId {
    SessionId(Uuid::from_u128(n))
}

/// The ambient Router the config names — deliberately NOT the child's
/// parent, so any ambient fallback is visible as a wrong admit target.
const AMBIENT_ROUTER: u128 = 0x999;

fn limits() -> MultiagentLimits {
    MultiagentLimits {
        max_children: 4,
        max_depth: 1,
        child_timeout_ms: 900_000,
    }
}

/// One seeded child: the wrapper bytes come from the real spawn-side
/// builder (C2's `WorkerSpawner::prepare`), so the join reads exactly
/// the bytes production writes.
struct Child {
    parent: SessionId,
    session: SessionId,
    thread_id: Uuid,
    identity: Option<SpawnIdentity>,
    task_message: Value,
}

fn make_child(seq: u64, with_identity: bool) -> Child {
    let router = sid(AMBIENT_ROUTER);
    let parent = sid(0x100 + u128::from(seq));
    let identity = with_identity.then(|| SpawnIdentity::derive(parent, seq, "mock-worker", 7));
    let spawner = WorkerSpawner::new(
        router,
        &worker_template("mock", "mock-worker", 7),
        noop_inbox(),
    );
    let spawned = spawner.prepare(&SpawnRequest {
        task: json!("build the thing"),
        identity: identity.clone(),
        ..SpawnRequest::default()
    });
    if let Some(identity) = &identity {
        // Pin P1/P7: the child session and thread id are the derived
        // ones — the fixture does not invent ids.
        assert_eq!(spawned.thread.id, identity.task_id);
        assert_eq!(spawned.worker_session, child_session_id(identity.task_id));
    }
    Child {
        parent,
        session: spawned.worker_session,
        thread_id: spawned.thread.id,
        identity,
        task_message: spawned.task_message,
    }
}

/// The child's committed stream prefix: creation, the wrapper as the
/// first admitted input (the same read `lineage_plan` performs), and
/// the worker-side `thread/started` naming the true parent.
fn child_events(child: &Child, started_bytes: Option<Value>) -> Vec<Event> {
    let started = started_bytes.unwrap_or_else(|| {
        thread_started_payload(
            &Thread {
                id: child.thread_id,
                parent_session: child.parent,
                pinned_version: 7,
            },
            child.session,
            &child.task_message,
        )
    });
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

/// Appends one more event with the next seq.
fn push(events: &mut Vec<Event>, session: SessionId, kind: EventKind, bytes: Value) {
    let seq = events.last().map(|e| e.seq + 1).unwrap_or(1);
    events.push(ev(seq, session, kind, bytes));
}

fn assistant(text: &str) -> (EventKind, Value) {
    (
        EventKind::AssistantMessage,
        json!({"content": {"text": text}}),
    )
}

fn completed(status: &str) -> (EventKind, Value) {
    (
        EventKind::ThreadCompleted,
        thread_completed_payload(Uuid::nil(), status, json!({"text": "all done"})),
    )
}

/// A committed worker-gate verdict (the shape `verdict_payload` writes).
fn worker_verdict(passed: bool) -> (EventKind, Value) {
    (
        EventKind::EvidenceVerdict,
        json!({
            "gate": "worker",
            "passed": passed,
            "checks": [
                {"name": "final_message_present", "passed": true},
                {"name": "no_error_tool_result_tail", "passed": passed, "detail": "tail=2"},
            ],
            "attempt": 1,
        }),
    )
}

fn turn_end_error(class: &str) -> (EventKind, Value) {
    (
        EventKind::TurnEnd,
        json!({
            "iteration": 3,
            "outcome": "soft_failure",
            "error": {"class": class, "action": "none"},
        }),
    )
}

fn control_interrupt(requested_by: &str) -> (EventKind, Value) {
    (
        EventKind::ControlInterrupt,
        json!({
            "action": "interrupt",
            "reason": "inactivity bound exceeded",
            "requested_by": requested_by,
            "requested_at_ms": 1_700_000_090_000_u64,
            "target_epoch": 2,
        }),
    )
}

fn session_ended(reason: EndReason) -> (EventKind, Value) {
    (EventKind::SessionEnded { reason }, json!({}))
}

// ---------------------------------------------------------------------------
// Fakes: inbox, stream port, inner driver
// ---------------------------------------------------------------------------

/// One recorded admit attempt: target session, payload, idempotency
/// key, and whether the inbox de-duplicated it onto an existing row.
#[derive(Debug, Clone)]
struct AdmitCall {
    session: SessionId,
    payload: Value,
    key: Option<String>,
    deduplicated: bool,
}

/// A recording inbox that de-duplicates by `(session, key)` exactly
/// like the real store admit, records every attempt (payload + key +
/// receipt), every wake, and can inject one admit failure to probe
/// the honest-settle path.
struct CaptureInbox {
    admits: Mutex<Vec<AdmitCall>>,
    rows: Mutex<Vec<(SessionId, String)>>,
    wakes: Mutex<Vec<SessionId>>,
    fail_admits: AtomicBool,
}

impl CaptureInbox {
    fn shared() -> Arc<Self> {
        Arc::new(Self {
            admits: Mutex::new(Vec::new()),
            rows: Mutex::new(Vec::new()),
            wakes: Mutex::new(Vec::new()),
            fail_admits: AtomicBool::new(false),
        })
    }

    fn admits(&self) -> Vec<AdmitCall> {
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
        let deduplicated = match &idempotency_key {
            Some(key) => {
                let known = {
                    let rows = self.rows.lock().unwrap();
                    rows.iter().any(|(s, k)| *s == session && k == key)
                };
                // A failed admit is not durable: the row only lands
                // when the admit succeeds (the real store's semantics).
                if !known && !self.fail_admits.load(Ordering::SeqCst) {
                    self.rows.lock().unwrap().push((session, key.clone()));
                }
                known
            }
            None => false,
        };
        self.admits.lock().unwrap().push(AdmitCall {
            session,
            payload,
            key: idempotency_key,
            deduplicated,
        });
        if self.fail_admits.load(Ordering::SeqCst) {
            return Box::pin(async {
                Err(InboxWriteError::Write("injected admit failure".into()))
            });
        }
        Box::pin(async move { Ok(AdmitReceipt { deduplicated }) })
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.wakes.lock().unwrap().push(session);
        WakeOutcome::Folded
    }
}

/// A minimal inbox for builders that need an `Arc<dyn InboxWriter>` but
/// never admit through it.
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

/// The in-memory stream port: emit appends with the next seq, read
/// returns the committed stream (the sole truth).
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

/// One assembled join under test: the driver, its recording inbox, and
/// the shared stream state the port reads and writes.
struct Join {
    driver: WorkerLoopDriver<ScriptedInner>,
    inbox: Arc<CaptureInbox>,
    events: Arc<Mutex<Vec<Event>>>,
}

fn assemble(
    events: Vec<Event>,
    session: SessionId,
    multiagent: Option<MultiagentLimits>,
    inner: LoopOutcome,
) -> Join {
    // Every drive installs the process-wide log capture first (the
    // tracing capture discipline: global default, one shared buffer).
    let _ = logs();
    let inbox = CaptureInbox::shared();
    let state = Arc::new(Mutex::new(events));
    let port_session = session;
    let port_state = Arc::clone(&state);
    let port_for: PortFor<()> = Arc::new(move |_ctx: &()| {
        Arc::new(FakePort {
            session: port_session,
            events: Arc::clone(&port_state),
        }) as Arc<dyn StreamPort>
    });
    let config = WorkerLoopConfig {
        router: sid(AMBIENT_ROUTER),
        worker_version: 7,
        gate: EvidenceGateConfig::default(),
        multiagent,
        board_gating: kian_superagent::BoardGating::default(),
    };
    let driver = WorkerLoopDriver::new(
        Arc::new(ScriptedInner { outcome: inner }),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
        config,
        port_for,
    );
    Join {
        driver,
        inbox,
        events: state,
    }
}

async fn drive(join: &Join) -> Vec<LoopOutcome> {
    join.driver.drive(()).collect().await
}

/// Seeds the crash window: the child stream carries the completion
/// pair's first write (`thread/completed`) but the parent has no row.
fn crash_window_stream(child: &Child) -> Vec<Event> {
    let mut events = child_events(child, None);
    let (kind, mut bytes) = completed("completed");
    bytes["thread_id"] = json!(child.thread_id.to_string());
    push(&mut events, child.session, kind, bytes);
    events
}

// ---------------------------------------------------------------------------
// Log capture (one global subscriber, one shared buffer)
// ---------------------------------------------------------------------------

static LOG: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();

/// The shared log buffer; installs the process-wide global subscriber
/// on first use (this binary owns the process, so `expect` is sound).
fn logs() -> Arc<Mutex<Vec<u8>>> {
    Arc::clone(LOG.get_or_init(|| {
        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let writer = LogBuf(Arc::clone(&buf));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("multiagent_join owns the global subscriber");
        buf
    }))
}

/// The log text appended since `mark` (a byte offset into the buffer).
fn logs_since(mark: usize) -> String {
    let buf = logs();
    let buf = buf.lock().unwrap();
    String::from_utf8_lossy(&buf[mark.min(buf.len())..]).into_owned()
}

#[derive(Clone)]
struct LogBuf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = LogBuf;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

// ---------------------------------------------------------------------------
// 1. The F2 crash-window test (RED on pre-C4 code)
// ---------------------------------------------------------------------------

/// `thread/completed` committed, the parent's inbox has NO row: the
/// join re-derives the report from the same committed events and
/// re-admits it under the pin-P3 key `spawn:<spawn_uid>` to the parent
/// named by the child's own `thread/started`. Pre-C4 code returns
/// before admitting (the crash window leaves the parent outstanding
/// forever) — this probe is the round's central honesty test.
#[tokio::test]
async fn thread_completed_without_the_parent_row_re_admits_the_report() {
    let child = make_child(7, true);
    let join = assemble(
        crash_window_stream(&child),
        child.session,
        Some(limits()),
        LoopOutcome::Settled,
    );

    let outcomes = drive(&join).await;

    // The settle itself is unchanged: the outcome the inner loop gave.
    assert_eq!(outcomes, vec![LoopOutcome::Settled]);
    // Exactly one admit, to the PARENT (not the ambient router), under
    // the spawn key, with the byte-stable re-derived report.
    let identity = child.identity.as_ref().expect("identity fixture");
    let admits = join.inbox.admits();
    assert_eq!(admits.len(), 1, "exactly one re-admit");
    let (session, payload, key, deduplicated) = {
        let a = &admits[0];
        (a.session, &a.payload, a.key.as_deref(), a.deduplicated)
    };
    assert_eq!(session, child.parent, "the join target is the parent");
    assert_ne!(session, sid(AMBIENT_ROUTER), "never the ambient router");
    assert_eq!(
        key,
        Some(format!("spawn:{}", identity.spawn_uid).as_str()),
        "pin P3 report key"
    );
    assert!(!deduplicated, "the recording inbox had no row");
    // The re-derived bytes: exactly the C2 ident builder over the same
    // committed facts (no verdict here ⇒ no `contract` key).
    let expected = kian_superagent::worker::thread_report_content_ident(
        identity,
        "completed",
        json!({"text": "all done"}),
        None,
    );
    assert_eq!(payload, &expected);
    assert!(payload.get("contract").is_none());
    // Durable row first, wake strictly after (invariant 1).
    assert_eq!(
        *join.inbox.wakes.lock().unwrap(),
        vec![child.parent],
        "the parent is woken after the row is durable"
    );
}

// ---------------------------------------------------------------------------
// 2. Byte-stable, idempotent re-admission
// ---------------------------------------------------------------------------

/// Driving the crash window twice re-admits byte-identical report
/// bytes under the same key; the inbox de-duplicates to one row (the
/// second receipt says `deduplicated`).
#[tokio::test]
async fn re_admission_is_byte_stable_and_idempotent() {
    let child = make_child(8, true);
    let join = assemble(
        crash_window_stream(&child),
        child.session,
        Some(limits()),
        LoopOutcome::Settled,
    );

    let first = drive(&join).await;
    assert_eq!(first, vec![LoopOutcome::Settled]);
    let second = drive(&join).await;
    assert_eq!(second, vec![LoopOutcome::Settled]);

    let admits = join.inbox.admits();
    assert_eq!(admits.len(), 2, "both drives reached the inbox");
    let (s1, p1, k1, d1) = {
        let a = &admits[0];
        (a.session, a.payload.clone(), a.key.clone(), a.deduplicated)
    };
    let (s2, p2, k2, d2) = {
        let a = &admits[1];
        (a.session, a.payload.clone(), a.key.clone(), a.deduplicated)
    };
    assert_eq!(s1, s2, "same target session");
    assert_eq!(p1, p2, "byte-identical report on the second admit");
    assert_eq!(k1, k2, "same idempotency key on the second admit");
    assert!(!d1, "first admit created the row");
    assert!(d2, "second admit de-duplicated onto the same row");
    assert_eq!(join.inbox.rows.lock().unwrap().len(), 1, "one row");
}

// ---------------------------------------------------------------------------
// 3. The audit line, and no synthesized outcome
// ---------------------------------------------------------------------------

/// The reconciliation is observable (one audit line naming the thread,
/// the key and the DERIVED outcome) and honest: no new event kind is
/// appended, no payload key carries the outcome, and the report never
/// mentions one (§6.1.1: derived in the join, never persisted — P4 is
/// §7.2).
#[tokio::test]
async fn reconciliation_emits_an_audit_line_and_never_reports_a_synthesized_outcome() {
    let child = make_child(9, true);
    let join = assemble(
        crash_window_stream(&child),
        child.session,
        Some(limits()),
        LoopOutcome::Settled,
    );
    let mark = logs().lock().unwrap().len();
    let events_before = join.events.lock().unwrap().len();

    let outcomes = drive(&join).await;

    assert_eq!(outcomes, vec![LoopOutcome::Settled]);
    // No new event at all on the reconciliation path: the completion
    // pair's first write is already committed.
    assert_eq!(
        join.events.lock().unwrap().len(),
        events_before,
        "the reconciliation appends no event kind"
    );
    // One audit line: thread id, key, and the derived outcome.
    let text = logs_since(mark);
    assert!(
        text.contains(
            "worker completion reconciliation: report re-admitted (thread/completed already committed)"
        ),
        "the audit line is present:\n{text}"
    );
    assert!(text.contains(&child.thread_id.to_string()));
    assert!(text.contains(&format!(
        "spawn:{}",
        child.identity.as_ref().unwrap().spawn_uid
    )));
    // The outcome (completed_unverified here: completed, no committed
    // worker verdict) appears ONLY in the log.
    assert!(text.contains("completed_unverified"));
    let payload = join.inbox.admits()[0].payload.clone();
    assert!(
        payload.get("outcome").is_none(),
        "no payload key carries the outcome"
    );
    assert!(!payload.to_string().contains("\"outcome\""));
    let committed_completed = {
        let events = join.events.lock().unwrap().clone();
        events
            .iter()
            .find(|e| e.kind == EventKind::ThreadCompleted)
            .cloned()
            .unwrap()
    };
    assert!(
        !committed_completed
            .bytes
            .to_string()
            .contains("\"outcome\""),
        "thread/completed bytes are unchanged"
    );
}

// ---------------------------------------------------------------------------
// 4. The join target comes from the child's own stream (F17)
// ---------------------------------------------------------------------------

/// The child's `thread/started.parent` is a non-Router session ⇒ THAT
/// session receives the report row — not the configured ambient
/// Router.
#[tokio::test]
async fn completion_report_is_admitted_to_the_join_target_from_the_child_stream() {
    let child = make_child(10, true);
    assert_ne!(child.parent, sid(AMBIENT_ROUTER));
    let join = assemble(
        crash_window_stream(&child),
        child.session,
        Some(limits()),
        LoopOutcome::Settled,
    );

    let outcomes = drive(&join).await;

    assert_eq!(outcomes, vec![LoopOutcome::Settled]);
    let admits = join.inbox.admits();
    assert_eq!(admits.len(), 1);
    assert_eq!(
        admits[0].session, child.parent,
        "the parent receives the row"
    );
    assert_eq!(
        *join.inbox.wakes.lock().unwrap(),
        vec![child.parent],
        "the parent is woken"
    );
}

// ---------------------------------------------------------------------------
// 5. The unreadable join target refusal (F17)
// ---------------------------------------------------------------------------

/// A worker stream with no readable `thread/started.parent` has no
/// join target: the join refuses — no report is emitted, no admit to
/// the ambient Router, `SoftFailed` names the exact reason — both on
/// the fresh path and inside the crash window.
#[tokio::test]
async fn an_unreadable_join_target_settles_honestly_and_never_falls_back_to_the_router() {
    let refusal =
        "worker completion: join target unknown (child stream carries no thread/started.parent)";
    let child = make_child(11, true);
    // The `thread/started` payload names the worker but carries no
    // readable `parent`.
    let mut started = thread_started_payload(
        &Thread {
            id: child.thread_id,
            parent_session: child.parent,
            pinned_version: 7,
        },
        child.session,
        &child.task_message,
    );
    started.as_object_mut().unwrap().remove("parent");

    // Fresh path (no thread/completed yet).
    let join = assemble(
        child_events(&child, Some(started.clone())),
        child.session,
        Some(limits()),
        LoopOutcome::Settled,
    );
    let outcomes = drive(&join).await;
    assert_eq!(
        outcomes,
        vec![LoopOutcome::SoftFailed {
            reason: refusal.to_string()
        }],
        "fresh path refuses honestly"
    );
    assert!(join.inbox.admits().is_empty(), "no report is emitted");
    assert!(join.inbox.wakes.lock().unwrap().is_empty());
    assert!(
        !join
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.kind == EventKind::ThreadCompleted),
        "no completion is fabricated for an unknowable target"
    );

    // Crash-window path (thread/completed already committed): same
    // refusal — the re-admit never guesses a target either.
    let mut events = child_events(&child, Some(started));
    let (kind, mut bytes) = completed("completed");
    bytes["thread_id"] = json!(child.thread_id.to_string());
    push(&mut events, child.session, kind, bytes);
    let join = assemble(events, child.session, Some(limits()), LoopOutcome::Settled);
    let outcomes = drive(&join).await;
    assert_eq!(
        outcomes,
        vec![LoopOutcome::SoftFailed {
            reason: refusal.to_string()
        }],
        "reconciliation path refuses honestly too"
    );
    assert!(join.inbox.admits().is_empty());
    assert!(join.inbox.wakes.lock().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// 6. Feature-off byte identity at the join (M8)
// ---------------------------------------------------------------------------

/// With the gate off (`multiagent: None`) the join is today's code
/// path exactly: the ambient Router is the admit target, the report is
/// the legacy builder's bytes under the legacy key (identity keys and
/// `contract` absent), and a `thread/completed`-without-row stream
/// still returns early without admitting.
#[tokio::test]
async fn feature_off_keeps_the_router_admit_target_and_the_legacy_report_key() {
    // The in-contract feature-off stream is a LEGACY child: the spawn
    // side never writes identity keys with the gate off (C2/C3), so
    // M8's byte identity is asserted over the legacy wrapper.
    let child = make_child(12, false);
    // Fresh completion: seed an assistant message so the summary is
    // exactly known.
    let mut events = child_events(&child, None);
    let (kind, bytes) = assistant("all done");
    push(&mut events, child.session, kind, bytes);
    let join = assemble(events, child.session, None, LoopOutcome::Settled);

    let outcomes = drive(&join).await;

    assert_eq!(outcomes, vec![LoopOutcome::Settled]);
    let admits = join.inbox.admits();
    assert_eq!(admits.len(), 1);
    let (session, payload, key) = {
        let a = &admits[0];
        (a.session, &a.payload, a.key.as_deref())
    };
    assert_eq!(session, sid(AMBIENT_ROUTER), "ambient router admits");
    assert_eq!(
        key,
        Some(format!("thread-report:{}", child.thread_id).as_str()),
        "legacy report key"
    );
    let expected = thread_report_content(child.thread_id, "completed", json!({"text": "all done"}));
    assert_eq!(payload, &expected);
    for key in ["spawn_uid", "task_id", "contract", "outcome"] {
        assert!(payload.get(key).is_none(), "`{key}` absent, never null");
    }
    assert_eq!(
        *join.inbox.wakes.lock().unwrap(),
        vec![sid(AMBIENT_ROUTER)],
        "the ambient router is woken"
    );

    // The crash window with the gate off is today's early return: no
    // re-admission is invented (M8 — no behavior change).
    let join = assemble(
        crash_window_stream(&child),
        child.session,
        None,
        LoopOutcome::Settled,
    );
    let outcomes = drive(&join).await;
    assert_eq!(outcomes, vec![LoopOutcome::Settled]);
    assert!(
        join.inbox.admits().is_empty(),
        "feature-off keeps today's early return"
    );

    // Mixed-fleet note (card item 4's letter: the key is ALWAYS
    // `report_key_for(&wrapper, ..)`): an identity wrapper completing
    // while the gate is off (a config flip after the spawn) keeps the
    // `spawn:` key family — the same key the gate-on join derives for
    // the same stream, so a flip can never double-admit one row.
    let flipped = make_child(16, true);
    let mut events = child_events(&flipped, None);
    let (kind, bytes) = assistant("flipped fleet");
    push(&mut events, flipped.session, kind, bytes);
    let join = assemble(events, flipped.session, None, LoopOutcome::Settled);
    let outcomes = drive(&join).await;
    assert_eq!(outcomes, vec![LoopOutcome::Settled]);
    let flipped_admit = join.inbox.admits()[0].clone();
    assert_eq!(
        flipped_admit.session,
        sid(AMBIENT_ROUTER),
        "feature-off target is ambient"
    );
    assert_eq!(
        flipped_admit.key.as_deref(),
        Some(format!("spawn:{}", flipped.identity.as_ref().unwrap().spawn_uid).as_str()),
        "one key family per stream across a config flip"
    );
}

// ---------------------------------------------------------------------------
// 7. Legacy child, legacy key (pin P3, upgrade safety)
// ---------------------------------------------------------------------------

/// A legacy child (pre-upgrade wrapper, no identity keys) completing
/// after the upgrade re-admits under its own legacy key with the
/// legacy report bytes — the two key families never collide, so the
/// upgrade needs no migration.
#[tokio::test]
async fn a_legacy_child_report_keeps_the_legacy_thread_report_key() {
    let child = make_child(13, false);
    assert!(child.identity.is_none(), "legacy fixture");
    let join = assemble(
        crash_window_stream(&child),
        child.session,
        Some(limits()),
        LoopOutcome::Settled,
    );

    let outcomes = drive(&join).await;

    assert_eq!(outcomes, vec![LoopOutcome::Settled]);
    let admits = join.inbox.admits();
    assert_eq!(admits.len(), 1);
    let (session, payload, key) = {
        let a = &admits[0];
        (a.session, &a.payload, a.key.as_deref())
    };
    assert_eq!(session, child.parent, "the parent is still the target");
    assert_eq!(
        key,
        Some(format!("thread-report:{}", child.thread_id).as_str()),
        "pin P3 legacy key (the migration hinge)"
    );
    assert_eq!(
        payload,
        &thread_report_content(child.thread_id, "completed", json!({"text": "all done"})),
        "legacy report bytes"
    );
    assert!(payload.get("spawn_uid").is_none());
    assert!(payload.get("task_id").is_none());
}

// ---------------------------------------------------------------------------
// 8. The pin-P6 class-C report keys
// ---------------------------------------------------------------------------

/// The multiagent report carries `spawn_uid`, `task_id` and
/// `contract:{passed, errors[]}` when a worker-gate verdict was
/// committed; without a verdict the `contract` key is absent (never
/// `null`).
#[tokio::test]
async fn report_content_carries_spawn_uid_task_id_and_contract_when_a_gate_ran() {
    let child = make_child(14, true);
    let identity = child.identity.clone().unwrap();

    // (a) A committed verdict (an earlier episode's gate run): the
    // report carries the contract object with the failed check names.
    let mut events = child_events(&child, None);
    let (kind, bytes) = worker_verdict(false);
    push(&mut events, child.session, kind, bytes);
    let join = assemble(events, child.session, Some(limits()), LoopOutcome::Settled);
    let outcomes = drive(&join).await;
    assert_eq!(outcomes, vec![LoopOutcome::Settled]);
    let admits = join.inbox.admits();
    assert_eq!(admits.len(), 1);
    let (payload, key) = {
        let a = &admits[0];
        (&a.payload, a.key.as_deref())
    };
    assert_eq!(key, Some(format!("spawn:{}", identity.spawn_uid).as_str()));
    assert_eq!(payload["spawn_uid"], json!(identity.spawn_uid.to_string()));
    assert_eq!(payload["task_id"], json!(identity.task_id.to_string()));
    assert_eq!(
        payload["contract"],
        json!({"passed": false, "errors": ["no_error_tool_result_tail"]}),
        "the contract names the failed checks"
    );

    // (b) No verdict anywhere: the `contract` key is absent — never
    // null — and the identity keys still ride.
    let child_b = make_child(15, true);
    let identity_b = child_b.identity.clone().unwrap();
    let mut events = child_events(&child_b, None);
    let (kind, bytes) = assistant("no gate ran");
    push(&mut events, child_b.session, kind, bytes);
    let join = assemble(
        events,
        child_b.session,
        Some(limits()),
        LoopOutcome::Settled,
    );
    let outcomes = drive(&join).await;
    assert_eq!(outcomes, vec![LoopOutcome::Settled]);
    let admit_b = join.inbox.admits()[0].clone();
    assert_eq!(
        admit_b.key.as_deref(),
        Some(format!("spawn:{}", identity_b.spawn_uid).as_str())
    );
    let payload = &admit_b.payload;
    assert!(payload.get("contract").is_none(), "absent, never null");
    assert!(!payload.to_string().contains("\"contract\""));
    assert_eq!(
        payload["spawn_uid"],
        json!(identity_b.spawn_uid.to_string())
    );
    assert_eq!(payload["task_id"], json!(identity_b.task_id.to_string()));
}

// ---------------------------------------------------------------------------
// 9. The outcome vocabulary, derived in the join (pin P8)
// ---------------------------------------------------------------------------

/// Table-driven over every reachable vocabulary row (all but
/// `invalid_output`): the committed events derive the row — pure
/// agreement with `derive_outcome` — and, for every row the join can
/// settle (a completion carrier exists), the reconciliation audit
/// line logs exactly that outcome. `empty_output` is non-success at
/// every policy setting (M6).
#[tokio::test]
async fn join_derives_the_outcome_for_every_reachable_vocabulary_row() {
    struct Row {
        name: &'static str,
        extra: Vec<(EventKind, Value)>,
        expected: TaskOutcome,
    }
    let rows = vec![
        Row {
            name: "completed_verified",
            extra: vec![worker_verdict(true), completed("completed")],
            expected: TaskOutcome::CompletedVerified,
        },
        Row {
            name: "completed_unverified",
            extra: vec![completed("completed")],
            expected: TaskOutcome::CompletedUnverified,
        },
        Row {
            name: "empty_output",
            extra: vec![turn_end_error("empty_response"), completed("failed")],
            expected: TaskOutcome::EmptyOutput,
        },
        Row {
            name: "budget_exhausted",
            extra: vec![turn_end_error("budget_exhausted"), completed("failed")],
            expected: TaskOutcome::BudgetExhausted,
        },
        Row {
            name: "stalled",
            extra: vec![
                control_interrupt("child_timeout"),
                completed("interrupted"),
                session_ended(EndReason::Interrupted),
            ],
            expected: TaskOutcome::Stalled,
        },
        Row {
            name: "interrupted",
            extra: vec![
                control_interrupt("user"),
                completed("interrupted"),
                session_ended(EndReason::Interrupted),
            ],
            expected: TaskOutcome::Interrupted,
        },
        Row {
            name: "failed",
            extra: vec![completed("failed")],
            expected: TaskOutcome::Failed,
        },
    ];

    for (i, row) in rows.into_iter().enumerate() {
        let child = make_child(20 + i as u64, true);
        let mut events = child_events(&child, None);
        for (kind, mut bytes) in row.extra {
            if kind == EventKind::ThreadCompleted {
                bytes["thread_id"] = json!(child.thread_id.to_string());
            }
            push(&mut events, child.session, kind, bytes);
        }

        // Pure agreement: the committed events derive the row, and
        // never `invalid_output` (unreachable at P1, §3.2.1).
        let derived = derive_outcome(&events, None).expect(row.name);
        assert_eq!(derived, row.expected, "row `{}`", row.name);
        assert_ne!(
            derived,
            TaskOutcome::InvalidOutput,
            "invalid_output is unreachable at P1"
        );

        // The join settles the row and logs exactly that outcome on
        // the reconciliation audit line (the outcome's only channel).
        let join = assemble(events, child.session, Some(limits()), LoopOutcome::Settled);
        let mark = logs().lock().unwrap().len();
        let outcomes = drive(&join).await;
        assert_eq!(outcomes, vec![LoopOutcome::Settled], "row `{}`", row.name);
        let text = logs_since(mark);
        assert!(
            text.contains(row.expected.as_str()),
            "row `{}`: the audit line logs the derived outcome `{}`:\n{text}",
            row.name,
            row.expected.as_str()
        );
    }

    // `input_required` is a LIVE child (no completion carrier, no
    // end, its own report row waiting): reachable vocabulary, but the
    // join never settles it — asserted at the pure derivation.
    let child = make_child(30, true);
    let events = child_events(&child, None);
    let own = ThreadReport {
        thread_id: child.thread_id,
        status: "waiting".to_string(),
        summary: json!({"text": "which region should I target?"}),
        state: Some(ReportState::Waiting),
    };
    assert_eq!(
        derive_outcome(&events, Some(&own)),
        Some(TaskOutcome::InputRequired)
    );

    // M6: an empty output is never a success at any policy setting.
    assert!(!TaskOutcome::EmptyOutput.is_success(false));
    assert!(!TaskOutcome::EmptyOutput.is_success(true));
}

// ---------------------------------------------------------------------------
// 10. Honest settle when the re-admit itself fails
// ---------------------------------------------------------------------------

/// A failed re-admit settles `SoftFailed` naming the failure (never a
/// silent parent miss) and the child stays resumable: the next claim
/// reconciles again — the admits are monotone, no row is lost.
#[tokio::test]
async fn a_failed_re_admit_settles_soft_failed_and_the_next_claim_reconciles() {
    let child = make_child(31, true);
    let join = assemble(
        crash_window_stream(&child),
        child.session,
        Some(limits()),
        LoopOutcome::Settled,
    );
    join.inbox.fail_admits.store(true, Ordering::SeqCst);

    let outcomes = drive(&join).await;

    assert_eq!(
        outcomes,
        vec![LoopOutcome::SoftFailed {
            reason:
                "worker completion reconciliation: re-admit failed: inbox write failed: injected admit failure"
                    .to_string()
        }],
        "the honest signal, never a silent parent miss"
    );
    assert!(
        join.inbox.wakes.lock().unwrap().is_empty(),
        "no wake without a durable row"
    );

    // The child stays resumable (session/ended{failed} returns to
    // idle): the next claim reconciles and lands the row.
    join.inbox.fail_admits.store(false, Ordering::SeqCst);
    let outcomes = drive(&join).await;
    assert_eq!(outcomes, vec![LoopOutcome::Settled]);
    let admits = join.inbox.admits();
    assert_eq!(admits.len(), 2, "the retry reached the inbox again");
    assert!(!admits[1].deduplicated, "the retry created the row");
    assert_eq!(
        admits[0].payload, admits[1].payload,
        "byte-identical re-derivation"
    );
    assert_eq!(join.inbox.rows.lock().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// (R17-C3, pin P2-5) invalid_output — reachable only through the
// committed schema verdict, both ways
// ---------------------------------------------------------------------------

/// A failed completion WITH a committed
/// `evidence/verdict{gate:"output_schema",passed:false}` derives
/// `invalid_output` (§6.1.1:640-644, §6.2 row 4) — checked before the
/// `turn/end` class rows: even a `turn/end{empty_response}` on the
/// same stream cannot shadow the schema verdict (the gate never runs
/// on an empty answer, so the two carriers are mutually exclusive in
/// production; the row order is pinned anyway).
#[test]
fn a_task_with_a_schema_verdict_reads_invalid_output() {
    let child = make_child(30, true);
    let mut events = child_events(&child, None);
    let (kind, bytes) = assistant("definitely not the declared schema");
    push(&mut events, child.session, kind, bytes);
    let (kind, bytes) = (
        kian_core::events::EventKind::EvidenceVerdict,
        serde_json::json!({
            "gate": "output_schema",
            "passed": false,
            "checks": [
                {"name": "json_parse", "passed": false,
                 "detail": "the final answer is not valid JSON"}
            ],
            "attempt": 1,
        }),
    );
    push(&mut events, child.session, kind, bytes);
    let (kind, bytes) = completed("failed");
    push(&mut events, child.session, kind, bytes);
    assert_eq!(
        derive_outcome(&events, None),
        Some(TaskOutcome::InvalidOutput)
    );
    // The row-4 precedence: an empty_response turn/end present beside
    // the verdict still reads invalid_output (row 4 first).
    let (kind, bytes) = turn_end_error("empty_response");
    push(&mut events, child.session, kind, bytes);
    assert_eq!(
        derive_outcome(&events, None),
        Some(TaskOutcome::InvalidOutput)
    );
}

/// Without the committed schema verdict the same failed completion
/// never reads `invalid_output` — a bare failure stays `failed`, an
/// empty answer stays `empty_output`, and a PASSED schema verdict
/// (which production never commits) derives nothing either: only a
/// committed `passed:false` carries the fact (pin P2-5 both ways).
#[test]
fn a_task_without_a_schema_verdict_never_reads_invalid_output() {
    let child = make_child(31, true);
    // Bare failure: no softer carrier.
    let mut events = child_events(&child, None);
    let (kind, bytes) = assistant("plain failure text");
    push(&mut events, child.session, kind, bytes);
    let (kind, bytes) = completed("failed");
    push(&mut events, child.session, kind, bytes);
    assert_eq!(derive_outcome(&events, None), Some(TaskOutcome::Failed));

    // Empty answer: row 3 owns it.
    let mut events = child_events(&child, None);
    let (kind, bytes) = completed("failed");
    push(&mut events, child.session, kind, bytes);
    let (kind, bytes) = turn_end_error("empty_response");
    push(&mut events, child.session, kind, bytes);
    assert_eq!(
        derive_outcome(&events, None),
        Some(TaskOutcome::EmptyOutput)
    );

    // A WORKER-gate verdict (the only pre-P2 verdict writer) is not a
    // schema verdict: the failure stays `failed`.
    let mut events = child_events(&child, None);
    let (kind, bytes) = worker_verdict(false);
    push(&mut events, child.session, kind, bytes);
    let (kind, bytes) = completed("failed");
    push(&mut events, child.session, kind, bytes);
    assert_eq!(derive_outcome(&events, None), Some(TaskOutcome::Failed));

    // A passed schema verdict beside a completed thread reads
    // completed_unverified — the outcome rows 1-2 are unaffected.
    let mut events = child_events(&child, None);
    let (kind, bytes) = assistant("{\"summary\": \"ok\"}");
    push(&mut events, child.session, kind, bytes);
    let (kind, bytes) = (
        kian_core::events::EventKind::EvidenceVerdict,
        serde_json::json!({
            "gate": "output_schema", "passed": true, "checks": [], "attempt": 0,
        }),
    );
    push(&mut events, child.session, kind, bytes);
    let (kind, bytes) = completed("completed");
    push(&mut events, child.session, kind, bytes);
    assert_eq!(
        derive_outcome(&events, None),
        Some(TaskOutcome::CompletedUnverified)
    );
}
