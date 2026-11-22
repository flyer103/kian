//! Integration tests: the answer surface (CONFIRMATION-MODEL §14d
//! phase 3a) — an inbound IM message mapped onto a pending `ask`
//! before the rule set and before any model call.
//!
//! The read side is a scripted [`RecallPort`] (the same seam the recall
//! toolpack reads through) and the write side a scripted
//! [`AnswerPort`]; the subscription service under test is the real one,
//! so the assertions cover the real interception ordering: a command
//! never reaches the inbox, a near miss is dropped, and ordinary input
//! still routes. No credentials, no network.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use kian_core::control::ConfirmDecision;
use kian_core::events::{Event, EventKind};
use kian_core::model::{Epoch, SessionId};
use kian_core::seams::{ChannelEvent, ToolCall, ToolResult};
use kian_orchestrator::WakeOutcome;
use kian_superagent::answers::{
    self, AnswerError, AnswerOutcome, AnswerPort, AnswerSurface, PendingDecisionTools,
};
use kian_superagent::{
    AdmitReceipt, InboxWriteError, InboxWriter, RecallPort, RecallPortError, ResidentBinding,
    Route, RoutingOutcome, RuleMatcher, SubscriptionRule, SubscriptionRuleSet, SubscriptionService,
};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

/// Scripted read port: per-session event streams plus the Router's
/// thread list.
#[derive(Clone, Default)]
struct FakeRecall {
    streams: BTreeMap<SessionId, Vec<Event>>,
    threads: Vec<(String, SessionId)>,
}

impl FakeRecall {
    fn with_stream(mut self, session: SessionId, events: Vec<Event>) -> Self {
        self.streams.insert(session, events);
        self
    }

    fn with_thread(mut self, thread_id: &str, worker: SessionId) -> Self {
        self.threads.push((thread_id.to_string(), worker));
        self
    }
}

impl RecallPort for FakeRecall {
    fn read_events(
        &self,
        session: SessionId,
        since_seq: u64,
        until_seq: u64,
    ) -> BoxFuture<'static, Result<Vec<Event>, RecallPortError>> {
        let events = self
            .streams
            .get(&session)
            .map(|events| {
                events
                    .iter()
                    .filter(|event| event.seq >= since_seq && event.seq < until_seq)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        Box::pin(async move { Ok(events) })
    }

    fn list_threads(
        &self,
        _parent: SessionId,
    ) -> BoxFuture<'static, Result<Vec<(String, SessionId)>, RecallPortError>> {
        let threads = self.threads.clone();
        Box::pin(async move { Ok(threads) })
    }

    fn spill_location(
        &self,
        _event_id: &str,
    ) -> BoxFuture<'static, Result<Option<(SessionId, u64)>, RecallPortError>> {
        Box::pin(async move { Ok(None) })
    }
}

/// One recorded confirmation write.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Write {
    session: SessionId,
    call_id: String,
    decision: ConfirmDecision,
    reason: Option<String>,
    requested_by: String,
}

/// Scripted write port: records every answer and returns a fixed
/// outcome (or a fixed error).
#[derive(Default)]
struct FakeAnswer {
    writes: Mutex<Vec<Write>>,
    outcome: Option<AnswerOutcome>,
    failure: Option<String>,
}

impl FakeAnswer {
    fn accepting() -> Arc<Self> {
        Arc::new(Self {
            outcome: Some(AnswerOutcome::Accepted),
            ..Self::default()
        })
    }

    fn returning(outcome: AnswerOutcome) -> Arc<Self> {
        Arc::new(Self {
            outcome: Some(outcome),
            ..Self::default()
        })
    }

    fn failing(message: &str) -> Arc<Self> {
        Arc::new(Self {
            failure: Some(message.to_string()),
            ..Self::default()
        })
    }

    fn writes(&self) -> Vec<Write> {
        self.writes.lock().unwrap().clone()
    }
}

impl AnswerPort for FakeAnswer {
    fn answer(
        &self,
        session: SessionId,
        call_id: String,
        decision: ConfirmDecision,
        reason: Option<String>,
        requested_by: String,
    ) -> BoxFuture<'static, Result<AnswerOutcome, AnswerError>> {
        self.writes.lock().unwrap().push(Write {
            session,
            call_id,
            decision,
            reason,
            requested_by,
        });
        let outcome = self.outcome;
        let failure = self.failure.clone();
        Box::pin(async move {
            match (outcome, failure) {
                (_, Some(message)) => Err(AnswerError::Write(message)),
                (Some(outcome), None) => Ok(outcome),
                (None, None) => Ok(AnswerOutcome::Accepted),
            }
        })
    }
}

/// Records admissions so a drop is observable as the absence of a row.
#[derive(Default)]
struct ScriptedInbox {
    admits: Mutex<Vec<(SessionId, Value)>>,
    wakes: Mutex<Vec<SessionId>>,
}

impl ScriptedInbox {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn admitted_rows(&self) -> usize {
        self.admits.lock().unwrap().len()
    }
}

impl InboxWriter for ScriptedInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        _idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        self.admits.lock().unwrap().push((session, payload));
        Box::pin(async move {
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

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Every channel routes to the Router, so anything the answer surface
/// does *not* intercept is observable as a routed inbox row.
fn eager_rules() -> SubscriptionRuleSet {
    SubscriptionRuleSet::new(vec![
        SubscriptionRule {
            channel: "phone".into(),
            matcher: RuleMatcher::Always,
            route: Route::Router,
        },
        SubscriptionRule {
            channel: "lark".into(),
            matcher: RuleMatcher::Always,
            route: Route::Router,
        },
    ])
}

fn event(channel: &str, event_id: &str, text: &str) -> ChannelEvent {
    ChannelEvent {
        channel: channel.into(),
        event_id: event_id.into(),
        topic: Some("phone".into()),
        sender: Some(json!("+14155551212")),
        group: None,
        payload: json!({ "text": text }),
        ts_ms: 1,
    }
}

/// One `tool/call` whose permission evaluation was `ask` — the shape
/// `pending_calls` folds into a pending decision.
fn ask_event(session: SessionId, seq: u64, call_id: &str) -> Event {
    Event {
        seq,
        session,
        epoch: Epoch::GENESIS,
        ts: 1_000,
        kind: EventKind::ToolCall,
        bytes: json!({
            "content": {
                "id": call_id,
                "name": "exec",
                "arguments": {"argv": ["rm", "-rf", "/tmp/x"]},
            },
            "permission": {"evaluated": "ask"},
        }),
        version: 0,
    }
}

fn surface(channels: &[&str], recall: FakeRecall, answer: Arc<FakeAnswer>) -> AnswerSurface {
    let channels: BTreeSet<String> = channels.iter().map(|c| (*c).to_string()).collect();
    AnswerSurface::new(
        channels,
        Arc::new(recall) as Arc<dyn RecallPort>,
        answer as Arc<dyn AnswerPort>,
        0,
    )
}

/// The service under test: a topic-bound worker plus the answer
/// surface, with the Router fixed and every rule routing.
fn service(
    inbox: Arc<ScriptedInbox>,
    answer_surface: AnswerSurface,
    worker: SessionId,
) -> SubscriptionService {
    let service = SubscriptionService::new(
        ResidentBinding::new(SessionId::new()),
        eager_rules(),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    )
    .with_answers(answer_surface);
    service.register_topic("phone", worker);
    service
}

/// The code the human would read off `pending_decisions`.
fn code(worker: SessionId, call_id: &str) -> String {
    answers::code_for(
        worker,
        &kian_core::confirmation::PendingCall {
            call_id: call_id.to_string(),
            tool: "exec".into(),
            arguments: json!({}),
            call_seq: 1,
            requested_at_ms: 1_000,
        },
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A complete `approve <code>` writes the decision and admits **nothing**:
/// the answer never becomes a model-visible input.
#[tokio::test]
async fn approve_code_writes_the_decision_and_admits_no_row() {
    let worker = SessionId::new();
    let inbox = ScriptedInbox::new();
    let answer = FakeAnswer::accepting();
    let surface = surface(
        &["phone"],
        FakeRecall::default().with_stream(worker, vec![ask_event(worker, 1, "call-1")]),
        Arc::clone(&answer),
    );
    let service = service(Arc::clone(&inbox), surface, worker);

    let outcome = service
        .handle_event(event(
            "phone",
            "evt-1",
            &format!("approve {}", code(worker, "call-1")),
        ))
        .await
        .unwrap();

    assert_eq!(
        outcome,
        RoutingOutcome::Answered {
            session: worker,
            call_id: "call-1".into(),
            decision: ConfirmDecision::Approve,
            applied: AnswerOutcome::Accepted,
        }
    );
    assert_eq!(inbox.admitted_rows(), 0, "an answer must never be admitted");
    assert!(inbox.wakes.lock().unwrap().is_empty());
    let writes = answer.writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].session, worker);
    assert_eq!(writes[0].call_id, "call-1");
    assert_eq!(writes[0].decision, ConfirmDecision::Approve);
    assert_eq!(writes[0].requested_by, "phone:+14155551212:evt-1");
}

/// A bare verb is ordinary input: people type "ok" constantly, so the
/// absence of a code means "not a command" (§14d X5).
#[tokio::test]
async fn bare_verb_falls_through_to_routing() {
    let worker = SessionId::new();
    let inbox = ScriptedInbox::new();
    let answer = FakeAnswer::accepting();
    let surface = surface(
        &["phone"],
        FakeRecall::default().with_stream(worker, vec![ask_event(worker, 1, "call-1")]),
        Arc::clone(&answer),
    );
    let service = service(Arc::clone(&inbox), surface, worker);

    let outcome = service
        .handle_event(event("phone", "evt-2", "ok"))
        .await
        .unwrap();

    assert!(matches!(outcome, RoutingOutcome::Routed { .. }));
    assert_eq!(inbox.admitted_rows(), 1);
    assert!(answer.writes().is_empty());
}

/// Command-shaped but unresolvable: dropped and audited, never
/// admitted — a typo cannot become a Router instruction.
#[tokio::test]
async fn a_verb_plus_prose_is_routed_like_any_input() {
    let worker = SessionId::new();
    let inbox = ScriptedInbox::new();
    let answer = FakeAnswer::accepting();
    let surface = surface(
        &["phone"],
        FakeRecall::default().with_stream(worker, vec![ask_event(worker, 1, "call-1")]),
        Arc::clone(&answer),
    );
    let service = service(Arc::clone(&inbox), surface, worker);

    let outcome = service
        .handle_event(event("phone", "evt-3", "approve 但先把目标路径改一下"))
        .await
        .unwrap();

    // §14d X5: the person is talking, not answering — the words reach
    // the Router as ordinary input and no decision is written.
    assert!(
        matches!(outcome, RoutingOutcome::Routed { .. }),
        "{outcome:?}"
    );
    assert_eq!(inbox.admitted_rows(), 1);
    assert!(answer.writes().is_empty());
}

/// A well-formed code that names no live pending call is dropped, not
/// guessed at.
#[tokio::test]
async fn unknown_code_is_dropped_not_admitted() {
    let worker = SessionId::new();
    let inbox = ScriptedInbox::new();
    let answer = FakeAnswer::accepting();
    let surface = surface(
        &["phone"],
        FakeRecall::default().with_stream(worker, vec![ask_event(worker, 1, "call-1")]),
        Arc::clone(&answer),
    );
    let service = service(Arc::clone(&inbox), surface, worker);

    let outcome = service
        .handle_event(event("phone", "evt-4", "approve zzzzzz"))
        .await
        .unwrap();

    assert_eq!(outcome, RoutingOutcome::DroppedAnswerNoMatch);
    assert_eq!(inbox.admitted_rows(), 0);
    assert!(answer.writes().is_empty());
}

/// A channel that did not opt in keeps the legacy intake path
/// byte-for-byte: no interceptor runs, even for command-shaped text.
#[tokio::test]
async fn unconfigured_channel_never_answers() {
    let worker = SessionId::new();
    let inbox = ScriptedInbox::new();
    let answer = FakeAnswer::accepting();
    let surface = surface(
        &["phone"],
        FakeRecall::default().with_stream(worker, vec![ask_event(worker, 1, "call-1")]),
        Arc::clone(&answer),
    );
    let service = service(Arc::clone(&inbox), surface, worker);

    let outcome = service
        .handle_event(event(
            "lark",
            "evt-5",
            &format!("approve {}", code(worker, "call-1")),
        ))
        .await
        .unwrap();

    assert!(matches!(outcome, RoutingOutcome::Routed { .. }));
    assert_eq!(inbox.admitted_rows(), 1);
    assert!(answer.writes().is_empty());
}

/// The reject path carries the sender's reason to the confirmation
/// plane (audit-only there; only the model-visible reject error renders
/// it).
#[tokio::test]
async fn reject_carries_the_reason() {
    let worker = SessionId::new();
    let inbox = ScriptedInbox::new();
    let answer = FakeAnswer::accepting();
    let surface = surface(
        &["phone"],
        FakeRecall::default().with_stream(worker, vec![ask_event(worker, 1, "call-1")]),
        Arc::clone(&answer),
    );
    let service = service(Arc::clone(&inbox), surface, worker);

    let outcome = service
        .handle_event(event(
            "phone",
            "evt-6",
            &format!("reject {} 数据库没备份", code(worker, "call-1")),
        ))
        .await
        .unwrap();

    assert!(matches!(outcome, RoutingOutcome::Answered { .. }));
    let writes = answer.writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].decision, ConfirmDecision::Reject);
    assert_eq!(writes[0].reason.as_deref(), Some("数据库没备份"));
}

/// A racing (or lapsed) answer is reported honestly as
/// `not_pending`/`expired` — the outcome is not dressed up as applied.
#[tokio::test]
async fn a_raced_answer_is_reported_as_not_pending() {
    let worker = SessionId::new();
    let inbox = ScriptedInbox::new();
    let answer = FakeAnswer::returning(AnswerOutcome::NotPending);
    let surface = surface(
        &["phone"],
        FakeRecall::default().with_stream(worker, vec![ask_event(worker, 1, "call-1")]),
        Arc::clone(&answer),
    );
    let service = service(Arc::clone(&inbox), surface, worker);

    let outcome = service
        .handle_event(event(
            "phone",
            "evt-7",
            &format!("approve {}", code(worker, "call-1")),
        ))
        .await
        .unwrap();

    assert_eq!(
        outcome,
        RoutingOutcome::Answered {
            session: worker,
            call_id: "call-1".into(),
            decision: ConfirmDecision::Approve,
            applied: AnswerOutcome::NotPending,
        }
    );
    assert_eq!(inbox.admitted_rows(), 0);
}

/// A failed write decides nothing and admits nothing: the honest
/// outcome is a drop with its own reason code.
#[tokio::test]
async fn write_failure_is_a_drop_not_an_admit() {
    let worker = SessionId::new();
    let inbox = ScriptedInbox::new();
    let answer = FakeAnswer::failing("store unavailable");
    let surface = surface(
        &["phone"],
        FakeRecall::default().with_stream(worker, vec![ask_event(worker, 1, "call-1")]),
        Arc::clone(&answer),
    );
    let service = service(Arc::clone(&inbox), surface, worker);

    let outcome = service
        .handle_event(event(
            "phone",
            "evt-8",
            &format!("approve {}", code(worker, "call-1")),
        ))
        .await
        .unwrap();

    assert_eq!(outcome, RoutingOutcome::DroppedAnswerWriteFailed);
    assert_eq!(inbox.admitted_rows(), 0);
}

/// The operator's way to learn the code: the read-only listing names
/// the worker, the call and the derived code, and never mutates
/// anything.
#[tokio::test]
async fn pending_decisions_lists_the_codes() {
    use kian_core::seams::ToolProvider;

    let worker = SessionId::new();
    let router = SessionId::new();
    let recall = FakeRecall::default()
        .with_stream(worker, vec![ask_event(worker, 1, "call-1")])
        .with_thread("thread-1", worker);
    let tools = PendingDecisionTools::new(
        Arc::new(recall) as Arc<dyn RecallPort>,
        ResidentBinding::new(router),
        0,
    );

    let schemas = tools.schemas();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, answers::PENDING_DECISIONS_TOOL);

    let result: ToolResult = tools
        .invoke(ToolCall {
            id: "c-1".into(),
            name: answers::PENDING_DECISIONS_TOOL.into(),
            arguments: json!({}),
        })
        .await
        .unwrap();

    assert!(!result.is_error);
    let pending = result.output["pending"].as_array().expect("pending list");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0]["thread_id"], "thread-1");
    assert_eq!(pending[0]["call_id"], "call-1");
    assert_eq!(pending[0]["code"], code(worker, "call-1"));
    assert_eq!(pending[0]["tool"], "exec");
    assert_eq!(pending[0]["expired"], false);
}

/// A thread whose worker stream cannot be read does not hide the
/// others: the gap is recorded honestly rather than swallowed.
#[tokio::test]
async fn pending_decisions_reports_an_unreadable_thread() {
    use kian_core::seams::ToolProvider;

    struct Unreadable;
    impl RecallPort for Unreadable {
        fn read_events(
            &self,
            _session: SessionId,
            _since_seq: u64,
            _until_seq: u64,
        ) -> BoxFuture<'static, Result<Vec<Event>, RecallPortError>> {
            Box::pin(async move { Err(RecallPortError::Read("disk gone".into())) })
        }
        fn list_threads(
            &self,
            _parent: SessionId,
        ) -> BoxFuture<'static, Result<Vec<(String, SessionId)>, RecallPortError>> {
            let worker = SessionId::new();
            Box::pin(async move { Ok(vec![("thread-x".to_string(), worker)]) })
        }
        fn spill_location(
            &self,
            _event_id: &str,
        ) -> BoxFuture<'static, Result<Option<(SessionId, u64)>, RecallPortError>> {
            Box::pin(async move { Ok(None) })
        }
    }

    let tools = PendingDecisionTools::new(
        Arc::new(Unreadable) as Arc<dyn RecallPort>,
        ResidentBinding::new(SessionId::new()),
        0,
    );
    let result = tools
        .invoke(ToolCall {
            id: "c-2".into(),
            name: answers::PENDING_DECISIONS_TOOL.into(),
            arguments: json!({}),
        })
        .await
        .unwrap();

    let pending = result.output["pending"].as_array().expect("pending list");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0]["thread_id"], "thread-x");
    assert!(pending[0]["error"].as_str().is_some());
}
