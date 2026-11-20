//! Follow-up ownership routing (DESIGN §4.1 + §4.7):
//! a person's follow-up belongs to the **live** Ticket's Executor
//! (reference 002) — the mechanical half is 's fan-out, and this
//! card closes its two gaps:
//!
//! 1. a follow-up on a topic with one live subscriber admits to that
//!    worker and spends **zero Router judge turns** on it (the Router
//!    session never receives the event's row, so no drain/turn can
//!    include it);
//! 2. The same topic after the worker's `thread/completed` releases to
//!    the Router — the *release to fresh evaluation* (002's Ticket-end
//!    rule), audited with the closed `released_to_eval` reason code
//!    and naming the released topic;
//! 3. `spawn_worker` with a topic correlation a live worker already
//!    owns is refused at the tool with an honest error naming the
//!    owner session — and spawns nothing (no inbox row, no
//!    `thread/started`); once the owner ended, the same spawn succeeds
//!    and the new worker takes the topic;
//! 4. The registry rebuild from a mixed live/ended stream equals the
//!    runtime map (the projection is disposable by construction).
//!
//! No credentials, no network egress. Log capture follows the
//! `tests/fanout_audit.rs` / `tests/speak_rules.rs` process-global
//! discipline (own test binary, own global subscriber).

use futures::future::BoxFuture;
use kian_core::events::{Event, EventKind};
use kian_core::model::{Epoch, SessionId};
use kian_core::seams::{ChannelAdapter, ChannelEvent, ToolCall, ToolError, ToolProvider};
use kian_orchestrator::WakeOutcome;
use kian_superagent::{
    AdmitReceipt, ChannelRegistry, CorrelateBy, InboxWriteError, InboxWriter, IntakeReason,
    MockChannel, PassthroughRegistry, Route, RouterHold, RouterProfile, RouterTools,
    RoutingOutcome, RuleMatcher, StreamPort, StreamPortError, SubscriptionRule,
    SubscriptionRuleSet, SubscriptionService, WorkerSpawner, WorkerStatus, router_template,
    worker_template,
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex, OnceLock};

// ---------------------------------------------------------------------------
// Log capture (audit lines) — one shared buffer per test binary
// ---------------------------------------------------------------------------

struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for SharedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct MakeShared(Arc<Mutex<Vec<u8>>>);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MakeShared {
    type Writer = SharedWriter;
    fn make_writer(&'a self) -> Self::Writer {
        SharedWriter(Arc::clone(&self.0))
    }
}

fn logs() -> Arc<Mutex<Vec<u8>>> {
    static BUF: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();
    BUF.get_or_init(|| {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(MakeShared(Arc::clone(&buf)))
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("followup_ownership owns its process's global subscriber");
        buf
    })
    .clone()
}

fn log_text() -> String {
    String::from_utf8_lossy(&logs().lock().unwrap()).into_owned()
}

/// Lines of the captured log that contain every needle.
fn audit_lines<'a>(text: &'a str, needles: &[&'a str]) -> Vec<&'a str> {
    text.lines()
        .filter(|line| needles.iter().all(|n| line.contains(n)))
        .collect()
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn sid(n: u128) -> SessionId {
    SessionId(uuid::Uuid::from_u128(n))
}

/// The passthrough rules of every routing test here: `ci` events on
/// `pr-9` correlate by topic.
fn rules() -> SubscriptionRuleSet {
    SubscriptionRuleSet::new(vec![SubscriptionRule {
        channel: "ci".into(),
        matcher: RuleMatcher::Topic("pr-9".into()),
        route: Route::Passthrough {
            correlate_by: CorrelateBy::Topic,
        },
    }])
}

fn ci_event(event_id: &str, topic: &str) -> ChannelEvent {
    ChannelEvent {
        channel: "ci".into(),
        event_id: event_id.into(),
        topic: Some(topic.into()),
        sender: None,
        group: None,
        payload: json!({"build": "ok"}),
        ts_ms: 1,
    }
}

/// The channel-event wrapper a Router passes as `spawn_worker`'s task
/// when dispatching on a channel event — the shape the registry rebuild
/// derives topic bindings from (one derivation, two readers).
fn channel_task(event_id: &str, topic: &str) -> Value {
    kian_superagent::provenance::wrap_channel_event(&ci_event(event_id, topic))
}

/// An inbox fake: records every admit (session + idempotency key) and
/// every wake — the observable surface "zero Router judge turns" is
/// asserted through (a Router judge turn requires a Router row).
struct RecordingInbox {
    admits: Mutex<Vec<(SessionId, Option<String>)>>,
    wakes: Mutex<Vec<SessionId>>,
}

impl RecordingInbox {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            admits: Mutex::new(Vec::new()),
            wakes: Mutex::new(Vec::new()),
        })
    }
}

impl InboxWriter for RecordingInbox {
    fn admit(
        &self,
        session: SessionId,
        _payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        self.admits.lock().unwrap().push((session, idempotency_key));
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

/// A stream port that records everything emitted (the Router's stream
/// as the tools see it).
struct RecordingPort {
    session: SessionId,
    events: Mutex<Vec<Event>>,
}

impl RecordingPort {
    fn shared(session: SessionId) -> (Arc<Self>, Arc<dyn StreamPort>) {
        let port = Arc::new(Self {
            session,
            events: Mutex::new(Vec::new()),
        });
        let dyn_port: Arc<dyn StreamPort> = Arc::clone(&port) as Arc<dyn StreamPort>;
        (port, dyn_port)
    }

    fn count_kind(&self, kind: EventKind) -> usize {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.kind == kind)
            .count()
    }
}

impl StreamPort for RecordingPort {
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
            epoch: Epoch(2),
            ts: 1_000 + seq,
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

/// The `spawn_worker` tool surface with the shared registry
/// installed (the assembly wiring: `with_followup_registry` over the
/// same Arc the subscription service would hold).
fn ownership_tools(
    registry: Arc<std::sync::RwLock<PassthroughRegistry>>,
    enabled: bool,
) -> (Arc<RouterTools>, Arc<RecordingInbox>, Arc<RecordingPort>) {
    let router = sid(1);
    let inbox = RecordingInbox::new();
    let spawner = Arc::new(WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_template("mock", "mock-worker", 7),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    ));
    let mut channels = ChannelRegistry::new();
    channels.register(
        "mock-im",
        Arc::new(MockChannel::new("mock-im")) as Arc<dyn ChannelAdapter>,
    );
    let hold = RouterHold::shared();
    let mut profile = RouterProfile::new(router, router_template("mock", "mock-router"));
    profile.followup_ownership = enabled;
    let tools = Arc::new(
        RouterTools::new(
            spawner,
            channels,
            &profile,
            hold.clone(),
            kian_superagent::ResidentBinding::new(profile.session),
        )
        .with_followup_registry(registry),
    );
    let (port, dyn_port) = RecordingPort::shared(router);
    hold.install(dyn_port);
    (tools, inbox, port)
}

fn spawn_call(task: Value) -> ToolCall {
    ToolCall {
        id: "call-1".into(),
        name: "spawn_worker".into(),
        arguments: json!({"task": task}),
    }
}

// ---------------------------------------------------------------------------
// Scripted streams (registry rebuild fixtures)
// ---------------------------------------------------------------------------

fn event(session: SessionId, seq: u64, kind: EventKind, bytes: Value) -> Event {
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

/// A Router-side `thread/started` delegating to `worker` with a task
/// content that is the channel-event wrapper of `topic` (the
/// rebuild source-1 shape).
fn thread_started(router: SessionId, worker: SessionId, topic: &str) -> Event {
    event(
        router,
        3,
        EventKind::ThreadStarted,
        json!({
            "thread_id": uuid::Uuid::new_v4().to_string(),
            "parent": router.0.to_string(),
            "worker": worker.0.to_string(),
            "pinned_version": 7,
            "task": {"role": "user", "content": channel_task("evt-spawn", topic)},
        }),
    )
}

/// A worker-side `thread/completed` (the §2 payload shape).
fn thread_completed(worker: SessionId) -> Event {
    event(
        worker,
        9,
        EventKind::ThreadCompleted,
        json!({
            "thread_id": uuid::Uuid::new_v4().to_string(),
            "status": "completed",
            "summary": {"role": "assistant", "content": "done"},
        }),
    )
}

// ---------------------------------------------------------------------------
// Gate 1 — a live Executor owns the follow-up; zero Router judge turns
// ---------------------------------------------------------------------------

/// A follow-up on a topic with one live subscriber admits to that
/// worker — the Router session never receives the event's row (a judge
/// turn requires one), never wakes on it, and no audit line names the
/// Router session for the event id.
#[tokio::test]
async fn live_executor_owns_the_follow_up_with_zero_router_judge_turns() {
    let _ = logs();
    let router = sid(1);
    let worker = sid(2);
    let inbox = RecordingInbox::new();
    let service = SubscriptionService::new(
        kian_superagent::ResidentBinding::new(router),
        rules(),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    );
    service.register_topic("pr-9", worker);

    let outcome = service
        .handle_event(ci_event("evt-live", "pr-9"))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RoutingOutcome::Routed {
            session: worker,
            deduplicated: false
        }
    );

    // Exactly one durable row, for the worker, keyed by the event id —
    // and no Router row or wake: the event cannot enter any Router
    // drain, so zero judge turns are spent on it.
    let admits = inbox.admits.lock().unwrap().clone();
    assert_eq!(admits, vec![(worker, Some("evt-live".into()))]);
    assert_eq!(*inbox.wakes.lock().unwrap(), vec![worker]);

    let text = log_text();
    // The admit line names the worker with the no-judge-turn code.
    assert_eq!(
        audit_lines(
            &text,
            &[
                "subscription audit: event admitted",
                &worker.0.to_string(),
                "evt-live",
                "reason=admitted_passthrough",
            ]
        )
        .len(),
        1,
        "expected the worker's admit line with reason=admitted_passthrough, got:\n{text}"
    );
    // Zero Router-side traces of the event: no audit line names both
    // the Router session and the event id (a judge-turn admission
    // would).
    assert!(
        audit_lines(&text, &[&router.0.to_string(), "evt-live"]).is_empty(),
        "the Router session must not appear in any audit line for evt-live:\n{text}"
    );
}

// ---------------------------------------------------------------------------
// Gate 2 — after the worker's thread/completed: release to fresh Eval
// ---------------------------------------------------------------------------

/// The same topic after the worker's `thread/completed`: the registry
/// rebuild projects the worker ended, the follow-up falls back to the
/// Router as a **release to fresh evaluation** — audited with
/// `reason=released_to_eval` and naming the released topic — and the
/// ended worker's terminal session receives no row.
#[tokio::test]
async fn ended_worker_releases_the_topic_to_fresh_eval() {
    let _ = logs();
    let router = sid(1);
    let worker = sid(2);
    let mut events = vec![thread_started(router, worker, "pr-9")];
    events.extend([thread_completed(worker)]);
    let registry = PassthroughRegistry::rebuild(&events);
    // The rebuild projection: the worker stays a historical subscriber
    // but is no longer live.
    assert_eq!(
        registry.resolve("pr-9"),
        std::collections::BTreeSet::from([worker])
    );
    assert!(registry.resolve_live("pr-9").is_empty());
    assert_eq!(registry.status(&worker), WorkerStatus::Ended);

    let inbox = RecordingInbox::new();
    let service = SubscriptionService::new(
        kian_superagent::ResidentBinding::new(router),
        rules(),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    );
    service.install_registry(registry);

    let outcome = service
        .handle_event(ci_event("evt-release", "pr-9"))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RoutingOutcome::Routed {
            session: router,
            deduplicated: false
        }
    );

    // One row, for the Router (the fresh evaluation); the ended
    // worker's terminal session never receives one.
    let admits = inbox.admits.lock().unwrap().clone();
    assert_eq!(admits, vec![(router, Some("evt-release".into()))]);

    let text = log_text();
    assert_eq!(
        audit_lines(
            &text,
            &[
                "subscription audit: topic released to fresh evaluation",
                "evt-release",
                "pr-9",
                "reason=released_to_eval",
            ]
        )
        .len(),
        1,
        "expected the release audit line naming the topic with reason=released_to_eval, got:\n{text}"
    );
    assert_eq!(
        IntakeReason::ReleasedToEval.code(),
        "released_to_eval",
        "the closed vocabulary's code spelling is the audit line's"
    );
}

// ---------------------------------------------------------------------------
// Gate 3 — duplicate-topic spawn refused at the tool
// ---------------------------------------------------------------------------

/// `spawn_worker` with a topic correlation a live worker already owns:
/// an honest tool error naming the owner session — and nothing happens
/// (no inbox row, no `thread/started`). Once the owner's stream ended,
/// the same spawn succeeds (fresh Eval) and the new worker takes the
/// topic; the next duplicate is refused naming the *new* owner.
#[tokio::test]
async fn duplicate_topic_spawn_refused_at_the_tool() {
    let _ = logs();
    let owner = sid(3);
    let registry = Arc::new(std::sync::RwLock::new({
        let mut r = PassthroughRegistry::new();
        r.register("pr-9", owner);
        r
    }));
    let (tools, inbox, port) = ownership_tools(Arc::clone(&registry), true);

    // Refused: the honest error names the owner and the rule.
    let err = tools
        .invoke(spawn_call(channel_task("evt-dup", "pr-9")))
        .await
        .unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("follow-up owned by live executor"),
        "the refusal names the rule, got: {text}"
    );
    assert!(
        text.contains(&owner.0.to_string()),
        "the refusal names the owner session, got: {text}"
    );
    assert!(
        text.contains("only stop"),
        "the refusal states the Router-may-only-stop rule, got: {text}"
    );
    assert!(
        matches!(err, ToolError::Execution(_)),
        "the refusal is an honest execution error the model sees"
    );

    // Spawns nothing: zero admits, zero thread/started.
    assert!(inbox.admits.lock().unwrap().is_empty());
    assert_eq!(port.count_kind(EventKind::ThreadStarted), 0);

    // The owner's stream ends: the same spawn is now the release to a
    // fresh evaluation — it succeeds, and the new worker is the live
    // owner the next duplicate is refused against.
    registry.write().unwrap().mark_ended(owner);
    let result = tools
        .invoke(spawn_call(channel_task("evt-dup", "pr-9")))
        .await
        .unwrap();
    assert!(!result.is_error);
    assert_eq!(port.count_kind(EventKind::ThreadStarted), 1);
    assert_eq!(inbox.admits.lock().unwrap().len(), 1);
    let new_worker = uuid::Uuid::parse_str(result.output["worker"].as_str().unwrap()).unwrap();
    assert_eq!(
        registry.read().unwrap().live_owner("pr-9"),
        Some(SessionId(new_worker)),
        "the spawned worker is registered as the topic's live executor"
    );

    let err = tools
        .invoke(spawn_call(channel_task("evt-dup-2", "pr-9")))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("follow-up owned by live executor"));
    assert!(err.to_string().contains(&new_worker.to_string()));
    assert_eq!(
        inbox.admits.lock().unwrap().len(),
        1,
        "still exactly one row"
    );
    assert_eq!(port.count_kind(EventKind::ThreadStarted), 1);
}

// ---------------------------------------------------------------------------
// Gate 4 — rebuild from a mixed live/ended stream equals the runtime map
// ---------------------------------------------------------------------------

/// The registry rebuild over a mixed live/ended stream equals the
/// runtime map built from `register` + `mark_ended` writes — the
/// projection is disposable by construction, and the equality pins the
/// two halves to the same facts.
#[test]
fn rebuild_from_mixed_stream_equals_the_runtime_map() {
    let router = sid(1);
    let live = sid(4);
    let done = sid(5);
    let mut events = vec![
        thread_started(router, live, "pr-9"),
        thread_started(router, done, "pr-9"),
        thread_started(router, done, "pr-10"),
    ];
    events.extend([thread_completed(done)]);

    let rebuilt = PassthroughRegistry::rebuild(&events);

    let mut runtime = PassthroughRegistry::new();
    runtime.register("pr-9", live);
    runtime.register("pr-9", done);
    runtime.register("pr-10", done);
    runtime.mark_ended(done);

    assert_eq!(rebuilt, runtime, "the projection is the same map");
    assert_eq!(
        rebuilt.resolve_live("pr-9"),
        std::collections::BTreeSet::from([live])
    );
    assert!(rebuilt.resolve_live("pr-10").is_empty());
    assert_eq!(rebuilt.live_owner("pr-9"), Some(live));
    assert_eq!(rebuilt.status(&done), WorkerStatus::Ended);
    assert_eq!(rebuilt.status(&live), WorkerStatus::Live);
}

// ---------------------------------------------------------------------------
// The config gate — off restores today's behavior
// ---------------------------------------------------------------------------

/// `superagent.followup_ownership: false`: an ended subscriber still
/// admits (the legacy behavior) and a duplicate-topic spawn goes
/// through ungated.
#[tokio::test]
async fn ownership_off_restores_the_pre_c59_behavior() {
    let _ = logs();
    let router = sid(1);
    let ended = sid(6);

    // Fan-out side: the ended subscriber admits again.
    let inbox = RecordingInbox::new();
    let service = SubscriptionService::new(
        kian_superagent::ResidentBinding::new(router),
        rules(),
        Arc::clone(&inbox) as Arc<dyn InboxWriter>,
    )
    .with_followup_ownership(false);
    service.register_topic("pr-9", ended);
    service.mark_worker_ended(ended);
    let outcome = service
        .handle_event(ci_event("evt-off", "pr-9"))
        .await
        .unwrap();
    assert_eq!(
        outcome,
        RoutingOutcome::Routed {
            session: ended,
            deduplicated: false
        }
    );

    // Tool side: the duplicate spawn goes through, ungated.
    let registry = Arc::new(std::sync::RwLock::new({
        let mut r = PassthroughRegistry::new();
        r.register("pr-9", ended);
        r
    }));
    let (tools, inbox, port) = ownership_tools(Arc::clone(&registry), false);
    let result = tools
        .invoke(spawn_call(channel_task("evt-off", "pr-9")))
        .await
        .unwrap();
    assert!(!result.is_error);
    assert_eq!(inbox.admits.lock().unwrap().len(), 1);
    assert_eq!(port.count_kind(EventKind::ThreadStarted), 1);
}
