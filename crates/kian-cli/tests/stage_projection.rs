//! Ticket stage projection acceptance.
//!
//! One scripted stream per arm of the first-match stage table (7 arms;
//! the `delivery-retry` arm via an injected fake `OutboxView`), the
//! first-match ordering, the legacy-report tolerance, the A2
//! export→import shape, replay stability, and the `kian tickets` CLI
//! (`run_tickets`) rendering text and JSON deterministically over a
//! fixture db (the tests/common fixture pattern).

use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, InputRow, SessionId};
use kian_core::seams::ChannelEvent;
use kian_store::{AppendGuard, EventStore};
use kian_superagent::{
    NoOutbox, OutboxView, PassthroughRegistry, ReportState, TicketStage, channel_input_payload,
    delegation_content, project_stage, thread_completed_payload, thread_report_content,
    thread_report_content_stated, thread_started_payload, worker_sessions,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------
// Scripted-stream helpers
// ---------------------------------------------------------------------

fn sid(n: u128) -> SessionId {
    SessionId(Uuid::from_u128(n))
}

fn tid(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn event(session: SessionId, seq: u64, kind: EventKind, bytes: Value) -> Event {
    Event {
        seq,
        session,
        epoch: Epoch(2),
        ts: 1_761_900_000_000 + seq,
        kind,
        bytes,
        version: SCHEMA_VERSION,
    }
}

fn topic_input(topic: &str, event_id: &str) -> Value {
    channel_input_payload(&ChannelEvent {
        channel: "mock-im".into(),
        event_id: event_id.into(),
        topic: Some(topic.into()),
        sender: None,
        group: None,
        payload: json!({"text": "a person's follow-up"}),
        ts_ms: 1_761_900_000_500,
    })
}

/// The honest spine of one worker stream: created → the delegation
/// wrapper input → worker-side `thread/started` lineage.
fn worker_base(worker: SessionId, thread: Uuid) -> Vec<Event> {
    let wrapper = delegation_content(thread, None, json!("the task"));
    let started = thread_started_payload(
        &kian_core::model::Thread {
            id: thread,
            parent_session: sid(0x100),
            pinned_version: 1,
        },
        worker,
        &json!({"role": "user", "content": wrapper}),
    );
    vec![
        event(worker, 1, EventKind::SessionCreated, Value::Null),
        event(
            worker,
            2,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": wrapper}),
        ),
        event(worker, 3, EventKind::ThreadStarted, started),
    ]
}

/// The worker is mid-drive: claimed, one turn, plain progress prose.
fn claimed(mut events: Vec<Event>) -> Vec<Event> {
    let worker = events[0].session;
    let seq = events.len() as u64 + 1;
    events.push(event(worker, seq, EventKind::DrainClaimed, json!({})));
    events.push(event(
        worker,
        seq + 1,
        EventKind::TurnStart,
        json!({"iteration": 0}),
    ));
    events
}

/// The worker's structured (stated) report as its assistant message.
fn stated_report(events: &mut Vec<Event>, state: ReportState) {
    let worker = events[0].session;
    let seq = events.len() as u64 + 1;
    let thread = tid(0xaaa);
    events.push(event(
        worker,
        seq,
        EventKind::AssistantMessage,
        json!({
            "content": thread_report_content_stated(
                thread,
                "progress",
                json!({"text": "asked the question"}),
                state,
            )
        }),
    ));
}

fn stage(events: &[Event]) -> TicketStage {
    project_stage(events, 0, &NoOutbox)
}

/// The fake outbox view: exactly one thread has a pending delivery.
struct PendingFor(Uuid);

impl OutboxView for PendingFor {
    fn has_pending_delivery(&self, thread_id: Uuid) -> bool {
        thread_id == self.0
    }
}

// ---------------------------------------------------------------------
// One scripted stream per arm
// ---------------------------------------------------------------------

/// Arm 1 — `delivery-retry`: only the outbox view can produce it, and
/// only for the ticket's own thread. With the stub view the arm is
/// unreachable (the honest v1 state until wires a real view).
#[test]
fn arm1_delivery_retry_comes_from_the_outbox_view_only() {
    let worker = sid(1);
    let thread = tid(0xaaa);
    let events = claimed(worker_base(worker, thread));

    assert_eq!(
        project_stage(&events, 0, &PendingFor(thread)),
        TicketStage::DeliveryRetry
    );
    // A different ticket's pending delivery does not project onto this
    // stream, and the stub view never arms it.
    assert_eq!(
        project_stage(&events, 0, &PendingFor(tid(0xbbb))),
        TicketStage::Working
    );
    assert_eq!(stage(&events), TicketStage::Working);
}

/// Arm 2 — `awaiting-answer`: latest structured report `waiting`, the
/// thread still outstanding, and no input admitted since the report.
/// A follow-up input on the topic answers it (the release rule) and
/// the ticket moves on (here: queued, waiting for the next claim).
#[test]
fn arm2_awaiting_answer_and_its_release_rule() {
    let worker = sid(2);
    let thread = tid(0xaaa);
    let mut events = claimed(worker_base(worker, thread));
    stated_report(&mut events, ReportState::Waiting);
    assert_eq!(stage(&events), TicketStage::AwaitingAnswer);

    // The person answered: an input admitted after the waiting report.
    let seq = events.len() as u64 + 1;
    events.push(event(
        worker,
        seq,
        EventKind::InputAdmitted,
        json!({"role": "user", "content": topic_input("pr-9", "evt-answer")}),
    ));
    assert_eq!(stage(&events), TicketStage::Queued);
}

/// Arm 3 — `awaiting-review`: latest structured report `ready`.
#[test]
fn arm3_awaiting_review() {
    let worker = sid(3);
    let thread = tid(0xaaa);
    let mut events = claimed(worker_base(worker, thread));
    stated_report(&mut events, ReportState::Ready);
    assert_eq!(stage(&events), TicketStage::AwaitingReview);
}

/// Arm 4 — `blocked-failed`: a `thread/completed{failed}` (or
/// `session/ended{failed}`) nobody has released. A message after the
/// failure releases the ticket (003's release rule).
#[test]
fn arm4_blocked_failed_and_its_release_rule() {
    let worker = sid(4);
    let thread = tid(0xaaa);
    let mut events = claimed(worker_base(worker, thread));
    let seq = events.len() as u64 + 1;
    events.push(event(
        worker,
        seq,
        EventKind::ThreadCompleted,
        thread_completed_payload(thread, "failed", json!({"text": "boom"})),
    ));
    assert_eq!(stage(&events), TicketStage::BlockedFailed);

    // The session-ended-failure variant arms the same stage.
    let worker = sid(5);
    let mut events = claimed(worker_base(worker, thread));
    let seq = events.len() as u64 + 1;
    events.push(event(
        worker,
        seq,
        EventKind::SessionEnded {
            reason: EndReason::Failed,
        },
        json!({"reason": "failed"}),
    ));
    assert_eq!(stage(&events), TicketStage::BlockedFailed);

    // A human message after the failure releases it (→ queued here).
    let seq = events.len() as u64 + 1;
    events.push(event(
        worker,
        seq,
        EventKind::InputAdmitted,
        json!({"role": "user", "content": topic_input("pr-9", "evt-release")}),
    ));
    assert_eq!(stage(&events), TicketStage::Queued);
}

/// Arm 5 — `queued`: input admitted, no subsequent `drain/claimed`
/// (stream half), or a pending inbox row (read-side half).
#[test]
fn arm5_queued_from_stream_shape_and_pending_inbox() {
    let worker = sid(6);
    let thread = tid(0xaaa);
    // Stream half: the wrapper input is admitted, never claimed.
    let events = worker_base(worker, thread);
    assert_eq!(stage(&events), TicketStage::Queued);

    // Read-side half: a pending inbox row on an otherwise claimed
    // stream (the input arrived after the last claim).
    let events = claimed(events);
    assert_eq!(project_stage(&events, 1, &NoOutbox), TicketStage::Queued);
}

/// Arm 6 — `idle`: thread completed successfully, or the session is
/// terminal-completed. The honest v1 deviation: resumable only via
/// fresh Eval, never same-Mandate resume.
#[test]
fn arm6_idle_on_success_and_terminal_completion() {
    let worker = sid(7);
    let thread = tid(0xaaa);
    let mut events = claimed(worker_base(worker, thread));
    let seq = events.len() as u64 + 1;
    events.push(event(
        worker,
        seq,
        EventKind::ThreadCompleted,
        thread_completed_payload(thread, "completed", json!({"text": "done"})),
    ));
    assert_eq!(stage(&events), TicketStage::Idle);

    // Terminal-completed (the freeze) with the thread still
    // outstanding (e.g. The session settled without a completion —
    // interrupted workers end this way; terminal beats outstanding).
    let worker = sid(8);
    let events = {
        let mut events = claimed(worker_base(worker, thread));
        let seq = events.len() as u64 + 1;
        events.push(event(
            worker,
            seq,
            EventKind::SessionEnded {
                reason: EndReason::Completed,
            },
            json!({"reason": "completed"}),
        ));
        events
    };
    assert_eq!(stage(&events), TicketStage::Idle);
}

/// Arm 7 — `working`: everything else (claimed, mid-turn, no stated
/// report). A plain non-worker session also projects `working` — the
/// projection never guesses a ticket where none was spawned.
#[test]
fn arm7_working_is_the_default() {
    let worker = sid(9);
    let thread = tid(0xaaa);
    let events = claimed(worker_base(worker, thread));
    assert_eq!(stage(&events), TicketStage::Working);

    // Plain prose (a progress report with no structured state) never
    // arms the person stages — 001's rule.
    let mut events = events;
    stated_report_legacy(&mut events);
    assert_eq!(stage(&events), TicketStage::Working);

    // A non-worker stream running a turn: no lineage, no wrapper —
    // never a ticket, never a person stage.
    let plain = vec![
        event(sid(0x10), 1, EventKind::SessionCreated, Value::Null),
        event(
            sid(0x10),
            2,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "hello"}),
        ),
        event(sid(0x10), 3, EventKind::DrainClaimed, json!({})),
        event(sid(0x10), 4, EventKind::TurnStart, json!({"iteration": 0})),
    ];
    assert_eq!(stage(&plain), TicketStage::Working);
}

/// A legacy assistant-message report (no `state` key) parses with
/// `state: None` and satisfies no arm by itself.
fn stated_report_legacy(events: &mut Vec<Event>) {
    let worker = events[0].session;
    let seq = events.len() as u64 + 1;
    events.push(event(
        worker,
        seq,
        EventKind::AssistantMessage,
        json!({
            "content": thread_report_content(tid(0xaaa), "progress", json!({"text": "still on it"}))
        }),
    ));
    let parsed = kian_superagent::parse_thread_report(
        events
            .last()
            .unwrap()
            .bytes
            .get("content")
            .expect("content"),
    )
    .expect("legacy report parses");
    assert_eq!(parsed.state, None);
}

// ---------------------------------------------------------------------
// First-match ordering, legacy tolerance, A2 shape, replay stability
// ---------------------------------------------------------------------

/// First-match ordering: a stream satisfying arms 2 and 4 (a waiting
/// report with the thread outstanding, then `session/ended{failed}`)
/// projects arm 2 — the table is read top to bottom.
#[test]
fn first_match_awaiting_answer_wins_over_blocked_failed() {
    let worker = sid(0x11);
    let thread = tid(0xaaa);
    let mut events = claimed(worker_base(worker, thread));
    stated_report(&mut events, ReportState::Waiting);
    let seq = events.len() as u64 + 1;
    events.push(event(
        worker,
        seq,
        EventKind::SessionEnded {
            reason: EndReason::Failed,
        },
        json!({"reason": "failed"}),
    ));
    assert_eq!(stage(&events), TicketStage::AwaitingAnswer);
}

/// Arm 1b — `awaiting-confirmation`: an unanswered `ask` on the
/// worker's stream (CONFIRMATION-MODEL phase 2). It outranks the
/// structured `waiting` report: the permission wait is the specific
/// fact, and the answer is a control row on the same session.
#[test]
fn arm1b_awaiting_confirmation_comes_from_the_pending_ask() {
    let worker = sid(1);
    let thread = tid(0xaaa);
    let mut events = claimed(worker_base(worker, thread));
    stated_report(&mut events, ReportState::Waiting);
    assert_eq!(stage(&events), TicketStage::AwaitingAnswer);

    let seq = events.len() as u64 + 1;
    events.push(event(
        worker,
        seq,
        EventKind::AssistantMessage,
        json!({"role": "assistant",
        "content": {"text": "", "tool_calls": [{
            "id": "call-ask", "name": "exec", "arguments": {"argv": ["echo", "x"]}
        }]}}),
    ));
    events.push(event(
        worker,
        seq + 1,
        EventKind::ToolCall,
        json!({"role": "assistant",
               "content": {"id": "call-ask", "name": "exec",
                           "arguments": {"argv": ["echo", "x"]}},
               "permission": {"evaluated": "ask", "policy": "configured",
                              "matched": "exec"}}),
    ));
    assert_eq!(stage(&events), TicketStage::AwaitingConfirmation);

    // The answer (a durable result) releases the confirmation arm; the
    // earlier structured waiting report is the next specific fact.
    events.push(event(
        worker,
        seq + 2,
        EventKind::ToolResult,
        json!({"role": "tool",
               "content": {"call_id": "call-ask", "output": {"ok": true},
                           "is_error": false}}),
    ));
    assert_eq!(stage(&events), TicketStage::AwaitingAnswer);
}

/// The serde names and display text are the stable wire contract of
/// `kian tickets --json`.
#[test]
fn stage_names_are_stable() {
    let all = [
        (TicketStage::DeliveryRetry, "delivery-retry"),
        (TicketStage::AwaitingConfirmation, "awaiting-confirmation"),
        (TicketStage::AwaitingAnswer, "awaiting-answer"),
        (TicketStage::AwaitingReview, "awaiting-review"),
        (TicketStage::BlockedFailed, "blocked-failed"),
        (TicketStage::Queued, "queued"),
        (TicketStage::Idle, "idle"),
        (TicketStage::Working, "working"),
    ];
    for (variant, name) in all {
        assert_eq!(variant.as_str(), name);
        assert_eq!(variant.to_string(), name);
        assert_eq!(serde_json::to_value(variant).unwrap(), json!(name));
        let back: TicketStage = serde_json::from_value(json!(name)).expect("round-trips");
        assert_eq!(back, variant);
    }
}

/// The A2 shape: the same stream exported (one JSON envelope per
/// event) and re-imported projects the identical stage — the stage is
/// a pure function of the persisted bytes, nothing else.
#[test]
fn reexported_stream_projects_the_identical_stage() {
    let worker = sid(0x12);
    let thread = tid(0xaaa);
    let streams = vec![
        claimed(worker_base(worker, thread)),
        {
            let mut events = claimed(worker_base(sid(0x13), tid(0xbbb)));
            stated_report(&mut events, ReportState::Ready);
            events
        },
        {
            let mut events = worker_base(sid(0x14), tid(0xccc));
            let seq = events.len() as u64 + 1;
            events.push(event(
                sid(0x14),
                seq,
                EventKind::ThreadCompleted,
                thread_completed_payload(tid(0xccc), "failed", json!({"text": "boom"})),
            ));
            events
        },
    ];
    for events in streams {
        let jsonl: Vec<String> = events
            .iter()
            .map(|e| serde_json::to_string(e).expect("serialize event"))
            .collect();
        let reimported: Vec<Event> = jsonl
            .iter()
            .map(|line| serde_json::from_str(line).expect("envelope round-trips"))
            .collect();
        assert_eq!(stage(&events), stage(&reimported));
        assert_eq!(
            stage(&events),
            project_stage(&reimported, 0, &NoOutbox),
            "the projection reads only the persisted bytes"
        );
    }
}

/// Replay-stable: projecting the same stream twice (and the reimported
/// copy) yields the same stage — a disposable projection in both
/// directions (drop it, replay, same answer).
#[test]
fn stage_is_replay_stable() {
    let worker = sid(0x15);
    let thread = tid(0xaaa);
    let mut events = claimed(worker_base(worker, thread));
    stated_report(&mut events, ReportState::Waiting);
    let first = stage(&events);
    let second = stage(&events);
    let cloned = stage(&events.clone());
    assert_eq!(first, second);
    assert_eq!(first, cloned);
    assert_eq!(first, TicketStage::AwaitingAnswer);
}

// ---------------------------------------------------------------------
// run_tickets over a fixture db (the CLI read path)
// ---------------------------------------------------------------------

fn temp_db_path(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("kian-c56-{tag}-{}.sqlite", Uuid::new_v4()))
}

/// A one-thread Router + one worker pair, appended as a lawful stream.
async fn append_pair(store: &EventStore, router: SessionId, worker: SessionId, thread: Uuid) {
    let topic_task = json!({
        "role": "user",
        "content": channel_input_payload(&ChannelEvent {
            channel: "mock-im".into(),
            event_id: format!("evt-spawn-{thread}"),
            topic: Some("pr-9".into()),
            sender: None,
            group: None,
            payload: json!({"text": "triage this"}),
            ts_ms: 1_761_900_000_000,
        }),
    });
    let started = thread_started_payload(
        &kian_core::model::Thread {
            id: thread,
            parent_session: router,
            pinned_version: 1,
        },
        worker,
        &topic_task,
    );
    let router_events = vec![
        (EventKind::SessionCreated, Value::Null),
        (EventKind::ThreadStarted, started),
    ];
    for (i, (kind, bytes)) in router_events.into_iter().enumerate() {
        store
            .append(
                router,
                event(router, (i + 1) as u64, kind, bytes),
                AppendGuard {
                    epoch: Epoch(2),
                    expected_seq: (i + 1) as u64,
                },
            )
            .await
            .expect("append router event");
    }
}

/// Appends a worker stream from the event kinds/payloads, seq 1..=n
/// under epoch 0.
async fn append_worker(
    store: &EventStore,
    worker: SessionId,
    thread: Uuid,
    tail: Vec<(EventKind, Value)>,
) {
    let wrapper = delegation_content(thread, None, json!("the task"));
    let started = thread_started_payload(
        &kian_core::model::Thread {
            id: thread,
            parent_session: sid(0x100),
            pinned_version: 1,
        },
        worker,
        &json!({"role": "user", "content": wrapper}),
    );
    let spine = vec![
        (EventKind::SessionCreated, Value::Null),
        (
            EventKind::InputAdmitted,
            json!({"role": "user", "content": wrapper}),
        ),
        (EventKind::ThreadStarted, started),
    ];
    let all: Vec<(EventKind, Value)> = spine.into_iter().chain(tail).collect();
    for (i, (kind, bytes)) in all.into_iter().enumerate() {
        store
            .append(
                worker,
                event(worker, (i + 1) as u64, kind, bytes),
                AppendGuard {
                    epoch: Epoch(2),
                    expected_seq: (i + 1) as u64,
                },
            )
            .await
            .expect("append worker event");
    }
}

/// `run_tickets` over a fixture db renders text and JSON
/// deterministically: same db, same rows, same order (session uuid
/// ascending), same stages — only the honest `now_ms`/`age_ms` fields
/// move with the wall clock.
#[tokio::test]
async fn run_tickets_renders_text_and_json_deterministically() {
    let path = temp_db_path("board");
    let store = EventStore::open(&path).await.expect("fixture db");

    // Four tickets, one per interesting stage. Worker session uuids in
    // ascending order (0x201..0x204) fix the expected board order.
    let working = sid(0x201);
    let idle = sid(0x202);
    let blocked = sid(0x203);
    let queued = sid(0x204);

    // One Router per pair keeps each fixture stream appendable in one
    // pass (a real Router carries all four `thread/started` events on
    // one stream; discovery only reads the events, not their shape).
    append_pair(&store, sid(0x100), working, tid(0xa01)).await;
    append_worker(
        &store,
        working,
        tid(0xa01),
        vec![
            (EventKind::DrainClaimed, json!({})),
            (EventKind::TurnStart, json!({"iteration": 0})),
        ],
    )
    .await;

    append_pair(&store, sid(0x101), idle, tid(0xa02)).await;
    append_worker(
        &store,
        idle,
        tid(0xa02),
        vec![
            (EventKind::DrainClaimed, json!({})),
            (
                EventKind::ThreadCompleted,
                thread_completed_payload(tid(0xa02), "completed", json!({"text": "done"})),
            ),
        ],
    )
    .await;

    append_pair(&store, sid(0x102), blocked, tid(0xa03)).await;
    append_worker(
        &store,
        blocked,
        tid(0xa03),
        vec![
            (EventKind::DrainClaimed, json!({})),
            (
                EventKind::ThreadCompleted,
                thread_completed_payload(tid(0xa03), "failed", json!({"text": "boom"})),
            ),
        ],
    )
    .await;

    append_pair(&store, sid(0x103), queued, tid(0xa04)).await;
    append_worker(&store, queued, tid(0xa04), vec![]).await;
    // One pending inbox row (the queued arm's read-side half) — the
    // row exists before the board reads it (invariant 1 shape).
    store
        .admit(InputRow {
            id: Uuid::new_v4(),
            session: queued,
            payload: json!({"text": "more work"}),
            admitted_at: 1_761_900_000_000,
            eligible_at: 1_761_900_000_000,
            idempotency_key: Some("c56-queued-extra".into()),
        })
        .await
        .expect("admit pending row");
    store.close().await;

    let no_config = std::path::Path::new("kian.yaml-does-not-exist.yaml");

    // -- JSON: two runs agree on everything but the wall clock ------
    let render = |json: bool| {
        let path = path.clone();
        async move {
            kian_cli::commands::run_tickets(no_config, Some(&path), json)
                .await
                .expect("run_tickets")
        }
    };
    let normalize = |doc: Value| {
        let mut doc = doc;
        doc.as_object_mut().unwrap().remove("now_ms");
        for ticket in doc["tickets"].as_array_mut().unwrap() {
            ticket.as_object_mut().unwrap().remove("age_ms");
        }
        doc
    };
    let first = normalize(serde_json::from_str(&render(true).await).expect("json"));
    let second = normalize(serde_json::from_str(&render(true).await).expect("json"));
    assert_eq!(first, second, "two runs render the identical board");

    let tickets = first["tickets"].as_array().expect("tickets");
    assert_eq!(tickets.len(), 4, "one row per worker session");
    let by_session: Vec<String> = tickets
        .iter()
        .map(|t| t["session"].as_str().expect("session").to_string())
        .collect();
    let mut sorted = by_session.clone();
    sorted.sort();
    assert_eq!(by_session, sorted, "deterministic order: session uuid");

    let expected: Vec<(String, &str, &str)> = vec![
        (working.0.to_string(), "working", "outstanding"),
        (idle.0.to_string(), "idle", "completed"),
        (blocked.0.to_string(), "blocked-failed", "failed"),
        (queued.0.to_string(), "queued", "outstanding"),
    ];
    for (ticket, (session, stage_name, status)) in tickets.iter().zip(expected) {
        assert_eq!(ticket["session"].as_str().unwrap(), session);
        assert_eq!(ticket["stage"].as_str().unwrap(), stage_name);
        assert_eq!(ticket["thread_status"].as_str().unwrap(), status);
    }
    // The topic binding comes from the same registry projection the
    // runtime rebuilds (spawn task wrapper names `pr-9`).
    assert_eq!(tickets[0]["topic"].as_str().unwrap(), "pr-9");
    // The pending row is visible on the queued ticket.
    assert_eq!(tickets[3]["pending_inbox"].as_u64().unwrap(), 1);

    // -- text: same determinism, modulo the AGE column ---------------
    let normalize_age = |text: String| -> String {
        text.lines()
            .map(|line| {
                let mut fields: Vec<String> = line.split_whitespace().map(str::to_string).collect();
                if fields.len() >= 7 {
                    let n = fields.len();
                    fields[n - 1] = "<age>".into();
                }
                fields.join(" ")
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let text1 = normalize_age(render(false).await);
    let text2 = normalize_age(render(false).await);
    assert_eq!(text1, text2, "two text runs render identically");
    assert!(text1.contains("working"), "{text1}");
    assert!(text1.contains("blocked-failed"), "{text1}");
    assert!(text1.contains("pr-9"), "{text1}");

    // The board's stage agrees with the pure projection over each
    // worker's own stream (the CLI adds no logic of its own).
    let store = EventStore::open_read_only(&path).await.expect("reopen ro");
    let mut all = Vec::new();
    for session in kian_cli::cache_metrics::sessions_with_events(&store)
        .await
        .expect("list sessions")
    {
        all.extend(store.rebuild(session).await.expect("rebuild"));
    }
    assert_eq!(
        worker_sessions(&all),
        vec![working, idle, blocked, queued],
        "worker discovery via thread/started across streams"
    );
    let registry = PassthroughRegistry::rebuild(&all);
    assert_eq!(registry.resolve("pr-9").len(), 4);
    store.close().await;

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

/// An empty store (no lineage) renders the honest empty board in both
/// modes — never an error, never a guess.
#[tokio::test]
async fn run_tickets_over_an_empty_store_is_an_honest_empty_board() {
    let path = temp_db_path("empty");
    let store = EventStore::open(&path).await.expect("empty db");
    store.close().await;
    let no_config = std::path::Path::new("kian.yaml-does-not-exist.yaml");

    let text = kian_cli::commands::run_tickets(no_config, Some(&path), false)
        .await
        .expect("text board");
    assert!(text.contains("no worker sessions"), "{text}");

    let json_out = kian_cli::commands::run_tickets(no_config, Some(&path), true)
        .await
        .expect("json board");
    let doc: Value = serde_json::from_str(&json_out).expect("json");
    assert_eq!(doc["tickets"].as_array().unwrap().len(), 0);

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

/// (run2): the live board negative, replayed as a
/// fixture — a parked `deliver_structured` whose ticket key is the
/// freeform correlation the model passed (the triggering event id, no
/// thread alias match), spawned through the string-form wrapper (no
/// registry binding — the live gap), with the park error
/// wrapped in `ToolError::Execution`'s Display. The board must still
/// surface the parked line and the `delivery-retry` stage, attributed
/// by the conversation both sides derive from the worker's own stream.
#[tokio::test]
async fn run_tickets_surfaces_the_live_parked_delivery_by_conversation() {
    let path = temp_db_path("kian76");
    let store = EventStore::open(&path).await.expect("fixture db");
    let router = sid(0x110);
    let worker = sid(0x205);
    let thread = tid(0xa05);

    // The live wrapper shape: the channel-event wrapper serialized as
    // a STRING (what the model passes as `task`), topic = the
    // thread key, correlation = the freeform event id.
    let wrapper_string = channel_input_payload(&ChannelEvent {
        channel: "issue-github".into(),
        event_id: "poll:issues:flyer103/kian-runtime#6:2026-11-03T21:31:04Z".into(),
        topic: Some("issue-github:flyer103/kian-runtime#6".into()),
        sender: None,
        group: Some("flyer103/kian-runtime".into()),
        payload: json!({"text": "triage this issue"}),
        ts_ms: 1_761_900_000_000,
    })
    .to_string();
    let target =
        json!({"kind":"issue_comment","number":6,"owner":"flyer103","repo":"kian-runtime"});
    let conversation = format!("issue-github:{}", serde_json::to_string(&target).unwrap());

    // Router stream: the spawn lineage (task = the stringified
    // wrapper — binds nothing in the registry), the delivery intent
    // (freeform correlation), and the wrapped park result.
    let router_started = thread_started_payload(
        &kian_core::model::Thread {
            id: thread,
            parent_session: router,
            pinned_version: 1,
        },
        worker,
        &json!({"role": "user", "content": {
            "correlation": "poll:issues:flyer103/kian-runtime#6:2026-11-03T21:31:04Z",
            "task": wrapper_string}}),
    );
    let router_events = vec![
        (EventKind::SessionCreated, Value::Null),
        (EventKind::ThreadStarted, router_started),
        (
            EventKind::ToolCall,
            json!({"role": "assistant", "content": {"id": "call-6", "name": "deliver_structured",
                "arguments": {"channel": "issue-github", "target": target,
                              "payload": {"background": "b", "changes": "c", "verification": "v"},
                              "correlation": "poll:issues:flyer103/kian-runtime#6:2026-11-03T21:31:04Z"}}}),
        ),
        (
            EventKind::ToolResult,
            json!({"role": "tool", "content": {"call_id": "call-6", "is_error": true,
                "output": {"error": format!(
                    "tool execution failed: outbox park: delivery to {conversation} parked after 6 \
                     attempts: delivery failed: github send rejected: HTTP 502 — no further sends; \
                     the ticket shows delivery-retry until released")}}}),
        ),
    ];

    // Worker stream: the live admitted input (correlation + the
    // stringified wrapper), worker-side lineage, and an in-progress
    // drain (the stage without the outbox arm would be `working`).
    let worker_started = thread_started_payload(
        &kian_core::model::Thread {
            id: thread,
            parent_session: router,
            pinned_version: 1,
        },
        worker,
        &json!({"role": "user", "content": {
            "correlation": "poll:issues:flyer103/kian-runtime#6:2026-11-03T21:31:04Z",
            "task": wrapper_string}}),
    );
    let worker_events = vec![
        (EventKind::SessionCreated, Value::Null),
        (
            EventKind::InputAdmitted,
            json!({"role": "user", "content": {
                "correlation": "poll:issues:flyer103/kian-runtime#6:2026-11-03T21:31:04Z",
                "task": wrapper_string}}),
        ),
        (EventKind::ThreadStarted, worker_started),
        (EventKind::DrainClaimed, json!({})),
        (EventKind::TurnStart, json!({"iteration": 0})),
    ];

    for (session, events) in [(router, router_events), (worker, worker_events)] {
        for (i, (kind, bytes)) in events.into_iter().enumerate() {
            store
                .append(
                    session,
                    event(session, (i + 1) as u64, kind, bytes),
                    AppendGuard {
                        epoch: Epoch(2),
                        expected_seq: (i + 1) as u64,
                    },
                )
                .await
                .expect("append fixture event");
        }
    }
    store.close().await;

    let no_config = std::path::Path::new("kian.yaml-does-not-exist.yaml");
    let json_out = kian_cli::commands::run_tickets(no_config, Some(&path), true)
        .await
        .expect("json board");
    let doc: Value = serde_json::from_str(&json_out).expect("json");
    let tickets = doc["tickets"].as_array().expect("tickets");
    assert_eq!(tickets.len(), 1, "{tickets:?}");
    let row = &tickets[0];
    assert_eq!(row["session"], worker.0.to_string());
    // The live negative, closed: the stage lights `delivery-retry` …
    assert_eq!(row["stage"], json!("delivery-retry"), "{row}");
    // … and the parked line names the conversation, attributed from
    // the worker's own stream (no alias matched — the correlation is
    // a freeform event id).
    let parked = row["parked"].as_array().expect("parked array");
    assert_eq!(parked.len(), 1, "{row}");
    assert!(
        parked[0].as_str().unwrap().contains(&conversation),
        "parked line `{}` must name `{conversation}`",
        parked[0]
    );

    let text = kian_cli::commands::run_tickets(no_config, Some(&path), false)
        .await
        .expect("text board");
    assert!(text.contains("delivery-retry"), "{text}");
    assert!(
        text.contains("parked:") && text.contains(&conversation),
        "the text board names the parked delivery:\n{text}"
    );

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}
