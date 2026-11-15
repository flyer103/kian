//! Integration tests: the child inactivity bound's pure planner (R15 C6
//! — `design/MULTIAGENT-MODEL.md` §5.3:587, plan pin P5).
//!
//! The planner is exercised over hand-built committed streams served
//! through a scripted [`WorkerStreamsPort`] — no store, no engine. The
//! tick and its engine-level idempotency live in `kian-cli`
//! (`multiagent_tick.rs`); the `stalled` derivation cross-check with
//! C1's `derive_outcome` runs in both places.

use std::collections::BTreeMap;

use futures::future::BoxFuture;
use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::{Epoch, SessionId};
use kian_superagent::{
    ChildTick, MultiagentLimits, TaskOutcome, WorkerStreamsError, WorkerStreamsPort,
    derive_outcome, due_interrupts,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// A scripted port over fixed streams (the `SingleWorkerStreams` /
/// `AllStreams` precedent in `worker.rs`'s tests). Sessions absent from
/// the map read as errors — the honest unreadable stream.
struct MapPort {
    streams: BTreeMap<SessionId, Result<Vec<Event>, String>>,
}

impl MapPort {
    fn new() -> Self {
        Self {
            streams: BTreeMap::new(),
        }
    }

    fn add(&mut self, session: SessionId, events: Vec<Event>) -> &mut Self {
        self.streams.insert(session, Ok(events));
        self
    }

    fn fail(&mut self, session: SessionId, error: &str) -> &mut Self {
        self.streams.insert(session, Err(error.to_string()));
        self
    }
}

impl WorkerStreamsPort for MapPort {
    fn read_stream(
        &self,
        session: SessionId,
    ) -> BoxFuture<'static, Result<Vec<Event>, WorkerStreamsError>> {
        match self.streams.get(&session) {
            Some(Ok(events)) => {
                let events = events.clone();
                Box::pin(std::future::ready(Ok(events)))
            }
            Some(Err(e)) => {
                let e = e.clone();
                Box::pin(std::future::ready(Err(WorkerStreamsError(e))))
            }
            None => Box::pin(std::future::ready(Err(WorkerStreamsError(format!(
                "no stream for session {}",
                session.0
            ))))),
        }
    }
}

fn ev(seq: u64, session: SessionId, ts: u64, kind: EventKind, bytes: Value) -> Event {
    Event {
        seq,
        session,
        epoch: Epoch(1),
        ts,
        kind,
        bytes,
        version: 0,
    }
}

/// A root stream whose `thread/started` lineages name `children`.
fn root_stream(root: SessionId, children: &[(Uuid, SessionId)]) -> Vec<Event> {
    let mut events = vec![ev(0, root, 0, EventKind::SessionCreated, json!({}))];
    for (i, (thread_id, worker)) in children.iter().enumerate() {
        events.push(ev(
            u64::try_from(i).unwrap() + 1,
            root,
            0,
            EventKind::ThreadStarted,
            json!({
                "thread_id": thread_id.to_string(),
                "parent": root.0.to_string(),
                "worker": worker.0.to_string(),
                "pinned_version": "v0",
                "task": {"role": "user", "content": "delegate"},
            }),
        ));
    }
    events
}

/// A live child stream whose last committed event landed at `last_ts`.
fn live_child(child: SessionId, last_ts: u64) -> Vec<Event> {
    vec![
        ev(0, child, 0, EventKind::SessionCreated, json!({})),
        ev(
            1,
            child,
            last_ts,
            EventKind::InputAdmitted,
            json!({"content": "task"}),
        ),
    ]
}

const T0: u64 = 1_700_000_000_000;

fn limits(timeout_ms: u64, max_depth: u32) -> MultiagentLimits {
    MultiagentLimits {
        max_children: 4,
        max_depth,
        child_timeout_ms: timeout_ms,
    }
}

fn tick_of(plan: &[ChildTick], child: SessionId) -> Option<&ChildTick> {
    plan.iter().find(|t| t.child == child)
}

// ---------------------------------------------------------------------------
// The planner
// ---------------------------------------------------------------------------

/// `last_event_ms + timeout <= now ⇒ due` (§5.3:587).
#[tokio::test]
async fn a_silent_live_child_is_due_after_the_inactivity_bound() {
    let root = SessionId::new();
    let child = SessionId::new();
    let thread_id = Uuid::new_v4();
    let mut port = MapPort::new();
    port.add(root, root_stream(root, &[(thread_id, child)]))
        .add(child, live_child(child, T0));

    // Exactly at the bound: due.
    let plan = due_interrupts(root, &port, T0 + 1_000, &limits(1_000, 1), 0)
        .await
        .unwrap();
    assert_eq!(plan.len(), 1);
    assert_eq!(
        plan[0],
        ChildTick {
            child,
            parent: root,
            thread_id,
            last_event_ms: T0,
            due: true,
        }
    );

    // One ms before the bound: not yet due.
    let plan = due_interrupts(root, &port, T0 + 999, &limits(1_000, 1), 0)
        .await
        .unwrap();
    assert!(!tick_of(&plan, child).unwrap().due);
}

/// The bound is inactivity, not total runtime (§5.3:587): a child that
/// appended recently is not due however long it has run.
#[tokio::test]
async fn a_child_that_appended_recently_is_not_due() {
    let root = SessionId::new();
    let child = SessionId::new();
    let mut port = MapPort::new();
    port.add(root, root_stream(root, &[(Uuid::new_v4(), child)]))
        .add(child, live_child(child, T0 + 3_600_000));

    let plan = due_interrupts(root, &port, T0 + 3_600_000 + 999, &limits(1_000, 1), 0)
        .await
        .unwrap();
    assert!(!tick_of(&plan, child).unwrap().due);
}

/// `thread/completed{completed}` and `session/ended{cancelled}` are not
/// due — a released worker is never interrupted.
#[tokio::test]
async fn a_released_child_is_never_interrupted() {
    let root = SessionId::new();
    let completed = SessionId::new();
    let cancelled = SessionId::new();
    let mut completed_stream = live_child(completed, 0);
    completed_stream.push(ev(
        2,
        completed,
        0,
        EventKind::ThreadCompleted,
        json!({"status": "completed"}),
    ));
    let mut cancelled_stream = live_child(cancelled, 0);
    cancelled_stream.push(ev(
        2,
        cancelled,
        0,
        EventKind::SessionEnded {
            reason: EndReason::Cancelled,
        },
        json!({}),
    ));
    let mut port = MapPort::new();
    port.add(
        root,
        root_stream(
            root,
            &[(Uuid::new_v4(), completed), (Uuid::new_v4(), cancelled)],
        ),
    )
    .add(completed, completed_stream)
    .add(cancelled, cancelled_stream);

    let plan = due_interrupts(root, &port, T0, &limits(1_000, 1), 0)
        .await
        .unwrap();
    assert_eq!(plan.len(), 2);
    for tick in &plan {
        assert!(!tick.due, "a released child is never due: {tick:?}");
    }
}

/// `ThreadLiveness::NeverStarted` (a lineage-only reservation) is
/// released by the queue timeout (DESIGN §4.1/ADR-020), not by us.
#[tokio::test]
async fn a_never_started_reservation_is_not_due() {
    let root = SessionId::new();
    let reservation = SessionId::new();
    // Worker-side lineage only: no session/created, no input/admitted.
    let never_started = vec![ev(
        0,
        reservation,
        0,
        EventKind::ThreadStarted,
        json!({
            "thread_id": Uuid::new_v4().to_string(),
            "parent": root.0.to_string(),
            "worker": reservation.0.to_string(),
        }),
    )];
    let mut port = MapPort::new();
    port.add(root, root_stream(root, &[(Uuid::new_v4(), reservation)]))
        .add(reservation, never_started);

    let plan = due_interrupts(root, &port, T0, &limits(1_000, 1), 0)
        .await
        .unwrap();
    assert_eq!(plan.len(), 1);
    assert!(!plan[0].due, "a never-started reservation is never due");
}

/// An unreadable child stream produces no interrupt — the honest
/// refusal (plan §0.3 item 2: unknown is not stalled).
#[tokio::test]
async fn an_unreadable_child_stream_produces_no_interrupt() {
    let root = SessionId::new();
    let readable = SessionId::new();
    let unreadable = SessionId::new();
    let mut port = MapPort::new();
    port.add(
        root,
        root_stream(
            root,
            &[(Uuid::new_v4(), readable), (Uuid::new_v4(), unreadable)],
        ),
    )
    .add(readable, live_child(readable, T0))
    .fail(unreadable, "sqlite: database is locked");

    let plan = due_interrupts(root, &port, T0 + 5_000, &limits(1_000, 1), 0)
        .await
        .unwrap();
    // The readable silent child is due; the unreadable one contributes
    // no entry at all — not a not-due entry, none.
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].child, readable);
    assert!(plan[0].due);

    // An unreadable ROOT stream is the one honest total failure.
    let err = due_interrupts(SessionId::new(), &port, T0, &limits(1_000, 1), 0)
        .await
        .unwrap_err();
    assert!(!err.is_empty());
}

/// A grandchild is reached with `max_depth: 2` and not with
/// `max_depth: 1` — the walk is bounded by `max_depth`.
#[tokio::test]
async fn a_nested_chain_is_walked_to_max_depth_only() {
    let root = SessionId::new();
    let child = SessionId::new();
    let grandchild = SessionId::new();
    let mut child_stream = live_child(child, T0);
    child_stream.push(ev(
        2,
        child,
        0,
        EventKind::ThreadStarted,
        json!({
            "thread_id": Uuid::new_v4().to_string(),
            "parent": child.0.to_string(),
            "worker": grandchild.0.to_string(),
        }),
    ));
    let mut port = MapPort::new();
    port.add(root, root_stream(root, &[(Uuid::new_v4(), child)]))
        .add(child, child_stream)
        .add(grandchild, live_child(grandchild, T0));

    // max_depth 2: the child sits at depth 1 (its walk runs while
    // 1 < 2), so the grandchild is visited and — silent past the
    // bound — due.
    let plan = due_interrupts(root, &port, T0 + 5_000, &limits(1_000, 2), 0)
        .await
        .unwrap();
    assert!(tick_of(&plan, child).is_some(), "the child is visited");
    assert!(
        tick_of(&plan, grandchild).is_some(),
        "the grandchild is visited at max_depth 2"
    );
    assert!(tick_of(&plan, grandchild).unwrap().due);
    assert!(
        tick_of(&plan, grandchild).unwrap().parent == child,
        "the grandchild's parent is the child"
    );

    // max_depth 1: the child is visited, the grandchild is not.
    let plan = due_interrupts(root, &port, T0 + 5_000, &limits(1_000, 1), 0)
        .await
        .unwrap();
    assert!(tick_of(&plan, child).is_some());
    assert!(
        tick_of(&plan, grandchild).is_none(),
        "the grandchild is NOT visited at max_depth 1"
    );
}

// ---------------------------------------------------------------------------
// The derivation cross-check (§6.2 row 6, §8.3:865)
// ---------------------------------------------------------------------------

/// The event pair the tick produces — `control/interrupt` named
/// `child_timeout` consumed by the owning drain, then
/// `session/ended{interrupted}` — derives `stalled`, never `outstanding`
/// (and never `failed`).
#[tokio::test]
async fn the_stalled_child_task_reads_stalled_not_outstanding() {
    let child = SessionId::new();
    let events = vec![
        ev(0, child, 0, EventKind::SessionCreated, json!({})),
        ev(
            1,
            child,
            0,
            EventKind::InputAdmitted,
            json!({"content": "task"}),
        ),
        ev(
            2,
            child,
            T0,
            EventKind::ControlInterrupt,
            json!({
                "type": "control",
                "action": "interrupt",
                "reason": null,
                "requested_by": "child_timeout",
                "requested_at_ms": T0,
                "target_epoch": 1,
            }),
        ),
        ev(
            3,
            child,
            T0,
            EventKind::SessionEnded {
                reason: EndReason::Interrupted,
            },
            json!({}),
        ),
    ];

    assert_eq!(
        derive_outcome(&events, None),
        Some(TaskOutcome::Stalled),
        "the interrupted-by-timeout pair derives stalled"
    );
    assert_ne!(
        derive_outcome(&events, None),
        None,
        "stalled is never outstanding"
    );
    assert_ne!(
        derive_outcome(&events, None),
        Some(TaskOutcome::Failed),
        "stalled != failed (M6)"
    );
}
