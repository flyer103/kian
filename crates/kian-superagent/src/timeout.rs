//! The child inactivity bound's pure planner (MULTIAGENT-MODEL §5.3:587,
//! plan pin P5): which children of a root session have been silent past
//! `child_timeout_ms` and therefore deserve a `control/interrupt`
//! carrying `requested_by:"child_timeout"`.
//!
//! Pure planning over committed streams read through the existing
//! [`WorkerStreamsPort`] seam — no store scan, no new query, no live-
//! session listing (plan §0.1 A3). The planner issues nothing: the
//! maintenance tick (`kian-cli`'s `multiagent_tick`) turns `due` entries
//! into interrupts through the control plane, which only writes the
//! idempotent inbox row (constraint 9).
//!
//! Honesty rules (plan §0.3 item 2):
//!
//! - `ThreadLiveness::Released` (completed / cancelled) and
//!   `NeverStarted` (a lineage-only reservation, released by the queue
//!   timeout — DESIGN §4.1/ADR-020, not by us) are never due.
//! - An unreadable child stream yields **no** entry: unknown is not
//!   stalled. The subtree below it is unreachable and skipped for the
//!   same reason.
//! - The walk is bounded: recursion stops at `limits.max_depth`, and
//!   every level is a `for` over that stream's finite `thread/started`
//!   child list — no unbounded loop.

use std::collections::BTreeMap;

use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use uuid::Uuid;

use crate::multiagent::MultiagentLimits;
use crate::worker::{ThreadLiveness, WorkerStreamsPort, thread_liveness};

/// One visited child of the inactivity walk. Every readable child on a
/// walked level contributes an entry (with its `due` decision); an
/// unreadable child contributes none — the honest refusal, not a guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChildTick {
    /// The child (worker) session, from `thread/started{worker}`.
    pub child: SessionId,
    /// The parent whose stream carries the `thread/started` lineage.
    pub parent: SessionId,
    /// The delegation's thread id, from `thread/started{thread_id}`.
    pub thread_id: Uuid,
    /// The child stream's last committed event `ts` (unix epoch ms) —
    /// the inactivity clock. `0` for an empty stream.
    pub last_event_ms: u64,
    /// Live **and** silent past `child_timeout_ms` at `now_ms`.
    /// Released/NeverStarted children are never due.
    pub due: bool,
}

/// `bytes.worker` as a session id, if present and well-formed (the same
/// reader `bound_workers`/`delegation_pairs` use).
fn event_worker_session(ev: &Event) -> Option<SessionId> {
    let raw = ev.bytes.get("worker").and_then(serde_json::Value::as_str)?;
    Some(SessionId(Uuid::parse_str(raw).ok()?))
}

/// `bytes.thread_id` as a uuid, if present and well-formed.
fn event_thread_uuid(ev: &Event) -> Option<Uuid> {
    let raw = ev
        .bytes
        .get("thread_id")
        .and_then(serde_json::Value::as_str)?;
    Uuid::parse_str(raw).ok()
}

/// The `thread/started` children of one stream: `(thread_id, worker)`
/// pairs in stream order, deduplicated by worker session (a re-listed
/// delegation names the same child; the first lineage wins). Malformed
/// lineage events (missing/ill-formed `thread_id` or `worker`)
/// contribute nothing — the existing readers' discipline.
fn children_of(events: &[Event]) -> Vec<(Uuid, SessionId)> {
    let mut seen: BTreeMap<SessionId, (Uuid, SessionId)> = BTreeMap::new();
    for ev in events {
        if ev.kind != EventKind::ThreadStarted {
            continue;
        }
        let (Some(thread_id), Some(worker)) = (event_thread_uuid(ev), event_worker_session(ev))
        else {
            continue;
        };
        seen.entry(worker).or_insert((thread_id, worker));
    }
    seen.into_values().collect()
}

/// Walks `root`'s stream for `thread/started` children, reads each
/// child's stream through `port`, and recurses while
/// `depth < limits.max_depth` — a grandchild is reached with
/// `max_depth: 2` and not with `max_depth: 1` (the caller passes the
/// root's own depth, `0` for the resident Router root).
///
/// A child is `due` when it is `ThreadLiveness::Live` **and**
/// `now_ms - last_event_ms >= child_timeout_ms` — the bound is
/// inactivity, not total runtime. `Err` is returned only when the
/// walk's own root stream cannot be read (nothing can be decided); a
/// child whose stream is unreadable is skipped without an entry.
///
/// The recursion is expressed as an explicit depth-bounded worklist:
/// every level is a `for` over that stream's finite `thread/started`
/// child list, and a node's children are queued only while
/// `depth + 1 < max_depth` — the walk cannot loop unboundedly.
pub async fn due_interrupts(
    root: SessionId,
    port: &dyn WorkerStreamsPort,
    now_ms: u64,
    limits: &MultiagentLimits,
    depth: u32,
) -> Result<Vec<ChildTick>, String> {
    // max_depth is spawn-chain hops from the root: children of a node
    // at `depth` exist only while `depth < max_depth`.
    if depth >= limits.max_depth {
        return Ok(Vec::new());
    }
    let root_events = port.read_stream(root).await.map_err(|e| e.to_string())?;

    // One walked level: the session, its already-read committed events,
    // and its depth. Events ride on the stack so every stream is read
    // exactly once.
    struct Level {
        session: SessionId,
        events: Vec<Event>,
        depth: u32,
    }
    let mut stack = vec![Level {
        session: root,
        events: root_events,
        depth,
    }];
    let mut out = Vec::new();
    while let Some(level) = stack.pop() {
        for (thread_id, child) in children_of(&level.events) {
            let child_events = match port.read_stream(child).await {
                Ok(events) => events,
                Err(e) => {
                    // Honest refusal (§0.3 item 2): unknown is not
                    // stalled. The subtree below an unreadable stream
                    // is skipped for the same reason — it cannot be
                    // walked.
                    tracing::warn!(
                        target: "kian::multiagent",
                        parent = %level.session.0,
                        child = %child.0,
                        thread_id = %thread_id,
                        error = %e,
                        "child inactivity walk: stream unreadable — no interrupt (unknown is not stalled)"
                    );
                    continue;
                }
            };
            let liveness = thread_liveness(&child_events);
            let last_event_ms = child_events.last().map_or(0, |ev| ev.ts);
            let due = liveness == ThreadLiveness::Live
                && now_ms.saturating_sub(last_event_ms) >= limits.child_timeout_ms;
            out.push(ChildTick {
                child,
                parent: level.session,
                thread_id,
                last_event_ms,
                due,
            });
            if level.depth + 1 < limits.max_depth {
                stack.push(Level {
                    session: child,
                    events: child_events,
                    depth: level.depth + 1,
                });
            }
        }
    }
    Ok(out)
}
