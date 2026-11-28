//! Worker binding release (ADR-035): resolve a worker's live bound thread,
//! apply the fail-closed gates, and close it through the engine.
//!
//! This is the shared implementation behind the `release_worker` ops method
//! (`kian workers release <id>`) and, indirectly, the reclaim tick (the tick
//! walks bindings itself but calls the same engine primitive). It is assembly
//! code: it reads the committed streams and the durable command queue, then
//! asks the engine to append the close — it never writes an event itself.
//!
//! Gates (in order; every refusal writes nothing):
//! 1. the worker id resolves to exactly one bound thread (unknown id, no
//!    binding or an ambiguous multi-live state is refused);
//! 2. the thread's projection is `idle` (running/recoverable is busy);
//! 3. its last event is `session/ended{interrupted|failed}` (a thread that
//!    never ended is not releasable);
//! 4. the engine re-evaluates no unconsumed inbox row, no live drain, the
//!    worker queue gate, and a fresh claim inside one per-session write
//!    transaction (the close is one atomic claim +
//!    `session/ended{cancelled}` batch).
//!
//! The earlier honest end is preserved; the close is terminal, so the thread
//! can never resume into the slot a newer thread now holds.

use std::collections::BTreeSet;

use kian_core::events::EventKind;
use kian_core::model::SessionId;
use kian_core::seams::LoopDriver;
use kian_core::state::{SessionState, project};
use kian_exec::ExecStore;
use kian_orchestrator::{DrainCtx, Engine, ReleaseOutcome};
use kian_store::EventStore;
use kian_superagent::{BoundThread, ThreadLiveness, bound_workers, thread_liveness};
use serde_json::json;
use uuid::Uuid;

use crate::ops::OpsError;

/// What a `release_worker` call did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseWorkerOutcome {
    /// The live thread was closed and the slot freed.
    Released,
    /// The thread is already terminal — nothing was written (idempotent).
    AlreadyReleased,
}

impl ReleaseWorkerOutcome {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Released => "released",
            Self::AlreadyReleased => "already_released",
        }
    }
}

/// The `release_worker` result: which thread was closed and where.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseWorkerReceipt {
    pub worker: String,
    pub outcome: ReleaseWorkerOutcome,
    pub session: SessionId,
    pub thread_id: Uuid,
    /// The terminal end write's seq (the release's own end, or the
    /// existing terminal end for `already_released`).
    pub seq: u64,
}

/// One worker's binding, as resolved from the committed streams.
enum Owner {
    /// One live bound thread.
    Live(BoundThread),
    /// The latest binding is terminal (any binding was released/ended).
    Released { thread: BoundThread, seq: u64 },
    /// No `thread/started` names this worker id.
    None,
    /// More than one distinct live thread — never guessed over.
    Ambiguous { sessions: Vec<SessionId> },
}

/// Resolves the binding from the committed `thread/started` events (both
/// sides name it; the worker session dedupes them) and each candidate's own
/// stream. Read-only.
async fn resolve_owner(store: &EventStore, worker: &str) -> Result<Owner, String> {
    let started = store
        .events_where_kinds(&["thread/started"])
        .await
        .map_err(|e| format!("thread/started read: {e}"))?;
    let mut bindings = bound_workers(&started);
    let Some(threads) = bindings.remove(worker) else {
        return Ok(Owner::None);
    };
    let mut seen: BTreeSet<SessionId> = BTreeSet::new();
    let mut live: Vec<BoundThread> = Vec::new();
    let mut latest_released: Option<(BoundThread, u64)> = None;
    for thread in threads {
        if !seen.insert(thread.worker_session) {
            continue; // the other side's thread/started names the same binding
        }
        let events = store
            .rebuild(thread.worker_session)
            .await
            .map_err(|e| format!("stream read for {}: {e}", thread.worker_session.0))?;
        match thread_liveness(&events) {
            ThreadLiveness::Live => live.push(thread),
            ThreadLiveness::Released => {
                let seq = events.last().map(|e| e.seq).unwrap_or(0);
                if latest_released
                    .as_ref()
                    .is_none_or(|(current, _)| thread.started_ts >= current.started_ts)
                {
                    latest_released = Some((thread, seq));
                }
            }
            ThreadLiveness::NeverStarted => {}
        }
    }
    match (live.len(), latest_released) {
        (1, _) => Ok(Owner::Live(live.remove(0))),
        (0, Some((thread, seq))) => Ok(Owner::Released { thread, seq }),
        (0, None) => Ok(Owner::None),
        _ => Ok(Owner::Ambiguous {
            sessions: live.iter().map(|t| t.worker_session).collect(),
        }),
    }
}

/// Releases the live bound thread of `worker` (ADR-035). Read-only refusals
/// write nothing; the only write is the engine's atomic close.
pub async fn release_worker<D>(
    engine: &Engine<D>,
    store: &EventStore,
    exec_store: &ExecStore,
    worker: &str,
    reason: Option<String>,
) -> Result<ReleaseWorkerReceipt, OpsError>
where
    D: LoopDriver<Ctx = DrainCtx> + 'static,
{
    let owner = resolve_owner(store, worker)
        .await
        .map_err(|e| OpsError::Internal(format!("release_worker: binding read failed: {e}")))?;
    let thread = match owner {
        Owner::Live(thread) => thread,
        Owner::Released { thread, seq } => {
            return Ok(ReleaseWorkerReceipt {
                worker: worker.to_string(),
                outcome: ReleaseWorkerOutcome::AlreadyReleased,
                session: thread.worker_session,
                thread_id: thread.thread_id,
                seq,
            });
        }
        Owner::None => {
            return Err(OpsError::WorkerNotBound {
                worker: worker.to_string(),
                detail: "no live bound worker session (unknown id, no binding, or a \
                         never-started reservation)"
                    .to_string(),
            });
        }
        Owner::Ambiguous { sessions } => {
            return Err(OpsError::Internal(format!(
                "release_worker `{worker}`: more than one live bound session \
                 ({}); refusing to guess which one to close",
                sessions
                    .iter()
                    .map(|s| s.0.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    };

    let events = store
        .rebuild(thread.worker_session)
        .await
        .map_err(|e| OpsError::Internal(format!("release_worker: stream read failed: {e}")))?;
    let state = project(&events)
        .map_err(|e| OpsError::Internal(format!("release_worker: projection failed: {e}")))?;
    let ended_non_terminal = matches!(
        events.last().map(|e| &e.kind),
        Some(EventKind::SessionEnded {
            reason: kian_core::events::EndReason::Interrupted
                | kian_core::events::EndReason::Failed
        })
    );
    if state != SessionState::Idle || !ended_non_terminal {
        return Err(OpsError::WorkerBusy {
            worker: worker.to_string(),
            session: thread.worker_session,
            detail: if state == SessionState::Running || state == SessionState::Recoverable {
                "its bound session is running; interrupt it first and release once it is idle"
                    .to_string()
            } else if ended_non_terminal {
                "its bound session is not an idle, already-ended thread; nothing to release"
                    .to_string()
            } else {
                "its bound session is idle but has not ended; release only closes an \
                 interrupted/failed thread"
                    .to_string()
            },
        });
    }

    // Unconsumed work is resumable work, including future-eligible rows.
    let has_unconsumed = store
        .has_unconsumed_inputs(thread.worker_session)
        .await
        .map_err(|e| OpsError::Internal(format!("release_worker: inbox read failed: {e}")))?;
    if has_unconsumed {
        return Err(OpsError::WorkerBusy {
            worker: worker.to_string(),
            session: thread.worker_session,
            detail: "its inbox still holds unconsumed input row(s); the thread is resumable, \
                     so releasing it would discard queued work"
                .to_string(),
        });
    }

    let payload = json!({
        "released": {
            "worker": worker,
            "thread": thread.thread_id.to_string(),
            "by": "operator",
            "reason": reason.unwrap_or_else(|| "operator release".to_string()),
        }
    });
    let queue_gate = async {
        exec_store
            .drivable(worker)
            .await
            .map(|rows| rows.is_empty())
            .map_err(|e| {
                kian_orchestrator::EngineError::ReleaseQueueGate(format!(
                    "durable queue read failed: {e}"
                ))
            })
    };
    match engine
        .release_idle_guarded(thread.worker_session, payload, queue_gate)
        .await
    {
        Ok(ReleaseOutcome::Released { seq, .. }) => Ok(ReleaseWorkerReceipt {
            worker: worker.to_string(),
            outcome: ReleaseWorkerOutcome::Released,
            session: thread.worker_session,
            thread_id: thread.thread_id,
            seq,
        }),
        Ok(ReleaseOutcome::AlreadyTerminal) => {
            let events = store.rebuild(thread.worker_session).await.map_err(|e| {
                OpsError::Internal(format!("release_worker: stream read failed: {e}"))
            })?;
            Ok(ReleaseWorkerReceipt {
                worker: worker.to_string(),
                outcome: ReleaseWorkerOutcome::AlreadyReleased,
                session: thread.worker_session,
                thread_id: thread.thread_id,
                seq: events.last().map(|e| e.seq).unwrap_or(0),
            })
        }
        Ok(ReleaseOutcome::Busy | ReleaseOutcome::NotEligible) => Err(OpsError::WorkerBusy {
            worker: worker.to_string(),
            session: thread.worker_session,
            detail: "the session moved while releasing (a drain/wake or new input); retry once \
                     it is idle again"
                .to_string(),
        }),
        Ok(ReleaseOutcome::QueueBusy) => Err(OpsError::WorkerBusy {
            worker: worker.to_string(),
            session: thread.worker_session,
            detail: "its durable queue still holds a live command row; wait for it to settle, \
                     then release"
                .to_string(),
        }),
        Ok(ReleaseOutcome::Unknown) => Err(OpsError::WorkerNotBound {
            worker: worker.to_string(),
            detail: "the bound worker session has no event stream".to_string(),
        }),
        Err(e) => Err(OpsError::Internal(format!("release_worker: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use futures::StreamExt;
    use futures::stream::BoxStream;
    use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};
    use kian_core::seams::{LoopDriver, LoopOutcome};
    use kian_core::state::{SessionState, project};
    use kian_orchestrator::{DrainCtx, Engine, EngineConfig, ManualClock};
    use kian_store::{AppendGuard, EventStore};
    use serde_json::json;
    use uuid::Uuid;

    use super::{ReleaseWorkerOutcome, release_worker};
    use crate::ops::OpsError;

    struct NoopDriver;

    impl LoopDriver for NoopDriver {
        type Ctx = DrainCtx;

        fn drive(&self, _ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
            futures::stream::once(async { LoopOutcome::Settled }).boxed()
        }
    }

    fn temp_db(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "kian-worker-release-{tag}-{}.sqlite",
            Uuid::new_v4()
        ))
    }

    fn cleanup(db: &Path) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
        }
    }

    const T0: u64 = 1_761_900_000_000;

    async fn append(
        store: &EventStore,
        session: SessionId,
        seq: u64,
        epoch: u64,
        ts: u64,
        kind: EventKind,
        bytes: serde_json::Value,
    ) {
        store
            .append(
                session,
                Event {
                    seq,
                    session,
                    epoch: Epoch(epoch),
                    ts,
                    kind,
                    bytes,
                    version: SCHEMA_VERSION,
                },
                AppendGuard {
                    epoch: Epoch(epoch),
                    expected_seq: seq,
                },
            )
            .await
            .expect("append");
    }

    async fn seed_binding(
        store: &EventStore,
        root: SessionId,
        thread_id: Uuid,
        worker: &str,
        worker_session: SessionId,
    ) {
        append(store, root, 1, 0, T0, EventKind::SessionCreated, json!({})).await;
        append(
            store,
            root,
            2,
            0,
            T0,
            EventKind::ThreadStarted,
            json!({
                "thread_id": thread_id.to_string(),
                "parent": root.0.to_string(),
                "worker": worker_session.0.to_string(),
                "pinned_version": 1,
                "task": {"role": "user", "content": "delegate"},
                "exec_worker": worker,
            }),
        )
        .await;
    }

    /// A worker stream in the idle-ended release shape.
    async fn seed_ended_worker(store: &EventStore, worker_session: SessionId, reason: EndReason) {
        append(
            store,
            worker_session,
            1,
            0,
            T0,
            EventKind::SessionCreated,
            json!({}),
        )
        .await;
        append(
            store,
            worker_session,
            2,
            1,
            T0,
            EventKind::DrainClaimed,
            json!({"holder": "test-holder"}),
        )
        .await;
        append(
            store,
            worker_session,
            3,
            1,
            T0,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "task"}),
        )
        .await;
        append(
            store,
            worker_session,
            4,
            1,
            T0,
            EventKind::SessionEnded { reason },
            json!({}),
        )
        .await;
    }

    /// A worker stream whose drain is still running (claim, no end).
    async fn seed_running_worker(store: &EventStore, worker_session: SessionId) {
        append(
            store,
            worker_session,
            1,
            0,
            T0,
            EventKind::SessionCreated,
            json!({}),
        )
        .await;
        append(
            store,
            worker_session,
            2,
            1,
            T0,
            EventKind::DrainClaimed,
            json!({"holder": "test-holder"}),
        )
        .await;
        append(
            store,
            worker_session,
            3,
            1,
            T0,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "task"}),
        )
        .await;
    }

    struct Fixture {
        engine: Arc<Engine<NoopDriver>>,
        store: EventStore,
        exec: kian_exec::ExecStore,
        worker_session: SessionId,
        thread_id: Uuid,
        events_db: PathBuf,
        exec_db: PathBuf,
    }

    impl Fixture {
        async fn new<F, Fut>(seed: F) -> Self
        where
            F: FnOnce(EventStore, SessionId) -> Fut,
            Fut: std::future::Future<Output = ()>,
        {
            let events_db = temp_db("events");
            let exec_db = temp_db("exec");
            let store = EventStore::open(&events_db).await.expect("events store");
            let exec = kian_exec::ExecStore::open(&exec_db)
                .await
                .expect("exec store");
            let engine = Engine::with_clock(
                store.clone(),
                Arc::new(NoopDriver),
                EngineConfig {
                    holder: "test-holder".into(),
                    lease_ttl_ms: 30_000,
                    heartbeat_interval_ms: 10_000,
                },
                Arc::new(ManualClock::new(T0)),
            );
            let root = SessionId::new();
            let worker_session = SessionId::new();
            let thread_id = Uuid::new_v4();
            seed_binding(&store, root, thread_id, "mac-mini", worker_session).await;
            seed(store.clone(), worker_session).await;
            Self {
                engine,
                store,
                exec,
                worker_session,
                thread_id,
                events_db,
                exec_db,
            }
        }

        fn cleanup(&self) {
            cleanup(&self.events_db);
            cleanup(&self.exec_db);
        }
    }

    /// The live thread is closed (terminal `cancelled`), the receipt names
    /// it, and a second call is the idempotent `already_released`.
    #[tokio::test]
    async fn release_worker_closes_the_live_thread_then_is_idempotent() {
        let fx = Fixture::new(|store, worker_session| async move {
            seed_ended_worker(&store, worker_session, EndReason::Interrupted).await;
        })
        .await;

        let receipt = release_worker(
            &*fx.engine,
            &fx.store,
            &fx.exec,
            "mac-mini",
            Some("stuck".into()),
        )
        .await
        .expect("release");
        assert_eq!(receipt.worker, "mac-mini");
        assert_eq!(receipt.outcome, ReleaseWorkerOutcome::Released);
        assert_eq!(receipt.session, fx.worker_session);
        assert_eq!(receipt.thread_id, fx.thread_id);
        let events = fx.store.rebuild(fx.worker_session).await.expect("rebuild");
        assert_eq!(project(&events).expect("project"), SessionState::Cancelled);
        assert_eq!(events.last().unwrap().seq, receipt.seq);
        assert_eq!(
            events.last().unwrap().bytes["released"]["worker"],
            json!("mac-mini")
        );

        let again = release_worker(&*fx.engine, &fx.store, &fx.exec, "mac-mini", None)
            .await
            .expect("idempotent release");
        assert_eq!(again.outcome, ReleaseWorkerOutcome::AlreadyReleased);
        assert_eq!(again.session, fx.worker_session);
        assert_eq!(again.seq, receipt.seq);
        fx.cleanup();
    }

    /// An unknown worker id is the honest `worker_not_bound`, not a guess.
    #[tokio::test]
    async fn release_worker_refuses_an_unknown_worker() {
        let fx = Fixture::new(|store, worker_session| async move {
            seed_ended_worker(&store, worker_session, EndReason::Failed).await;
        })
        .await;
        let err = release_worker(&*fx.engine, &fx.store, &fx.exec, "nope", None)
            .await
            .expect_err("unknown worker");
        assert!(
            matches!(err, OpsError::WorkerNotBound { ref worker, .. } if worker == "nope"),
            "{err:?}"
        );
        fx.cleanup();
    }

    /// A running thread is never force-closed.
    #[tokio::test]
    async fn release_worker_refuses_a_running_thread() {
        let fx = Fixture::new(|store, worker_session| async move {
            seed_running_worker(&store, worker_session).await;
        })
        .await;
        let before = fx.store.head(fx.worker_session).await.expect("head");
        let err = release_worker(&*fx.engine, &fx.store, &fx.exec, "mac-mini", None)
            .await
            .expect_err("running thread");
        assert!(
            matches!(err, OpsError::WorkerBusy { ref detail, .. } if detail.contains("running")),
            "{err:?}"
        );
        assert_eq!(
            fx.store
                .head(fx.worker_session)
                .await
                .expect("head")
                .next_seq,
            before.next_seq,
            "nothing was written"
        );
        fx.cleanup();
    }

    /// A live command row means the machine may still be working: refuse.
    #[tokio::test]
    async fn release_worker_refuses_a_live_queue_row() {
        let fx = Fixture::new(|store, worker_session| async move {
            seed_ended_worker(&store, worker_session, EndReason::Interrupted).await;
        })
        .await;
        fx.exec
            .enqueue(
                Uuid::new_v4(),
                "mac-mini",
                kian_core::seams::ExecCommand {
                    argv: vec!["true".into()],
                    env: Default::default(),
                    cwd: None,
                    timeout_ms: None,
                },
                Some("tool-call-1"),
            )
            .await
            .expect("enqueue");
        let before = fx.store.head(fx.worker_session).await.expect("head");
        let err = release_worker(&*fx.engine, &fx.store, &fx.exec, "mac-mini", None)
            .await
            .expect_err("live queue row");
        assert!(
            matches!(err, OpsError::WorkerBusy { ref detail, .. } if detail.contains("queue")),
            "{err:?}"
        );
        assert_eq!(
            fx.store
                .head(fx.worker_session)
                .await
                .expect("head")
                .next_seq,
            before.next_seq
        );
        fx.cleanup();
    }

    /// Unconsumed input is resumable work: never discarded by a release.
    #[tokio::test]
    async fn release_worker_refuses_unconsumed_input() {
        let fx = Fixture::new(|store, worker_session| async move {
            seed_ended_worker(&store, worker_session, EndReason::Interrupted).await;
        })
        .await;
        fx.engine
            .admit_quiet(fx.worker_session, json!("resume me"), None)
            .await
            .expect("quiet admit");
        let before = fx.store.head(fx.worker_session).await.expect("head");
        let err = release_worker(&*fx.engine, &fx.store, &fx.exec, "mac-mini", None)
            .await
            .expect_err("unconsumed input");
        assert!(
            matches!(err, OpsError::WorkerBusy { ref detail, .. } if detail.contains("unconsumed")),
            "{err:?}"
        );
        assert_eq!(
            fx.store
                .head(fx.worker_session)
                .await
                .expect("head")
                .next_seq,
            before.next_seq
        );
        fx.cleanup();
    }

    /// A terminal thread answers `already_released` and writes nothing.
    #[tokio::test]
    async fn release_worker_reports_a_terminal_thread_as_already_released() {
        let fx = Fixture::new(|store, worker_session| async move {
            seed_ended_worker(&store, worker_session, EndReason::Completed).await;
        })
        .await;
        let before = fx.store.head(fx.worker_session).await.expect("head");
        let receipt = release_worker(&*fx.engine, &fx.store, &fx.exec, "mac-mini", None)
            .await
            .expect("already released");
        assert_eq!(receipt.outcome, ReleaseWorkerOutcome::AlreadyReleased);
        assert_eq!(receipt.seq, before.next_seq - 1);
        assert_eq!(
            fx.store
                .head(fx.worker_session)
                .await
                .expect("head")
                .next_seq,
            before.next_seq,
            "nothing was written"
        );
        fx.cleanup();
    }
}
