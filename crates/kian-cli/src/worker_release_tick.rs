//! The worker-binding release tick (ADR-035): one beat of the idle-ended
//! worker-thread reclaim.
//!
//! Strictly maintenance, strictly outside every drain. Each beat reads the
//! committed `thread/started` bindings, then each bound worker stream, and
//! calls [`kian_orchestrator::Engine::release_idle`] for every thread that
//! is **idle, already ended `interrupted`/`failed`, old enough, and not
//! holding live work**:
//!
//! - the projection is `idle` and the last event is a non-terminal end;
//! - `now_ms - last.ts >= after_ms` (the configured grace);
//! - the engine re-evaluates no unconsumed inbox row, no live drain, the
//!   worker queue gate, and a fresh claim inside one per-session write
//!   transaction.
//!
//! The tick writes nothing of its own: the only write is the engine's
//! atomic claim + `cancelled` end. Errors are swallowed and logged — a
//! maintenance task never panics. `after_ms == 0` disables the reclaim
//! (the explicit verb remains).
//!
//! Reclaim is bounded by construction: one pass over the current bindings,
//! one release attempt per live-idle-ended thread, no retry loop.

use std::collections::BTreeSet;

use kian_core::events::EventKind;
use kian_core::seams::LoopDriver;
use kian_core::state::{SessionState, project};
use kian_exec::ExecStore;
use kian_orchestrator::{DrainCtx, Engine, ReleaseOutcome};
use kian_store::EventStore;
use kian_superagent::{ThreadLiveness, bound_workers, thread_liveness};
use serde_json::json;

/// One reclaim beat. `now_ms` is the beat's clock reading, injected so the
/// beat is deterministic under test.
pub async fn tick<D>(
    engine: &Engine<D>,
    store: &EventStore,
    exec_store: &ExecStore,
    after_ms: u64,
    now_ms: u64,
) where
    D: LoopDriver<Ctx = DrainCtx> + 'static,
{
    if after_ms == 0 {
        return;
    }
    let started = match store.events_where_kinds(&["thread/started"]).await {
        Ok(events) => events,
        Err(e) => {
            tracing::warn!(
                target: "kian::workers",
                error = %e,
                "worker release tick: thread/started read failed — no reclaim this beat"
            );
            return;
        }
    };
    // The Router-side and the worker-side `thread/started` name the same
    // binding; visit each bound worker session once.
    let mut seen: BTreeSet<kian_core::model::SessionId> = BTreeSet::new();
    for (worker, threads) in bound_workers(&started) {
        for thread in threads {
            if !seen.insert(thread.worker_session) {
                continue;
            }
            let events = match store.rebuild(thread.worker_session).await {
                Ok(events) => events,
                Err(e) => {
                    tracing::warn!(
                        target: "kian::workers",
                        worker = %worker,
                        session = %thread.worker_session.0,
                        error = %e,
                        "worker release tick: worker stream read failed — skipped"
                    );
                    continue;
                }
            };
            if thread_liveness(&events) != ThreadLiveness::Live {
                continue;
            }
            let state = match project(&events) {
                Ok(state) => state,
                Err(e) => {
                    tracing::warn!(
                        target: "kian::workers",
                        worker = %worker,
                        session = %thread.worker_session.0,
                        error = %e,
                        "worker release tick: projection failed — skipped, never guessed"
                    );
                    continue;
                }
            };
            if state != SessionState::Idle {
                continue;
            }
            let Some(last) = events.last() else {
                continue;
            };
            if !matches!(
                last.kind,
                EventKind::SessionEnded {
                    reason: kian_core::events::EndReason::Interrupted
                        | kian_core::events::EndReason::Failed
                }
            ) {
                continue;
            }
            if now_ms.saturating_sub(last.ts) < after_ms {
                continue;
            }
            let payload = json!({
                "released": {
                    "worker": worker,
                    "thread": thread.thread_id.to_string(),
                    "by": "reclaim",
                    "reason": format!(
                        "idle-ended worker thread reclaimed after {after_ms} ms"
                    ),
                }
            });
            let queue_gate = async {
                exec_store
                    .drivable(&worker)
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
                Ok(ReleaseOutcome::Released { seq, epoch }) => {
                    tracing::info!(
                        target: "kian::workers",
                        worker = %worker,
                        session = %thread.worker_session.0,
                        thread = %thread.thread_id,
                        seq,
                        epoch,
                        after_ms,
                        "worker release tick: idle-ended thread closed — binding released"
                    );
                }
                Ok(other) => {
                    tracing::debug!(
                        target: "kian::workers",
                        worker = %worker,
                        session = %thread.worker_session.0,
                        outcome = ?other,
                        "worker release tick: release deferred"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        target: "kian::workers",
                        worker = %worker,
                        session = %thread.worker_session.0,
                        error = %e,
                        "worker release tick: release failed — skipped"
                    );
                }
            }
        }
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

    use super::tick;

    /// A driver that never runs: the tick only claims + ends; no drain is
    /// started by the test.
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

    /// The Router-side lineage: `session/created` + one `thread/started`
    /// naming `worker_session` for `worker`.
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

    /// The worker stream in the release shape: created, claimed, input,
    /// `session/ended{reason}` — projection `idle` for interrupted/failed.
    async fn seed_worker_stream(
        store: &EventStore,
        worker_session: SessionId,
        reason: EndReason,
        last_ts: u64,
    ) {
        append(
            store,
            worker_session,
            1,
            0,
            last_ts,
            EventKind::SessionCreated,
            json!({}),
        )
        .await;
        append(
            store,
            worker_session,
            2,
            1,
            last_ts,
            EventKind::DrainClaimed,
            json!({"holder": "h"}),
        )
        .await;
        append(
            store,
            worker_session,
            3,
            1,
            last_ts,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "task"}),
        )
        .await;
        append(
            store,
            worker_session,
            4,
            1,
            last_ts,
            EventKind::SessionEnded { reason },
            json!({}),
        )
        .await;
    }

    async fn fixture(
        reason: EndReason,
    ) -> (
        Arc<Engine<NoopDriver>>,
        EventStore,
        kian_exec::ExecStore,
        SessionId,
        PathBuf,
        PathBuf,
        String,
    ) {
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
                holder: "tick-test".into(),
                lease_ttl_ms: 30_000,
                heartbeat_interval_ms: 10_000,
            },
            Arc::new(ManualClock::new(T0)),
        );
        let root = SessionId::new();
        let worker_session = SessionId::new();
        let thread_id = Uuid::new_v4();
        let worker = "mac-mini".to_string();
        seed_binding(&store, root, thread_id, &worker, worker_session).await;
        seed_worker_stream(&store, worker_session, reason, T0).await;
        (
            engine,
            store,
            exec,
            worker_session,
            events_db,
            exec_db,
            worker,
        )
    }

    /// An interrupted idle-ended thread past the grace is closed; the
    /// binding reads Released afterwards.
    #[tokio::test]
    async fn an_idle_ended_thread_past_the_grace_is_reclaimed() {
        let (engine, store, exec, worker_session, events_db, exec_db, _worker) =
            fixture(EndReason::Interrupted).await;

        tick(&*engine, &store, &exec, 300_000, T0 + 300_000).await;

        let events = store.rebuild(worker_session).await.expect("rebuild");
        assert_eq!(
            project(&events).expect("project"),
            SessionState::Cancelled,
            "the tick closed the idle-ended thread"
        );
        assert!(matches!(
            events.last().unwrap().kind,
            EventKind::SessionEnded {
                reason: EndReason::Cancelled
            }
        ));
        assert_eq!(
            events.last().unwrap().bytes["released"]["by"],
            json!("reclaim")
        );
        cleanup(&events_db);
        cleanup(&exec_db);
    }

    /// Inside the grace nothing is written.
    #[tokio::test]
    async fn a_thread_inside_the_grace_is_left_alone() {
        let (engine, store, exec, worker_session, events_db, exec_db, _worker) =
            fixture(EndReason::Failed).await;
        let before = store.head(worker_session).await.expect("head");

        tick(&*engine, &store, &exec, 300_000, T0 + 299_999).await;

        assert_eq!(
            store.head(worker_session).await.expect("head").next_seq,
            before.next_seq,
            "no write before the grace elapses"
        );
        assert_eq!(before.next_seq, 5);
        cleanup(&events_db);
        cleanup(&exec_db);
    }

    /// `release_after_ms: 0` disables the reclaim entirely.
    #[tokio::test]
    async fn zero_after_ms_disables_the_reclaim() {
        let (engine, store, exec, worker_session, events_db, exec_db, _worker) =
            fixture(EndReason::Interrupted).await;
        let before = store.head(worker_session).await.expect("head");

        tick(&*engine, &store, &exec, 0, T0 + 10 * 300_000).await;

        assert_eq!(
            store.head(worker_session).await.expect("head").next_seq,
            before.next_seq
        );
        cleanup(&events_db);
        cleanup(&exec_db);
    }

    /// A live command row for the worker is a fail-closed gate: the
    /// machine may still be executing it.
    #[tokio::test]
    async fn a_live_command_row_blocks_the_reclaim() {
        let (engine, store, exec, worker_session, events_db, exec_db, worker) =
            fixture(EndReason::Interrupted).await;
        exec.enqueue(
            Uuid::new_v4(),
            &worker,
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
        let before = store.head(worker_session).await.expect("head");

        tick(&*engine, &store, &exec, 300_000, T0 + 300_000).await;

        assert_eq!(
            store.head(worker_session).await.expect("head").next_seq,
            before.next_seq,
            "the thread is not closed while the worker owes a command"
        );
        cleanup(&events_db);
        cleanup(&exec_db);
    }
}
