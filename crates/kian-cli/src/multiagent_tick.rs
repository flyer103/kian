//! The child inactivity maintenance tick (MULTIAGENT-MODEL §5.3:587,
//! plan pin P5): one beat of the `child_timeout_ms` sweeper.
//!
//! Strictly maintenance, strictly outside every drain: the tick calls
//! the pure planner ([`due_interrupts`]), then requests
//! `control/interrupt{requested_by:"child_timeout"}` through the
//! control plane for every due child. It writes nothing else — the
//! interrupt inbox row is the tick's only write, and it is idempotent
//! per `interrupt:<session>:<target_epoch>` (EVENT-MODEL:203-205), so a
//! `kill -9` mid-tick leaves nothing partial and a doubled tick commits
//! nothing. No promotion, no spawn, no store mutation. Errors are
//! swallowed and logged — a maintenance task never panics.
//!
//! `kian serve` arms the periodic driver only when the superagent
//! bundle configures multiagent limits (see `Server::serve`'s ticker
//! block); feature-off arms no timer at all (M8).

use kian_core::model::SessionId;
use kian_core::seams::LoopDriver;
use kian_orchestrator::Engine;
use kian_superagent::{MultiagentLimits, WorkerStreamsPort, due_interrupts};

/// One maintenance beat: plan, then interrupt every due child.
///
/// `port` is the store's [`WorkerStreamsPort`] read surface (assembly's
/// `StoreWorkerStreams`); `root` is the walk root (the resident Router
/// session); `now_ms` is the tick's clock reading, injected so the beat
/// is deterministic under test.
pub async fn tick<D>(
    engine: &Engine<D>,
    port: &dyn WorkerStreamsPort,
    root: SessionId,
    limits: &MultiagentLimits,
    now_ms: u64,
) where
    D: LoopDriver<Ctx = kian_orchestrator::DrainCtx> + 'static,
{
    let plan = match due_interrupts(root, port, now_ms, limits, 0).await {
        Ok(plan) => plan,
        Err(e) => {
            // The walk root itself is unreadable: nothing can be
            // decided this beat. Honest no-op, never a panic.
            tracing::warn!(
                target: "kian::multiagent",
                root = %root.0,
                error = %e,
                "child inactivity tick: walk root unreadable — no interrupts issued"
            );
            return;
        }
    };
    for child in plan {
        if !child.due {
            continue;
        }
        // The only write: the idempotent control/interrupt intent row,
        // carrying requested_by:"child_timeout" so the join derives
        // `stalled` (§6.2 row 6).
        match engine
            .interrupt_as(child.child, kian_superagent::CHILD_TIMEOUT_REQUESTED_BY)
            .await
        {
            Ok(outcome) => tracing::info!(
                target: "kian::multiagent",
                child = %child.child.0,
                parent = %child.parent.0,
                thread_id = %child.thread_id,
                last_event_ms = child.last_event_ms,
                outcome = ?outcome,
                "child inactivity bound: interrupt requested (child_timeout)"
            ),
            Err(e) => tracing::warn!(
                target: "kian::multiagent",
                child = %child.child.0,
                parent = %child.parent.0,
                thread_id = %child.thread_id,
                error = %e,
                "child inactivity tick: interrupt failed — swallowed (maintenance never panics)"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use futures::stream::BoxStream;
    use kian_core::control::InterruptIntent;
    use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};
    use kian_core::seams::{LoopDriver, LoopOutcome};
    use kian_orchestrator::{DrainCtx, Engine, EngineConfig};
    use kian_store::{AppendGuard, EventStore};
    use kian_superagent::{MultiagentLimits, TaskOutcome, derive_outcome};
    use serde_json::{Value, json};
    use tokio::sync::Notify;
    use uuid::Uuid;

    use super::tick;

    /// A driver that hangs mid-turn until released, then settles — the
    /// shape of a child worker mid-provider-turn when the inactivity
    /// bound fires.
    struct GatedDriver {
        release: Arc<Notify>,
    }

    impl LoopDriver for GatedDriver {
        type Ctx = DrainCtx;

        fn drive(&self, _ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
            let release = Arc::clone(&self.release);
            futures::stream::once(async move {
                release.notified().await;
                LoopOutcome::Settled
            })
            .boxed()
        }
    }

    fn temp_db(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "kian-multiagent-tick-{tag}-{}.sqlite",
            Uuid::new_v4()
        ))
    }

    fn cleanup(db: &std::path::Path) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", db.display()));
        }
    }

    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Appends one event at `(seq, epoch)` with the given timestamp.
    async fn append(
        store: &EventStore,
        session: SessionId,
        seq: u64,
        epoch: u64,
        ts: u64,
        kind: EventKind,
        bytes: Value,
    ) {
        let event = Event {
            seq,
            session,
            epoch: Epoch(epoch),
            ts,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        };
        store
            .append(
                session,
                event,
                AppendGuard {
                    epoch: Epoch(epoch),
                    expected_seq: seq,
                },
            )
            .await
            .expect("append");
    }

    /// Builds the walk root: `session/created` + one `thread/started`
    /// lineage naming `child` (the frozen §2 payload shape). Streams
    /// number events from seq 1 (the `archive_e2e` seeding precedent).
    async fn seed_root(store: &EventStore, root: SessionId, thread_id: Uuid, child: SessionId) {
        append(store, root, 1, 0, 0, EventKind::SessionCreated, json!({})).await;
        append(
            store,
            root,
            2,
            0,
            0,
            EventKind::ThreadStarted,
            json!({
                "thread_id": thread_id.to_string(),
                "parent": root.0.to_string(),
                "worker": child.0.to_string(),
                "pinned_version": "v0",
                "task": {"role": "user", "content": "delegate"},
            }),
        )
        .await;
    }

    /// Builds a child stream that projects `running`: created → input
    /// admitted → drain claimed (epoch 1), last event at `last_ts`.
    async fn seed_running_child(store: &EventStore, child: SessionId, last_ts: u64) {
        append(
            store,
            child,
            1,
            0,
            last_ts,
            EventKind::SessionCreated,
            json!({}),
        )
        .await;
        append(
            store,
            child,
            2,
            0,
            last_ts,
            EventKind::InputAdmitted,
            json!({"content": "task"}),
        )
        .await;
        append(
            store,
            child,
            3,
            1,
            last_ts,
            EventKind::DrainClaimed,
            json!({}),
        )
        .await;
    }

    fn limits() -> MultiagentLimits {
        MultiagentLimits {
            max_children: 4,
            max_depth: 1,
            child_timeout_ms: 1_000,
        }
    }

    /// Drive the tick twice; the recording control plane (the real
    /// inbox) shows exactly one row, keyed to the child's epoch, named
    /// `child_timeout`.
    #[tokio::test]
    async fn the_interrupt_is_admitted_once_per_epoch_and_names_child_timeout() {
        let db = temp_db("once");
        let store = EventStore::open(&db).await.expect("open");
        let engine = Engine::new(
            store.clone(),
            Arc::new(GatedDriver {
                release: Arc::new(Notify::new()),
            }),
            EngineConfig::default(),
        );

        let root = SessionId::new();
        let child = SessionId::new();
        let thread_id = Uuid::new_v4();
        seed_root(&store, root, thread_id, child).await;
        // Silent for far past the bound.
        let last_ts = now_ms() - 60_000;
        seed_running_child(&store, child, last_ts).await;

        let port = crate::superagent::StoreWorkerStreams::new(store.clone());
        let port: &dyn kian_superagent::WorkerStreamsPort = port.as_ref();
        tick(&*engine, port, root, &limits(), now_ms()).await;
        tick(&*engine, port, root, &limits(), now_ms()).await;

        let eligible = store
            .eligible_inputs(child, now_ms() + 60_000)
            .await
            .expect("inbox");
        let control: Vec<_> = eligible
            .iter()
            .filter(|r| InterruptIntent::is_control_envelope(&r.payload))
            .collect();
        assert_eq!(control.len(), 1, "one durable intent row per epoch");
        let intent = InterruptIntent::parse(&control[0].payload).expect("envelope");
        assert_eq!(
            intent.requested_by.as_deref(),
            Some("child_timeout"),
            "the row names child_timeout"
        );
        assert_eq!(
            control[0].idempotency_key.as_deref(),
            Some(InterruptIntent::idempotency_key(&child, Some(1)).as_str()),
            "the idempotency key targets the running episode"
        );
        // The tick's ONLY write is that row: the child stream is
        // untouched (no event was appended by the control plane).
        let events = store.rebuild(child).await.expect("rebuild");
        assert_eq!(events.len(), 3, "no event write, only the inbox row");

        store.close().await;
        cleanup(&db);
    }

    /// The takeover drain consumes the tick's row into a
    /// `control/interrupt{child_timeout}` audit event and settles
    /// `session/ended{interrupted}` — the pair C1's `derive_outcome`
    /// reads as `stalled`, never `outstanding` (§6.2 row 6, §8.3:865).
    #[tokio::test]
    async fn the_stalled_child_task_reads_stalled_not_outstanding() {
        let db = temp_db("stalled");
        let store = EventStore::open(&db).await.expect("open");
        let release = Arc::new(Notify::new());
        let engine = Engine::new(
            store.clone(),
            Arc::new(GatedDriver {
                release: Arc::clone(&release),
            }),
            EngineConfig::default(),
        );

        let root = SessionId::new();
        let child = SessionId::new();
        seed_root(&store, root, Uuid::new_v4(), child).await;
        let last_ts = now_ms() - 60_000;
        seed_running_child(&store, child, last_ts).await;

        let port = crate::superagent::StoreWorkerStreams::new(store.clone());
        let port: &dyn kian_superagent::WorkerStreamsPort = port.as_ref();
        tick(&*engine, port, root, &limits(), now_ms()).await;

        // The takeover drain (running projection, no local handle)
        // consumes the row and settles interrupted.
        engine.wake(child);
        release.notify_one();
        for _ in 0..500 {
            if !engine.is_draining(&child) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(!engine.is_draining(&child), "the takeover drain settled");

        let events = store.rebuild(child).await.expect("rebuild");
        let control: Vec<&Event> = events
            .iter()
            .filter(|e| e.kind == EventKind::ControlInterrupt)
            .collect();
        assert_eq!(control.len(), 1, "exactly one control/interrupt event");
        assert_eq!(
            control[0].bytes.get("requested_by").and_then(Value::as_str),
            Some("child_timeout"),
            "the audit event names child_timeout"
        );
        assert!(matches!(
            events.last().expect("non-empty").kind,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Interrupted
            }
        ));
        assert_eq!(
            derive_outcome(&events, None),
            Some(TaskOutcome::Stalled),
            "the resulting pair derives stalled, not outstanding"
        );

        store.close().await;
        cleanup(&db);
    }
}
