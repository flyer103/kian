//! ADR-040 phase 2 + ADR-047: runtime rotation orchestration.
//!
//! A real store + a real `Engine` (a no-op test driver: the planner never
//! drives a turn) exercise the rotation paths: the single-replica tick,
//! a second replica catching up with another replica's rotation (and
//! migrating the row it wrote to the frozen predecessor), and a replica
//! repairing a rotation whose owner died between the close and the
//! successor's bootstrap.

use std::sync::Arc;

use futures::StreamExt;
use futures::future::BoxFuture;
use kian_cli::rotation::RuntimeRotation;
use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, InputRow, SessionId};
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_core::secrets::SecretRedactor;
use kian_orchestrator::{DrainCtx, Engine, EngineConfig, WakeOutcome};
use kian_store::{AppendGuard, EventStore};
use kian_superagent::rearm::successor_session;
use kian_superagent::{
    AdmitReceipt, InboxWriteError, InboxWriter, ResidentBinding, RouterInbox, RouterProfile,
    SessionBootstrap,
};
use serde_json::json;
use uuid::Uuid;

/// The planner never drives: an empty stream is enough for the engine.
struct TestDriver;

impl LoopDriver for TestDriver {
    type Ctx = DrainCtx;

    fn drive(&self, _ctx: DrainCtx) -> futures::stream::BoxStream<'static, LoopOutcome> {
        futures::stream::empty().boxed()
    }
}

/// The mounted residency inbox over the real engine (the judge prompt).
struct FakeInbox {
    engine: Arc<Engine<TestDriver>>,
}

impl InboxWriter for FakeInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        let engine = Arc::clone(&self.engine);
        Box::pin(async move {
            engine
                .admit_quiet(session, payload, idempotency_key)
                .await
                .map(|_| AdmitReceipt {
                    deduplicated: false,
                })
                .map_err(|e| InboxWriteError::Write(e.to_string()))
        })
    }

    fn wake(&self, _session: SessionId) -> WakeOutcome {
        WakeOutcome::Started
    }
}

/// The ADR-038 seam: the memory index, first (a quiet admit).
struct FakeBootstrap {
    engine: Arc<Engine<TestDriver>>,
}

impl SessionBootstrap for FakeBootstrap {
    fn bootstrap(&self, session: SessionId) -> BoxFuture<'static, Result<(), String>> {
        let engine = Arc::clone(&self.engine);
        Box::pin(async move {
            engine
                .admit_quiet(
                    session,
                    json!({"synthetic": true, "content": "[memory index]"}),
                    Some(format!("memory-inject:{}", session.0)),
                )
                .await
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
    }
}

fn ev(session: SessionId, seq: u64, kind: EventKind, bytes: serde_json::Value) -> Event {
    Event {
        seq,
        session,
        epoch: Epoch(1),
        ts: 1_761_900_000_000 + seq,
        kind,
        bytes,
        version: SCHEMA_VERSION,
    }
}

async fn seed_idle(store: &EventStore, live: SessionId) {
    store
        .append_batch(
            live,
            &[
                ev(live, 1, EventKind::SessionCreated, json!({})),
                ev(
                    live,
                    2,
                    EventKind::InputAdmitted,
                    json!({"role": "user", "content": "the first task"}),
                ),
                ev(
                    live,
                    3,
                    EventKind::DrainClaimed,
                    json!({"holder": "seed", "epoch": 1}),
                ),
                ev(live, 4, EventKind::TurnStart, json!({"iteration": 0})),
                ev(
                    live,
                    5,
                    EventKind::AssistantMessage,
                    json!({"role": "assistant", "content": {"text": "did the first task"}}),
                ),
                ev(live, 6, EventKind::TurnEnd, json!({})),
                ev(
                    live,
                    7,
                    EventKind::SessionEnded {
                        reason: EndReason::Interrupted,
                    },
                    json!({}),
                ),
            ],
            AppendGuard {
                epoch: Epoch(1),
                expected_seq: 1,
            },
        )
        .await
        .expect("seed stream");
}

fn engine(store: &EventStore) -> Arc<Engine<TestDriver>> {
    Engine::new(
        store.clone(),
        Arc::new(TestDriver),
        EngineConfig {
            holder: "rotation-test".into(),
            lease_ttl_ms: 30_000,
            heartbeat_interval_ms: 1_000,
        },
    )
}

/// One replica's planner + binding over the shared store/engine.
fn replica(
    engine: &Arc<Engine<TestDriver>>,
    pinned: SessionId,
    live: SessionId,
    threshold: u64,
) -> (RuntimeRotation, Arc<ResidentBinding>) {
    let binding = ResidentBinding::new(live);
    let router_inbox = RouterInbox::new(
        Arc::new(FakeInbox {
            engine: Arc::clone(engine),
        }) as Arc<dyn InboxWriter>,
        Arc::clone(&binding),
    );
    let profile = RouterProfile::new(live, kian_superagent::router_template("mock", "mock-model"));
    let planner = RuntimeRotation::new(
        pinned,
        Arc::clone(&binding),
        threshold,
        1_000,
        profile,
        Some(Arc::new(FakeBootstrap {
            engine: Arc::clone(engine),
        })),
        router_inbox,
        SecretRedactor::default(),
    );
    (planner, binding)
}

fn content_text(ev: &Event) -> String {
    let content = ev.bytes.get("content").cloned().unwrap_or_default();
    if let Some(text) = content.as_str() {
        return text.to_string();
    }
    content
        .get("content")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

/// The successor's first drain reads the admitted rows in order; poll
/// until the drain has consumed them (the planner wakes the engine).
async fn successor_inputs(store: &EventStore, successor: SessionId, at_least: usize) -> Vec<Event> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let events = store.rebuild(successor).await.expect("successor stream");
        let inputs: Vec<Event> = events
            .iter()
            .filter(|ev| ev.kind == EventKind::InputAdmitted)
            .cloned()
            .collect();
        if inputs.len() >= at_least {
            return inputs;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the successor never drained its context: {events:#?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

async fn temp_store() -> (std::path::PathBuf, EventStore) {
    let dir = std::env::temp_dir().join(format!("kian-runtime-rotation-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let sqlite = dir.join("kian.sqlite");
    let store = EventStore::open(&sqlite).await.expect("store");
    (sqlite, store)
}

#[tokio::test]
async fn a_runtime_tick_retires_the_router_and_bootstraps_the_successor() {
    let (sqlite, store) = temp_store().await;
    let _ = sqlite;
    let live = SessionId(Uuid::new_v4());
    seed_idle(&store, live).await;
    store
        .admit(InputRow {
            id: Uuid::new_v4(),
            session: live,
            payload: json!({"role": "user", "content": "the pending follow-up"}),
            admitted_at: 0,
            eligible_at: 0,
            idempotency_key: Some("pending:follow-up".into()),
        })
        .await
        .expect("seed pending row");
    let engine = engine(&store);
    let (planner, binding) = replica(&engine, live, live, 2);

    let successor = planner
        .tick(&engine, &store)
        .await
        .expect("tick")
        .expect("the over-threshold idle Router rotates");
    assert_eq!(successor, successor_session(live));
    assert_eq!(binding.load(), successor, "the binding re-points");

    // The predecessor is retired atomically and names its successor.
    let old = store.rebuild(live).await.expect("predecessor stream");
    let end = old.last().expect("an end write");
    let EventKind::SessionEnded {
        reason: EndReason::Rotated,
    } = end.kind
    else {
        panic!("the predecessor must end rotated: {:?}", end.kind);
    };
    assert_eq!(
        end.bytes.get("rotated_to").and_then(|v| v.as_str()),
        Some(successor.0.to_string().as_str())
    );

    // The successor's first drain reads the four rows in order.
    let inputs = successor_inputs(&store, successor, 4).await;
    assert_eq!(content_text(&inputs[0]), "[memory index]");
    assert_eq!(content_text(&inputs[1]), "the pending follow-up");
    assert!(
        content_text(&inputs[2]).contains("[rotation]"),
        "the handoff names the rotation: {}",
        inputs[2].bytes
    );
    assert!(
        content_text(&inputs[3]).len() > 100,
        "the judge prompt is the successor's bootstrap: {}",
        inputs[3].bytes
    );
}

/// ADR-047 (a): a second replica whose binding still points at the
/// retired session derives the live successor from the durable chain,
/// re-points, and migrates the row it wrote to the frozen predecessor.
#[tokio::test]
async fn a_second_replica_catches_up_and_migrates_its_straggler() {
    let (sqlite, store) = temp_store().await;
    let _ = sqlite;
    let live = SessionId(Uuid::new_v4());
    seed_idle(&store, live).await;
    let engine = engine(&store);
    let (planner_a, binding_a) = replica(&engine, live, live, 2);
    let (planner_b, binding_b) = replica(&engine, live, live, 1_000);

    let successor = planner_a
        .tick(&engine, &store)
        .await
        .expect("tick a")
        .expect("replica A rotates");
    assert_eq!(binding_a.load(), successor);

    // Replica B has not ticked yet: its stale binding admits to the now
    // frozen predecessor (the multi-replica lost-work window).
    let straggler = Uuid::new_v4();
    store
        .admit(InputRow {
            id: straggler,
            session: live,
            payload: json!({"role": "user", "content": "written by a stale replica"}),
            admitted_at: 0,
            eligible_at: 0,
            idempotency_key: Some("stale:write".into()),
        })
        .await
        .expect("straggler");

    assert!(
        planner_b
            .tick(&engine, &store)
            .await
            .expect("tick b")
            .is_none(),
        "a catch-up is not a rotation"
    );
    assert_eq!(binding_b.load(), successor, "replica B re-points");
    let rows = store
        .eligible_inputs(successor, u64::MAX / 2)
        .await
        .expect("successor inbox");
    let migrated = rows
        .iter()
        .find(|row| row.payload.get("content") == Some(&json!("written by a stale replica")))
        .expect("the straggler is migrated to the successor");
    assert_eq!(
        migrated.idempotency_key,
        Some(format!("rotate:{}:{}", live.0, straggler))
    );
}

/// ADR-047 (b): a rotation whose owner died between the close and the
/// successor's bootstrap is repaired by any replica (every admit is
/// idempotent by its durable key).
#[tokio::test]
async fn a_replica_repairs_a_rotation_that_never_bootstrapped() {
    let (sqlite, store) = temp_store().await;
    let _ = sqlite;
    let live = SessionId(Uuid::new_v4());
    seed_idle(&store, live).await;
    let engine = engine(&store);
    let successor = successor_session(live);
    // The winner closed the predecessor and died before the bootstrap.
    engine
        .rotate_idle(
            live,
            json!({"rotated_to": successor.0.to_string(), "events": 7}),
        )
        .await
        .expect("close");
    let (planner, binding) = replica(&engine, live, live, 2);

    assert!(
        planner.tick(&engine, &store).await.expect("tick").is_none(),
        "a repair is not a rotation"
    );
    assert_eq!(binding.load(), successor, "the repair re-points");
    let inputs = successor_inputs(&store, successor, 3).await;
    assert_eq!(content_text(&inputs[0]), "[memory index]");
    assert!(
        content_text(&inputs[1]).contains("[rotation]"),
        "the repair admits the handoff: {}",
        inputs[1].bytes
    );
    assert!(
        content_text(&inputs[2]).len() > 100,
        "the repair admits the judge prompt: {}",
        inputs[2].bytes
    );
}
