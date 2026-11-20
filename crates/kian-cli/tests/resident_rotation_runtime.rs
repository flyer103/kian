//! ADR-040 phase 2: the runtime rotation orchestration.
//!
//! A real store + a real `Engine` (a no-op test driver: the planner never
//! drives a turn) exercise one rotation tick end to end: the predecessor
//! is retired with the engine's atomic `rotated` close, the successor is
//! bound, and its context order is the memory index, the migrated pending
//! row, the handoff and the judge prompt.

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

#[tokio::test]
async fn a_runtime_tick_retires_the_router_and_bootstraps_the_successor() {
    let dir = std::env::temp_dir().join(format!("kian-runtime-rotation-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let sqlite = dir.join("kian.sqlite");
    let store = EventStore::open(&sqlite).await.expect("store");
    let live = SessionId(Uuid::new_v4());

    // An idle, resumable Router stream over the threshold plus one
    // pending row that must reach the successor.
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

    let engine = Engine::new(
        store.clone(),
        Arc::new(TestDriver),
        EngineConfig {
            holder: "rotation-test".into(),
            lease_ttl_ms: 30_000,
            heartbeat_interval_ms: 1_000,
        },
    );
    let binding = ResidentBinding::new(live);
    let router_inbox = RouterInbox::new(
        Arc::new(FakeInbox {
            engine: Arc::clone(&engine),
        }) as Arc<dyn InboxWriter>,
        Arc::clone(&binding),
    );
    let profile = RouterProfile::new(live, kian_superagent::router_template("mock", "mock-model"));
    let planner = RuntimeRotation {
        binding: Arc::clone(&binding),
        threshold: 2,
        period_ms: 1_000,
        profile,
        bootstrap: Some(Arc::new(FakeBootstrap {
            engine: Arc::clone(&engine),
        })),
        router_inbox,
        redactor: SecretRedactor::default(),
    };

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

    // The successor's first drain is woken by the planner; its
    // pre-drive take reads the four rows in order: memory index, the
    // migrated pending row, the handoff, the judge prompt.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let inputs: Vec<Event> = loop {
        let events = store.rebuild(successor).await.expect("successor stream");
        let inputs: Vec<Event> = events
            .iter()
            .filter(|ev| ev.kind == EventKind::InputAdmitted)
            .cloned()
            .collect();
        if inputs.len() >= 4 {
            break inputs;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the successor never drained its context: {events:#?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };
    let content_text = |ev: &Event| -> String {
        let content = ev.bytes.get("content").cloned().unwrap_or_default();
        if let Some(text) = content.as_str() {
            return text.to_string();
        }
        content
            .get("content")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
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
