//! C32 acceptance: the C14 `session replay` A2 re-derivation passes over
//! streams containing the spill pair (`input/admitted` + `input/spilled`
//! consumed from one `{"content","spill"}` envelope row).
//!
//! The stream is built through the real engine surface (`DrainCtx`) so
//! the pair is produced by the consumption write itself, then re-derived
//! through the same `kian_cli::commands::run_replay` entry point the
//! binary calls. No HTTP, no credentials, no proxy concerns.

use std::path::PathBuf;
use std::sync::Arc;

use kian_cli::commands::run_replay;
use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, InputRow, SessionId};
use kian_loop::react::to_chat_message;
use kian_orchestrator::{DrainCtx, ManualClock, Metrics};
use kian_store::EventStore;
use serde_json::json;
use uuid::Uuid;

const T0: u64 = 1_761_900_000_000;

fn temp_db_path() -> PathBuf {
    std::env::temp_dir().join(format!("kian-spill-replay-test-{}.sqlite", Uuid::new_v4()))
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

fn row(sid: SessionId, id: u128, payload: serde_json::Value) -> InputRow {
    InputRow {
        id: Uuid::from_u128(id),
        session: sid,
        payload,
        admitted_at: T0,
        eligible_at: T0,
        idempotency_key: Some(format!("key-{id}")),
    }
}

/// `session replay` over a stream that contains the spill pair: the
/// projection re-derives, the invariants hold, and the A2 model-context
/// check — the recorded `step/request` body is byte-identical to the
/// re-derived context, which must NOT contain the sidecar — passes.
#[tokio::test]
async fn session_replay_rederives_streams_containing_the_spill_pair() {
    let path = temp_db_path();
    let store = EventStore::open(&path).await.expect("open store");
    let sid = SessionId::new();

    let created = Event {
        seq: 1,
        session: sid,
        epoch: Epoch::GENESIS,
        ts: T0,
        kind: EventKind::SessionCreated,
        bytes: json!({}),
        version: SCHEMA_VERSION,
    };
    store
        .append(
            sid,
            created,
            kian_store::AppendGuard {
                epoch: Epoch::GENESIS,
                expected_seq: 1,
            },
        )
        .await
        .expect("bootstrap");
    let ctx = DrainCtx::new(
        store.clone(),
        sid,
        Epoch(1),
        Arc::new(ManualClock::new(T0)),
        Arc::new(Metrics::default()),
    )
    .await
    .expect("drain ctx");
    ctx.append(EventKind::DrainClaimed, json!({}))
        .await
        .unwrap();

    // The C32 envelope row: the Inline excerpt as content, the full raw
    // as the opaque sidecar.
    let sidecar = json!({
        "event_id": "evt-1",
        "channel": "mock",
        "topic": null,
        "raw": {"text": "the full verbatim channel payload"}
    });
    store
        .admit(row(
            sid,
            1,
            json!({"content": "inline excerpt", "spill": sidecar.clone()}),
        ))
        .await
        .expect("admit envelope row");
    assert_eq!(ctx.take_eligible_inputs().await.unwrap().len(), 1);

    // One provider turn whose recorded request body is derived exactly
    // as the loop would derive it (A2 byte-identity is then checkable).
    let pre_turn = ctx.events().await.expect("pre-turn events");
    let messages = kian_core::context::derive_model_context(&pre_turn).expect("derive context");
    let chat: Vec<_> = messages.iter().filter_map(to_chat_message).collect();
    // The sidecar stayed out of the model-visible request body.
    assert!(
        !serde_json::to_string(&chat)
            .expect("chat body")
            .contains("the full verbatim channel payload"),
        "the spilled raw never enters model context"
    );
    ctx.begin_turn().await.expect("turn/start");
    ctx.append(EventKind::StepRequest, json!({"body": {"messages": chat}}))
        .await
        .expect("step/request");
    ctx.append(
        EventKind::AssistantMessage,
        json!({"role": "assistant", "content": "done"}),
    )
    .await
    .expect("assistant/message");
    ctx.end_turn().await.expect("turn/end");
    ctx.end_session(EndReason::Completed)
        .await
        .expect("end write");
    store.close().await;

    let out = run_replay(
        std::path::Path::new("kian.yaml-does-not-exist.yaml"),
        Some(&path),
        None,
        sid,
        true,
    )
    .await
    .expect("replay runs");
    let report: serde_json::Value = serde_json::from_str(&out).expect("replay json");

    assert_eq!(report["invariants_ok"], json!(true), "report: {report}");
    assert_eq!(
        report["derived_state"],
        json!("completed"),
        "report: {report}"
    );
    assert_eq!(report["event_count"], json!(9), "report: {report}");
    assert_eq!(
        report["model_context"]["requests_checked"],
        json!(1),
        "report: {report}"
    );
    assert_eq!(
        report["model_context"]["byte_identical"],
        json!(1),
        "the recorded request replays byte-identically over the pair: {report}"
    );
    assert!(
        report["model_context"]["mismatches"]
            .as_array()
            .expect("mismatches array")
            .is_empty(),
        "report: {report}"
    );
    cleanup(&path);
}
