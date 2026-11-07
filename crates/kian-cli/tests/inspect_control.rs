//! E2E acceptance for the operator surface: `kian session inspect`
//! shows the `pending control` line (and the
//! `counters.pending_control_inputs` JSON key) for a session holding an
//! unconsumed durable interrupt-intent row, and omits both otherwise.
//! Read-only projection over a seeded store (the admit→consume pipeline
//! is proven in kian-orchestrator's engine tests; here the CLI surface
//! is under test). Runs with `NO_PROXY=127.0.0.1,localhost` (AGENTS.md
//! gotcha).

mod common;

use common::{force_no_proxy, spawn_mock_provider, temp_dir};
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, InputRow, SessionId};
use kian_store::{AppendGuard, EventStore};
use serde_json::{Value, json};

/// Appends one event at the session's current head.
async fn push_event(store: &EventStore, session: SessionId, kind: EventKind, bytes: Value) {
    let head = store.head(session).await.expect("head");
    store
        .append(
            session,
            Event {
                seq: head.next_seq,
                session,
                epoch: Epoch(1),
                ts: 1_762_000_000_000 + head.next_seq,
                kind,
                bytes,
                version: SCHEMA_VERSION,
            },
            AppendGuard {
                epoch: Epoch(1),
                expected_seq: head.next_seq,
            },
        )
        .await
        .expect("append");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inspect_reports_pending_control_inputs_from_the_inbox() {
    force_no_proxy();
    let dir = temp_dir("c68");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let _config_path = common::write_config(&dir, &sqlite, &provider_base, "127.0.0.1:0");

    let store = EventStore::open(&sqlite).await.expect("open store");

    // A session with a pending durable interrupt intent: running
    // stream plus one reserved control-envelope row.
    let busy = SessionId::new();
    push_event(&store, busy, EventKind::SessionCreated, json!({})).await;
    push_event(
        &store,
        busy,
        EventKind::InputAdmitted,
        json!({"role": "user", "content": "deploy"}),
    )
    .await;
    push_event(&store, busy, EventKind::DrainClaimed, json!({})).await;
    let intent = kian_core::control::InterruptIntent {
        reason: Some("stop the deploy".into()),
        requested_by: Some("ops".into()),
        requested_at_ms: 1_762_000_000_000,
        target_epoch: Some(1),
    };
    store
        .admit(InputRow {
            id: uuid::Uuid::new_v4(),
            session: busy,
            payload: intent.envelope(),
            admitted_at: 1_762_000_000_000,
            eligible_at: 1_762_000_000_000,
            idempotency_key: Some(kian_core::control::InterruptIntent::idempotency_key(
                &busy,
                Some(1),
            )),
        })
        .await
        .expect("admit intent");

    // A quiet session: stream, no pending rows of any kind.
    let quiet = SessionId::new();
    push_event(&store, quiet, EventKind::SessionCreated, json!({})).await;
    store.close().await;

    // -- text: the pending control line appears only when one exists.
    let out = kian_cli::commands::run_inspect(
        std::path::Path::new("kian.yaml-does-not-exist.yaml"),
        Some(&sqlite),
        busy,
        false,
    )
    .await
    .expect("inspect busy");
    assert!(out.contains("pending inputs      1"), "{out}");
    assert!(out.contains("pending control     1"), "{out}");

    let out = kian_cli::commands::run_inspect(
        std::path::Path::new("kian.yaml-does-not-exist.yaml"),
        Some(&sqlite),
        quiet,
        false,
    )
    .await
    .expect("inspect quiet");
    assert!(out.contains("pending inputs      0"), "{out}");
    assert!(!out.contains("pending control"), "{out}");

    // -- JSON: the additive counter key is always present.
    let out = kian_cli::commands::run_inspect(
        std::path::Path::new("kian.yaml-does-not-exist.yaml"),
        Some(&sqlite),
        busy,
        true,
    )
    .await
    .expect("inspect busy json");
    let report: Value = serde_json::from_str(&out).expect("json");
    assert_eq!(report["counters"]["pending_control_inputs"], json!(1));
    assert_eq!(report["counters"]["pending_eligible_inputs"], json!(1));

    let out = kian_cli::commands::run_inspect(
        std::path::Path::new("kian.yaml-does-not-exist.yaml"),
        Some(&sqlite),
        quiet,
        true,
    )
    .await
    .expect("inspect quiet json");
    let report: Value = serde_json::from_str(&out).expect("json");
    assert_eq!(report["counters"]["pending_control_inputs"], json!(0));

    std::fs::remove_dir_all(&dir).ok();
}
