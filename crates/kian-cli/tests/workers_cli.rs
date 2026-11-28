//! `kian workers` L1 tests (R12-04 / kian-130): the read-only operator
//! verb's durable view — state derivation per spec D-L2, text/`--json`
//! agreement (parse both, assert the same facts), sorted-by-id
//! determinism, and the never-refuses contract on stores that predate
//! remote exec.

use std::path::PathBuf;

use futures::StreamExt;
use futures::stream::BoxStream;
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_orchestrator::{DrainCtx, Engine, EngineConfig, ManualClock};
use kian_store::{AppendGuard, EventStore};
use serde_json::json;
use uuid::Uuid;

use kian_cli::ops::OpsError;

fn temp_db() -> PathBuf {
    std::env::temp_dir().join(format!("kian-workers-cli-{}.sqlite", Uuid::new_v4()))
}

/// Seeds a registry through a real (write-mode, then closed) store —
/// the durable projection a running serve would leave behind.
async fn seeded_registry() -> PathBuf {
    let db = temp_db();
    let store = kian_exec::ExecStore::open(&db).await.unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    // Fresh online row, stale online row (reads offline per D-L2), and
    // an already-offline row.
    store
        .upsert_endpoint_with_declaration(
            "builder-1",
            "127.0.0.1:50001",
            now,
            "online",
            Some("linux builder"),
            2,
            &[],
        )
        .await
        .unwrap();
    store
        .upsert_endpoint("mac-mini", "127.0.0.1:50002", now - 10 * 60_000, "online")
        .await
        .unwrap();
    store
        .upsert_endpoint("zbox", "127.0.0.1:50003", now - 60_000, "offline")
        .await
        .unwrap();
    store.close().await;
    db
}

/// One `kian workers` row as the text form renders it (`id state`).
fn text_facts(text: &str) -> Vec<(String, String)> {
    text.lines()
        .skip(1) // header
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut parts = line.split_whitespace();
            let id = parts.next().expect("id column").to_string();
            let state = parts.next().expect("state column").to_string();
            (id, state)
        })
        .collect()
}

/// Text and `--json` carry the same rows (same ids, same states, same
/// order). This read-only process has no live connection, so registry rows
/// are offline even when their cached heartbeat is fresh.
#[tokio::test]
async fn workers_text_and_json_agree() {
    let db = seeded_registry().await;
    let config = std::path::Path::new("no-such-kian.yaml"); // degrades honestly

    let json_text = kian_cli::commands::run_workers(config, Some(&db), true)
        .await
        .expect("json form never refuses");
    let plain_text = kian_cli::commands::run_workers(config, Some(&db), false)
        .await
        .expect("text form never refuses");

    let report: serde_json::Value = serde_json::from_str(&json_text).unwrap();
    let rows = report["workers"].as_array().expect("workers array");
    assert_eq!(rows.len(), 3, "sorted, nothing pruned (M5)");
    let json_facts: Vec<(String, String)> = rows
        .iter()
        .map(|row| {
            (
                row["id"].as_str().expect("id").to_string(),
                row["state"].as_str().expect("state").to_string(),
            )
        })
        .collect();
    // Sorted by id ascending (byte order).
    let mut sorted = json_facts.clone();
    sorted.sort();
    assert_eq!(json_facts, sorted, "id-ascending order");

    // A cached registry state and heartbeat cannot establish membership
    // in this process.
    assert_eq!(
        json_facts,
        vec![
            ("builder-1".into(), "offline".into()),
            ("mac-mini".into(), "offline".into()),
            ("zbox".into(), "offline".into()),
        ]
    );
    // ADR-025: the last-declared cache is visible for the worker that
    // declared it; workers with no declaration keep the fields absent.
    for row in rows {
        assert!(row["bound_session"].is_null());
        assert!(row["bound_thread"].is_null());
        match row["id"].as_str().unwrap() {
            "builder-1" => {
                assert_eq!(row["description"], "linux builder");
                assert_eq!(row["protocol_version"], 2);
            }
            _ => {
                assert!(row.get("description").is_none());
                assert!(row.get("protocol_version").is_none());
            }
        }
    }

    // The text board carries exactly the same facts.
    assert_eq!(text_facts(&plain_text), json_facts);
}

/// Never refuses: a database file that never carried the exec tables
/// (predates remote exec) prints an empty board and exits 0.
#[tokio::test]
async fn workers_never_refuses_on_a_legacy_store() {
    let db = temp_db();
    std::fs::write(&db, b"").expect("empty sqlite file placeholder");
    // (A zero-byte file is not a valid sqlite db — create a real empty
    // one through the event store's sqlite driver instead.)
    let empty = {
        let store = kian_store::EventStore::open(&db).await.unwrap();
        store.close().await;
        db.clone()
    };
    let config = std::path::Path::new("no-such-kian.yaml");
    let text = kian_cli::commands::run_workers(config, Some(&empty), false)
        .await
        .expect("never refuses");
    assert!(text.contains("no workers registered"), "was: {text}");
    let json_text = kian_cli::commands::run_workers(config, Some(&empty), true)
        .await
        .expect("never refuses");
    let report: serde_json::Value = serde_json::from_str(&json_text).unwrap();
    assert_eq!(report["workers"].as_array().map(Vec::len), Some(0));
}

/// The read-only contract: running the verb twice changes no rows —
/// the registry content is identical before and after both forms.
#[tokio::test]
async fn workers_verb_writes_nothing() {
    let db = seeded_registry().await;
    let config = std::path::Path::new("no-such-kian.yaml");
    let before = {
        let exec = kian_exec::ExecStore::open_read_only(&db).await.unwrap();
        let rows = exec.endpoints().await.unwrap();
        exec.close().await;
        rows
    };
    for json in [true, false] {
        kian_cli::commands::run_workers(config, Some(&db), json)
            .await
            .expect("never refuses");
    }
    let after = {
        let exec = kian_exec::ExecStore::open_read_only(&db).await.unwrap();
        let rows = exec.endpoints().await.unwrap();
        exec.close().await;
        rows
    };
    assert_eq!(before, after, "the verb writes nothing");
}

struct NoopReleaseDriver;

impl LoopDriver for NoopReleaseDriver {
    type Ctx = DrainCtx;

    fn drive(&self, _ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        futures::stream::once(async { LoopOutcome::Settled }).boxed()
    }
}

async fn append(
    store: &EventStore,
    session: SessionId,
    seq: u64,
    kind: EventKind,
    bytes: serde_json::Value,
) {
    store
        .append(
            session,
            Event {
                seq,
                session,
                epoch: Epoch(0),
                ts: 1,
                kind,
                bytes,
                version: SCHEMA_VERSION,
            },
            AppendGuard {
                epoch: Epoch(0),
                expected_seq: seq,
            },
        )
        .await
        .unwrap();
}

/// An eager-created child with no accepted input is a never-started
/// reservation. Release reports `worker_not_bound`, even after its lease,
/// rather than treating the reservation as a busy session.
#[tokio::test]
async fn release_reports_worker_not_bound_for_an_expired_reservation() {
    let events_db = temp_db();
    let exec_db = temp_db();
    let store = EventStore::open(&events_db).await.unwrap();
    let exec = kian_exec::ExecStore::open(&exec_db).await.unwrap();
    let engine = Engine::with_clock(
        store.clone(),
        std::sync::Arc::new(NoopReleaseDriver),
        EngineConfig {
            holder: "test-holder".into(),
            lease_ttl_ms: 30_000,
            heartbeat_interval_ms: 10_000,
        },
        std::sync::Arc::new(ManualClock::new(1)),
    );
    let parent = SessionId::new();
    let child = SessionId::new();
    let thread = Uuid::new_v4();
    append(&store, parent, 1, EventKind::SessionCreated, json!({})).await;
    append(
        &store,
        parent,
        2,
        EventKind::ThreadStarted,
        json!({
            "thread_id": thread.to_string(),
            "parent": parent.0.to_string(),
            "worker": child.0.to_string(),
            "pinned_version": 1,
            "task": {"role": "user", "content": "delegate"},
            "exec_worker": "builder-1",
        }),
    )
    .await;
    append(&store, child, 1, EventKind::SessionCreated, json!({})).await;
    let before = store.head(child).await.unwrap().next_seq;

    let error =
        kian_cli::worker_release::release_worker(&*engine, &store, &exec, "builder-1", None)
            .await
            .expect_err("never-started reservation is not releasable");
    assert!(
        matches!(
            error,
            OpsError::WorkerNotBound { ref worker, .. } if worker == "builder-1"
        ),
        "{error:?}"
    );
    assert_eq!(store.head(child).await.unwrap().next_seq, before);

    exec.close().await;
    store.close().await;
    let _ = std::fs::remove_file(events_db);
    let _ = std::fs::remove_file(exec_db);
}
