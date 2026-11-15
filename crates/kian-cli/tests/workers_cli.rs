//! `kian workers` L1 tests (R12-04 / kian-130): the read-only operator
//! verb's durable view — state derivation per spec D-L2, text/`--json`
//! agreement (parse both, assert the same facts), sorted-by-id
//! determinism, and the never-refuses contract on stores that predate
//! remote exec.

use std::path::PathBuf;

use uuid::Uuid;

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
/// order); `online` requires the row's freshness conjunction (D-L2).
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

    // The D-L2 derivation: fresh+online → online; stale+online →
    // offline; offline → offline. Default bound 30s (no config).
    assert_eq!(
        json_facts,
        vec![
            ("builder-1".into(), "online".into()),
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
