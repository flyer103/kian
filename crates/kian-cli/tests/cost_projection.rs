//! E2E acceptance for `kian cost`: a real store plus a real config
//! `pricing:` book, projected into exact micro-currency totals. Timestamps
//! are chosen inside and outside a peak window to prove attribution to the
//! attempt, not to the settle time.

mod common;

use common::{temp_dir, write_config};
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_store::{AppendGuard, EventStore};
use serde_json::{Value, json};

fn ts(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> u64 {
    use chrono::TimeZone;
    chrono::Utc
        .with_ymd_and_hms(y, mo, d, h, mi, 0)
        .unwrap()
        .timestamp_millis() as u64
}

async fn push_at(store: &EventStore, session: SessionId, ts: u64, kind: EventKind, bytes: Value) {
    let head = store.head(session).await.expect("head");
    store
        .append(
            session,
            Event {
                seq: head.next_seq,
                session,
                epoch: Epoch(1),
                ts,
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

/// One turn whose step/request names mock/mock-model at the attempt
/// timestamp and whose turn/end settles later.
async fn seed_turn(store: &EventStore, session: SessionId, attempt: u64) {
    push_at(store, session, 1, EventKind::SessionCreated, json!({})).await;
    push_at(
        store,
        session,
        2,
        EventKind::InputAdmitted,
        json!({"role": "user", "content": "hello"}),
    )
    .await;
    push_at(store, session, 3, EventKind::DrainClaimed, json!({})).await;
    push_at(
        store,
        session,
        4,
        EventKind::TurnStart,
        json!({"iteration": 0}),
    )
    .await;
    push_at(
        store,
        session,
        attempt,
        EventKind::StepRequest,
        json!({"slot": "main", "provider": "mock", "model": "mock-model",
               "iteration": 0, "body": {}}),
    )
    .await;
    push_at(
        store,
        session,
        attempt + 3_600_000,
        EventKind::TurnEnd,
        json!({"iteration": 0, "outcome": "final_answer", "usage": {
            "prompt_tokens": 1000, "completion_tokens": 10,
            "prompt_cache_hit_tokens": 800, "prompt_cache_miss_tokens": 200
        }}),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cost_cli_prices_attempts_from_the_config_book() {
    use std::io::Write;

    let dir = temp_dir("cost-e2e");
    let sqlite = dir.join("kian.sqlite");
    let config_path = write_config(&dir, &sqlite, "http://127.0.0.1:9", "127.0.0.1:0");
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&config_path)
        .expect("open config");
    file.write_all(
        br#"pricing:
  currency: USD
  peak_multiplier: 2.0
  peak_windows:
    - { weekdays: [mon], start: "01:00", end: "04:00", timezone: UTC }
  models:
    mock/mock-model: { cache_hit: 0.003, cache_miss: 0.15, output: 0.6 }
"#,
    )
    .expect("append pricing");
    drop(file);

    let off_peak = ts(2026, 11, 22, 12, 0);
    let peak = ts(2026, 11, 23, 2, 0);
    let store = EventStore::open(&sqlite).await.expect("open store");
    let a = SessionId::new();
    seed_turn(&store, a, off_peak).await;
    let b = SessionId::new();
    seed_turn(&store, b, peak).await;
    store.close().await;

    let json_out = kian_cli::cost::run_cost(&config_path, None, true)
        .await
        .expect("cost json");
    let report: Value = serde_json::from_str(&json_out).expect("json");
    assert_eq!(report["sessions"], json!(2));
    assert_eq!(report["requests_with_usage"], json!(2));
    assert_eq!(report["priced_requests"], json!(2));
    assert_eq!(report["unpriced_requests"], json!(0));
    assert_eq!(report["cache_hit_tokens"], json!(1600));
    assert_eq!(report["cache_miss_tokens"], json!(400));
    assert_eq!(report["output_tokens"], json!(20));
    assert_eq!(report["peak_requests"], json!(1));
    assert_eq!(report["off_peak_requests"], json!(1));
    assert_eq!(report["currency"], json!("USD"));
    assert_eq!(report["cost_micros"], json!(115));

    let text = kian_cli::cost::run_cost(&config_path, Some(a), false)
        .await
        .expect("cost text");
    assert!(text.contains("priced requests         1"), "{text}");
    assert!(text.contains("off-peak requests       1"), "{text}");
    assert!(
        text.contains("cost                    USD 0.000038"),
        "{text}"
    );
    assert!(!text.contains("unpriced models"), "{text}");

    std::fs::remove_dir_all(&dir).ok();
}
