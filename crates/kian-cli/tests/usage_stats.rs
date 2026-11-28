//! E2E acceptance for the usage surfaces: `kian session inspect`
//! usage lines, `kian usage-stats` (text + JSON, deterministic) and the
//! `GET /metrics` `"usage"` aggregate — all derived from the same
//! seeded store carrying usage-bearing `turn/end` events.
//!
//! The store is seeded directly through the real `EventStore` (the
//! provider→loop→`turn/end` pipeline is proven in kian-loop's
//! integration tests; here the surfaces are under test). Runs with
//! `NO_PROXY=127.0.0.1,localhost` (AGENTS.md gotcha).

mod common;

use std::sync::Arc;

use common::{force_no_proxy, spawn_mock_provider, temp_dir, write_config};
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
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

/// Writes a minimal session stream whose `turn/end` events carry the
/// given usage payloads (verbatim, exactly what the loop would append).
async fn seed_session(store: &EventStore, session: SessionId, usages: &[Value]) {
    push_event(store, session, EventKind::SessionCreated, json!({})).await;
    push_event(
        store,
        session,
        EventKind::InputAdmitted,
        json!({"role": "user", "content": "hello"}),
    )
    .await;
    // Turns are drain activity: a real stream brackets them inside a
    // claim (the projection rejects turn events from Idle).
    push_event(store, session, EventKind::DrainClaimed, json!({})).await;
    for (i, usage) in usages.iter().enumerate() {
        push_event(
            store,
            session,
            EventKind::TurnStart,
            json!({"iteration": i}),
        )
        .await;
        push_event(
            store,
            session,
            EventKind::TurnEnd,
            json!({"iteration": i, "outcome": "final_answer", "usage": usage.clone()}),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn usage_surfaces_inspect_stats_and_metrics_over_a_seeded_store() {
    force_no_proxy();
    let dir = temp_dir("c67");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config_path = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:0");

    // Seed: session A with one openai-spelled usage turn; session B with
    // one anthropic-spelled usage turn and one unknown spelling.
    let store = EventStore::open(&sqlite).await.expect("open store");
    let a = SessionId::new();
    seed_session(
        &store,
        a,
        &[json!({
            "prompt_tokens": 11, "completion_tokens": 4,
            "prompt_tokens_details": {"cached_tokens": 3}
        })],
    )
    .await;
    let b = SessionId::new();
    seed_session(
        &store,
        b,
        &[
            json!({"input_tokens": 100, "output_tokens": 50,
                   "cache_read_input_tokens": 60, "cache_creation_input_tokens": 7}),
            json!({"exotic": "spelling"}),
        ],
    )
    .await;
    store.close().await;

    // -- 1. `session inspect` renders the usage lines only when observed.
    let out = kian_cli::commands::run_inspect(
        std::path::Path::new("kian.yaml-does-not-exist.yaml"),
        Some(&sqlite),
        a,
        false,
    )
    .await
    .expect("inspect");
    assert!(out.contains("usage input tokens   11"), "{out}");
    assert!(out.contains("usage output tokens  4"), "{out}");
    assert!(out.contains("usage cache read     3"), "{out}");
    assert!(out.contains("usage cache miss     8"), "{out}");
    assert!(out.contains("usage cache creation 0"), "{out}");
    assert!(out.contains("usage requests       1 (0 unknown)"), "{out}");

    // JSON: the usage key appears when observed …
    let out = kian_cli::commands::run_inspect(
        std::path::Path::new("kian.yaml-does-not-exist.yaml"),
        Some(&sqlite),
        b,
        true,
    )
    .await
    .expect("inspect json");
    let inspect: Value = serde_json::from_str(&out).expect("json");
    assert_eq!(inspect["usage"]["requests_with_usage"], json!(2));
    assert_eq!(inspect["usage"]["unknown_usage_requests"], json!(1));
    assert_eq!(inspect["usage"]["input_tokens"], json!(100));
    assert_eq!(inspect["usage"]["cache_miss_input_tokens"], json!(100));
    assert_eq!(inspect["usage"]["cache_creation_input_tokens"], json!(7));

    // … and is absent for a usage-free session.
    let store = EventStore::open(&sqlite).await.expect("reopen store");
    let plain = SessionId::new();
    seed_session(&store, plain, &[]).await;
    store.close().await;
    let out = kian_cli::commands::run_inspect(
        std::path::Path::new("kian.yaml-does-not-exist.yaml"),
        Some(&sqlite),
        plain,
        true,
    )
    .await
    .expect("inspect json");
    let inspect: Value = serde_json::from_str(&out).expect("json");
    assert!(inspect.get("usage").is_none(), "{inspect}");
    let out = kian_cli::commands::run_inspect(
        std::path::Path::new("kian.yaml-does-not-exist.yaml"),
        Some(&sqlite),
        plain,
        false,
    )
    .await
    .expect("inspect text");
    assert!(!out.contains("usage"), "{out}");

    // -- 2. `kian usage-stats`: text and JSON, deterministic across runs.
    let text1 = kian_cli::usage_metrics::run_usage_stats(&config_path, None, false)
        .await
        .expect("usage-stats text");
    let text2 = kian_cli::usage_metrics::run_usage_stats(&config_path, None, false)
        .await
        .expect("usage-stats text again");
    assert_eq!(text1, text2, "text output must be deterministic");
    assert!(
        text1.contains("sessions                3")
            && text1.contains("requests with usage     3")
            && text1.contains("unknown usage requests  1")
            && text1.contains("input tokens            111")
            && text1.contains("output tokens           54")
            && text1.contains("cache read tokens       63")
            && text1.contains("cache miss tokens       108")
            && text1.contains("cache hit rate          36.8%")
            && text1.contains("cache creation tokens   7"),
        "{text1}"
    );
    let json1 = kian_cli::usage_metrics::run_usage_stats(&config_path, None, true)
        .await
        .expect("usage-stats json");
    let json2 = kian_cli::usage_metrics::run_usage_stats(&config_path, None, true)
        .await
        .expect("usage-stats json again");
    assert_eq!(json1, json2, "json output must be deterministic");
    let report: Value = serde_json::from_str(&json1).expect("json");
    assert_eq!(
        report,
        json!({
            "sessions": 3,
            "requests_with_usage": 3,
            "unknown_usage_requests": 1,
            "input_tokens": 111,
            "output_tokens": 54,
            "cache_read_input_tokens": 63,
            "cache_miss_input_tokens": 108,
            "cache_creation_input_tokens": 7,
        })
    );

    // -- 3. `GET /metrics` carries the `"usage"` aggregate object.
    let config = kian_cli::config::ServeConfig::load(&config_path).expect("config loads");
    let server = Arc::new(
        kian_cli::server::Server::from_config(config)
            .await
            .expect("server"),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let base = format!("http://{}", listener.local_addr().unwrap());
    let router = server.router();
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    let metrics: Value = reqwest::Client::new()
        .get(format!("{base}/metrics?full=1"))
        .send()
        .await
        .expect("metrics")
        .json()
        .await
        .expect("metrics json");
    assert_eq!(metrics["usage"], report, "{}", metrics);
    assert!(metrics.get("engine").is_some(), "{}", metrics);

    std::fs::remove_dir_all(&dir).ok();
}
