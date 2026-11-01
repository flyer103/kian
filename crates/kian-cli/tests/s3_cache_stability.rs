//! S3 cache-byte stability smoke (card C17, acceptance scenario S3).
//!
//! Within one session driven end-to-end through the real server
//! (scripted mock provider over the openai-compat wire format):
//!
//! 1. consecutive `step/request` audit copies share a byte-identical
//!    message prefix (`prefix_stable_pairs == compared_pairs`,
//!    `unexpected_prefix_change` stays zero) — even with tool
//!    transcripts, turns and a mid-drain queued input between them;
//! 2. `GET /metrics` exposes the cache report and the engine counters;
//! 3. `kian cache-stats --json` (the real binary) reports the same
//!    numbers;
//! 4. a crafted session containing a `compact` event shows up under
//!    the `compact` invalidation cause and only under it.
//!
//! The unit tests in `src/cache_metrics.rs` prove the compact and
//! violation counting directly; this file proves it against the
//! assembled system.
//!
//! The fixtures below (no-proxy env, kian.yaml writer, temp dir) are
//! deliberately local: including `tests/common/mod.rs` would dead-code
//! its mock-provider half inside this binary (clippy `-D warnings`
//! fails the gate), and sibling cards are extending that shared file
//! concurrently.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use serde_json::Value;

/// A scripted openai-compat endpoint:
/// - request 1 answers with one tool call,
/// - request 2 is held until `released` (the fault window in which the
///   test lands a queued input mid-drain), then answers with text,
/// - request 3+ answer with text.
#[derive(Default)]
struct ScriptedProvider {
    requests: AtomicUsize,
    released: AtomicBool,
}

fn sse(frames: &[Value]) -> String {
    let mut body = String::new();
    for f in frames {
        body.push_str(&format!("data: {}\n\n", serde_json::to_string(f).unwrap()));
    }
    body
}

async fn completions(
    State(mock): State<Arc<ScriptedProvider>>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let n = mock.requests.fetch_add(1, Ordering::SeqCst) + 1;
    let _ = body;
    if n == 2 {
        // Hold mid-provider-turn (bounded well above any test timeout)
        // until the test has queued its input.
        for _ in 0..800 {
            if mock.released.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    let body_sse = if n == 1 {
        sse(&[
            serde_json::json!({"choices":[{"delta":{"tool_calls":[
                {"index":0,"id":"call-1","function":{"name":"fs.read","arguments":"{\"path\":\"notes.txt\"}"}}
            ]}}]}),
            serde_json::json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
        ])
    } else {
        sse(&[
            serde_json::json!({"choices":[{"delta":{"content":"All done."}}]}),
            serde_json::json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
        ])
    };
    (
        StatusCode::OK,
        [("content-type", "text/event-stream")],
        body_sse,
    )
}

async fn spawn_scripted_provider() -> (String, Arc<ScriptedProvider>) {
    let mock = Arc::new(ScriptedProvider::default());
    let app = Router::new()
        .route("/chat/completions", post(completions))
        .with_state(Arc::clone(&mock));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock");
    let addr = listener.local_addr().expect("mock addr");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("mock provider serve");
    });
    (format!("http://{addr}"), mock)
}

async fn rpc(client: &reqwest::Client, base: &str, method: &str, params: Value) -> Value {
    let resp = client
        .post(format!("{base}/rpc"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .expect("rpc call");
    let body: Value = resp.json().await.expect("rpc json");
    assert!(body.get("error").is_none(), "rpc {method} failed: {body}");
    body["result"].clone()
}

// -- local fixtures (shape shared with tests/common/mod.rs) ----------------

/// The environment variable holding the (dummy) API key.
const KEY_ENV: &str = "KIAN_E2E_API_KEY";

/// Forces the no-proxy environment for localhost (must run before any
/// reqwest client is built; AGENTS.md macOS proxy gotcha).
fn force_no_proxy() {
    unsafe {
        std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
        std::env::set_var("no_proxy", "127.0.0.1,localhost");
        std::env::set_var(KEY_ENV, "test-key");
    }
}

/// Writes a kian.yaml pointing at the mock provider; returns its path.
fn write_config(dir: &Path, sqlite: &Path, provider_base: &str, listen: &str) -> PathBuf {
    let yaml = format!(
        r#"listen: "{listen}"
storage:
  sqlite: "{{}}"
agent:
  id: e2e-agent
  model_slots:
    main:
      provider: mock
      model: mock-model
  tools: []
providers:
  mock:
    base_url: "{provider_base}"
    api_key_env: {KEY_ENV}
drain:
  max_iterations: 8
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#
    );
    let path = dir.join("kian.yaml");
    let yaml = yaml.replace("{}", &sqlite.display().to_string());
    std::fs::write(&path, yaml).expect("write kian.yaml");
    path
}

/// A fresh temp dir.
fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("kian-c17-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

/// Polls `cond` every 25 ms until it holds or `timeout_ms` elapses.
async fn wait_for<F>(timeout_ms: u64, mut cond: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Drives one full session over HTTP and asserts the prefix identity
/// of consecutive requests, the metrics endpoint and the CLI binary.
#[tokio::test]
async fn s3_consecutive_requests_share_prefix_and_metrics_expose_it() {
    force_no_proxy();
    let dir = temp_dir("s3");
    let sqlite = dir.join("s3.db");
    let (provider_base, mock) = spawn_scripted_provider().await;
    let config_path = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:0");

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
    tokio::spawn(async move {
        axum::serve(listener, server.router()).await.expect("serve");
    });

    let client = reqwest::Client::new();

    // -- 1. one session: submit → tool turn → (queued input mid-turn) →
    //       final answer twice.
    let session = rpc(&client, &base, "create_session", serde_json::json!({})).await["session"]
        .as_str()
        .expect("session uuid")
        .to_string();
    rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({"session": session, "content": "read notes.txt and summarize"}),
    )
    .await;

    // Wait until the provider is holding request #2 mid-turn, then
    // land a queued input (tail append — must never touch the prefix)
    // and release.
    assert!(
        wait_for(5_000, || mock.requests.load(Ordering::SeqCst) >= 2).await,
        "provider never saw request 2"
    );
    rpc(
        &client,
        &base,
        "queue",
        serde_json::json!({"session": session, "content": "and translate the summary"}),
    )
    .await;
    mock.released.store(true, Ordering::SeqCst);

    // Wait for completion (honest terminal state), polling the
    // projection read like any other client.
    let mut completed = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        let state = rpc(
            &client,
            &base,
            "projection",
            serde_json::json!({"session": session}),
        )
        .await;
        if state["state"].as_str() == Some("completed") {
            completed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(completed, "session never completed");
    assert!(mock.requests.load(Ordering::SeqCst) >= 3);

    // -- 2. S3: every consecutive request pair shares the byte prefix,
    //       and nothing but a compact counts as an invalidation.
    let report = kian_cli::cache_metrics::run_cache_stats(&config_path, None, true)
        .await
        .expect("cache stats");
    let value: Value = serde_json::from_str(&report).expect("json output");
    assert_eq!(value["requests"].as_u64(), Some(3), "{value}");
    assert_eq!(value["compared_pairs"].as_u64(), Some(2), "{value}");
    assert_eq!(value["prefix_stable_pairs"].as_u64(), Some(2), "{value}");
    assert_eq!(value["parse_errors"].as_u64(), Some(0), "{value}");
    assert!(
        value["invalidation_causes"].as_object().unwrap().is_empty(),
        "no invalidation cause may fire without a compact: {value}"
    );

    // The prefix is really byte-identity, not a vacuous pass: rebuild
    // the per-request message bytes from the store and compare hashes.
    let store = kian_store::EventStore::open(&sqlite).await.expect("store");
    let session_id = kian_core::model::SessionId(session.parse().unwrap());
    let events = store.rebuild(session_id).await.expect("events");
    let requests: Vec<&kian_core::events::Event> = events
        .iter()
        .filter(|e| matches!(e.kind, kian_core::events::EventKind::StepRequest))
        .collect();
    assert!(requests.len() >= 3, "expected >= 3 step/request events");
    let msg_bytes: Vec<Vec<Vec<u8>>> = requests
        .iter()
        .map(|e| {
            e.bytes
                .pointer("/body/messages")
                .and_then(Value::as_array)
                .expect("audit copy has body.messages")
                .iter()
                .map(|m| serde_json::to_vec(m).unwrap())
                .collect()
        })
        .collect();
    // Request 2 extends request 1; request 3 extends request 2 — and
    // each extension strictly grows (the assertion would be vacuous
    // otherwise).
    for w in 0..msg_bytes.len().saturating_sub(1) {
        let (prev, next) = (&msg_bytes[w], &msg_bytes[w + 1]);
        assert!(prev.len() < next.len(), "request {} did not grow", w + 1);
        for i in 0..prev.len() {
            assert_eq!(
                prev[i],
                next[i],
                "prefix message {i} changed between request {} and {}",
                w + 1,
                w + 2
            );
        }
    }
    store.close().await;

    // -- 3. GET /metrics exposes the cache report and engine counters.
    let metrics: Value = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics")
        .json()
        .await
        .expect("metrics json");
    assert_eq!(metrics["cache"]["requests"].as_u64(), Some(3), "{metrics}");
    assert_eq!(metrics["cache"]["prefix_stable_pairs"].as_u64(), Some(2));
    assert!(
        metrics["engine"]["drains_started"].as_u64().unwrap_or(0) >= 1,
        "engine counters present: {metrics}"
    );

    // -- 4. the real binary reports the same numbers.
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_kian"))
        .arg("cache-stats")
        .arg("--config")
        .arg(&config_path)
        .arg("--json")
        .output()
        .expect("run kian cache-stats");
    assert!(out.status.success(), "cache-stats failed: {out:?}");
    let bin: Value = serde_json::from_slice(&out.stdout).expect("binary json output");
    assert_eq!(bin["prefix_stable_pairs"].as_u64(), Some(2), "{bin}");
    assert_eq!(bin["compared_pairs"].as_u64(), Some(2));

    std::fs::remove_dir_all(&dir).ok();
}

/// A session whose stream contains a `compact` event reports the
/// invalidation under the `compact` cause — and the queued-input
/// session above proves nothing else fires. Crafted directly against
/// the store (a compact is a recovery-path event, not API-drivable in
/// Phase 1) and surfaced through the same CLI path.
#[tokio::test]
async fn s3_compact_event_bumps_the_invalidation_cause_counter() {
    force_no_proxy();
    let dir = temp_dir("s3-compact");
    let sqlite = dir.join("s3-compact.db");
    let store = kian_store::EventStore::open(&sqlite).await.expect("store");

    use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};
    use serde_json::json;

    let session = SessionId::new();
    let seq = std::cell::Cell::new(1u64);
    let push = |kind: EventKind, bytes: Value| -> Event {
        let ev = Event {
            seq: seq.get(),
            session,
            epoch: Epoch(1),
            ts: 1_000 + seq.get(),
            kind,
            bytes,
            version: SCHEMA_VERSION,
        };
        seq.set(seq.get() + 1);
        ev
    };
    let request = |msgs: Value| {
        json!({"slot": "main", "provider": "p", "model": "m", "iteration": 0,
               "body": {"model": "m", "messages": msgs, "stream": true}})
    };
    let msgs = |items: &[(&str, &str)]| {
        Value::Array(
            items
                .iter()
                .map(|(r, c)| json!({"role": r, "content": c}))
                .collect(),
        )
    };

    let mut stream = vec![push(EventKind::SessionCreated, json!({}))];
    stream.push(push(
        EventKind::StepRequest,
        request(msgs(&[("user", "q1"), ("assistant", "a1"), ("user", "q2")])),
    ));
    stream.push(push(
        EventKind::Compact,
        json!({"from_seq": 2, "to_seq": 3, "summary": {"role": "assistant", "content": "compressed"}}),
    ));
    stream.push(push(
        EventKind::StepRequest,
        request(msgs(&[
            ("user", "q1"),
            ("assistant", "compressed"),
            ("assistant", "a3"),
        ])),
    ));

    for ev in stream {
        let head = store.head(session).await.unwrap();
        store
            .append(
                session,
                ev,
                kian_store::AppendGuard {
                    epoch: Epoch(1),
                    expected_seq: head.next_seq,
                },
            )
            .await
            .expect("append");
    }
    store.close().await;

    let (provider_base, _mock) = spawn_scripted_provider().await;
    let config_path = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:0");
    let out = kian_cli::cache_metrics::run_cache_stats(&config_path, Some(session), false)
        .await
        .expect("cache stats");
    assert!(out.contains("invalidation causes     compact=1"), "{out}");
    assert!(!out.contains("unexpected"), "{out}");

    std::fs::remove_dir_all(&dir).ok();
}
