//! L2 for ADR-067: an oversized request is classified as
//! `request_too_large` against a REAL `kian serve` process.
//!
//! Two shapes, both driven over HTTP against a real child process and
//! observed in that process's own journal (stderr):
//!
//! - the provider answers 413: one attempt, the journal names the class
//!   and the status, and no retry reaches the provider;
//! - the provider declares `max_request_bytes` and the serialized body
//!   is over it: nothing leaves the process (the provider sees zero
//!   requests) and the journal says so.
//!
//! Runs with `NO_PROXY=127.0.0.1,localhost` (AGENTS.md gotcha).

mod common;

use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{
    ConfigTuning, force_no_proxy, spawn_mock_provider, temp_dir, write_config, write_config_with,
};

/// A mock provider that refuses every request with 413 and counts the
/// arrivals — the shape a carrier takes when the body is over its bound.
async fn spawn_refusing_provider(
    limit: u16,
) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    async fn refuse(
        axum::extract::State((count, status)): axum::extract::State<(
            std::sync::Arc<std::sync::atomic::AtomicUsize>,
            u16,
        )>,
    ) -> axum::response::Response {
        count.fetch_add(1, Ordering::SeqCst);
        (
            axum::http::StatusCode::from_u16(status).expect("status"),
            "{\"error\":{\"message\":\"request entity too large\"}}",
        )
            .into_response()
    }
    use axum::response::IntoResponse;
    let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let app = axum::Router::new()
        .route("/chat/completions", axum::routing::post(refuse))
        .with_state((count.clone(), limit));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock");
    let addr = listener.local_addr().expect("mock addr");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("mock provider serve");
    });
    (format!("http://{addr}"), count)
}

async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    listener.local_addr().expect("addr").port()
}

/// Spawns a real `kian serve` child whose stdout carries the tracing
/// journal (`tracing_subscriber::fmt` writes there) and whose stderr
/// carries panics/refusals; both land beside the test.
fn spawn_serve(config: &std::path::Path, log: &std::path::Path) -> std::process::Child {
    let out = std::fs::File::create(log).expect("journal file");
    let err = std::fs::File::create(log.with_extension("stderr")).expect("stderr file");
    std::process::Command::new(env!("CARGO_BIN_EXE_kian"))
        .args(["serve", "--config", config.to_str().unwrap()])
        .stdout(std::process::Stdio::from(out))
        .stderr(std::process::Stdio::from(err))
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .env("KIAN_E2E_API_KEY", "test-key")
        .spawn()
        .expect("spawn kian serve")
}

fn journal(log: &std::path::Path) -> String {
    let mut text = std::fs::read_to_string(log).unwrap_or_default();
    text.push_str(&std::fs::read_to_string(log.with_extension("stderr")).unwrap_or_default());
    text
}

/// Waits until the child's API accepts connections. A child that exits
/// first fails the test with its journal attached.
async fn wait_ready(base: &str, child: &mut std::process::Child, log: &std::path::Path) {
    let addr = base.trim_start_matches("http://");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        if let Some(status) = child.try_wait().expect("try_wait") {
            panic!(
                "kian serve exited ({status}) before listening:\n{}",
                journal(log)
            );
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "kian serve did not listen within 30s:\n{}",
            journal(log)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Polls the journal until it contains `needle`; returns the whole text.
async fn wait_journal(log: &std::path::Path, needle: &str, timeout_ms: u64) -> String {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        let text = journal(log);
        if text.contains(needle) {
            return text;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the journal never carried {needle:?}:\n{text}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn rpc(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{base}/rpc"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .expect("rpc send");
    assert_eq!(resp.status(), 200);
    resp.json().await.expect("rpc json")
}

async fn admit_one_turn(client: &reqwest::Client, base: &str, content: &str) -> String {
    let out = rpc(client, base, "create_session", serde_json::json!({})).await;
    let sid = out["result"]["session"]
        .as_str()
        .expect("session id")
        .to_owned();
    let out = rpc(
        client,
        base,
        "submit",
        serde_json::json!({"session": sid, "content": content, "idempotency_key": "adr-067"}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false);
    sid
}

fn shutdown(mut child: std::process::Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_413_answer_ends_the_turn_as_request_too_large_without_a_retry() {
    force_no_proxy();
    let dir = temp_dir("r413");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, arrivals) = spawn_refusing_provider(413).await;
    let port = free_port().await;
    let config = write_config(&dir, &sqlite, &provider_base, &format!("127.0.0.1:{port}"));
    let log = dir.join("serve.log");
    let mut child = spawn_serve(&config, &log);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &mut child, &log).await;

    let client = reqwest::Client::new();
    admit_one_turn(&client, &base, "Say all done.").await;

    let text = wait_journal(&log, "provider error (request_too_large)", 20_000).await;
    assert!(
        text.contains("HTTP 413"),
        "the journal must name the status the provider refused with:\n{text}"
    );
    assert!(
        text.contains("request entity too large"),
        "the provider's own words must survive into the class:\n{text}"
    );
    assert_eq!(
        arrivals.load(Ordering::SeqCst),
        1,
        "a 413 is not retried: the same context cannot succeed on a resend"
    );

    shutdown(child);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_configured_bound_refuses_before_any_byte_crosses_the_wire() {
    force_no_proxy();
    let dir = temp_dir("rbound");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, mock) = spawn_mock_provider(0).await;
    let port = free_port().await;
    let config = write_config_with(
        &dir,
        "kian.yaml",
        &sqlite,
        &provider_base,
        &format!("127.0.0.1:{port}"),
        &ConfigTuning {
            max_request_bytes: Some(1024),
            ..ConfigTuning::default()
        },
    );
    let log = dir.join("serve.log");
    let mut child = spawn_serve(&config, &log);
    let base = format!("http://127.0.0.1:{port}");
    wait_ready(&base, &mut child, &log).await;

    let client = reqwest::Client::new();
    // Well over the 1024-byte bound with the protocol preamble alone; the
    // long content makes the refusal independent of that preamble.
    admit_one_turn(&client, &base, &"x".repeat(4096)).await;

    let text = wait_journal(&log, "no request byte left this process", 20_000).await;
    assert!(
        text.contains("request_too_large"),
        "the local refusal keeps its own class, never provider_down:\n{text}"
    );
    assert!(
        text.contains("1024"),
        "the journal must name the configured bound:\n{text}"
    );
    assert_eq!(
        mock.requests.load(Ordering::SeqCst),
        0,
        "the bound is checked before the first byte: the provider must see nothing"
    );

    shutdown(child);
    let _ = std::fs::remove_dir_all(&dir);
}
