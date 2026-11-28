//! L2: a supervisor stop (SIGTERM) releases the drain it interrupts
//! (ADR-077). The process is parked mid provider turn against a gated mock
//! and asked to stop; it must interrupt the drain, write its own
//! interrupted end, and exit successfully — leaving no un-ended claim for
//! the next process's crash-loop counter (ADR-041).

#![cfg(unix)]

mod common;

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{force_no_proxy, spawn_mock_provider, temp_dir, wait_for, write_config};

fn spawn_kian_serve(config: &Path, log: &Path) -> Child {
    // tracing_subscriber::fmt writes to stdout by default (the serve log).
    let stdout = std::fs::File::create(log).expect("create serve log");
    Command::new(env!("CARGO_BIN_EXE_kian"))
        .args(["serve", "--config", config.to_str().unwrap()])
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .env(common::KEY_ENV, "test-key")
        .env("RUST_LOG", "info")
        .stdout(stdout)
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn kian serve")
}

async fn wait_ready(base: &str) -> bool {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let resp = client
            .get(format!("{base}/healthz"))
            .timeout(Duration::from_secs(2))
            .send()
            .await;
        if let Ok(r) = resp
            && r.status().as_u16() == 200
        {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn rpc(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    client
        .post(format!("{base}/rpc"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("rpc send")
        .json()
        .await
        .expect("rpc json")
}

/// Polls the child until it exits; returns its status, or None when it is
/// still alive at the deadline.
fn wait_exit(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => return Some(status),
            None if std::time::Instant::now() >= deadline => return None,
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_interrupts_the_drain_and_ends_the_session_interrupted() {
    force_no_proxy();

    let dir = temp_dir("shutdown");
    let sqlite = dir.join("kian.sqlite");
    let log = dir.join("serve.log");

    // The mock holds its first request forever: the doomed process is parked
    // inside a provider turn when the stop arrives, so only the hard cut can
    // release it.
    let (provider_base, mock) = spawn_mock_provider(1).await;

    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let base = format!("http://127.0.0.1:{port}");
    let config = write_config(&dir, &sqlite, &provider_base, &format!("127.0.0.1:{port}"));

    let mut child = spawn_kian_serve(&config, &log);
    assert!(wait_ready(&base).await, "serve never became ready");
    let client = reqwest::Client::new();

    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid = out["result"]["session"].as_str().unwrap().to_owned();
    let out = rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({"session": sid, "content": "task A"}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false);
    assert!(
        wait_for(15_000, || mock.requests.load(Ordering::SeqCst) >= 1).await,
        "the mock provider never saw the request: the drain is not mid-turn"
    );

    // -- the supervisor's stop ---------------------------------------------
    let pid = child.id().to_string();
    let killed = Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("run kill");
    assert!(killed.success(), "kill -TERM dispatch failed");

    let status = wait_exit(&mut child, Duration::from_secs(20));
    match status {
        Some(status) => assert!(status.success(), "a graceful stop exits 0: {status}"),
        None => {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the process ignored SIGTERM for 20 s (the drain was not cut)");
        }
    }

    // The process must have taken the shutdown path, not died by signal.
    let serve_log = std::fs::read_to_string(&log).unwrap_or_default();
    assert!(
        serve_log.contains("shutdown: interrupting every in-flight drain"),
        "the stop went through the shutdown path, log: {serve_log}"
    );

    // -- the stream after the stop: one claim, its own interrupted end ------
    {
        use kian_core::events::{EndReason, EventKind};
        use kian_core::state::{SessionState, project};
        use kian_store::EventStore;
        let store = EventStore::open(&sqlite).await.expect("reopen store");
        let sid = kian_core::model::SessionId(sid.parse().unwrap());
        let events = store.rebuild(sid).await.expect("rebuild");
        let kinds: Vec<&str> = events.iter().map(|e| e.kind.as_str()).collect();
        assert!(
            kinds.contains(&"turn/start"),
            "the drain must have reached a provider turn: {kinds:?}"
        );
        assert_eq!(
            kinds.iter().filter(|k| **k == "drain/claimed").count(),
            1,
            "one claim: {kinds:?}"
        );
        assert_eq!(
            events.last().map(|e| e.kind),
            Some(EventKind::SessionEnded {
                reason: EndReason::Interrupted
            }),
            "the stop must end the claim interrupted, not leave it un-ended: {kinds:?}"
        );
        assert!(
            !events.iter().any(|e| matches!(
                e.kind,
                EventKind::SessionEnded {
                    reason: EndReason::Failed | EndReason::Completed
                }
            )),
            "a graceful stop never writes a failed or completed end: {kinds:?}"
        );
        assert_eq!(
            project(&events).expect("projection"),
            SessionState::Idle,
            "the interruption releases ownership cleanly"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
