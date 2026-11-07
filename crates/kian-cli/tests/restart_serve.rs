//! preview at API level: kill -9 `kian serve` mid-session
//! (mid provider turn, with a queued-but-unconsumed input), restart it,
//! and prove the startup inbox rebuild lets the drain continue to an
//! honest `completed` — zero lost work.

mod common;

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{force_no_proxy, spawn_mock_provider, temp_dir, wait_for, write_config};

fn spawn_kian_serve(config: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_kian"))
        .args(["serve", "--config", config.to_str().unwrap()])
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .env(common::KEY_ENV, "test-key")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn kian serve")
}

/// Waits until the server answers an RPC (readiness probe).
async fn wait_ready(base: &str) -> bool {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let resp = client
            .post(format!("{base}/rpc"))
            .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "create_session", "params": {}}))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill_and_restart_serve_rebuilds_inbox_and_drain_continues() {
    force_no_proxy();

    let dir = temp_dir("restart");
    let sqlite = dir.join("kian.sqlite");

    // Gated mock: request #1 (from the doomed process) holds forever;
    // after the kill, request #2 (from the restarted process) completes.
    let (provider_base, mock) = spawn_mock_provider(1).await;

    // A fixed free port for the child server.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let base = format!("http://127.0.0.1:{port}");

    let config = write_config(&dir, &sqlite, &provider_base, &format!("127.0.0.1:{port}"));

    // -- boot #1 ----------------------------------------------------------
    let mut child = spawn_kian_serve(&config);
    assert!(
        wait_ready(&base).await,
        "first serve process never became ready"
    );
    let client = reqwest::Client::new();

    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid = out["result"]["session"].as_str().unwrap().to_owned();

    // Input A: admitted; its drain reaches the provider and hangs (gated).
    let out = rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({"session": sid, "content": "task A"}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false);

    // Wait until the drain is truly mid-provider-turn.
    assert!(
        wait_for(15_000, || mock.requests.load(Ordering::SeqCst) >= 1).await,
        "mock provider never saw the first request"
    );

    // Input B: queued while the drain is in flight; the row is durable
    // but NOT yet consumed — the work the inbox rebuild must not lose.
    let out = rpc(
        &client,
        &base,
        "queue",
        serde_json::json!({"session": sid, "content": "task B"}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false);

    // -- kill -9 mid-session ----------------------------------------------
    child.kill().expect("kill -9");
    let _ = child.wait();

    // At this point the session is mid-turn with a queued input; nothing
    // terminal has been written (no session/ended event exists).
    {
        use kian_store::EventStore;
        let store = EventStore::open(&sqlite).await.expect("reopen store");
        let sid = kian_core::model::SessionId(sid.parse().unwrap());
        let events = store.rebuild(sid).await.expect("rebuild");
        assert!(
            events
                .iter()
                .all(|e| !matches!(e.kind, kian_core::events::EventKind::SessionEnded { .. })),
            "no session/ended may exist before the kill lands a terminal state"
        );
        assert!(
            events
                .iter()
                .any(|e| matches!(e.kind, kian_core::events::EventKind::TurnStart)),
            "the doomed process must have reached a provider turn"
        );
    }

    // -- boot #2 on the same store ----------------------------------------
    let mut child2 = spawn_kian_serve(&config);
    assert!(
        wait_ready(&base).await,
        "restarted serve process never became ready"
    );

    // The restarted drain retries the provider; release the gate so the
    // hung request #1 semantics are moot and request #2 completes.
    mock.released.store(true, Ordering::SeqCst);

    // Poll the projection until it honestly reaches completed.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let out = rpc(
            &client,
            &base,
            "projection",
            serde_json::json!({"session": sid}),
        )
        .await;
        let state = out["result"]["state"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        if state == "completed" {
            break;
        }
        assert!(
            state != "failed" && state != "cancelled",
            "restart must not mislabel the session; got {state}"
        );
        if tokio::time::Instant::now() >= deadline {
            panic!("session never reached completed; last state {state}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The event stream tells the whole honest story: both inputs
    // admitted, the kill left no half-terminated state, and exactly one
    // end write with reason completed.
    {
        use kian_store::EventStore;
        let store = EventStore::open(&sqlite).await.expect("reopen store 2");
        let sid = kian_core::model::SessionId(sid.parse().unwrap());
        let events = store.rebuild(sid).await.expect("rebuild 2");
        let kinds: Vec<&str> = events.iter().map(|e| e.kind.as_str()).collect();
        let ended: Vec<_> = events
            .iter()
            .filter(|e| matches!(e.kind, kian_core::events::EventKind::SessionEnded { .. }))
            .collect();
        assert_eq!(ended.len(), 1, "exactly one end write, got kinds {kinds:?}");
        assert!(
            matches!(
                ended[0].kind,
                kian_core::events::EventKind::SessionEnded {
                    reason: kian_core::events::EndReason::Completed
                }
            ),
            "end reason must be completed, stream: {kinds:?}"
        );
        assert_eq!(
            kinds.iter().filter(|k| **k == "input/admitted").count(),
            2,
            "both queued inputs must be consumed (NoLostWork): {kinds:?}"
        );
        // The restarted process re-claimed (new epoch) and re-drove the loop.
        assert!(
            kinds.iter().filter(|k| **k == "drain/claimed").count() >= 2,
            "restart must re-claim: {kinds:?}"
        );
        assert!(
            kinds.iter().filter(|k| **k == "turn/start").count() >= 2,
            "the drain must continue after restart: {kinds:?}"
        );
        // Epoch fencing held: every post-restart write carries a higher
        // epoch than the killed process's claim.
        let epochs: Vec<u64> = events.iter().map(|e| e.epoch.0).collect();
        assert!(
            epochs.windows(2).all(|w| w[0] <= w[1]),
            "epochs are monotone across the restart: {epochs:?}"
        );
    }

    let _ = child2.kill();
    let _ = child2.wait();
    let _ = std::fs::remove_dir_all(&dir);
}
