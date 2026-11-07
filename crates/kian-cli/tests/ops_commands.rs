//! E2E tests for the session commands.
//!
//! Everything runs against the REAL assembled server (fixtures from
//! 's e2e suite): the mock provider speaks the openai-compat wire
//! format, so the streams under test are produced by the true
//! engine + ReAct loop. The commands are exercised through the same
//! `kian_cli::commands` entry points the binary calls, plus one
//! real-binary smoke test per command family.
//!
//! Runs with `NO_PROXY=127.0.0.1,localhost` (AGENTS.md gotcha).

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::{force_no_proxy, spawn_mock_provider, temp_dir, write_config};
use kian_cli::commands::{
    import_jsonl, run_budget_reset, run_export, run_inspect, run_lease_inspect, run_replay,
    run_wake_now,
};
use kian_cli::server::Server;
use kian_core::events::EventKind;
use kian_core::model::SessionId;
use kian_core::state::{SessionState, project};
use kian_store::EventStore;

const DEFAULT_CONFIG: &str = "kian.yaml-does-not-exist.yaml";

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

async fn ops_rpc(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{base}/ops/rpc"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .expect("ops send");
    assert_eq!(resp.status(), 200);
    resp.json().await.expect("ops json")
}

async fn spawn_server(config_path: &Path) -> (String, Arc<Server>) {
    let server = Arc::new(
        Server::from_config_file(config_path)
            .await
            .expect("assemble server")
            .0,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = server.router();
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    (format!("http://{addr}"), server)
}

/// The full acceptance path:
/// 1. run a session to `completed` against the mock provider,
/// 2. `replay` re-derives it: derived == live, invariants ok, and the
///    A2 check — every recorded `step/request` body is byte-identical
///    to the re-derived `derive_model_context`,
/// 3. `export` → fresh-store re-import projects the identical state,
/// 4. `wake-now` distinguishes started/unknown/terminal,
/// 5. `recovery budget reset` appends the explicit event (no silent
///    wipe) — visible in `inspect` counters,
/// 6. `lease inspect` shows epoch/TTL/holder.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replay_export_wake_reset_lease_over_the_real_server() {
    force_no_proxy();
    let dir = temp_dir("c14");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:7331");
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    // -- 1. run to completion ------------------------------------------
    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid: String = out["result"]["session"].as_str().expect("sid").into();
    rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({"session": sid, "content": "Say all done.", "idempotency_key": "c14-1"}),
    )
    .await;

    // poll the projection to completion (async)
    let session_id = poll_to_completed(&client, &base, &sid).await;

    // -- 2. replay: derived == live, A2 byte-identity -------------------
    let out = run_replay(
        Path::new(DEFAULT_CONFIG),
        Some(&sqlite),
        Some(&base),
        session_id,
        true,
    )
    .await
    .expect("replay");
    let report: serde_json::Value = serde_json::from_str(&out).expect("replay json");
    assert_eq!(report["derived_state"], "completed");
    assert_eq!(report["live_state"], "completed", "live projection");
    assert_eq!(report["state_matches_live"], true);
    assert_eq!(
        report["invariants_ok"], true,
        "violations: {}",
        report["violations"]
    );
    let mc = &report["model_context"];
    assert!(
        mc["requests_checked"].as_u64().unwrap_or(0) >= 1,
        "the run made at least one model request"
    );
    assert_eq!(
        mc["byte_identical"], mc["requests_checked"],
        "every recorded request body must be byte-identical to the re-derived context (A2): {mc}"
    );
    assert_eq!(
        mc["mismatches"].as_array().map(Vec::len),
        Some(0),
        "no A2 mismatches"
    );

    // -- 3. export → fresh-store round-trip -----------------------------
    let jsonl = run_export(
        Path::new(DEFAULT_CONFIG),
        Some(&sqlite),
        session_id,
        None,
        None,
        None,
        false,
    )
    .await
    .expect("export");
    let lines: Vec<&str> = jsonl.lines().collect();
    assert!(!lines.is_empty(), "export produced lines");
    let fresh_path = dir.join("fresh.sqlite");
    let fresh = EventStore::open(&fresh_path).await.expect("fresh store");
    import_jsonl(&fresh, &jsonl).await.expect("import");
    let original = EventStore::open_read_only(&sqlite).await.expect("reader");
    let orig_events = original.rebuild(session_id).await.expect("orig events");
    let fresh_events = fresh.rebuild(session_id).await.expect("fresh events");
    assert_eq!(orig_events.len(), fresh_events.len());
    for (a, b) in orig_events.iter().zip(fresh_events.iter()) {
        assert_eq!(
            serde_json::to_string(a).unwrap(),
            serde_json::to_string(b).unwrap(),
            "exported envelopes round-trip byte-identically"
        );
    }
    assert_eq!(
        project(&orig_events).unwrap(),
        project(&fresh_events).unwrap(),
        "re-imported export projects the identical session state"
    );
    original.close().await;
    fresh.close().await;

    // -- 4. wake-now: terminal / unknown / started ----------------------
    let out = ops_rpc(
        &client,
        &base,
        "wake_now",
        serde_json::json!({"session": sid}),
    )
    .await;
    assert_eq!(
        out["error"]["code"], -32003,
        "terminal session refused: {out}"
    );

    let out = ops_rpc(
        &client,
        &base,
        "wake_now",
        serde_json::json!({"session": uuid::Uuid::new_v4().to_string()}),
    )
    .await;
    assert_eq!(out["error"]["code"], -32001, "unknown session: {out}");

    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let idle: String = out["result"]["session"].as_str().expect("sid").into();
    // CLI-level wake-now (same path the binary drives): started/folded.
    let idle_id = idle.parse::<uuid::Uuid>().unwrap();
    let idle_id = SessionId(idle_id);
    let out = run_wake_now(Path::new(DEFAULT_CONFIG), Some(&base), idle_id, true)
        .await
        .expect("cli wake-now");
    let doc: serde_json::Value = serde_json::from_str(&out).expect("wake json");
    let outcome = doc["outcome"].as_str().expect("outcome");
    assert!(
        outcome == "started" || outcome == "folded",
        "wake on an idle session spawns or folds: {doc}"
    );

    // -- 5. budget reset: explicit event, visible, neutral --------------
    // An idle session never consumed a recovery budget → honest refusal.
    let out = ops_rpc(
        &client,
        &base,
        "recovery_budget_reset",
        serde_json::json!({"session": idle, "reason": "e2e operator reset"}),
    )
    .await;
    assert_eq!(out["error"]["code"], -32004, "idle reset refused: {out}");

    // The operationally real case: a Running session orphaned mid-drain
    // (crashed after budget exhaustion, no active drain here). Write its
    // stream directly through a writer store on the same file.
    let orphan = SessionId::new();
    let writer = EventStore::open(&sqlite).await.expect("writer");
    let mk = |seq: u64, epoch: u64, kind: EventKind| kian_core::events::Event {
        seq,
        session: orphan,
        epoch: kian_core::model::Epoch(epoch),
        ts: 1,
        kind,
        bytes: serde_json::json!({}),
        version: kian_core::events::SCHEMA_VERSION,
    };
    writer
        .append(
            orphan,
            mk(1, 0, EventKind::SessionCreated),
            kian_store::AppendGuard {
                epoch: kian_core::model::Epoch(0),
                expected_seq: 1,
            },
        )
        .await
        .expect("created");
    writer
        .append(
            orphan,
            mk(2, 1, EventKind::DrainClaimed),
            kian_store::AppendGuard {
                epoch: kian_core::model::Epoch(1),
                expected_seq: 2,
            },
        )
        .await
        .expect("claimed");
    writer.close().await;

    let out = ops_rpc(
        &client,
        &base,
        "recovery_budget_reset",
        serde_json::json!({"session": orphan.0.to_string(), "reason": "e2e operator reset"}),
    )
    .await;
    assert!(out.get("result").is_some(), "reset succeeded: {out}");
    let reset_seq = out["result"]["seq"].as_u64().expect("seq");

    let reader = EventStore::open_read_only(&sqlite).await.expect("reader");
    let events = reader.rebuild(orphan).await.expect("orphan events");
    let reset_event = events
        .iter()
        .find(|e| e.seq == reset_seq)
        .expect("reset event at the receipt seq");
    assert!(matches!(
        e_kind(reset_event),
        EventKind::RecoveryBudgetReset
    ));
    assert_eq!(reset_event.bytes["reason"], "e2e operator reset");
    assert_eq!(
        project(&events).unwrap(),
        SessionState::Running,
        "the reset event is projection-neutral (state stays Running)"
    );
    reader.close().await;

    let out = run_inspect(Path::new(DEFAULT_CONFIG), Some(&sqlite), orphan, true)
        .await
        .expect("inspect");
    let inspect: serde_json::Value = serde_json::from_str(&out).expect("inspect json");
    assert_eq!(inspect["state"], "running");
    assert_eq!(inspect["counters"]["budget_resets"], 1);

    // CLI-level budget reset (same path the binary drives): explicit
    // event, honest receipt.
    let out = run_budget_reset(
        Path::new(DEFAULT_CONFIG),
        Some(&base),
        orphan,
        Some("cli reset"),
        true,
    )
    .await
    .expect("cli budget reset");
    let receipt: serde_json::Value = serde_json::from_str(&out).expect("receipt json");
    assert_eq!(receipt["seq"], serde_json::json!(reset_seq + 1));

    // -- 6. lease inspect shows epoch/TTL/holder ------------------------
    let out = run_lease_inspect(Path::new(DEFAULT_CONFIG), Some(&sqlite), true)
        .await
        .expect("lease inspect");
    let leases: serde_json::Value = serde_json::from_str(&out).expect("lease json");
    let rows = leases["leases"].as_array().expect("lease rows");
    assert!(!rows.is_empty(), "the completed session left a claims row");
    for row in rows {
        assert!(row["epoch"].as_u64().unwrap_or(0) >= 1, "epoch visible");
        assert!(row["holder"].as_str().is_some_and(|h| !h.is_empty()));
        assert!(row["ttl_ms"].as_u64().unwrap_or(0) > 0);
    }

    std::fs::remove_dir_all(&dir).ok();
}

fn e_kind(e: &kian_core::events::Event) -> EventKind {
    e.kind
}

async fn poll_to_completed(client: &reqwest::Client, base: &str, sid: &str) -> SessionId {
    let id = SessionId(sid.parse::<uuid::Uuid>().expect("sid uuid"));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let out = rpc(
            client,
            base,
            "projection",
            serde_json::json!({"session": sid}),
        )
        .await;
        let state = out["result"]["state"].as_str().unwrap_or_default();
        if state == "completed" {
            return id;
        }
        assert!(
            state != "failed" && state != "cancelled",
            "session ended {state} before completing"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "session did not complete in time (last state {state})"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The real `kian` binary smoke test: `session inspect --json`,
/// `lease inspect --json`, `session export` (stdout JSONL) and
/// `recovery budget reset` against an unreachable server (fail-closed
/// exit code) — proving the dispatch wiring end-to-end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kian_binary_session_commands_smoke() {
    force_no_proxy();
    let dir = temp_dir("c14bin");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:7331");
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid: String = out["result"]["session"].as_str().expect("sid").into();
    rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({"session": sid, "content": "Say all done.", "idempotency_key": "bin-1"}),
    )
    .await;
    poll_to_completed(&client, &base, &sid).await;

    let bin = env!("CARGO_BIN_EXE_kian");
    let run = |args: &[&str]| {
        std::process::Command::new(bin)
            .args(args)
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost")
            .output()
            .expect("run kian binary")
    };

    // session inspect --json
    let out = run(&[
        "session",
        "inspect",
        &sid,
        "--db",
        sqlite.to_str().unwrap(),
        "--json",
    ]);
    assert!(
        out.status.success(),
        "inspect: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let doc: serde_json::Value = serde_json::from_slice(&out.stdout).expect("inspect json");
    assert_eq!(doc["state"], "completed");
    assert!(doc["counters"]["requests"].as_u64().unwrap_or(0) >= 1);

    // lease inspect --json
    let out = run(&[
        "lease",
        "inspect",
        "--db",
        sqlite.to_str().unwrap(),
        "--json",
    ]);
    assert!(
        out.status.success(),
        "lease: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let doc: serde_json::Value = serde_json::from_slice(&out.stdout).expect("lease json");
    assert!(doc["leases"].as_array().is_some_and(|l| !l.is_empty()));

    // session export (stdout JSONL)
    let out = run(&["session", "export", &sid, "--db", sqlite.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "export: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let first: serde_json::Value =
        serde_json::from_str(stdout.lines().next().expect("jsonl line")).expect("event line");
    assert_eq!(first["kind"], "session/created");

    // export --since/--until interval
    let out = run(&[
        "session",
        "export",
        &sid,
        "--db",
        sqlite.to_str().unwrap(),
        "--since",
        "1",
        "--until",
        "1",
    ]);
    assert!(out.status.success());
    assert_eq!(String::from_utf8_lossy(&out.stdout).lines().count(), 1);

    // unknown session id → non-zero exit, clear error
    let out = run(&[
        "session",
        "inspect",
        &uuid::Uuid::new_v4().to_string(),
        "--db",
        sqlite.to_str().unwrap(),
        "--json",
    ]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("unknown session"));

    // budget reset without a reachable server → clear transport error
    let out = run(&[
        "recovery",
        "budget",
        "reset",
        &sid,
        "--url",
        "http://127.0.0.1:9",
        "--json",
    ]);
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("budget reset failed"));

    std::fs::remove_dir_all(&dir).ok();
}
