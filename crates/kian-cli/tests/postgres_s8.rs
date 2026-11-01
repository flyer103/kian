//! S8 — the multi-replica acceptance e2e (card C52, round-4 plan §3:
//! "S1 lifted to multi-process scale").
//!
//! Two `kian serve` replicas over ONE postgres database: a submitted
//! task is claimed and drained by replica A; A is `kill -9`'d
//! mid-provider-turn; replica B's recovery sweep takes over the
//! expired lease (C47 conditional claim), replays, and completes —
//! zero lost work, exactly one end write, and a stale-epoch probe
//! write "from A" (A's frozen epoch, issued directly against the
//! store) is rejected: `NoSplitBrainWrite` between processes.
//!
//! Env-gated on `KIAN_PG_URL` (absent → SKIP with a log line, never
//! fail — the C44 pattern). Child processes are driven the way
//! `chaos_s1`/`restart_serve` drive them; localhost runs under
//! `NO_PROXY` (AGENTS.md). Lease timing is bounded, not sleep-raced:
//! takeover is bounded by `lease_ttl_ms + heartbeat_interval_ms` and
//! every wait is a deadline-poll.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{force_no_proxy, spawn_mock_provider, temp_dir, wait_for};
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::SessionId;
use kian_store::{AppendGuard, EventStore, StoreError};

/// True when the e2e should run; prints the skip line otherwise.
fn gated() -> bool {
    match std::env::var("KIAN_PG_URL") {
        Ok(url) if !url.is_empty() => true,
        _ => {
            eprintln!(
                "SKIP: KIAN_PG_URL is not set — S8 multi-replica e2e disabled (C52 env gate)"
            );
            false
        }
    }
}

/// `postgres://…/db?opts` → `postgres://…/<other-db>?opts`.
fn with_db(base: &str, db: &str) -> String {
    let (path_part, query) = match base.split_once('?') {
        Some((p, q)) => (p, format!("?{q}")),
        None => (base, String::new()),
    };
    let scheme_end = path_part.find("://").map(|i| i + 3).unwrap_or(0);
    let rest = &path_part[scheme_end..];
    let root = match rest.find('/') {
        Some(i) => path_part[..scheme_end + i].to_string(),
        None => path_part.to_string(),
    };
    format!("{root}/{db}{query}")
}

/// One replica's config: postgres storage over `KIAN_C52_PG_URL`, a
/// unique explicit holder (C47 condition (c) — config validation
/// refuses the default under postgres), and a short lease so B's
/// takeover is bounded by `ttl + heartbeat`.
fn write_replica_config(
    dir: &Path,
    file: &str,
    provider_base: &str,
    listen: &str,
    holder: &str,
) -> PathBuf {
    let yaml = format!(
        r#"listen: "{listen}"
agent:
  id: s8-agent
  model_slots:
    main:
      provider: mock
      model: mock-model
  tools: []
providers:
  mock:
    base_url: "{provider_base}"
    api_key_env: KIAN_E2E_API_KEY
drain:
  holder: {holder}
  max_iterations: 8
  lease_ttl_ms: 1500
  heartbeat_interval_ms: 500
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
storage:
  event_store: postgres
  postgres:
    url_env: KIAN_C52_PG_URL
"#
    );
    let path = dir.join(file);
    std::fs::write(&path, yaml).expect("write replica kian.yaml");
    path
}

fn spawn_kian_serve(config: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_kian"))
        .args(["serve", "--config", config.to_str().unwrap()])
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .env("KIAN_E2E_API_KEY", "test-key")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn kian serve")
}

/// Binds a probe listener to reserve a free port, then releases it.
async fn free_port() -> u16 {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    port
}

async fn wait_ready(base: &str) -> bool {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
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

/// S8: two replicas, one postgres DB, kill -9 the drain owner
/// mid-provider-turn, the survivor's sweep takes over and completes,
/// and the killed replica's frozen epoch can never write again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s8_two_replicas_kill9_takeover_no_split_brain() {
    force_no_proxy();
    if !gated() {
        return;
    }

    // Throwaway database shared by both replicas (one DB, two
    // processes — the hosted topology this card exists for).
    let base = std::env::var("KIAN_PG_URL").unwrap();
    let db = format!("kian_cli_s8_{}", uuid::Uuid::new_v4().simple());
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&base)
        .await
        .expect("connect KIAN_PG_URL");
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create throwaway database");
    let db_url = with_db(&base, &db);
    unsafe {
        std::env::set_var("KIAN_C52_PG_URL", &db_url);
    }

    // Gated mock: request #1 (replica A's provider turn) holds until
    // released; request #2 (replica B's re-drive) answers normally.
    let (provider_base, mock) = spawn_mock_provider(1).await;

    let dir = temp_dir("s8");
    let port_a = free_port().await;
    let port_b = free_port().await;
    let config_a = write_replica_config(
        &dir,
        "replica-a.yaml",
        &provider_base,
        &format!("127.0.0.1:{port_a}"),
        "s8-replica-a",
    );
    let config_b = write_replica_config(
        &dir,
        "replica-b.yaml",
        &provider_base,
        &format!("127.0.0.1:{port_b}"),
        "s8-replica-b",
    );
    let base_a = format!("http://127.0.0.1:{port_a}");
    let base_b = format!("http://127.0.0.1:{port_b}");

    // -- boot both replicas on the same database ---------------------------
    let mut replica_a = spawn_kian_serve(&config_a);
    assert!(wait_ready(&base_a).await, "replica A never became ready");
    let mut replica_b = spawn_kian_serve(&config_b);
    assert!(wait_ready(&base_b).await, "replica B never became ready");

    // Direct store reader for the assertions (independent of both
    // processes — the database is the truth, constraint 1).
    let reader = EventStore::open_postgres(&db_url)
        .await
        .expect("open reader");

    let client = reqwest::Client::new();
    let out = rpc(&client, &base_a, "create_session", serde_json::json!({})).await;
    let sid = SessionId(out["result"]["session"].as_str().unwrap().parse().unwrap());

    // A claim and drains the task; its provider turn parks on the gate.
    let out = rpc(
        &client,
        &base_a,
        "submit",
        serde_json::json!({"session": sid.0.to_string(), "content": "task A"}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false);
    assert!(
        wait_for(15_000, || mock.requests.load(Ordering::SeqCst) >= 1).await,
        "mock provider never saw replica A's request"
    );

    // A's frozen epoch: the highest epoch A committed before the kill.
    let events_before = reader.rebuild(sid).await.unwrap();
    let epoch_a = events_before
        .iter()
        .map(|e| e.epoch.0)
        .max()
        .expect("A committed events");

    // -- kill -9 replica A mid-provider-turn -------------------------------
    replica_a.kill().expect("kill -9 replica A");
    let _ = replica_a.wait();
    // Release the gate: A's hung request is dead with it; B's re-drive
    // request (#2) answers immediately.
    mock.released.store(true, Ordering::SeqCst);

    // -- B's sweep takes over the expired lease and completes --------------
    // Bounded wait: lease (1500ms) + heartbeat tick (500ms) + poll.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let out = rpc(
            &client,
            &base_b,
            "projection",
            serde_json::json!({"session": sid.0.to_string()}),
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
            "takeover must not mislabel the session; got {state}"
        );
        if tokio::time::Instant::now() >= deadline {
            panic!("replica B never completed the takeover; last state {state}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // -- the honest stream --------------------------------------------------
    let events = reader.rebuild(sid).await.unwrap();
    let kinds: Vec<&str> = events.iter().map(|e| e.kind.as_str()).collect();
    let ended: Vec<&Event> = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
        .collect();
    assert_eq!(
        ended.len(),
        1,
        "the session never projects two end writes: {kinds:?}"
    );
    assert!(
        matches!(
            ended[0].kind,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Completed
            }
        ),
        "end reason must be completed, stream: {kinds:?}"
    );
    assert_eq!(
        kinds.iter().filter(|k| **k == "input/admitted").count(),
        1,
        "the submitted input was consumed exactly once (NoLostWork): {kinds:?}"
    );
    assert!(
        kinds.iter().filter(|k| **k == "drain/claimed").count() >= 2,
        "the takeover re-claimed (A's claim + B's takeover): {kinds:?}"
    );
    let epochs: Vec<u64> = events.iter().map(|e| e.epoch.0).collect();
    assert!(
        epochs.windows(2).all(|w| w[0] <= w[1]),
        "epochs monotone across the takeover: {epochs:?}"
    );

    // -- NoSplitBrainWrite between processes --------------------------------
    // A "zombie" write with A's frozen epoch, issued directly against
    // the store at the current head: the epoch fence (not any lease)
    // rejects it — S1's guarantee, now at multi-process scale.
    let head = reader.head(sid).await.unwrap();
    assert!(
        head.epoch.0 > epoch_a,
        "B's takeover issued a higher epoch ({} vs A's {})",
        head.epoch.0,
        epoch_a
    );
    let probe = Event {
        seq: head.next_seq,
        session: sid,
        epoch: kian_core::model::Epoch(epoch_a),
        ts: 0,
        kind: EventKind::AssistantMessage,
        bytes: serde_json::json!({"content": "zombie write from dead replica A"}),
        version: SCHEMA_VERSION,
    };
    let denied = reader
        .append(
            sid,
            probe,
            AppendGuard {
                epoch: kian_core::model::Epoch(epoch_a),
                expected_seq: head.next_seq,
            },
        )
        .await;
    match denied {
        Err(StoreError::StaleEpoch { .. }) => {}
        other => {
            panic!("stale-epoch probe with A's frozen epoch must be StaleEpoch, got {other:?}")
        }
    }
    // And nothing landed: the head is unchanged.
    assert_eq!(reader.head(sid).await.unwrap().next_seq, head.next_seq);

    let _ = replica_b.kill();
    let _ = replica_b.wait();
    reader.close().await;
    let _ = sqlx::query(&format!("DROP DATABASE {db}"))
        .execute(&admin)
        .await;
    admin.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}
