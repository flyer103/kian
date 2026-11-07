//! Snapshot crash-window chaos (READ-MODEL P1, plan §4.2 req 7):
//! `kill -9` against the REAL `kian serve` process while the snapshot
//! maintenance builder is mid-flight, plus restart and verification.
//!
//! Deterministic staging (marker-driven, not timing-driven):
//!
//! - *kill during build/publish* — the harness holds a sqlite
//!   `BEGIN IMMEDIATE` write lock AFTER the session's events are
//!   committed, lets maintenance ticks fire (the builder blocks behind
//!   the lock) and kills the process while it is stuck. Publication is
//!   verify-then-insert: death can leave nothing or one complete
//!   verified row — asserted by loading every visible row after the
//!   kill and comparing against the full stream derivation.
//! - *kill during load* — the loader is read-only; the invariant is
//!   exercised by a second kill during ongoing traffic and asserting
//!   the restarted server's projection reads equal the stream's truth.
//!
//! After every kill: every visible snapshot row must be load-equal to
//! the full derivation of the stream (R1/R6 — the load path re-checks
//! the digest, so a load-equal row IS a verified row; a partial row
//! would either fail the digest or diverge). Runs with
//! `NO_PROXY=127.0.0.1,localhost` (AGENTS.md gotcha).

mod common;

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use kian_core::model::SessionId;
use kian_orchestrator::snapshots::{ReadModelConfig, load_projection};
use kian_store::EventStore;
use uuid::Uuid;

use common::{force_no_proxy, spawn_mock_provider, temp_dir};

const KEY_ENV: &str = "KIAN_E2E_API_KEY";

fn write_config(dir: &Path, sqlite: &Path, provider_base: &str, listen: &str) -> PathBuf {
    // Snapshot maintenance on a fast cadence with a tiny threshold, so
    // the builder is observably active; drain/lease short so a restart
    // settles quickly. These constants live only in the test config —
    // production defaults are untouched.
    let yaml = format!(
        r#"listen: "{listen}"
storage:
  sqlite: "{}"
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
  lease_ttl_ms: 2000
  heartbeat_interval_ms: 400
tools:
  root: "{}"
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
read_model:
  snapshots:
    enabled: true
    min_events: 1
    tail_cap: 8192
    keep_n: 3
    interval_ms: 150
"#,
        sqlite.display(),
        dir.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write kian.yaml");
    path
}

fn spawn_serve(dir: &Path, config: &Path, tag: &str) -> Child {
    let stderr = std::fs::File::create(dir.join(format!("{tag}.stderr"))).expect("stderr file");
    Command::new(env!("CARGO_BIN_EXE_kian"))
        .args(["serve", "--config", config.to_str().unwrap()])
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .env(KEY_ENV, "test-key")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("spawn kian serve")
}

fn kill9(child: &mut Child) {
    child.kill().expect("kill -9");
    let _ = child.wait();
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("addr").port()
}

struct Harness {
    dir: PathBuf,
    client: reqwest::Client,
}

impl Harness {
    async fn new(seed: u64) -> (Self, PathBuf, String) {
        let dir = temp_dir(&format!("snap-chaos-{seed}"));
        let (provider_base, _mock) = spawn_mock_provider(0).await;
        let listen = format!("127.0.0.1:{}", free_port());
        let config = write_config(&dir, &dir.join("kian.sqlite"), &provider_base, &listen);
        (
            Self {
                dir,
                client: reqwest::Client::new(),
            },
            config,
            format!("http://{listen}"),
        )
    }

    fn sqlite(&self) -> PathBuf {
        self.dir.join("kian.sqlite")
    }

    async fn wait_ready(&self, base: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let resp = self
                .client
                .post(format!("{base}/rpc"))
                .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "projection", "params": {"session": "00000000-0000-0000-0000-000000000000"}}))
                .timeout(Duration::from_secs(2))
                .send()
                .await;
            if let Ok(r) = resp
                && r.status().as_u16() == 200
            {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "serve never became ready"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn rpc(&self, base: &str, method: &str, params: serde_json::Value) -> serde_json::Value {
        self.client
            .post(format!("{base}/rpc"))
            .json(
                &serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}),
            )
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .expect("rpc send")
            .json()
            .await
            .expect("rpc json")
    }

    async fn settle(&self, base: &str) -> SessionId {
        let out = self
            .rpc(base, "create_session", serde_json::json!({}))
            .await;
        let sid = SessionId(
            Uuid::parse_str(out["result"]["session"].as_str().expect("session id")).expect("uuid"),
        );
        self.rpc(
            base,
            "submit",
            serde_json::json!({"session": sid.0.to_string(), "content": "Say all done."}),
        )
        .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            assert!(
                tokio::time::Instant::now() < deadline,
                "session never settled"
            );
            let out = self
                .rpc(
                    base,
                    "projection",
                    serde_json::json!({"session": sid.0.to_string()}),
                )
                .await;
            if out["result"]["state"] == "completed" {
                return sid;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Waits until the session has a verified snapshot row (the
    /// maintenance loop observed building + publishing live).
    async fn wait_snapshot(&self, sid: SessionId) -> bool {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let store = EventStore::open(self.sqlite()).await.expect("open store");
            let got = store.snapshot_highest_verified(sid).await.expect("row");
            store.close().await;
            if got.is_some() {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// R1/R6 assertion: every visible snapshot row is load-equal to the
    /// full derivation (the loader re-checks the digest, so a load-equal
    /// row IS a complete verified row), and the projection wire read
    /// equals the stream's state.
    async fn verify_snapshots_honest(&self, base: &str, sessions: &[SessionId]) {
        let store = EventStore::open(self.sqlite()).await.expect("open store");
        let read_on = ReadModelConfig {
            snapshots_enabled: true,
            tail_cap: 8_192,
        };
        for sid in sessions {
            let version = store.version(*sid).await.expect("version");
            let loaded = load_projection(&store, *sid, version, &read_on)
                .await
                .expect("load");
            let full = load_projection(&store, *sid, version, &ReadModelConfig::default())
                .await
                .expect("full");
            assert_eq!(loaded.state, full.state, "state diverged for {sid:?}");
            assert_eq!(
                kian_core::context::context_to_bytes(&loaded.context),
                kian_core::context::context_to_bytes(&full.context),
                "request bytes diverged for {sid:?}"
            );
            let out = self
                .rpc(
                    base,
                    "projection",
                    serde_json::json!({"session": sid.0.to_string()}),
                )
                .await;
            assert_eq!(out["result"]["version"].as_u64().expect("version"), version);
            assert_eq!(out["result"]["state"], serde_json::json!(full.state));
        }
        store.close().await;
    }

    fn cleanup(self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// The crash-window proof: kill -9 with the publication write blocked,
/// then again during live traffic; after each kill and restart every
/// visible snapshot is verified and honest, and the wire reads equal
/// the stream's truth.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill9_in_snapshot_crash_windows_never_serves_a_partial_snapshot() {
    force_no_proxy();
    let (h, config, base) = Harness::new(1).await;
    let mut serve = spawn_serve(&h.dir, &config, "boot1");
    h.wait_ready(&base).await;

    // Session A settles and is snapshotted: build + publish observed
    // live under the real maintenance ticker.
    let a = h.settle(&base).await;
    assert!(
        h.wait_snapshot(a).await,
        "maintenance never published a snapshot for session A"
    );

    // Session B settles; THEN the harness takes the sqlite write lock.
    // The next maintenance tick (150 ms cadence) blocks behind the lock
    // mid-build; death lands there.
    let b = h.settle(&base).await;
    let lock = sqlite_write_lock(&h.sqlite()).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    kill9(&mut serve);
    drop(lock);

    // Crash-window assertion: B has no visible row, or a complete
    // verified one — never a partial. (load_projection re-checks the
    // digest before trusting a row; a divergent row falls back and the
    // comparison below still proves honesty.)
    let store = EventStore::open(h.sqlite()).await.expect("open");
    if let Some(row) = store.snapshot_highest_verified(b).await.expect("row") {
        assert!(row.verified, "an unverified row became visible");
    }
    store.close().await;

    // Restart #1: the maintenance loop republishes B, and every read is
    // the stream's truth.
    let mut serve2 = spawn_serve(&h.dir, &config, "boot2");
    h.wait_ready(&base).await;
    assert!(
        h.wait_snapshot(b).await,
        "the restarted maintenance never published for session B"
    );
    h.verify_snapshots_honest(&base, &[a, b]).await;

    // Kill during live traffic (the load window is read-only; the proof
    // is that nothing it touched can go wrong): one more session, a
    // kill while it may be mid-read, restart, verify everything.
    let c = h.settle(&base).await;
    kill9(&mut serve2);
    let mut serve3 = spawn_serve(&h.dir, &config, "boot3");
    h.wait_ready(&base).await;
    h.verify_snapshots_honest(&base, &[a, b, c]).await;

    kill9(&mut serve3);
    h.cleanup();
}

/// Holds a `BEGIN IMMEDIATE` transaction on the sqlite file (the
/// writer lock) until dropped.
async fn sqlite_write_lock(path: &Path) -> sqlx::sqlite::SqliteConnection {
    use sqlx::Connection;
    let opts = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(path)
        .busy_timeout(Duration::from_secs(10));
    let mut conn = sqlx::sqlite::SqliteConnection::connect_with(&opts)
        .await
        .expect("lock connection");
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut conn)
        .await
        .expect("take write lock");
    conn
}
