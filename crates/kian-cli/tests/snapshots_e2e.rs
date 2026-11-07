//! READ-MODEL P1 e2e: verified snapshots serve the projection read path
//! with a byte-identical wire surface.
//!
//! The in-process server (router-only spawn) is driven through the real
//! JSON-RPC `projection` surface; snapshots are built through the real
//! builder against the same sqlite file the server serves from. What is
//! proven here:
//!
//! - the wire response (state, version, unchanged fast path) is
//!   identical before and after a snapshot exists — no wire change;
//! - the built row is verified and loadable, and the loaded projection
//!   equals the full derivation of the same stream (the request-bytes
//!   witness at the e2e level);
//! - with `read_model.snapshots.enabled: false` a planted snapshot is
//!   invisible: reads derive from the stream, results identical.

mod common;

use std::sync::Arc;
use std::time::Duration;

use kian_cli::server::Server;
use kian_core::model::SessionId;
use kian_orchestrator::snapshots::{
    ReadModelConfig, SnapshotBuildConfig, build_snapshot, load_projection,
};
use kian_store::EventStore;
use uuid::Uuid;

use common::{force_no_proxy, spawn_mock_provider, temp_dir};

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

/// The read-model-enabled e2e config (snapshots on, an aggressive
/// threshold so a settled session qualifies).
fn write_config(
    dir: &std::path::Path,
    sqlite: &std::path::Path,
    provider_base: &str,
    listen: &str,
) -> std::path::PathBuf {
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
    api_key_env: KIAN_E2E_API_KEY
drain:
  max_iterations: 8
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
    interval_ms: 60000
"#,
        sqlite.display(),
        dir.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write kian.yaml");
    path
}

async fn spawn_server(config_path: &std::path::Path) -> (String, Arc<Server>) {
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

/// Creates a session, submits one input and waits until the stream
/// settles `completed`; returns `(session id, final version)`.
async fn settle_session(client: &reqwest::Client, base: &str) -> (SessionId, u64) {
    let out = rpc(client, base, "create_session", serde_json::json!({})).await;
    let sid = SessionId(
        Uuid::parse_str(out["result"]["session"].as_str().expect("session id")).expect("uuid"),
    );
    let out = rpc(
        client,
        base,
        "submit",
        serde_json::json!({"session": sid.0.to_string(), "content": "Say all done."}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "session never settled"
        );
        let out = rpc(
            client,
            base,
            "projection",
            serde_json::json!({"session": sid.0.to_string()}),
        )
        .await;
        if out["result"]["state"] == "completed" {
            let version = out["result"]["version"].as_u64().expect("version present");
            return (sid, version);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The wire surface is identical before and after a snapshot exists:
/// same state, same version, the unchanged fast path unchanged — and
/// the built row loads equal to the full derivation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn projection_wire_identical_with_snapshots_e2e() {
    force_no_proxy();
    let dir = temp_dir("snapshots-e2e");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:0");
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    let (sid, version) = settle_session(&client, &base).await;

    // The wire read before any snapshot exists.
    let before = rpc(
        &client,
        &base,
        "projection",
        serde_json::json!({"session": sid.0.to_string()}),
    )
    .await;
    assert_eq!(before["result"]["state"], "completed");
    assert_eq!(before["result"]["version"], version);

    // Build a verified snapshot for the settled session (the same
    // builder the serve maintenance ticker runs).
    let store = EventStore::open(&sqlite).await.expect("open store");
    let out = build_snapshot(
        &store,
        sid,
        &SnapshotBuildConfig {
            min_events: 1,
            ..SnapshotBuildConfig::default()
        },
        "e2e",
        1_000,
    )
    .await
    .expect("build");
    assert!(
        matches!(out, kian_orchestrator::BuildOutcome::Published { version: v } if v == version),
        "unexpected outcome: {out:?}"
    );

    // The wire read after: byte-identical shape and values.
    let after = rpc(
        &client,
        &base,
        "projection",
        serde_json::json!({"session": sid.0.to_string()}),
    )
    .await;
    assert_eq!(after, before, "the wire response must not change");

    // The unchanged fast path is untouched by the snapshot row.
    let same = rpc(
        &client,
        &base,
        "projection",
        serde_json::json!({"session": sid.0.to_string(), "since": version}),
    )
    .await;
    assert_eq!(same["result"]["unchanged"], true);
    assert!(same["result"].get("state").is_none());

    // And the snapshot loads equal to the full derivation of the same
    // stream (request bytes included).
    let read_on = ReadModelConfig {
        snapshots_enabled: true,
        tail_cap: 8_192,
    };
    let loaded = load_projection(&store, sid, version, &read_on)
        .await
        .expect("load");
    assert_eq!(loaded.from_snapshot, Some(version));
    let full = load_projection(&store, sid, version, &ReadModelConfig::default())
        .await
        .expect("full");
    assert_eq!(loaded.state, full.state);
    assert_eq!(
        kian_core::context::context_to_bytes(&loaded.context),
        kian_core::context::context_to_bytes(&full.context),
        "request bytes differ across the snapshot path (constraint 3)"
    );

    store.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Disabling the read model hides snapshots: a planted (valid!) snapshot
/// row is invisible and reads derive from the stream — results
/// identical, byte for byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_read_model_hides_snapshots_e2e() {
    force_no_proxy();
    let dir = temp_dir("snapshots-e2e-off");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config_path = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:0");
    // Flip the gate off.
    let yaml = std::fs::read_to_string(&config_path).expect("read config");
    let yaml = yaml.replace("enabled: true", "enabled: false");
    std::fs::write(&config_path, yaml).expect("write config");

    let (base, _server) = spawn_server(&config_path).await;
    let client = reqwest::Client::new();
    let (sid, version) = settle_session(&client, &base).await;

    let before = rpc(
        &client,
        &base,
        "projection",
        serde_json::json!({"session": sid.0.to_string()}),
    )
    .await;

    // Plant a valid verified snapshot behind the gate's back.
    let store = EventStore::open(&sqlite).await.expect("open store");
    build_snapshot(
        &store,
        sid,
        &SnapshotBuildConfig {
            min_events: 1,
            ..SnapshotBuildConfig::default()
        },
        "e2e",
        1_000,
    )
    .await
    .expect("plant");

    // Hidden: the loader ignores the row entirely.
    let hidden = load_projection(
        &store,
        sid,
        version,
        &ReadModelConfig {
            snapshots_enabled: false,
            tail_cap: 8_192,
        },
    )
    .await
    .expect("load");
    assert_eq!(hidden.from_snapshot, None, "the row must be invisible");

    let after = rpc(
        &client,
        &base,
        "projection",
        serde_json::json!({"session": sid.0.to_string()}),
    )
    .await;
    assert_eq!(after, before);

    store.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}
