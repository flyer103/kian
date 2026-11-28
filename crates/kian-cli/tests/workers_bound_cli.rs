//! `kian workers` bound-session projection over a real store (L1): the
//! board derives the live owner from indexed kind reads — the Router
//! lineage's `thread/started` events plus each candidate stream's
//! terminal kinds — so the verb never has to replay every session.

use std::path::{Path, PathBuf};

use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_store::{AppendGuard, EventStore};
use serde_json::{Value, json};
use uuid::Uuid;

fn temp_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("kian-workers-bound-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

/// A loadable serve config with the `superagent:` section (the branch that
/// projects the bound owner). Everything else is the smallest document the
/// loader accepts; nothing here starts a server.
fn write_config(dir: &Path) -> PathBuf {
    let path = dir.join("kian.yaml");
    std::fs::write(
        &path,
        r#"listen: "127.0.0.1:0"
storage:
  sqlite: "unused.sqlite"
agent:
  id: default
  model_slots:
    main:
      provider: qa-mock
      model: mock-model
agents:
  router:
    id: router
    version: 3
    model_slots:
      router:
        provider: qa-mock
        model: mock-router
  worker:
    id: worker
    version: 7
    model_slots:
      main:
        provider: qa-mock
        model: mock-worker
superagent:
  router:
    session_id: "0a0a0a0a-0a0a-0a0a-0a0a-0a0a0a0a0a0a"
    agent: router
    slot: router
  worker:
    agent: worker
  subscriptions:
    - channel: mock-im
      match: always
      route: router
  channels:
    mock-im:
      type: mock-im
plugins: ["openai-compat", "local-tools", "superagent"]
providers:
  qa-mock:
    base_url: "http://127.0.0.1:9"
    api_key_env: KIAN_E2E_API_KEY
recovery:
  budget:
    tokens: 64
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#,
    )
    .expect("write config");
    path
}

async fn seed_endpoint(db: &Path, id: &str) {
    let store = kian_exec::ExecStore::open(db).await.expect("exec store");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    store
        .upsert_endpoint(id, "127.0.0.1:1", now, "online")
        .await
        .expect("upsert endpoint");
    store.close().await;
}

async fn append(
    store: &EventStore,
    session: SessionId,
    seq: u64,
    ts: u64,
    kind: EventKind,
    bytes: Value,
) {
    store
        .append(
            session,
            Event {
                seq,
                session,
                epoch: Epoch(0),
                ts,
                kind,
                bytes,
                version: SCHEMA_VERSION,
            },
            AppendGuard {
                epoch: Epoch(0),
                expected_seq: seq,
            },
        )
        .await
        .expect("seed append");
}

/// The Router-side lineage for one bound thread, with the worker stream
/// started (`session/created`) and optionally ended.
async fn seed_bound_thread(
    store: &EventStore,
    root: SessionId,
    worker_session: SessionId,
    thread_id: Uuid,
    worker: &str,
    ended: Option<EndReason>,
    ts: u64,
) {
    let seq = store.head(root).await.expect("root head").next_seq;
    await_append_session_created(store, root, seq, ts).await;
    let seq = store.head(root).await.expect("root head").next_seq;
    append(
        store,
        root,
        seq,
        ts,
        EventKind::ThreadStarted,
        json!({
            "thread_id": thread_id.to_string(),
            "parent": root.0.to_string(),
            "worker": worker_session.0.to_string(),
            "pinned_version": 1,
            "task": {"role": "user", "content": format!("job-{worker}")},
            "exec_worker": worker,
        }),
    )
    .await;
    let seq = store
        .head(worker_session)
        .await
        .expect("worker head")
        .next_seq;
    append(
        store,
        worker_session,
        seq,
        ts,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
    if let Some(reason) = ended {
        let seq = store
            .head(worker_session)
            .await
            .expect("worker head")
            .next_seq;
        append(
            store,
            worker_session,
            seq,
            ts,
            EventKind::SessionEnded { reason },
            json!({}),
        )
        .await;
    }
}

async fn await_append_session_created(store: &EventStore, session: SessionId, seq: u64, ts: u64) {
    append(
        store,
        session,
        seq,
        ts,
        EventKind::SessionCreated,
        json!({}),
    )
    .await;
}

/// The board names the live owner (session + thread) of a busy worker, and
/// a released thread of the same worker never shadows it.
#[tokio::test]
async fn workers_reports_the_live_bound_owner() {
    let dir = temp_dir();
    let db = dir.join("kian.sqlite");
    let config = write_config(&dir);
    let loaded = kian_cli::config::ServeConfig::load(&config).expect("test config must load");
    assert!(
        loaded.superagent.is_some(),
        "the bound projection only runs with the superagent section configured"
    );
    seed_endpoint(&db, "builder-1").await;

    let root = SessionId::new();
    let released = SessionId::new();
    let live = SessionId::new();
    let released_thread = Uuid::new_v4();
    let live_thread = Uuid::new_v4();
    {
        let store = EventStore::open(&db).await.expect("event store");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        seed_bound_thread(
            &store,
            root,
            released,
            released_thread,
            "builder-1",
            Some(EndReason::Completed),
            now - 60_000,
        )
        .await;
        seed_bound_thread(&store, root, live, live_thread, "builder-1", None, now).await;
        store.close().await;
    }

    let text = kian_cli::commands::run_workers(&config, Some(&db), true)
        .await
        .expect("workers never refuses");
    let report: Value = serde_json::from_str(&text).expect("json report");
    let rows = report["workers"].as_array().expect("workers array");
    assert_eq!(rows.len(), 1, "one registered worker");
    assert_eq!(rows[0]["id"], "builder-1");
    assert_eq!(
        rows[0]["bound_session"].as_str(),
        Some(live.0.to_string().as_str()),
        "the live stream is the owner, not the released one: {text}"
    );
    assert_eq!(
        rows[0]["bound_thread"].as_str(),
        Some(live_thread.to_string().as_str()),
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// A worker whose only thread ended is free: no bound fields.
#[tokio::test]
async fn workers_reports_no_owner_once_the_stream_is_released() {
    let dir = temp_dir();
    let db = dir.join("kian.sqlite");
    let config = write_config(&dir);
    seed_endpoint(&db, "mac-mini").await;

    let root = SessionId::new();
    let worker_session = SessionId::new();
    {
        let store = EventStore::open(&db).await.expect("event store");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        seed_bound_thread(
            &store,
            root,
            worker_session,
            Uuid::new_v4(),
            "mac-mini",
            Some(EndReason::Cancelled),
            now,
        )
        .await;
        store.close().await;
    }

    let text = kian_cli::commands::run_workers(&config, Some(&db), true)
        .await
        .expect("workers never refuses");
    let report: Value = serde_json::from_str(&text).expect("json report");
    let rows = report["workers"].as_array().expect("workers array");
    assert_eq!(rows.len(), 1);
    assert!(rows[0]["bound_session"].is_null(), "was: {text}");
    assert!(rows[0]["bound_thread"].is_null(), "was: {text}");
    std::fs::remove_dir_all(&dir).ok();
}
