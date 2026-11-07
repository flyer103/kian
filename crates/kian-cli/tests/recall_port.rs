//! acceptance: the `RecallPort` read port and the spill index.
//!
//! The store-level tests exercise assembly's port directly over a real
//! sqlite store (committed events for arbitrary sessions, honest-empty
//! semantics, thread derivation, drop-and-rebuild spill index). The
//! liveness test runs the full assembled server with a gated scripted
//! provider so a Router drain is parked mid-provider-turn, then proves
//! a concurrent range read completes — the read never blocks the drain
//! and the drain never blocks the read (the pattern).
//!
//! All tests run with `NO_PROXY=127.0.0.1,localhost` (AGENTS.md
//! gotcha), zero credentials, no network egress.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir, wait_for};
use kian_cli::server::Server;
use kian_cli::superagent::StoreRecall;
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_store::{AppendGuard, EventStore};
use kian_superagent::RecallPort as _;
use serde_json::json;
use uuid::Uuid;

const T0: u64 = 1_761_900_000_000;
const ROUTER_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
/// `until_seq` meaning "everything" (the store clamps to its domain).
const ALL: u64 = i64::MAX as u64;

fn temp_db_path() -> PathBuf {
    std::env::temp_dir().join(format!("kian-recall-port-test-{}.sqlite", Uuid::new_v4()))
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

fn event(session: SessionId, seq: u64, kind: EventKind, bytes: serde_json::Value) -> Event {
    Event {
        seq,
        session,
        epoch: Epoch::GENESIS,
        ts: T0 + seq,
        kind,
        bytes,
        version: SCHEMA_VERSION,
    }
}

async fn append(store: &EventStore, ev: Event) {
    let seq = ev.seq;
    let session = ev.session;
    store
        .append(
            session,
            ev,
            AppendGuard {
                epoch: Epoch::GENESIS,
                expected_seq: seq,
            },
        )
        .await
        .expect("append");
}

fn sid(n: u128) -> SessionId {
    SessionId(Uuid::from_u128(n))
}

/// One session's stream: session/created + two turns of noise.
async fn seed_stream(store: &EventStore, session: SessionId) {
    append(
        store,
        event(session, 1, EventKind::SessionCreated, json!({})),
    )
    .await;
    append(
        store,
        event(
            session,
            2,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "hello"}),
        ),
    )
    .await;
    append(
        store,
        event(
            session,
            3,
            EventKind::AssistantMessage,
            json!({"role": "assistant", "content": "hi"}),
        ),
    )
    .await;
}

/// `read_events` returns an arbitrary session's committed events in
/// seq order, and an explicit window slices exactly `[since, until)`.
#[tokio::test]
async fn read_port_returns_committed_events_for_an_arbitrary_session() {
    let path = temp_db_path();
    let store = EventStore::open(&path).await.expect("open store");
    // Two sessions, read the *other* one — cross-session is the point.
    let mine = sid(1);
    let other = sid(2);
    seed_stream(&store, mine).await;
    seed_stream(&store, other).await;
    let recall = StoreRecall::new(store.clone());

    let events = recall
        .read_events(other, 0, ALL)
        .await
        .expect("cross-session read");
    assert_eq!(
        events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![1, 2, 3],
        "committed events in seq order"
    );
    assert!(events.iter().all(|e| e.session == other));

    // A window [2, 3) yields exactly seq 2.
    let window = recall
        .read_events(other, 2, 3)
        .await
        .expect("windowed read");
    assert_eq!(window.len(), 1);
    assert_eq!(window[0].seq, 2);
    assert_eq!(window[0].kind, EventKind::InputAdmitted);

    store.close().await;
    cleanup(&path);
}

/// The port refuses nothing read-only: unknown sessions, empty and
/// inverted windows, unknown spill ids — all honest empties, no error.
#[tokio::test]
async fn read_port_refuses_nothing_read_only() {
    let path = temp_db_path();
    let store = EventStore::open(&path).await.expect("open store");
    seed_stream(&store, sid(1)).await;
    let recall = StoreRecall::new(store.clone());

    // A session the log never saw.
    let unknown = recall
        .read_events(sid(99), 0, ALL)
        .await
        .expect("unknown session is an honest empty");
    assert!(unknown.is_empty());

    // A window past the head, and inverted / empty windows.
    assert!(
        recall
            .read_events(sid(1), 10, ALL)
            .await
            .unwrap()
            .is_empty(),
        "past-the-head window is empty"
    );
    assert!(
        recall.read_events(sid(1), 2, 2).await.unwrap().is_empty(),
        "empty window is empty"
    );
    assert!(
        recall.read_events(sid(1), 3, 2).await.unwrap().is_empty(),
        "inverted window is empty, not an error"
    );

    // A parent with no thread/started events derives no threads.
    assert!(
        recall.list_threads(sid(1)).await.unwrap().is_empty(),
        "no threads on a threadless stream"
    );

    // An id the index never saw is None, not an error.
    assert_eq!(
        recall.spill_location("evt-unknown").await.unwrap(),
        None,
        "unknown spill id is an honest None"
    );

    store.close().await;
    cleanup(&path);
}

/// `list_threads` derives `(thread_id, worker)` pairs from the parent
/// stream's `thread/started` events — the PassthroughRegistry source.
#[tokio::test]
async fn list_threads_derives_pairs_from_the_parent_stream() {
    let path = temp_db_path();
    let store = EventStore::open(&path).await.expect("open store");
    let router = sid(1);
    let worker_a = sid(2);
    let worker_b = sid(3);
    let started = |seq: u64, thread_id: &str, worker: SessionId| {
        event(
            router,
            seq,
            EventKind::ThreadStarted,
            json!({
                "thread_id": thread_id,
                "parent": router.0.to_string(),
                "worker": worker.0.to_string(),
                "pinned_version": 1,
                "task": {"role": "user", "content": "t"},
            }),
        )
    };
    append(
        &store,
        event(router, 1, EventKind::SessionCreated, json!({})),
    )
    .await;
    append(&store, started(2, "t-a", worker_a)).await;
    append(&store, event(router, 3, EventKind::TurnStart, json!({}))).await;
    append(&store, started(4, "t-b", worker_b)).await;

    let recall = StoreRecall::new(store.clone());
    assert_eq!(
        recall.list_threads(router).await.unwrap(),
        vec![("t-a".to_string(), worker_a), ("t-b".to_string(), worker_b),],
        "pairs in stream order from thread/started"
    );

    store.close().await;
    cleanup(&path);
}

/// The startup rebuild reconstructs the spill index from a store with
/// N spilled events across the Router and worker streams, and
/// drop-and-rebuild is equivalent (the index is disposable).
#[tokio::test]
async fn startup_rebuild_reconstructs_the_spill_index() {
    let path = temp_db_path();
    let store = EventStore::open(&path).await.expect("open store");
    let router = sid(1);
    let worker_a = sid(2);
    let worker_b = sid(3);

    // Router stream: created, a spill, two thread/started bindings.
    append(
        &store,
        event(router, 1, EventKind::SessionCreated, json!({})),
    )
    .await;
    append(
        &store,
        event(
            router,
            2,
            EventKind::InputSpilled,
            json!({
                "event_id": "evt-router-1",
                "channel": "mock-im",
                "topic": null,
                "raw": {"text": "router raw"},
            }),
        ),
    )
    .await;
    append(
        &store,
        event(
            router,
            3,
            EventKind::ThreadStarted,
            json!({
                "thread_id": "t-a",
                "parent": router.0.to_string(),
                "worker": worker_a.0.to_string(),
                "pinned_version": 1,
                "task": {"role": "user", "content": "t"},
            }),
        ),
    )
    .await;
    append(
        &store,
        event(
            router,
            4,
            EventKind::ThreadStarted,
            json!({
                "thread_id": "t-b",
                "parent": router.0.to_string(),
                "worker": worker_b.0.to_string(),
                "pinned_version": 1,
                "task": {"role": "user", "content": "t"},
            }),
        ),
    )
    .await;

    // Worker A's stream: created + two spills. Worker B: created only
    // (a worker with no spills must not disturb the rebuild).
    let spilled = |session: SessionId, seq: u64, id: &str| {
        event(
            session,
            seq,
            EventKind::InputSpilled,
            json!({
                "event_id": id,
                "channel": "mock-im",
                "topic": Some("pr-9".to_string()),
                "raw": {"text": "worker raw"},
            }),
        )
    };
    append(
        &store,
        event(worker_a, 1, EventKind::SessionCreated, json!({})),
    )
    .await;
    append(&store, spilled(worker_a, 2, "evt-worker-1")).await;
    append(&store, spilled(worker_a, 3, "evt-worker-2")).await;
    append(
        &store,
        event(worker_b, 1, EventKind::SessionCreated, json!({})),
    )
    .await;

    // The startup rebuild: index fills from the streams alone.
    let recall = StoreRecall::new(store.clone());
    recall
        .rebuild_spill_index(&[router])
        .await
        .expect("rebuild");
    assert_eq!(
        recall.spill_location("evt-router-1").await.unwrap(),
        Some((router, 2))
    );
    assert_eq!(
        recall.spill_location("evt-worker-1").await.unwrap(),
        Some((worker_a, 2))
    );
    assert_eq!(
        recall.spill_location("evt-worker-2").await.unwrap(),
        Some((worker_a, 3))
    );

    // Drop-and-rebuild equivalence: a fresh port over the same log
    // reconstructs the identical map (constraint 1 — the index is a
    // pure projection, never authoritative).
    let rebuilt = StoreRecall::new(store.clone());
    rebuilt
        .rebuild_spill_index(&[router])
        .await
        .expect("re-rebuild");
    for id in ["evt-router-1", "evt-worker-1", "evt-worker-2"] {
        assert_eq!(
            rebuilt.spill_location(id).await.unwrap(),
            recall.spill_location(id).await.unwrap(),
            "drop-and-rebuild yields the same location for {id}"
        );
    }
    assert_eq!(rebuilt.spill_location("evt-nope").await.unwrap(), None);

    // The located raws read back through the port itself — the index
    // holds locations only, the stream is the sole copy of content.
    let (session, seq) = recall
        .spill_location("evt-worker-1")
        .await
        .unwrap()
        .expect("located");
    let raw_event = recall
        .read_events(session, seq, seq + 1)
        .await
        .unwrap()
        .pop()
        .expect("spilled event reads back");
    assert_eq!(raw_event.kind, EventKind::InputSpilled);
    assert_eq!(raw_event.bytes["raw"]["text"], "worker raw");

    store.close().await;
    cleanup(&path);
}

// ---------------------------------------------------------------------------
// Liveness: a range read never blocks (and never blocks) a drain
// ---------------------------------------------------------------------------

fn write_config(dir: &std::path::Path, router_provider: &str, worker_provider: &str) -> PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let yaml = format!(
        r#"listen: "127.0.0.1:7341"
storage:
  sqlite: "{sqlite}"
agent:
  id: default
  model_slots:
    main:
      provider: router-mock
      model: mock-model
  tools: ["fs_read"]
agents:
  router:
    id: router
    version: 3
    model_slots:
      router:
        provider: router-mock
        model: mock-router
    tools: ["spawn_worker", "reply", "deliver_structured"]
  worker:
    id: worker
    version: 7
    model_slots:
      main:
        provider: worker-mock
        model: mock-worker
    tools: ["fs_read", "fs_write", "exec"]
superagent:
  router:
    session_id: "{ROUTER_SESSION}"
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
providers:
  router-mock:
    base_url: "{router_provider}"
    api_key_env: KIAN_E2E_API_KEY
  worker-mock:
    base_url: "{worker_provider}"
    api_key_env: KIAN_E2E_API_KEY
plugins: ["openai-compat", "local-tools", "superagent"]
drain:
  max_iterations: 8
tools:
  root: "{dir}"
  exec_timeout_ms: 5000
recovery:
  budget:
    tokens: 64
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#,
        sqlite = sqlite.display(),
        dir = dir.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write kian.yaml");
    path
}

/// A Router drain parked mid-provider-turn (the gated scripted
/// provider) stays concurrent with range reads: the read completes
/// while the drain is in flight, and the drain completes after the
/// concurrent read — neither blocks the other (the pattern).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_of_a_range_never_blocks_a_concurrent_drain() {
    force_no_proxy();
    let dir = temp_dir("c34-liveness");
    // The router provider gates its first request: the bootstrap judge
    // turn parks inside the provider call until released.
    let (router_base, router_mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("ack".into())], 1).await;
    let (worker_base, _worker_mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("ack".into())], 0).await;
    let config = write_config(&dir, &router_base, &worker_base);

    let (server, _cfg) = Server::from_config_file(&config).await.expect("assemble");

    // Startup wiring is part of the same acceptance: the spill index
    // rebuilds strictly after the inbox rebuild, before traffic.
    let order = server.startup_order();
    let pos = |step: &str| order.iter().position(|s| *s == step);
    let rebuild = pos("startup_rebuild_done").expect("rebuild recorded");
    let spill = pos("spill_index_rebuilt").expect("spill index rebuild recorded");
    let subscriptions = pos("subscriptions_started").expect("subscriptions recorded");
    assert!(
        rebuild < spill && spill < subscriptions,
        "spill index rebuilds after the inbox rebuild, before consumers: {order:?}"
    );

    let runtime = server
        .superagent_runtime()
        .expect("superagent runtime")
        .clone();
    let recall = Arc::clone(&runtime.recall);
    let router: SessionId = SessionId(ROUTER_SESSION.parse().expect("router uuid"));

    // Park the drain: the bootstrap judge turn reaches the gated
    // provider and holds there (request arrived, nothing delivered).
    assert!(
        wait_for(5_000, || {
            router_mock.requests.load(Ordering::SeqCst) >= 1
        })
        .await,
        "the router judge turn reached the provider"
    );
    assert_eq!(
        router_mock.delivered.load(Ordering::SeqCst),
        0,
        "the drain is parked mid-provider-turn"
    );

    // The concurrent range read completes promptly while the drain
    // holds the session — and sees the committed prefix.
    let read = tokio::time::timeout(Duration::from_secs(2), recall.read_events(router, 0, ALL))
        .await
        .expect("read completes while the drain is parked (no blocking)")
        .expect("read is an honest result");
    assert!(
        !read.is_empty(),
        "the committed prefix is readable mid-drain"
    );
    assert_eq!(
        read.iter().map(|e| e.seq).min(),
        Some(1),
        "reads start at the stream head"
    );
    // The index lookup is equally live mid-drain.
    assert_eq!(
        recall.spill_location("evt-none").await.unwrap(),
        None,
        "spill lookup is live mid-drain"
    );

    // Release: the parked drain must still complete — the concurrent
    // read blocked nothing.
    router_mock.released.store(true, Ordering::SeqCst);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let done = recall
            .read_events(router, 0, ALL)
            .await
            .map(|events| events.iter().any(|e| e.kind == EventKind::AssistantMessage))
            .unwrap_or(false);
        if done {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the drain completed after release (the read blocked nothing)"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(router_mock.delivered.load(Ordering::SeqCst) >= 1);

    let _ = std::fs::remove_dir_all(&dir);
}
