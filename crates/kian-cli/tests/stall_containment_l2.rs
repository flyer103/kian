//! L2 proof for ADR-041 over the REAL `kian serve` assembly:
//!
//! 1. `a_stalled_model_call_ends_the_turn_instead_of_parking_the_drain` — the
//!    provider accepts the request and stops sending after one SSE frame. The
//!    configured `providers.mock.idle_timeout_ms` must end the turn honestly
//!    (`failed`), not park the drain forever (the pre-ADR-041 shape).
//! 2. `a_crash_looping_bound_session_ends_honestly_on_boot` — a committed
//!    stream that already crashed twice without progress (two un-ended
//!    `drain/claimed` events + an expired lease) must be closed by the boot
//!    recovery sweep with ONE honest `failed` end, not re-driven forever, and
//!    the server must stay healthy.
//!
//! Runs with `NO_PROXY=127.0.0.1,localhost` (AGENTS.md gotcha: the macOS
//! system proxy intercepts localhost).

mod common;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{force_no_proxy, temp_dir};
use kian_cli::server::Server;
use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::SessionId;
use kian_store::{AppendGuard, EventStore};
use serde_json::json;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

const KEY_ENV: &str = "KIAN_E2E_API_KEY";

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("free port addr").port()
}

async fn rpc(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{base}/rpc"))
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .expect("rpc send");
    assert_eq!(resp.status(), 200);
    resp.json().await.expect("rpc json")
}

/// Writes the L2 config: the real assembly plus the ADR-041 provider bounds
/// (and a short lease so the boot sweep is observable).
fn write_config(
    dir: &Path,
    sqlite: &Path,
    provider_base: &str,
    port: u16,
    idle_timeout_ms: u64,
) -> PathBuf {
    write_config_with_workers(dir, sqlite, provider_base, port, idle_timeout_ms, None)
}

/// [`write_config`] with an optional `workers:` section (which arms the
/// ADR-035 reclaim tick; `Some(port)` is the registration listener).
fn write_config_with_workers(
    dir: &Path,
    sqlite: &Path,
    provider_base: &str,
    port: u16,
    idle_timeout_ms: u64,
    workers_port: Option<u16>,
) -> PathBuf {
    let workers = match workers_port {
        Some(worker_port) => format!(
            "plugins: [\"openai-compat\", \"local-tools\", \"remote-exec\"]\nworkers:\n  listen: \"127.0.0.1:{worker_port}\"\n  token_env: KIAN_WORKER_TOKEN\n  release_after_ms: 300\n"
        ),
        None => String::new(),
    };
    let yaml = format!(
        r#"listen: "127.0.0.1:{port}"
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
    connect_timeout_ms: 1000
    idle_timeout_ms: {idle_timeout_ms}
drain:
  max_iterations: 8
  lease_ttl_ms: 100
  heartbeat_interval_ms: 50
tools:
  root: "{}"
{workers}recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: provider_down
      action: fail
    - class: unknown
      action: fail
"#,
        sqlite.display(),
        dir.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write kian.yaml");
    path
}

struct ServeHandle {
    base: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ServeHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Boots the REAL server (assembly + all maintenance tickers) on `port`.
async fn boot(config: &Path, port: u16) -> ServeHandle {
    let server = Arc::new(
        Server::from_config_file(config)
            .await
            .expect("assemble server")
            .0,
    );
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("addr");
    let task = tokio::spawn(async move {
        let _ = Arc::clone(&server).serve(addr).await;
    });
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    for _ in 0..400 {
        if client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            return ServeHandle { base, task };
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server did not become ready");
}

async fn wait_for<F, Fut>(timeout_ms: u64, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if cond().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Appends one seed event under its own epoch (the store's CAS fence
/// requires `event.epoch == claims.epoch == guard.epoch`).
async fn seed_append(
    store: &EventStore,
    sid: SessionId,
    seq: u64,
    epoch: kian_core::model::Epoch,
    ts: u64,
    kind: EventKind,
    bytes: serde_json::Value,
) {
    store
        .append(
            sid,
            Event {
                seq,
                session: sid,
                epoch,
                ts,
                kind,
                bytes,
                version: SCHEMA_VERSION,
            },
            AppendGuard {
                epoch,
                expected_seq: seq,
            },
        )
        .await
        .expect("seed append");
}

async fn last_end(store: &EventStore, sid: SessionId) -> Option<(EndReason, serde_json::Value)> {
    let events = store.rebuild(sid).await.ok()?;
    events.iter().rev().find_map(|ev| match ev.kind {
        EventKind::SessionEnded { reason } => Some((reason, ev.bytes.clone())),
        _ => None,
    })
}

/// A raw HTTP/1.1 SSE provider that sends headers + one frame and then goes
/// silent — the half-dead-proxy shape that used to park a drain forever.
async fn spawn_stalling_provider() -> (String, Arc<AtomicUsize>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stalling provider");
    let addr = listener.local_addr().expect("stalling provider addr");
    let requests = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&requests);
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let counter = Arc::clone(&counter);
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                let _ = socket.read(&mut buf).await;
                counter.fetch_add(1, Ordering::SeqCst);
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n")
                    .await;
                let _ = socket
                    .write_all(b"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
                    .await;
                let _ = socket.flush().await;
                // Hold the body open with no further bytes: only the idle
                // bound can end this turn.
                tokio::time::sleep(Duration::from_secs(60)).await;
            });
        }
    });
    (format!("http://{addr}"), requests)
}

/// The pre-ADR-041 shape over the REAL assembly: the drain sits inside the
/// provider stream. With `idle_timeout_ms` the turn ends honestly in the
/// bound; without it the session would stay `running` forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_model_call_ends_the_turn_instead_of_parking_the_drain() {
    force_no_proxy();
    let dir = temp_dir("stall-containment");
    let db = dir.join("kian.sqlite");
    let (provider_base, provider_requests) = spawn_stalling_provider().await;
    let port = free_port();
    let config = write_config(&dir, &db, &provider_base, port, 250);
    let serve = boot(&config, port).await;
    let client = reqwest::Client::new();

    let created = rpc(&client, &serve.base, "create_session", json!({})).await;
    let sid = created["result"]["session"]
        .as_str()
        .unwrap_or_else(|| panic!("create_session failed: {created}"))
        .to_owned();
    let submitted = rpc(
        &client,
        &serve.base,
        "submit",
        json!({"session": sid, "content": "Say something."}),
    )
    .await;
    assert!(
        submitted.get("error").is_none(),
        "submit failed: {submitted}"
    );
    let sid = uuid::Uuid::parse_str(&sid).expect("session uuid");
    let sid = SessionId(sid);

    let store = EventStore::open_read_only(&db).await.expect("ro store");
    assert!(
        wait_for(10_000, || async {
            matches!(last_end(&store, sid).await, Some((EndReason::Failed, _)))
        })
        .await,
        "a stalled model call must settle the turn honestly as failed \
         (the pre-ADR-041 shape parks the drain forever)"
    );
    assert!(
        provider_requests.load(Ordering::SeqCst) >= 1,
        "the provider was actually called"
    );
    // The end is a real terminal turn, not a fabricated completion.
    let (reason, bytes) = last_end(&store, sid).await.expect("end");
    assert_eq!(reason, EndReason::Failed);
    assert!(bytes.get("released").is_none(), "not a release: {bytes}");
    store.close().await;
}

/// A committed crash chain (two `drain/claimed` without an intervening end,
/// expired lease) is finished with ONE honest `failed` end on boot; the
/// server stays healthy and the stream freezes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_looping_bound_session_ends_honestly_on_boot() {
    force_no_proxy();
    let dir = temp_dir("crash-containment");
    let db = dir.join("kian.sqlite");
    let port = free_port();
    // The provider is never called by the crash chain (the bounded taker
    // writes the end before driving); point it at a loopback port that is
    // not listening so a drive would also fail honestly.
    let config = write_config(&dir, &db, "http://127.0.0.1:1/v1", port, 5_000);

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64;
    let sid = SessionId::new();
    {
        let store = EventStore::open(&db).await.expect("pre-boot store");
        // seq starts at 1 (the store's CAS fence treats an empty stream as
        // max(seq) == 0).
        seed_append(
            &store,
            sid,
            1,
            kian_core::model::Epoch::GENESIS,
            now_ms - 60_000,
            EventKind::SessionCreated,
            json!({}),
        )
        .await;
        for (seq, i) in (2u64..).zip(0..2u64) {
            let lease = store
                .claim(sid, "dead-holder", 100, now_ms - 60_000 + i * 1_000)
                .await
                .expect("seed claim");
            seed_append(
                &store,
                sid,
                seq,
                lease.epoch,
                now_ms - 60_000 + i * 1_000,
                EventKind::DrainClaimed,
                json!({"holder": "dead-holder"}),
            )
            .await;
        }
        store.close().await;
    }

    let serve = boot(&config, port).await;
    let store = EventStore::open_read_only(&db).await.expect("ro store");
    assert!(
        wait_for(10_000, || async {
            matches!(last_end(&store, sid).await, Some((EndReason::Failed, _)))
        })
        .await,
        "the boot recovery sweep must close the crashed chain honestly"
    );
    let (reason, bytes) = last_end(&store, sid).await.expect("end");
    assert_eq!(reason, EndReason::Failed);
    assert_eq!(
        bytes["crashed"]["takeovers"],
        json!(2),
        "the payload names the crashed predecessors: {bytes}"
    );
    let frozen_at = store.rebuild(sid).await.expect("rebuild").len();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        store.rebuild(sid).await.expect("rebuild").len(),
        frozen_at,
        "the ended stream must freeze (no endless re-drive)"
    );
    assert_eq!(
        store
            .rebuild(sid)
            .await
            .expect("rebuild")
            .iter()
            .filter(|ev| matches!(ev.kind, EventKind::SessionEnded { .. }))
            .count(),
        1,
        "exactly one honest end"
    );
    store.close().await;

    // The process is still healthy: a new session is served normally.
    let client = reqwest::Client::new();
    let created = rpc(&client, &serve.base, "create_session", json!({})).await;
    assert!(
        created["result"]["session"].as_str().is_some(),
        "the server must stay healthy: {created}"
    );
}

/// Composition smoke (AGENTS.md step 7): an abnormally interrupted bound
/// worker thread must not stay busy forever. A committed crash chain (two
/// un-ended claims + an expired lease) is closed by the ADR-041 crash
/// containment with one honest `failed` end, and the ADR-035 reclaim then
/// frees the worker slot through the same tick an operator would otherwise
/// have to poke — no manual `release_worker` anywhere in this test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crashed_bound_worker_is_reclaimed_without_an_operator() {
    force_no_proxy();
    // The workers section resolves its shared token from the environment.
    unsafe {
        std::env::set_var("KIAN_WORKER_TOKEN", "test-worker-token");
    }
    let dir = temp_dir("crash-reclaim");
    let db = dir.join("kian.sqlite");
    let port = free_port();
    let workers_port = free_port();
    let config = write_config_with_workers(
        &dir,
        &db,
        "http://127.0.0.1:1/v1",
        port,
        5_000,
        Some(workers_port),
    );

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64;
    let sid = SessionId::new();
    let thread_id = uuid::Uuid::new_v4();
    {
        let store = EventStore::open(&db).await.expect("pre-boot store");
        seed_append(
            &store,
            sid,
            1,
            kian_core::model::Epoch::GENESIS,
            now_ms - 60_000,
            EventKind::SessionCreated,
            json!({}),
        )
        .await;
        // The worker-side lineage: this is what the 1:1 check and the
        // reclaim tick key the worker id on.
        seed_append(
            &store,
            sid,
            2,
            kian_core::model::Epoch::GENESIS,
            now_ms - 60_000,
            EventKind::ThreadStarted,
            json!({
                "thread_id": thread_id.to_string(),
                "parent": uuid::Uuid::new_v4().to_string(),
                "worker": sid.0.to_string(),
                "pinned_version": 2,
                "task": {"role": "user", "content": "delegated task"},
                "exec_worker": "w-crash",
            }),
        )
        .await;
        for (seq, i) in (3u64..).zip(0..2u64) {
            let lease = store
                .claim(sid, "dead-holder", 100, now_ms - 60_000 + i * 1_000)
                .await
                .expect("seed claim");
            seed_append(
                &store,
                sid,
                seq,
                lease.epoch,
                now_ms - 60_000 + i * 1_000,
                EventKind::DrainClaimed,
                json!({"holder": "dead-holder"}),
            )
            .await;
        }
        store.close().await;
    }

    let _serve = boot(&config, port).await;
    let store = EventStore::open_read_only(&db).await.expect("ro store");

    // 1) ADR-041: the boot sweep bounds the crash chain with one honest
    //    failed end (a `failed` end is the resumable shape ADR-035 gates on).
    assert!(
        wait_for(10_000, || async {
            matches!(last_end(&store, sid).await, Some((EndReason::Failed, _)))
        })
        .await,
        "the crashed chain must be closed honestly"
    );

    // 2) ADR-035: the maintenance reclaim closes the idle-ended thread —
    //    no operator verb is called.
    assert!(
        wait_for(10_000, || async {
            let events = store.rebuild(sid).await.unwrap_or_default();
            matches!(
                events.last().map(|ev| &ev.kind),
                Some(EventKind::SessionEnded {
                    reason: EndReason::Cancelled
                })
            ) && events
                .last()
                .is_some_and(|ev| ev.bytes["released"]["by"] == json!("reclaim"))
        })
        .await,
        "the idle-ended bound thread must be reclaimed automatically"
    );

    let events = store.rebuild(sid).await.expect("rebuild");
    let last = events.last().expect("release end");
    assert_eq!(last.bytes["released"]["worker"], json!("w-crash"));
    assert_eq!(
        events
            .iter()
            .filter(|ev| matches!(ev.kind, EventKind::SessionEnded { .. }))
            .count(),
        2,
        "one honest failed end + one reclaim close"
    );
    assert_eq!(
        kian_superagent::thread_liveness(&events),
        kian_superagent::ThreadLiveness::Released,
        "the worker slot is free"
    );
    store.close().await;
}
