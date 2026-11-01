//! kian-62 integration tests: the resident re-arm path.
//!
//! Three claims, against the real assembly, the real sqlite event
//! store, the real engine and in-process scripted providers (no
//! credentials, no network egress):
//!
//! 1. **Terminal pinned → successor.** A resident Router whose pinned
//!    session ended `session/ended{completed}` (the frozen state the
//!    C61 live run actually left behind — the loop's lifetime iteration
//!    budget exhausting) re-arms on boot onto the deterministic
//!    successor session, and the live watch (channel events → judge
//!    turns) runs THERE, while the frozen stream stays untouched.
//! 2. **Reboot dedupe.** A second boot admits no second re-arm prompt
//!    (per-generation idempotency key) and the watch continues on the
//!    SAME successor session, seq growing.
//! 3. **Soft failure is not a succession.** A judge turn whose provider
//!    stream dies mid-flight ends the session honestly
//!    (`session/ended{failed}` — projects idle), and the NEXT admitted
//!    input re-opens the SAME pinned session. No successor exists.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use kian_cli::server::Server;
use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_store::{AppendGuard, EventStore};
use serde_json::json;

const ROUTER_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

fn router_session() -> SessionId {
    SessionId(uuid::Uuid::parse_str(ROUTER_SESSION).unwrap())
}

/// The expected successor derivation, spelled out locally on purpose:
/// the test pins the chain contract (uuid5 over the frozen name
/// "kian:resident:re-arm") instead of importing the implementation.
fn successor_session() -> SessionId {
    successor_of(router_session())
}

fn successor_of(session: SessionId) -> SessionId {
    SessionId(uuid::Uuid::new_v5(&session.0, b"kian:resident:re-arm"))
}

/// A super-agent `kian.yaml` over two scripted providers, with
/// reboot-friendly drain tuning (short lease TTL so a killed boot's
/// lease expires inside the test window).
fn write_config(
    dir: &Path,
    router_provider: &str,
    worker_provider: &str,
    listen: &str,
    lease_ttl_ms: u64,
    heartbeat_interval_ms: u64,
) -> PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let yaml = format!(
        r#""listen": "{listen}"
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
  lease_ttl_ms: {lease_ttl_ms}
  heartbeat_interval_ms: {heartbeat_interval_ms}
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
    std::fs::write(&path, yaml).expect("write config");
    path
}

/// Seeds a terminal resident stream for `session` (the frozen state a
/// previous run left behind): created → claimed → ended{reason}.
async fn seed_terminal(sqlite: &Path, session: SessionId, reason: EndReason) {
    let store = EventStore::open(sqlite).await.expect("seed store open");
    let kinds = [
        EventKind::SessionCreated,
        EventKind::DrainClaimed,
        EventKind::SessionEnded { reason },
    ];
    for (i, kind) in kinds.into_iter().enumerate() {
        let seq = (i + 1) as u64;
        store
            .append(
                session,
                Event {
                    seq,
                    session,
                    epoch: Epoch(1),
                    ts: 1_000 + seq,
                    kind,
                    bytes: json!({}),
                    version: SCHEMA_VERSION,
                },
                AppendGuard {
                    epoch: Epoch(1),
                    expected_seq: seq,
                },
            )
            .await
            .expect("seed append");
    }
}

async fn spawn_server(config_path: &Path) -> (String, Arc<Server>) {
    let server = Arc::new(
        Server::from_config_file(config_path)
            .await
            .expect("assemble superagent server")
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

use std::sync::Arc;

async fn ops_mock_channel_send(client: &reqwest::Client, base: &str, text: &str) -> String {
    let resp = client
        .post(format!("{base}/ops/rpc"))
        .json(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "mock_channel_send",
            "params": {"channel": "mock-im", "payload": {"text": text}}
        }))
        .send()
        .await
        .expect("ops send");
    assert_eq!(resp.status(), 200);
    let out: serde_json::Value = resp.json().await.expect("ops json");
    out["result"]["event_id"]
        .as_str()
        .expect("event id")
        .to_owned()
}

async fn wait_for_session(
    store: &EventStore,
    session: SessionId,
    desc: &str,
    pred: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    for _ in 0..1600 {
        let events = store.rebuild(session).await.expect("rebuild session");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

fn count_kind(events: &[Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind).count()
}

// ---------------------------------------------------------------------------
// 1. Terminal pinned session → the successor arms and carries the watch
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_pinned_router_re_arms_onto_the_successor_on_boot() {
    force_no_proxy();
    let dir = temp_dir("c62-rearm-successor");
    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::Text("re-arm acknowledged".into()),
            ScriptedResponse::Text("tick noted".into()),
        ],
        0,
    )
    .await;
    let (worker_base, _worker_mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;
    let config = write_config(
        &dir,
        &router_base,
        &worker_base,
        "127.0.0.1:7340",
        30_000,
        10_000,
    );

    // The frozen state: the pinned session ended terminally before boot.
    seed_terminal(
        &dir.join("kian.sqlite"),
        router_session(),
        EndReason::Completed,
    )
    .await;

    let (base, _server) = spawn_server(&config).await;
    let read_only = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");
    let client = reqwest::Client::new();

    // The successor armed: its bootstrap judge turn ran.
    let successor = successor_session();
    let events = wait_for_session(
        &read_only,
        successor,
        "successor bootstrap judge turn",
        |ev| count_kind(ev, EventKind::TurnStart) >= 1 && count_kind(ev, EventKind::TurnEnd) >= 1,
    )
    .await;

    // The observable cause: the first admitted input is the re-arm
    // prompt naming the frozen predecessor and its reason.
    let prompt = events
        .iter()
        .find(|e| e.kind == EventKind::InputAdmitted)
        .expect("re-arm prompt admitted")
        .bytes["content"]
        .as_str()
        .expect("prompt text")
        .to_owned();
    assert!(prompt.starts_with("[re-arm]"), "prompt: {prompt}");
    assert!(prompt.contains(&router_session().0.to_string()));
    assert!(prompt.contains("(completed)"));
    assert!(prompt.contains("You are the Router of a super-agent"));

    // The live watch runs on the successor.
    let event_id = ops_mock_channel_send(&client, &base, "tick one").await;
    let events = wait_for_session(
        &read_only,
        successor,
        "tick judged on the successor",
        |ev| {
            count_kind(ev, EventKind::TurnStart) >= 2
                && ev.iter().any(|e| {
                    e.kind == EventKind::InputAdmitted
                        && e.bytes.pointer("/content/kian/event_id") == Some(&json!(event_id))
                })
        },
    )
    .await;
    let request = events
        .iter()
        .rev()
        .find(|e| e.kind == EventKind::StepRequest)
        .expect("judge request");
    let body = serde_json::to_string(&request.bytes["body"]).unwrap();
    assert!(body.contains(&event_id), "judge saw the event: {body}");

    // The frozen predecessor is untouched: still exactly the seeded
    // three events, nothing admitted to it.
    let pinned = read_only.rebuild(router_session()).await.expect("pinned");
    assert_eq!(pinned.len(), 3, "the frozen stream stays frozen");
    // And no successor-of-successor appeared.
    let next = successor_of(successor);
    assert!(read_only.rebuild(next).await.expect("next").is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 2. Reboot: the re-arm bootstrap dedupes; the watch continues on the
//    SAME successor session (child processes, real kill -9)
// ---------------------------------------------------------------------------

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

async fn wait_metrics_ready(base: &str) {
    let client = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(resp) = client
            .get(format!("{base}/metrics"))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            && resp.status().as_u16() == 200
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "kian serve did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn re_arm_bootstrap_dedupes_across_reboots_and_the_watch_continues() {
    force_no_proxy();
    let dir = temp_dir("c62-rearm-reboot");
    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::Text("re-arm acknowledged".into()),
            ScriptedResponse::Text("tick one noted".into()),
            ScriptedResponse::Text("tick two noted".into()),
            ScriptedResponse::Text("and on".into()),
        ],
        0,
    )
    .await;
    let (worker_base, _worker_mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;

    // A fixed free port for the child servers; short lease TTL so the
    // killed boot's lease expires inside the test window.
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let base = format!("http://127.0.0.1:{port}");
    let config = write_config(
        &dir,
        &router_base,
        &worker_base,
        &format!("127.0.0.1:{port}"),
        1_500,
        500,
    );

    seed_terminal(
        &dir.join("kian.sqlite"),
        router_session(),
        EndReason::Completed,
    )
    .await;

    // -- boot #1 ---------------------------------------------------------
    let mut child = spawn_kian_serve(&config);
    wait_metrics_ready(&base).await;
    let read_only = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");
    let successor = successor_session();
    let client = reqwest::Client::new();

    wait_for_session(&read_only, successor, "boot1: successor armed", |ev| {
        count_kind(ev, EventKind::TurnEnd) >= 1
    })
    .await;
    ops_mock_channel_send(&client, &base, "tick one").await;
    wait_for_session(&read_only, successor, "boot1: tick judged", |ev| {
        count_kind(ev, EventKind::TurnEnd) >= 2
    })
    .await;
    let seq_after_boot1 = read_only.rebuild(successor).await.expect("successor").len() as u64;

    // -- kill -9 mid-residency, reboot on the same store ------------------
    child.kill().expect("kill -9 boot1");
    child.wait().expect("reap boot1");

    let mut child2 = spawn_kian_serve(&config);
    wait_metrics_ready(&base).await;

    // -- boot #2: the watch continues on the SAME successor ----------------
    let event_id = ops_mock_channel_send(&client, &base, "tick two").await;
    let events = wait_for_session(
        &read_only,
        successor,
        "boot2: tick judged on successor",
        |ev| {
            count_kind(ev, EventKind::TurnStart) >= 3
                && ev.iter().any(|e| {
                    e.kind == EventKind::InputAdmitted
                        && e.bytes.pointer("/content/kian/event_id") == Some(&json!(event_id))
                })
        },
    )
    .await;
    let seq_after_boot2 = events.len() as u64;
    assert!(
        seq_after_boot2 > seq_after_boot1,
        "the same successor session continues, seq grows ({seq_after_boot1} → {seq_after_boot2})"
    );

    // Exactly one re-arm prompt across both boots: the per-generation
    // key dedupes the second bootstrap admit onto the first row.
    let prompts = events
        .iter()
        .filter(|e| {
            e.kind == EventKind::InputAdmitted
                && e.bytes["content"]
                    .as_str()
                    .is_some_and(|s| s.starts_with("[re-arm]"))
        })
        .count();
    assert_eq!(prompts, 1, "the re-arm prompt is admitted exactly once");

    // The frozen predecessor is still frozen.
    let pinned = read_only.rebuild(router_session()).await.expect("pinned");
    assert_eq!(pinned.len(), 3);

    child2.kill().expect("kill boot2");
    let _ = child2.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// 3. Soft failure is not a succession: the same session re-opens
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn soft_failed_router_re_opens_on_the_same_session() {
    force_no_proxy();
    let dir = temp_dir("c62-softfail-same-session");
    // Bootstrap ok → the next judge turn's stream dies mid-flight → the
    // turn after that recovers.
    let (router_base, router_mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::Text("bootstrap acknowledged".into()),
            ScriptedResponse::DieMidStream,
            ScriptedResponse::Text("recovered".into()),
            ScriptedResponse::Text("and on".into()),
        ],
        0,
    )
    .await;
    let (worker_base, _worker_mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;
    let config = write_config(
        &dir,
        &router_base,
        &worker_base,
        "127.0.0.1:7340",
        30_000,
        10_000,
    );

    let (base, _server) = spawn_server(&config).await;
    let read_only = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");
    let client = reqwest::Client::new();
    let pinned = router_session();

    // Bootstrap judge turn.
    wait_for_session(&read_only, pinned, "bootstrap judge turn", |ev| {
        count_kind(ev, EventKind::TurnEnd) >= 1
    })
    .await;

    // This tick's provider stream dies mid-turn: the session ends
    // honestly with `failed` (which projects idle — re-openable).
    ops_mock_channel_send(&client, &base, "tick one").await;
    wait_for_session(&read_only, pinned, "honest soft failure end", |ev| {
        ev.iter().any(|e| {
            matches!(
                e.kind,
                EventKind::SessionEnded {
                    reason: EndReason::Failed
                }
            )
        })
    })
    .await;

    // The next admitted input re-opens the SAME session and is judged
    // there — no successor session exists.
    let event_id = ops_mock_channel_send(&client, &base, "tick two").await;
    let events = wait_for_session(&read_only, pinned, "same-session re-open judged", |ev| {
        let failed_at = ev
            .iter()
            .position(|e| {
                matches!(
                    e.kind,
                    EventKind::SessionEnded {
                        reason: EndReason::Failed
                    }
                )
            })
            .expect("the failed end is still in the stream");
        ev.iter().any(|e| {
            e.kind == EventKind::InputAdmitted
                && e.seq > ev[failed_at].seq
                && e.bytes.pointer("/content/kian/event_id") == Some(&json!(event_id))
        }) && count_kind(ev, EventKind::TurnStart) >= 3
    })
    .await;
    assert!(
        events.iter().any(|e| e.kind
            == EventKind::SessionEnded {
                reason: EndReason::Failed
            }),
        "the honest failure stays on the record — no fake completion"
    );
    let successor = successor_session();
    assert!(
        read_only
            .rebuild(successor)
            .await
            .expect("successor")
            .is_empty(),
        "a soft failure must not spawn a successor"
    );
    // Exactly three provider calls: bootstrap, the turn that died
    // mid-stream, and the recovered turn — no hidden retries.
    assert_eq!(
        router_mock
            .requests
            .load(std::sync::atomic::Ordering::SeqCst),
        3
    );

    let _ = std::fs::remove_dir_all(&dir);
}
