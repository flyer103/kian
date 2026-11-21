//! ADR-040 phase 2, live: the real `kian serve` ticker + the resident
//! Router driver rotate without a restart.
//!
//! The Router runs its bootstrap judge turn (a scripted provider), parks,
//! sees its stream over `rotate_after_events` and ends itself `rotated`
//! (the hold owner is the only one that can end a parked drain). The
//! serve rotation ticker then catches up: it derives the uuid5 successor,
//! re-points the binding and bootstraps the successor (the handoff and
//! the judge prompt), which runs its own turn. Observations read the
//! committed streams through a read-only store handle.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{force_no_proxy, spawn_mock_provider, temp_dir};
use kian_cli::server::Server;
use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::SessionId;
use kian_store::EventStore;
use uuid::Uuid;

const ROUTER_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

fn write_config(
    dir: &std::path::Path,
    provider_base: &str,
    sqlite: &std::path::Path,
) -> std::path::PathBuf {
    let yaml = format!(
        r#"listen: "127.0.0.1:0"
storage:
  sqlite: "{sqlite}"
agent:
  id: default
  model_slots:
    main:
      provider: mock
      model: mock-model
  tools: ["fs_read"]
agents:
  router:
    id: router
    version: 3
    model_slots:
      router:
        provider: mock
        model: mock-router
    tools: ["spawn_worker", "reply", "deliver_structured"]
  worker:
    id: worker
    version: 7
    model_slots:
      main:
        provider: mock
        model: mock-worker
    tools: ["fs_read", "fs_write", "exec"]
superagent:
  router:
    session_id: "{ROUTER_SESSION}"
    agent: router
    slot: router
    rotate_after_events: 2
  worker:
    agent: worker
  channels:
    mock-im:
      type: mock-im
providers:
  mock:
    base_url: "{provider_base}"
    api_key_env: KIAN_E2E_API_KEY
plugins: ["openai-compat", "local-tools", "superagent"]
drain:
  max_iterations: 4
  heartbeat_interval_ms: 1200
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
        dir = dir.display(),
        sqlite = sqlite.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write kian.yaml");
    path
}

fn router_session() -> SessionId {
    SessionId(Uuid::parse_str(ROUTER_SESSION).unwrap())
}

/// Polls `session`'s stream until `pred` holds.
async fn wait_for(
    store: &EventStore,
    session: SessionId,
    desc: &str,
    pred: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    for _ in 0..800 {
        let events = store.rebuild(session).await.expect("rebuild");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

fn input_texts(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter(|ev| ev.kind == EventKind::InputAdmitted)
        .map(|ev| {
            let content = ev.bytes.get("content").cloned().unwrap_or_default();
            if let Some(text) = content.as_str() {
                return text.to_string();
            }
            content
                .get("content")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect()
}

#[tokio::test]
async fn the_serve_ticker_rotates_the_router_without_a_restart() {
    force_no_proxy();
    let dir = temp_dir("rotation-live");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config_path = write_config(&dir, &provider_base, &sqlite);
    let (server, _) = Server::from_config_file(&config_path)
        .await
        .expect("assemble the server");
    let server = Arc::new(server);
    // The real serve path: it binds and spawns the maintenance tickers
    // (including the rotation ticker) — the rotation under test.
    let serving = tokio::spawn(async move {
        server
            .serve("127.0.0.1:0".parse().expect("addr"))
            .await
            .expect("serve");
    });

    let store = EventStore::open_read_only(&sqlite)
        .await
        .expect("read-only store");
    let pinned = router_session();

    // 1. The Router driver (the hold owner) ends the parked session
    //    `rotated` once its stream is over the threshold.
    let old = wait_for(&store, pinned, "the driver-driven rotated end", |events| {
        matches!(
            events.last().map(|ev| &ev.kind),
            Some(EventKind::SessionEnded {
                reason: EndReason::Rotated
            })
        )
    })
    .await;
    assert!(
        old.len() >= 2,
        "the rotated session carries its bootstrap turn: {}",
        old.len()
    );

    // 2. The serve rotation ticker catches up: the deterministic
    //    successor receives the handoff and the judge prompt (no memory
    //    section in this config, so the handoff is first).
    let successor = kian_superagent::rearm::successor_session(pinned);
    let events = wait_for(&store, successor, "the successor's bootstrap", |events| {
        let texts = input_texts(events);
        texts.iter().any(|text| text.contains("[rotation]"))
            && texts.iter().any(|text| text.len() > 100)
    })
    .await;
    let texts = input_texts(&events);
    assert!(
        texts.iter().any(|text| text.contains("[rotation]")),
        "the successor receives the rotation handoff: {texts:?}"
    );
    assert!(
        texts.iter().any(|text| text.len() > 100),
        "the successor receives the judge prompt: {texts:?}"
    );

    serving.abort();
}
