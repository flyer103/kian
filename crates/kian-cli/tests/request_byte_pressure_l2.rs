//! integration (ADR-072): a request over the declared provider byte bound
//! folds its oldest turns and only then flies.
//!
//! Real `kian serve` assembly, real sqlite event store, real HTTP router,
//! in-process mock providers/channel (no credentials, no network egress).
//! Everything asserted here is read back from the committed stream.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use kian_cli::server::Server;
use kian_core::events::{Event, EventKind};
use kian_store::EventStore;
use serde_json::{Value, json};

const ROUTER_SESSION: &str = "ad071000-0000-4000-8000-000000000071";
/// The declared provider bound, small enough that eight 30 kB pictures
/// push the rendered request over it while four retained turns still fit.
const BOUND: u64 = 250_000;
const IMAGE_BYTES: usize = 30_000;
const MESSAGES: usize = 10;

fn write_config(
    dir: &std::path::Path,
    router_provider: &str,
    worker_provider: &str,
) -> std::path::PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let artifacts = dir.join("artifacts");
    let yaml = format!(
        r#"listen: "127.0.0.1:7391"
storage:
  sqlite: "{sqlite}"
artifacts:
  dir: "{artifacts}"
media:
  base_url: "http://127.0.0.1:9"
  api_key_env: KIAN_E2E_API_KEY
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
    tools: ["spawn_worker", "reply", "image_ocr"]
  worker:
    id: worker
    version: 7
    model_slots:
      main:
        provider: worker-mock
        model: mock-worker
    tools: ["fs_read", "fs_write", "exec"]
superagent:
  shaping:
    vision: true
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
model_profiles:
  router-mock/mock-router:
    vision: true
providers:
  router-mock:
    base_url: "{router_provider}"
    api_key_env: KIAN_E2E_API_KEY
    max_request_bytes: {BOUND}
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
        artifacts = artifacts.display(),
        dir = dir.display(),
        router_provider = router_provider,
        worker_provider = worker_provider,
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write config");
    path
}

async fn spawn_server(config: &std::path::Path) -> (String, Arc<Server>) {
    let server = Arc::new(
        Server::from_config_file(config)
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

async fn ops_rpc(client: &reqwest::Client, base: &str, method: &str, params: Value) -> Value {
    let resp = client
        .post(format!("{base}/ops/rpc"))
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .expect("ops send");
    assert_eq!(resp.status(), 200);
    resp.json().await.expect("ops json")
}

fn router_session() -> kian_core::model::SessionId {
    kian_core::model::SessionId(uuid::Uuid::parse_str(ROUTER_SESSION).unwrap())
}

fn count_kind(events: &[Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind).count()
}

async fn wait_for_router(
    store: &EventStore,
    desc: &str,
    pred: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    let session = router_session();
    for _ in 0..1600 {
        let events = store.rebuild(session).await.expect("rebuild router");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

fn request_messages(event: &Event) -> Vec<Value> {
    let body = event
        .bytes
        .get("body")
        .cloned()
        .unwrap_or_else(|| event.bytes.clone());
    body.get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn image_parts(messages: &[Value]) -> usize {
    messages
        .iter()
        .flat_map(|message| message["content"].as_array().cloned().unwrap_or_default())
        .filter(|part| part["type"] == "image_url")
        .count()
}

fn image_payload(index: usize) -> Value {
    let bytes = vec![index as u8 + 1; IMAGE_BYTES];
    json!({
        "text": format!("picture {index}"),
        "media": [{
            "kind": "image",
            "media_type": "image/png",
            "base64": base64_encode(&bytes),
        }],
    })
}

fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_over_the_bound_folds_history_before_it_is_sent() {
    force_no_proxy();
    let dir = temp_dir("adr071-byte-pressure");
    let (router_base, _router_mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("noted".into())], 0).await;
    let (worker_base, _worker_mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;
    let config = write_config(&dir, &router_base, &worker_base);
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    let read_only = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");
    wait_for_router(&read_only, "router bootstrap turn", |events| {
        count_kind(events, EventKind::AssistantMessage) >= 1
    })
    .await;

    for index in 0..MESSAGES {
        let want = count_kind(
            &read_only.rebuild(router_session()).await.expect("rebuild"),
            EventKind::TurnStart,
        ) + 1;
        ops_rpc(
            &client,
            &base,
            "mock_channel_send",
            json!({"channel": "mock-im", "payload": image_payload(index)}),
        )
        .await;
        wait_for_router(&read_only, "the injected turn starts", |events| {
            count_kind(events, EventKind::TurnStart) >= want
        })
        .await;
        wait_for_router(&read_only, "the injected turn ends", |events| {
            events
                .iter()
                .filter(|e| e.kind == EventKind::TurnEnd)
                .count()
                >= want
        })
        .await;
    }

    let events = wait_for_router(&read_only, "a byte-pressure fold", |events| {
        events.iter().any(|e| {
            e.kind == EventKind::Compact
                && e.bytes["reason"]
                    .as_str()
                    .is_some_and(|reason| reason.starts_with("request byte pressure: "))
        })
    })
    .await;

    let fold = events
        .iter()
        .position(|e| {
            e.kind == EventKind::Compact
                && e.bytes["reason"]
                    .as_str()
                    .is_some_and(|reason| reason.starts_with("request byte pressure: "))
        })
        .expect("byte fold");
    let reason = events[fold].bytes["reason"].as_str().unwrap();
    assert!(
        reason.contains(&format!("> provider bound {BOUND} bytes")),
        "the fold names the bound and the measured size: {reason}"
    );

    // The gate runs before the provider call: the requests that could have
    // flown are the pixels the fold reclaimed, and the first request after
    // the fold is the one that actually went out.
    let widest_before = events[..fold]
        .iter()
        .filter(|e| e.kind == EventKind::StepRequest)
        .map(|e| image_parts(&request_messages(e)))
        .max()
        .expect("the over-bound request was rendered");
    let after = events[fold + 1..]
        .iter()
        .find(|e| e.kind == EventKind::StepRequest)
        .expect("the folded request flies");
    let after_parts = image_parts(&request_messages(after));
    assert!(
        widest_before > after_parts,
        "the fold reclaims pixels: before={widest_before} after={after_parts}"
    );
    assert!(
        after_parts > 0,
        "the newest turns keep their pixels: {after_parts}"
    );
    assert!(
        events[fold + 1..]
            .iter()
            .filter(|e| e.kind == EventKind::TurnEnd)
            .any(|e| e.bytes["outcome"] == json!("final_answer")),
        "the folded turn settles with an answer"
    );

    std::fs::remove_dir_all(&dir).ok();
}
