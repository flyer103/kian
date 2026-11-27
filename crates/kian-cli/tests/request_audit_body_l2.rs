//! integration (ADR-073): a persisted request body holds its images by
//! reference, and `kian session request --expand` rebuilds the exact bytes
//! that flew — verified against the recorded `body_sha256`.
//!
//! Real `kian serve` assembly, real sqlite event store, real HTTP router,
//! in-process mock providers/channel (no credentials, no network egress),
//! and the real CLI binary for the read surface.

mod common;

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use kian_cli::server::Server;
use kian_core::events::{Event, EventKind};
use kian_store::EventStore;
use serde_json::{Value, json};

const ROUTER_SESSION: &str = "ad073000-0000-4000-8000-000000000073";
const IMAGE_BYTES: usize = 30_000;

fn write_config(
    dir: &std::path::Path,
    router_provider: &str,
    worker_provider: &str,
) -> std::path::PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let artifacts = dir.join("artifacts");
    let yaml = format!(
        r#"listen: "127.0.0.1:7393"
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

fn image_parts(messages: &[Value]) -> Vec<Value> {
    messages
        .iter()
        .flat_map(|message| message["content"].as_array().cloned().unwrap_or_default())
        .filter(|part| part["type"] == "image_url")
        .collect()
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
async fn a_stored_request_body_names_its_image_and_expand_rebuilds_the_bytes() {
    force_no_proxy();
    let dir = temp_dir("adr073-audit-body");
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

    let pixels = vec![1u8; IMAGE_BYTES];
    let resp = client
        .post(format!("{base}/ops/rpc"))
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "mock_channel_send",
            "params": {
                "channel": "mock-im",
                "payload": {
                    "text": "picture",
                    "media": [{
                        "kind": "image",
                        "media_type": "image/png",
                        "base64": base64_encode(&pixels),
                    }],
                },
            },
        }))
        .send()
        .await
        .expect("ops send");
    assert_eq!(resp.status(), 200);

    let events = wait_for_router(&read_only, "the picture request is stored", |events| {
        events.iter().any(|e| {
            e.kind == EventKind::StepRequest && !image_parts(&request_messages(e)).is_empty()
        })
    })
    .await;
    let step = events
        .iter()
        .find(|e| e.kind == EventKind::StepRequest && !image_parts(&request_messages(e)).is_empty())
        .expect("the picture request");
    assert_eq!(step.bytes["body_form"], json!("artifact_refs"));
    let stored_part = image_parts(&request_messages(step))
        .into_iter()
        .next()
        .expect("a stored image part");
    let stored_url = stored_part["image_url"]["url"]
        .as_str()
        .expect("stored url");
    assert!(
        stored_url.starts_with("artifact:"),
        "the stored body must name the blob, not carry it: {stored_url}"
    );
    assert_eq!(stored_part["image_url"]["media_type"], json!("image/png"));
    assert_eq!(stored_part["image_url"]["size"], json!(IMAGE_BYTES as u64));

    let out = Command::new(env!("CARGO_BIN_EXE_kian"))
        .args([
            "session",
            "request",
            ROUTER_SESSION,
            "--seq",
            &step.seq.to_string(),
            "--expand",
            "--json",
            "--config",
            &config.display().to_string(),
        ])
        .env("KIAN_E2E_API_KEY", "test")
        .output()
        .expect("run kian session request");
    assert!(
        out.status.success(),
        "kian session request failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let report: Value = serde_json::from_slice(&out.stdout).expect("json report");
    assert_eq!(report["kind"], json!("step/request"));
    assert_eq!(report["body_form"], json!("artifact_refs"));
    assert_eq!(report["expanded"], json!(true));
    assert_eq!(report["references"], json!(1));
    let stored_bytes = report["stored_bytes"].as_u64().expect("stored_bytes");
    let wire_bytes = report["body_bytes"].as_u64().expect("body_bytes");
    assert!(
        stored_bytes < wire_bytes,
        "the stored body is smaller than the wire body: {stored_bytes} vs {wire_bytes}"
    );
    // The command verifies the rebuild against `body_sha256`, so reaching
    // this point means the expanded body hashed back to the recorded wire
    // bytes; compare the payload to make the identity visible.
    let expanded = report["body"]["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .flat_map(|message| message["content"].as_array().cloned().unwrap_or_default())
        .find(|part| part["type"] == "image_url")
        .expect("an expanded image part");
    assert_eq!(
        expanded["image_url"]["url"],
        json!(format!("data:image/png;base64,{}", base64_encode(&pixels)))
    );

    // A seq that is not a request is named as such, never rendered.
    let not_a_request = events
        .iter()
        .find(|e| e.kind == EventKind::TurnStart)
        .expect("turn/start");
    let out = Command::new(env!("CARGO_BIN_EXE_kian"))
        .args([
            "session",
            "request",
            ROUTER_SESSION,
            "--seq",
            &not_a_request.seq.to_string(),
            "--config",
            &config.display().to_string(),
        ])
        .env("KIAN_E2E_API_KEY", "test")
        .output()
        .expect("run kian session request");
    assert!(!out.status.success(), "a turn/start is not a request");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("not a model request"),
        "the error must name the kind: {stderr}"
    );

    std::fs::remove_dir_all(&dir).ok();
}
