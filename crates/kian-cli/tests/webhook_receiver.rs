//! webhook receiver e2e: the real assembled server (`kian serve`
//! shape) with a `type: github` channel, driven over the real HTTP
//! route with HMAC-signed deliveries — and the honest fail-closed
//! startup refusals. No credentials, no network (the provider is the
//! in-process mock; GitHub is a fixture body). Runs with
//! `NO_PROXY=127.0.0.1,localhost` (AGENTS.md gotcha).

mod common;

use std::time::Duration;

use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use hmac::{Hmac, Mac};
use kian_cli::server::Server;
use kian_core::events::EventKind;
use kian_core::model::SessionId;
use kian_store::EventStore;
use serde_json::{Value, json};
use sha2::Sha256;

const ROUTER_SESSION: &str = "1234abcd-1234-abcd-1234-abcd1234abcd";
const WEBHOOK_SECRET: &str = "whsec_e2e";

/// Env-var tests mutate process-global state; serialize them. The
/// guard is held across awaits (server startup), so it must be
/// async-aware (clippy await_holding_lock).
static ENV_MUTEX: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

fn sign(body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(WEBHOOK_SECRET.as_bytes()).expect("key");
    mac.update(body);
    let digest = mac.finalize().into_bytes();
    format!(
        "sha256={}",
        digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    )
}

/// A full super-agent config with a github channel (receive-only —
/// no token, no send) routed `always → router`.
fn write_github_config(dir: &std::path::Path, provider_base: &str) -> std::path::PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let yaml = format!(
        r#"listen: "127.0.0.1:7350"
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
  worker:
    agent: worker
  subscriptions:
    - channel: github
      match: always
      route: router
  channels:
    github:
      type: github
      github:
        topics:
          flyer103/kian-runtime: ci
providers:
  mock:
    base_url: "{provider_base}"
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
    std::fs::write(&path, yaml).expect("write github config");
    path
}

/// Spawns the assembled server on an ephemeral port.
async fn spawn_server(config_path: &std::path::Path) -> (String, std::sync::Arc<Server>) {
    let server = std::sync::Arc::new(
        Server::from_config_file(config_path)
            .await
            .expect("assemble github server")
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

/// An issue_comment delivery body for the mapped repo.
fn delivery_body() -> Value {
    json!({
        "action": "created",
        "issue": {"number": 9, "title": "build broken on main", "body": "the CI log shows a panic"},
        "comment": {"id": 99, "body": "please take a look"},
        "repository": {"id": 1, "full_name": "flyer103/kian-runtime", "name": "kian"},
        "sender": {"login": "reporter", "type": "User", "id": 7}
    })
}

async fn post_delivery(
    base: &str,
    delivery_id: &str,
    body: &Value,
    signature: Option<&str>,
) -> reqwest::Response {
    let text = serde_json::to_string(body).expect("serialize body");
    let mut builder = reqwest::Client::new()
        .post(format!("{base}/webhooks/github"))
        .header("content-type", "application/json")
        .header("x-github-event", "issue_comment")
        .header("x-github-delivery", delivery_id)
        .body(text.clone());
    builder = builder.header(
        "x-hub-signature-256",
        signature
            .map(str::to_owned)
            .unwrap_or_else(|| sign(text.as_bytes())),
    );
    builder.send().await.expect("webhook post")
}

/// How many `input/admitted` events in the Router stream carry the
/// given delivery id (the durable admission count — the (session,
/// event_id) idempotency is what a replay must not double).
async fn admission_count(sqlite: &std::path::Path, delivery_id: &str) -> usize {
    let store = EventStore::open_read_only(sqlite)
        .await
        .expect("read-only store");
    let session = SessionId(uuid::Uuid::parse_str(ROUTER_SESSION).unwrap());
    let events = store.rebuild(session).await.expect("router stream");
    events
        .iter()
        .filter(|event| {
            event.kind == EventKind::InputAdmitted
                && serde_json::to_string(&event.bytes)
                    .map(|bytes| bytes.contains(delivery_id))
                    .unwrap_or(false)
        })
        .count()
}

/// Polls the admission count until it equals `expected` (bounded).
async fn await_admissions(sqlite: &std::path::Path, delivery_id: &str, expected: usize) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if admission_count(sqlite, delivery_id).await == expected {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return admission_count(sqlite, delivery_id).await == expected;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// The full acceptance path: a validly signed, known event
/// admits exactly once through the whole stack; a delivery-id replay
/// stays one admission; an invalid signature admits nothing.
#[tokio::test]
async fn verified_delivery_admits_exactly_once_and_replay_is_idempotent() {
    let _guard = ENV_MUTEX.lock().await;
    force_no_proxy();
    unsafe { std::env::set_var("KIAN_GITHUB_WEBHOOK_SECRET", WEBHOOK_SECRET) };

    let dir = temp_dir("c49-e2e");
    let (provider_base, _mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("triaged.".into())], 0).await;
    let config = write_github_config(&dir, &provider_base);
    let (base, _server) = spawn_server(&config).await;

    // Valid signature + known event → surfaced 1.
    let response = post_delivery(&base, "d-e2e-1", &delivery_body(), None).await;
    assert_eq!(response.status(), 200);
    assert_eq!(response.json::<Value>().await.expect("json")["surfaced"], 1);

    // Exactly one admission lands in the Router stream.
    let sqlite = dir.join("kian.sqlite");
    assert!(
        await_admissions(&sqlite, "d-e2e-1", 1).await,
        "the verified delivery must admit exactly once"
    );

    // Delivery-id replay: still 200 (GitHub redelivers), still ONE
    // admission — the pipeline's (session, event_id) idempotency plus
    // the adapter ring.
    let response = post_delivery(&base, "d-e2e-1", &delivery_body(), None).await;
    assert_eq!(response.status(), 200);
    assert_eq!(response.json::<Value>().await.expect("json")["surfaced"], 0);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        admission_count(&sqlite, "d-e2e-1").await,
        1,
        "the replay must not double-admit"
    );
}

/// Invalid signature → 401, zero admissions.
#[tokio::test]
async fn invalid_signature_is_401_with_zero_admissions() {
    let _guard = ENV_MUTEX.lock().await;
    force_no_proxy();
    unsafe { std::env::set_var("KIAN_GITHUB_WEBHOOK_SECRET", WEBHOOK_SECRET) };

    let dir = temp_dir("c49-bad-sig");
    let (provider_base, _mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("ok".into())], 0).await;
    let config = write_github_config(&dir, &provider_base);
    let (base, _server) = spawn_server(&config).await;

    let response = post_delivery(&base, "d-bad", &delivery_body(), Some("sha256=deadbeef")).await;
    assert_eq!(response.status(), 401);
    assert_eq!(
        admission_count(&dir.join("kian.sqlite"), "d-bad").await,
        0,
        "no admission for an unverified delivery"
    );
}

/// A configured route without the env secret refuses to start
/// (fail-closed, never a signature-less receiver).
#[tokio::test]
async fn configured_route_without_secret_refuses_to_start() {
    let _guard = ENV_MUTEX.lock().await;
    force_no_proxy();
    unsafe { std::env::remove_var("KIAN_GITHUB_WEBHOOK_SECRET") };

    let dir = temp_dir("c49-no-secret");
    let (provider_base, _mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("never reached".into())], 0).await;
    let config = write_github_config(&dir, &provider_base);
    let error = match Server::from_config_file(&config).await {
        Ok(_) => panic!("must refuse to start without the webhook secret"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("KIAN_GITHUB_WEBHOOK_SECRET"),
        "error names the env var: {error}"
    );

    // Restore for any later test in this binary.
    unsafe { std::env::set_var("KIAN_GITHUB_WEBHOOK_SECRET", WEBHOOK_SECRET) };
}

/// Without a github channel the route is simply absent (404) — the
/// built-in/ servers never expose the webhook surface.
#[tokio::test]
async fn route_is_absent_without_a_github_channel() {
    let _guard = ENV_MUTEX.lock().await;
    force_no_proxy();
    unsafe { std::env::remove_var("KIAN_GITHUB_WEBHOOK_SECRET") };

    let dir = temp_dir("c49-no-route");
    let (provider_base, _mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("ok".into())], 0).await;
    // A plain super-agent config (mock channel only).
    let sqlite = dir.join("kian.sqlite");
    let yaml = format!(
        r#"listen: "127.0.0.1:7351"
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
  mock:
    base_url: "{provider_base}"
    api_key_env: KIAN_E2E_API_KEY
plugins: ["openai-compat", "local-tools", "superagent"]
drain:
  max_iterations: 8
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
    );
    let config = dir.join("kian.yaml");
    std::fs::write(&config, yaml).expect("write plain config");
    let (base, _server) = spawn_server(&config).await;

    let response = reqwest::Client::new()
        .post(format!("{base}/webhooks/github"))
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .expect("post");
    assert_eq!(response.status(), 404, "no github channel, no route");

    unsafe { std::env::set_var("KIAN_GITHUB_WEBHOOK_SECRET", WEBHOOK_SECRET) };
}
