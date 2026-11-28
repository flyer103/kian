//! E2E API tests: create → submit → SSE shows
//! `input/admitted` / `drain/claimed` / turn events → projection reaches
//! `completed` against a mock provider, over the REAL openai-compat
//! provider + built-in ReAct loop + local tools assembled by
//! `Server::from_config` from a written `kian.yaml`.
//!
//! Runs with `NO_PROXY=127.0.0.1,localhost` (AGENTS.md gotcha).

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{force_no_proxy, spawn_mock_provider, temp_dir, write_config};
use futures::StreamExt;
use kian_cli::server::Server;

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

/// Spawns the assembled server on an ephemeral port; returns its base
/// URL and the server handle (kept alive for the test duration).
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

/// The mock provider must speak the exact wire format the real
/// openai-compat provider parses (sanity check of the fixture itself).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mock_provider_speaks_the_real_wire_format() {
    force_no_proxy();
    use futures::StreamExt;
    use kian_core::model::SlotName;
    use kian_core::seams::{ModelChunk, ModelProvider, ModelRequest};
    use kian_llm::provider::{
        OpenAiCompatConfig, OpenAiCompatProvider, PersistError, PersistedRequest,
    };

    let (base, mock) = spawn_mock_provider(0).await;
    let provider = OpenAiCompatProvider::new(
        OpenAiCompatConfig {
            name: "mock".into(),
            base_url: base,
            api_key: "test".into(),
            chat_completions_path: "/chat/completions".into(),
            connect_timeout_ms: kian_llm::provider::DEFAULT_PROVIDER_CONNECT_TIMEOUT_MS,
            idle_timeout_ms: kian_llm::provider::DEFAULT_PROVIDER_IDLE_TIMEOUT_MS,
            max_request_bytes: None,
        },
        std::sync::Arc::new(|_r: PersistedRequest| async { Ok::<(), PersistError>(()) }),
    );
    let mut stream = provider.complete(ModelRequest {
        slot: SlotName("main".into()),
        body: serde_json::json!({"model": "mock-model", "messages": []}),
    });
    let mut chunks: Vec<ModelChunk> = Vec::new();
    let mut err: Option<String> = None;
    while let Some(item) = stream.next().await {
        match item {
            Ok(c) => chunks.push(c),
            Err(e) => {
                err = Some(format!("{e:?}"));
                break;
            }
        }
    }
    assert_eq!(mock.requests.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(err.is_none(), "provider error against mock: {err:?}");
    assert!(
        !chunks.is_empty(),
        "mock produced no chunks; fixture is broken"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_submit_sse_turn_events_projection_completed() {
    force_no_proxy();
    let dir = temp_dir("sse");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, mock) = spawn_mock_provider(0).await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:7331");

    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    // create_session
    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid = out["result"]["session"]
        .as_str()
        .expect("session id")
        .to_owned();
    assert!(
        uuid::Uuid::parse_str(&sid).is_ok(),
        "session id must be a uuid, got {sid}"
    );

    // Open the SSE stream BEFORE submitting so the whole trail is seen.
    let sse = client
        .get(format!("{base}/sessions/{sid}/events"))
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .expect("sse open");
    assert_eq!(sse.status(), 200);

    // submit (idempotent key exercised by the follow-up dedup check)
    let params = serde_json::json!({
        "session": sid,
        "content": "Say all done.",
        "idempotency_key": "e2e-1"
    });
    let out = rpc(&client, &base, "submit", params).await;
    assert_eq!(out["result"]["deduplicated"], false);

    // Same idempotency key: the inbox deduplicates.
    let out = rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({
            "session": sid,
            "content": "Say all done.",
            "idempotency_key": "e2e-1"
        }),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], true);

    // Read the SSE stream until the `done` frame.
    let mut kinds: Vec<String> = Vec::new();
    let mut saw_done = false;
    let mut stream = sse.bytes_stream();
    let mut buf = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let chunk = tokio::time::timeout_at(deadline, stream.next())
            .await
            .expect("sse timeout")
            .expect("sse ended early")
            .expect("sse chunk");
        buf.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(pos) = buf.find("\n\n") {
            let frame: String = buf.drain(..pos + 2).collect();
            let mut event_name = "";
            let mut data = String::new();
            for line in frame.lines() {
                if let Some(v) = line.strip_prefix("event: ") {
                    event_name = v.trim();
                } else if let Some(v) = line.strip_prefix("data: ") {
                    data.push_str(v.trim());
                }
            }
            match event_name {
                "kian" => {
                    let ev: serde_json::Value = serde_json::from_str(&data)
                        .unwrap_or_else(|e| panic!("bad kian frame {data}: {e}"));
                    if let Some(kind) = ev["kind"].as_str() {
                        kinds.push(kind.to_owned());
                    }
                }
                "done" => {
                    saw_done = true;
                }
                "error" => panic!("sse error frame: {data}"),
                _ => {}
            }
        }
        if saw_done {
            break;
        }
    }

    // The acceptance trail: admitted → claimed → turn events → ended.
    for expected in [
        "session/created",
        "input/admitted",
        "drain/claimed",
        "slot/binding",
        "turn/start",
        "step/request",
        "assistant/message",
        "turn/end",
        "session/ended",
    ] {
        assert!(
            kinds.contains(&expected.to_owned()),
            "SSE trail missing `{expected}`; saw {kinds:?}"
        );
    }
    assert!(
        saw_done,
        "SSE must end with a done frame after session/ended"
    );

    // Projection reads completed.
    let out = rpc(
        &client,
        &base,
        "projection",
        serde_json::json!({"session": sid}),
    )
    .await;
    assert_eq!(out["result"]["state"], "completed");

    // The mock provider saw exactly one request (the second submit
    // deduplicated, so no extra turn).
    assert_eq!(mock.requests.load(std::sync::atomic::Ordering::SeqCst), 1);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_and_interrupt_semantics_at_the_api_level() {
    force_no_proxy();
    let dir = temp_dir("queue");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config = write_config(
        &dir,
        &dir.join("kian.sqlite"),
        &provider_base,
        "127.0.0.1:7332",
    );
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    // Unknown session: interrupt is a no-op surfaced as unknown_session.
    let ghost = uuid::Uuid::new_v4().to_string();
    let out = rpc(
        &client,
        &base,
        "interrupt",
        serde_json::json!({"session": ghost}),
    )
    .await;
    assert!(
        out["error"]["code"].as_i64().unwrap_or(0) == -32001,
        "interrupt of unknown session must error with unknown-session, got {out}"
    );

    // projection of an unknown session errors the same way.
    let out = rpc(
        &client,
        &base,
        "projection",
        serde_json::json!({"session": ghost}),
    )
    .await;
    assert!(out["error"]["code"].as_i64().unwrap_or(0) == -32001);

    // queue on a fresh session behaves like an inbox write.
    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid = out["result"]["session"].as_str().unwrap().to_owned();
    let out = rpc(
        &client,
        &base,
        "queue",
        serde_json::json!({"session": sid, "content": "queued"}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false);

    // Unknown method is rejected with the standard code.
    let out = rpc(&client, &base, "no_such_method", serde_json::json!({})).await;
    assert_eq!(out["error"]["code"], -32601);

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_config_refuses_to_start_with_clear_error() {
    force_no_proxy();
    let dir = temp_dir("malformed");
    let bad = dir.join("kian.yaml");
    std::fs::write(&bad, "listen: [broken\n").expect("write bad config");

    // Library level: load fails with a clear message.
    let err = kian_cli::config::ServeConfig::load(&bad).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("malformed kian.yaml"),
        "message not clear: {msg}"
    );

    // Binary level: `kian serve` refuses to start, exit code 2, message
    // on stderr.
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_kian"))
        .args(["serve", "--config", bad.to_str().unwrap()])
        .output()
        .expect("run kian");
    assert!(
        !out.status.success(),
        "kian serve must refuse to start on malformed config"
    );
    assert_eq!(out.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("refusing to start"),
        "stderr must say refusing to start, got: {stderr}"
    );

    // Semantic failure (slot → unknown provider) also refuses.
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let sem = dir.join("sem.yaml");
    std::fs::write(
        &sem,
        format!(
            "listen: \"127.0.0.1:7333\"\nstorage:\n  sqlite: \"{}\"\nagent:\n  id: x\n  \
             model_slots:\n    main:\n      provider: missing\n      model: m\nproviders:\n  \
             mock:\n    base_url: \"{provider_base}\"\n    api_key_env: KIAN_E2E_API_KEY\nrecovery:\n  rules:\n    - class: unknown\n      action: fail\n",
            dir.join("sem.sqlite").display()
        ),
    )
    .expect("write sem config");
    let err = kian_cli::config::ServeConfig::load(&sem).unwrap_err();
    assert!(
        err.to_string().contains("not defined in providers"),
        "{}",
        err
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// API-CONTRACT §1.1 (ADR-076): the liveness probe answers from the
/// process alone — 200, `ok`, the crate version — with no token and no
/// store read, so a supervisor never pays the `/metrics` derivation to
/// learn that the listener is serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthz_answers_from_the_process_alone() {
    force_no_proxy();
    let dir = temp_dir("healthz");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:7334");

    let (base, _server) = spawn_server(&config).await;
    let resp = reqwest::Client::new()
        .get(format!("{base}/healthz"))
        .send()
        .await
        .expect("healthz send");
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.expect("healthz json");
    assert_eq!(body["ok"], serde_json::json!(true));
    assert_eq!(
        body["version"],
        serde_json::json!(env!("CARGO_PKG_VERSION"))
    );

    let _ = std::fs::remove_dir_all(&dir);
}
