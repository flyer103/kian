//! integration tests: the super-agent assembly in `kian serve`.
//!
//! All tests run with `NO_PROXY=127.0.0.1,localhost` (AGENTS.md
//! gotcha), against the real openai-compat provider pointed at
//! in-process mock providers (the `common` fixtures — no
//! credentials, no network egress), the real sqlite event store and
//! the real engine. Observations read the committed streams through a
//! read-only store handle, never the server's internals.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use kian_cli::server::Server;
use kian_core::events::EventKind;
use kian_store::EventStore;

const ROUTER_SESSION: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

/// One unused 127.0.0.1 port (dialed by the server, nothing answers —
/// the "endpoint offline" stand-in for the watchdog test).
fn dead_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    port
}

/// A full super-agent `kian.yaml` over two mock providers (router /
/// worker scripts stay independent) and, when `endpoint` is set, one
/// dead remote endpoint.
fn write_superagent_config(
    dir: &std::path::Path,
    router_provider: &str,
    worker_provider: &str,
    endpoint: Option<(&str, u64, u64)>,
    worker_gate: bool,
) -> std::path::PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let worker_gate = if worker_gate {
        "    evidence_gate:\n      enabled: true\n      checks:\n        - type: final_message_present\n      max_retries: 1\n"
    } else {
        ""
    };
    // remote-exec is presence-locked to the endpoints list.
    let remote = if endpoint.is_some() {
        ", \"remote-exec\""
    } else {
        ""
    };
    let yaml = format!(
        r#"listen: "127.0.0.1:7340"
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
{worker_gate}superagent:
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
{{endpoints}}providers:
  router-mock:
    base_url: "{router_provider}"
    api_key_env: KIAN_E2E_API_KEY
  worker-mock:
    base_url: "{worker_provider}"
    api_key_env: KIAN_E2E_API_KEY
plugins: ["openai-compat", "local-tools", "superagent"{remote}]
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
        worker_gate = worker_gate,
        remote = remote,
    );
    let yaml = if let Some((addr, queue_timeout_ms, offline_after_ms)) = endpoint {
        yaml.replace(
            "{endpoints}",
            &format!(
                "endpoints:\n  - id: ep1\n    addr: \"{addr}\"\n    queue_timeout_ms: {queue_timeout_ms}\n    offline_after_ms: {offline_after_ms}\n"
            ),
        )
    } else {
        yaml.replace("{endpoints}", "")
    };
    let _ = (&router_provider, &worker_provider);
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write superagent config");
    path
}

/// Spawns the assembled server on an ephemeral port.
async fn spawn_server(config_path: &std::path::Path) -> (String, Arc<Server>) {
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

async fn ops_rpc(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{base}/ops/rpc"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .expect("ops send");
    assert_eq!(resp.status(), 200);
    resp.json().await.expect("ops json")
}

fn router_session() -> kian_core::model::SessionId {
    kian_core::model::SessionId(uuid::Uuid::parse_str(ROUTER_SESSION).unwrap())
}

fn count_kind(events: &[kian_core::events::Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind).count()
}

/// Polls the committed router stream until `pred` holds.
async fn wait_for_router(
    store: &EventStore,
    desc: &str,
    pred: impl Fn(&[kian_core::events::Event]) -> bool,
) -> Vec<kian_core::events::Event> {
    let session = router_session();
    for _ in 0..800 {
        let events = store.rebuild(session).await.expect("rebuild router");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

/// Polls an arbitrary session's committed stream until `pred` holds.
async fn wait_for_session(
    store: &EventStore,
    session: kian_core::model::SessionId,
    desc: &str,
    pred: impl Fn(&[kian_core::events::Event]) -> bool,
) -> Vec<kian_core::events::Event> {
    for _ in 0..1200 {
        let events = store.rebuild(session).await.expect("rebuild session");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

// ---------------------------------------------------------------------------
// Serve smoke: ops mock_channel_send → subscription → Router drain
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_smoke_mock_channel_send_observes_router_drain() {
    force_no_proxy();
    let dir = temp_dir("c27-smoke");

    // Router script: bootstrap ack, then an idle final answer for the
    // judged batch (the resident Router parks afterward).
    let (router_base, router_mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::Text("bootstrap acknowledged".into()),
            ScriptedResponse::Text("batch noted".into()),
        ],
        0,
    )
    .await;
    let (worker_base, _worker_mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;

    let config = write_superagent_config(&dir, &router_base, &worker_base, None, false);
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    // Wait for the resident Router's bootstrap judge turn.
    let read_only = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");
    wait_for_router(&read_only, "bootstrap judge turn", |events| {
        count_kind(events, EventKind::TurnStart) >= 1
            && count_kind(events, EventKind::AssistantMessage) >= 1
    })
    .await;

    // The demo path: one ops injection onto the mock channel.
    let out = ops_rpc(
        &client,
        &base,
        "mock_channel_send",
        serde_json::json!({"channel": "mock-im", "payload": {"text": "please summarize"}}),
    )
    .await;
    let event_id = out["result"]["event_id"]
        .as_str()
        .expect("event id")
        .to_owned();
    assert_eq!(out["result"]["channel"], "mock-im");

    // The subscription engine admitted it and the Router judged it in
    // its own drain: input/admitted with the channel marker (event id
    // verbatim), then a second provider turn whose request body saw it.
    let events = wait_for_router(&read_only, "batch judge turn", |events| {
        count_kind(events, EventKind::TurnStart) >= 2 && count_kind(events, EventKind::TurnEnd) >= 2
    })
    .await;

    let admitted = events
        .iter()
        .find(|e| {
            e.kind == EventKind::InputAdmitted
                && e.bytes.pointer("/content/kian/type")
                    == Some(&serde_json::json!("channel_event"))
        })
        .expect("channel input admitted on the router stream");
    assert_eq!(
        admitted.bytes.pointer("/content/kian/event_id"),
        Some(&serde_json::json!(event_id)),
        "the input carries the injected event id verbatim"
    );

    let request = events
        .iter()
        .rev()
        .find(|e| e.kind == EventKind::StepRequest)
        .expect("judge request");
    let body = serde_json::to_string(&request.bytes["body"]).unwrap();
    assert!(
        body.contains(&event_id),
        "judge turn saw the event; body: {body}"
    );

    // Exactly two router provider calls: bootstrap + batch.
    assert_eq!(
        router_mock
            .requests
            .load(std::sync::atomic::Ordering::SeqCst),
        2
    );

    // /metrics carries the counters.
    let metrics: serde_json::Value = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics")
        .json()
        .await
        .expect("metrics json");
    let sa = &metrics["superagent"];
    assert_eq!(sa["router_session"], *ROUTER_SESSION);
    assert!(
        sa["subscriptions"]["routed"].as_u64().unwrap_or(0) >= 1,
        "subscription routed counter: {sa}"
    );
    assert!(sa["evidence_verdicts"].is_object());
    assert!(sa["channel_policy"].is_object());
    assert!(sa["watchdog"].is_null(), "no endpoints ⇒ no watchdog");

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// /metrics exposes the router-batching series (article §7.2 shape)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_route_exposes_the_router_batching_series() {
    force_no_proxy();
    let dir = temp_dir("c41-router-batching");

    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::Text("bootstrap acknowledged".into()),
            ScriptedResponse::Text("batch noted".into()),
        ],
        0,
    )
    .await;
    let (worker_base, _worker_mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into())], 0).await;

    let config = write_superagent_config(&dir, &router_base, &worker_base, None, false);
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    let read_only = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");
    wait_for_router(&read_only, "bootstrap judge turn", |events| {
        count_kind(events, EventKind::TurnStart) >= 1
            && count_kind(events, EventKind::AssistantMessage) >= 1
    })
    .await;

    // One injected channel event → a second judge turn (batch 1).
    let out = ops_rpc(
        &client,
        &base,
        "mock_channel_send",
        serde_json::json!({"channel": "mock-im", "payload": {"text": "please summarize"}}),
    )
    .await;
    assert!(out["result"]["event_id"].as_str().is_some());
    wait_for_router(&read_only, "batch judge turn", |events| {
        count_kind(events, EventKind::TurnStart) >= 2 && count_kind(events, EventKind::TurnEnd) >= 2
    })
    .await;

    // The judge-turn record lands as the parked turn closes; poll the
    // route until it shows both turns.
    let mut sa = serde_json::Value::Null;
    for _ in 0..200 {
        let metrics: serde_json::Value = client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .expect("metrics")
            .json()
            .await
            .expect("metrics json");
        sa = metrics["superagent"].clone();
        if sa["router_batching"]["judge_turns"].as_u64().unwrap_or(0) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let rb = &sa["router_batching"];
    assert!(
        rb["judge_turns"].as_u64().unwrap_or(0) >= 2,
        "router_batching must record the judge turns: {sa}"
    );
    assert!(
        rb["inputs_consumed"].as_u64().unwrap_or(0) >= 2,
        "bootstrap input + injected event: {sa}"
    );
    // The existing naming scheme: snake_case series keys plus the
    // bounded, fully materialized batch histogram.
    for key in ["1", "2", "4", "8", "16", "32", "64", "128", "128+"] {
        assert!(
            rb["batch_histogram"].get(key).is_some(),
            "bucket `{key}` missing from {sa}"
        );
    }
    assert!(
        rb["batch_histogram"]["1"].as_u64().unwrap_or(0) >= 2,
        "both turns were batches of 1: {sa}"
    );
    assert!(rb["turn_duration_ms_total"].as_u64().is_some());
    assert!(rb["mean_turn_duration_ms"].as_f64().is_some());
    assert!(rb["amortized_ms_per_input"].as_f64().is_some());
    assert!(
        rb["last_turn"]["batch"].as_u64().unwrap_or(0) >= 1,
        "last_turn records the most recent batch: {sa}"
    );
    assert!(rb["last_turn"]["duration_ms"].as_u64().is_some());
    assert!(rb["last_turn"]["amortized_ms"].as_f64().is_some());

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Watchdog: offline endpoint → health input admitted to the affected
// worker session (the durable-row acceptance )
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watchdog_offline_admits_health_input_to_affected_worker_session() {
    force_no_proxy();
    let dir = temp_dir("c27-watchdog");

    // Router: ack, then dispatch a worker for the incoming event.
    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::Text("bootstrap acknowledged".into()),
            ScriptedResponse::ToolCall {
                id: "call-spawn-1".into(),
                name: "spawn_worker".into(),
                arguments: serde_json::json!({"task": "run the release checklist"}),
            },
            ScriptedResponse::Text("worker dispatched".into()),
        ],
        0,
    )
    .await;
    // Worker: one exec call onto the dead endpoint, then a final answer
    // after the honest timeout.
    let (worker_base, _worker_mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::ToolCall {
                id: "call-exec-1".into(),
                name: "exec".into(),
                arguments: serde_json::json!({"argv": ["echo", "hi"]}),
            },
            ScriptedResponse::Text("exec endpoint is down; giving up honestly".into()),
        ],
        0,
    )
    .await;

    let port = dead_port();
    let config = write_superagent_config(
        &dir,
        &router_base,
        &worker_base,
        Some((&format!("127.0.0.1:{port}"), 3000, 300)),
        true, // worker evidence gate: final_message_present
    );
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    let read_only = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");
    wait_for_router(&read_only, "bootstrap judge turn", |events| {
        count_kind(events, EventKind::AssistantMessage) >= 1
    })
    .await;

    // The work request that triggers spawn → worker → exec (queued,
    // endpoint offline).
    ops_rpc(
        &client,
        &base,
        "mock_channel_send",
        serde_json::json!({"channel": "mock-im", "payload": {"text": "deploy please"}}),
    )
    .await;

    // The Router dispatched: thread/started names the worker session.
    let router_events = wait_for_router(&read_only, "router dispatches the worker", |events| {
        count_kind(events, EventKind::ThreadStarted) >= 1
    })
    .await;
    let worker_session: kian_core::model::SessionId = {
        let started = router_events
            .iter()
            .find(|e| e.kind == EventKind::ThreadStarted)
            .unwrap();
        let raw = started.bytes["worker"].as_str().unwrap();
        kian_core::model::SessionId(uuid::Uuid::parse_str(raw).unwrap())
    };

    // The worker called exec; the row parks on the dead endpoint.
    let worker_events = wait_for_session(
        &read_only,
        worker_session,
        "worker issues the exec tool call",
        |events| count_kind(events, EventKind::ToolCall) >= 1,
    )
    .await;
    assert!(
        worker_events
            .iter()
            .any(|e| e.kind == EventKind::ToolCall && e.bytes["content"]["name"] == "exec"),
        "the worker's tool call is the remote exec"
    );

    // The watchdog's offline edge reaches the worker as a durable
    // health input row (the acceptance).
    let worker_events = wait_for_session(
        &read_only,
        worker_session,
        "health input admitted to the worker session",
        |events| {
            events.iter().any(|e| {
                e.kind == EventKind::InputAdmitted
                    && e.bytes.pointer("/content/kian/type")
                        == Some(&serde_json::json!("endpoint_health"))
            })
        },
    )
    .await;
    let health = worker_events
        .iter()
        .rev()
        .find(|e| {
            e.kind == EventKind::InputAdmitted
                && e.bytes.pointer("/content/kian/type")
                    == Some(&serde_json::json!("endpoint_health"))
        })
        .unwrap();
    assert_eq!(
        health.bytes.pointer("/content/kian/endpoint"),
        Some(&serde_json::json!("ep1"))
    );
    assert_eq!(
        health.bytes.pointer("/content/kian/status"),
        Some(&serde_json::json!("offline"))
    );
    assert_eq!(
        health.bytes.pointer("/content/kian/queued_commands"),
        Some(&serde_json::json!(1))
    );

    // Honesty across the offline window: the queue timeout resolves to
    // an honest error tool result, the worker settles completed (its
    // final answer), the gate passes, and the report pair exists.
    let worker_events = wait_for_session(
        &read_only,
        worker_session,
        "worker settles after the honest timeout",
        |events| {
            events
                .iter()
                .any(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
        },
    )
    .await;
    let exec_result = worker_events
        .iter()
        .find(|e| e.kind == EventKind::ToolResult)
        .expect("exec tool result");
    assert_eq!(
        exec_result.bytes.pointer("/content/is_error"),
        Some(&serde_json::json!(true)),
        "the offline exec is an honest error, never a fake success"
    );
    assert!(
        exec_result
            .bytes
            .pointer("/content/output/error")
            .and_then(|v| v.as_str())
            .is_some_and(|e| {
                let lower = e.to_lowercase();
                lower.contains("timeout") || lower.contains("timed out")
            }),
        "the honest error is the queue timeout: {exec_result:?}"
    );
    assert!(
        worker_events
            .iter()
            .any(|e| e.kind == EventKind::EvidenceVerdict
                && e.bytes["passed"] == serde_json::json!(true)),
        "the evidence gate passed the worker's final answer"
    );
    assert_eq!(
        worker_events
            .iter()
            .filter(|e| e.kind == EventKind::ThreadCompleted)
            .count(),
        1
    );

    // /metrics: the watchdog transitions are observable.
    let metrics: serde_json::Value = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics")
        .json()
        .await
        .expect("metrics json");
    assert!(
        metrics["superagent"]["watchdog"]["offline_transitions"]
            .as_u64()
            .unwrap_or(0)
            >= 1,
        "watchdog counters wired: {}",
        metrics["superagent"]
    );
    assert_eq!(
        metrics["superagent"]["evidence_verdicts"]["passed"],
        serde_json::json!(1)
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Startup order: nothing super-agent starts before startup_rebuild
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_order_subscriptions_and_watchdog_start_after_rebuild() {
    force_no_proxy();
    let dir = temp_dir("c27-order");
    let (router_base, _a) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("ack".into())], 0).await;
    let (worker_base, _b) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("ack".into())], 0).await;
    let port = dead_port();
    let config = write_superagent_config(
        &dir,
        &router_base,
        &worker_base,
        Some((&format!("127.0.0.1:{port}"), 5000, 500)),
        false,
    );

    let (server, _cfg) = Server::from_config_file(&config).await.expect("assemble");

    let order = server.startup_order();
    let pos = |step: &str| order.iter().position(|s| *s == step);
    let rebuild = pos("startup_rebuild_done").expect("rebuild recorded");
    let subscriptions = pos("subscriptions_started").expect("subscriptions recorded");
    let watchdog = pos("watchdog_started").expect("watchdog recorded");
    let bootstrap = pos("router_bootstrapped").expect("bootstrap recorded");
    assert!(
        rebuild < subscriptions,
        "the subscription engine must not start before startup_rebuild returned: {order:?}"
    );
    assert!(
        rebuild < watchdog,
        "watchdog starts after the rebuild: {order:?}"
    );
    assert!(
        subscriptions < bootstrap && watchdog < bootstrap,
        "bootstrap is last: {order:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// built-in compatibility: no `superagent:` section boots identically
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn baseline_config_boots_identically_without_superagent() {
    force_no_proxy();
    let dir = temp_dir("baseline-config");
    let (provider_base, _mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("All done.".into())], 0).await;
    let config = common::write_config(
        &dir,
        &dir.join("kian.sqlite"),
        &provider_base,
        "127.0.0.1:7341",
    );

    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    // The built-in control plane still works end to end.
    let out = client
        .post(format!("{base}/rpc"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "create_session", "params": {}}))
        .send()
        .await
        .expect("rpc")
        .json::<serde_json::Value>()
        .await
        .expect("json");
    let sid = out["result"]["session"].as_str().unwrap().to_owned();
    client
        .post(format!("{base}/rpc"))
        .json(
            &serde_json::json!({"jsonrpc": "2.0", "id": 2, "method": "submit", "params": {
            "session": sid, "content": "Say all done."}}),
        )
        .send()
        .await
        .expect("submit");

    // /metrics keeps its built-in shape (superagent: null).
    let metrics: serde_json::Value = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics")
        .json()
        .await
        .expect("json");
    assert!(metrics["superagent"].is_null());

    // The demo injection is an honest error without the assembly.
    let out = ops_rpc(
        &client,
        &base,
        "mock_channel_send",
        serde_json::json!({"channel": "mock-im", "payload": {"text": "hi"}}),
    )
    .await;
    assert_eq!(out["error"]["code"], -32000, "honest refusal: {out}");
    assert!(
        out["error"]["message"]
            .as_str()
            .unwrap()
            .contains("requires the superagent assembly"),
        "{}",
        out
    );

    // And no superagent steps were recorded for this assembly.
    assert!(
        !_server.startup_order().contains(&"subscriptions_started"),
        "phase-1 assembly starts no subscription engine"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
