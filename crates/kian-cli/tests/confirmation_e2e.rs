//! E2E: the durable confirmation plane (CONFIRMATION-MODEL, ADR-033).
//!
//! Everything runs against the REAL assembled server + the mock
//! openai-compat provider, so the streams under test are produced by the
//! true engine + ReAct loop: an `ask` permission defers a tool call, the
//! drain ends `awaiting_input`, the answer is admitted over `/rpc`, and
//! the loop applies it in call order and resumes the same turn.
//!
//! Runs with `NO_PROXY=127.0.0.1,localhost` (AGENTS.md gotcha).

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use common::{KEY_ENV, ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use kian_cli::server::Server;
use kian_core::events::{EndReason, Event, EventKind};
use kian_store::EventStore;

/// A config whose default agent may call `exec`, with `exec: ask` and an
/// optional TTL.
fn write_ask_config(
    dir: &Path,
    sqlite: &Path,
    provider_base: &str,
    listen: &str,
    ttl_ms: Option<u64>,
) -> PathBuf {
    let confirmation = match ttl_ms {
        Some(ttl) => format!("confirmation:\n  ttl_ms: {ttl}\n"),
        None => String::new(),
    };
    let yaml = format!(
        r#"listen: "{listen}"
storage:
  sqlite: "{}"
agent:
  id: confirm-e2e
  model_slots:
    main:
      provider: mock
      model: mock-model
  tools: ["exec"]
  permissions:
    exec: ask
providers:
  mock:
    base_url: "{provider_base}"
    api_key_env: {KEY_ENV}
drain:
  max_iterations: 8
tools:
  root: "{}"
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
{confirmation}"#,
        sqlite.display(),
        dir.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write kian.yaml");
    path
}

async fn spawn_server(config_path: &Path) -> (String, Arc<Server>) {
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
    let body: serde_json::Value = resp.json().await.expect("rpc json");
    body
}

fn result(body: &serde_json::Value) -> &serde_json::Value {
    assert!(body.get("error").is_none(), "unexpected rpc error: {body}");
    body.get("result").expect("result")
}

/// Reads the session stream from the store (a second sqlite reader).
async fn events_of(sqlite: &Path, sid: &str) -> Vec<Event> {
    let uuid = sid.parse::<uuid::Uuid>().expect("sid uuid");
    let store = EventStore::open_read_only(sqlite).await.expect("reader");
    let events = store
        .rebuild(kian_core::model::SessionId(uuid))
        .await
        .expect("rebuild");
    store.close().await;
    events
}

fn last_end_reason(events: &[Event]) -> Option<EndReason> {
    events.iter().rev().find_map(|e| match e.kind {
        EventKind::SessionEnded { reason } => Some(reason),
        _ => None,
    })
}

fn ask_call_ids(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter(|e| {
            e.kind == EventKind::ToolCall
                && e.bytes
                    .pointer("/permission/evaluated")
                    .and_then(serde_json::Value::as_str)
                    == Some("ask")
        })
        .filter_map(|e| {
            e.bytes
                .pointer("/content/id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect()
}

fn results_for<'a>(events: &'a [Event], call_id: &str) -> Vec<&'a Event> {
    events
        .iter()
        .filter(|e| {
            e.kind == EventKind::ToolResult
                && e.bytes
                    .pointer("/content/call_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(call_id)
        })
        .collect()
}

/// Polls the durable stream until the drain settles `awaiting_input`.
async fn wait_awaiting(sqlite: &Path, sid: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let ev = events_of(sqlite, sid).await;
        if last_end_reason(&ev) == Some(EndReason::AwaitingInput) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the ask never reached awaiting_input"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn state(client: &reqwest::Client, base: &str, sid: &str) -> String {
    let out = rpc(
        client,
        base,
        "projection",
        serde_json::json!({"session": sid}),
    )
    .await;
    result(&out)["state"]
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

/// The acceptance path: ask → idle/awaiting_input → pending →
/// approve → the tool runs once → the same turn resumes → completed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ask_defers_then_approval_resumes_the_same_turn() {
    force_no_proxy();
    let dir = temp_dir("confirm-approve");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::ToolCall {
                id: "call-approve".into(),
                name: "exec".into(),
                arguments: serde_json::json!({"argv": ["echo", "confirmation-approved"]}),
            },
            ScriptedResponse::Text("All done.".into()),
        ],
        0,
    )
    .await;
    let config = write_ask_config(&dir, &sqlite, &provider_base, "127.0.0.1:7331", None);
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid: String = result(&out)["session"].as_str().expect("sid").into();
    rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({"session": sid, "content": "Send the mail.",
                           "idempotency_key": "confirm-1"}),
    )
    .await;

    // The drain ends resumably, never completed.
    wait_awaiting(&sqlite, &sid).await;
    let ev = events_of(&sqlite, &sid).await;
    assert_eq!(
        ask_call_ids(&ev),
        vec!["call-approve".to_string()],
        "exactly one ask call is logged"
    );
    assert!(
        results_for(&ev, "call-approve").is_empty(),
        "the ask arm must not execute or answer the call"
    );
    assert_eq!(state(&client, &base, &sid).await, "idle");

    // The pending read reports the ask with the frozen arguments.
    let out = rpc(
        &client,
        &base,
        "pending",
        serde_json::json!({"session": sid}),
    )
    .await;
    let pending = result(&out)["pending"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0]["call_id"], "call-approve");
    assert_eq!(pending[0]["tool"], "exec");
    assert_eq!(pending[0]["expired"], false);
    assert_eq!(
        pending[0]["arguments"],
        serde_json::json!({"argv": ["echo", "confirmation-approved"]})
    );

    // Approve: the durable row wakes the drain, which applies it.
    let out = rpc(
        &client,
        &base,
        "confirm",
        serde_json::json!({"session": sid, "call_id": "call-approve", "decision": "approve",
                           "requested_by": "e2e"}),
    )
    .await;
    assert_eq!(result(&out)["outcome"], "accepted");

    // Wait for completion, then assert the applied decision.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while state(&client, &base, &sid).await != "completed" {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the answered session did not complete"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ev = events_of(&sqlite, &sid).await;
    let results = results_for(&ev, "call-approve");
    assert_eq!(results.len(), 1, "exactly one result for the answered call");
    let truth = results[0];
    assert_eq!(
        truth
            .bytes
            .pointer("/confirmation/decision")
            .and_then(serde_json::Value::as_str),
        Some("approve"),
        "the result carries the applied decision: {}",
        truth.bytes
    );
    assert_eq!(
        truth
            .bytes
            .pointer("/content/is_error")
            .and_then(serde_json::Value::as_bool),
        Some(false),
        "the approved tool actually ran: {}",
        truth.bytes
    );
    assert_eq!(last_end_reason(&ev), Some(EndReason::Completed));
    // The pending read is empty afterwards.
    let out = rpc(
        &client,
        &base,
        "pending",
        serde_json::json!({"session": sid}),
    )
    .await;
    assert_eq!(result(&out)["pending"], serde_json::json!([]));
}

/// Reject: the honest-error result, no invocation, the turn resumes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejection_is_an_honest_error_result_and_resumes() {
    force_no_proxy();
    let dir = temp_dir("confirm-reject");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::ToolCall {
                id: "call-reject".into(),
                name: "exec".into(),
                arguments: serde_json::json!({"argv": ["echo", "must-not-run"]}),
            },
            ScriptedResponse::Text("Understood.".into()),
        ],
        0,
    )
    .await;
    let config = write_ask_config(&dir, &sqlite, &provider_base, "127.0.0.1:7331", None);
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid: String = result(&out)["session"].as_str().expect("sid").into();
    rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({"session": sid, "content": "Send the mail.", "idempotency_key": "r-1"}),
    )
    .await;

    wait_awaiting(&sqlite, &sid).await;

    let out = rpc(
        &client,
        &base,
        "confirm",
        serde_json::json!({"session": sid, "call_id": "call-reject", "decision": "reject",
                           "reason": "not now"}),
    )
    .await;
    assert_eq!(result(&out)["outcome"], "accepted");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while state(&client, &base, &sid).await != "completed" {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the rejected session did not complete"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ev = events_of(&sqlite, &sid).await;
    let results = results_for(&ev, "call-reject");
    assert_eq!(results.len(), 1);
    let result_payload = &results[0].bytes;
    assert_eq!(
        result_payload
            .pointer("/content/is_error")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        result_payload
            .pointer("/content/output/policy")
            .and_then(serde_json::Value::as_str),
        Some("ask")
    );
    assert_eq!(
        result_payload
            .pointer("/content/output/reason")
            .and_then(serde_json::Value::as_str),
        Some("not now")
    );
    assert_eq!(
        result_payload
            .pointer("/confirmation/decision")
            .and_then(serde_json::Value::as_str),
        Some("reject")
    );
}

/// Duplicate answers: the second is idempotent (`accepted`, deduplicated
/// row) or `not_pending`; either way exactly one result ever exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_answers_never_produce_a_second_result() {
    force_no_proxy();
    let dir = temp_dir("confirm-dup");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::ToolCall {
                id: "call-dup".into(),
                name: "exec".into(),
                arguments: serde_json::json!({"argv": ["echo", "dup"]}),
            },
            ScriptedResponse::Text("All done.".into()),
        ],
        0,
    )
    .await;
    let config = write_ask_config(&dir, &sqlite, &provider_base, "127.0.0.1:7331", None);
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid: String = result(&out)["session"].as_str().expect("sid").into();
    rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({"session": sid, "content": "Send the mail.", "idempotency_key": "d-1"}),
    )
    .await;

    wait_awaiting(&sqlite, &sid).await;

    for _ in 0..2 {
        let out = rpc(
            &client,
            &base,
            "confirm",
            serde_json::json!({"session": sid, "call_id": "call-dup", "decision": "approve"}),
        )
        .await;
        let outcome = result(&out)["outcome"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        assert!(
            outcome == "accepted" || outcome == "not_pending",
            "unexpected duplicate outcome: {outcome}"
        );
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while state(&client, &base, &sid).await != "completed" {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the duplicate-answered session did not complete"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ev = events_of(&sqlite, &sid).await;
    assert_eq!(
        results_for(&ev, "call-dup").len(),
        1,
        "one answer, one result"
    );
}

/// TTL: a late answer is `expired`, becomes a durable reject-expired, and
/// the session resumes (never wedged, never silently dropped).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_answer_degrades_to_expired_and_resumes() {
    force_no_proxy();
    let dir = temp_dir("confirm-ttl");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::ToolCall {
                id: "call-ttl".into(),
                name: "exec".into(),
                arguments: serde_json::json!({"argv": ["echo", "ttl"]}),
            },
            ScriptedResponse::Text("Understood.".into()),
        ],
        0,
    )
    .await;
    // 1 ms TTL: by the time the ask is observable, it has lapsed.
    let config = write_ask_config(&dir, &sqlite, &provider_base, "127.0.0.1:7331", Some(1));
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid: String = result(&out)["session"].as_str().expect("sid").into();
    rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({"session": sid, "content": "Send the mail.", "idempotency_key": "t-1"}),
    )
    .await;

    wait_awaiting(&sqlite, &sid).await;

    // The pending read already reports the lapse.
    let out = rpc(
        &client,
        &base,
        "pending",
        serde_json::json!({"session": sid}),
    )
    .await;
    assert_eq!(result(&out)["pending"][0]["expired"], true);

    let out = rpc(
        &client,
        &base,
        "confirm",
        serde_json::json!({"session": sid, "call_id": "call-ttl", "decision": "approve"}),
    )
    .await;
    assert_eq!(result(&out)["outcome"], "expired");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while state(&client, &base, &sid).await != "completed" {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the expired-answer session did not resume"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ev = events_of(&sqlite, &sid).await;
    let results = results_for(&ev, "call-ttl");
    assert_eq!(results.len(), 1);
    let payload = &results[0].bytes;
    assert_eq!(
        payload
            .pointer("/content/is_error")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        payload
            .pointer("/confirmation/expired")
            .and_then(serde_json::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        payload
            .pointer("/confirmation/decision")
            .and_then(serde_json::Value::as_str),
        Some("reject")
    );
}

/// A message that arrives while a decision is pending must NOT be promoted
/// into the stream: doing so would interleave a user message between the
/// unanswered `tool/call` and its `tool/result`, and the resumed provider
/// request would carry a tool block broken by a user message (strict
/// OpenAI-compatible endpoints reject it). After the answer, the queued
/// message is promoted at the next safe boundary, after the tool result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_during_the_wait_is_promoted_after_the_answer_in_call_order() {
    force_no_proxy();
    let dir = temp_dir("confirm-interleave");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::ToolCall {
                id: "call-wait".into(),
                name: "exec".into(),
                arguments: serde_json::json!({"argv": ["echo", "confirmation-waited"]}),
            },
            ScriptedResponse::Text("Approved and done.".into()),
            ScriptedResponse::Text("Yes, still here.".into()),
        ],
        0,
    )
    .await;
    let config = write_ask_config(&dir, &sqlite, &provider_base, "127.0.0.1:7332", None);
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid: String = result(&out)["session"].as_str().expect("sid").into();
    rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({"session": sid, "content": "Send the mail.",
                           "idempotency_key": "interleave-1"}),
    )
    .await;
    wait_awaiting(&sqlite, &sid).await;

    // A plain message arrives while the ask waits. It is admitted
    // durably but must stay unpromoted (no `input/admitted`).
    rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({"session": sid, "content": "Are you there?",
                           "idempotency_key": "interleave-2"}),
    )
    .await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let ev = events_of(&sqlite, &sid).await;
        let awaits = ev
            .iter()
            .filter(|e| {
                e.kind
                    == EventKind::SessionEnded {
                        reason: EndReason::AwaitingInput,
                    }
            })
            .count();
        if awaits >= 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the blocked message's no-op episode never settled: {awaits} awaits"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let ev = events_of(&sqlite, &sid).await;
    assert!(
        !ev.iter().any(|e| {
            e.kind == EventKind::InputAdmitted
                && e.bytes.get("content").and_then(serde_json::Value::as_str)
                    == Some("Are you there?")
        }),
        "the waiting message must not be promoted into the call's open window"
    );
    assert_eq!(state(&client, &base, &sid).await, "idle");

    // Approve; the queued message promotes after the tool result.
    let out = rpc(
        &client,
        &base,
        "confirm",
        serde_json::json!({"session": sid, "call_id": "call-wait", "decision": "approve",
                           "requested_by": "e2e"}),
    )
    .await;
    assert_eq!(result(&out)["outcome"], "accepted");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while state(&client, &base, &sid).await != "completed" {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the answered session did not complete"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let ev = events_of(&sqlite, &sid).await;
    let result_seq = ev
        .iter()
        .find(|e| {
            e.kind == EventKind::ToolResult
                && e.bytes
                    .pointer("/content/call_id")
                    .and_then(serde_json::Value::as_str)
                    == Some("call-wait")
        })
        .map(|e| e.seq)
        .expect("the answered call has a result");
    let queued_seq = ev
        .iter()
        .find(|e| {
            e.kind == EventKind::InputAdmitted
                && e.bytes.get("content").and_then(serde_json::Value::as_str)
                    == Some("Are you there?")
        })
        .map(|e| e.seq)
        .expect("the queued message is eventually promoted");
    assert!(
        result_seq < queued_seq,
        "the tool result ({result_seq}) must precede the queued message ({queued_seq})"
    );

    // The resumed request's message order is provider-legal: the
    // assistant turn carrying `tool_calls` is answered immediately.
    let resumed = ev
        .iter()
        .filter(|e| e.kind == EventKind::StepRequest)
        .filter_map(|e| {
            e.bytes
                .pointer("/body/messages")
                .and_then(serde_json::Value::as_array)
        })
        .find(|messages| {
            messages.iter().any(|m| {
                m.get("tool_calls")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|calls| {
                        calls.iter().any(|c| {
                            c.get("id").and_then(serde_json::Value::as_str) == Some("call-wait")
                        })
                    })
            })
        })
        .expect("a request carried the tool call");
    let at = resumed
        .iter()
        .position(|m| {
            m.get("tool_calls")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|calls| {
                    calls.iter().any(|c| {
                        c.get("id").and_then(serde_json::Value::as_str) == Some("call-wait")
                    })
                })
        })
        .expect("the assistant tool-call turn");
    assert_eq!(
        resumed[at + 1]
            .get("role")
            .and_then(serde_json::Value::as_str),
        Some("tool"),
        "the tool message must immediately follow its assistant turn: {resumed:?}"
    );
    assert_eq!(
        resumed[at + 1]
            .get("tool_call_id")
            .and_then(serde_json::Value::as_str),
        Some("call-wait"),
        "and answer that call id: {resumed:?}"
    );
}
