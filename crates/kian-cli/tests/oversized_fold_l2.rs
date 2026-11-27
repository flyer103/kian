//! L2 for ADR-071: an oversized request folds the old turns under the
//! recovery policy's `compress_then_retry`, against a real assembled
//! server.
//!
//! The shape is the production one: a resident Router session whose
//! history crosses the provider's declared byte bound. The seam refuses
//! the request locally (`request_too_large`), the recovery table resolves
//! `compress_then_retry`, the loop folds the old turns into a summary and
//! retries — so the input that would otherwise have failed is answered by
//! a smaller request.
//!
//! Batches arrive the way the Router receives them in production: one
//! message on a mock channel, admitted by the subscription engine into the
//! resident session. Observations read the committed stream through a
//! read-only store handle, never the server's internals.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use kian_cli::server::Server;
use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_store::EventStore;

const ROUTER_SESSION: &str = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff";

/// `providers.router-mock.max_request_bytes`: five judged batches fit,
/// the sixth does not.
const BOUND: usize = 5_767_168;
/// One judged batch's content. Some five of them (plus the small
/// bootstrap turn) stay under the bound; the next one crosses it.
const TURN_BYTES: usize = 1024 * 1024;

fn router_session() -> SessionId {
    SessionId(uuid::Uuid::parse_str(ROUTER_SESSION).expect("router uuid"))
}

/// The recovery table from docs/advanced.md's oversized-request section,
/// verbatim: an explicit table replaces the defaults, so every class this
/// deployment governs is listed.
fn write_fold_config(dir: &std::path::Path, provider: &str) -> std::path::PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let yaml = format!(
        r#"listen: "127.0.0.1:7399"
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
        provider: router-mock
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
    base_url: "{provider}"
    api_key_env: KIAN_E2E_API_KEY
    max_request_bytes: {BOUND}
plugins: ["openai-compat", "local-tools", "superagent"]
drain:
  max_iterations: 16
tools:
  root: "{dir}"
  exec_timeout_ms: 5000
recovery:
  budget:
    tokens: 64
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: rate_limited
      action: {{ backoff_retry: {{ delay_ms: 500 }} }}
    - class: empty_response
      action: {{ backoff_retry: {{ delay_ms: 250 }} }}
    - class: context_overflow
      action: compress_then_retry
    - class: request_too_large
      action: compress_then_retry
    - class: provider_down
      action: failover
    - class: tool_failed
      action: interrupt_ask
    - class: unknown
      action: fail
"#,
        sqlite = sqlite.display(),
        dir = dir.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write fold config");
    path
}

async fn spawn_server(config: &std::path::Path) -> (String, Arc<Server>) {
    let server = Arc::new(
        Server::from_config_file(config)
            .await
            .expect("assemble the fold server")
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
        .expect("ops rpc send");
    assert_eq!(resp.status(), 200);
    resp.json().await.expect("ops rpc json")
}

/// The session is between episodes: every started turn has ended, the
/// head is quiet, and the last turn did not hand the episode to a fold —
/// so a new batch cannot interleave a running turn (the loop takes every
/// eligible input into one turn). The state projection cannot answer
/// this: the resident hold keeps the session non-idle while it serves
/// inputs, so the stream is the only evidence.
async fn wait_turn_settled(store: &EventStore, prev_turns: usize) -> Vec<Event> {
    for _ in 0..2400 {
        let events = store
            .rebuild(router_session())
            .await
            .expect("rebuild router");
        let starts = count_kind(&events, EventKind::TurnStart);
        let ends = count_kind(&events, EventKind::TurnEnd);
        let tail_ended = events
            .last()
            .map(|event| event.kind == EventKind::TurnEnd)
            .unwrap_or(false);
        let last_end = events
            .iter()
            .rev()
            .find(|event| event.kind == EventKind::TurnEnd);
        let fold_pending = last_end
            .map(|event| {
                event.bytes["outcome"] == serde_json::json!("soft_failure")
                    && event.bytes["error"]["action"] == serde_json::json!("compress_then_retry")
            })
            .unwrap_or(false);
        if starts > prev_turns && starts == ends && tail_ended && !fold_pending {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let quiet = store
                .rebuild(router_session())
                .await
                .expect("rebuild router");
            if quiet.len() == events.len() {
                return quiet;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the router never settled after turn {prev_turns}");
}

fn count_kind(events: &[Event], kind: EventKind) -> usize {
    events.iter().filter(|e| e.kind == kind).count()
}

fn step_body_len(event: &Event) -> usize {
    serde_json::to_string(&event.bytes["body"])
        .expect("serialize request body")
        .len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_oversized_request_folds_old_turns_under_the_policy_and_answers_the_input() {
    force_no_proxy();
    let dir = temp_dir("adr070-fold");
    // The Router's script: the bootstrap ack, then one answer per judged
    // batch (the last entry repeats).
    let (provider_base, mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::Text("bootstrap acknowledged".into()),
            ScriptedResponse::Text("batch noted".into()),
        ],
        0,
    )
    .await;
    let config = write_fold_config(&dir, &provider_base);
    let (base, _server) = spawn_server(&config).await;
    let read_only = EventStore::open_read_only(&dir.join("kian.sqlite"))
        .await
        .expect("read-only store");
    let client = reqwest::Client::new();

    // The resident Router's bootstrap turn.
    let mut events = wait_turn_settled(&read_only, 0).await;

    // Grow the history one judged batch at a time until the bound refuses
    // a request. Each message waits for the park, so no two messages share
    // a turn and every turn keeps its own context slice.
    let content = "x".repeat(TURN_BYTES);
    for batch in 0..8 {
        let turns = count_kind(&events, EventKind::TurnStart);
        let out = ops_rpc(
            &client,
            &base,
            "mock_channel_send",
            serde_json::json!({
                "channel": "mock-im",
                "payload": {"text": content},
            }),
        )
        .await;
        assert_eq!(
            out["result"]["channel"],
            serde_json::json!("mock-im"),
            "batch {batch}: {out}"
        );
        events = wait_turn_settled(&read_only, turns).await;
        if count_kind(&events, EventKind::Compact) > 0 {
            break;
        }
    }

    // One fold, under the policy's opt-in, naming the byte verdict.
    let compacts: Vec<&Event> = events
        .iter()
        .filter(|e| e.kind == EventKind::Compact)
        .collect();
    assert_eq!(
        compacts.len(),
        1,
        "exactly one fold; the per-episode cap allows two, the episode needs one"
    );
    let compact = compacts[0];
    assert_eq!(
        compact.bytes["reason"],
        serde_json::json!("provider refused the request as too large (ADR-068)"),
        "the fold names the byte trigger, not a window overflow"
    );
    assert!(
        compact.bytes["folded_messages"].as_u64().unwrap_or(0) >= 2,
        "the fold dropped old turns: {}",
        compact.bytes
    );
    assert!(
        compact.bytes["reclaimed_tokens"].as_u64().unwrap_or(0) >= 1024,
        "the fold reclaims more than the planner's floor: {}",
        compact.bytes
    );

    // The refusal was audited as the class AND the action.
    let refused_end = events
        .iter()
        .rev()
        .find(|e| e.kind == EventKind::TurnEnd && e.seq < compact.seq)
        .expect("a turn ended before the fold");
    assert_eq!(
        refused_end.bytes["outcome"],
        serde_json::json!("soft_failure")
    );
    assert_eq!(
        refused_end.bytes["error"]["class"],
        serde_json::json!("request_too_large")
    );
    assert_eq!(
        refused_end.bytes["error"]["action"],
        serde_json::json!("compress_then_retry")
    );

    // The batch that was refused is answered afterwards, by a smaller
    // request: the retried turn ends with a final answer.
    let answered = events.iter().any(|e| {
        e.kind == EventKind::TurnEnd
            && e.seq > compact.seq
            && e.bytes["outcome"] == serde_json::json!("final_answer")
    });
    assert!(
        answered,
        "the retried request completed the turn the refusal ended: {:?}",
        events
            .iter()
            .filter(|e| e.kind == EventKind::TurnEnd)
            .map(|e| (&e.seq, &e.bytes["outcome"]))
            .collect::<Vec<_>>()
    );

    // The refused bytes are persisted (ADR-068 decision 5) and are over
    // the bound; the retry is under it and smaller.
    let requests: Vec<&Event> = events
        .iter()
        .filter(|e| e.kind == EventKind::StepRequest)
        .collect();
    let sizes: Vec<usize> = requests.iter().map(|e| step_body_len(e)).collect();
    let largest = *sizes.iter().max().expect("a request was persisted");
    assert!(
        largest > BOUND,
        "the refused request is persisted with its exact bytes ({largest} <= {BOUND})"
    );
    let retry = *sizes.last().expect("a retry was persisted");
    assert!(
        retry < BOUND,
        "the retried request fits under the bound ({retry} vs {BOUND})"
    );
    assert!(
        retry < largest,
        "the fold shrank the request ({retry} vs the refused {largest})"
    );

    // Nothing over the bound ever reached the provider: every persisted
    // under-bound request arrived, and the refused one did not.
    let arrived = sizes.iter().filter(|size| **size <= BOUND).count();
    assert_eq!(
        mock.requests.load(std::sync::atomic::Ordering::SeqCst),
        arrived,
        "the refused body never left the process; sizes: {sizes:?}"
    );

    // A resident session outlives its turns: the fold never ended it.
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
            .count(),
        0,
        "the fold is a maintenance turn, not a session end"
    );
}
