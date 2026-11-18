//! (R11-05 / kian-125) Independent QA acceptance probes — L1 routing
//! and config surface of the durable execution-worker binding
//! (ADR-020), exercised through the **`kian-cli` assembly surface**
//! (`select_side`, the refusing `exec` side) and the **real config
//! parser** (`ServeConfig::parse`) with QA-authored YAML.
//!
//! Not a re-run of the implementers' in-crate assertions: every
//! fixture (ids, catalogs, YAML documents) is authored here.

use std::borrow::Cow;

use kian_cli::config::ServeConfig;
use kian_cli::superagent::{Refusal, RefusingExecTool, SelectionKind, select_side};
use kian_core::seams::{ToolCall, ToolError, ToolProvider as _};
use kian_superagent::WorkerBinding;

// ---------------------------------------------------------------------------
// The routing selection table — the pure function (spec §4)
// ---------------------------------------------------------------------------

fn ids(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

#[test]
fn qa_selection_table_is_the_spec_pure_function() {
    // Bound to a configured worker ⇒ THAT worker's side — even when it
    // is NOT the first endpoint (a default-to-first bug goes red).
    assert_eq!(
        select_side(
            &WorkerBinding::Bound(Cow::Borrowed("qa-second")),
            &ids(&["qa-first", "qa-second"])
        ),
        SelectionKind::Bound("qa-second")
    );
    assert_eq!(
        select_side(
            &WorkerBinding::Bound(Cow::Borrowed("qa-first")),
            &ids(&["qa-first", "qa-second"])
        ),
        SelectionKind::Bound("qa-first")
    );

    // Bound to an id that is no longer configured ⇒ refusal — never a
    // re-route to another worker, never local exec (D16).
    assert_eq!(
        select_side(
            &WorkerBinding::Bound(Cow::Borrowed("qa-gone")),
            &ids(&["qa-first", "qa-second"])
        ),
        SelectionKind::BoundUnconfigured("qa-gone")
    );

    // Legacy unbound with workers configured ⇒ the FIRST endpoint
    // (never a bound worker): "no binding must not default to a bound
    // worker".
    assert_eq!(
        select_side(&WorkerBinding::Unbound, &ids(&["qa-first", "qa-second"])),
        SelectionKind::LegacyFirst("qa-first")
    );

    // Legacy unbound with zero workers ⇒ today's local-exec path.
    assert_eq!(
        select_side(&WorkerBinding::Unbound, &[]),
        SelectionKind::NoWorkers
    );

    // Malformed ⇒ refusal, on any configuration.
    assert_eq!(
        select_side(&WorkerBinding::Malformed, &ids(&["qa-first"])),
        SelectionKind::Malformed
    );
    assert_eq!(
        select_side(&WorkerBinding::Malformed, &[]),
        SelectionKind::Malformed
    );
}

// ---------------------------------------------------------------------------
// The refusing side (D16/D2) — honest, backend-free
// ---------------------------------------------------------------------------

fn exec_call() -> ToolCall {
    ToolCall {
        id: "qa-exec-1".into(),
        name: "exec".into(),
        arguments: serde_json::json!({"argv": ["/bin/true"]}),
    }
}

#[tokio::test]
async fn qa_refusing_exec_answers_honestly_without_a_backend() {
    // The unconfigured-bound refusal names the id and states that
    // nothing was queued and nothing ran elsewhere.
    let tool = RefusingExecTool::new(Refusal::Unconfigured("qa-gone".to_string()));
    let err = tool.invoke(exec_call()).await.unwrap_err();
    match &err {
        ToolError::Execution(message) => {
            assert!(message.contains("qa-gone"), "{message}");
            assert!(message.contains("exec refused"), "{message}");
            assert!(message.contains("no command was queued"), "{message}");
            assert!(message.contains("nothing ran elsewhere"), "{message}");
        }
        other => panic!("expected an Execution refusal, got {other:?}"),
    }

    // The malformed refusal names the malformed binding.
    let tool = RefusingExecTool::new(Refusal::Malformed);
    let err = tool.invoke(exec_call()).await.unwrap_err();
    assert!(
        err.to_string().contains("malformed `exec_worker` binding"),
        "{}",
        err
    );

    // The tool surface is unchanged: one tool named `exec` with the
    // uniform argv schema (prompt-prefix neutral, D8).
    let schema = RefusingExecTool::new(Refusal::Malformed)
        .schemas()
        .into_iter()
        .next()
        .expect("one schema");
    assert_eq!(schema.name, "exec");
    assert_eq!(
        schema.parameters["properties"]["argv"]["type"],
        serde_json::json!("array")
    );
    assert_eq!(schema.parameters["required"], serde_json::json!(["argv"]));
}

// ---------------------------------------------------------------------------
// The real config parser: description / catalog / duplicates / dsh (D5, D12)
// ---------------------------------------------------------------------------

/// A parseable QA serve config with `endpoints_count` endpoints whose
/// descriptions come from `descriptions` (None ⇒ the field is absent).
fn qa_yaml(endpoints_count: usize, descriptions: &[Option<&str>]) -> String {
    let mut endpoints = String::new();
    for index in 0..endpoints_count {
        let port = 7400 + index;
        endpoints.push_str(&format!(
            "  - id: qa-ep{index}\n    addr: \"127.0.0.1:{port}\"\n"
        ));
        if let Some(Some(description)) = descriptions.get(index) {
            endpoints.push_str(&format!("    description: \"{description}\"\n"));
        }
        endpoints.push_str("    queue_timeout_ms: 60000\n    offline_after_ms: 30000\n");
    }
    if endpoints_count == 0 {
        endpoints.push_str("  []\n");
    }
    let remote = if endpoints_count == 0 {
        ""
    } else {
        ", \"remote-exec\""
    };
    format!(
        r#"listen: "127.0.0.1:17420"
storage:
  sqlite: "/tmp/qa-worker-binding-unused.sqlite"
agent:
  id: default
  model_slots:
    main:
      provider: qa-mock
      model: mock-model
agents:
  router:
    id: router
    version: 3
    model_slots:
      router:
        provider: qa-mock
        model: mock-router
  worker:
    id: worker
    version: 7
    model_slots:
      main:
        provider: qa-mock
        model: mock-worker
superagent:
  router:
    session_id: "0a0a0a0a-0a0a-0a0a-0a0a-0a0a0a0a0a0a"
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
endpoints:
{endpoints}providers:
  qa-mock:
    base_url: "http://127.0.0.1:9"
    api_key_env: KIAN_E2E_API_KEY
plugins: ["openai-compat", "local-tools", "superagent"{remote}]
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
"#
    )
}

#[test]
fn qa_config_description_accepted_reported_and_in_catalog_order() {
    let yaml = qa_yaml(
        2,
        &[
            Some("the first QA machine — cargo and git"),
            Some("the second QA machine — long builds"),
        ],
    );
    let cfg = ServeConfig::parse(&yaml).expect("descriptions parse");
    let catalog = cfg.worker_catalog();
    assert_eq!(catalog.len(), 2);
    assert_eq!(catalog[0].id, "qa-ep0");
    assert_eq!(
        catalog[0].description.as_deref(),
        Some("the first QA machine — cargo and git")
    );
    assert_eq!(catalog[1].id, "qa-ep1");
    assert_eq!(
        catalog[1].description.as_deref(),
        Some("the second QA machine — long builds")
    );

    // Absent descriptions keep the catalog entry with no description.
    let yaml = qa_yaml(1, &[None]);
    let cfg = ServeConfig::parse(&yaml).expect("absent description parses");
    assert_eq!(cfg.worker_catalog().len(), 1);
    assert_eq!(cfg.worker_catalog()[0].description, None);

    // Queue timeouts flow per worker id.
    let timeouts = cfg.worker_queue_timeouts();
    assert_eq!(
        timeouts.get("qa-ep0").copied(),
        Some(60_000),
        "{timeouts:?}"
    );
}

#[test]
fn qa_config_description_validates_fail_closed() {
    for bad in [
        "",    // empty
        "   ", // whitespace only
    ] {
        let yaml = qa_yaml(1, &[Some(bad)]);
        let err = ServeConfig::parse(&yaml).unwrap_err();
        assert!(
            err.to_string().contains("description must be non-empty"),
            "{bad:?}: {err}"
        );
    }
    let long = "x".repeat(201);
    let yaml = qa_yaml(1, &[Some(&long)]);
    let err = ServeConfig::parse(&yaml).unwrap_err();
    assert!(err.to_string().contains("at most 200 characters"), "{err}");
    // The YAML escape `\n` becomes a real control character in the
    // parsed value — the fail-closed row.
    let yaml = qa_yaml(1, &[Some("line one\\nline two")]);
    let err = ServeConfig::parse(&yaml).unwrap_err();
    assert!(err.to_string().contains("control characters"), "{err}");

    // Exactly 200 chars parse (the bound is inclusive).
    let exact = "y".repeat(200);
    let yaml = qa_yaml(1, &[Some(&exact)]);
    assert!(
        ServeConfig::parse(&yaml).is_ok(),
        "exactly 200 characters must parse"
    );
}

#[test]
fn qa_config_duplicate_endpoint_ids_still_refuse() {
    let yaml = qa_yaml(2, &[None, None]).replace("qa-ep1", "qa-ep0");
    let err = ServeConfig::parse(&yaml).unwrap_err();
    let text = err.to_string();
    assert!(text.contains("duplicate endpoint id"), "{text}");
    assert!(text.contains("qa-ep0"), "{text}");
}

#[test]
fn qa_config_dsh_and_endpoints_are_mutually_exclusive() {
    // dsh + endpoints: fail-closed, naming both sections.
    let combined = qa_yaml(1, &[None]).replace(
        "superagent:\n",
        "superagent:\n  harness:\n    dsh:\n      command: /usr/bin/true\n",
    );
    let err = ServeConfig::parse(&combined).unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("`superagent.harness.dsh` and a worker surface"),
        "{text}"
    );
    assert!(
        text.contains("non-empty `endpoints:` list or a `workers:` section"),
        "{text}"
    );
    assert!(text.contains("mutually exclusive"), "{text}");

    // dsh with ZERO endpoints boots exactly as before (no remote-exec
    // plugin in the zero-endpoint shape).
    let dsh_only = qa_yaml(0, &[]).replace(
        "superagent:\n",
        "superagent:\n  harness:\n    dsh:\n      command: /usr/bin/true\n",
    );
    let cfg = ServeConfig::parse(&dsh_only).expect("zero-endpoints dsh boots as before");
    assert!(cfg.worker_catalog().is_empty());
}

#[test]
fn qa_config_zero_endpoints_means_feature_off() {
    let cfg = ServeConfig::parse(&qa_yaml(0, &[])).expect("zero endpoints parse");
    assert!(cfg.worker_catalog().is_empty());
    assert!(cfg.worker_queue_timeouts().is_empty());
}

// ---------------------------------------------------------------------------
// The async-tools carry decision (spec D11) — QA's own store-level probe
// ---------------------------------------------------------------------------

mod common;

use common::temp_dir;
use kian_core::model::SessionId;
use kian_exec::{
    AsyncJobs, AsyncToolPair, ConnectionConfig, ConnectionManager, ExecStore, RemoteExecBackend,
    RemoteExecConfig, tool::command_id_for_tool_call,
};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// A no-op completion sink: the row admission is under test, not the
/// observer delivery.
struct QaNoopSink;

impl kian_exec::AsyncResultSink for QaNoopSink {
    fn deliver(
        &self,
        _job: kian_exec::AsyncJob,
    ) -> futures::future::BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}

/// One backend over a fresh sqlite exec store with `ids` routes — the
/// managers are constructed but never started, so nothing dials.
async fn qa_backend(ids: &[&str]) -> (ExecStore, Arc<RemoteExecBackend>) {
    let dir = temp_dir("qa-worker-binding-async");
    let store = ExecStore::open(dir.join("exec.sqlite"))
        .await
        .expect("exec store opens");
    let routes = ids
        .iter()
        .map(|id| {
            let addr: std::net::SocketAddr = "127.0.0.1:1".parse().expect("addr");
            (
                (*id).to_string(),
                Arc::new(ConnectionManager::new(ConnectionConfig::new(addr))),
            )
        })
        .collect();
    let backend = RemoteExecBackend::with_row_leases_redacting(
        store.clone(),
        routes,
        ids.first().copied().expect("at least one id"),
        RemoteExecConfig {
            queue_timeout: Duration::from_millis(200),
            poll_interval: Duration::from_millis(50),
            max_redrives: 1,
        },
        "qa-holder",
        Duration::from_millis(200),
        kian_core::secrets::SecretRedactor::default(),
    )
    .expect("backend builds");
    (store, Arc::new(backend))
}

fn qa_session_for_call() -> kian_exec::SessionForCall {
    Arc::new(|_call_id: &str| Box::pin(async { Some(SessionId(Uuid::new_v4())) }))
}

fn qa_start_call(id: &str) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: "start".into(),
        arguments: serde_json::json!({"argv": ["/bin/true"]}),
    }
}

/// A `start` through the SECOND worker side's async pair writes an
/// `async_jobs` row carrying THAT endpoint id (the carry decision) —
/// and a single-endpoint backend keeps today's default exactly.
#[tokio::test]
async fn qa_async_pairs_carry_their_own_endpoint_id() {
    let (store, backend) = qa_backend(&["qa-b1", "qa-b2"]).await;
    let jobs_first = AsyncJobs::new(
        Arc::clone(&backend),
        "qa-b1",
        Arc::new(QaNoopSink) as Arc<dyn kian_exec::AsyncResultSink>,
        qa_session_for_call(),
    );
    let jobs_second = AsyncJobs::new(
        Arc::clone(&backend),
        "qa-b2",
        Arc::new(QaNoopSink) as Arc<dyn kian_exec::AsyncResultSink>,
        qa_session_for_call(),
    );
    let pair_second = AsyncToolPair::new(jobs_second);
    let pair_first = AsyncToolPair::new(jobs_first);

    let result = pair_second
        .invoke(qa_start_call("qa-async-on-second"))
        .await
        .expect("start on the second side");
    let job_id = Uuid::parse_str(result.output["job_id"].as_str().expect("job_id")).expect("uuid");
    assert_eq!(
        job_id,
        command_id_for_tool_call("qa-async-on-second"),
        "the job id derives from the tool-call id (correlation)"
    );
    let row = store
        .get_async_job(job_id)
        .await
        .expect("row read")
        .expect("row exists");
    assert_eq!(
        row.endpoint_id, "qa-b2",
        "the second side's pair writes its own id"
    );

    // The first side's pair still writes the first endpoint (unchanged
    // behavior per side — no cross-contamination from the carry).
    pair_first
        .invoke(qa_start_call("qa-async-on-first"))
        .await
        .expect("start on the first side");
    let row = store
        .get_async_job(command_id_for_tool_call("qa-async-on-first"))
        .await
        .expect("row read")
        .expect("row exists");
    assert_eq!(row.endpoint_id, "qa-b1");

    // Single endpoint: the default pair keeps the only endpoint —
    // today's behavior exactly.
    let (store, backend) = qa_backend(&["qa-only"]).await;
    let jobs = AsyncJobs::new(
        Arc::clone(&backend),
        "qa-only",
        Arc::new(QaNoopSink) as Arc<dyn kian_exec::AsyncResultSink>,
        qa_session_for_call(),
    );
    AsyncToolPair::new(jobs)
        .invoke(qa_start_call("qa-async-only"))
        .await
        .expect("start on the only endpoint");
    let row = store
        .get_async_job(command_id_for_tool_call("qa-async-only"))
        .await
        .expect("row read")
        .expect("row exists");
    assert_eq!(row.endpoint_id, "qa-only");
}
