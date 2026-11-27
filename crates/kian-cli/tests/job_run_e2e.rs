//! L3: one declarative job runs to completion through "kian serve".
//!
//! The plan is deliver-only on purpose. A delivery step is a conductor
//! action (JOB-MODEL §13.1), so the run needs no worker and no provider
//! turn: the end-to-end property under test is that the real channel
//! adapter returns a real receipt and the run commits one step verdict,
//! one run verdict and one completed end. The row is admitted by the real
//! "kian job run" command before any server exists, which is also the
//! durability claim: a run admitted while nothing is running is drained at
//! startup from its durable row alone (§5.2, §15.3).

mod common;

use std::time::Duration;

use common::{ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir};
use kian_cli::job_cli::JobCommand;
use kian_cli::server::Server;
use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::SessionId;
use kian_store::EventStore;
use kian_superagent::job::plan;
use serde_json::Value;

const JOB: &str = "notify-only";
const SLOT: &str = "manual:01J0JOBRUN0000000000000000";

fn write_config(dir: &std::path::Path, router: &str, worker: &str) -> std::path::PathBuf {
    let sqlite = dir.join("kian.sqlite");
    let yaml = format!(
        r#"listen: "127.0.0.1:0"
storage:
  sqlite: "{sqlite}"
agent:
  id: default
  model_slots:
    main:
      provider: router-mock
      model: mock-model
  tools: []
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
    session_id: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    agent: router
    slot: router
  worker:
    agent: worker
  channels:
    mock-im:
      type: mock-im
  jobs:
    jobs:
      - id: notify-only
        plan:
          - id: notify
            action: deliver
            channel: mock-im
            target: {{ handle: owner }}
            content: {{ text: "the digest is ready" }}
    triggers: []
    alerts: []
providers:
  router-mock:
    base_url: "{router}"
    api_key_env: KIAN_E2E_API_KEY
  worker-mock:
    base_url: "{worker}"
    api_key_env: KIAN_E2E_API_KEY
plugins: ["openai-compat", "local-tools", "superagent"]
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
        router = router,
        worker = worker,
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write config");
    path
}

fn of_kind(events: &[Event], kind: EventKind) -> Vec<&Event> {
    events.iter().filter(|event| event.kind == kind).collect()
}

fn verdict<'a>(events: &'a [Event], gate: &str) -> Option<&'a Event> {
    events
        .iter()
        .find(|event| event.kind == EventKind::EvidenceVerdict && event.bytes["gate"] == gate)
}

async fn wait_for_run(
    store: &EventStore,
    session: SessionId,
    desc: &str,
    pred: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    for _ in 0..1200 {
        let events = store.rebuild(session).await.expect("rebuild run");
        if pred(&events) {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("timed out waiting for: {desc}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deliver_only_run_ends_completed_with_a_real_receipt() {
    force_no_proxy();
    let dir = temp_dir("job-run-e2e");
    let (router_base, _router_mock) = spawn_scripted_provider(
        vec![ScriptedResponse::Text("bootstrap acknowledged".into()); 4],
        0,
    )
    .await;
    let (worker_base, _worker_mock) =
        spawn_scripted_provider(vec![ScriptedResponse::Text("unused".into()); 4], 0).await;
    let config = write_config(&dir, &router_base, &worker_base);

    let admitted = kian_cli::job_cli::execute(&JobCommand::Run {
        job: JOB.to_string(),
        slot: Some(SLOT.to_string()),
        pack: None,
        base: None,
        force: false,
        config: config.clone(),
        json: true,
    })
    .await
    .expect("kian job run admits the row");
    let admitted: Value = serde_json::from_str(&admitted).expect("job run --json");
    assert_eq!(admitted["run_id"], format!("{JOB}@{SLOT}"));
    assert_eq!(admitted["state"], "admitted");

    let session = plan::run_session(JOB, SLOT);
    // Nothing drains the row yet: the server below is the only process that
    // can, and it learns about the run from startup_rebuild alone.
    let _server = Server::from_config_file(&config)
        .await
        .expect("assemble the serve")
        .0;
    let store = EventStore::open_read_only(dir.join("kian.sqlite"))
        .await
        .expect("open the committed stream read-only");
    let events = wait_for_run(&store, session, "the run ends", |events| {
        events
            .iter()
            .any(|event| matches!(event.kind, EventKind::SessionEnded { .. }))
    })
    .await;

    // The frozen plan arrived under the additive job key on the admitted row
    // (A1) — the conductor classifies the session from that marker, not from
    // content.
    let inputs = of_kind(&events, EventKind::InputAdmitted);
    assert_eq!(inputs.len(), 1, "one admitted input row: {events:#?}");
    let marker = &inputs[0].bytes["job"];
    assert_eq!(marker["job_id"], JOB);
    assert_eq!(marker["slot"], SLOT);
    assert_eq!(marker["trigger"], "manual", "a kian job run is manual");
    assert_eq!(marker["run_id"], format!("{JOB}@{SLOT}"));
    assert_eq!(marker["plan"]["steps"][0]["id"], "notify");
    let digest = marker["plan_digest"]
        .as_str()
        .expect("the marker carries the plan digest")
        .to_string();
    assert!(digest.starts_with("sha256:"), "digest: {digest}");

    // One delivery attempt: the intent, the adapter receipt, the closed
    // intent. A delivery is never proven by an exit code — the receipt is
    // the evidence (§13.1).
    let calls = of_kind(&events, EventKind::ToolCall);
    assert_eq!(calls.len(), 1, "one delivery intent: {events:#?}");
    assert_eq!(calls[0].bytes["call_id"], "job:deliver:notify:1");
    assert_eq!(calls[0].bytes["tool"], "reply");
    assert_eq!(calls[0].bytes["args"]["channel"], "mock-im");
    assert_eq!(calls[0].bytes["args"]["target"]["handle"], "owner");
    assert_eq!(
        calls[0].bytes["args"]["content"]["text"],
        "the digest is ready"
    );

    let receipts = of_kind(&events, EventKind::ChannelDelivered);
    assert_eq!(receipts.len(), 1, "one receipt: {events:#?}");
    assert_eq!(receipts[0].bytes["channel"], "mock-im");
    assert_eq!(receipts[0].bytes["target"]["handle"], "owner");
    assert!(
        receipts[0].bytes["detail"]["message_id"].is_string(),
        "the receipt names the adapter message: {:#?}",
        receipts[0].bytes
    );

    let results = of_kind(&events, EventKind::ToolResult);
    assert_eq!(results.len(), 1, "the intent is closed once: {events:#?}");
    assert_eq!(results[0].bytes["call_id"], "job:deliver:notify:1");
    assert_eq!(results[0].bytes["ok"], true);

    // Two verdicts and nothing else: the step gate and the run gate share one
    // shape, so evidence and outcome cannot disagree (§11.1).
    let verdicts = of_kind(&events, EventKind::EvidenceVerdict);
    assert_eq!(
        verdicts.len(),
        2,
        "one step verdict and one run verdict: {events:#?}"
    );
    let step = verdict(&events, "step:notify").expect("the step verdict");
    assert_eq!(step.bytes["passed"], true);
    assert_eq!(step.bytes["job"]["step"], "notify");
    assert_eq!(step.bytes["job"]["attempt"], 1);
    assert_eq!(step.bytes["job"]["outcome"], "succeeded");
    let run = verdict(&events, "run").expect("the run verdict");
    assert_eq!(run.bytes["passed"], true);
    assert_eq!(run.bytes["attempt"], 0);
    assert_eq!(run.bytes["job"]["outcome"], "succeeded");
    assert_eq!(run.bytes["job"]["plan_digest"], digest.as_str());
    assert_eq!(run.bytes["job"]["steps"][0]["step"], "notify");
    assert_eq!(run.bytes["job"]["steps"][0]["outcome"], "succeeded");
    assert!(
        run.bytes["job"].get("failed_step").is_none(),
        "a succeeded run names no failed step: {:#?}",
        run.bytes
    );

    // The engine owns the end write; the conductor only chose the outcome.
    let ended = of_kind(
        &events,
        EventKind::SessionEnded {
            reason: EndReason::Completed,
        },
    );
    assert_eq!(ended.len(), 1, "one end, completed: {events:#?}");
}
