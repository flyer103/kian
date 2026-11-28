use std::path::{Path, PathBuf};

use kian_cli::job_cli::{self, JobCommand};
use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_store::{AppendGuard, EventStore};
use kian_superagent::job::{plan, status};
use serde_json::{Value, json};
use uuid::Uuid;

struct Temp(PathBuf);

impl Temp {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("kian-job-status-{name}-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("temp dir");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn write_config(dir: &Path) -> PathBuf {
    let sqlite = dir.join("events.sqlite");
    let yaml = format!(
        r#"listen: "127.0.0.1:0"
storage:
  sqlite: "{sqlite}"
agent:
  id: job-status-agent
  model_slots:
    main:
      provider: mock
      model: mock-model
  tools: []
providers:
  mock:
    base_url: "http://127.0.0.1:1"
    api_key_env: KIAN_JOB_STATUS_KEY
tools:
  root: "{dir}"
  exec_timeout_ms: 30000
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
plugins: ["openai-compat", "local-tools", "superagent"]
agents:
  router:
    id: router
    version: 3
    model_slots:
      router:
        provider: mock
        model: mock-model
  worker:
    id: worker
    version: 7
    model_slots:
      main:
        provider: mock
        model: mock-model
superagent:
  router:
    session_id: "5b5b5b5b-5b5b-5b5b-5b5b-5b5b5b5b5b5b"
    agent: router
    slot: router
  worker:
    agent: worker
  channels:
    mock-im:
      type: mock-im
  jobs:
    jobs:
      - id: digest
        plan:
          - id: fetch
            prompt: "fetch the news"
            verify:
              - check: final_message_present
          - id: notify
            action: deliver
            depends_on: ["fetch"]
            channel: mock-im
            target:
              handle: owner
            content:
              text: "done"
    triggers: []
    alerts: []
"#,
        sqlite = sqlite.display(),
        dir = dir.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write config");
    path
}

fn job_plan() -> plan::JobPlan {
    serde_json::from_value(json!({
        "id": "digest",
        "steps": [
            {
                "id": "fetch",
                "prompt": "fetch the news",
                "verify": [{"check": "final_message_present"}]
            },
            {
                "id": "notify",
                "action": "deliver",
                "depends_on": ["fetch"],
                "channel": "mock-im",
                "target": {"handle": "owner"},
                "content": {"text": "done"}
            }
        ]
    }))
    .expect("plan")
}

fn event(session: SessionId, seq: u64, ts: u64, kind: EventKind, bytes: Value) -> Event {
    Event {
        seq,
        session,
        epoch: Epoch(1),
        ts,
        kind,
        bytes,
        version: SCHEMA_VERSION,
    }
}

fn admitted_event(session: SessionId, seq: u64, ts: u64, slot: &str, trigger: &str) -> Event {
    event(
        session,
        seq,
        ts,
        EventKind::InputAdmitted,
        json!({
            "role": "user",
            "content": "[kian-job] run=digest",
            "job": status::marker_payload("digest", slot, trigger, &job_plan()),
        }),
    )
}

async fn append(store: &EventStore, events: &[Event]) {
    for event in events {
        store
            .append(
                event.session,
                event.clone(),
                AppendGuard {
                    epoch: Epoch(1),
                    expected_seq: event.seq,
                },
            )
            .await
            .expect("append event");
    }
}

async fn append_admitted(
    store: &EventStore,
    slot: &str,
    trigger: &str,
    started_ms: u64,
) -> SessionId {
    let session = plan::run_session("digest", slot);
    let run = plan::run_id("digest", slot);
    store
        .admit(kian_core::model::InputRow {
            id: Uuid::new_v4(),
            session,
            payload: json!({
                "content": "[kian-job] run=digest",
                "job": status::marker_payload("digest", slot, trigger, &job_plan()),
            }),
            admitted_at: started_ms,
            eligible_at: started_ms,
            idempotency_key: Some(plan::run_input_key(&run)),
        })
        .await
        .expect("admit run row");
    append(
        store,
        &[admitted_event(session, 1, started_ms, slot, trigger)],
    )
    .await;
    session
}

async fn run_json(command: JobCommand) -> Value {
    let output = job_cli::execute(&command).await.expect("job status");
    serde_json::from_str(&output).expect("status json")
}

#[tokio::test]
async fn list_distinguishes_no_run_from_the_latest_manual_and_alert_run() {
    let temp = Temp::new("list");
    let config = write_config(temp.path());
    let store = EventStore::open(temp.path().join("events.sqlite"))
        .await
        .expect("store");

    let listed = job_cli::execute(&JobCommand::List {
        config: config.clone(),
        json: true,
    })
    .await
    .expect("first list");
    let listed: Value = serde_json::from_str(&listed).expect("list json");
    assert!(listed["jobs"][0]["last_run"].is_null());

    append_admitted(&store, "manual:ONE", "manual", 1_000).await;
    let listed = job_cli::execute(&JobCommand::List {
        config: config.clone(),
        json: true,
    })
    .await
    .expect("manual list");
    let listed: Value = serde_json::from_str(&listed).expect("list json");
    assert_eq!(listed["jobs"][0]["last_run"]["run_id"], "digest@manual:ONE");
    assert_eq!(listed["jobs"][0]["last_run"]["status"], "pending");

    let alert_slot = plan::alert_slot("digest@manual:ONE");
    append_admitted(&store, &alert_slot, "alert", 2_000).await;
    let listed = job_cli::execute(&JobCommand::List {
        config: config.clone(),
        json: true,
    })
    .await
    .expect("alert list");
    let listed: Value = serde_json::from_str(&listed).expect("list json");
    assert_eq!(
        listed["jobs"][0]["last_run"]["run_id"],
        format!("digest@{alert_slot}")
    );
    store.close().await;
}

#[tokio::test]
async fn status_by_job_resolves_manual_then_alert_runs() {
    let temp = Temp::new("by-job");
    let config = write_config(temp.path());
    let store = EventStore::open(temp.path().join("events.sqlite"))
        .await
        .expect("store");
    append_admitted(&store, "manual:ONE", "manual", 1_000).await;

    let manual = job_cli::execute(&JobCommand::Status {
        target: "digest".into(),
        config: config.clone(),
        json: true,
    })
    .await
    .expect("manual job status");
    let manual: Value = serde_json::from_str(&manual).expect("manual json");
    assert_eq!(manual["run_id"], "digest@manual:ONE");
    assert_eq!(manual["trigger"], "manual");
    assert_eq!(manual["status"], "pending");
    assert_eq!(manual["status_basis"], "stream_only");

    let alert_slot = plan::alert_slot("digest@manual:ONE");
    append_admitted(&store, &alert_slot, "alert", 2_000).await;
    let alert = job_cli::execute(&JobCommand::Status {
        target: "digest".into(),
        config,
        json: true,
    })
    .await
    .expect("alert job status");
    let alert: Value = serde_json::from_str(&alert).expect("alert json");
    assert_eq!(alert["run_id"], format!("digest@{alert_slot}"));
    assert_eq!(alert["trigger"], "alert");
    store.close().await;
}

#[tokio::test]
async fn status_json_exposes_the_full_run_step_and_attempt_shape() {
    let temp = Temp::new("shape");
    let config = write_config(temp.path());
    let store = EventStore::open(temp.path().join("events.sqlite"))
        .await
        .expect("store");
    let session = append_admitted(&store, "manual:FULL", "manual", 10).await;
    let dispatch = event(
        session,
        2,
        20,
        EventKind::ThreadStarted,
        json!({
            "thread_id": Uuid::from_u128(101).to_string(),
            "parent": session.0.to_string(),
            "worker": Uuid::from_u128(201).to_string(),
            "pinned_version": 1,
            "task": {"role": "user", "content": "fetch"},
            "exec_worker": "vps",
            "job": {"step": "fetch", "attempt": 1},
        }),
    );
    let checks = json!([{"check": "final_message_present", "passed": true}]);
    let step_verdict = event(
        session,
        3,
        40,
        EventKind::EvidenceVerdict,
        json!({
            "gate": "step:fetch",
            "passed": true,
            "checks": checks,
            "attempt": 0,
            "job": {
                "run": "digest@manual:FULL",
                "step": "fetch",
                "attempt": 1,
                "outcome": "succeeded"
            }
        }),
    );
    let verdict = event(
        session,
        4,
        50,
        EventKind::EvidenceVerdict,
        json!({
            "gate": "run",
            "passed": false,
            "checks": [],
            "attempt": 0,
            "job": {
                "run": "digest@manual:FULL",
                "outcome": "failed",
                "plan_digest": job_plan().digest(),
                "steps": [
                    {"step": "fetch", "outcome": "succeeded"},
                    {"step": "notify", "outcome": "skipped"}
                ],
                "reason": "step_failed"
            }
        }),
    );
    let end = event(
        session,
        5,
        60,
        EventKind::SessionEnded {
            reason: EndReason::Completed,
        },
        json!({}),
    );
    append(&store, &[dispatch, step_verdict, verdict, end]).await;

    let view = run_json(JobCommand::Status {
        target: "digest@manual:FULL".into(),
        config,
        json: true,
    })
    .await;
    assert_eq!(view["status"], "failed");
    assert_eq!(view["status_basis"], "stream_only");
    assert_eq!(view["reason"], "step_failed");
    assert_eq!(view["outcome"], "failed");
    assert_eq!(view["started_ms"], 10);
    assert_eq!(view["ended_ms"], 60);
    let fetch = &view["steps"][0];
    assert_eq!(fetch["state"], "settled");
    assert_eq!(fetch["gate"], "step:fetch");
    assert_eq!(fetch["checks"][0]["check"], "final_message_present");
    assert!(fetch.get("next_due_ms").is_some());
    let attempt = &fetch["attempts"][0];
    assert_eq!(attempt["attempt"], 1);
    assert_eq!(attempt["worker"], "vps");
    assert_eq!(attempt["started_ms"], 20);
    assert_eq!(attempt["ended_ms"], 40);
    assert_eq!(attempt["outcome"], "succeeded");
    assert_eq!(attempt["gate"], "step:fetch");
    assert_eq!(attempt["checks"][0]["check"], "final_message_present");
    assert_eq!(view["steps"][1]["state"], "skipped");
    store.close().await;
}

#[tokio::test]
async fn status_of_an_admitted_but_unread_run_is_pending() {
    let temp = Temp::new("pending");
    let config = write_config(temp.path());
    let store = EventStore::open(temp.path().join("events.sqlite"))
        .await
        .expect("store");
    let session = plan::run_session("digest", "manual:PENDING");
    store
        .admit(kian_core::model::InputRow {
            id: Uuid::new_v4(),
            session,
            payload: json!({
                "content": "[kian-job] run=digest",
                "job": status::marker_payload(
                    "digest",
                    "manual:PENDING",
                    "manual",
                    &job_plan(),
                ),
            }),
            admitted_at: 123,
            eligible_at: 123,
            idempotency_key: Some(plan::run_input_key("digest@manual:PENDING")),
        })
        .await
        .expect("admit pending run");
    store.close().await;

    let listed = job_cli::execute(&JobCommand::List {
        config: config.clone(),
        json: true,
    })
    .await
    .expect("list pending run");
    let listed: Value = serde_json::from_str(&listed).expect("list json");
    assert_eq!(
        listed["jobs"][0]["last_run"]["run_id"],
        "digest@manual:PENDING"
    );
    assert_eq!(listed["jobs"][0]["last_run"]["status"], "pending");

    let by_job = job_cli::execute(&JobCommand::Status {
        target: "digest".into(),
        config: config.clone(),
        json: true,
    })
    .await
    .expect("pending job status");
    let by_job: Value = serde_json::from_str(&by_job).expect("pending job json");
    assert_eq!(by_job["run_id"], "digest@manual:PENDING");
    assert_eq!(by_job["status"], "pending");

    let view = run_json(JobCommand::Status {
        target: "digest@manual:PENDING".into(),
        config,
        json: true,
    })
    .await;
    assert_eq!(view["status"], "pending");
    assert_eq!(view["status_basis"], "stream_only");
    assert_eq!(view["started_ms"], 123);
    assert!(view["outcome"].is_null());
    assert_eq!(view["steps"][0]["state"], "pending");
}
