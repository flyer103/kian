//! (JOB-MODEL §18.4) Acceptance probes for the run conductor: one run
//! dispatches a ready step, settles it from the child's committed stream,
//! delivers as a conductor action, and ends with exactly one run verdict.
//!
//! Also the two honest refusals the contract names: an empty candidate set
//! fails the step immediately (never a wait), and a failed delivery fails the
//! run and admits exactly one alert run.
//!
//! The `w1..w8` tests are the §17 crash windows, each simulated the same way:
//! build the committed prefix a crash at that point leaves behind, throw the
//! conductor away, and drive a brand-new one over exactly that prefix. W9 (the
//! sweep settles the firing) is scheduler side and covered by the cron
//! settlement tests.
//!
//! Fakes only: no store, no daemon, no network.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_core::seams::{ChannelError, LoopDriver, LoopOutcome, SendReceipt};
use kian_orchestrator::WakeOutcome;
use kian_superagent::directory::{
    MemberState, MemberView, WorkerDirectoryError, WorkerDirectoryPort,
};
use kian_superagent::job::alert::AlertSpec;
use kian_superagent::job::conduct::{
    AlertPort, Clock, DeliverPort, GateReceipt, JOB_FAILED_REASON, JOB_FINISHED_STOP_REASON,
    JobDeps, JobLoopDriver, PublishError, PublishPort, PublishReceipt, PublishRequest,
    PublishTargets, RunPort, check_publish_ok,
};
use kian_superagent::job::outcome::AttemptOutcome;
use kian_superagent::job::plan::{self, JobPlan};
use kian_superagent::job::status;
use kian_superagent::worker::{WorkerStreamsError, WorkerStreamsPort};
use kian_superagent::{
    AdmitReceipt, InboxWriteError, InboxWriter, ResidentBinding, WorkerSpawner, worker_template,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const JOB: &str = "digest";
const SLOT: &str = "manual:T1";
const WORKER: &str = "laptop";

fn event(session: SessionId, seq: u64, kind: EventKind, bytes: Value, ts: u64) -> Event {
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

fn online(id: &str, capabilities: &[&str]) -> MemberView {
    MemberView {
        id: id.to_string(),
        description: Some("test member".to_string()),
        version: Some(1),
        capabilities: capabilities.iter().map(|cap| (*cap).to_string()).collect(),
        declaration_stale: false,
        state: MemberState::Online,
        last_seen_ms: 1_000_000,
    }
}

fn digest_plan() -> JobPlan {
    serde_json::from_value(json!({
        "id": JOB,
        "steps": [
            {
                "id": "fetch",
                "prompt": "fetch the news",
                "requires": ["net.unrestricted"],
                "verify": [{"check": "final_message_present"}]
            },
            {
                "id": "notify",
                "action": "deliver",
                "depends_on": ["fetch"],
                "channel": "mock-im",
                "target": {"handle": "owner"},
                "content": {"text": "the digest"},
                "verify": [{
                    "check": "delivery_receipt",
                    "channel": "mock-im",
                    "target": {"handle": "owner"}
                }]
            }
        ]
    }))
    .expect("a valid digest plan")
}

struct World {
    run: Mutex<Vec<Event>>,
    children: Mutex<BTreeMap<SessionId, Vec<Event>>>,
    admits: Mutex<Vec<(SessionId, Value, Option<String>)>>,
    admit_keys: Mutex<BTreeSet<String>>,
    sends: Mutex<Vec<(String, Value, Value)>>,
    alerts: Mutex<Vec<(SessionId, Value, String)>>,
    alert_keys: Mutex<BTreeSet<String>>,
    deliver_error: Mutex<Option<ChannelError>>,
    publish_behavior: Mutex<PublishBehavior>,
    publish_requests: Mutex<Vec<PublishRequest>>,
    clock: Mutex<u64>,
}

impl World {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            run: Mutex::new(Vec::new()),
            children: Mutex::new(BTreeMap::new()),
            admits: Mutex::new(Vec::new()),
            admit_keys: Mutex::new(BTreeSet::new()),
            sends: Mutex::new(Vec::new()),
            alerts: Mutex::new(Vec::new()),
            alert_keys: Mutex::new(BTreeSet::new()),
            deliver_error: Mutex::new(None),
            publish_behavior: Mutex::new(PublishBehavior::Commit),
            publish_requests: Mutex::new(Vec::new()),
            clock: Mutex::new(1_700_000_000_000),
        })
    }

    fn now_ms(&self) -> u64 {
        *self.clock.lock().expect("the fake clock")
    }

    fn run_events(&self) -> Vec<Event> {
        self.run.lock().expect("the run log").clone()
    }
}

#[derive(Clone)]
struct RunCtx {
    session: SessionId,
    world: Arc<World>,
}

impl RunPort for RunCtx {
    fn session(&self) -> SessionId {
        self.session
    }

    fn emit(&self, kind: EventKind, bytes: Value) -> BoxFuture<'static, Result<(), String>> {
        let world = Arc::clone(&self.world);
        let session = self.session;
        Box::pin(async move {
            let now = world.now_ms();
            let mut log = world.run.lock().map_err(|error| error.to_string())?;
            let seq = log.last().map(|event| event.seq + 1).unwrap_or(1);
            log.push(event(session, seq, kind, bytes, now));
            Ok(())
        })
    }

    fn read_stream(&self) -> BoxFuture<'static, Result<Vec<Event>, String>> {
        let world = Arc::clone(&self.world);
        Box::pin(async move {
            let log = world.run.lock().map_err(|error| error.to_string())?;
            Ok(log.clone())
        })
    }

    fn interrupt_requested(&self) -> bool {
        false
    }

    fn take_inputs(&self) -> BoxFuture<'static, Result<Vec<Value>, String>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct FakeStreams(Arc<World>);

impl WorkerStreamsPort for FakeStreams {
    fn read_stream(
        &self,
        session: SessionId,
    ) -> BoxFuture<'static, Result<Vec<Event>, WorkerStreamsError>> {
        let world = Arc::clone(&self.0);
        Box::pin(async move {
            let children = world
                .children
                .lock()
                .map_err(|error| WorkerStreamsError(error.to_string()))?;
            match children.get(&session) {
                Some(events) => Ok(events.clone()),
                None => Err(WorkerStreamsError(format!("no child stream {session:?}"))),
            }
        })
    }
}

struct FakeDirectory(Vec<MemberView>);

impl WorkerDirectoryPort for FakeDirectory {
    fn known_ids(&self) -> BoxFuture<'static, Result<Vec<String>, WorkerDirectoryError>> {
        let ids = self.0.iter().map(|member| member.id.clone()).collect();
        Box::pin(async move { Ok(ids) })
    }

    fn members(&self) -> BoxFuture<'static, Result<Vec<MemberView>, WorkerDirectoryError>> {
        let members = self.0.clone();
        Box::pin(async move { Ok(members) })
    }

    fn member(
        &self,
        id: &str,
    ) -> BoxFuture<'static, Result<Option<MemberView>, WorkerDirectoryError>> {
        let found = self.0.iter().find(|member| member.id == id).cloned();
        Box::pin(async move { Ok(found) })
    }
}

struct FakeDeliver(Arc<World>);

impl DeliverPort for FakeDeliver {
    fn send(
        &self,
        channel: &str,
        target: Value,
        content: Value,
    ) -> BoxFuture<'static, Result<SendReceipt, ChannelError>> {
        let world = Arc::clone(&self.0);
        let channel = channel.to_string();
        Box::pin(async move {
            world
                .sends
                .lock()
                .map_err(|error| ChannelError::Unavailable(error.to_string()))?
                .push((channel, target, content));
            let injected = world
                .deliver_error
                .lock()
                .map_err(|error| ChannelError::Unavailable(error.to_string()))?
                .take();
            match injected {
                Some(error) => Err(error),
                None => Ok(SendReceipt {
                    message_id: "mock-1".to_string(),
                    ts_ms: world.now_ms(),
                }),
            }
        })
    }
}

struct FakeAlerts(Arc<World>);

impl AlertPort for FakeAlerts {
    fn alerts_for(&self, job_id: &str) -> Vec<AlertSpec> {
        if job_id != JOB {
            return Vec::new();
        }
        vec![AlertSpec {
            id: "ops".to_string(),
            channel: "alert-im".to_string(),
            target: json!({"handle": "owner"}),
        }]
    }

    fn raise(
        &self,
        session: SessionId,
        payload: Value,
        key: String,
    ) -> BoxFuture<'static, Result<(), String>> {
        let world = Arc::clone(&self.0);
        Box::pin(async move {
            let first = world
                .alert_keys
                .lock()
                .map_err(|error| error.to_string())?
                .insert(key.clone());
            if !first {
                return Ok(());
            }
            world
                .alerts
                .lock()
                .map_err(|error| error.to_string())?
                .push((session, payload, key));
            Ok(())
        })
    }
}

struct FakeClock(Arc<World>);

impl Clock for FakeClock {
    fn now_ms(&self) -> u64 {
        self.0.now_ms()
    }
}

struct CaptureInbox(Arc<World>);

impl InboxWriter for CaptureInbox {
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        let world = Arc::clone(&self.0);
        Box::pin(async move {
            if let Some(key) = &idempotency_key {
                let first = world
                    .admit_keys
                    .lock()
                    .map_err(|error| InboxWriteError::Write(error.to_string()))?
                    .insert(key.clone());
                if !first {
                    return Ok(AdmitReceipt { deduplicated: true });
                }
            }
            world
                .admits
                .lock()
                .map_err(|error| InboxWriteError::Write(error.to_string()))?
                .push((session, payload, idempotency_key));
            Ok(AdmitReceipt {
                deduplicated: false,
            })
        })
    }

    fn wake(&self, _session: SessionId) -> WakeOutcome {
        WakeOutcome::Folded
    }
}

struct Harness {
    world: Arc<World>,
    run: SessionId,
    deps: Arc<JobDeps>,
    driver: JobLoopDriver<RunCtx>,
}

impl Harness {
    /// A second conductor over the same committed stream and the same ports:
    /// the shape a takeover has after a crash (J1/J4 — nothing in memory is
    /// carried over, so whatever this driver decides is re-derived).
    fn new_driver(&self) -> JobLoopDriver<RunCtx> {
        JobLoopDriver::new(
            Arc::new(|ctx: &RunCtx| Arc::new(ctx.clone()) as Arc<dyn RunPort>),
            Arc::clone(&self.deps),
        )
    }
}

fn harness(members: Vec<MemberView>, deliver_error: Option<ChannelError>) -> Harness {
    harness_for(digest_plan(), members, deliver_error)
}

fn harness_for(
    plan: JobPlan,
    members: Vec<MemberView>,
    deliver_error: Option<ChannelError>,
) -> Harness {
    let world = World::new();
    let run = plan::run_session(JOB, SLOT);
    let now = world.now_ms();
    // A plan that declares a surface is a self-dev plan, and its marker must
    // carry the pack facts an admission verified (V24); the probe only needs
    // the key to exist for the conductor to accept the run.
    let selfdev = plan.declares_surface().then(|| {
        json!({
            "pack": {
                "dir": "/tmp/evidence",
                "manifest_sha256": "sha256:00",
                "files_sha256": "sha256:00",
            },
            "base_commit": "0".repeat(40),
            "kian_version": "0.3.0",
        })
    });
    world.run.lock().expect("the run log").push(event(
        run,
        1,
        EventKind::InputAdmitted,
        json!({
            "role": "user",
            "content": "run the digest",
            "job": status::marker_payload_with_selfdev(JOB, SLOT, "manual", &plan, selfdev),
        }),
        now,
    ));
    *world.deliver_error.lock().expect("the delivery error") = deliver_error;
    let spawner = WorkerSpawner::new(
        ResidentBinding::new(run),
        &worker_template("mock", "mock-worker", 7),
        Arc::new(CaptureInbox(Arc::clone(&world))),
    );
    let mut deps = JobDeps::new(
        Arc::new(FakeStreams(Arc::clone(&world))),
        Arc::new(spawner),
        Arc::new(FakeDirectory(members)),
        Arc::new(FakeDeliver(Arc::clone(&world))),
        Arc::new(FakeAlerts(Arc::clone(&world))),
        Arc::new(FakeClock(Arc::clone(&world))),
    )
    .with_publish(Arc::new(FakePublish(Arc::clone(&world))));
    // A test tick does not need the production park cadence; the bounds are
    // still bounded (floor <= ceiling), which is what clamp_wait requires.
    deps.tick_floor_ms = 1;
    deps.tick_ceiling_ms = 5;
    let deps = Arc::new(deps);
    let driver = JobLoopDriver::new(
        Arc::new(|ctx: &RunCtx| Arc::new(ctx.clone()) as Arc<dyn RunPort>),
        Arc::clone(&deps),
    );
    Harness {
        world,
        run,
        deps,
        driver,
    }
}

fn context(harness: &Harness) -> RunCtx {
    RunCtx {
        session: harness.run,
        world: Arc::clone(&harness.world),
    }
}

async fn next_outcome(stream: &mut BoxStream<'static, LoopOutcome>) -> LoopOutcome {
    stream
        .next()
        .await
        .expect("the conductor yielded an outcome")
}

/// The child session the conductor spawned in its dispatch record.
fn dispatched_child(harness: &Harness) -> SessionId {
    let events = harness.world.run_events();
    let started = events
        .iter()
        .find(|event| matches!(event.kind, EventKind::ThreadStarted))
        .expect("a dispatch record");
    let raw = started.bytes["worker"].as_str().expect("the child session");
    SessionId(Uuid::parse_str(raw).expect("a uuid"))
}

/// The child's committed stream: the wrapper row, one tool round trip, a final
/// message, and the end the child wrote for itself.
fn child_stream(child: SessionId, ended: EndReason) -> Vec<Event> {
    let now = 1_700_000_000_100;
    vec![
        event(
            child,
            1,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "do the step"}),
            now,
        ),
        event(
            child,
            2,
            EventKind::ToolCall,
            json!({"call_id": "c1", "tool": "fetch", "args": {}}),
            now + 1,
        ),
        event(
            child,
            3,
            EventKind::ToolResult,
            json!({"call_id": "c1", "ok": true, "result": "the news"}),
            now + 2,
        ),
        event(
            child,
            4,
            EventKind::AssistantMessage,
            json!({"role": "assistant", "content": {"text": "here is the news"}}),
            now + 3,
        ),
        event(
            child,
            5,
            EventKind::SessionEnded { reason: ended },
            json!({}),
            now + 4,
        ),
    ]
}

const PATCH: &str = "--- a/crates/kian-cli/src/lib.rs
+++ b/crates/kian-cli/src/lib.rs
` -1 +1 `
-old
+new
";

fn proposal_plan() -> JobPlan {
    serde_json::from_value(json!({
        "id": JOB,
        "steps": [{
            "id": "patch",
            "prompt": "propose a patch",
            "requires": ["repo.kian"],
            "produces": {"name": "patch", "max_bytes": 4096},
            "verify": [
                {"check": "artifact_within_cap", "name": "patch"},
                {
                    "check": "patch_paths_allowed",
                    "artifact": "patch",
                    "allow": ["crates/**"],
                    "deny": ["design/decisions/**"]
                }
            ]
        }]
    }))
    .expect("a valid proposal plan")
}

const PUBLISH_COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
const PUBLISH_CALL: &str = "job:publish:publish:1";

/// What the fake publish does: commit (the receipt), or find the derived
/// worktree already occupied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishBehavior {
    Commit,
    Occupied,
}

struct FakePublish(Arc<World>);

impl PublishPort for FakePublish {
    fn derive(&self, run: SessionId) -> Option<PublishTargets> {
        Some(PublishTargets {
            repo: "/tmp/kian".to_string(),
            worktree: format!("/tmp/kian/.agents/worktrees/selfdev-{}", run.0),
        })
    }

    fn publish(
        &self,
        request: PublishRequest,
    ) -> BoxFuture<'static, Result<PublishReceipt, PublishError>> {
        let world = Arc::clone(&self.0);
        Box::pin(async move {
            let behavior = *world
                .publish_behavior
                .lock()
                .map_err(|error| PublishError::Failed(error.to_string()))?;
            {
                let mut requests = world
                    .publish_requests
                    .lock()
                    .map_err(|error| PublishError::Failed(error.to_string()))?;
                requests.push(request.clone());
            }
            match behavior {
                PublishBehavior::Commit => Ok(PublishReceipt {
                    commit: PUBLISH_COMMIT.to_string(),
                    branch: request.branch,
                    worktree: request.worktree,
                    gate: request.gates.then(|| GateReceipt {
                        command: "./scripts/selfcheck.sh".to_string(),
                        exit_code: 0,
                        digest: "sha256:00".to_string(),
                        output_tail: String::new(),
                    }),
                }),
                PublishBehavior::Occupied => Err(PublishError::Occupied(format!(
                    "the worktree {} already exists",
                    request.worktree
                ))),
            }
        })
    }
}

fn publish_plan() -> JobPlan {
    serde_json::from_value(json!({
        "id": JOB,
        "steps": [{
            "id": "patch",
            "prompt": "propose a patch",
            "requires": ["repo.kian"],
            "produces": {"name": "patch", "max_bytes": 4096},
            "verify": [
                {"check": "artifact_within_cap", "name": "patch"},
                {
                    "check": "patch_paths_allowed",
                    "artifact": "patch",
                    "allow": ["crates/**"],
                    "deny": ["design/decisions/**"]
                }
            ]
        }, {
            "id": "publish",
            "action": "publish",
            "depends_on": ["patch"],
            "inputs": [{"step": "patch", "name": "patch"}],
            "publish": {"base": "main", "gates": true},
            "effect": "write",
            "verify": [{"check": "publish_ok"}]
        }]
    }))
    .expect("a valid publish plan")
}

/// The child's committed stream for a proposal: one final answer, no tool round.
fn proposal_child(child: SessionId) -> Vec<Event> {
    let now = 1_700_000_000_100;
    vec![
        event(
            child,
            1,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "propose a patch"}),
            now,
        ),
        event(
            child,
            2,
            EventKind::AssistantMessage,
            json!({"role": "assistant", "content": {"text": PATCH}}),
            now + 1,
        ),
        event(
            child,
            3,
            EventKind::SessionEnded {
                reason: EndReason::Completed,
            },
            json!({}),
            now + 2,
        ),
    ]
}

// ---------------------------------------------------------------------------
// Probes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_run_dispatches_settles_delivers_and_ends_with_one_verdict() {
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
    let mut stream = harness.driver.drive(context(&harness));

    // Tick 1: the ready agent step is dispatched to the one admissible worker.
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let events = harness.world.run_events();
    let started = events
        .iter()
        .find(|event| matches!(event.kind, EventKind::ThreadStarted))
        .expect("a dispatch record before the child exists");
    assert_eq!(started.bytes["parent"], harness.run.0.to_string());
    assert_eq!(started.bytes["exec_worker"], WORKER);
    assert_eq!(started.bytes["job"]["step"], "fetch");
    assert_eq!(started.bytes["job"]["attempt"], 1);
    assert_eq!(started.bytes["job"]["effect"], "read");
    assert_eq!(started.bytes["job"]["candidates"][0], WORKER);
    let admits = harness.world.admits.lock().expect("admits").clone();
    assert_eq!(admits.len(), 1, "one child, admitted once");
    assert_eq!(admits[0].0, dispatched_child(&harness));
    assert_eq!(
        admits[0].1["kian.parent"],
        harness.run.0.to_string(),
        "the wrapper names the run as the lineage parent"
    );

    // The child ran and ended on its own.
    let child = dispatched_child(&harness);
    harness
        .world
        .children
        .lock()
        .expect("children")
        .insert(child, child_stream(child, EndReason::Completed));

    // Tick 2: the settled attempt becomes one step verdict.
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let verdicts = status::verdicts(&harness.world.run_events());
    assert_eq!(verdicts.len(), 1);
    assert_eq!(verdicts[0].step, "fetch");
    assert_eq!(
        verdicts[0].outcome,
        Some(kian_superagent::job::outcome::AttemptOutcome::Succeeded)
    );
    assert!(verdicts[0].passed);

    // Tick 3: the dependent deliver step runs in the conductor's own drain.
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let sends = harness.world.sends.lock().expect("sends").clone();
    assert_eq!(sends.len(), 1);
    assert_eq!(sends[0].0, "mock-im");
    assert_eq!(sends[0].1, json!({"handle": "owner"}));
    assert_eq!(sends[0].2, json!({"text": "the digest"}));

    // Tick 4: every step is terminal, so the run writes one verdict and stops.
    match next_outcome(&mut stream).await {
        LoopOutcome::Stopped { reason } => assert_eq!(reason, JOB_FINISHED_STOP_REASON),
        other => panic!("the run must stop, not {other:?}"),
    }
    let events = harness.world.run_events();
    let verdict = status::run_verdict(&events).expect("one run verdict");
    assert_eq!(
        verdict.outcome,
        kian_superagent::job::outcome::RunOutcome::Succeeded
    );
    assert_eq!(
        status::ended(&events),
        Some(kian_superagent::job::outcome::RunOutcome::Succeeded)
    );
    assert!(
        harness.world.alerts.lock().expect("alerts").is_empty(),
        "a succeeded run declares no alert"
    );
    let view = status::view(&events).expect("a run view");
    assert_eq!(view.steps.len(), 2);
    assert!(
        view.steps
            .iter()
            .all(|step| step.state == status::StepState::Settled),
        "{:?}",
        view.steps
    );
}

#[tokio::test]
async fn an_empty_candidate_set_fails_the_step_at_once_and_creates_no_child() {
    let harness = harness(Vec::new(), None);
    let mut stream = harness.driver.drive(context(&harness));

    // No admissible worker: no park, no child, no wait (§9.4).
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let verdicts = status::verdicts(&harness.world.run_events());
    assert_eq!(verdicts.len(), 1);
    assert_eq!(
        verdicts[0].outcome,
        Some(kian_superagent::job::outcome::AttemptOutcome::Failed)
    );
    assert!(
        harness
            .world
            .run_events()
            .iter()
            .all(|event| !matches!(event.kind, EventKind::ThreadStarted)),
        "a refusal writes no dispatch record"
    );
    assert!(
        harness.world.admits.lock().expect("admits").is_empty(),
        "a refusal creates no child"
    );

    match next_outcome(&mut stream).await {
        LoopOutcome::SoftFailed { reason } => assert_eq!(reason, JOB_FAILED_REASON),
        other => panic!("a refused step must fail the run, not {other:?}"),
    }
    let events = harness.world.run_events();
    let verdict = status::run_verdict(&events).expect("one run verdict");
    assert_eq!(
        verdict.outcome,
        kian_superagent::job::outcome::RunOutcome::Failed
    );
    assert_eq!(verdict.payload["job"]["failed_step"], "fetch");
    let step = events
        .iter()
        .find(|event| {
            matches!(event.kind, EventKind::EvidenceVerdict)
                && event.bytes["gate"].as_str() == Some("step:fetch")
        })
        .expect("one step verdict");
    assert_eq!(step.bytes["passed"], false);
    let detail = step.bytes["checks"][0]["detail"]
        .as_str()
        .expect("the refusal names its reason");
    assert!(detail.contains("net.unrestricted"), "{detail}");
}

#[tokio::test]
async fn a_failed_delivery_fails_the_run_and_admits_exactly_one_alert() {
    let harness = harness(
        vec![online(WORKER, &["net.unrestricted"])],
        Some(ChannelError::DeliveryFailed(
            "the bridge is down".to_string(),
        )),
    );
    let mut stream = harness.driver.drive(context(&harness));

    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let child = dispatched_child(&harness);
    harness
        .world
        .children
        .lock()
        .expect("children")
        .insert(child, child_stream(child, EndReason::Completed));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    // The delivery attempt is made once and settles failed; a delivery is a
    // write, so a failure with no receipt is not auto-retried.
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    assert_eq!(harness.world.sends.lock().expect("sends").len(), 1);

    match next_outcome(&mut stream).await {
        LoopOutcome::SoftFailed { reason } => assert_eq!(reason, JOB_FAILED_REASON),
        other => panic!("a failed delivery must fail the run, not {other:?}"),
    }
    let events = harness.world.run_events();
    let payload = status::run_verdict(&events)
        .expect("one run verdict")
        .payload;
    assert_eq!(payload["job"]["outcome"], "failed");
    assert_eq!(payload["job"]["failed_step"], "notify");

    let alerts = harness.world.alerts.lock().expect("alerts").clone();
    assert_eq!(alerts.len(), 1, "exactly one alert run per failed run");
    assert_eq!(alerts[0].2, format!("kian:alert:{JOB}@{SLOT}"));
    assert_eq!(
        alerts[0].0,
        plan::run_session("alert:ops", &plan::alert_slot(&format!("{JOB}@{SLOT}")))
    );
    // The admitted row starts the alert run; the words the operator reads are
    // the alert plan's own deliver step, frozen in the marker with the row.
    assert_eq!(alerts[0].1["job"]["job_id"], "alert:ops");
    let text = alerts[0].1["job"]["plan"]["steps"][0]["content"]["text"]
        .as_str()
        .expect("the alert text");
    assert!(text.contains("[kian-alert]"), "{text}");
    assert!(text.contains(&format!("{JOB}@{SLOT}")), "{text}");
}

#[tokio::test]
async fn a_run_without_a_marker_is_refused_rather_than_guessed() {
    let harness = harness(Vec::new(), None);
    harness.world.run.lock().expect("the run log").clear();
    harness.world.run.lock().expect("the run log").push(event(
        harness.run,
        1,
        EventKind::InputAdmitted,
        json!({"role": "user", "content": "not a job run"}),
        1_700_000_000_000,
    ));
    let mut stream = harness.driver.drive(context(&harness));
    match next_outcome(&mut stream).await {
        LoopOutcome::SoftFailed { reason } => {
            assert!(reason.contains("no job marker"), "{reason}");
        }
        other => panic!("a stream without a marker is refused, not {other:?}"),
    }
    assert!(harness.world.admits.lock().expect("admits").is_empty());
}
#[tokio::test]
async fn a_timed_out_delivery_settles_needs_operator_not_failed() {
    let harness = harness(
        vec![online(WORKER, &["net.unrestricted"])],
        Some(ChannelError::Timeout),
    );
    let mut stream = harness.driver.drive(context(&harness));

    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let child = dispatched_child(&harness);
    harness
        .world
        .children
        .lock()
        .expect("children")
        .insert(child, child_stream(child, EndReason::Completed));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    // A timed-out write with no receipt may have landed: one attempt, then a
    // verdict an operator reads, never a silent retry (JOB-MODEL §10.2, §12.2).
    assert_eq!(harness.world.sends.lock().expect("sends").len(), 1);

    match next_outcome(&mut stream).await {
        LoopOutcome::SoftFailed { reason } => assert_eq!(reason, JOB_FAILED_REASON),
        other => panic!("a delivery that needs an operator fails the run, not {other:?}"),
    }
    let events = harness.world.run_events();
    let step = events
        .iter()
        .find(|event| {
            matches!(event.kind, EventKind::EvidenceVerdict)
                && event.bytes["gate"].as_str() == Some("step:notify")
        })
        .expect("one verdict for the delivery step");
    assert_eq!(step.bytes["passed"], false);
    assert_eq!(step.bytes["job"]["outcome"], "needs_operator");
    let detail = step.bytes["checks"][0]["detail"]
        .as_str()
        .expect("the verdict names its reason");
    assert!(detail.contains("may have landed"), "{detail}");
    let payload = status::run_verdict(&events)
        .expect("one run verdict")
        .payload;
    assert_eq!(payload["job"]["outcome"], "failed");
    assert_eq!(payload["job"]["failed_step"], "notify");
    assert_eq!(
        harness.world.alerts.lock().expect("alerts").len(),
        1,
        "a run that needs an operator still alerts once"
    );
}

#[tokio::test]
async fn a_takeover_re_derives_instead_of_redispatching() {
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
    let mut first = harness.driver.drive(context(&harness));
    assert!(matches!(
        next_outcome(&mut first).await,
        LoopOutcome::TurnDone
    ));
    // The process dies before the child settles: the dispatch record is
    // committed, no verdict was written.
    drop(first);
    assert!(status::verdicts(&harness.world.run_events()).is_empty());

    // The takeover is a brand-new conductor: nothing in memory survives.
    let mut second = harness.new_driver().drive(context(&harness));
    assert!(matches!(
        next_outcome(&mut second).await,
        LoopOutcome::TurnDone
    ));
    let started: Vec<_> = harness
        .world
        .run_events()
        .into_iter()
        .filter(|event| matches!(event.kind, EventKind::ThreadStarted))
        .collect();
    assert_eq!(
        started.len(),
        1,
        "the re-drive must not dispatch a second attempt"
    );
    assert_eq!(started[0].bytes["job"]["attempt"], 1);
    assert_eq!(harness.world.admits.lock().expect("admits").len(), 1);

    // The child that was already running finishes; the takeover settles it.
    let child = dispatched_child(&harness);
    harness
        .world
        .children
        .lock()
        .expect("children")
        .insert(child, child_stream(child, EndReason::Completed));
    assert!(matches!(
        next_outcome(&mut second).await,
        LoopOutcome::TurnDone
    ));
    let verdicts = status::verdicts(&harness.world.run_events());
    assert_eq!(verdicts.len(), 1);
    assert_eq!(verdicts[0].step, "fetch");
    assert!(verdicts[0].passed);
    // Then the deliver step and the single run verdict, exactly as on the
    // first drive: replay converges, it never double-writes.
    assert!(matches!(
        next_outcome(&mut second).await,
        LoopOutcome::TurnDone
    ));
    match next_outcome(&mut second).await {
        LoopOutcome::Stopped { reason } => assert_eq!(reason, JOB_FINISHED_STOP_REASON),
        other => panic!("the run must stop, not {other:?}"),
    }
    assert_eq!(
        status::run_verdict(&harness.world.run_events())
            .expect("one run verdict")
            .outcome,
        kian_superagent::job::outcome::RunOutcome::Succeeded
    );
    assert_eq!(harness.world.sends.lock().expect("sends").len(), 1);
}

// ---------------------------------------------------------------------------
// Crash windows (§17 W1–W8, §18.4 L2)
//
// Every window is simulated the same way: build the committed prefix a crash
// at that point leaves behind, throw the conductor away, and drive a brand-new
// one over exactly that prefix. W9 (the sweep settles the firing) lives on the
// scheduler side and is covered by the cron settlement tests.
// ---------------------------------------------------------------------------

fn dispatch_count(harness: &Harness) -> usize {
    status::dispatches(&harness.world.run_events()).len()
}

fn admit_count(harness: &Harness) -> usize {
    harness.world.admits.lock().expect("admits").len()
}

fn step_verdict_count(harness: &Harness) -> usize {
    status::verdicts(&harness.world.run_events()).len()
}

fn run_verdict_count(harness: &Harness) -> usize {
    harness
        .world
        .run_events()
        .into_iter()
        .filter(|event| {
            event.kind == EventKind::EvidenceVerdict
                && event.bytes.get("gate").and_then(Value::as_str) == Some("run")
        })
        .count()
}

fn alert_count(harness: &Harness) -> usize {
    harness.world.alerts.lock().expect("alerts").len()
}

/// One tick of a conductor that did not exist before this call.
async fn fresh_tick(harness: &Harness) -> LoopOutcome {
    let driver = harness.new_driver();
    let mut stream = driver.drive(context(harness));
    next_outcome(&mut stream).await
}

fn push_event(harness: &Harness, kind: EventKind, bytes: Value) {
    let now = harness.world.now_ms();
    let mut log = harness.world.run.lock().expect("the run log");
    let seq = log.last().map(|event| event.seq + 1).unwrap_or(1);
    log.push(event(harness.run, seq, kind, bytes, now));
}

#[tokio::test]
async fn w1_before_the_claim_a_redrive_dispatches_exactly_once() {
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
    assert_eq!(
        harness.world.run_events().len(),
        1,
        "only the durable run row"
    );
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(dispatch_count(&harness), 1);
    assert_eq!(admit_count(&harness), 1);
    // A second conductor over the same prefix must not dispatch a second child.
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(dispatch_count(&harness), 1);
    assert_eq!(
        admit_count(&harness),
        1,
        "the task key folds the repeat admit"
    );
}

#[tokio::test]
async fn w2_after_the_claim_before_any_dispatch_the_tick_dispatches_from_scratch() {
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
    push_event(&harness, EventKind::DrainClaimed, json!({}));
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(dispatch_count(&harness), 1);
    assert_eq!(admit_count(&harness), 1);
    // Claims are engine state, not the conductor's: replay derives the same
    // single dispatch and writes no verdict out of them.
    assert!(status::verdicts(&harness.world.run_events()).is_empty());
}

#[tokio::test]
async fn w3_a_record_without_a_child_is_recreated_with_the_same_ids() {
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    let child = dispatched_child(&harness);
    assert_eq!(admit_count(&harness), 1);

    // The crash landed between the record and create_session: the child has
    // no stream and no task row.
    harness.world.children.lock().expect("children").clear();
    harness.world.admits.lock().expect("admits").clear();
    harness.world.admit_keys.lock().expect("keys").clear();

    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(dispatch_count(&harness), 1, "the record is not rewritten");
    assert_eq!(admit_count(&harness), 1, "the same child is created again");
    assert_eq!(
        dispatched_child(&harness),
        child,
        "the ids derive from (run, step, attempt)"
    );

    harness
        .world
        .children
        .lock()
        .expect("children")
        .insert(child, child_stream(child, EndReason::Completed));
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(step_verdict_count(&harness), 1, "the attempt settles once");
}

#[tokio::test]
async fn w4_a_child_created_but_never_admitted_is_readmitted_with_the_same_key() {
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    let child = dispatched_child(&harness);
    // The spawner's idempotency key is thread-scoped, not task-scoped
    // (crates/kian-superagent/src/worker.rs: admit keys on the thread id), and
    // the thread id is derived from (run, step, attempt), so a re-admit
    // reconstructs the same key.
    let key = format!(
        "thread:{}",
        plan::child_thread_id(harness.run, plan::spawn_uid(harness.run, "fetch", 1))
    );

    // The crash landed between create_session and admit: the child stream has
    // one session/created event and the inbox row is missing.
    harness.world.admits.lock().expect("admits").clear();
    harness.world.admit_keys.lock().expect("keys").clear();
    let now = harness.world.now_ms();
    harness.world.children.lock().expect("children").insert(
        child,
        vec![event(child, 1, EventKind::SessionCreated, json!({}), now)],
    );

    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(dispatch_count(&harness), 1);
    let admits = harness.world.admits.lock().expect("admits").clone();
    assert_eq!(admits.len(), 1);
    assert_eq!(
        admits[0].2.as_deref(),
        Some(key.as_str()),
        "the task key is the idempotency key"
    );

    // A crash after the admit: the row is durable, so the repeat admit folds.
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(admit_count(&harness), 1, "first write wins");
}

#[tokio::test]
async fn w5_an_admitted_child_that_never_drained_is_never_re_admitted() {
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    let child = dispatched_child(&harness);
    let now = harness.world.now_ms();
    harness.world.children.lock().expect("children").insert(
        child,
        vec![
            event(child, 1, EventKind::SessionCreated, json!({}), now),
            event(
                child,
                2,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "do the step"}),
                now + 1,
            ),
        ],
    );
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(dispatch_count(&harness), 1);
    assert_eq!(
        admit_count(&harness),
        1,
        "a durable row is not written twice"
    );

    harness
        .world
        .children
        .lock()
        .expect("children")
        .insert(child, child_stream(child, EndReason::Completed));
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(step_verdict_count(&harness), 1);
}

#[tokio::test]
async fn w6_a_child_mid_turn_is_only_observed() {
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    let child = dispatched_child(&harness);
    let now = harness.world.now_ms();
    harness.world.children.lock().expect("children").insert(
        child,
        vec![
            event(child, 1, EventKind::SessionCreated, json!({}), now),
            event(
                child,
                2,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "do the step"}),
                now + 1,
            ),
            event(
                child,
                3,
                EventKind::ToolCall,
                json!({"call_id": "c1", "tool": "fetch", "args": {}}),
                now + 2,
            ),
        ],
    );
    for _ in 0..2 {
        assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    }
    assert_eq!(dispatch_count(&harness), 1);
    assert_eq!(admit_count(&harness), 1);
    assert_eq!(
        step_verdict_count(&harness),
        0,
        "a running child is not a verdict"
    );

    harness
        .world
        .children
        .lock()
        .expect("children")
        .insert(child, child_stream(child, EndReason::Completed));
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(step_verdict_count(&harness), 1);
}

#[tokio::test]
async fn w7_a_deliver_intent_without_a_receipt_is_never_resent() {
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    let child = dispatched_child(&harness);
    harness
        .world
        .children
        .lock()
        .expect("children")
        .insert(child, child_stream(child, EndReason::Completed));
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(step_verdict_count(&harness), 1);

    // The crash landed after the send intent and before the adapter returned.
    push_event(
        &harness,
        EventKind::ToolCall,
        json!({
            "call_id": "job:deliver:notify:1",
            "tool": "reply",
            "args": {
                "channel": "mock-im",
                "target": {"handle": "owner"},
                "content": {"text": "the digest"},
            },
        }),
    );

    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    let verdicts = status::verdicts(&harness.world.run_events());
    assert_eq!(verdicts.len(), 2);
    assert_eq!(verdicts[1].step, "notify");
    assert_eq!(
        verdicts[1].outcome,
        Some(kian_superagent::job::outcome::AttemptOutcome::NeedsOperator),
        "an unknown send outcome is needs_operator, never a silent retry"
    );
    assert!(
        harness.world.sends.lock().expect("sends").is_empty(),
        "the adapter is not called again"
    );

    match fresh_tick(&harness).await {
        LoopOutcome::SoftFailed { reason } => assert_eq!(reason, JOB_FAILED_REASON),
        other => panic!("the run must fail, not {other:?}"),
    }
    assert_eq!(run_verdict_count(&harness), 1);
    assert_eq!(alert_count(&harness), 1);

    // A re-drive of the ended stream writes nothing new.
    assert!(matches!(
        fresh_tick(&harness).await,
        LoopOutcome::SoftFailed { .. }
    ));
    assert_eq!(run_verdict_count(&harness), 1);
    assert_eq!(alert_count(&harness), 1);
}

#[tokio::test]
async fn w8_committed_verdicts_without_an_end_settle_the_run_once() {
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    let child = dispatched_child(&harness);
    harness
        .world
        .children
        .lock()
        .expect("children")
        .insert(child, child_stream(child, EndReason::Failed));
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(step_verdict_count(&harness), 1);
    assert_eq!(
        run_verdict_count(&harness),
        0,
        "the crash left no run verdict"
    );
    assert_eq!(alert_count(&harness), 0);

    match fresh_tick(&harness).await {
        LoopOutcome::SoftFailed { reason } => assert_eq!(reason, JOB_FAILED_REASON),
        other => panic!("the run must fail, not {other:?}"),
    }
    assert_eq!(run_verdict_count(&harness), 1);
    assert_eq!(alert_count(&harness), 1);

    // The run verdict is the truth a re-drive reads: no second one, no second
    // alert, no rewritten step verdict.
    assert!(matches!(
        fresh_tick(&harness).await,
        LoopOutcome::SoftFailed { .. }
    ));
    assert_eq!(run_verdict_count(&harness), 1);
    assert_eq!(step_verdict_count(&harness), 1);
    assert_eq!(alert_count(&harness), 1);
}

#[tokio::test]
async fn a_settled_step_harvests_its_artifact_from_the_final_message() {
    let harness = harness_for(proposal_plan(), vec![online(WORKER, &["repo.kian"])], None);
    let mut stream = harness.driver.drive(context(&harness));

    let first = next_outcome(&mut stream).await;
    assert!(
        matches!(first, LoopOutcome::TurnDone),
        "first tick: {first:?}"
    );
    let child = dispatched_child(&harness);
    harness
        .world
        .children
        .lock()
        .expect("children")
        .insert(child, proposal_child(child));

    // Tick 2: the child's final message is the declared artifact, so the gate
    // and the harvest read the same committed bytes.
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let events = harness.world.run_events();
    let harvest = events
        .iter()
        .find(|event| {
            event.kind == EventKind::ToolCall && event.bytes["tool"] == status::ARTIFACT_TOOL
        })
        .expect("one artifact intent on the run stream");
    assert_eq!(harvest.bytes["call_id"], "job:artifact:patch:1");
    let artifact = &harvest.bytes["args"]["artifact"];
    assert_eq!(artifact["step"], "patch");
    assert_eq!(artifact["name"], "patch");
    assert_eq!(artifact["media"], "text");
    assert_eq!(artifact["bytes"].as_u64(), Some(PATCH.len() as u64));
    assert_eq!(artifact["sha256"], plan::digest_text(PATCH));
    assert_eq!(artifact["ref"]["session"], child.0.to_string());
    assert_eq!(artifact["ref"]["seq"].as_u64(), Some(2));
    assert_eq!(artifact["producer"]["run"], format!("{JOB}@{SLOT}"));
    assert_eq!(artifact["producer"]["step"], "patch");
    assert_eq!(artifact["producer"]["attempt"].as_u64(), Some(1));

    let result = events
        .iter()
        .find(|event| {
            event.kind == EventKind::ToolResult && event.bytes["call_id"] == "job:artifact:patch:1"
        })
        .expect("the artifact intent is closed");
    assert_eq!(result.bytes["ok"], true);
    assert_eq!(&result.bytes["result"]["artifact"], artifact);

    let verdicts = status::verdicts(&events);
    assert_eq!(verdicts.len(), 1);
    assert_eq!(verdicts[0].step, "patch");
    assert!(verdicts[0].passed, "{verdicts:#?}");

    // Tick 3: the only step is terminal, so the run settles succeeded.
    match next_outcome(&mut stream).await {
        LoopOutcome::Stopped { reason } => assert_eq!(reason, JOB_FINISHED_STOP_REASON),
        other => panic!("the run must stop, not {other:?}"),
    }
    let events = harness.world.run_events();
    let verdict = status::run_verdict(&events).expect("one run verdict");
    assert_eq!(
        verdict.outcome,
        kian_superagent::job::outcome::RunOutcome::Succeeded
    );
    let artifacts = status::artifacts(&events, &proposal_plan());
    assert_eq!(artifacts.len(), 1, "{artifacts:#?}");
    assert_eq!(artifacts[0].name, "patch");
    assert!(artifacts[0].within_cap);
}

#[tokio::test]
async fn a_publish_step_commits_the_artifact_and_settles_from_its_receipt() {
    let harness = harness_for(publish_plan(), vec![online(WORKER, &["repo.kian"])], None);
    let mut stream = harness.driver.drive(context(&harness));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let child = dispatched_child(&harness);
    harness
        .world
        .children
        .lock()
        .expect("the children")
        .insert(child, proposal_child(child));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let finished = next_outcome(&mut stream).await;
    assert!(
        matches!(finished, LoopOutcome::Stopped { .. }),
        "the run finished: {finished:?}"
    );

    let events = harness.world.run_events();
    let branch = format!("selfdev/{}", harness.run.0);
    let intent = events
        .iter()
        .find(|event| {
            event.kind == EventKind::ToolCall
                && event.bytes.get("tool").and_then(Value::as_str) == Some("job.publish")
        })
        .expect("a publish intent");
    assert_eq!(intent.bytes["call_id"], json!(PUBLISH_CALL));
    assert_eq!(intent.bytes["args"]["step"], json!("publish"));
    assert_eq!(intent.bytes["args"]["attempt"], json!(1));
    assert_eq!(intent.bytes["args"]["branch"], json!(branch));
    assert_eq!(intent.bytes["args"]["base_commit"], json!("0".repeat(40)));
    assert_eq!(
        intent.bytes["args"]["worktree"],
        json!(format!(
            "/tmp/kian/.agents/worktrees/selfdev-{}",
            harness.run.0
        ))
    );
    assert_eq!(
        intent.bytes["args"]["from"]["sha256"],
        json!(plan::digest_text(PATCH))
    );

    let receipt = events
        .iter()
        .find(|event| {
            event.kind == EventKind::ToolResult
                && event.bytes.get("call_id").and_then(Value::as_str) == Some(PUBLISH_CALL)
        })
        .expect("a publish receipt");
    assert_eq!(receipt.bytes["ok"], json!(true));
    assert_eq!(receipt.bytes["result"]["commit"], json!(PUBLISH_COMMIT));
    assert_eq!(receipt.bytes["result"]["gate"]["exit_code"], json!(0));

    let requests = harness
        .world
        .publish_requests
        .lock()
        .expect("the publish requests")
        .clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].patch, PATCH);
    assert_eq!(
        requests[0]
            .trailers
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>(),
        vec![
            "Kian-Run",
            "Kian-Plan-Digest",
            "Kian-Patch-Sha256",
            "Kian-Pack"
        ]
    );

    let verdict = status::verdicts(&events)
        .into_iter()
        .find(|verdict| verdict.step == "publish")
        .expect("a publish verdict");
    assert!(verdict.passed);
    assert_eq!(verdict.outcome, Some(AttemptOutcome::Succeeded));
    assert_eq!(status::publish_receipts(&events).len(), 1);
}

#[tokio::test]
async fn a_publish_intent_without_a_receipt_waits_for_an_operator() {
    let harness = harness_for(publish_plan(), vec![online(WORKER, &["repo.kian"])], None);
    let mut stream = harness.driver.drive(context(&harness));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let child = dispatched_child(&harness);
    harness
        .world
        .children
        .lock()
        .expect("the children")
        .insert(child, proposal_child(child));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    {
        let mut log = harness.world.run.lock().expect("the run log");
        log.retain(|event| {
            let publish_result = event.kind == EventKind::ToolResult
                && event.bytes.get("call_id").and_then(Value::as_str) == Some(PUBLISH_CALL);
            let publish_verdict = event.kind == EventKind::EvidenceVerdict
                && event.bytes.get("gate").and_then(Value::as_str) == Some("step:publish");
            !publish_result && !publish_verdict
        });
    }
    let mut restarted = harness.new_driver().drive(context(&harness));
    assert!(matches!(
        next_outcome(&mut restarted).await,
        LoopOutcome::TurnDone
    ));
    let events = harness.world.run_events();
    let verdict = status::verdicts(&events)
        .into_iter()
        .find(|verdict| verdict.step == "publish")
        .expect("a re-derived publish verdict");
    assert_eq!(verdict.outcome, Some(AttemptOutcome::NeedsOperator));
    assert!(!verdict.passed);
}

#[tokio::test]
async fn an_occupied_worktree_needs_an_operator_and_is_never_retried() {
    let harness = harness_for(publish_plan(), vec![online(WORKER, &["repo.kian"])], None);
    *harness
        .world
        .publish_behavior
        .lock()
        .expect("the publish behavior") = PublishBehavior::Occupied;
    let mut stream = harness.driver.drive(context(&harness));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let child = dispatched_child(&harness);
    harness
        .world
        .children
        .lock()
        .expect("the children")
        .insert(child, proposal_child(child));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let finished = next_outcome(&mut stream).await;
    assert!(
        matches!(finished, LoopOutcome::SoftFailed { .. }),
        "a needs_operator step fails the run: {finished:?}"
    );
    let events = harness.world.run_events();
    let verdict = status::verdicts(&events)
        .into_iter()
        .find(|verdict| verdict.step == "publish")
        .expect("a publish verdict");
    assert_eq!(verdict.outcome, Some(AttemptOutcome::NeedsOperator));
    assert_eq!(
        harness
            .world
            .publish_requests
            .lock()
            .expect("the publish requests")
            .len(),
        1,
        "a publish is never retried"
    );
}

#[test]
fn publish_ok_judges_the_receipt_not_the_intent() {
    let branch = "selfdev/abc";
    let good = json!({
        "ok": true,
        "result": {
            "branch": branch,
            "worktree": "/tmp/w",
            "commit": PUBLISH_COMMIT,
            "gate": {"command": "g", "exit_code": 0, "digest": "sha256:00", "output_tail": ""}
        }
    });
    assert!(check_publish_ok(&good, branch, true).passed);

    let short = json!({
        "ok": true,
        "result": {"branch": branch, "worktree": "/tmp/w", "commit": "0123",
                   "gate": {"exit_code": 0}}
    });
    assert!(!check_publish_ok(&short, branch, true).passed);

    let foreign = json!({
        "ok": true,
        "result": {"branch": "main", "worktree": "/tmp/w", "commit": PUBLISH_COMMIT,
                   "gate": {"exit_code": 0}}
    });
    assert!(!check_publish_ok(&foreign, branch, true).passed);

    let failed_gate = json!({
        "ok": true,
        "result": {"branch": branch, "worktree": "/tmp/w", "commit": PUBLISH_COMMIT,
                   "gate": {"exit_code": 1}}
    });
    assert!(!check_publish_ok(&failed_gate, branch, true).passed);
    assert!(
        check_publish_ok(&failed_gate, branch, false).passed,
        "a plan that runs no gate is judged without one"
    );

    let refused = json!({"ok": false, "error": "the patch does not apply: nope"});
    let outcome = check_publish_ok(&refused, branch, true);
    assert!(!outcome.passed);
    assert!(
        outcome
            .detail
            .unwrap_or_default()
            .contains("does not apply")
    );
}
