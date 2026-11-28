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
use std::sync::atomic::{AtomicBool, Ordering};
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
    PublishTargets, PullRequestReceipt, PushReceipt, RunPort, check_publish_ok,
};
use kian_superagent::job::outcome::AttemptOutcome;
use kian_superagent::job::plan::{self, JobPlan};
use kian_superagent::job::status;
use kian_superagent::worker::{
    BindingAdmissionPort, BusyOwner, WorkerStreamsError, WorkerStreamsPort,
};
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
    inputs: Mutex<Vec<Value>>,
    interrupt_requested: AtomicBool,
    emit_failures: Mutex<BTreeMap<String, usize>>,
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
            inputs: Mutex::new(Vec::new()),
            interrupt_requested: AtomicBool::new(false),
            emit_failures: Mutex::new(BTreeMap::new()),
            clock: Mutex::new(1_700_000_000_000),
        })
    }

    fn now_ms(&self) -> u64 {
        *self.clock.lock().expect("the fake clock")
    }

    fn run_events(&self) -> Vec<Event> {
        self.run.lock().expect("the run log").clone()
    }

    fn queue_input(&self, payload: Value) {
        self.inputs.lock().expect("the queued inputs").push(payload);
    }

    fn fail_next_emit(&self, key: String) {
        *self
            .emit_failures
            .lock()
            .expect("the emit failures")
            .entry(key)
            .or_insert(0) += 1;
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
            let key = emit_key(&kind, &bytes);
            let mut failures = world
                .emit_failures
                .lock()
                .map_err(|error| error.to_string())?;
            if let Some(remaining) = failures.get_mut(&key)
                && *remaining > 0
            {
                *remaining -= 1;
                return Err(format!("injected {key} append failure"));
            }
            drop(failures);
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
        self.world.interrupt_requested.load(Ordering::Acquire)
    }

    fn take_inputs(&self) -> BoxFuture<'static, Result<Vec<Value>, String>> {
        let world = Arc::clone(&self.world);
        Box::pin(async move {
            let rows =
                std::mem::take(&mut *world.inputs.lock().map_err(|error| error.to_string())?);
            if rows.iter().any(|row| {
                row.get("content")
                    .and_then(Value::as_str)
                    .is_some_and(|content| content.trim().eq_ignore_ascii_case("cancel"))
            }) {
                world.interrupt_requested.store(true, Ordering::Release);
            }
            Ok(rows)
        })
    }
}

fn emit_key(kind: &EventKind, bytes: &Value) -> String {
    let call = bytes
        .get("call_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    format!("{kind:?}:{call}")
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

struct AlwaysBound;

impl BindingAdmissionPort for AlwaysBound {
    fn check_binding(
        &self,
        _worker_id: &str,
        _candidate_thread: Uuid,
        _candidate_session: SessionId,
        _now_ms: u64,
        _ttl_ms: u64,
    ) -> BoxFuture<'static, Result<Option<BusyOwner>, WorkerStreamsError>> {
        Box::pin(async move {
            Ok(Some(BusyOwner {
                thread_id: Uuid::from_u128(900),
                worker_session: SessionId(Uuid::from_u128(901)),
                reason: "live worker session",
            }))
        })
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
                PublishBehavior::Commit => {
                    let push = request.push.then(|| PushReceipt {
                        remote: "origin".to_string(),
                        reference: format!("refs/heads/{}", request.branch),
                    });
                    let pull_request = request.pull_request.then(|| PullRequestReceipt {
                        base: request.base.clone(),
                        head: request.branch.clone(),
                        url: Some("https://example.test/pr/1".to_string()),
                    });
                    Ok(PublishReceipt {
                        commit: PUBLISH_COMMIT.to_string(),
                        branch: request.branch,
                        worktree: request.worktree,
                        gate: request.gates.then(|| GateReceipt {
                            command: "./scripts/selfcheck.sh".to_string(),
                            exit_code: 0,
                            digest: "sha256:00".to_string(),
                            output_tail: String::new(),
                        }),
                        push,
                        pull_request,
                    })
                }
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
            "publish": {"base": "main", "gates": true, "push": true, "pull_request": true},
            "effect": "write",
            "verify": [{"check": "publish_ok"}]
        }]
    }))
    .expect("a valid publish plan")
}

fn optional_agent_plan(accept_unverified: bool) -> JobPlan {
    serde_json::from_value(json!({
        "id": JOB,
        "limits": {
            "accept_unverified": if accept_unverified { vec!["fetch"] } else { Vec::<&str>::new() },
            "deadline_ms": 60_000
        },
        "steps": [{
            "id": "fetch",
            "prompt": "fetch",
            "optional": true,
            "on_failure": "continue",
            "verify": if accept_unverified {
                Vec::<Value>::new()
            } else {
                vec![json!({"check": "final_message_present"})]
            }
        }]
    }))
    .expect("a valid optional plan")
}

fn retrying_agent_plan(timeout_ms: u64, defer_budget_ms: u64) -> JobPlan {
    serde_json::from_value(json!({
        "id": JOB,
        "limits": {
            "deadline_ms": 20_000,
            "defer_budget_ms": defer_budget_ms
        },
        "steps": [{
            "id": "fetch",
            "prompt": "fetch",
            "timeout_ms": timeout_ms,
            "retries": 1,
            "verify": [{"check": "final_message_present"}]
        }]
    }))
    .expect("a valid retrying plan")
}

fn deliver_only_plan(optional: bool) -> JobPlan {
    serde_json::from_value(json!({
        "id": JOB,
        "limits": {"deadline_ms": 60_000},
        "steps": [{
            "id": "notify",
            "action": "deliver",
            "optional": optional,
            "on_failure": if optional { "continue" } else { "halt" },
            "channel": "mock-im",
            "target": {"handle": "owner"},
            "content": {"text": "hello"},
            "verify": [{
                "check": "delivery_receipt",
                "channel": "mock-im",
                "target": {"handle": "owner"}
            }]
        }]
    }))
    .expect("a valid deliver-only plan")
}

fn deadline_agent_plan(effect: &str) -> JobPlan {
    serde_json::from_value(json!({
        "id": JOB,
        "limits": {"deadline_ms": 1_000},
        "steps": [{
            "id": "fetch",
            "prompt": "fetch",
            "timeout_ms": 5_000,
            "effect": effect,
            "verify": [{"check": "final_message_present"}]
        }]
    }))
    .expect("a valid deadline plan")
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
    let task_bytes = serde_json::to_string(&admits[0].1).expect("task bytes");
    assert!(!task_bytes.contains(&format!("{JOB}@{SLOT}")));
    assert!(!task_bytes.contains(&format!("\"{JOB}\"")));
    assert!(!task_bytes.contains(&digest_plan().digest()));
    assert!(!task_bytes.contains(&digest_plan().steps[0].digest()));

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

    // Tick 2: the child's final message is committed as the artifact call.
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
    let artifact = harvest.bytes["args"]["artifact"].clone();
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

    assert!(
        !events.iter().any(|event| {
            event.kind == EventKind::ToolResult && event.bytes["call_id"] == "job:artifact:patch:1"
        }),
        "the call is committed before its result"
    );

    // Tick 3 closes the pair.
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let events = harness.world.run_events();
    let result = events
        .iter()
        .find(|event| {
            event.kind == EventKind::ToolResult && event.bytes["call_id"] == "job:artifact:patch:1"
        })
        .expect("the artifact intent is closed");
    assert_eq!(result.bytes["ok"], true);
    assert_eq!(result.bytes["result"]["artifact"], artifact);

    // Tick 4 settles the attempt only after the pair exists.
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let events = harness.world.run_events();
    let verdicts = status::verdicts(&events);
    assert_eq!(verdicts.len(), 1);
    assert_eq!(verdicts[0].step, "patch");
    assert!(verdicts[0].passed, "{verdicts:#?}");

    // Tick 5: the only step is terminal, so the run settles succeeded.
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
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.kind == EventKind::ToolCall
                    && event.bytes.get("call_id").and_then(Value::as_str)
                        == Some("job:artifact:patch:1")
            })
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.kind == EventKind::ToolResult
                    && event.bytes.get("call_id").and_then(Value::as_str)
                        == Some("job:artifact:patch:1")
            })
            .count(),
        1
    );
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
    assert_eq!(
        receipt.bytes["result"]["push"]["ref"],
        json!(format!("refs/heads/{branch}"))
    );
    assert_eq!(
        receipt.bytes["result"]["pull_request"]["head"],
        json!(branch)
    );
    assert_eq!(
        receipt.bytes["result"]["pull_request"]["url"],
        json!("https://example.test/pr/1")
    );

    let requests = harness
        .world
        .publish_requests
        .lock()
        .expect("the publish requests")
        .clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].patch, PATCH);
    assert!(requests[0].push, "the plan asked for the push");
    assert!(
        requests[0].pull_request,
        "the plan asked for the pull request"
    );
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
    let view = status::view(&events).expect("the run's view");
    let rendered = status::render(&view);
    assert!(rendered.contains("pushed"), "{rendered}");
    assert!(
        rendered.contains("pr https://example.test/pr/1"),
        "{rendered}"
    );
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

#[tokio::test]
async fn cancel_before_dispatch_writes_one_cancelled_verdict_and_no_child() {
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
    harness
        .world
        .queue_input(json!({"role": "user", "content": "cancel"}));
    let mut stream = harness.driver.drive(context(&harness));

    match next_outcome(&mut stream).await {
        LoopOutcome::Stopped { reason } => {
            assert_eq!(reason, kian_core::seams::TERMINAL_CANCEL_STOP_REASON);
        }
        other => panic!("cancel must be terminal, not {other:?}"),
    }
    let events = harness.world.run_events();
    assert_eq!(run_verdict_count(&harness), 1);
    let verdict = status::run_verdict(&events).expect("a cancelled run verdict");
    assert_eq!(
        verdict.outcome,
        kian_superagent::job::outcome::RunOutcome::Cancelled
    );
    assert_eq!(verdict.payload["job"]["reason"], "cancelled");
    assert_eq!(verdict.payload["job"]["steps"].as_array().unwrap().len(), 2);
    assert!(
        verdict.payload["job"]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .all(|step| step["outcome"] == "skipped")
    );
    assert_eq!(dispatch_count(&harness), 0);
    assert_eq!(admit_count(&harness), 0);
    assert!(alert_count(&harness) == 0);

    harness
        .world
        .queue_input(json!({"role": "user", "content": "cancel"}));
    assert!(matches!(
        fresh_tick(&harness).await,
        LoopOutcome::Stopped { .. }
    ));
    assert_eq!(
        run_verdict_count(&harness),
        1,
        "duplicate cancel is a no-op"
    );
}

#[tokio::test]
async fn cancel_detects_the_engine_wrapped_message_shape() {
    // The engine promotes an arbitrary input payload as the *content* of a
    // user message, so a real cancel row reaches the conductor as
    // {"role": "user", "content": {"role": "user", "content": "cancel"}}.
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
    harness.world.queue_input(json!({
        "role": "user",
        "content": {"role": "user", "content": "cancel"},
    }));
    let mut stream = harness.driver.drive(context(&harness));
    match next_outcome(&mut stream).await {
        LoopOutcome::Stopped { reason } => {
            assert_eq!(reason, kian_core::seams::TERMINAL_CANCEL_STOP_REASON);
        }
        other => panic!("cancel must be terminal, not {other:?}"),
    }
    let verdict = status::run_verdict(&harness.world.run_events()).expect("a cancelled verdict");
    assert_eq!(
        verdict.outcome,
        kian_superagent::job::outcome::RunOutcome::Cancelled
    );
    assert_eq!(dispatch_count(&harness), 0);
}

#[tokio::test]
async fn cancel_wins_over_a_completed_child_that_was_not_yet_observed() {
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
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
    harness
        .world
        .queue_input(json!({"role": "user", "content": "cancel"}));

    match next_outcome(&mut stream).await {
        LoopOutcome::Stopped { reason } => {
            assert_eq!(reason, kian_core::seams::TERMINAL_CANCEL_STOP_REASON);
        }
        other => panic!("cancel must win the race, not {other:?}"),
    }
    let events = harness.world.run_events();
    let step = verdict(&events, "step:fetch").expect("the abandoned attempt verdict");
    assert_eq!(step.bytes["job"]["outcome"], "interrupted");
    assert_eq!(step.bytes["checks"][0]["name"], "cancelled");
    let run = status::run_verdict(&events).expect("run verdict");
    assert_eq!(
        run.outcome,
        kian_superagent::job::outcome::RunOutcome::Cancelled
    );
    assert_eq!(harness.world.sends.lock().expect("sends").len(), 0);
    assert_eq!(
        dispatch_count(&harness),
        1,
        "cancel dispatches no second step"
    );
}

fn verdict<'a>(events: &'a [Event], gate: &str) -> Option<&'a Event> {
    events.iter().find(|event| {
        event.kind == EventKind::EvidenceVerdict
            && event.bytes.get("gate").and_then(Value::as_str) == Some(gate)
    })
}

#[tokio::test]
async fn a_terminal_end_without_a_verdict_is_failed_not_completed() {
    let harness = harness(Vec::new(), None);
    push_event(
        &harness,
        EventKind::SessionEnded {
            reason: EndReason::Completed,
        },
        json!({}),
    );
    assert!(matches!(
        fresh_tick(&harness).await,
        LoopOutcome::SoftFailed { .. }
    ));
    assert_eq!(
        status::ended(&harness.world.run_events()),
        Some(kian_superagent::job::outcome::RunOutcome::Failed)
    );
}

#[tokio::test]
async fn a_second_job_marked_input_is_invalid_and_marks_every_step_skipped() {
    let harness = harness(Vec::new(), None);
    let plan = digest_plan();
    push_event(
        &harness,
        EventKind::InputAdmitted,
        json!({
            "role": "user",
            "content": "steer",
            "job": status::marker_payload(JOB, SLOT, "manual", &plan),
        }),
    );
    assert!(matches!(
        fresh_tick(&harness).await,
        LoopOutcome::SoftFailed { .. }
    ));
    let events = harness.world.run_events();
    let run = status::run_verdict(&events).expect("the invalid-input verdict");
    assert_eq!(run.payload["job"]["reason"], "invalid_job_input");
    assert_eq!(
        run.payload["job"]["steps"],
        json!([
            {"step": "fetch", "outcome": "skipped"},
            {"step": "notify", "outcome": "skipped"}
        ])
    );
    assert_eq!(dispatch_count(&harness), 0);
}

#[tokio::test]
async fn a_marker_not_on_the_first_input_is_invalid() {
    let harness = harness(Vec::new(), None);
    harness.world.run.lock().expect("the run log").clear();
    let now = harness.world.now_ms();
    harness.world.run.lock().expect("the run log").extend([
        event(
            harness.run,
            1,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "plain first"}),
            now,
        ),
        event(
            harness.run,
            2,
            EventKind::InputAdmitted,
            json!({
                "role": "user",
                "content": "late marker",
                "job": status::marker_payload(JOB, SLOT, "manual", &digest_plan()),
            }),
            now + 1,
        ),
    ]);

    assert!(matches!(
        fresh_tick(&harness).await,
        LoopOutcome::SoftFailed { .. }
    ));
    let run = status::run_verdict(&harness.world.run_events()).expect("run verdict");
    assert_eq!(run.payload["job"]["reason"], "invalid_job_input");
    assert_eq!(run.payload["job"]["steps"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn optional_failure_keeps_a_run_successful() {
    let harness = harness_for(
        optional_agent_plan(false),
        vec![online(WORKER, &["net.unrestricted"])],
        None,
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
        .insert(child, child_stream(child, EndReason::Failed));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    match next_outcome(&mut stream).await {
        LoopOutcome::Stopped { reason } => assert_eq!(reason, JOB_FINISHED_STOP_REASON),
        other => panic!("optional failure must not fail the run: {other:?}"),
    }
    let verdict = status::run_verdict(&harness.world.run_events()).expect("run verdict");
    assert_eq!(
        verdict.outcome,
        kian_superagent::job::outcome::RunOutcome::Succeeded
    );
}

#[tokio::test]
async fn optional_unverified_makes_the_run_unverified() {
    let harness = harness_for(
        optional_agent_plan(true),
        vec![online(WORKER, &["net.unrestricted"])],
        None,
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
        LoopOutcome::Stopped { .. }
    ));
    let verdict = status::run_verdict(&harness.world.run_events()).expect("run verdict");
    assert_eq!(
        verdict.outcome,
        kian_superagent::job::outcome::RunOutcome::Unverified
    );
    assert_eq!(alert_count(&harness), 0, "unverified runs never alert");
}

#[tokio::test]
async fn optional_needs_operator_fails_the_run_with_effect_unknown() {
    let harness = harness_for(
        deliver_only_plan(true),
        Vec::new(),
        Some(ChannelError::Timeout),
    );
    let mut stream = harness.driver.drive(context(&harness));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::SoftFailed { .. }
    ));
    let run = status::run_verdict(&harness.world.run_events()).expect("run verdict");
    assert_eq!(run.payload["job"]["outcome"], "failed");
    assert_eq!(run.payload["job"]["reason"], "effect_unknown");
    assert_eq!(run.payload["job"]["failed_step"], "notify");
}

#[tokio::test]
async fn a_retry_can_reuse_the_only_worker_after_its_child_releases() {
    let harness = harness_for(
        retrying_agent_plan(10_000, 100),
        vec![online(WORKER, &["net.unrestricted"])],
        None,
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
        .insert(child, child_stream(child, EndReason::Failed));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let settled = status::verdicts(&harness.world.run_events())[0].at_ms;
    *harness.world.clock.lock().expect("clock") = settled + 2_000;
    let mut released = child_stream(child, EndReason::Failed);
    released.push(event(
        child,
        6,
        EventKind::SessionEnded {
            reason: EndReason::Cancelled,
        },
        json!({}),
        settled + 1,
    ));
    harness
        .world
        .children
        .lock()
        .expect("children")
        .insert(child, released);

    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let dispatches = status::dispatches(&harness.world.run_events());
    assert_eq!(dispatches.len(), 2, "{dispatches:#?}");
    assert_eq!(dispatches[1].worker, dispatches[0].worker);
}

#[tokio::test]
async fn all_busy_defers_without_a_write_then_settles_no_candidate() {
    let harness = harness_for(
        retrying_agent_plan(10, 50),
        vec![online(WORKER, &["net.unrestricted"])],
        None,
    );
    let mut stream = harness.driver.drive(context(&harness));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let child = dispatched_child(&harness);
    let started = harness.world.now_ms();
    harness.world.children.lock().expect("children").insert(
        child,
        vec![
            event(child, 1, EventKind::SessionCreated, json!({}), started),
            event(
                child,
                2,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "live"}),
                started,
            ),
            event(
                child,
                3,
                EventKind::ToolCall,
                json!({"call_id": "c1", "tool": "read", "args": {}}),
                started + 1,
            ),
        ],
    );
    *harness.world.clock.lock().expect("clock") = started + 10;
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let timeout = status::verdicts(&harness.world.run_events())[0].at_ms;
    assert_eq!(
        status::verdicts(&harness.world.run_events())[0].outcome,
        Some(AttemptOutcome::TimedOut)
    );

    *harness.world.clock.lock().expect("clock") = timeout + 2_025;
    let before = harness.world.run_events().len();
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    assert_eq!(
        harness.world.run_events().len(),
        before,
        "a deferred placement writes nothing"
    );

    *harness.world.clock.lock().expect("clock") = timeout + 2_075;
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let verdicts = status::verdicts(&harness.world.run_events());
    assert_eq!(verdicts.len(), 2);
    assert_eq!(verdicts[1].outcome, Some(AttemptOutcome::Failed));
    let event = harness
        .world
        .run_events()
        .into_iter()
        .rfind(|event| {
            event.kind == EventKind::EvidenceVerdict
                && event.bytes.get("gate").and_then(Value::as_str) == Some("step:fetch")
        })
        .expect("the no-candidate verdict");
    assert_eq!(event.bytes["checks"][0]["name"], "no_candidate");
    assert!(
        event.bytes["checks"][0]["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("all_busy"))
    );
    assert_eq!(dispatch_count(&harness), 1);
}

#[tokio::test]
async fn the_deadline_starts_at_the_first_job_input_not_session_creation() {
    let harness = harness_for(
        deadline_agent_plan("read"),
        vec![online(WORKER, &["net.unrestricted"])],
        None,
    );
    let now = harness.world.now_ms();
    {
        let mut events = harness.world.run.lock().expect("run log");
        let input = events.pop().expect("the admitted run");
        events.push(event(
            harness.run,
            1,
            EventKind::SessionCreated,
            json!({}),
            now - 10_000,
        ));
        let mut input = input;
        input.seq = 2;
        input.ts = now;
        events.push(input);
    }
    *harness.world.clock.lock().expect("clock") = now + 999;
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    let child = dispatched_child(&harness);
    assert_eq!(dispatch_count(&harness), 1);
    harness.world.children.lock().expect("children").insert(
        child,
        vec![event(
            child,
            1,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "live"}),
            now + 999,
        )],
    );
    *harness.world.clock.lock().expect("clock") = now + 1_000;
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(
        status::verdicts(&harness.world.run_events())[0].outcome,
        Some(AttemptOutcome::TimedOut)
    );
    assert!(matches!(
        fresh_tick(&harness).await,
        LoopOutcome::SoftFailed { .. }
    ));
    let run = status::run_verdict(&harness.world.run_events()).expect("run verdict");
    assert_eq!(run.payload["job"]["reason"], "deadline_exceeded");
}

#[tokio::test]
async fn a_deadline_with_a_possible_write_effect_is_effect_unknown() {
    let harness = harness_for(
        deadline_agent_plan("write"),
        vec![online(WORKER, &["net.unrestricted"])],
        None,
    );
    let input_ts = harness.world.now_ms();
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    let child = dispatched_child(&harness);
    harness.world.children.lock().expect("children").insert(
        child,
        vec![
            event(
                child,
                1,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "live"}),
                input_ts,
            ),
            event(
                child,
                2,
                EventKind::ToolCall,
                json!({"call_id": "c1", "tool": "write", "args": {}}),
                input_ts + 1,
            ),
        ],
    );
    *harness.world.clock.lock().expect("clock") = input_ts + 1_000;
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(
        status::verdicts(&harness.world.run_events())[0].outcome,
        Some(AttemptOutcome::NeedsOperator)
    );
    assert!(matches!(
        fresh_tick(&harness).await,
        LoopOutcome::SoftFailed { .. }
    ));
    let run = status::run_verdict(&harness.world.run_events()).expect("run verdict");
    assert_eq!(run.payload["job"]["reason"], "effect_unknown");
    assert_eq!(run.payload["job"]["failed_step"], "fetch");
}

#[tokio::test]
async fn a_typed_spawn_binding_refusal_is_recorded_as_binding_conflict() {
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
    harness
        .deps
        .spawner
        .install_binding_admission(Arc::new(AlwaysBound), BTreeMap::new());
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    let events = harness.world.run_events();
    let step = verdict(&events, "step:fetch").expect("a refused dispatch verdict");
    assert_eq!(step.bytes["checks"][0]["name"], "binding_conflict");
    assert_eq!(step.bytes["job"]["outcome"], "failed");
}

#[tokio::test]
async fn a_changed_dispatch_identity_fails_with_child_identity_conflict() {
    let harness = harness(vec![online(WORKER, &["net.unrestricted"])], None);
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    {
        let mut events = harness.world.run.lock().expect("run log");
        let started = events
            .iter_mut()
            .find(|event| event.kind == EventKind::ThreadStarted)
            .expect("dispatch record");
        started.bytes["task"] = json!({"role": "user", "content": "different task"});
    }
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    let events = harness.world.run_events();
    let step = verdict(&events, "step:fetch").expect("identity conflict verdict");
    assert_eq!(step.bytes["checks"][0]["name"], "child_identity_conflict");
    assert!(matches!(
        fresh_tick(&harness).await,
        LoopOutcome::SoftFailed { .. }
    ));
    let run = status::run_verdict(&harness.world.run_events()).expect("run verdict");
    assert_eq!(run.payload["job"]["reason"], "child_identity_conflict");
}

#[tokio::test]
async fn a_receipt_without_its_result_is_repaired_without_resending() {
    let harness = harness_for(deliver_only_plan(false), Vec::new(), None);
    let call_id = "job:deliver:notify:1";
    push_event(
        &harness,
        EventKind::ToolCall,
        json!({
            "tool": "reply",
            "call_id": call_id,
            "args": {
                "channel": "mock-im",
                "target": {"handle": "owner"},
                "content": {"text": "hello"}
            }
        }),
    );
    push_event(
        &harness,
        EventKind::ChannelDelivered,
        json!({
            "call_id": call_id,
            "attempt": 1,
            "channel": "mock-im",
            "target": {"handle": "owner"},
            "policy": "job",
            "parts_sent": 1,
            "parts_total": 1,
            "truncated": false,
            "detail": {"message_id": "already-sent"}
        }),
    );
    let mut stream = harness.driver.drive(context(&harness));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    assert!(
        harness.world.sends.lock().expect("sends").is_empty(),
        "a receipt is a commit point; it is never resent"
    );
    let events = harness.world.run_events();
    assert!(events.iter().any(|event| {
        event.kind == EventKind::ToolResult
            && event.bytes.get("call_id").and_then(Value::as_str) == Some(call_id)
            && event.bytes.get("ok").and_then(Value::as_bool) == Some(true)
    }));
    assert_eq!(
        status::verdicts(&events)[0].outcome,
        Some(AttemptOutcome::Succeeded)
    );
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::Stopped { .. }
    ));
}

#[tokio::test]
async fn delivery_receipts_must_correlate_with_the_attempt_and_be_complete() {
    let call_id = "job:deliver:notify:1";
    let cases = [
        (
            "wrong call id",
            json!({
                "call_id": "job:deliver:notify:2",
                "attempt": 1,
                "channel": "mock-im",
                "target": {"handle": "owner"},
                "parts_sent": 1,
                "parts_total": 1
            }),
        ),
        (
            "wrong attempt",
            json!({
                "call_id": call_id,
                "attempt": 2,
                "channel": "mock-im",
                "target": {"handle": "owner"},
                "parts_sent": 1,
                "parts_total": 1
            }),
        ),
        (
            "wrong target",
            json!({
                "call_id": call_id,
                "attempt": 1,
                "channel": "mock-im",
                "target": {"handle": "someone-else"},
                "parts_sent": 1,
                "parts_total": 1
            }),
        ),
        (
            "partial receipt",
            json!({
                "call_id": call_id,
                "attempt": 1,
                "channel": "mock-im",
                "target": {"handle": "owner"},
                "parts_sent": 1,
                "parts_total": 2
            }),
        ),
    ];

    for (label, receipt) in cases {
        let harness = harness_for(deliver_only_plan(false), Vec::new(), None);
        push_event(
            &harness,
            EventKind::ToolCall,
            json!({
                "tool": "reply",
                "call_id": call_id,
                "args": {
                    "channel": "mock-im",
                    "target": {"handle": "owner"},
                    "content": {"text": "hello"}
                }
            }),
        );
        push_event(&harness, EventKind::ChannelDelivered, receipt);
        assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
        let verdicts = status::verdicts(&harness.world.run_events());
        assert_eq!(verdicts.len(), 1, "{label}");
        assert!(!verdicts[0].passed, "{label}");
        assert!(
            harness.world.sends.lock().expect("sends").is_empty(),
            "{label}: a non-correlated receipt never triggers a send"
        );
        assert!(
            matches!(fresh_tick(&harness).await, LoopOutcome::SoftFailed { .. }),
            "{label}"
        );
    }
}

#[tokio::test]
async fn an_ok_result_without_a_receipt_is_not_delivery_evidence() {
    let harness = harness_for(deliver_only_plan(false), Vec::new(), None);
    let call_id = "job:deliver:notify:1";
    push_event(
        &harness,
        EventKind::ToolResult,
        json!({
            "call_id": call_id,
            "ok": true,
            "result": {"message_id": "unproven"}
        }),
    );
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(
        status::verdicts(&harness.world.run_events())[0].outcome,
        Some(AttemptOutcome::NeedsOperator)
    );
    assert!(harness.world.sends.lock().expect("sends").is_empty());
}

#[tokio::test]
async fn a_failed_result_after_a_receipt_invalidates_the_delivery() {
    let harness = harness_for(deliver_only_plan(false), Vec::new(), None);
    let call_id = "job:deliver:notify:1";
    push_event(
        &harness,
        EventKind::ToolCall,
        json!({
            "tool": "reply",
            "call_id": call_id,
            "args": {
                "channel": "mock-im",
                "target": {"handle": "owner"},
                "content": {"text": "hello"}
            }
        }),
    );
    push_event(
        &harness,
        EventKind::ChannelDelivered,
        json!({
            "call_id": call_id,
            "attempt": 1,
            "channel": "mock-im",
            "target": {"handle": "owner"},
            "parts_sent": 1,
            "parts_total": 1
        }),
    );
    push_event(
        &harness,
        EventKind::ToolResult,
        json!({
            "call_id": call_id,
            "ok": false,
            "disposition": "delivery_failed"
        }),
    );
    assert!(matches!(fresh_tick(&harness).await, LoopOutcome::TurnDone));
    assert_eq!(
        status::verdicts(&harness.world.run_events())[0].outcome,
        Some(AttemptOutcome::Failed)
    );
    assert!(harness.world.sends.lock().expect("sends").is_empty());
}

#[tokio::test]
async fn a_receipt_append_failure_never_writes_ok_or_succeeds() {
    let harness = harness_for(deliver_only_plan(false), Vec::new(), None);
    let call_id = "job:deliver:notify:1";
    harness
        .world
        .fail_next_emit(format!("{:?}:{call_id}", EventKind::ChannelDelivered));
    let mut stream = harness.driver.drive(context(&harness));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let events = harness.world.run_events();
    assert!(!events.iter().any(|event| {
        event.kind == EventKind::ChannelDelivered
            || event.kind == EventKind::EvidenceVerdict
            || event.kind == EventKind::ToolResult
                && event.bytes.get("ok").and_then(Value::as_bool) == Some(true)
    }));
    assert_eq!(harness.world.sends.lock().expect("sends").len(), 1);

    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let events = harness.world.run_events();
    let step = verdict(&events, "step:notify").expect("unknown-effect verdict");
    assert_eq!(step.bytes["job"]["outcome"], "needs_operator");
    assert_eq!(harness.world.sends.lock().expect("sends").len(), 1);
}

#[tokio::test]
async fn an_artifact_result_without_a_call_is_paired_then_failed() {
    let harness = harness_for(proposal_plan(), vec![online(WORKER, &["repo.kian"])], None);
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
        .insert(child, proposal_child(child));
    let orphan = json!({
        "step": "patch",
        "name": "patch",
        "media": "text",
        "bytes": PATCH.len(),
        "sha256": plan::digest_text(PATCH),
        "ref": {"session": child.0.to_string(), "seq": 2},
        "producer": {"run": format!("{JOB}@{SLOT}"), "step": "patch", "attempt": 1}
    });
    push_event(
        &harness,
        EventKind::ToolResult,
        json!({
            "call_id": "job:artifact:patch:1",
            "ok": true,
            "result": {"artifact": orphan}
        }),
    );

    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let events = harness.world.run_events();
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.kind == EventKind::ToolCall
                    && event.bytes.get("call_id").and_then(Value::as_str)
                        == Some("job:artifact:patch:1")
            })
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.kind == EventKind::ToolResult
                    && event.bytes.get("call_id").and_then(Value::as_str)
                        == Some("job:artifact:patch:1")
            })
            .count(),
        1
    );
    assert_eq!(status::verdicts(&events).len(), 0);

    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let events = harness.world.run_events();
    let step = verdict(&events, "step:patch").expect("artifact verdict");
    assert_eq!(step.bytes["job"]["outcome"], "failed");
    assert!(
        step.bytes["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| { check["name"] == "artifact_unavailable" && check["passed"] == false })
    );
}

#[tokio::test]
async fn artifact_bytes_are_verified_and_a_corrupt_pair_is_recorded_once() {
    let harness = harness_for(proposal_plan(), vec![online(WORKER, &["repo.kian"])], None);
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
        .insert(child, proposal_child(child));
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    {
        let mut events = harness.world.run.lock().expect("run log");
        let call = events
            .iter_mut()
            .find(|event| {
                event.kind == EventKind::ToolCall
                    && event.bytes.get("call_id").and_then(Value::as_str)
                        == Some("job:artifact:patch:1")
            })
            .expect("artifact call");
        call.bytes["args"]["artifact"]["sha256"] = Value::String(
            "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string(),
        );
    }

    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    assert_eq!(status::verdicts(&harness.world.run_events()).len(), 0);
    assert!(matches!(
        next_outcome(&mut stream).await,
        LoopOutcome::TurnDone
    ));
    let events = harness.world.run_events();
    let step = verdict(&events, "step:patch").expect("corrupt artifact verdict");
    assert_eq!(step.bytes["job"]["outcome"], "failed");
    assert!(
        step.bytes["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| {
                check["name"] == "artifact_digest_mismatch" && check["passed"] == false
            })
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.kind == EventKind::ToolCall
                    && event.bytes.get("call_id").and_then(Value::as_str)
                        == Some("job:artifact:patch:1")
            })
            .count(),
        1
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
    assert!(check_publish_ok(&good, branch, true, false, false).passed);

    let short = json!({
        "ok": true,
        "result": {"branch": branch, "worktree": "/tmp/w", "commit": "0123",
                   "gate": {"exit_code": 0}}
    });
    assert!(!check_publish_ok(&short, branch, true, false, false).passed);

    let foreign = json!({
        "ok": true,
        "result": {"branch": "main", "worktree": "/tmp/w", "commit": PUBLISH_COMMIT,
                   "gate": {"exit_code": 0}}
    });
    assert!(!check_publish_ok(&foreign, branch, true, false, false).passed);

    let failed_gate = json!({
        "ok": true,
        "result": {"branch": branch, "worktree": "/tmp/w", "commit": PUBLISH_COMMIT,
                   "gate": {"exit_code": 1}}
    });
    assert!(!check_publish_ok(&failed_gate, branch, true, false, false).passed);
    assert!(
        check_publish_ok(&failed_gate, branch, false, false, false).passed,
        "a plan that runs no gate is judged without one"
    );

    let unpushed = check_publish_ok(&good, branch, true, true, false);
    assert!(!unpushed.passed);
    assert!(
        unpushed
            .detail
            .unwrap_or_default()
            .contains("carries no push"),
        "a plan that asks for a push is judged by its push"
    );
    let unopened = check_publish_ok(&good, branch, true, false, true);
    assert!(!unopened.passed);

    let pushed = json!({
        "ok": true,
        "result": {
            "branch": branch,
            "worktree": "/tmp/w",
            "commit": PUBLISH_COMMIT,
            "gate": {"exit_code": 0},
            "push": {"remote": "origin", "ref": format!("refs/heads/{branch}")},
            "pull_request": {"base": "main", "head": branch, "url": "https://example.test/pr/1"}
        }
    });
    assert!(check_publish_ok(&pushed, branch, true, true, true).passed);
    let foreign_ref = json!({
        "ok": true,
        "result": {
            "branch": branch,
            "worktree": "/tmp/w",
            "commit": PUBLISH_COMMIT,
            "gate": {"exit_code": 0},
            "push": {"remote": "origin", "ref": "refs/heads/main"}
        }
    });
    assert!(!check_publish_ok(&foreign_ref, branch, true, true, false).passed);
    let foreign_head = json!({
        "ok": true,
        "result": {
            "branch": branch,
            "worktree": "/tmp/w",
            "commit": PUBLISH_COMMIT,
            "gate": {"exit_code": 0},
            "push": {"remote": "origin", "ref": format!("refs/heads/{branch}")},
            "pull_request": {"base": "main", "head": "main"}
        }
    });
    assert!(!check_publish_ok(&foreign_head, branch, true, true, true).passed);

    let refused = json!({"ok": false, "error": "the patch does not apply: nope"});
    let outcome = check_publish_ok(&refused, branch, true, false, false);
    assert!(!outcome.passed);
    assert!(
        outcome
            .detail
            .unwrap_or_default()
            .contains("does not apply")
    );
}
