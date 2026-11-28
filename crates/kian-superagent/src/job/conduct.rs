//! The conductor: the [`LoopDriver`] of a run session (JOB-MODEL §5.3).
//!
//! A run is an ordinary session whose first input carries the additive
//! `job` marker. This driver reads that marker, derives every fact it needs
//! from the run's own committed stream plus its children's streams, and
//! dispatches ready steps through the existing spawner. It is **stateless**
//! between ticks: a takeover after a crash re-derives the same decision from
//! the same events (constraint 1, J1/J4).
//!
//! Three load-bearing properties (§5.3):
//!
//! 1. **It never settles while the run is unfinished.** Returning
//!    `LoopOutcome::Settled` with an empty inbox makes the engine end the
//!    session `completed` — terminal, stream frozen (J6). A waiting
//!    conductor therefore *parks*: it sleeps, yields `TurnDone` and writes
//!    nothing. The engine keeps consuming the same stream, so the drain
//!    stays claimed and the run stays resumable.
//! 2. **A tick writes at most one state transition** (one settlement, one
//!    dispatch, one run verdict). Everything it writes is keyed or derived,
//!    so re-running a tick after a crash changes nothing.
//! 3. **It uses only legal ports**: its own [`RunPort`] (its drain), the
//!    read-only [`WorkerStreamsPort`](crate::worker::WorkerStreamsPort), the
//!    [`WorkerSpawner`], the [`WorkerDirectoryPort`], a deliver port and an
//!    alert port. It never touches another session's drain, never writes a
//!    projection and never reads the live tail (constraint 9).
//!
//! The engine owns the session end, so the run's outcome is carried by **one
//! `evidence/verdict` with gate `run`** written here *before* the driver
//! yields its final outcome (§11.1, §14 A3).

use std::sync::Arc;
use std::time::Duration;

use futures::stream::BoxStream;
use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::SessionId;
use kian_core::seams::{
    ChannelDelivery, ChannelError, LoopDriver, LoopOutcome, SendReceipt,
    TERMINAL_CANCEL_STOP_REASON,
};
use kian_loop::evidence::{CheckOutcome, CheckSpec, GateVerdict, evaluate_evidence_gate};
use serde_json::{Value, json};
use tracing::warn;
use uuid::Uuid;

use crate::directory::WorkerDirectoryPort;
use crate::job::alert::{self, AlertFacts, AlertSpec};
use crate::job::outcome::{
    ArtifactFact, AttemptFact, AttemptOutcome, EffectGate, EffectSignals, Retry, RunFacts,
    RunOutcome, StepFacts, attempt_outcome, effect_gate, effective_effect, readiness,
    retry_decision, run_outcome,
};
use crate::job::placement::{self, Placement, PlacementFacts, PlacementRefusal};
use crate::job::plan::{JobCheck, JobPlan, StepPlan};
use crate::job::status::{self, DispatchRecord, VerdictRecord};
use crate::multiagent::SpawnIdentity;
use crate::worker::{
    SpawnError, SpawnRequest, ThreadLiveness, WorkerSpawner, thread_liveness,
    thread_started_payload_bound,
};

/// The stop reason a finished run yields (§5.3): the engine maps a
/// non-rotation `Stopped` to `completed`.
pub const JOB_FINISHED_STOP_REASON: &str = "job run finished";

/// The soft-failure reason of a failed run (§5.5): the engine maps
/// `SoftFailed` to `failed`.
pub const JOB_FAILED_REASON: &str = "job run failed";

/// The exact run-verdict reason for an operator cancel.
pub const CANCELLED_REASON: &str = "cancelled";

/// The exact run-verdict reason for malformed or conflicting job input.
pub const INVALID_JOB_INPUT_REASON: &str = "invalid_job_input";

/// The exact run-verdict reason for a terminal session with no run verdict.
pub const ENDED_WITHOUT_OUTCOME_REASON: &str = "ended_without_outcome";

/// The floor of one park tick (§5.4): fast enough for an operator cancel.
pub const JOB_TICK_MIN_MS: u64 = 100;

/// The ceiling of one park tick (§5.4): a run can wait hours for a child.
pub const JOB_TICK_MAX_MS: u64 = 5_000;

/// The park tick of a step that is waiting on a busy worker (§9.4).
pub const JOB_DEFER_TICK_MS: u64 = 2_000;

/// The tool name of the delivery intent (§13.2): the same `reply` intent the
/// outbox uses, so an operator reads one vocabulary.
pub const DELIVER_TOOL: &str = "reply";

/// The policy tag a job receipt carries (§13.2).
pub const DELIVERY_POLICY: &str = "job";

/// The call-id prefix of a delivery intent: `job:deliver:<step>:<attempt>`.
pub const DELIVER_CALL_PREFIX: &str = "job:deliver:";

/// The call-id prefix of an artifact harvest: `job:artifact:<step>:<attempt>`.
pub const ARTIFACT_CALL_PREFIX: &str = "job:artifact:";

/// The tool name of the publish pair (SELF-EVOLUTION-MODEL §13 A16).
pub const PUBLISH_TOOL: &str = "job.publish";

/// The call-id prefix of a publish attempt: `job:publish:<step>:<attempt>`
/// (§9.4 step 4).
pub const PUBLISH_CALL_PREFIX: &str = "job:publish:";

/// The check a publish receipt is judged by (§13 A17).
pub const PUBLISH_CHECK: &str = "publish_ok";

/// The honest failure of a publish in a process with no `selfdev.publish`
/// section (§14 V32).
pub const PUBLISH_NOT_CONFIGURED: &str = "publish is not configured";

/// The tool name the publish tool advertises to a job worker (SELF-EVOLUTION-MODEL
/// §9.4). The conductor's frozen pair keeps [`PUBLISH_TOOL`] on the run stream;
/// this name appears only on the child's own stream.
pub const PUBLISH_AGENT_TOOL: &str = "publish";

/// The check a placement refusal records (§9.4).
pub const NO_CANDIDATE_CHECK: &str = "no_candidate";

/// The check a binding refusal records (§8.3).
pub const BINDING_CONFLICT_CHECK: &str = "binding_conflict";

/// The check a cross-run child identity disagreement records (§8.3).
pub const CHILD_IDENTITY_CONFLICT_CHECK: &str = "child_identity_conflict";

/// The check a spent attempt budget records (§8.5).
pub const TIMEOUT_CHECK: &str = "timeout";

/// The check a dispatch failure records (§8.3).
pub const DISPATCH_CHECK: &str = "dispatch";

/// The check a delivery records (§13.2).
pub const DELIVERY_CHECK: &str = "delivery_receipt";

/// The check an input that cannot be inlined records (§12.2).
pub const INPUT_CHECK: &str = "input";

/// The check a consumed cancel records on each abandoned attempt (§5.6).
pub const CANCELLED_CHECK: &str = "cancelled";

/// The port the conductor writes its own stream through (§5.3).
///
/// It is the drain's stream plus the two observations a drain can make: the
/// interrupt flag and the eligible inputs it promoted. Nothing else: a
/// conductor that could reach another session's drain would break the iron
/// rule.
pub trait RunPort: Send + Sync {
    /// This run's session.
    fn session(&self) -> SessionId;
    /// Append one event to the run's own stream.
    fn emit(
        &self,
        kind: EventKind,
        bytes: Value,
    ) -> futures::future::BoxFuture<'static, Result<(), String>>;
    /// Read the run's own committed events.
    fn read_stream(&self) -> futures::future::BoxFuture<'static, Result<Vec<Event>, String>>;
    /// Whether an interrupt has been requested for this drain (§5.4).
    fn interrupt_requested(&self) -> bool;
    /// Promote the rows this drain has admitted since the last tick and
    /// return their payloads (§5.4). Control rows are consumed by the drain
    /// and flip [`interrupt_requested`](Self::interrupt_requested).
    fn take_inputs(&self) -> futures::future::BoxFuture<'static, Result<Vec<Value>, String>>;
}

/// Builds the run port from the drain context.
pub type RunPortFor<Ctx> = Arc<dyn Fn(&Ctx) -> Arc<dyn RunPort> + Send + Sync>;

/// Sends one message through a mounted channel (§13.2).
///
/// The conductor never names an adapter: the port resolves the channel name
/// the plan froze, and an unmounted channel is an honest
/// `Unavailable` — never a silently dropped message.
pub trait DeliverPort: Send + Sync {
    /// Send `content` to `target` on `channel`.
    fn send(
        &self,
        channel: &str,
        target: Value,
        content: Value,
    ) -> futures::future::BoxFuture<'static, Result<SendReceipt, ChannelError>>;
}

impl DeliverPort for crate::router::ChannelRegistry {
    fn send(
        &self,
        channel: &str,
        target: Value,
        content: Value,
    ) -> futures::future::BoxFuture<'static, Result<SendReceipt, ChannelError>> {
        let adapter = self.get(channel);
        let name = channel.to_string();
        let delivery = ChannelDelivery {
            channel: name.clone(),
            target,
            content,
        };
        Box::pin(async move {
            match adapter {
                Some(adapter) => adapter.send(delivery).await,
                None => Err(ChannelError::Unavailable(format!(
                    "channel {name} is not mounted"
                ))),
            }
        })
    }
}

/// The paths a publish derives from the operator's configuration (§9.4
/// step 3): nothing here reads a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishTargets {
    /// The repository the worktree is created from.
    pub repo: String,
    /// `<repo>/.agents/worktrees/selfdev-<run_session>`.
    pub worktree: String,
}

/// What the conductor asks the process to publish (§9.4 steps 5–8).
#[derive(Debug, Clone)]
pub struct PublishRequest {
    /// The publish step.
    pub step: String,
    /// The attempt ordinal.
    pub attempt: u32,
    /// The frozen artifact ref: `{step, name, sha256, bytes, ref}` (§12.1).
    pub from: Value,
    /// The artifact's committed bytes, re-verified against `from.sha256`.
    pub patch: String,
    /// `job.selfdev.base_commit`, frozen at admission (§14 V30).
    pub base_commit: String,
    /// `selfdev/<run_session>` (§9.4 step 3).
    pub branch: String,
    /// The derived worktree path.
    pub worktree: String,
    /// The commit subject.
    pub subject: String,
    /// The audit trailers the commit message carries (§11).
    pub trailers: Vec<(String, String)>,
    /// The branch a pull request would target (S2b).
    pub base: String,
    /// Whether the configured gate runs (§9.5).
    pub gates: bool,
    /// S2b: push the branch.
    pub push: bool,
    /// S2b: open the pull request.
    pub pull_request: bool,
}

/// One gate run's receipt (§9.4 step 8, A16).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateReceipt {
    /// The configured command, as the operator wrote it.
    pub command: String,
    /// Its exit code.
    pub exit_code: i32,
    /// A digest over its bounded output.
    pub digest: String,
    /// The tail of its output, bounded.
    pub output_tail: String,
}

/// One push's receipt (§9.4 step 8, A16): the remote and the ref it now
/// holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushReceipt {
    /// The configured remote name.
    pub remote: String,
    /// The ref the push created: refs/heads/<branch>.
    pub reference: String,
}

/// One pull request's receipt (§9.4 step 8, A16).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequestReceipt {
    /// The branch the pull request targets.
    pub base: String,
    /// The branch it comes from.
    pub head: String,
    /// The URL the tool printed, when it printed one.
    pub url: Option<String>,
}

/// The receipt of one publish (§9.4 step 9, A16).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishReceipt {
    /// The full commit.
    pub commit: String,
    /// The branch it landed on: `selfdev/<run_session>`.
    pub branch: String,
    /// The worktree that holds it.
    pub worktree: String,
    /// The gate run, when `publish.gates` is true.
    pub gate: Option<GateReceipt>,
    /// The push of the branch, when `publish.push` is true.
    pub push: Option<PushReceipt>,
    /// The pull request, when `publish.pull_request` is true.
    pub pull_request: Option<PullRequestReceipt>,
}

/// Why a publish produced no receipt (§9.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishError {
    /// No `selfdev.publish` section: the step fails honestly and writes
    /// nothing (§14 V32).
    NotConfigured,
    /// The configuration cannot derive a worktree.
    Unavailable(String),
    /// The derived worktree already exists: another attempt (or another run)
    /// left state there, so only an operator can decide what it is (§9.2).
    Occupied(String),
    /// The sequence started and its end is unknown (a gate that outlived its
    /// bound): an operator has to look.
    Unknown(String),
    /// A write landed and its intended end did not: the branch is on the
    /// remote but the pull request failed, so only an operator can finish it
    /// (§12).
    Partial(String),
    /// The sequence failed; the detail says how far it got.
    Failed(String),
}

impl PublishError {
    /// The operator-facing reason.
    #[must_use]
    pub fn detail(&self) -> String {
        match self {
            Self::NotConfigured => PUBLISH_NOT_CONFIGURED.to_string(),
            Self::Unavailable(detail)
            | Self::Occupied(detail)
            | Self::Unknown(detail)
            | Self::Partial(detail)
            | Self::Failed(detail) => detail.clone(),
        }
    }

    /// The attempt outcome this failure settles.
    #[must_use]
    pub fn outcome(&self) -> AttemptOutcome {
        match self {
            Self::Occupied(_) | Self::Unknown(_) | Self::Partial(_) => {
                AttemptOutcome::NeedsOperator
            }
            Self::NotConfigured | Self::Unavailable(_) | Self::Failed(_) => AttemptOutcome::Failed,
        }
    }
}

/// The side effects a frozen publish needs (§9.4), injected so the conductor
/// itself never touches a repository or a process. Replay only: a new plan
/// cannot declare the retired publish action (ADR-084).
pub trait PublishPort: Send + Sync {
    /// The worktree a publish of this run would create, derived from
    /// configuration alone. `None` means the process has no
    /// `selfdev.publish` section.
    fn derive(&self, run: SessionId) -> Option<PublishTargets>;
    /// Run steps 6–8 of §9.4: the worktree, the applied patch, the commit and
    /// the gate.
    fn publish(
        &self,
        request: PublishRequest,
    ) -> futures::future::BoxFuture<'static, Result<PublishReceipt, PublishError>>;
}

/// The port of a process whose configuration has no `selfdev.publish`
/// section: every frozen publish fails honestly instead of writing (§14 V32).
pub struct UnconfiguredPublish;

impl PublishPort for UnconfiguredPublish {
    fn derive(&self, _run: SessionId) -> Option<PublishTargets> {
        None
    }

    fn publish(
        &self,
        _request: PublishRequest,
    ) -> futures::future::BoxFuture<'static, Result<PublishReceipt, PublishError>> {
        Box::pin(async { Err(PublishError::NotConfigured) })
    }
}

/// Admits an alert run (§13.3).
pub trait AlertPort: Send + Sync {
    /// The alerts declared for `job_id`, in declaration order.
    fn alerts_for(&self, job_id: &str) -> Vec<AlertSpec>;
    /// Admit one alert run's first input row, then wake it. The key makes a
    /// repeated admission the same admission (§13.3).
    fn raise(
        &self,
        session: SessionId,
        payload: Value,
        key: String,
    ) -> futures::future::BoxFuture<'static, Result<(), String>>;
}

/// The clock every deadline, backoff and deferral comparison uses (§5.4).
///
/// Declared in the bundle's own vocabulary: the conductor implements the loop
/// seam (it is the run session's driver) but names no orchestrator type, so
/// the bundle's only `kian-orchestrator` surface stays the pure
/// `WakeOutcome` the inbox port already carries.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// What the conductor needs from the process around it (§5.3).
pub struct JobDeps {
    /// Read-only cross-session stream access (children, producers).
    pub streams: Arc<dyn crate::worker::WorkerStreamsPort>,
    /// The one spawner: a conductor spawns its steps, nobody else does (J5).
    pub spawner: Arc<WorkerSpawner>,
    /// The worker catalog placement is computed from (§9.2).
    pub directory: Arc<dyn WorkerDirectoryPort>,
    /// Delivery, as a conductor action (§13.1).
    pub deliver: Arc<dyn DeliverPort>,
    /// Publishing, as a conductor action (SELF-EVOLUTION-MODEL §9.4).
    pub publish: Arc<dyn PublishPort>,
    /// Alerts, as runs of a synthetic job (§13.3).
    pub alerts: Arc<dyn AlertPort>,
    /// The clock every comparison uses.
    pub clock: Arc<dyn Clock>,
    /// The floor of a park (§5.4).
    pub tick_floor_ms: u64,
    /// The ceiling of a park (§5.4).
    pub tick_ceiling_ms: u64,
}

impl JobDeps {
    /// The deps with the default tick bounds.
    #[must_use]
    pub fn new(
        streams: Arc<dyn crate::worker::WorkerStreamsPort>,
        spawner: Arc<WorkerSpawner>,
        directory: Arc<dyn WorkerDirectoryPort>,
        deliver: Arc<dyn DeliverPort>,
        alerts: Arc<dyn AlertPort>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            streams,
            spawner,
            directory,
            deliver,
            publish: Arc::new(UnconfiguredPublish),
            alerts,
            clock,
            tick_floor_ms: JOB_TICK_MIN_MS,
            tick_ceiling_ms: JOB_TICK_MAX_MS,
        }
    }

    /// Replace the publish port (SELF-EVOLUTION-MODEL §9.4).
    #[must_use]
    pub fn with_publish(mut self, publish: Arc<dyn PublishPort>) -> Self {
        self.publish = publish;
        self
    }

    fn clamp_wait(&self, wait_ms: u64) -> u64 {
        wait_ms.clamp(
            self.tick_floor_ms.max(1),
            self.tick_ceiling_ms.max(self.tick_floor_ms),
        )
    }
}

/// What one tick decided (§5.3): park, or finish the run.
enum Tick {
    /// Sleep this long, then yield `TurnDone` and tick again.
    Park { wait_ms: u64 },
    /// Yield this outcome; the stream then ends and the engine owns the end.
    Finish { outcome: LoopOutcome },
}

impl Tick {
    fn park(wait_ms: u64) -> Self {
        Self::Park { wait_ms }
    }

    fn finish(outcome: LoopOutcome) -> Self {
        Self::Finish { outcome }
    }
}

/// The stateless conductor (§5.3).
///
/// Every decision is a function of the run's committed stream, its children's
/// streams, the worker catalog and the clock. There is no in-memory run state
/// to lose: a takeover re-derives the same next action (J1/J4).
pub struct JobLoopDriver<Ctx: Send + 'static> {
    port_for: RunPortFor<Ctx>,
    deps: Arc<JobDeps>,
}

impl<Ctx: Send + 'static> JobLoopDriver<Ctx> {
    /// Build a conductor over a drain context.
    #[must_use]
    pub fn new(port_for: RunPortFor<Ctx>, deps: Arc<JobDeps>) -> Self {
        Self { port_for, deps }
    }

    /// The ports this conductor was built with.
    #[must_use]
    pub fn deps(&self) -> &Arc<JobDeps> {
        &self.deps
    }
}

struct JobState<Ctx> {
    ctx: Ctx,
    port_for: RunPortFor<Ctx>,
    deps: Arc<JobDeps>,
    done: bool,
}

impl<Ctx: Send + 'static> LoopDriver for JobLoopDriver<Ctx> {
    type Ctx = Ctx;

    fn drive(&self, ctx: Self::Ctx) -> BoxStream<'static, LoopOutcome> {
        let state = Some(JobState {
            ctx,
            port_for: Arc::clone(&self.port_for),
            deps: Arc::clone(&self.deps),
            done: false,
        });
        Box::pin(futures::stream::unfold(state, |state| async move {
            let state = state?;
            if state.done {
                return None;
            }
            let port = (state.port_for)(&state.ctx);
            match tick(&port, &state.deps).await {
                Tick::Park { wait_ms } => {
                    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
                    let next = JobState {
                        ctx: state.ctx,
                        port_for: state.port_for,
                        deps: state.deps,
                        done: false,
                    };
                    Some((LoopOutcome::TurnDone, Some(next)))
                }
                Tick::Finish { outcome } => {
                    let next = JobState {
                        ctx: state.ctx,
                        port_for: state.port_for,
                        deps: state.deps,
                        done: true,
                    };
                    Some((outcome, Some(next)))
                }
            }
        }))
    }
}

/// One tick of the conductor (§5.3): derive, then take **at most one** state
/// transition.
async fn tick(port: &Arc<dyn RunPort>, deps: &JobDeps) -> Tick {
    let promoted = match port.take_inputs().await {
        Ok(rows) => rows,
        Err(err) => {
            warn!(
                session = %port.session().0,
                error = %err,
                "job run: cannot promote inputs"
            );
            return Tick::park(JOB_TICK_MIN_MS);
        }
    };
    let mut cancelled = port.interrupt_requested() || promoted.iter().any(is_cancel_row);
    let events = match port.read_stream().await {
        Ok(events) => events,
        Err(err) => {
            warn!(
                session = %port.session().0,
                error = %err,
                "job run: cannot read its own stream"
            );
            return Tick::park(JOB_TICK_MIN_MS);
        }
    };
    cancelled |= port.interrupt_requested();
    // The engine promotes eligible rows before it drives the loop
    // (orchestrator `supervise`), so a cancel row usually reaches the
    // conductor only through the run stream, never through `take_inputs`.
    cancelled |= events.iter().any(|event| {
        event.kind == kian_core::events::EventKind::InputAdmitted && is_cancel_row(&event.bytes)
    });

    if let Some(verdict) = status::run_verdict(&events) {
        if let Some(marker) = status::scan_marker(&events)
            .first
            .and_then(Result::ok)
            .map(|record| record.marker)
        {
            let (failed_step, reason) = run_failure(&verdict.payload);
            admit_alerts(
                deps,
                &marker,
                verdict.outcome,
                failed_step.as_deref(),
                reason.as_deref(),
            )
            .await;
        }
        return Tick::finish(outcome_to_loop(verdict.outcome));
    }

    if cancelled {
        return cancel_run(port, &events).await;
    }

    let found = status::marker(&events);
    let Some(found) = found else {
        return Tick::finish(LoopOutcome::SoftFailed {
            reason: "job run: the stream carries no job marker".to_string(),
        });
    };
    if terminal_end_without_verdict(&events) {
        return Tick::finish(LoopOutcome::SoftFailed {
            reason: format!("job run: {ENDED_WITHOUT_OUTCOME_REASON}"),
        });
    }
    let marker = match found {
        Ok(marker) => marker,
        Err(err) => return finish_invalid_input(port, &events, err).await,
    };
    let plan = &marker.plan;

    if plan.declares_surface() && marker.selfdev.is_none() {
        return finish_invalid_input(
            port,
            &events,
            status::MarkerError::Malformed(
                "selfdev job requires an evidence pack: the plan declares patch_paths_allowed and the marker carries no selfdev value"
                    .to_string(),
            ),
        )
        .await;
    }

    let dispatches = status::dispatches(&events);
    let verdict_log = status::verdicts(&events);
    let mut intents = deliver_intents(&events);
    intents.extend(publish_intents(&events));
    let facts = facts_from(plan, &dispatches, &intents, &verdict_log);
    let artifacts = status::artifacts(&events, plan);
    let now = deps.clock.now_ms();
    let started_ms = run_input_ts(&events).unwrap_or(now);
    let deadline_at = started_ms.saturating_add(plan.limits.deadline_ms);
    let deadline_passed = now >= deadline_at;

    if let Some((step, attempt)) = in_flight(plan, &facts) {
        return observe(
            port,
            deps,
            &marker,
            &dispatches,
            &events,
            step,
            attempt,
            now,
            deadline_at,
        )
        .await;
    }

    if run_halted(plan, &facts) {
        return settle_run(
            port,
            deps,
            &marker,
            &facts,
            &artifacts,
            deadline_passed,
            &events,
        )
        .await;
    }

    if all_steps_decided(plan, &facts, &artifacts) {
        let settled_outcome = run_outcome(
            plan,
            &RunFacts {
                steps: &facts,
                artifacts: &artifacts,
                accepted: &plan.limits.accept_unverified,
                halted: run_halted(plan, &facts),
                deadline_passed: false,
            },
            false,
        );
        return settle_run(
            port,
            deps,
            &marker,
            &facts,
            &artifacts,
            deadline_passed && settled_outcome == RunOutcome::Failed,
            &events,
        )
        .await;
    }

    if deadline_passed {
        return settle_run(port, deps, &marker, &facts, &artifacts, true, &events).await;
    }

    let run_facts = RunFacts {
        steps: &facts,
        artifacts: &artifacts,
        accepted: &plan.limits.accept_unverified,
        halted: run_halted(plan, &facts),
        deadline_passed: false,
    };
    for step in &plan.steps {
        let Some(known) = facts.iter().find(|known| known.step == step.id) else {
            continue;
        };
        if let Retry::Later {
            attempt,
            not_before_ms,
        } = retry_decision(
            step,
            known,
            EffectSignals {
                tool_calls: 0,
                receipt: false,
            },
            false,
        ) && now >= not_before_ms
        {
            return dispatch_attempt(port, deps, &marker, &facts, &events, step, attempt).await;
        }
    }

    for step in &plan.steps {
        if readiness(plan, &run_facts, step).is_ok() {
            return dispatch_attempt(port, deps, &marker, &facts, &events, step, 1).await;
        }
    }

    let wait = next_wait_ms(plan, &facts, now, deadline_at).unwrap_or(deps.tick_ceiling_ms);
    Tick::park(deps.clamp_wait(wait))
}

fn outcome_to_loop(outcome: RunOutcome) -> LoopOutcome {
    match outcome {
        RunOutcome::Failed => LoopOutcome::SoftFailed {
            reason: JOB_FAILED_REASON.to_string(),
        },
        RunOutcome::Cancelled => LoopOutcome::Stopped {
            reason: TERMINAL_CANCEL_STOP_REASON.to_string(),
        },
        RunOutcome::Succeeded | RunOutcome::Unverified => LoopOutcome::Stopped {
            reason: JOB_FINISHED_STOP_REASON.to_string(),
        },
    }
}

fn terminal_end_without_verdict(events: &[Event]) -> bool {
    if status::run_verdict(events).is_some() {
        return false;
    }
    let reason = events.iter().rev().find_map(|event| match event.kind {
        EventKind::SessionEnded { reason } => Some(reason),
        _ => None,
    });
    reason.is_some_and(|reason| {
        matches!(
            reason,
            EndReason::Completed | EndReason::Cancelled | EndReason::Rotated
        )
    })
}

/// The reason a finished run gives its alert (§13.3).
fn run_failure(payload: &Value) -> (Option<String>, Option<String>) {
    let job = payload.get(status::JOB_KEY);
    let failed_step = job
        .and_then(|job| job.get("failed_step"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let reason = job
        .and_then(|job| job.get("reason"))
        .and_then(Value::as_str)
        .map(str::to_string);
    (failed_step, reason)
}

fn is_cancel_row(row: &Value) -> bool {
    let content = match row.get("content") {
        Some(Value::String(content)) => Some(content.as_str()),
        Some(wrapped) => wrapped.get("content").and_then(Value::as_str),
        None => None,
    };
    content.is_some_and(|content| content.trim().eq_ignore_ascii_case("cancel"))
}

/// Every delivery intent on the run stream: `(step, attempt)` (§13.2).
#[must_use]
pub fn deliver_intents(events: &[Event]) -> Vec<(String, u32)> {
    intents_with(events, DELIVER_TOOL, DELIVER_CALL_PREFIX)
}

/// Every publish intent on the run stream: `(step, attempt)` (§9.4 step 4).
#[must_use]
pub fn publish_intents(events: &[Event]) -> Vec<(String, u32)> {
    intents_with(events, PUBLISH_TOOL, PUBLISH_CALL_PREFIX)
}

/// The `(step, attempt)` pairs of one conductor action's intents on the run
/// stream. An intent is the only evidence that the action may have happened.
fn intents_with(events: &[Event], tool: &str, prefix: &str) -> Vec<(String, u32)> {
    let mut out = Vec::new();
    for event in events {
        if event.kind != EventKind::ToolCall {
            continue;
        }
        if event.bytes.get("tool").and_then(Value::as_str) != Some(tool) {
            continue;
        }
        let Some(call_id) = event.bytes.get("call_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(rest) = call_id.strip_prefix(prefix) else {
            continue;
        };
        let Some((step, attempt)) = rest.rsplit_once(':') else {
            continue;
        };
        let Ok(attempt) = attempt.parse::<u32>() else {
            continue;
        };
        out.push((step.to_string(), attempt));
    }
    out
}

/// Rebuild the per-step facts from the stream (§8.1, J1).
#[must_use]
pub fn facts_from(
    plan: &JobPlan,
    dispatches: &[DispatchRecord],
    intents: &[(String, u32)],
    verdict_log: &[VerdictRecord],
) -> Vec<StepFacts> {
    let mut facts: Vec<StepFacts> = plan
        .steps
        .iter()
        .map(|step| StepFacts::pending(step.id.clone()))
        .collect();
    for known in &mut facts {
        let dispatched = dispatches
            .iter()
            .filter(|record| record.step == known.step)
            .count() as u32;
        let delivered = intents
            .iter()
            .filter(|(step, _)| *step == known.step)
            .count() as u32;
        let attempts = dispatched.max(delivered);
        for verdict in verdict_log
            .iter()
            .filter(|verdict| verdict.step == known.step)
        {
            known.settled.push(AttemptFact {
                attempt: verdict.ordinal,
                outcome: verdict.outcome.unwrap_or(if verdict.passed {
                    AttemptOutcome::Succeeded
                } else {
                    AttemptOutcome::Failed
                }),
                worker: dispatches
                    .iter()
                    .find(|record| record.step == verdict.step && record.attempt == verdict.ordinal)
                    .map(|record| record.worker.clone()),
                ended_ms: verdict.at_ms,
            });
        }
        if attempts > known.settled.len() as u32 {
            known.in_flight = Some(known.settled.len() as u32 + 1);
        }
    }
    facts
}

fn in_flight<'a>(plan: &'a JobPlan, facts: &[StepFacts]) -> Option<(&'a StepPlan, u32)> {
    facts.iter().find_map(|known| {
        known
            .in_flight
            .and_then(|attempt| plan.step(&known.step).map(|step| (step, attempt)))
    })
}

fn run_input_ts(events: &[Event]) -> Option<u64> {
    let first = status::scan_marker(events).first_seq?;
    events
        .iter()
        .find(|event| event.seq == first)
        .map(|event| event.ts)
}

/// Whether a settled step ends the run because of its failure policy (§10.2).
fn run_halted(plan: &JobPlan, facts: &[StepFacts]) -> bool {
    plan.steps.iter().any(|step| {
        let Some(known) = facts.iter().find(|known| known.step == step.id) else {
            return false;
        };
        let Some(outcome) = known.outcome() else {
            return false;
        };
        if outcome.is_success(plan.limits.accept_unverified.contains(&step.id)) {
            return false;
        }
        if step.optional || step.on_failure == crate::job::plan::OnFailure::Continue {
            return false;
        }
        let retries_left = usize::from(step.retries) + 1 > known.attempts_used();
        !(outcome.is_retryable() && retries_left)
    })
}

fn step_has_retry(step: &StepPlan, known: &StepFacts) -> bool {
    let Some(outcome) = known.outcome() else {
        return false;
    };
    outcome.is_retryable() && known.attempts_used() <= usize::from(step.retries)
}

/// Whether every step is either final or permanently blocked by committed
/// dependency/input facts. A blocked step is skipped in the run verdict and
/// never written as a fake attempt.
fn all_steps_decided(plan: &JobPlan, facts: &[StepFacts], artifacts: &[ArtifactFact]) -> bool {
    let run_facts = RunFacts {
        steps: facts,
        artifacts,
        accepted: &plan.limits.accept_unverified,
        halted: run_halted(plan, facts),
        deadline_passed: false,
    };
    plan.steps.iter().all(|step| {
        let Some(known) = facts.iter().find(|known| known.step == step.id) else {
            return false;
        };
        if known.in_flight.is_some() {
            return false;
        }
        if step_has_retry(step, known) {
            return false;
        }
        known.outcome().is_some() || blocked_reason(plan, &run_facts, step).is_some()
    })
}

/// Why a step can never become ready, if it cannot (§8.1). A step whose
/// dependency or input settled without success is skipped, not waited on: the
/// plan froze a shape, so an unreachable branch is an outcome.
fn blocked_reason(plan: &JobPlan, facts: &RunFacts<'_>, step: &StepPlan) -> Option<Blocked> {
    let Err(not_ready) = readiness(plan, facts, step) else {
        return None;
    };
    match not_ready {
        crate::job::outcome::NotReady::Dependency { step: dep, outcome } => {
            let outcome = outcome?;
            Some(Blocked {
                detail: format!("dependency {dep} ended {}", outcome.as_str()),
            })
        }
        crate::job::outcome::NotReady::Input { step: dep, name } => {
            let outcome = facts.step(&dep)?.outcome()?;
            (!outcome.is_success(false)).then(|| Blocked {
                detail: format!(
                    "input {name} from step {dep} is unavailable: the step ended {}",
                    outcome.as_str()
                ),
            })
        }
        _ => None,
    }
}

#[allow(dead_code)]
struct Blocked {
    detail: String,
}

/// How long the conductor may sleep before something becomes due (§5.4).
fn next_wait_ms(plan: &JobPlan, facts: &[StepFacts], now: u64, deadline_at: u64) -> Option<u64> {
    let mut wait: Option<u64> = None;
    if deadline_at > now {
        wait = Some(deadline_at - now);
    }
    for step in &plan.steps {
        let Some(known) = facts.iter().find(|known| known.step == step.id) else {
            continue;
        };
        if let Retry::Later { not_before_ms, .. } = retry_decision(
            step,
            known,
            EffectSignals {
                tool_calls: 0,
                receipt: false,
            },
            false,
        ) && not_before_ms > now
        {
            let delta = not_before_ms - now;
            wait = Some(wait.map_or(delta, |known| known.min(delta)));
        }
    }
    wait
}

/// The per-step outcomes a run verdict carries (§11.1).
#[must_use]
pub fn step_outcomes(plan: &JobPlan, facts: &[StepFacts]) -> Vec<(String, AttemptOutcome)> {
    plan.steps
        .iter()
        .map(|step| {
            let outcome = facts
                .iter()
                .find(|known| known.step == step.id)
                .and_then(StepFacts::outcome)
                .unwrap_or(AttemptOutcome::Skipped);
            (step.id.clone(), outcome)
        })
        .collect()
}

fn failed_gate(attempt: u32, check: &str, detail: Option<String>) -> GateVerdict {
    GateVerdict {
        passed: false,
        checks: vec![CheckOutcome {
            name: check.to_string(),
            passed: false,
            detail,
        }],
        attempt: u64::from(attempt.saturating_sub(1)),
    }
}

fn passed_gate(attempt: u32, check: &str, detail: Option<String>) -> GateVerdict {
    GateVerdict {
        passed: true,
        checks: vec![CheckOutcome {
            name: check.to_string(),
            passed: true,
            detail,
        }],
        attempt: u64::from(attempt.saturating_sub(1)),
    }
}

/// The declared checks of a step, split into the loop's evaluable specs and
/// the job's own checks (§11.2).
#[must_use]
pub fn declared_checks(step: &StepPlan, events: &[Event]) -> (Vec<CheckSpec>, Vec<CheckOutcome>) {
    let mut specs = Vec::new();
    let mut extra = Vec::new();
    for check in &step.verify {
        if let Some(spec) = check.to_check_spec() {
            specs.push(spec);
            continue;
        }
        extra.push(extra_check(step, check, events));
    }
    (specs, extra)
}

fn extra_check(
    step: &StepPlan,
    check: &crate::job::plan::JobCheck,
    events: &[Event],
) -> CheckOutcome {
    use crate::job::plan::JobCheck;
    match check {
        JobCheck::ArtifactWithinCap { name } => {
            let cap = step
                .produces
                .as_ref()
                .filter(|produces| &produces.name == name)
                .map(|produces| produces.max_bytes);
            let (passed, detail) = match (cap, artifact_of(step, events)) {
                (None, _) => (
                    false,
                    format!(
                        "step {} declares no artifact named {name}, so no cap applies",
                        step.id
                    ),
                ),
                (Some(_), None) => (false, format!("artifact {name} was not produced")),
                (Some(cap), Some(artifact)) => {
                    let bytes = artifact
                        .get("bytes")
                        .and_then(Value::as_u64)
                        .unwrap_or(u64::MAX);
                    (bytes <= cap, format!("{name} is {bytes} bytes, cap {cap}"))
                }
            };
            CheckOutcome {
                name: check.name().to_string(),
                passed,
                detail: Some(detail),
            }
        }
        JobCheck::DeliveryReceipt { channel, target } => {
            let receipt = events.iter().find(|event| {
                event.kind == EventKind::ChannelDelivered
                    && event.bytes.get("channel").and_then(Value::as_str) == Some(channel)
                    && event.bytes.get("target").is_some_and(|seen| seen == target)
            });
            CheckOutcome {
                name: check.name().to_string(),
                passed: receipt.is_some(),
                detail: receipt.map(|event| format!("receipt at seq {}", event.seq)),
            }
        }
        JobCheck::PatchPathsAllowed {
            artifact,
            allow,
            deny,
        } => {
            let produced = artifact_of(step, events).filter(|value| {
                value.get("name").and_then(Value::as_str) == Some(artifact.as_str())
            });
            let (passed, detail) = match produced {
                None => (
                    false,
                    format!("artifact {artifact} was not produced, so no paths can be read"),
                ),
                Some(value) => match artifact_text(events, artifact, &value) {
                    Err(reason) => (false, reason),
                    Ok(diff) => {
                        let verdict = crate::job::surface::check_surface(&diff, allow, deny);
                        (verdict.passed, verdict.detail)
                    }
                },
            };
            CheckOutcome {
                name: check.name().to_string(),
                passed,
                detail: Some(detail),
            }
        }
        JobCheck::PublishOk => {
            let calls = publish_tool_calls(events);
            let (passed, detail) = match calls.as_slice() {
                [] => (false, "the step never called the publish tool".to_string()),
                [call] => {
                    let empty_args = call
                        .arguments
                        .as_object()
                        .is_some_and(|arguments| arguments.is_empty());
                    if !empty_args {
                        (false, "the publish tool takes no arguments; the run, step and patch are derived from the committed streams".to_string())
                    } else {
                        match &call.result {
                            None => (false, "the publish call has no receipt: the worktree may exist and an operator must inspect".to_string()),
                            Some(result) if result.get("ok").and_then(Value::as_bool) != Some(true) => (
                                false,
                                format!(
                                    "the publish tool reported: {}",
                                    result
                                        .get("error")
                                        .and_then(Value::as_str)
                                        .unwrap_or("no receipt")
                                ),
                            ),
                            Some(result) => match child_run_session(events) {
                                None => (false, "the child stream carries no thread/started lineage, so a receipt has no run to bind".to_string()),
                                Some(run) => {
                                    let branch = publish_branch(run);
                                    let gates = result.pointer("/result/gate").is_some();
                                    let push = result.pointer("/result/push").is_some();
                                    let pull_request = result.pointer("/result/pull_request").is_some();
                                    let outcome = check_publish_ok(result, &branch, gates, push, pull_request);
                                    (outcome.passed, outcome.detail.unwrap_or_default())
                                }
                            },
                        }
                    }
                }
                _ => (
                    false,
                    format!(
                        "the child called the publish tool {} times; a run publishes once",
                        calls.len()
                    ),
                ),
            };
            CheckOutcome {
                name: check.name().to_string(),
                passed,
                detail: Some(detail),
            }
        }
        _ => CheckOutcome {
            name: check.name().to_string(),
            passed: false,
            detail: Some("the check is not evaluable by the conductor".to_string()),
        },
    }
}

/// One publish tool call on a child stream (§9.4): the loop commits the call
/// before the tool writes, so a call with no result is a write whose end
/// nobody saw.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishToolCall {
    pub call_id: String,
    pub call_seq: u64,
    pub arguments: Value,
    pub result: Option<Value>,
    pub result_seq: Option<u64>,
}

/// Every publish tool call on a child stream, in commit order. The child's
/// tool envelope carries `content.{id,name,arguments}`; the result envelope
/// carries `content.{call_id,output,is_error}`.
#[must_use]
pub fn publish_tool_calls(events: &[Event]) -> Vec<PublishToolCall> {
    let mut calls: Vec<PublishToolCall> = Vec::new();
    for event in events {
        let content = event.bytes.get("content");
        match event.kind {
            EventKind::ToolCall => {
                if content
                    .and_then(|value| value.get("name"))
                    .and_then(Value::as_str)
                    != Some(PUBLISH_AGENT_TOOL)
                {
                    continue;
                }
                let Some(call_id) = content
                    .and_then(|value| value.get("id"))
                    .and_then(Value::as_str)
                    .map(str::to_string)
                else {
                    continue;
                };
                calls.push(PublishToolCall {
                    call_id,
                    call_seq: event.seq,
                    arguments: content
                        .and_then(|value| value.get("arguments"))
                        .cloned()
                        .unwrap_or(Value::Null),
                    result: None,
                    result_seq: None,
                });
            }
            EventKind::ToolResult => {
                let Some(call_id) = content
                    .and_then(|value| value.get("call_id"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                if let Some(call) = calls
                    .iter_mut()
                    .find(|call| call.call_id == call_id && call.result.is_none())
                {
                    call.result = Some(
                        content
                            .and_then(|value| value.get("output"))
                            .cloned()
                            .unwrap_or(Value::Null),
                    );
                    call.result_seq = Some(event.seq);
                }
            }
            _ => {}
        }
    }
    calls
}

/// The run a job child executes for: its `thread/started.parent` names the run
/// session (JOB-MODEL A5).
pub fn child_run_session(events: &[Event]) -> Option<SessionId> {
    events
        .iter()
        .find(|event| event.kind == EventKind::ThreadStarted)
        .and_then(|event| event.bytes.get("parent"))
        .and_then(Value::as_str)
        .and_then(|raw| uuid::Uuid::parse_str(raw).ok())
        .map(SessionId)
}

/// The text an artifact reference points at, read from the same child stream:
/// the ref names a seq and that event carries the bytes the producer sent
/// (§12.1). The gate runs before the harvest, so this reads the child stream,
/// never the run stream's refs.
fn artifact_text(events: &[Event], name: &str, artifact: &Value) -> Result<String, String> {
    let seq = artifact
        .get("ref")
        .and_then(|reference| reference.get("seq"))
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("artifact {name} carries no ref, so its bytes cannot be read"))?;
    let event = events
        .iter()
        .find(|event| event.seq == seq)
        .ok_or_else(|| {
            format!("artifact {name} points at seq {seq}, which is not on this stream")
        })?;
    message_text(event)
        .ok_or_else(|| format!("artifact {name} points at seq {seq}, which is not a text message"))
}

/// The artifact a step's child produced, derived from the child's own stream
/// (§12.1): a text artifact is the committed bytes of the producer's final
/// `assistant/message` — the last assistant message that carries text and no
/// tool calls. The ref names that event and the digest covers its exact text,
/// so a reader detects divergence instead of trusting a copy.
fn artifact_of(step: &StepPlan, events: &[Event]) -> Option<Value> {
    let produces = step.produces.as_ref()?;
    let event = events.iter().rev().find(|event| {
        event.kind == EventKind::AssistantMessage
            && closes_turn(&event.bytes)
            && message_text(event).is_some_and(|text| !text.is_empty())
    })?;
    let text = message_text(event)?;
    Some(json!({
        "step": &step.id,
        "name": &produces.name,
        "media": "text",
        "bytes": text.len(),
        "sha256": crate::job::plan::digest_text(&text),
        "ref": {"session": event.session.0.to_string(), "seq": event.seq},
    }))
}

/// A final-answer message carries no tool calls; a tool round does (§4.3).
fn closes_turn(bytes: &Value) -> bool {
    match bytes
        .get("content")
        .and_then(|content| content.get("tool_calls"))
    {
        None | Some(Value::Null) => true,
        Some(Value::Array(calls)) => calls.is_empty(),
        Some(_) => false,
    }
}

struct PlanContext {
    run_id: String,
    digest: String,
    steps: Vec<String>,
}

fn plan_context(events: &[Event]) -> Option<PlanContext> {
    let scan = status::scan_marker(events);
    match scan.first {
        Some(Ok(record)) => Some(PlanContext {
            run_id: record.marker.run_id,
            digest: record.marker.plan_digest,
            steps: record
                .marker
                .plan
                .steps
                .into_iter()
                .map(|step| step.id)
                .collect(),
        }),
        _ => None,
    }
}

fn skipped_outcomes(context: Option<&PlanContext>) -> Vec<(String, AttemptOutcome)> {
    context
        .map(|context| {
            context
                .steps
                .iter()
                .map(|step| (step.clone(), AttemptOutcome::Skipped))
                .collect()
        })
        .unwrap_or_default()
}

/// Settle a run that cannot be read as a job (§5.2): one run verdict, then
/// fail. The plan is represented as skipped whenever its first marker is
/// still readable; a malformed marker has no plan to enumerate.
async fn finish_invalid_input(
    port: &Arc<dyn RunPort>,
    events: &[Event],
    err: status::MarkerError,
) -> Tick {
    let context = plan_context(events);
    let steps = skipped_outcomes(context.as_ref());
    let digest = context
        .as_ref()
        .map_or("", |context| context.digest.as_str());
    let run = context.as_ref().map(|context| context.run_id.as_str());
    warn!(
        session = %port.session().0,
        error = %err,
        "job run: invalid input"
    );
    let payload = status::run_verdict_payload(
        run,
        RunOutcome::Failed,
        digest,
        &steps,
        None,
        Some(INVALID_JOB_INPUT_REASON),
    );
    if let Err(write) = port.emit(EventKind::EvidenceVerdict, payload).await {
        warn!(session = %port.session().0, error = %write, "job run: cannot write the run verdict");
        return Tick::park(JOB_TICK_MIN_MS);
    }
    Tick::finish(LoopOutcome::SoftFailed {
        reason: JOB_FAILED_REASON.to_string(),
    })
}

/// Consume a terminal cancel before observation or dispatch (§5.6).
async fn cancel_run(port: &Arc<dyn RunPort>, events: &[Event]) -> Tick {
    let context = plan_context(events);
    let dispatches = status::dispatches(events);
    let mut intents = deliver_intents(events);
    intents.extend(publish_intents(events));
    let verdict_log = status::verdicts(events);
    let plan = status::scan_marker(events)
        .first
        .and_then(|found| match found {
            Ok(record) => Some(record.marker.plan),
            Err(_) => None,
        });

    let mut facts = plan
        .as_ref()
        .map(|plan| facts_from(plan, &dispatches, &intents, &verdict_log))
        .unwrap_or_default();
    if let Some(plan) = &plan {
        for step in &plan.steps {
            let Some(known) = facts.iter_mut().find(|known| known.step == step.id) else {
                continue;
            };
            let Some(attempt) = known.in_flight.take() else {
                continue;
            };
            let gate = failed_gate(
                attempt,
                CANCELLED_CHECK,
                Some("the run was cancelled before this attempt settled".to_string()),
            );
            let payload = status::step_verdict_payload(
                context
                    .as_ref()
                    .map_or("", |context| context.run_id.as_str()),
                &step.id,
                &gate,
                AttemptOutcome::Interrupted,
            );
            if let Err(err) = port.emit(EventKind::EvidenceVerdict, payload).await {
                warn!(session = %port.session().0, error = %err, "job run: cannot write a cancelled step verdict");
                return Tick::park(JOB_TICK_MIN_MS);
            }
            known.settled.push(AttemptFact {
                attempt,
                outcome: AttemptOutcome::Interrupted,
                worker: dispatches
                    .iter()
                    .find(|record| record.step == step.id && record.attempt == attempt)
                    .map(|record| record.worker.clone()),
                ended_ms: 0,
            });
        }
    }

    let steps = plan.as_ref().map_or_else(
        || skipped_outcomes(context.as_ref()),
        |plan| step_outcomes(plan, &facts),
    );
    let digest = context
        .as_ref()
        .map_or("", |context| context.digest.as_str());
    let run = context.as_ref().map(|context| context.run_id.as_str());
    let payload = status::run_verdict_payload(
        run,
        RunOutcome::Cancelled,
        digest,
        &steps,
        None,
        Some(CANCELLED_REASON),
    );
    if let Err(err) = port.emit(EventKind::EvidenceVerdict, payload).await {
        warn!(session = %port.session().0, error = %err, "job run: cannot write the cancelled run verdict");
        return Tick::park(JOB_TICK_MIN_MS);
    }
    Tick::finish(LoopOutcome::Stopped {
        reason: TERMINAL_CANCEL_STOP_REASON.to_string(),
    })
}

/// Watch the one in-flight attempt (§8.5, §17 W3–W6).
#[allow(clippy::too_many_arguments)]
async fn observe(
    port: &Arc<dyn RunPort>,
    deps: &JobDeps,
    marker: &status::JobMarker,
    dispatches: &[DispatchRecord],
    events: &[Event],
    step: &StepPlan,
    attempt: u32,
    now: u64,
    deadline_at: u64,
) -> Tick {
    if step.is_publish() {
        return observe_publish(port, marker, step, attempt, events).await;
    }
    let record = dispatches
        .iter()
        .find(|record| record.step == step.id && record.attempt == attempt);
    let Some(record) = record else {
        if step.is_deliver() {
            return settle_delivery_from_stream(port, marker, step, attempt, events).await;
        }
        let gate = failed_gate(
            attempt,
            DISPATCH_CHECK,
            Some(
                "the attempt has no dispatch record, so no child exists and nothing may be assumed"
                    .to_string(),
            ),
        );
        return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
    };
    let timeout_at = record.at_ms.saturating_add(step.timeout_ms);
    let expiry_at = timeout_at.min(deadline_at);
    let child = match deps.streams.read_stream(record.child).await {
        Ok(child) => child,
        Err(err) => {
            if now >= expiry_at {
                let gate = failed_gate(
                    attempt,
                    TIMEOUT_CHECK,
                    Some(format!("the child stream is unreadable: {err}")),
                );
                return settle_step(port, marker, step, attempt, gate, AttemptOutcome::TimedOut)
                    .await;
            }
            // W3: the record is the placement decision (J3) and the child ids
            // derive from (run, step, attempt), so the very same child is
            // created again. Nothing is re-selected.
            return readmit_child(port, deps, marker, events, step, attempt, record).await;
        }
    };
    if let Some(reason) = end_reason(&child) {
        return settle_agent(port, deps, marker, step, attempt, &child, reason).await;
    }
    if !child_started(&child) {
        if now >= expiry_at {
            let gate = failed_gate(
                attempt,
                TIMEOUT_CHECK,
                Some(format!(
                    "attempt {attempt} never started before {}",
                    if expiry_at == deadline_at {
                        "the run deadline"
                    } else {
                        "the step timeout"
                    }
                )),
            );
            let outcome = attempt_with_effect(step, &child, AttemptOutcome::TimedOut);
            return settle_step(port, marker, step, attempt, gate, outcome).await;
        }
        // W4/W5: the child exists but was never admitted, or its wake was
        // lost. The task key makes the re-admit a no-op when the row is
        // already durable, so a crash on either side of the admit is safe.
        return readmit_child(port, deps, marker, events, step, attempt, record).await;
    }
    if now >= expiry_at {
        let gate = failed_gate(
            attempt,
            TIMEOUT_CHECK,
            Some(format!(
                "attempt {attempt} ran for {} ms without ending",
                now.saturating_sub(record.at_ms)
            )),
        );
        let outcome = attempt_with_effect(step, &child, AttemptOutcome::TimedOut);
        return settle_step(port, marker, step, attempt, gate, outcome).await;
    }
    Tick::park(deps.clamp_wait(expiry_at.saturating_sub(now)))
}

/// Re-drive the child of a recorded attempt that never started (§17 W3/W4).
///
/// The record fixed the placement, so the worker is not chosen again; the ids
/// derive from (run, step, attempt), so prepare yields the same thread and
/// session. admit is idempotent on the task key: a second call after the row
/// landed is a no-op, and create_session returns early when the stream exists.
async fn readmit_child(
    port: &Arc<dyn RunPort>,
    deps: &JobDeps,
    marker: &status::JobMarker,
    events: &[Event],
    step: &StepPlan,
    attempt: u32,
    record: &DispatchRecord,
) -> Tick {
    let step_digest = step.digest();
    if record.step_digest.as_deref() != Some(step_digest.as_str()) {
        let gate = failed_gate(
            attempt,
            CHILD_IDENTITY_CONFLICT_CHECK,
            Some("the recorded dispatch does not match the frozen step digest".to_string()),
        );
        return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
    }
    let task = match child_task_text(
        deps,
        events,
        port.session(),
        step,
        attempt,
        marker.plan.limits.inline_max_bytes,
    )
    .await
    {
        Ok(task) => task,
        Err(detail) => {
            let gate = failed_gate(attempt, INPUT_CHECK, Some(detail));
            return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
        }
    };
    let run_session = port.session();
    let spawn = crate::job::plan::spawn_uid(run_session, &step.id, attempt);
    let thread_id = crate::job::plan::child_thread_id(run_session, spawn);
    let request = SpawnRequest {
        task: Value::String(task),
        correlation: None,
        speculative: false,
        mandate: None,
        exec_worker: Some(record.worker.clone()),
        identity: Some(SpawnIdentity {
            spawn_uid: spawn,
            thread_id,
            task_id: crate::job::plan::step_task_id(run_session, &step.id, attempt),
            agent_def: deps.spawner.worker_template_id().to_string(),
            agent_def_version: deps.spawner.pinned_version(),
            assignee: None,
        }),
        output_schema: step.output_schema.clone(),
        parent: Some(run_session),
    };
    let spawned = deps.spawner.prepare(&request);
    if spawned.worker_session != record.child || thread_id != record.thread_id {
        let gate = failed_gate(
            attempt,
            CHILD_IDENTITY_CONFLICT_CHECK,
            Some("the recorded dispatch does not name the child its ids derive".to_string()),
        );
        return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
    }
    if record.task != spawned.task_message {
        let gate = failed_gate(
            attempt,
            CHILD_IDENTITY_CONFLICT_CHECK,
            Some("the recorded dispatch does not match the frozen step bytes".to_string()),
        );
        return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
    }
    if let Err(err) = deps.spawner.admit(&spawned).await {
        return settle_spawn_error(port, marker, step, attempt, err).await;
    }
    Tick::park(JOB_TICK_MIN_MS)
}

async fn settle_spawn_error(
    port: &Arc<dyn RunPort>,
    marker: &status::JobMarker,
    step: &StepPlan,
    attempt: u32,
    err: SpawnError,
) -> Tick {
    let check = if matches!(err, SpawnError::BindingConflict { .. }) {
        BINDING_CONFLICT_CHECK
    } else {
        DISPATCH_CHECK
    };
    let gate = failed_gate(
        attempt,
        check,
        Some(format!("the child could not be admitted: {err}")),
    );
    settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await
}

/// Whether the child ever took a turn of its own (§17 W5).
fn child_started(events: &[Event]) -> bool {
    events
        .iter()
        .any(|event| event.kind == EventKind::InputAdmitted)
}

fn end_reason(events: &[Event]) -> Option<EndReason> {
    events.iter().rev().find_map(|event| match &event.kind {
        EventKind::SessionEnded { reason } => Some(*reason),
        _ => None,
    })
}

fn attempt_with_effect(
    step: &StepPlan,
    events: &[Event],
    outcome: AttemptOutcome,
) -> AttemptOutcome {
    let signals = EffectSignals {
        tool_calls: events
            .iter()
            .filter(|event| event.kind == EventKind::ToolCall)
            .count() as u64,
        receipt: false,
    };
    if outcome.is_retryable() && effect_gate(step, signals) == EffectGate::NeedsOperator {
        AttemptOutcome::NeedsOperator
    } else {
        outcome
    }
}

/// Settle one agent attempt from the child's own stream (§8.5, §11.2).
async fn settle_agent(
    port: &Arc<dyn RunPort>,
    deps: &JobDeps,
    marker: &status::JobMarker,
    step: &StepPlan,
    attempt: u32,
    child: &[Event],
    reason: EndReason,
) -> Tick {
    let (specs, extra) = declared_checks(step, child);
    let mut gate = evaluate_evidence_gate(&status::step_gate_name(&step.id), &specs, child);
    gate.checks.extend(extra);
    gate.passed = !gate.checks.is_empty() && gate.checks.iter().all(|check| check.passed);
    gate.attempt = u64::from(attempt.saturating_sub(1));
    let verdict = if gate.checks.is_empty() {
        None
    } else {
        Some(gate.passed)
    };
    let mut outcome = attempt_outcome(reason, verdict, true);
    let signals = EffectSignals {
        tool_calls: child
            .iter()
            .filter(|event| event.kind == EventKind::ToolCall)
            .count() as u64,
        receipt: gate
            .checks
            .iter()
            .any(|check| check.name == DELIVERY_CHECK && check.passed),
    };
    if outcome.is_retryable() && effect_gate(step, signals) == EffectGate::NeedsOperator {
        outcome = AttemptOutcome::NeedsOperator;
    }
    if step.produces.is_some() {
        match commit_artifact(port, deps, marker, step, attempt, child).await {
            ArtifactCommit::Ready => {}
            ArtifactCommit::NoArtifact => {}
            ArtifactCommit::Park => return Tick::park(JOB_TICK_MIN_MS),
            ArtifactCommit::Failed { check, detail } => {
                gate.passed = false;
                gate.checks.push(CheckOutcome {
                    name: check.to_string(),
                    passed: false,
                    detail: Some(detail),
                });
                outcome = AttemptOutcome::Failed;
            }
        }
    }
    settle_step(port, marker, step, attempt, gate, outcome).await
}

enum ArtifactCommit {
    Ready,
    NoArtifact,
    Park,
    Failed { check: &'static str, detail: String },
}

/// Commit and verify the one idempotent artifact pair for `(step, attempt)`.
async fn commit_artifact(
    port: &Arc<dyn RunPort>,
    deps: &JobDeps,
    marker: &status::JobMarker,
    step: &StepPlan,
    attempt: u32,
    child: &[Event],
) -> ArtifactCommit {
    let call_id = format!("{ARTIFACT_CALL_PREFIX}{}:{attempt}", step.id);
    let run_events = match port.read_stream().await {
        Ok(events) => events,
        Err(err) => {
            warn!(session = %port.session().0, error = %err, "job run: cannot re-read an artifact pair");
            return ArtifactCommit::Park;
        }
    };
    let call = run_events
        .iter()
        .rev()
        .find(|event| {
            event.kind == EventKind::ToolCall
                && event.bytes.get("call_id").and_then(Value::as_str) == Some(call_id.as_str())
        })
        .cloned();
    let result = run_events
        .iter()
        .rev()
        .find(|event| {
            event.kind == EventKind::ToolResult
                && event.bytes.get("call_id").and_then(Value::as_str) == Some(call_id.as_str())
        })
        .cloned();

    let Some(call) = call else {
        if let Some(result) = result {
            let Some(artifact) = result
                .bytes
                .get("result")
                .and_then(|value| value.get("artifact"))
                .cloned()
            else {
                return ArtifactCommit::Failed {
                    check: "artifact_unavailable",
                    detail: format!("{call_id} has a result without its call"),
                };
            };
            let call = json!({
                "tool": status::ARTIFACT_TOOL,
                "call_id": call_id,
                "args": {
                    "artifact": artifact,
                    "orphan_result": true,
                },
            });
            if let Err(err) = port.emit(EventKind::ToolCall, call).await {
                warn!(session = %port.session().0, error = %err, "job run: cannot repair an orphan artifact call");
                return ArtifactCommit::Park;
            }
            return ArtifactCommit::Park;
        }
        let Some(mut artifact) = artifact_of(step, child) else {
            return ArtifactCommit::NoArtifact;
        };
        artifact["producer"] = json!({
            "run": &marker.run_id,
            "step": &step.id,
            "attempt": attempt,
        });
        if let Err(err) =
            verify_artifact(deps, step, attempt, &marker.run_id, child, &artifact).await
        {
            return manifest_artifact_failure(&err);
        }
        let call = json!({
            "tool": status::ARTIFACT_TOOL,
            "call_id": call_id,
            "args": { "artifact": artifact },
        });
        if let Err(err) = port.emit(EventKind::ToolCall, call).await {
            warn!(session = %port.session().0, error = %err, "job run: cannot record an artifact ref");
            return ArtifactCommit::Park;
        }
        return ArtifactCommit::Park;
    };

    let artifact = call
        .bytes
        .get("args")
        .and_then(|args| args.get("artifact"))
        .cloned();
    let Some(artifact) = artifact else {
        return ArtifactCommit::Failed {
            check: "artifact_unavailable",
            detail: format!("{call_id} carries no artifact ref"),
        };
    };
    if call
        .bytes
        .get("args")
        .and_then(|args| args.get("orphan_result"))
        .and_then(Value::as_bool)
        == Some(true)
    {
        return ArtifactCommit::Failed {
            check: "artifact_unavailable",
            detail: format!("{call_id} was reconstructed from a result without a call"),
        };
    }
    if let Err(err) = verify_artifact(deps, step, attempt, &marker.run_id, child, &artifact).await {
        if result.is_some() {
            return manifest_artifact_failure(&err);
        }
        let result = json!({
            "call_id": call_id,
            "ok": false,
            "error": match &err {
                ArtifactVerifyError::Unavailable(detail)
                | ArtifactVerifyError::DigestMismatch(detail)
                | ArtifactVerifyError::OverCap(detail) => detail,
            },
            "result": { "artifact": artifact },
        });
        if let Err(write) = port.emit(EventKind::ToolResult, result).await {
            warn!(session = %port.session().0, error = %write, "job run: cannot fail an invalid artifact pair");
            return ArtifactCommit::Park;
        }
        return ArtifactCommit::Park;
    }

    if let Some(result) = result {
        if result.bytes.get("ok").and_then(Value::as_bool) != Some(true)
            || result
                .bytes
                .get("result")
                .and_then(|value| value.get("artifact"))
                != Some(&artifact)
        {
            return ArtifactCommit::Failed {
                check: "artifact_unavailable",
                detail: format!("{call_id} result does not verify the paired call"),
            };
        }
        return ArtifactCommit::Ready;
    }

    let result = json!({
        "call_id": call_id,
        "ok": true,
        "result": { "artifact": artifact },
    });
    if let Err(err) = port.emit(EventKind::ToolResult, result).await {
        warn!(session = %port.session().0, error = %err, "job run: cannot close an artifact ref");
        return ArtifactCommit::Park;
    }
    ArtifactCommit::Park
}

enum ArtifactVerifyError {
    Unavailable(String),
    DigestMismatch(String),
    OverCap(String),
}

fn manifest_artifact_failure(err: &ArtifactVerifyError) -> ArtifactCommit {
    match err {
        ArtifactVerifyError::Unavailable(detail) => ArtifactCommit::Failed {
            check: "artifact_unavailable",
            detail: detail.clone(),
        },
        ArtifactVerifyError::DigestMismatch(detail) => ArtifactCommit::Failed {
            check: "artifact_digest_mismatch",
            detail: detail.clone(),
        },
        ArtifactVerifyError::OverCap(detail) => ArtifactCommit::Failed {
            check: "artifact_within_cap",
            detail: detail.clone(),
        },
    }
}

async fn verify_artifact(
    deps: &JobDeps,
    step: &StepPlan,
    attempt: u32,
    run: &str,
    child: &[Event],
    artifact: &Value,
) -> Result<(), ArtifactVerifyError> {
    let unavailable = |why: String| ArtifactVerifyError::Unavailable(why);
    if artifact.get("step").and_then(Value::as_str) != Some(step.id.as_str()) {
        return Err(unavailable("the artifact names another step".to_string()));
    }
    let name = artifact
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| unavailable("the artifact names no artifact".to_string()))?;
    if artifact.get("media").and_then(Value::as_str) != Some("text") {
        return Err(unavailable(
            "the artifact is not a text artifact".to_string(),
        ));
    }
    let produces = step
        .produces
        .as_ref()
        .filter(|produces| produces.name == name)
        .ok_or_else(|| unavailable(format!("step {} does not produce {name}", step.id)))?;
    let reference = artifact
        .get("ref")
        .ok_or_else(|| unavailable("the artifact names no event".to_string()))?;
    let session = reference
        .get("session")
        .and_then(Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
        .map(SessionId)
        .ok_or_else(|| unavailable("the artifact names no producer session".to_string()))?;
    let seq = reference
        .get("seq")
        .and_then(Value::as_u64)
        .ok_or_else(|| unavailable("the artifact names no producer seq".to_string()))?;
    let producer = artifact
        .get("producer")
        .ok_or_else(|| unavailable("the artifact names no producer".to_string()))?;
    if producer.get("run").and_then(Value::as_str) != Some(run)
        || producer.get("step").and_then(Value::as_str) != Some(step.id.as_str())
        || producer.get("attempt").and_then(Value::as_u64) != Some(u64::from(attempt))
    {
        return Err(unavailable(
            "the artifact producer does not match this step attempt".to_string(),
        ));
    }
    let events = if child.first().is_some_and(|event| event.session == session) {
        child.to_vec()
    } else {
        deps.streams
            .read_stream(session)
            .await
            .map_err(|error| unavailable(format!("the producer stream is unreadable: {error}")))?
    };
    let event = events
        .iter()
        .find(|event| event.seq == seq)
        .ok_or_else(|| unavailable(format!("the producer has no event at seq {seq}")))?;
    let text = message_text(event)
        .ok_or_else(|| unavailable(format!("producer event {seq} carries no text")))?;
    let bytes = artifact.get("bytes").and_then(Value::as_u64);
    if bytes != Some(text.len() as u64) {
        return Err(ArtifactVerifyError::DigestMismatch(format!(
            "the artifact declares {bytes:?} bytes but the producer committed {}",
            text.len()
        )));
    }
    let expected = artifact
        .get("sha256")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let computed = crate::job::plan::digest_text(&text);
    if expected != computed {
        return Err(ArtifactVerifyError::DigestMismatch(format!(
            "the producer bytes hash to {computed}, not {expected}"
        )));
    }
    if text.len() as u64 > produces.max_bytes {
        return Err(ArtifactVerifyError::OverCap(format!(
            "{name} is {} bytes, above the cap of {}",
            text.len(),
            produces.max_bytes
        )));
    }
    Ok(())
}

/// Settle one attempt and park one tick so the next tick reads it back (§8.5).
async fn settle_step(
    port: &Arc<dyn RunPort>,
    marker: &status::JobMarker,
    step: &StepPlan,
    _attempt: u32,
    gate: GateVerdict,
    outcome: AttemptOutcome,
) -> Tick {
    let payload = status::step_verdict_payload(marker.run_id.as_str(), &step.id, &gate, outcome);
    if let Err(err) = port.emit(EventKind::EvidenceVerdict, payload).await {
        warn!(session = %port.session().0, error = %err, "job run: cannot write the step verdict");
    }
    Tick::park(JOB_TICK_MIN_MS)
}

/// Settle the whole run (§5.5, §11.3): one run verdict, then the alert, then
/// the outcome the engine maps onto the session end.
async fn settle_run(
    port: &Arc<dyn RunPort>,
    deps: &JobDeps,
    marker: &status::JobMarker,
    facts: &[StepFacts],
    artifacts: &[ArtifactFact],
    deadline_passed: bool,
    events: &[Event],
) -> Tick {
    let plan = &marker.plan;
    let halted = run_halted(plan, facts);
    let run_facts = RunFacts {
        steps: facts,
        artifacts,
        accepted: &plan.limits.accept_unverified,
        halted,
        deadline_passed,
    };
    let outcome = run_outcome(plan, &run_facts, false);
    let steps = step_outcomes(plan, facts);
    let (failed_step, reason) = run_failure_reason(plan, facts, events, deadline_passed);
    let payload = status::run_verdict_payload(
        Some(marker.run_id.as_str()),
        outcome,
        &marker.plan_digest,
        &steps,
        failed_step.as_deref(),
        reason.as_deref(),
    );
    if let Err(err) = port.emit(EventKind::EvidenceVerdict, payload).await {
        warn!(session = %port.session().0, error = %err, "job run: cannot write the run verdict");
        return Tick::park(JOB_TICK_MIN_MS);
    }
    admit_alerts(
        deps,
        marker,
        outcome,
        failed_step.as_deref(),
        reason.as_deref(),
    )
    .await;
    Tick::finish(outcome_to_loop(outcome))
}

fn run_failure_reason(
    plan: &JobPlan,
    facts: &[StepFacts],
    events: &[Event],
    deadline_passed: bool,
) -> (Option<String>, Option<String>) {
    if let Some(step) = plan.steps.iter().find(|step| {
        facts
            .iter()
            .find(|known| known.step == step.id)
            .and_then(StepFacts::outcome)
            == Some(AttemptOutcome::NeedsOperator)
    }) {
        return (Some(step.id.clone()), Some("effect_unknown".to_string()));
    }

    if let Some(step) = plan.steps.iter().find(|step| {
        events.iter().any(|event| {
            event.kind == EventKind::EvidenceVerdict
                && event.bytes.get("gate").and_then(Value::as_str)
                    == Some(status::step_gate_name(&step.id).as_str())
                && event
                    .bytes
                    .get("checks")
                    .and_then(Value::as_array)
                    .is_some_and(|checks| {
                        checks.iter().any(|check| {
                            check.get("name").and_then(Value::as_str)
                                == Some(CHILD_IDENTITY_CONFLICT_CHECK)
                                && check.get("passed").and_then(Value::as_bool) == Some(false)
                        })
                    })
        })
    }) {
        return (
            Some(step.id.clone()),
            Some("child_identity_conflict".to_string()),
        );
    }

    if deadline_passed {
        let failed_step = plan.steps.iter().find_map(|step| {
            let outcome = facts
                .iter()
                .find(|known| known.step == step.id)
                .and_then(StepFacts::outcome);
            (!step.optional && outcome != Some(AttemptOutcome::Succeeded)).then(|| step.id.clone())
        });
        return (failed_step, Some("deadline_exceeded".to_string()));
    }

    for step in &plan.steps {
        let outcome = facts
            .iter()
            .find(|known| known.step == step.id)
            .and_then(StepFacts::outcome);
        let reason = match outcome {
            Some(AttemptOutcome::Succeeded) => continue,
            Some(AttemptOutcome::Unverified) => {
                if step.optional || plan.limits.accept_unverified.contains(&step.id) {
                    continue;
                }
                Some("step_failed")
            }
            Some(AttemptOutcome::NeedsOperator) => {
                return (Some(step.id.clone()), Some("effect_unknown".to_string()));
            }
            Some(AttemptOutcome::Skipped) | None => {
                if step.optional {
                    continue;
                }
                Some("dependency_failed")
            }
            Some(AttemptOutcome::Interrupted) => (!step.optional).then_some("step_interrupted"),
            Some(AttemptOutcome::TimedOut) => (!step.optional).then_some("step_timed_out"),
            Some(AttemptOutcome::InvalidOutput) => {
                (!step.optional).then_some("step_invalid_output")
            }
            Some(AttemptOutcome::Failed) => (!step.optional).then_some("step_failed"),
        };
        if let Some(reason) = reason {
            return (Some(step.id.clone()), Some(reason.to_string()));
        }
    }
    (None, None)
}

/// Admit one alert run per declared alert (§13.3). A cancelled run is
/// deliberate and never alerts; an alert that cannot be admitted is logged and
/// never fails the run.
async fn admit_alerts(
    deps: &JobDeps,
    marker: &status::JobMarker,
    outcome: RunOutcome,
    failed_step: Option<&str>,
    detail: Option<&str>,
) {
    if outcome != RunOutcome::Failed {
        return;
    }
    let facts = AlertFacts {
        failed_run: &marker.run_id,
        outcome,
        failed_step,
        detail,
    };
    for spec in deps.alerts.alerts_for(&marker.job_id) {
        let admitted = alert::admission(&spec, &facts);
        if let Err(err) = deps
            .alerts
            .raise(admitted.session, admitted.payload, admitted.key)
            .await
        {
            warn!(
                run = %marker.run_id,
                alert = %spec.id,
                error = %err,
                "job run: cannot admit the alert"
            );
        }
    }
}

async fn dispatch_attempt(
    port: &Arc<dyn RunPort>,
    deps: &JobDeps,
    marker: &status::JobMarker,
    facts: &[StepFacts],
    events: &[Event],
    step: &StepPlan,
    attempt: u32,
) -> Tick {
    if step.is_deliver() {
        return execute_deliver(port, deps, marker, step, attempt, events).await;
    }
    if step.is_publish() {
        return execute_publish(port, deps, marker, events, step, attempt).await;
    }
    dispatch_agent(port, deps, marker, facts, events, step, attempt).await
}

/// Dispatch one agent attempt (§8.3): select a placement, commit the record,
/// then create and admit the child. The record comes first, always.
async fn dispatch_agent(
    port: &Arc<dyn RunPort>,
    deps: &JobDeps,
    marker: &status::JobMarker,
    facts: &[StepFacts],
    events: &[Event],
    step: &StepPlan,
    attempt: u32,
) -> Tick {
    let task = match child_task_text(
        deps,
        events,
        port.session(),
        step,
        attempt,
        marker.plan.limits.inline_max_bytes,
    )
    .await
    {
        Ok(task) => task,
        Err(detail) => {
            let gate = failed_gate(attempt, INPUT_CHECK, Some(detail));
            return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
        }
    };
    let members = match deps.directory.members().await {
        Ok(members) => members,
        Err(err) => {
            warn!(session = %port.session().0, error = %err.0, "job run: the worker catalog is unreadable");
            return Tick::park(deps.clamp_wait(JOB_DEFER_TICK_MS));
        }
    };
    let tried: Vec<String> = facts
        .iter()
        .find(|known| known.step == step.id)
        .map(StepFacts::exhausted_workers)
        .unwrap_or_default()
        .into_iter()
        .map(str::to_string)
        .collect();
    let busy = busy_workers(deps, &status::dispatches(events)).await;
    let placement: Placement = match placement::select(
        step,
        &marker.plan.limits,
        &PlacementFacts {
            members: &members,
            busy: &busy,
            tried: &tried,
        },
    ) {
        Ok(placement) => placement,
        Err(PlacementRefusal::AllBusy { workers }) => {
            let now = deps.clock.now_ms();
            let due = defer_due_at(events, step, facts, attempt)
                .saturating_add(marker.plan.limits.defer_budget_ms);
            if now >= due {
                let gate = failed_gate(
                    attempt,
                    NO_CANDIDATE_CHECK,
                    Some(format!(
                        "all_busy: every capable worker is busy: {}",
                        workers.join(", ")
                    )),
                );
                return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed)
                    .await;
            }
            return Tick::park(deps.clamp_wait(due - now));
        }
        Err(refusal) => {
            let gate = failed_gate(attempt, NO_CANDIDATE_CHECK, Some(refusal_detail(&refusal)));
            return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
        }
    };

    let run_session = port.session();
    let spawn = crate::job::plan::spawn_uid(run_session, &step.id, attempt);
    let identity = SpawnIdentity {
        spawn_uid: spawn,
        thread_id: crate::job::plan::child_thread_id(run_session, spawn),
        task_id: crate::job::plan::step_task_id(run_session, &step.id, attempt),
        agent_def: deps.spawner.worker_template_id().to_string(),
        agent_def_version: deps.spawner.pinned_version(),
        assignee: None,
    };
    let request = SpawnRequest {
        task: Value::String(task),
        correlation: None,
        speculative: false,
        mandate: None,
        exec_worker: Some(placement.worker.clone()),
        identity: Some(identity),
        output_schema: step.output_schema.clone(),
        parent: Some(run_session),
    };
    let spawned = deps.spawner.prepare(&request);
    let step_digest = step.digest();

    if let Some(existing) = status::dispatches(events)
        .into_iter()
        .find(|record| record.step == step.id && record.attempt == attempt)
        && (existing.child != spawned.worker_session
            || existing.thread_id != spawned.thread.id
            || existing.step_digest.as_deref() != Some(step_digest.as_str())
            || existing.task != spawned.task_message)
    {
        let gate = failed_gate(
            attempt,
            CHILD_IDENTITY_CONFLICT_CHECK,
            Some("the existing dispatch names different frozen bytes".to_string()),
        );
        return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
    }
    if let Ok(child) = deps.streams.read_stream(spawned.worker_session).await
        && !child.is_empty()
        && !child.iter().any(|event| {
            event.kind == EventKind::InputAdmitted && event.bytes == spawned.task_message
        })
    {
        let gate = failed_gate(
            attempt,
            CHILD_IDENTITY_CONFLICT_CHECK,
            Some("the derived child session already carries a different task".to_string()),
        );
        return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
    }

    let mut record = thread_started_payload_bound(
        &spawned.thread,
        spawned.worker_session,
        &spawned.task_message,
        &placement.worker,
    );
    record[status::JOB_KEY] = json!({
        "run": marker.run_id,
        "step": step.id,
        "attempt": attempt,
        "step_digest": step_digest,
        "effect": effect_name(effective_effect(step)),
        "candidates": placement.candidates.iter().map(|candidate| candidate.id.clone()).collect::<Vec<_>>(),
        "rationale": placement.rationale(),
    });
    if let Err(err) = port.emit(EventKind::ThreadStarted, record).await {
        warn!(session = %port.session().0, error = %err, "job run: cannot record a dispatch");
        return Tick::park(deps.clamp_wait(JOB_DEFER_TICK_MS));
    }
    if let Err(err) = deps.spawner.admit(&spawned).await {
        return settle_spawn_error(port, marker, step, attempt, err).await;
    }
    Tick::park(JOB_TICK_MIN_MS)
}

async fn busy_workers(deps: &JobDeps, dispatches: &[status::DispatchRecord]) -> Vec<String> {
    let mut busy = Vec::new();
    for record in dispatches {
        if let Ok(events) = deps.streams.read_stream(record.child).await
            && thread_liveness(&events) == ThreadLiveness::Live
            && !busy.contains(&record.worker)
        {
            busy.push(record.worker.clone());
        }
    }
    busy
}

fn defer_due_at(events: &[Event], step: &StepPlan, facts: &[StepFacts], attempt: u32) -> u64 {
    let mut anchor = run_input_ts(events).unwrap_or(0);
    for dependency in &step.depends_on {
        if let Some(settled) = facts
            .iter()
            .find(|known| known.step == *dependency)
            .and_then(StepFacts::last)
        {
            anchor = anchor.max(settled.ended_ms);
        }
    }
    if attempt > 1
        && let Some(previous) = facts
            .iter()
            .find(|known| known.step == step.id)
            .and_then(StepFacts::last)
    {
        anchor = anchor.max(
            previous
                .ended_ms
                .saturating_add(crate::job::outcome::retry_backoff_ms(attempt as usize - 1)),
        );
    }
    anchor
}

/// Run one deliver step in the conductor's own drain (§13.2). Not placed: no
/// worker can be selected into the send path, so the spec's escalation case
/// cannot arise.
async fn execute_deliver(
    port: &Arc<dyn RunPort>,
    deps: &JobDeps,
    marker: &status::JobMarker,
    step: &StepPlan,
    attempt: u32,
    events: &[Event],
) -> Tick {
    let Some(deliver) = step.deliver() else {
        let gate = failed_gate(
            attempt,
            DELIVERY_CHECK,
            Some("the step is not a well-formed deliver step".to_string()),
        );
        return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
    };
    let call_id = format!("{DELIVER_CALL_PREFIX}{}:{attempt}", step.id);
    if delivery_has_fact(events, &call_id) {
        return settle_delivery_from_stream(port, marker, step, attempt, events).await;
    }

    let text = match (&deliver.content.text, &deliver.content.from_artifact) {
        (Some(text), _) => text.clone(),
        (None, Some(from)) => {
            let Some(artifact) = artifact_intent(events, &from.step, &from.name) else {
                let gate = failed_gate(
                    attempt,
                    "artifact_unavailable",
                    Some(format!(
                        "artifact {} from step {} is not committed",
                        from.name, from.step
                    )),
                );
                return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed)
                    .await;
            };
            let cap = marker
                .plan
                .step(&from.step)
                .and_then(|producer| producer.produces.as_ref())
                .filter(|produces| produces.name == from.name)
                .map(|produces| produces.max_bytes)
                .unwrap_or(0);
            if artifact
                .get("bytes")
                .and_then(Value::as_u64)
                .is_none_or(|bytes| bytes > cap)
            {
                let gate = failed_gate(
                    attempt,
                    "artifact_unavailable",
                    Some(format!(
                        "artifact {} from step {} is missing or above its cap",
                        from.name, from.step
                    )),
                );
                return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed)
                    .await;
            }
            match verified_artifact_text(deps.streams.as_ref(), &artifact).await {
                Ok(text) => text,
                Err((check, detail)) => {
                    let gate = failed_gate(attempt, check, Some(detail));
                    return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed)
                        .await;
                }
            }
        }
        (None, None) => {
            let gate = failed_gate(
                attempt,
                DELIVERY_CHECK,
                Some("the deliver step carries no content".to_string()),
            );
            return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
        }
    };
    let shaped = crate::policy::ImPolicy::default().enforce(&text);
    let content = json!({ "text": shaped.text });
    let content_digest = crate::job::plan::digest_json(&content);
    let intent = json!({
        "tool": DELIVER_TOOL,
        "call_id": call_id,
        "args": {
            "channel": deliver.channel,
            "target": deliver.target,
            "content": content,
            "content_sha256": content_digest,
        },
    });
    if let Err(err) = port.emit(EventKind::ToolCall, intent).await {
        warn!(session = %port.session().0, error = %err, "job run: cannot record a delivery intent");
        return Tick::park(JOB_DEFER_TICK_MS);
    }
    match deps
        .deliver
        .send(deliver.channel, deliver.target.clone(), content.clone())
        .await
    {
        Ok(receipt) => {
            let delivered = json!({
                "call_id": call_id,
                "channel": deliver.channel,
                "target": deliver.target,
                "policy": DELIVERY_POLICY,
                "attempt": attempt,
                "words": shaped.words,
                "parts_sent": 1,
                "parts_total": 1,
                "truncated": shaped.truncated,
                "first_attempt_ms": 0,
                "detail": { "message_id": receipt.message_id },
            });
            if let Err(err) = port.emit(EventKind::ChannelDelivered, delivered).await {
                warn!(session = %port.session().0, error = %err, "job run: cannot write a delivery receipt");
                return Tick::park(JOB_TICK_MIN_MS);
            }
            let reread = match port.read_stream().await {
                Ok(events) => events,
                Err(err) => {
                    warn!(session = %port.session().0, error = %err, "job run: cannot re-read a committed receipt");
                    return Tick::park(JOB_TICK_MIN_MS);
                }
            };
            settle_delivery_from_stream(port, marker, step, attempt, &reread).await
        }
        Err(err) => {
            let (disposition, needs_operator) = match &err {
                ChannelError::Unavailable(_) => ("unavailable", false),
                ChannelError::Timeout => ("may_have_completed", true),
                ChannelError::DeliveryFailed(_) => ("may_have_completed", true),
            };
            let result = json!({
                "call_id": call_id,
                "ok": false,
                "error": err.to_string(),
                "disposition": disposition,
                "needs_operator": needs_operator,
                "channel": deliver.channel,
                "target": deliver.target,
            });
            if let Err(write) = port.emit(EventKind::ToolResult, result).await {
                warn!(session = %port.session().0, error = %write, "job run: cannot close a delivery intent");
                return Tick::park(JOB_TICK_MIN_MS);
            }
            let reread = match port.read_stream().await {
                Ok(events) => events,
                Err(write) => {
                    warn!(session = %port.session().0, error = %write, "job run: cannot re-read a delivery result");
                    return Tick::park(JOB_TICK_MIN_MS);
                }
            };
            settle_delivery_from_stream(port, marker, step, attempt, &reread).await
        }
    }
}

fn delivery_has_fact(events: &[Event], call_id: &str) -> bool {
    events.iter().any(|event| {
        ((event.kind == EventKind::ToolCall || event.kind == EventKind::ToolResult)
            && event.bytes.get("call_id").and_then(Value::as_str) == Some(call_id))
            || (event.kind == EventKind::ChannelDelivered
                && event.bytes.get("call_id").and_then(Value::as_str) == Some(call_id))
    })
}

async fn settle_delivery_from_stream(
    port: &Arc<dyn RunPort>,
    marker: &status::JobMarker,
    step: &StepPlan,
    attempt: u32,
    events: &[Event],
) -> Tick {
    let Some(deliver) = step.deliver() else {
        let gate = failed_gate(
            attempt,
            DELIVERY_CHECK,
            Some("the step is not a well-formed deliver step".to_string()),
        );
        return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
    };
    let call_id = format!("{DELIVER_CALL_PREFIX}{}:{attempt}", step.id);
    let intent = events.iter().rev().find(|event| {
        event.kind == EventKind::ToolCall
            && event.bytes.get("call_id").and_then(Value::as_str) == Some(call_id.as_str())
    });
    let receipt = events.iter().rev().find(|event| {
        event.kind == EventKind::ChannelDelivered
            && event.bytes.get("call_id").and_then(Value::as_str) == Some(call_id.as_str())
    });
    let result = events.iter().rev().find(|event| {
        event.kind == EventKind::ToolResult
            && event.bytes.get("call_id").and_then(Value::as_str) == Some(call_id.as_str())
    });

    if let Some(receipt) = receipt {
        if let Err(detail) = delivery_receipt_matches(receipt, &call_id, attempt, deliver) {
            let gate = failed_gate(attempt, DELIVERY_CHECK, Some(detail));
            return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
        }
        let later_error = events.iter().any(|event| {
            event.seq > receipt.seq
                && event.kind == EventKind::ToolResult
                && event.bytes.get("call_id").and_then(Value::as_str) == Some(call_id.as_str())
                && event.bytes.get("ok").and_then(Value::as_bool) == Some(false)
        });
        if later_error {
            let gate = failed_gate(
                attempt,
                DELIVERY_CHECK,
                Some("an error result followed the receipt".to_string()),
            );
            return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
        }
        let ok_result = events.iter().any(|event| {
            event.seq > receipt.seq
                && event.kind == EventKind::ToolResult
                && event.bytes.get("call_id").and_then(Value::as_str) == Some(call_id.as_str())
                && event.bytes.get("ok").and_then(Value::as_bool) == Some(true)
        });
        if !ok_result {
            let message_id = receipt
                .bytes
                .get("detail")
                .and_then(|detail| detail.get("message_id"))
                .cloned()
                .unwrap_or(Value::Null);
            let result = json!({
                "call_id": call_id,
                "ok": true,
                "result": { "message_id": message_id },
            });
            if let Err(err) = port.emit(EventKind::ToolResult, result).await {
                warn!(session = %port.session().0, error = %err, "job run: cannot repair a delivery result");
                return Tick::park(JOB_TICK_MIN_MS);
            }
        }
        let gate = passed_gate(
            attempt,
            DELIVERY_CHECK,
            Some(format!("receipt at seq {}", receipt.seq)),
        );
        return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Succeeded).await;
    }

    if let Some(result) = result {
        if result.bytes.get("ok").and_then(Value::as_bool) == Some(true) {
            let gate = failed_gate(
                attempt,
                DELIVERY_CHECK,
                Some("an ok result without a receipt is not delivery evidence".to_string()),
            );
            return settle_step(
                port,
                marker,
                step,
                attempt,
                gate,
                AttemptOutcome::NeedsOperator,
            )
            .await;
        }
        let disposition = result
            .bytes
            .get("disposition")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let retryable_failure = matches!(disposition, "not_started" | "unavailable");
        let outcome = if retryable_failure {
            AttemptOutcome::Failed
        } else {
            AttemptOutcome::NeedsOperator
        };
        let gate = failed_gate(
            attempt,
            DELIVERY_CHECK,
            Some(format!(
                "delivery disposition: {disposition}; the message may have landed"
            )),
        );
        return settle_step(port, marker, step, attempt, gate, outcome).await;
    }

    let detail = if intent.is_some() {
        "the conductor has a delivery intent but no receipt: the send may have landed"
    } else {
        "the attempt has no committed delivery fact"
    };
    let gate = failed_gate(attempt, DELIVERY_CHECK, Some(detail.to_string()));
    let outcome = if intent.is_some() {
        AttemptOutcome::NeedsOperator
    } else {
        AttemptOutcome::Failed
    };
    settle_step(port, marker, step, attempt, gate, outcome).await
}

fn delivery_receipt_matches(
    receipt: &Event,
    call_id: &str,
    attempt: u32,
    deliver: crate::job::plan::DeliverSpecRef<'_>,
) -> Result<(), String> {
    if receipt.bytes.get("call_id").and_then(Value::as_str) != Some(call_id) {
        return Err("the receipt names another call_id".to_string());
    }
    if receipt.bytes.get("attempt").and_then(Value::as_u64) != Some(u64::from(attempt)) {
        return Err("the receipt names another attempt".to_string());
    }
    if receipt.bytes.get("channel").and_then(Value::as_str) != Some(deliver.channel) {
        return Err("the receipt names another channel".to_string());
    }
    if receipt.bytes.get("target") != Some(deliver.target) {
        return Err("the receipt names another target".to_string());
    }
    let sent = receipt.bytes.get("parts_sent").and_then(Value::as_u64);
    let total = receipt.bytes.get("parts_total").and_then(Value::as_u64);
    if sent.is_none() || sent != total {
        return Err("the receipt does not prove every part was delivered".to_string());
    }
    Ok(())
}

/// The branch a frozen publish derives from its run (§9.4 step 3).
#[must_use]
pub fn publish_branch(run: SessionId) -> String {
    format!("selfdev/{}", run.0)
}

/// Observe an in-flight frozen publish attempt (§9.4, §12): a committed receipt
/// settles it from the same bytes a re-drive reads, and an intent without one
/// is a write whose end nobody saw.
async fn observe_publish(
    port: &Arc<dyn RunPort>,
    marker: &status::JobMarker,
    step: &StepPlan,
    attempt: u32,
    events: &[Event],
) -> Tick {
    let call_id = format!("{PUBLISH_CALL_PREFIX}{}:{attempt}", step.id);
    let branch = publish_branch(port.session());
    if let Some(result) = events.iter().rev().find(|event| {
        event.kind == EventKind::ToolResult
            && event.bytes.get("call_id").and_then(Value::as_str) == Some(call_id.as_str())
    }) {
        return settle_publish(port, marker, step, attempt, &result.bytes, &branch).await;
    }
    let attempted = events.iter().any(|event| {
        event.kind == EventKind::ToolCall
            && event.bytes.get("call_id").and_then(Value::as_str) == Some(call_id.as_str())
    });
    let (detail, outcome) = if attempted {
        (
            "the conductor stopped between the publish intent and its receipt: the worktree may exist and what landed is unknown",
            AttemptOutcome::NeedsOperator,
        )
    } else {
        (
            "the attempt has no publish intent, so nothing was written",
            AttemptOutcome::Failed,
        )
    };
    let gate = failed_gate(attempt, PUBLISH_CHECK, Some(detail.to_string()));
    settle_step(port, marker, step, attempt, gate, outcome).await
}

/// Run one frozen publish step in the conductor's own drain (§9.4). The intent is
/// committed before the first write and the receipt after the last one: the
/// pair is the only evidence, so a crash between them leaves a fact an
/// operator can act on.
async fn execute_publish(
    port: &Arc<dyn RunPort>,
    deps: &JobDeps,
    marker: &status::JobMarker,
    events: &[Event],
    step: &StepPlan,
    attempt: u32,
) -> Tick {
    let Some(publish) = step.publish() else {
        let gate = failed_gate(
            attempt,
            PUBLISH_CHECK,
            Some("invalid publish step: the step carries no publish block".to_string()),
        );
        return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
    };
    let Some(input) = step.inputs.first() else {
        let gate = failed_gate(
            attempt,
            INPUT_CHECK,
            Some("the step names no input to publish".to_string()),
        );
        return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
    };
    let Some(from) = artifact_intent(events, &input.step, &input.name) else {
        let gate = failed_gate(
            attempt,
            INPUT_CHECK,
            Some(format!(
                "artifact {} from step {} was not produced",
                input.name, input.step
            )),
        );
        return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
    };
    let Some(targets) = deps.publish.derive(port.session()) else {
        let gate = failed_gate(
            attempt,
            PUBLISH_CHECK,
            Some(PUBLISH_NOT_CONFIGURED.to_string()),
        );
        return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
    };
    let base_commit = marker
        .selfdev
        .as_ref()
        .and_then(|selfdev| selfdev.get("base_commit"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if base_commit.is_empty() {
        let gate = failed_gate(
            attempt,
            PUBLISH_CHECK,
            Some(
                "the marker carries no base commit: the run was not admitted from an evidence pack"
                    .to_string(),
            ),
        );
        return settle_step(port, marker, step, attempt, gate, AttemptOutcome::Failed).await;
    }

    let branch = publish_branch(port.session());
    let call_id = format!("{PUBLISH_CALL_PREFIX}{}:{attempt}", step.id);
    let patch = match verified_patch(deps, &from).await {
        Ok(patch) => patch,
        Err(detail) => {
            return fail_publish(port, marker, step, attempt, &call_id, &branch, detail).await;
        }
    };
    let intent = json!({
        "tool": PUBLISH_TOOL,
        "call_id": call_id,
        "args": {
            "step": &step.id,
            "attempt": attempt,
            "from": &from,
            "base_commit": &base_commit,
            "branch": &branch,
            "worktree": &targets.worktree,
        },
    });
    if let Err(err) = port.emit(EventKind::ToolCall, intent).await {
        warn!(session = %port.session().0, error = %err, "job run: cannot record a publish intent");
        return Tick::park(JOB_DEFER_TICK_MS);
    }

    let request = PublishRequest {
        step: step.id.clone(),
        attempt,
        from: from.clone(),
        patch,
        base_commit,
        branch: branch.clone(),
        worktree: targets.worktree,
        subject: format!("selfdev: the proposal from run {}", port.session().0),
        trailers: publish_trailers(marker, &from),
        base: publish.base.clone(),
        gates: publish.gates,
        push: publish.push,
        pull_request: publish.pull_request,
    };
    let result = match deps.publish.publish(request).await {
        Ok(receipt) => {
            let receipt = publish_receipt_json(&receipt);
            let verdict = publish_receipt_ok(
                &receipt,
                &branch,
                publish.gates,
                publish.push,
                publish.pull_request,
            );
            let ok = verdict.is_ok();
            let mut result = json!({ "call_id": call_id, "ok": ok, "result": receipt });
            if let Err(detail) = verdict {
                result["error"] = Value::String(detail);
            }
            result
        }
        Err(err) => json!({
            "call_id": call_id,
            "ok": false,
            "error": err.detail(),
            "needs_operator": matches!(
                err,
                PublishError::Occupied(_) | PublishError::Unknown(_) | PublishError::Partial(_)
            ),
        }),
    };
    if let Err(err) = port.emit(EventKind::ToolResult, result.clone()).await {
        warn!(session = %port.session().0, error = %err, "job run: cannot close a publish intent");
        return Tick::park(JOB_TICK_MIN_MS);
    }
    settle_publish(port, marker, step, attempt, &result, &branch).await
}

/// Fail one publish attempt before any write: the intent stands, and the
/// receipt says why nothing was published (§9.4 step 5).
async fn fail_publish(
    port: &Arc<dyn RunPort>,
    marker: &status::JobMarker,
    step: &StepPlan,
    attempt: u32,
    call_id: &str,
    branch: &str,
    detail: String,
) -> Tick {
    let result = json!({ "call_id": call_id, "ok": false, "error": detail });
    if let Err(err) = port.emit(EventKind::ToolResult, result.clone()).await {
        warn!(session = %port.session().0, error = %err, "job run: cannot close a publish intent");
        return Tick::park(JOB_TICK_MIN_MS);
    }
    settle_publish(port, marker, step, attempt, &result, branch).await
}

/// Settle one publish attempt from its committed receipt (§9.4, A17). The
/// gate is derived from the receipt, so a re-drive after a crash between the
/// receipt and the verdict decides the same way.
async fn settle_publish(
    port: &Arc<dyn RunPort>,
    marker: &status::JobMarker,
    step: &StepPlan,
    attempt: u32,
    result: &Value,
    branch: &str,
) -> Tick {
    let publish = step.publish();
    let gates = publish.is_none_or(|publish| publish.gates);
    let push = publish.is_some_and(|publish| publish.push);
    let pull_request = publish.is_some_and(|publish| publish.pull_request);
    let mut checks = vec![check_publish_ok(result, branch, gates, push, pull_request)];
    for check in &step.verify {
        if matches!(check, JobCheck::PublishOk) {
            continue;
        }
        checks.push(CheckOutcome {
            name: check.name().to_string(),
            passed: false,
            detail: Some(
                "a publish step is judged by its receipt: this check is not evaluable".to_string(),
            ),
        });
    }
    let passed = checks.iter().all(|check| check.passed);
    let gate = GateVerdict {
        passed,
        checks,
        attempt: u64::from(attempt.saturating_sub(1)),
    };
    let needs_operator = result
        .get("needs_operator")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let outcome = if passed {
        AttemptOutcome::Succeeded
    } else if needs_operator {
        AttemptOutcome::NeedsOperator
    } else {
        AttemptOutcome::Failed
    };
    settle_step(port, marker, step, attempt, gate, outcome).await
}

/// Judge one publish receipt (§9.4): a full commit, the derived branch, a gate
/// that exited zero when the plan asked for one, and, when the plan asked for
/// them, the push of that branch and one pull request from it.
#[must_use]
pub fn check_publish_ok(
    result: &Value,
    branch: &str,
    gates: bool,
    push: bool,
    pull_request: bool,
) -> CheckOutcome {
    let fail = |detail: String| CheckOutcome {
        name: PUBLISH_CHECK.to_string(),
        passed: false,
        detail: Some(detail),
    };
    if result.get("ok").and_then(Value::as_bool) != Some(true) {
        let detail = result
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or("the publish produced no receipt");
        return fail(detail.to_string());
    }
    let receipt = result.get("result").cloned().unwrap_or(Value::Null);
    let commit = receipt
        .get("commit")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !is_full_commit(commit) {
        return fail(format!("the receipt names no full commit: {commit:?}"));
    }
    let named = receipt
        .get("branch")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if named != branch {
        return fail(format!(
            "the receipt names branch {named:?}, not the derived {branch:?}"
        ));
    }
    if gates {
        let Some(gate) = receipt.get("gate") else {
            return fail(
                "the receipt carries no gate: the configured gate did not run".to_string(),
            );
        };
        if gate.get("exit_code").and_then(Value::as_i64) != Some(0) {
            let code = gate
                .get("exit_code")
                .map_or_else(|| "nothing".to_string(), |code| code.to_string());
            return fail(format!("the gate exited {code}, not 0"));
        }
    }
    if push {
        let Some(pushed) = receipt.get("push") else {
            return fail(
                "the receipt carries no push: the branch never left this machine".to_string(),
            );
        };
        let expected = format!("refs/heads/{branch}");
        let reference = pushed
            .get("ref")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if reference != expected {
            return fail(format!(
                "the receipt names ref {reference:?}, not the pushed {expected:?}"
            ));
        }
    }
    if pull_request {
        let Some(request) = receipt.get("pull_request") else {
            return fail(
                "the receipt carries no pull request: none was opened for the branch".to_string(),
            );
        };
        let head = request
            .get("head")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if head != branch {
            return fail(format!(
                "the pull request names head {head:?}, not the pushed {branch:?}"
            ));
        }
    }
    CheckOutcome {
        name: PUBLISH_CHECK.to_string(),
        passed: true,
        detail: Some(format!("receipt {commit}")),
    }
}

fn publish_receipt_ok(
    receipt: &Value,
    branch: &str,
    gates: bool,
    push: bool,
    pull_request: bool,
) -> Result<(), String> {
    let wrapped = json!({ "ok": true, "result": receipt });
    let outcome = check_publish_ok(&wrapped, branch, gates, push, pull_request);
    if outcome.passed {
        Ok(())
    } else {
        Err(outcome
            .detail
            .unwrap_or_else(|| "the receipt does not satisfy the publish contract".to_string()))
    }
}

fn is_full_commit(commit: &str) -> bool {
    commit.len() == 40 && commit.chars().all(|c| c.is_ascii_hexdigit())
}

pub fn publish_receipt_json(receipt: &PublishReceipt) -> Value {
    let mut value = json!({
        "branch": &receipt.branch,
        "worktree": &receipt.worktree,
        "commit": &receipt.commit,
    });
    if let Some(gate) = &receipt.gate {
        value["gate"] = json!({
            "command": &gate.command,
            "exit_code": gate.exit_code,
            "digest": &gate.digest,
            "output_tail": &gate.output_tail,
        });
    }
    if let Some(push) = &receipt.push {
        value["push"] = json!({"remote": &push.remote, "ref": &push.reference});
    }
    if let Some(request) = &receipt.pull_request {
        let mut pr = json!({"base": &request.base, "head": &request.head});
        if let Some(url) = &request.url {
            pr["url"] = Value::String(url.clone());
        }
        value["pull_request"] = pr;
    }
    value
}

pub fn publish_trailers(marker: &status::JobMarker, from: &Value) -> Vec<(String, String)> {
    let pack = marker
        .selfdev
        .as_ref()
        .and_then(|selfdev| selfdev.get("pack"))
        .and_then(|pack| pack.get("manifest_sha256"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let patch = from
        .get("sha256")
        .and_then(Value::as_str)
        .unwrap_or_default();
    vec![
        ("Kian-Run".to_string(), marker.run_id.clone()),
        ("Kian-Plan-Digest".to_string(), marker.plan_digest.clone()),
        ("Kian-Patch-Sha256".to_string(), patch.to_string()),
        ("Kian-Pack".to_string(), pack.to_string()),
    ]
}

/// Re-read the producer's committed bytes and hold them to the frozen digest
/// (§9.4 step 5): a commit can only contain bytes the plan's checks saw.
async fn verified_patch(deps: &JobDeps, from: &Value) -> Result<String, String> {
    let reference = from.get("ref").ok_or("the artifact ref names no event")?;
    let producer = reference
        .get("session")
        .and_then(Value::as_str)
        .ok_or("the artifact ref names no session")?;
    let seq = reference
        .get("seq")
        .and_then(Value::as_u64)
        .ok_or("the artifact ref names no seq")?;
    let producer = uuid::Uuid::parse_str(producer)
        .map(SessionId)
        .map_err(|err| format!("the artifact ref names no session: {err}"))?;
    let events = deps
        .streams
        .read_stream(producer)
        .await
        .map_err(|err| format!("the producer's stream is unreadable: {err}"))?;
    let event = events
        .iter()
        .find(|event| event.seq == seq)
        .ok_or_else(|| format!("the producer has no event at seq {seq}"))?;
    let text = message_text(event)
        .ok_or_else(|| format!("event {seq} of the producer carries no text"))?;
    let expected = from
        .get("sha256")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let digest = crate::job::plan::digest_text(&text);
    if digest != expected {
        return Err(format!(
            "the artifact's committed bytes hash to {digest}, not the frozen {expected}"
        ));
    }
    Ok(text)
}

pub async fn verified_artifact_text(
    streams: &dyn crate::worker::WorkerStreamsPort,
    artifact: &Value,
) -> Result<String, (&'static str, String)> {
    let unavailable = |detail: String| ("artifact_unavailable", detail);
    let reference = artifact
        .get("ref")
        .ok_or_else(|| unavailable("the artifact names no event".to_string()))?;
    let producer = reference
        .get("session")
        .and_then(Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
        .map(SessionId)
        .ok_or_else(|| unavailable("the artifact names no producer session".to_string()))?;
    let seq = reference
        .get("seq")
        .and_then(Value::as_u64)
        .ok_or_else(|| unavailable("the artifact names no producer seq".to_string()))?;
    let events = streams
        .read_stream(producer)
        .await
        .map_err(|err| unavailable(format!("the producer's stream is unreadable: {err}")))?;
    let event = events
        .iter()
        .find(|event| event.seq == seq)
        .ok_or_else(|| unavailable(format!("the producer has no event at seq {seq}")))?;
    let text = message_text(event)
        .ok_or_else(|| unavailable(format!("event {seq} of the producer carries no text")))?;
    let expected_bytes = artifact.get("bytes").and_then(Value::as_u64);
    if expected_bytes != Some(text.len() as u64) {
        return Err((
            "artifact_digest_mismatch",
            format!(
                "the artifact declares {expected_bytes:?} bytes but the producer committed {}",
                text.len()
            ),
        ));
    }
    let expected = artifact
        .get("sha256")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let digest = crate::job::plan::digest_text(&text);
    if digest != expected {
        return Err((
            "artifact_digest_mismatch",
            format!("the artifact's committed bytes hash to {digest}, not {expected}"),
        ));
    }
    Ok(text)
}

/// The child's task text (§8.4): deterministic, so a re-drive admits identical
/// bytes. Declared inputs are inlined from the producer's own committed event,
/// never from a copy (§12.2).
async fn child_task_text(
    deps: &JobDeps,
    events: &[Event],
    run_session: SessionId,
    step: &StepPlan,
    attempt: u32,
    inline_max_bytes: u64,
) -> Result<String, String> {
    let mut text = child_task_header(run_session, &step.id, attempt);
    if let Some(prompt) = &step.prompt {
        text.push('\n');
        text.push_str(prompt);
    }
    let inputs = input_block(deps, events, step, inline_max_bytes).await?;
    if !inputs.is_empty() {
        text.push_str("\ninputs:");
        text.push_str(&inputs);
    }
    Ok(text)
}

fn child_task_header(run_session: SessionId, step: &str, attempt: u32) -> String {
    format!(
        "[kian-job] run={} step={step} attempt={attempt}\n",
        run_session.0
    )
}

/// The inline form of a step's declared inputs (§8.4, §12.2).
async fn input_block(
    deps: &JobDeps,
    events: &[Event],
    step: &StepPlan,
    inline_max_bytes: u64,
) -> Result<String, String> {
    if step.inputs.is_empty() {
        return Ok(String::new());
    }
    let cap = usize::try_from(inline_max_bytes).unwrap_or(usize::MAX);
    let mut block = String::new();
    for input in &step.inputs {
        let (producer, seq, artifact) =
            artifact_ref(events, &input.step, &input.name).ok_or_else(|| {
                format!(
                    "artifact {} from step {} has no committed call/result pair",
                    input.name, input.step
                )
            })?;
        let producer_events = deps
            .streams
            .read_stream(producer)
            .await
            .map_err(|err| format!("cannot read the producer stream: {err}"))?;
        let text = producer_events
            .iter()
            .find(|event| event.seq == seq)
            .and_then(message_text)
            .ok_or_else(|| {
                format!(
                    "event {seq} of step {} carries no text to inline as {}",
                    input.step, input.name
                )
            })?;
        let expected = artifact
            .get("sha256")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let computed = crate::job::plan::digest_text(&text);
        if computed != expected {
            return Err(format!(
                "artifact {} from step {} hashes to {computed}, not {expected}",
                input.name, input.step
            ));
        }
        if text.len() > cap {
            return Err(format!(
                "artifact {} is {} bytes, above the inline cap of {cap}",
                input.name,
                text.len()
            ));
        }
        block.push_str(&format!(
            "\n## input: {} (from step {}, seq {seq})\n{text}\n",
            input.name, input.step
        ));
    }
    Ok(block)
}

/// Where an artifact lives: the producer session and the seq of the event that
/// carries it (§12.1).
fn artifact_ref(events: &[Event], step: &str, name: &str) -> Option<(SessionId, u64, Value)> {
    events.iter().rev().find_map(|event| {
        if event.kind != EventKind::ToolCall {
            return None;
        }
        if event.bytes.get("tool").and_then(Value::as_str) != Some(status::ARTIFACT_TOOL) {
            return None;
        }
        let artifact = event.bytes.get("args")?.get("artifact")?;
        if artifact.get("step").and_then(Value::as_str) != Some(step) {
            return None;
        }
        if artifact.get("name").and_then(Value::as_str) != Some(name) {
            return None;
        }
        let reference = artifact.get("ref")?;
        let producer = reference.get("session")?.as_str()?;
        let seq = reference.get("seq")?.as_u64()?;
        let call_id = event.bytes.get("call_id")?.as_str()?;
        let result = events.iter().find(|result| {
            result.kind == EventKind::ToolResult
                && result.bytes.get("call_id").and_then(Value::as_str) == Some(call_id)
                && result.bytes.get("ok").and_then(Value::as_bool) == Some(true)
                && result
                    .bytes
                    .get("result")
                    .and_then(|value| value.get("artifact"))
                    == Some(artifact)
        })?;
        if result.seq <= event.seq {
            return None;
        }
        Some((
            SessionId(uuid::Uuid::parse_str(producer).ok()?),
            seq,
            artifact.clone(),
        ))
    })
}

/// The frozen ref of one artifact on the run stream (§12.1): the value a
/// publish carries as `from`.
pub fn artifact_intent(events: &[Event], step: &str, name: &str) -> Option<Value> {
    artifact_ref(events, step, name).map(|(_, _, artifact)| artifact)
}

fn message_text(event: &Event) -> Option<String> {
    let content = event.bytes.get("content")?;
    if let Some(text) = content.as_str() {
        return Some(text.to_string());
    }
    content
        .get("text")
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn effect_name(effect: crate::job::plan::Effect) -> &'static str {
    match effect {
        crate::job::plan::Effect::Read => "read",
        crate::job::plan::Effect::Write => "write",
    }
}

fn refusal_detail(refusal: &PlacementRefusal) -> String {
    match refusal {
        PlacementRefusal::PinUnknown { pin } => {
            format!("the pinned worker {pin} is not in the catalog")
        }
        PlacementRefusal::PinIneligible { pin, missing } => {
            format!(
                "the pinned worker {pin} declares none of: {}",
                missing.join(", ")
            )
        }
        PlacementRefusal::NoCapable { requires } => {
            format!("no worker declares: {}", requires.join(", "))
        }
        PlacementRefusal::AllOffline { workers } => {
            format!("every capable worker is offline: {}", workers.join(", "))
        }
        PlacementRefusal::AllBusy { workers } => {
            format!("every capable worker is busy: {}", workers.join(", "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::outcome::{AttemptOutcome, RunFacts};
    use crate::job::plan::JobPlan;
    use kian_core::model::Epoch;
    use uuid::Uuid;

    fn plan() -> JobPlan {
        serde_json::from_value(json!({
            "id": "digest",
            "steps": [
                {
                    "id": "fetch",
                    "prompt": "fetch the news",
                    "requires": ["net.unrestricted"],
                    "produces": {"name": "raw", "max_bytes": 1024},
                    "verify": [{"check": "min_tool_results", "n": 1}]
                },
                {
                    "id": "notify",
                    "action": "deliver",
                    "depends_on": ["fetch"],
                    "inputs": [{"step": "fetch", "name": "raw"}],
                    "channel": "imessage",
                    "target": {"handle": "+15551234567"},
                    "content": {"from_artifact": {"step": "fetch", "name": "raw"}},
                    "verify": [{
                        "check": "delivery_receipt",
                        "channel": "imessage",
                        "target": {"handle": "+15551234567"}
                    }]
                }
            ]
        }))
        .unwrap_or_else(|error| panic!("the fixture is a valid plan: {error}"))
    }

    fn event(seq: u64, kind: EventKind, bytes: Value) -> Event {
        Event {
            seq,
            session: SessionId(Uuid::from_u128(1)),
            epoch: Epoch(1),
            ts: seq.saturating_mul(10),
            kind,
            bytes,
            version: 0,
        }
    }

    fn dispatched(step: &str, attempt: u32, worker: &str) -> DispatchRecord {
        DispatchRecord {
            step: step.to_string(),
            attempt,
            worker: worker.to_string(),
            thread_id: Uuid::from_u128(100 + u128::from(attempt)),
            child: SessionId(Uuid::from_u128(200 + u128::from(attempt))),
            step_digest: None,
            task: Value::Null,
            at_ms: 10,
        }
    }

    fn settled(step: &str, ordinal: u32, outcome: AttemptOutcome) -> VerdictRecord {
        VerdictRecord {
            step: step.to_string(),
            passed: outcome.is_success(false),
            ordinal,
            outcome: Some(outcome),
            at_ms: 100,
        }
    }

    #[test]
    fn facts_turn_dispatches_and_deliveries_into_attempts() {
        let plan = plan();
        let dispatches = vec![dispatched("fetch", 1, "vps")];
        let intents = vec![("notify".to_string(), 1u32)];
        let verdicts = vec![settled("fetch", 1, AttemptOutcome::Succeeded)];
        let facts = facts_from(&plan, &dispatches, &intents, &verdicts);
        let fetch = facts
            .iter()
            .find(|known| known.step == "fetch")
            .expect("a fetch entry");
        assert_eq!(fetch.attempts_used(), 1);
        assert_eq!(fetch.outcome(), Some(AttemptOutcome::Succeeded));
        assert_eq!(fetch.settled[0].worker.as_deref(), Some("vps"));
        let notify = facts
            .iter()
            .find(|known| known.step == "notify")
            .expect("a notify entry");
        assert_eq!(notify.attempts_used(), 1);
        assert_eq!(notify.in_flight, Some(1));
        assert_eq!(notify.outcome(), None);
    }

    #[test]
    fn facts_keep_the_outcome_a_verdict_recorded() {
        let plan = plan();
        let verdicts = vec![settled("fetch", 1, AttemptOutcome::TimedOut)];
        let facts = facts_from(&plan, &[dispatched("fetch", 1, "vps")], &[], &verdicts);
        let fetch = facts
            .iter()
            .find(|known| known.step == "fetch")
            .expect("a fetch entry");
        assert_eq!(fetch.outcome(), Some(AttemptOutcome::TimedOut));
    }

    #[test]
    fn facts_fall_back_to_the_passed_bit_of_a_legacy_verdict() {
        let plan = plan();
        let legacy = VerdictRecord {
            step: "fetch".to_string(),
            passed: true,
            ordinal: 1,
            outcome: None,
            at_ms: 100,
        };
        let facts = facts_from(&plan, &[dispatched("fetch", 1, "vps")], &[], &[legacy]);
        let fetch = facts
            .iter()
            .find(|known| known.step == "fetch")
            .expect("a fetch entry");
        assert_eq!(fetch.outcome(), Some(AttemptOutcome::Succeeded));
    }

    #[test]
    fn deliver_intents_reads_only_delivery_calls() {
        let events = vec![
            event(
                1,
                EventKind::ToolCall,
                json!({
                    "tool": DELIVER_TOOL,
                    "call_id": "job:deliver:notify:2",
                    "args": {"channel": "imessage", "target": {"handle": "+1"}, "content": {"text": "hi"}},
                }),
            ),
            event(
                2,
                EventKind::ToolCall,
                json!({"tool": "exec", "call_id": "call-7", "args": {"cmd": "true"}}),
            ),
        ];
        assert_eq!(
            deliver_intents(&events),
            vec![("notify".to_string(), 2u32)],
            "one intent, and the attempt parsed from its call id"
        );
    }

    #[test]
    fn run_halted_stops_a_required_failure() {
        let plan = plan();
        let facts = facts_from(
            &plan,
            &[dispatched("fetch", 1, "vps")],
            &[],
            &[settled("fetch", 1, AttemptOutcome::Failed)],
        );
        assert!(
            run_halted(&plan, &facts),
            "a required step failed with no retry left"
        );
        assert!(
            !all_steps_decided(&plan, &facts, &[]),
            "the deliver step never ran, so the run is not done"
        );
    }

    #[test]
    fn run_halted_waits_while_a_retry_remains() {
        let plan: JobPlan = serde_json::from_value(json!({
            "id": "digest",
            "steps": [{
                "id": "fetch",
                "prompt": "fetch",
                "retries": 1,
                "verify": [{"check": "final_message_present"}]
            }]
        }))
        .expect("a valid plan");
        let facts = facts_from(
            &plan,
            &[dispatched("fetch", 1, "vps")],
            &[],
            &[settled("fetch", 1, AttemptOutcome::Failed)],
        );
        assert!(!run_halted(&plan, &facts), "one retry is still available");
        assert!(!all_steps_decided(&plan, &facts, &[]));
    }

    #[test]
    fn run_halted_ignores_an_optional_failure() {
        let plan: JobPlan = serde_json::from_value(json!({
            "id": "digest",
            "steps": [{
                "id": "fetch",
                "prompt": "fetch",
                "optional": true,
                "verify": [{"check": "final_message_present"}]
            }]
        }))
        .expect("a valid plan");
        let facts = facts_from(
            &plan,
            &[dispatched("fetch", 1, "vps")],
            &[],
            &[settled("fetch", 1, AttemptOutcome::Failed)],
        );
        assert!(
            !run_halted(&plan, &facts),
            "an optional step never halts the run"
        );
        assert!(
            all_steps_decided(&plan, &facts, &[]),
            "an optional failure is a settled attempt"
        );
    }

    #[test]
    fn all_terminal_is_false_for_a_step_that_never_started() {
        let plan = plan();
        let facts = facts_from(&plan, &[], &[], &[]);
        assert!(!all_steps_decided(&plan, &facts, &[]));
    }

    #[test]
    fn a_dependent_of_a_failed_step_is_blocked_not_awaited() {
        let plan = plan();
        let facts = facts_from(
            &plan,
            &[dispatched("fetch", 1, "vps")],
            &[],
            &[settled("fetch", 1, AttemptOutcome::Failed)],
        );
        let run_facts = RunFacts {
            steps: &facts,
            artifacts: &[],
            accepted: &[],
            halted: false,
            deadline_passed: false,
        };
        let notify = plan.step("notify").expect("the notify step");
        let blocked = blocked_reason(&plan, &run_facts, notify).expect("a blocked deliver step");
        assert!(
            blocked.detail.contains("fetch"),
            "the reason names the dependency: {}",
            blocked.detail
        );
        let fetch = plan.step("fetch").expect("the fetch step");
        assert!(
            blocked_reason(&plan, &run_facts, fetch).is_none(),
            "a root is never blocked"
        );
    }

    #[test]
    fn step_outcomes_include_skipped_steps_in_plan_order() {
        let plan = plan();
        let facts = facts_from(
            &plan,
            &[dispatched("fetch", 1, "vps")],
            &[],
            &[settled("fetch", 1, AttemptOutcome::Succeeded)],
        );
        assert_eq!(
            step_outcomes(&plan, &facts),
            vec![
                ("fetch".to_string(), AttemptOutcome::Succeeded),
                ("notify".to_string(), AttemptOutcome::Skipped),
            ]
        );
    }

    #[test]
    fn a_delivery_check_needs_a_receipt_for_the_frozen_target() {
        let plan = plan();
        let notify = plan.step("notify").expect("the notify step");
        let (specs, extra) = declared_checks(notify, &[]);
        assert!(specs.is_empty(), "delivery_receipt is a job-owned check");
        assert_eq!(extra.len(), 1);
        assert_eq!(extra[0].name, DELIVERY_CHECK);
        assert!(!extra[0].passed);

        let delivered = vec![event(
            9,
            EventKind::ChannelDelivered,
            json!({
                "channel": "imessage",
                "target": {"handle": "+15551234567"},
                "policy": DELIVERY_POLICY,
            }),
        )];
        let (_, extra) = declared_checks(notify, &delivered);
        assert!(extra[0].passed, "a matching receipt passes");

        let other = vec![event(
            9,
            EventKind::ChannelDelivered,
            json!({"channel": "imessage", "target": {"handle": "+15550000000"}}),
        )];
        let (_, extra) = declared_checks(notify, &other);
        assert!(
            !extra[0].passed,
            "a receipt for another target is not this delivery"
        );
    }

    #[test]
    fn a_surface_check_reads_the_patch_artifact_text() {
        let plan = plan();
        let fetch = plan.step("fetch").expect("the fetch step");
        let mut patched = fetch.clone();
        patched.verify = vec![crate::job::plan::JobCheck::PatchPathsAllowed {
            artifact: "raw".to_string(),
            allow: vec!["crates/**".to_string()],
            deny: vec!["design/decisions/**".to_string()],
        }];
        let stream = |diff: &str| {
            vec![event(
                3,
                EventKind::AssistantMessage,
                json!({"role": "assistant", "content": {"text": diff}}),
            )]
        };
        let inside = "--- a/crates/kian-cli/src/lib.rs
+++ b/crates/kian-cli/src/lib.rs
` -1 +1 `
-old
+new
";
        let (specs, extra) = declared_checks(&patched, &stream(inside));
        assert!(specs.is_empty(), "patch_paths_allowed is a job-owned check");
        assert!(extra[0].passed, "{:?}", extra[0].detail);

        let denied = "--- a/design/decisions/ADR-069-x.md
+++ b/design/decisions/ADR-069-x.md
` -1 +1 `
-old
+new
";
        let (_, extra) = declared_checks(&patched, &stream(denied));
        assert!(!extra[0].passed);
        assert!(
            extra[0]
                .detail
                .as_deref()
                .unwrap_or("")
                .starts_with("denied: "),
            "{:?}",
            extra[0].detail
        );

        let (_, extra) = declared_checks(&patched, &[]);
        assert!(!extra[0].passed);
        assert!(
            extra[0]
                .detail
                .as_deref()
                .unwrap_or("")
                .contains("was not produced"),
            "{:?}",
            extra[0].detail
        );

        // A child that ends on a tool round never reached a final answer, so
        // its message is not an artifact (JOB-MODEL §4.3).
        let calling = vec![event(
            4,
            EventKind::AssistantMessage,
            json!({
                "role": "assistant",
                "content": {
                    "text": "calling the fetch tool",
                    "tool_calls": [{"id": "c1", "name": "fetch", "arguments": "{}"}],
                },
            }),
        )];
        let (_, extra) = declared_checks(&patched, &calling);
        assert!(!extra[0].passed);
        assert!(
            extra[0]
                .detail
                .as_deref()
                .unwrap_or("")
                .contains("was not produced"),
            "{:?}",
            extra[0].detail
        );
    }

    #[test]
    fn an_artifact_cap_check_reads_the_committed_text() {
        let plan = plan();
        let fetch = plan.step("fetch").expect("the fetch step");
        let artifact = |bytes: u64| {
            vec![event(
                3,
                EventKind::AssistantMessage,
                json!({
                    "role": "assistant",
                    "content": {"text": "x".repeat(bytes as usize)},
                }),
            )]
        };
        let small = artifact(512);
        let within = artifact_of(fetch, &small).expect("the artifact");
        assert_eq!(within.get("bytes").and_then(Value::as_u64), Some(512));
        assert_eq!(within["ref"]["seq"].as_u64(), Some(3));
        assert_eq!(
            within["sha256"].as_str(),
            Some(crate::job::plan::digest_text(&"x".repeat(512)).as_str())
        );

        let mut capped = fetch.clone();
        capped.verify = vec![crate::job::plan::JobCheck::ArtifactWithinCap {
            name: "raw".to_string(),
        }];
        let (_, extra) = declared_checks(&capped, &artifact(512));
        assert!(extra[0].passed, "512 bytes is within the 1024 byte cap");
        let (_, extra) = declared_checks(&capped, &artifact(2048));
        assert!(!extra[0].passed, "2048 bytes is above the cap");
        let (_, extra) = declared_checks(&capped, &[]);
        assert!(!extra[0].passed, "an unreported artifact never passes");
    }

    #[test]
    fn next_wait_uses_the_earliest_retry() {
        let plan: JobPlan = serde_json::from_value(json!({
            "id": "digest",
            "steps": [
                {"id": "a", "prompt": "a", "retries": 2, "verify": [{"check": "final_message_present"}]},
                {"id": "b", "prompt": "b", "verify": [{"check": "final_message_present"}]}
            ]
        }))
        .expect("a valid plan");
        let facts = facts_from(
            &plan,
            &[dispatched("a", 1, "vps")],
            &[],
            &[settled("a", 1, AttemptOutcome::Failed)],
        );
        let wait = next_wait_ms(&plan, &facts, 0, 0).expect("a retry is scheduled");
        assert_eq!(wait, 100 + crate::job::outcome::retry_backoff_ms(1));
        assert_eq!(
            next_wait_ms(&plan, &facts, wait, 0),
            None,
            "the retry is due"
        );
    }

    #[test]
    fn child_task_header_carries_only_the_run_uuid_step_and_attempt() {
        let run = SessionId(Uuid::from_u128(7));
        let first = child_task_header(run, "fetch", 1);
        assert_eq!(
            first,
            format!("[kian-job] run={} step=fetch attempt=1\n", run.0)
        );
        assert!(!first.contains("digest@"));
        assert!(!first.contains("job="));
        assert!(!first.contains("plan="));
        assert!(!first.contains("step_digest"));
    }

    fn publish_tool_step() -> StepPlan {
        let plan: JobPlan = serde_json::from_value(json!({
            "id": "tool-publish",
            "limits": {"deadline_ms": 1_800_000},
            "steps": [{
                "id": "patch",
                "prompt": "propose a patch",
                "produces": {"name": "patch", "max_bytes": 65536},
                "verify": [{"check": "artifact_within_cap", "name": "patch"}]
            }, {
                "id": "publish",
                "prompt": "call the publish tool once; it takes no arguments",
                "depends_on": ["patch"],
                "inputs": [{"step": "patch", "name": "patch"}],
                "effect": "write",
                "retries": 0,
                "verify": [{"check": "publish_ok"}]
            }]
        }))
        .expect("a valid tool-publish plan");
        plan.step("publish").expect("declared").clone()
    }

    fn tool_receipt(run: SessionId) -> Value {
        json!({
            "ok": true,
            "result": {
                "branch": publish_branch(run),
                "worktree": "/tmp/selfdev-worktree",
                "commit": "a".repeat(40),
                "gate": {"command": "true", "exit_code": 0, "digest": "sha256:00", "output_tail": ""}
            }
        })
    }

    fn child_stream(run: SessionId, calls: Vec<Value>) -> Vec<Event> {
        let mut events = vec![event(
            1,
            EventKind::ThreadStarted,
            json!({"worker": Uuid::from_u128(2).to_string(), "parent": run.0.to_string()}),
        )];
        let mut seq = 2;
        for call in calls {
            events.push(event(
                seq,
                EventKind::ToolCall,
                json!({"role": "assistant", "content": call["call"]}),
            ));
            seq += 1;
            if let Some(result) = call.get("result") {
                events.push(event(
                    seq,
                    EventKind::ToolResult,
                    json!({"role": "tool", "content": result}),
                ));
                seq += 1;
            }
        }
        events
    }

    #[test]
    fn publish_tool_calls_pair_each_intent_with_its_receipt() {
        let events = child_stream(
            SessionId(Uuid::from_u128(9)),
            vec![
                json!({
                    "call": {"id": "c1", "name": PUBLISH_AGENT_TOOL, "arguments": {}},
                    "result": {"call_id": "c1", "output": {"ok": true}, "is_error": false}
                }),
                json!({"call": {"id": "c2", "name": PUBLISH_AGENT_TOOL, "arguments": {}}}),
                json!({"call": {"id": "c3", "name": "read", "arguments": {}}}),
            ],
        );
        let calls = publish_tool_calls(&events);
        assert_eq!(
            calls.len(),
            2,
            "only the publish tool's calls are collected"
        );
        assert_eq!(calls[0].result, Some(json!({"ok": true})));
        assert_eq!(calls[1].result, None);
        assert_eq!(calls[0].call_seq, 2);
        assert_eq!(calls[0].result_seq, Some(3));
    }

    #[test]
    fn publish_ok_settles_from_the_childs_tool_receipt() {
        let run = SessionId(Uuid::from_u128(9));
        let step = publish_tool_step();
        let events = child_stream(
            run,
            vec![json!({
                "call": {"id": "call-1", "name": PUBLISH_AGENT_TOOL, "arguments": {}},
                "result": {"call_id": "call-1", "output": tool_receipt(run), "is_error": false}
            })],
        );
        let (specs, extra) = declared_checks(&step, &events);
        assert!(specs.is_empty());
        assert!(extra[0].passed, "{:?}", extra[0].detail);

        let no_receipt = child_stream(
            run,
            vec![json!({"call": {"id": "call-1", "name": PUBLISH_AGENT_TOOL, "arguments": {}}})],
        );
        let (_, extra) = declared_checks(&step, &no_receipt);
        assert!(!extra[0].passed);
        assert!(
            extra[0]
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("no receipt")),
            "{:?}",
            extra[0].detail
        );

        let coordinates = child_stream(
            run,
            vec![json!({
                "call": {"id": "call-1", "name": PUBLISH_AGENT_TOOL, "arguments": {"repo": "/elsewhere"}},
                "result": {"call_id": "call-1", "output": tool_receipt(run), "is_error": false}
            })],
        );
        let (_, extra) = declared_checks(&step, &coordinates);
        assert!(!extra[0].passed, "the tool takes no arguments");

        let silent = child_stream(run, vec![]);
        let (_, extra) = declared_checks(&step, &silent);
        assert!(!extra[0].passed, "a step that never publishes cannot pass");
    }
}
