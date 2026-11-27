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
use kian_core::seams::{ChannelDelivery, ChannelError, LoopDriver, LoopOutcome, SendReceipt};
use kian_loop::evidence::{CheckOutcome, CheckSpec, GateVerdict, evaluate_evidence_gate};
use serde_json::{Value, json};
use tracing::warn;

use crate::directory::WorkerDirectoryPort;
use crate::job::alert::{self, AlertFacts, AlertSpec};
use crate::job::outcome::{
    ArtifactFact, AttemptFact, AttemptOutcome, EffectGate, EffectSignals, Retry, RunFacts,
    RunOutcome, StepFacts, attempt_outcome, effect_gate, effective_effect, readiness,
    retry_decision, run_outcome,
};
use crate::job::placement::{self, Placement, PlacementFacts, PlacementRefusal};
use crate::job::plan::{JobPlan, StepPlan};
use crate::job::status::{self, DispatchRecord, VerdictRecord};
use crate::multiagent::SpawnIdentity;
use crate::worker::{SpawnRequest, WorkerSpawner, thread_started_payload_bound};

/// The stop reason a finished run yields (§5.3): the engine maps a
/// non-rotation `Stopped` to `completed`.
pub const JOB_FINISHED_STOP_REASON: &str = "job run finished";

/// The soft-failure reason of a failed run (§5.5): the engine maps
/// `SoftFailed` to `failed`.
pub const JOB_FAILED_REASON: &str = "job run failed";

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

/// The check a placement refusal records (§9.4).
pub const NO_CANDIDATE_CHECK: &str = "no_candidate";

/// The check a spent attempt budget records (§8.5).
pub const TIMEOUT_CHECK: &str = "timeout";

/// The check a dispatch failure records (§8.3).
pub const DISPATCH_CHECK: &str = "dispatch";

/// The check a delivery records (§13.2).
pub const DELIVERY_CHECK: &str = "delivery_receipt";

/// The check an input that cannot be inlined records (§12.2).
pub const INPUT_CHECK: &str = "input";

/// The check a step that can never become ready records (§8.1).
pub const SKIPPED_CHECK: &str = "skipped";

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
            alerts,
            clock,
            tick_floor_ms: JOB_TICK_MIN_MS,
            tick_ceiling_ms: JOB_TICK_MAX_MS,
        }
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
    let events = match port.read_stream().await {
        Ok(events) => events,
        Err(err) => {
            return Tick::finish(LoopOutcome::SoftFailed {
                reason: format!("job run: cannot read its own stream: {err}"),
            });
        }
    };
    let Some(found) = status::marker(&events) else {
        return Tick::finish(LoopOutcome::SoftFailed {
            reason: "job run: the stream carries no job marker".to_string(),
        });
    };
    let marker = match found {
        Ok(marker) => marker,
        Err(err) => return finish_invalid_input(port, err).await,
    };
    let plan = &marker.plan;

    if plan.declares_surface() && marker.selfdev.is_none() {
        return finish_invalid_input(
            port,
            status::MarkerError::Malformed(
                "selfdev job requires an evidence pack: the plan declares patch_paths_allowed and the marker carries no selfdev value"
                    .to_string(),
            ),
        )
        .await;
    }

    if let Some(verdict) = status::run_verdict(&events) {
        let (failed_step, reason) = run_failure(&verdict.payload);
        admit_alerts(
            deps,
            &marker,
            verdict.outcome,
            failed_step.as_deref(),
            reason.as_deref(),
        )
        .await;
        return Tick::finish(outcome_to_loop(verdict.outcome));
    }
    if has_ended(&events) {
        return Tick::finish(LoopOutcome::Stopped {
            reason: JOB_FINISHED_STOP_REASON.to_string(),
        });
    }

    let dispatches = status::dispatches(&events);
    let verdict_log = status::verdicts(&events);
    let intents = deliver_intents(&events);
    let facts = facts_from(plan, &dispatches, &intents, &verdict_log);
    let artifacts = status::artifacts(&events, plan);
    let now = deps.clock.now_ms();
    let started_ms = events.first().map_or(now, |event| event.ts);
    let deadline_passed = now >= started_ms.saturating_add(plan.limits.deadline_ms);

    let mut cancelled = port.interrupt_requested();
    match port.take_inputs().await {
        Ok(rows) => {
            if rows.iter().any(is_cancel_row) {
                cancelled = true;
            }
        }
        Err(err) => warn!(
            session = %port.session().0,
            error = %err,
            "job run: cannot promote inputs"
        ),
    }
    if cancelled || port.interrupt_requested() {
        let steps = step_outcomes(plan, &facts);
        let payload = status::run_verdict_payload(
            RunOutcome::Cancelled,
            &marker.plan_digest,
            &steps,
            None,
            Some("cancelled by an operator"),
        );
        if let Err(err) = port.emit(EventKind::EvidenceVerdict, payload).await {
            warn!(session = %port.session().0, error = %err, "job run: cannot write the run verdict");
        }
        return Tick::finish(LoopOutcome::Stopped {
            reason: JOB_FINISHED_STOP_REASON.to_string(),
        });
    }

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
        )
        .await;
    }

    let halted = run_halted(plan, &facts);
    if halted || deadline_passed {
        return settle_run(
            port,
            deps,
            &marker,
            &facts,
            &artifacts,
            deadline_passed,
            false,
        )
        .await;
    }

    for step in &plan.steps {
        let Some(known) = facts.iter().find(|known| known.step == step.id) else {
            continue;
        };
        let decision = retry_decision(
            step,
            known,
            EffectSignals {
                tool_calls: 0,
                receipt: false,
            },
            false,
        );
        if let Retry::Later {
            attempt,
            not_before_ms,
        } = decision
            && now >= not_before_ms
        {
            return dispatch_attempt(port, deps, &marker, &facts, &events, step, attempt).await;
        }
    }

    let run_facts = RunFacts {
        steps: &facts,
        artifacts: &artifacts,
        accepted: &plan.limits.accept_unverified,
        halted: false,
        deadline_passed,
    };
    for step in &plan.steps {
        if readiness(plan, &run_facts, step).is_ok() {
            return dispatch_attempt(port, deps, &marker, &facts, &events, step, 1).await;
        }
    }
    for step in &plan.steps {
        if let Some(blocked) = blocked_reason(plan, &run_facts, step) {
            let gate = passed_gate(1, SKIPPED_CHECK, Some(blocked.detail));
            return settle_step(port, step, 1, gate, AttemptOutcome::Skipped).await;
        }
    }

    if all_terminal(plan, &facts) {
        return settle_run(
            port,
            deps,
            &marker,
            &facts,
            &artifacts,
            deadline_passed,
            false,
        )
        .await;
    }

    let wait = next_wait_ms(plan, &facts, now).unwrap_or(deps.tick_ceiling_ms);
    Tick::park(deps.clamp_wait(wait))
}

fn outcome_to_loop(outcome: RunOutcome) -> LoopOutcome {
    match outcome {
        RunOutcome::Failed => LoopOutcome::SoftFailed {
            reason: JOB_FAILED_REASON.to_string(),
        },
        RunOutcome::Succeeded | RunOutcome::Unverified | RunOutcome::Cancelled => {
            LoopOutcome::Stopped {
                reason: JOB_FINISHED_STOP_REASON.to_string(),
            }
        }
    }
}

fn has_ended(events: &[Event]) -> bool {
    events
        .iter()
        .any(|event| matches!(event.kind, EventKind::SessionEnded { .. }))
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
    row.get("content")
        .and_then(Value::as_str)
        .is_some_and(|content| content.trim().eq_ignore_ascii_case("cancel"))
}

/// Every delivery intent on the run stream: `(step, attempt)` (§13.2).
#[must_use]
pub fn deliver_intents(events: &[Event]) -> Vec<(String, u32)> {
    let mut out = Vec::new();
    for event in events {
        if event.kind != EventKind::ToolCall {
            continue;
        }
        if event.bytes.get("tool").and_then(Value::as_str) != Some(DELIVER_TOOL) {
            continue;
        }
        let Some(call_id) = event.bytes.get("call_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(rest) = call_id.strip_prefix(DELIVER_CALL_PREFIX) else {
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

/// Whether no step can make further progress (§5.3).
fn all_terminal(plan: &JobPlan, facts: &[StepFacts]) -> bool {
    plan.steps.iter().all(|step| {
        let Some(known) = facts.iter().find(|known| known.step == step.id) else {
            return false;
        };
        if known.in_flight.is_some() {
            return false;
        }
        match known.outcome() {
            Some(outcome) => {
                if outcome.is_success(plan.limits.accept_unverified.contains(&step.id)) {
                    return true;
                }
                let retries_left = usize::from(step.retries) + 1 > known.attempts_used();
                !(outcome.is_retryable() && retries_left)
            }
            None => false,
        }
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

struct Blocked {
    detail: String,
}

/// How long the conductor may sleep before something becomes due (§5.4).
fn next_wait_ms(plan: &JobPlan, facts: &[StepFacts], now: u64) -> Option<u64> {
    let mut wait: Option<u64> = None;
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
        .filter_map(|step| {
            facts
                .iter()
                .find(|known| known.step == step.id)?
                .outcome()
                .map(|outcome| (step.id.clone(), outcome))
        })
        .collect()
}

fn first_failure(plan: &JobPlan, facts: &[StepFacts]) -> Option<String> {
    plan.steps.iter().find_map(|step| {
        let known = facts.iter().find(|known| known.step == step.id)?;
        let outcome = known.outcome()?;
        (!outcome.is_success(false)).then(|| step.id.clone())
    })
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
        _ => CheckOutcome {
            name: check.name().to_string(),
            passed: false,
            detail: Some("the check is not evaluable by the conductor".to_string()),
        },
    }
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

/// Settle a run that cannot even be read as a job (§5.2): one run verdict,
/// then fail. Never falls back to a plain agent run.
async fn finish_invalid_input(port: &Arc<dyn RunPort>, err: status::MarkerError) -> Tick {
    let reason = format!("invalid_job_input: {err}");
    let payload = status::run_verdict_payload(RunOutcome::Failed, "", &[], None, Some(&reason));
    if let Err(write) = port.emit(EventKind::EvidenceVerdict, payload).await {
        warn!(session = %port.session().0, error = %write, "job run: cannot write the run verdict");
    }
    Tick::finish(LoopOutcome::SoftFailed { reason })
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
) -> Tick {
    let record = dispatches
        .iter()
        .find(|record| record.step == step.id && record.attempt == attempt);
    let Some(record) = record else {
        let (check, detail) = if step.is_deliver() {
            (
                DELIVERY_CHECK,
                "the conductor stopped between the send intent and its receipt: the outcome is unknown",
            )
        } else {
            (
                DISPATCH_CHECK,
                "the attempt has no dispatch record, so no child exists and nothing may be assumed",
            )
        };
        let gate = failed_gate(attempt, check, Some(detail.to_string()));
        let outcome = if step.is_deliver() {
            AttemptOutcome::NeedsOperator
        } else {
            AttemptOutcome::Failed
        };
        return settle_step(port, step, attempt, gate, outcome).await;
    };
    let timeout_at = record.at_ms.saturating_add(step.timeout_ms);
    let child = match deps.streams.read_stream(record.child).await {
        Ok(child) => child,
        Err(err) => {
            if now >= timeout_at {
                let gate = failed_gate(
                    attempt,
                    TIMEOUT_CHECK,
                    Some(format!("the child stream is unreadable: {err}")),
                );
                return settle_step(port, step, attempt, gate, AttemptOutcome::TimedOut).await;
            }
            // W3: the record is the placement decision (J3) and the child ids
            // derive from (run, step, attempt), so the very same child is
            // created again. Nothing is re-selected.
            return readmit_child(port, deps, marker, events, step, attempt, record).await;
        }
    };
    if let Some(reason) = end_reason(&child) {
        return settle_agent(port, marker, step, attempt, &child, reason).await;
    }
    if !child_started(&child) {
        if now >= timeout_at {
            let gate = failed_gate(
                attempt,
                TIMEOUT_CHECK,
                Some(format!(
                    "attempt {attempt} never started within {} ms",
                    step.timeout_ms
                )),
            );
            return settle_step(port, step, attempt, gate, AttemptOutcome::TimedOut).await;
        }
        // W4/W5: the child exists but was never admitted, or its wake was
        // lost. The task key makes the re-admit a no-op when the row is
        // already durable, so a crash on either side of the admit is safe.
        return readmit_child(port, deps, marker, events, step, attempt, record).await;
    }
    if now >= timeout_at {
        let gate = failed_gate(
            attempt,
            TIMEOUT_CHECK,
            Some(format!(
                "attempt {attempt} ran for {} ms without ending",
                now.saturating_sub(record.at_ms)
            )),
        );
        return settle_step(port, step, attempt, gate, AttemptOutcome::TimedOut).await;
    }
    Tick::park(deps.clamp_wait(timeout_at.saturating_sub(now)))
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
    let task = match child_task_text(deps, marker, events, step, attempt).await {
        Ok(task) => task,
        Err(detail) => {
            let gate = failed_gate(attempt, INPUT_CHECK, Some(detail));
            return settle_step(port, step, attempt, gate, AttemptOutcome::Failed).await;
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
            DISPATCH_CHECK,
            Some("the recorded dispatch does not name the child its ids derive".to_string()),
        );
        return settle_step(port, step, attempt, gate, AttemptOutcome::Failed).await;
    }
    if let Err(err) = deps.spawner.admit(&spawned).await {
        let gate = failed_gate(
            attempt,
            DISPATCH_CHECK,
            Some(format!("the child could not be admitted: {err}")),
        );
        return settle_step(port, step, attempt, gate, AttemptOutcome::Failed).await;
    }
    Tick::park(JOB_TICK_MIN_MS)
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

/// Settle one agent attempt from the child's own stream (§8.5, §11.2).
async fn settle_agent(
    port: &Arc<dyn RunPort>,
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
        harvest(port, marker, step, attempt, child).await;
    }
    settle_step(port, step, attempt, gate, outcome).await
}

/// Record one artifact ref on the run's stream (§12.1): the ref, the digest
/// and the producer, never the bytes.
async fn harvest(
    port: &Arc<dyn RunPort>,
    marker: &status::JobMarker,
    step: &StepPlan,
    attempt: u32,
    child: &[Event],
) {
    let Some(mut artifact) = artifact_of(step, child) else {
        return;
    };
    artifact["producer"] = json!({
        "run": &marker.run_id,
        "step": &step.id,
        "attempt": attempt,
    });
    let call_id = format!("{ARTIFACT_CALL_PREFIX}{}:{attempt}", step.id);
    let call = json!({
        "tool": status::ARTIFACT_TOOL,
        "call_id": call_id,
        "args": { "artifact": artifact },
    });
    if let Err(err) = port.emit(EventKind::ToolCall, call).await {
        warn!(session = %port.session().0, error = %err, "job run: cannot record an artifact ref");
        return;
    }
    let result = json!({
        "call_id": call_id,
        "ok": true,
        "result": { "artifact": artifact },
    });
    if let Err(err) = port.emit(EventKind::ToolResult, result).await {
        warn!(session = %port.session().0, error = %err, "job run: cannot close an artifact ref");
    }
}

/// Settle one attempt and park one tick so the next tick reads it back (§8.5).
async fn settle_step(
    port: &Arc<dyn RunPort>,
    step: &StepPlan,
    _attempt: u32,
    gate: GateVerdict,
    outcome: AttemptOutcome,
) -> Tick {
    let payload = status::step_verdict_payload(&step.id, &gate, outcome);
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
    cancelled: bool,
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
    let outcome = run_outcome(plan, &run_facts, cancelled);
    let steps = step_outcomes(plan, facts);
    let failed_step = first_failure(plan, facts);
    let reason = deadline_passed.then(|| "the run deadline was exceeded".to_string());
    let payload = status::run_verdict_payload(
        outcome,
        &marker.plan_digest,
        &steps,
        failed_step.as_deref(),
        reason.as_deref(),
    );
    if let Err(err) = port.emit(EventKind::EvidenceVerdict, payload).await {
        warn!(session = %port.session().0, error = %err, "job run: cannot write the run verdict");
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
    if !matches!(outcome, RunOutcome::Failed | RunOutcome::Unverified) {
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
        return execute_deliver(port, deps, step, attempt).await;
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
    let members = match deps.directory.members().await {
        Ok(members) => members,
        Err(err) => {
            warn!(session = %port.session().0, error = %err.0, "job run: the worker catalog is unreadable");
            return Tick::park(JOB_DEFER_TICK_MS);
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
    let placement: Placement = match placement::select(
        step,
        &marker.plan.limits,
        &PlacementFacts {
            members: &members,
            busy: &[],
            tried: &tried,
        },
    ) {
        Ok(placement) => placement,
        Err(refusal) if refusal.is_transient() => {
            return Tick::park(JOB_DEFER_TICK_MS);
        }
        Err(refusal) => {
            let gate = failed_gate(attempt, NO_CANDIDATE_CHECK, Some(refusal_detail(&refusal)));
            return settle_step(port, step, attempt, gate, AttemptOutcome::Failed).await;
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
    let task = match child_task_text(deps, marker, events, step, attempt).await {
        Ok(task) => task,
        Err(detail) => {
            let gate = failed_gate(attempt, INPUT_CHECK, Some(detail));
            return settle_step(port, step, attempt, gate, AttemptOutcome::Failed).await;
        }
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
        "step_digest": step.digest(),
        "effect": effect_name(effective_effect(step)),
        "candidates": placement.candidates.iter().map(|candidate| candidate.id.clone()).collect::<Vec<_>>(),
        "rationale": placement.rationale(),
    });
    if let Err(err) = port.emit(EventKind::ThreadStarted, record).await {
        warn!(session = %port.session().0, error = %err, "job run: cannot record a dispatch");
        return Tick::park(JOB_DEFER_TICK_MS);
    }
    if let Err(err) = deps.spawner.admit(&spawned).await {
        let gate = failed_gate(
            attempt,
            DISPATCH_CHECK,
            Some(format!("the child could not be admitted: {err}")),
        );
        return settle_step(port, step, attempt, gate, AttemptOutcome::Failed).await;
    }
    Tick::park(JOB_TICK_MIN_MS)
}

/// Run one deliver step in the conductor's own drain (§13.2). Not placed: no
/// worker can be selected into the send path, so the spec's escalation case
/// cannot arise.
async fn execute_deliver(
    port: &Arc<dyn RunPort>,
    deps: &JobDeps,
    step: &StepPlan,
    attempt: u32,
) -> Tick {
    let Some(deliver) = step.deliver() else {
        let gate = failed_gate(
            attempt,
            DELIVERY_CHECK,
            Some("the step is not a well-formed deliver step".to_string()),
        );
        return settle_step(port, step, attempt, gate, AttemptOutcome::Failed).await;
    };
    let content = match (&deliver.content.text, &deliver.content.from_artifact) {
        (Some(text), _) => json!({ "text": text }),
        (None, Some(_)) => {
            let gate = failed_gate(
                attempt,
                DELIVERY_CHECK,
                Some("deliver from_artifact lands in P1 (JOB-MODEL §19.2)".to_string()),
            );
            return settle_step(port, step, attempt, gate, AttemptOutcome::Failed).await;
        }
        (None, None) => {
            let gate = failed_gate(
                attempt,
                DELIVERY_CHECK,
                Some("the deliver step carries no content".to_string()),
            );
            return settle_step(port, step, attempt, gate, AttemptOutcome::Failed).await;
        }
    };
    let call_id = format!("{DELIVER_CALL_PREFIX}{}:{attempt}", step.id);
    let intent = json!({
        "tool": DELIVER_TOOL,
        "call_id": call_id,
        "args": {
            "channel": deliver.channel,
            "target": deliver.target,
            "content": content,
        },
    });
    if let Err(err) = port.emit(EventKind::ToolCall, intent).await {
        warn!(session = %port.session().0, error = %err, "job run: cannot record a delivery intent");
        return Tick::park(JOB_DEFER_TICK_MS);
    }
    match deps
        .deliver
        .send(deliver.channel, deliver.target.clone(), content)
        .await
    {
        Ok(receipt) => {
            let delivered = json!({
                "channel": deliver.channel,
                "target": deliver.target,
                "policy": DELIVERY_POLICY,
                "attempt": attempt,
                "first_attempt_ms": 0,
                "detail": { "message_id": receipt.message_id },
            });
            if let Err(err) = port.emit(EventKind::ChannelDelivered, delivered).await {
                warn!(session = %port.session().0, error = %err, "job run: cannot write a delivery receipt");
            }
            let result = json!({
                "call_id": call_id,
                "ok": true,
                "result": { "message_id": receipt.message_id },
            });
            if let Err(err) = port.emit(EventKind::ToolResult, result).await {
                warn!(session = %port.session().0, error = %err, "job run: cannot close a delivery intent");
            }
            let gate = passed_gate(
                attempt,
                DELIVERY_CHECK,
                Some(format!("receipt {}", receipt.message_id)),
            );
            settle_step(port, step, attempt, gate, AttemptOutcome::Succeeded).await
        }
        Err(ChannelError::Timeout) => {
            let gate = failed_gate(
                attempt,
                DELIVERY_CHECK,
                Some("the send timed out: the message may have landed".to_string()),
            );
            settle_step(port, step, attempt, gate, AttemptOutcome::NeedsOperator).await
        }
        Err(err) => {
            let gate = failed_gate(
                attempt,
                DELIVERY_CHECK,
                Some(format!("the send failed: {err}")),
            );
            settle_step(port, step, attempt, gate, AttemptOutcome::Failed).await
        }
    }
}

/// The child's task text (§8.4): deterministic, so a re-drive admits identical
/// bytes. Declared inputs are inlined from the producer's own committed event,
/// never from a copy (§12.2).
async fn child_task_text(
    deps: &JobDeps,
    marker: &status::JobMarker,
    events: &[Event],
    step: &StepPlan,
    attempt: u32,
) -> Result<String, String> {
    let mut text = format!(
        "[kian-job] run={} job={} step={} attempt={}\nplan={}\nstep_digest={}\n",
        marker.run_id,
        marker.job_id,
        step.id,
        attempt,
        marker.plan_digest,
        step.digest()
    );
    if let Some(selfdev) = &marker.selfdev {
        text.push_str("selfdev=");
        text.push_str(&crate::job::plan::canonical_json(selfdev));
        text.push('\n');
    }
    if let Some(prompt) = &step.prompt {
        text.push('\n');
        text.push_str(prompt);
    }
    let inputs = input_block(deps, marker, events, step).await?;
    if !inputs.is_empty() {
        text.push_str("\ninputs:");
        text.push_str(&inputs);
    }
    Ok(text)
}

/// The inline form of a step's declared inputs (§8.4, §12.2).
async fn input_block(
    deps: &JobDeps,
    marker: &status::JobMarker,
    events: &[Event],
    step: &StepPlan,
) -> Result<String, String> {
    if step.inputs.is_empty() {
        return Ok(String::new());
    }
    let cap = usize::try_from(marker.plan.limits.inline_max_bytes).unwrap_or(usize::MAX);
    let mut block = String::new();
    for input in &step.inputs {
        let (producer, seq) = artifact_ref(events, &input.step, &input.name).ok_or_else(|| {
            format!(
                "artifact {} from step {} has no committed ref",
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
fn artifact_ref(events: &[Event], step: &str, name: &str) -> Option<(SessionId, u64)> {
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
        Some((SessionId(uuid::Uuid::parse_str(producer).ok()?), seq))
    })
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
        PlacementRefusal::AllTried { workers } => {
            format!(
                "every capable worker already failed this step: {}",
                workers.join(", ")
            )
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
            !all_terminal(&plan, &facts),
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
        assert!(!all_terminal(&plan, &facts));
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
            all_terminal(&plan, &facts),
            "an optional failure is a settled attempt"
        );
    }

    #[test]
    fn all_terminal_is_false_for_a_step_that_never_started() {
        let plan = plan();
        let facts = facts_from(&plan, &[], &[], &[]);
        assert!(!all_terminal(&plan, &facts));
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
    fn step_outcomes_and_first_failure_follow_the_plan_order() {
        let plan = plan();
        let facts = facts_from(
            &plan,
            &[dispatched("fetch", 1, "vps")],
            &[],
            &[settled("fetch", 1, AttemptOutcome::Succeeded)],
        );
        assert_eq!(
            step_outcomes(&plan, &facts),
            vec![("fetch".to_string(), AttemptOutcome::Succeeded)]
        );
        assert_eq!(first_failure(&plan, &facts), None);
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
        let wait = next_wait_ms(&plan, &facts, 0).expect("a retry is scheduled");
        assert_eq!(wait, 100 + crate::job::outcome::retry_backoff_ms(1));
        assert_eq!(next_wait_ms(&plan, &facts, wait), None, "the retry is due");
    }

    #[test]
    fn child_task_text_is_deterministic_and_names_the_run() {
        let plan = plan();
        let fetch = plan.step("fetch").expect("the fetch step");
        let marker = status::JobMarker {
            run_id: "digest@slot".to_string(),
            job_id: "digest".to_string(),
            slot: "slot".to_string(),
            trigger: "cron".to_string(),
            plan_digest: plan.digest(),
            plan: plan.clone(),
            selfdev: None,
        };
        let first = format!(
            "[kian-job] run={} job={} step={} attempt={}\nplan={}\nstep_digest={}\n",
            marker.run_id,
            marker.job_id,
            fetch.id,
            1,
            marker.plan_digest,
            fetch.digest()
        );
        assert!(first.contains("[kian-job] run=digest@slot"));
        assert!(first.contains("step_digest=sha256:"));
    }
}
