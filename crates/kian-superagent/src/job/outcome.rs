//! Outcomes, readiness and retry policy — the pure decision surface of a
//! job run (`design/JOB-MODEL.md` §8, §10, §11).
//!
//! The conductor never keeps these decisions in memory across ticks: it
//! re-derives them from the facts committed to the run's stream and to its
//! children's streams, so a takeover reaches the same conclusion as the
//! drain it replaced (J1, J4, J12). Everything here is therefore a
//! function of facts, never of history.

use kian_core::events::EndReason;

use crate::job::plan::{Effect, JobPlan, StepPlan};

/// The OUTBOX retry curve (`crates/kian-superagent/src/outbox.rs`): the
/// first retry waits 2 s, each subsequent one doubles, capped at 60 s.
pub const RETRY_BACKOFF_START_MS: u64 = 2_000;

/// The retry curve's ceiling.
pub const RETRY_BACKOFF_CAP_MS: u64 = 60_000;

/// How one step attempt ended (§11.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// A step the run never had to run (an optional step behind a failed
    /// dependency).
    Skipped,
    /// The declared evidence held.
    Succeeded,
    /// The work happened but the evidence could not be established. Never
    /// treated as success unless the plan accepts it (§8.1, J7).
    Unverified,
    /// The child produced output that did not match its schema.
    InvalidOutput,
    /// The attempt ran out of its budget.
    TimedOut,
    /// The attempt was stopped before it finished its work.
    Interrupted,
    /// The effect may or may not have landed; a person decides (§10.3).
    NeedsOperator,
    /// The child failed.
    Failed,
}

impl AttemptOutcome {
    /// The wire/verdict spelling (§11.1).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Skipped => "skipped",
            Self::Succeeded => "succeeded",
            Self::Unverified => "unverified",
            Self::InvalidOutput => "invalid_output",
            Self::TimedOut => "timed_out",
            Self::Interrupted => "interrupted",
            Self::NeedsOperator => "needs_operator",
            Self::Failed => "failed",
        }
    }

    /// Whether this outcome satisfies a dependency (§8.1). An
    /// `Unverified` attempt counts only when the plan names the step in
    /// `limits.accept_unverified`, and then the run itself is `unverified`.
    #[must_use]
    pub fn is_success(self, accepted_unverified: bool) -> bool {
        match self {
            Self::Succeeded => true,
            Self::Unverified => accepted_unverified,
            Self::InvalidOutput
            | Self::TimedOut
            | Self::Interrupted
            | Self::NeedsOperator
            | Self::Skipped
            | Self::Failed => false,
        }
    }

    /// Whether another attempt is allowed at all (§8.6).
    #[must_use]
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::Failed | Self::TimedOut | Self::Interrupted | Self::InvalidOutput
        )
    }

    /// Whether this outcome means "the effect may have landed" (§10.3).
    #[must_use]
    pub fn is_unknown_effect(self) -> bool {
        matches!(self, Self::NeedsOperator)
    }

    /// The inverse of [`Self::as_str`], for the verdicts the conductor reads
    /// back after a restart. An unknown spelling is `None` — never guessed.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "skipped" => Self::Skipped,
            "succeeded" => Self::Succeeded,
            "unverified" => Self::Unverified,
            "invalid_output" => Self::InvalidOutput,
            "timed_out" => Self::TimedOut,
            "interrupted" => Self::Interrupted,
            "needs_operator" => Self::NeedsOperator,
            "failed" => Self::Failed,
            _ => return None,
        })
    }
}

/// The run's outcome (§11.3). Total order of severity:
/// `cancelled > failed > unverified > succeeded`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RunOutcome {
    /// Every step's evidence held.
    Succeeded,
    /// The work happened; the evidence did not establish success.
    Unverified,
    /// A step failed, or the run could not finish.
    Failed,
    /// An operator cancelled it.
    Cancelled,
}

impl RunOutcome {
    /// The wire/verdict spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Unverified => "unverified",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// The inverse of [`Self::as_str`] (§11.1's run verdict).
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "succeeded" => Self::Succeeded,
            "unverified" => Self::Unverified,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            _ => return None,
        })
    }
}

/// Derive an attempt's outcome from the child's end and its verdict (§11.3).
///
/// `verdict` is `None` when the step has no evaluable check for this
/// attempt — the honest answer is then `unverified`, never success (J7).
#[must_use]
pub fn attempt_outcome(
    end: EndReason,
    verdict: Option<bool>,
    output_valid: bool,
) -> AttemptOutcome {
    match end {
        EndReason::Completed => {
            if !output_valid {
                AttemptOutcome::InvalidOutput
            } else {
                match verdict {
                    Some(true) => AttemptOutcome::Succeeded,
                    Some(false) => AttemptOutcome::Failed,
                    None => AttemptOutcome::Unverified,
                }
            }
        }
        // The child asked a question: the conductor cannot answer it, so
        // a person must (§11.1).
        EndReason::AwaitingInput => AttemptOutcome::NeedsOperator,
        // A child parked on its token budget needs an operator to raise it;
        // the conductor cannot (ADR-055).
        EndReason::AwaitingBudget => AttemptOutcome::NeedsOperator,
        EndReason::Interrupted => AttemptOutcome::Interrupted,
        // An owner stop is not a failure (constraint 7): the attempt did
        // not finish its work, and it is not evidence of a broken step.
        EndReason::Cancelled | EndReason::Rotated => AttemptOutcome::Interrupted,
        EndReason::Failed => AttemptOutcome::Failed,
    }
}

/// One settled attempt of a step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptFact {
    /// 1-based attempt number (§6).
    pub attempt: u32,
    /// How it ended.
    pub outcome: AttemptOutcome,
    /// The worker it was placed on; `None` for a conductor action.
    pub worker: Option<String>,
    /// When it settled, in epoch milliseconds.
    pub ended_ms: u64,
}

/// Everything the run knows about one step (§15.1).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StepFacts {
    /// The step id.
    pub step: String,
    /// Settled attempts, oldest first.
    pub settled: Vec<AttemptFact>,
    /// The attempt currently dispatched, if any. A dispatch record exists
    /// but the attempt has not settled yet.
    pub in_flight: Option<u32>,
}

impl StepFacts {
    /// A step with no attempt yet.
    #[must_use]
    pub fn pending(step: impl Into<String>) -> Self {
        Self {
            step: step.into(),
            settled: Vec::new(),
            in_flight: None,
        }
    }

    /// How many attempts have been made, counting the one in flight.
    #[must_use]
    pub fn attempts_used(&self) -> usize {
        self.settled.len() + usize::from(self.in_flight.is_some())
    }

    /// The last settled attempt.
    #[must_use]
    pub fn last(&self) -> Option<&AttemptFact> {
        self.settled.last()
    }

    /// The step's current outcome: the last settled attempt's, or `None`
    /// while nothing has settled (truth is monotone — J12 — so the last
    /// record is the only one that counts).
    #[must_use]
    pub fn outcome(&self) -> Option<AttemptOutcome> {
        self.last().map(|attempt| attempt.outcome)
    }

    /// Whether the step is finished for this run.
    #[must_use]
    pub fn is_settled(&self) -> bool {
        self.in_flight.is_none() && self.outcome().is_some()
    }

    /// The workers already tried and not succeeded. They remain admissible;
    /// placement ranks them after untried workers (§9.2).
    #[must_use]
    pub fn exhausted_workers(&self) -> Vec<&str> {
        self.settled
            .iter()
            .filter(|attempt| !matches!(attempt.outcome, AttemptOutcome::Succeeded))
            .filter_map(|attempt| attempt.worker.as_deref())
            .collect()
    }
}

/// One produced artifact as the run knows it (§12).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactFact {
    /// The producing step.
    pub step: String,
    /// The artifact name.
    pub name: String,
    /// The byte length actually produced.
    pub bytes: u64,
    /// Whether it fits its declared cap. An over-cap artifact is a step
    /// failure, never a truncated artifact (§12.2).
    pub within_cap: bool,
}

/// The run-level facts a decision is allowed to depend on.
#[derive(Debug, Clone, Copy, Default)]
pub struct RunFacts<'a> {
    /// Per-step progress.
    pub steps: &'a [StepFacts],
    /// Produced artifacts.
    pub artifacts: &'a [ArtifactFact],
    /// `limits.accept_unverified`.
    pub accepted: &'a [String],
    /// Whether the run has halted `on_failure` (§11.3).
    pub halted: bool,
    /// Whether `limits.deadline_ms` has passed (§8.7).
    pub deadline_passed: bool,
}

impl<'a> RunFacts<'a> {
    /// The facts of one step.
    #[must_use]
    pub fn step(&self, id: &str) -> Option<&'a StepFacts> {
        self.steps.iter().find(|facts| facts.step == id)
    }

    /// Whether the plan accepts an unverified outcome for this step.
    #[must_use]
    pub fn accepts_unverified(&self, id: &str) -> bool {
        self.accepted.iter().any(|accepted| accepted == id)
    }

    /// The artifact a step produced, if it is known and within its cap.
    #[must_use]
    pub fn artifact(&self, step: &str, name: &str) -> Option<&'a ArtifactFact> {
        self.artifacts
            .iter()
            .find(|artifact| artifact.step == step && artifact.name == name && artifact.within_cap)
    }
}

/// Why a step may not be dispatched yet (§8.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NotReady {
    /// An attempt is already dispatched.
    InFlight {
        /// The attempt number in flight.
        attempt: u32,
    },
    /// The step already settled; it is not dispatched again (§4 J12).
    Settled {
        /// How many attempts it took.
        attempts: u32,
    },
    /// A dependency has not succeeded.
    Dependency {
        /// The dependency.
        step: String,
        /// Its current outcome, if it has one.
        outcome: Option<AttemptOutcome>,
    },
    /// A consumed artifact is missing or over its cap.
    Input {
        /// The producing step.
        step: String,
        /// The artifact name.
        name: String,
    },
    /// The run halted after a failure.
    Halted,
    /// The run's deadline passed: nothing new is dispatched (§8.7).
    DeadlinePassed,
}

impl NotReady {
    /// The reason code an operator surface shows (§15.3).
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            Self::InFlight { .. } => "in_flight",
            Self::Settled { .. } => "settled",
            Self::Dependency { .. } => "dependency",
            Self::Input { .. } => "input",
            Self::Halted => "halted",
            Self::DeadlinePassed => "deadline_exceeded",
        }
    }
}

/// Whether a step may be dispatched now.
///
/// The whole readiness rule of §8.1, as one pure function: no attempt yet,
/// every dependency succeeded (and is accepted), every consumed artifact
/// exists within its cap, the run is neither halted nor out of time.
pub fn readiness(plan: &JobPlan, facts: &RunFacts<'_>, step: &StepPlan) -> Result<(), NotReady> {
    if facts.halted {
        return Err(NotReady::Halted);
    }
    if facts.deadline_passed {
        return Err(NotReady::DeadlinePassed);
    }
    if let Some(attempt) = facts.step(&step.id).and_then(|known| known.in_flight) {
        return Err(NotReady::InFlight { attempt });
    }
    if let Some(known) = facts.step(&step.id).filter(|known| known.is_settled()) {
        return Err(NotReady::Settled {
            attempts: known.attempts_used() as u32,
        });
    }

    for dependency in &step.depends_on {
        let outcome = facts.step(dependency).and_then(StepFacts::outcome);
        let accepted = facts.accepts_unverified(dependency);
        match outcome {
            Some(outcome) if outcome.is_success(accepted) => {}
            other => {
                return Err(NotReady::Dependency {
                    step: dependency.clone(),
                    outcome: other,
                });
            }
        }
    }

    for input in &step.inputs {
        if facts.artifact(&input.step, &input.name).is_none() {
            return Err(NotReady::Input {
                step: input.step.clone(),
                name: input.name.clone(),
            });
        }
    }

    let _ = plan;
    Ok(())
}

/// The effect class a step actually has: a deliver step is a write even
/// when its declaration omits `effect` (§10).
#[must_use]
pub fn effective_effect(step: &StepPlan) -> Effect {
    if step.is_deliver() {
        Effect::Write
    } else {
        step.effect
    }
}

/// What a failed attempt's child actually did, as far as the run can tell.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EffectSignals {
    /// `tool/call` events issued by the child.
    pub tool_calls: u64,
    /// Whether a delivery receipt exists for this attempt.
    pub receipt: bool,
}

/// Whether repeating the step is safe (§10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectGate {
    /// A repeat cannot duplicate the effect.
    Safe,
    /// The effect may have landed; do not retry without a person (§10.3).
    NeedsOperator,
}

/// The effect gate of §10.1: read and idempotent writes always repeat; a
/// non-idempotent write repeats only when the child issued no `tool/call`
/// at all (the conservative test, because a queued exec command can
/// outlive its session).
#[must_use]
pub fn effect_gate(step: &StepPlan, signals: EffectSignals) -> EffectGate {
    match effective_effect(step) {
        Effect::Read => EffectGate::Safe,
        Effect::Write => {
            if step.idempotent || signals.tool_calls == 0 {
                EffectGate::Safe
            } else {
                let _ = signals.receipt;
                EffectGate::NeedsOperator
            }
        }
    }
}

/// The retry decision of §8.6.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Retry {
    /// Dispatch the next attempt, after the backoff.
    Later {
        /// The attempt number to dispatch.
        attempt: u32,
        /// The earliest epoch-millisecond instant to dispatch it.
        not_before_ms: u64,
    },
    /// No further attempt.
    Stop(RetryRefusal),
}

/// Why a step stops retrying.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryRefusal {
    /// The outcome is not retryable (success, needs-operator, skipped).
    NotRetryable,
    /// `retries` is used up.
    Exhausted,
    /// The effect may already have landed.
    EffectUnsafe,
    /// The run has no time left (§8.7).
    DeadlinePassed,
}

impl RetryRefusal {
    /// The reason code an operator surface shows.
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            Self::NotRetryable => "not_retryable",
            Self::Exhausted => "retries_exhausted",
            Self::EffectUnsafe => "effect_unsafe",
            Self::DeadlinePassed => "deadline_exceeded",
        }
    }
}

/// The backoff before attempt `n + 1`, measured from the last attempt's
/// settle: the outbox's curve, reused so operators learn one shape.
#[must_use]
pub fn retry_backoff_ms(attempts_used: usize) -> u64 {
    let exponent = attempts_used.saturating_sub(1).min(16) as u32;
    RETRY_BACKOFF_START_MS
        .saturating_mul(1u64 << exponent)
        .min(RETRY_BACKOFF_CAP_MS)
}

/// Decide whether to retry (the §8.6 rule, in one place).
pub fn retry_decision(
    step: &StepPlan,
    facts: &StepFacts,
    signals: EffectSignals,
    deadline_passed: bool,
) -> Retry {
    let Some(last) = facts.last() else {
        return Retry::Stop(RetryRefusal::NotRetryable);
    };
    if !last.outcome.is_retryable() {
        return Retry::Stop(RetryRefusal::NotRetryable);
    }
    if facts.attempts_used() > usize::from(step.retries) {
        return Retry::Stop(RetryRefusal::Exhausted);
    }
    if effect_gate(step, signals) == EffectGate::NeedsOperator {
        return Retry::Stop(RetryRefusal::EffectUnsafe);
    }
    if deadline_passed {
        return Retry::Stop(RetryRefusal::DeadlinePassed);
    }
    Retry::Later {
        attempt: facts.attempts_used() as u32 + 1,
        not_before_ms: last
            .ended_ms
            .saturating_add(retry_backoff_ms(facts.attempts_used())),
    }
}

/// Fold the run's outcome (§11.3).
///
/// Cancellation wins, then a failure, then (accepted or produced but
/// unevidenced) unverified, then success. A step that never settled makes
/// the run `failed`: the run did not do what it declared, and saying so is
/// the honest answer (J7).
#[must_use]
pub fn run_outcome(plan: &JobPlan, facts: &RunFacts<'_>, cancelled: bool) -> RunOutcome {
    if cancelled {
        return RunOutcome::Cancelled;
    }
    if facts.deadline_passed || facts.halted {
        return RunOutcome::Failed;
    }

    let mut unverified = false;
    for step in &plan.steps {
        let known = facts.step(&step.id);
        let outcome = known.and_then(StepFacts::outcome);
        let accepted = facts.accepts_unverified(&step.id);
        match outcome {
            None => {
                if !step.optional {
                    return RunOutcome::Failed;
                }
            }
            Some(AttemptOutcome::Succeeded) => {}
            Some(AttemptOutcome::Skipped) => {
                if !step.optional {
                    return RunOutcome::Failed;
                }
            }
            Some(AttemptOutcome::Unverified) => {
                if accepted || step.optional {
                    unverified = true;
                } else {
                    return RunOutcome::Failed;
                }
            }
            Some(AttemptOutcome::NeedsOperator) => return RunOutcome::Failed,
            Some(_) => {
                if !step.optional {
                    return RunOutcome::Failed;
                }
            }
        }
        if known.is_some_and(|known| known.in_flight.is_some()) {
            return RunOutcome::Failed;
        }
    }

    if unverified {
        RunOutcome::Unverified
    } else {
        RunOutcome::Succeeded
    }
}

// ---------------------------------------------------------------------------
// Tests (L1): the decision ladder, readiness, retries, the outcome fold
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::plan::{StepPlan, validate_plan};
    use serde_json::json;

    fn step(value: serde_json::Value) -> StepPlan {
        match serde_json::from_value(value) {
            Ok(step) => step,
            Err(error) => panic!("the fixture is a valid step: {error}"),
        }
    }

    fn agent(id: &str) -> StepPlan {
        step(json!({"id": id, "action": "agent", "prompt": "do it",
                    "verify": [{"check": "final_message_present"}]}))
    }

    fn plan() -> JobPlan {
        let plan = JobPlan {
            id: "digest".into(),
            limits: Default::default(),
            steps: vec![
                agent("fetch"),
                step(json!({"id": "summarise", "action": "agent", "prompt": "b",
                            "depends_on": ["fetch"],
                            "verify": [{"check": "final_message_present"}]})),
            ],
        };
        assert!(validate_plan(&plan).is_empty());
        plan
    }

    fn settled(step: &str, attempt: u32, outcome: AttemptOutcome) -> StepFacts {
        StepFacts {
            step: step.into(),
            settled: vec![AttemptFact {
                attempt,
                outcome,
                worker: Some(format!("w{attempt}")),
                ended_ms: 1_000,
            }],
            in_flight: None,
        }
    }

    #[test]
    fn a_completed_child_is_only_successful_with_evidence() {
        assert_eq!(
            attempt_outcome(EndReason::Completed, Some(true), true),
            AttemptOutcome::Succeeded
        );
        assert_eq!(
            attempt_outcome(EndReason::Completed, Some(false), true),
            AttemptOutcome::Failed
        );
        assert_eq!(
            attempt_outcome(EndReason::Completed, None, true),
            AttemptOutcome::Unverified
        );
        assert_eq!(
            attempt_outcome(EndReason::Completed, Some(true), false),
            AttemptOutcome::InvalidOutput
        );
    }

    #[test]
    fn non_completed_ends_never_map_onto_success() {
        assert_eq!(
            attempt_outcome(EndReason::Failed, Some(true), true),
            AttemptOutcome::Failed
        );
        assert_eq!(
            attempt_outcome(EndReason::Interrupted, Some(true), true),
            AttemptOutcome::Interrupted
        );
        assert_eq!(
            attempt_outcome(EndReason::Cancelled, None, true),
            AttemptOutcome::Interrupted
        );
        assert_eq!(
            attempt_outcome(EndReason::Rotated, None, true),
            AttemptOutcome::Interrupted
        );
        assert_eq!(
            attempt_outcome(EndReason::AwaitingInput, None, true),
            AttemptOutcome::NeedsOperator
        );
    }

    #[test]
    fn unverified_satisfies_a_dependency_only_when_accepted() {
        assert!(AttemptOutcome::Succeeded.is_success(false));
        assert!(!AttemptOutcome::Skipped.is_success(false));
        assert!(!AttemptOutcome::Unverified.is_success(false));
        assert!(AttemptOutcome::Unverified.is_success(true));
        assert!(!AttemptOutcome::Failed.is_success(true));
        assert!(!AttemptOutcome::NeedsOperator.is_success(true));
    }

    #[test]
    fn retryable_outcomes_are_the_recoverable_ones() {
        for outcome in [
            AttemptOutcome::Failed,
            AttemptOutcome::TimedOut,
            AttemptOutcome::Interrupted,
            AttemptOutcome::InvalidOutput,
        ] {
            assert!(outcome.is_retryable(), "{} retries", outcome.as_str());
        }
        for outcome in [
            AttemptOutcome::Succeeded,
            AttemptOutcome::Unverified,
            AttemptOutcome::NeedsOperator,
            AttemptOutcome::Skipped,
        ] {
            assert!(
                !outcome.is_retryable(),
                "{} never retries",
                outcome.as_str()
            );
        }
    }

    #[test]
    fn readiness_waits_for_every_dependency_and_input() {
        let plan = plan();
        let summarise = plan.step("summarise").expect("declared");
        let fetch = settled("fetch", 1, AttemptOutcome::Succeeded);
        let facts = RunFacts {
            steps: std::slice::from_ref(&fetch),
            ..RunFacts::default()
        };
        assert_eq!(readiness(&plan, &facts, summarise), Ok(()));

        let pending = StepFacts::pending("fetch");
        let facts = RunFacts {
            steps: std::slice::from_ref(&pending),
            ..RunFacts::default()
        };
        assert_eq!(
            readiness(&plan, &facts, summarise),
            Err(NotReady::Dependency {
                step: "fetch".into(),
                outcome: None
            })
        );

        let failed = settled("fetch", 1, AttemptOutcome::Failed);
        let facts = RunFacts {
            steps: std::slice::from_ref(&failed),
            ..RunFacts::default()
        };
        assert_eq!(
            readiness(&plan, &facts, summarise).map_err(|reason| reason.reason()),
            Err("dependency")
        );
    }

    #[test]
    fn readiness_refuses_in_flight_and_settled_steps() {
        let plan = plan();
        let fetch = plan.step("fetch").expect("declared");

        let mut in_flight = StepFacts::pending("fetch");
        in_flight.in_flight = Some(1);
        let facts = RunFacts {
            steps: std::slice::from_ref(&in_flight),
            ..RunFacts::default()
        };
        assert_eq!(
            readiness(&plan, &facts, fetch),
            Err(NotReady::InFlight { attempt: 1 })
        );

        let done = settled("fetch", 1, AttemptOutcome::Succeeded);
        let facts = RunFacts {
            steps: std::slice::from_ref(&done),
            ..RunFacts::default()
        };
        assert_eq!(
            readiness(&plan, &facts, fetch),
            Err(NotReady::Settled { attempts: 1 })
        );
    }

    #[test]
    fn readiness_refuses_a_halted_or_expired_run() {
        let plan = plan();
        let fetch = plan.step("fetch").expect("declared");
        let halted = RunFacts {
            halted: true,
            ..RunFacts::default()
        };
        assert_eq!(readiness(&plan, &halted, fetch), Err(NotReady::Halted));
        let expired = RunFacts {
            deadline_passed: true,
            ..RunFacts::default()
        };
        assert_eq!(
            readiness(&plan, &expired, fetch),
            Err(NotReady::DeadlinePassed)
        );
    }

    #[test]
    fn readiness_waits_for_a_consumed_artifact() {
        let consumer = step(json!({"id": "summarise", "action": "agent", "prompt": "a",
                                   "depends_on": ["fetch"],
                                   "inputs": [{"step": "fetch", "name": "news"}],
                                   "verify": [{"check": "final_message_present"}]}));
        let producer = step(json!({"id": "fetch", "action": "agent", "prompt": "a",
                                   "produces": {"name": "news"},
                                   "verify": [{"check": "final_message_present"}]}));
        let plan = JobPlan {
            id: "digest".into(),
            limits: Default::default(),
            steps: vec![producer, consumer.clone()],
        };
        assert!(validate_plan(&plan).is_empty());
        let fetch = settled("fetch", 1, AttemptOutcome::Succeeded);

        let missing = RunFacts {
            steps: std::slice::from_ref(&fetch),
            ..RunFacts::default()
        };
        assert_eq!(
            readiness(&plan, &missing, &consumer),
            Err(NotReady::Input {
                step: "fetch".into(),
                name: "news".into()
            })
        );

        let oversized = ArtifactFact {
            step: "fetch".into(),
            name: "news".into(),
            bytes: 300_000,
            within_cap: false,
        };
        let refused = RunFacts {
            steps: std::slice::from_ref(&fetch),
            artifacts: std::slice::from_ref(&oversized),
            ..RunFacts::default()
        };
        assert!(readiness(&plan, &refused, &consumer).is_err());

        let present = ArtifactFact {
            step: "fetch".into(),
            name: "news".into(),
            bytes: 1_024,
            within_cap: true,
        };
        let ok = RunFacts {
            steps: std::slice::from_ref(&fetch),
            artifacts: std::slice::from_ref(&present),
            ..RunFacts::default()
        };
        assert_eq!(readiness(&plan, &ok, &consumer), Ok(()));
    }

    #[test]
    fn a_read_or_idempotent_step_always_retries() {
        let read = agent("fetch");
        assert_eq!(
            effect_gate(&read, EffectSignals::default()),
            EffectGate::Safe
        );

        let idempotent = step(json!({"id": "send", "action": "agent", "prompt": "a",
                                     "effect": "write", "idempotent": true,
                                     "verify": [{"check": "final_message_present"}]}));
        assert_eq!(
            effect_gate(
                &idempotent,
                EffectSignals {
                    tool_calls: 9,
                    receipt: false
                }
            ),
            EffectGate::Safe
        );
    }

    #[test]
    fn a_non_idempotent_write_retries_only_when_nothing_was_issued() {
        let write = step(json!({"id": "send", "action": "agent", "prompt": "a",
                                "effect": "write",
                                "verify": [{"check": "final_message_present"}]}));
        assert_eq!(
            effect_gate(&write, EffectSignals::default()),
            EffectGate::Safe
        );
        assert_eq!(
            effect_gate(
                &write,
                EffectSignals {
                    tool_calls: 1,
                    receipt: false
                }
            ),
            EffectGate::NeedsOperator
        );
    }

    #[test]
    fn a_deliver_step_is_a_write_whatever_it_declares() {
        let deliver = step(
            json!({"id": "send", "action": "deliver", "channel": "imessage",
                                  "target": {"handle": "+1"}, "content": {"text": "hi"}}),
        );
        assert_eq!(effective_effect(&deliver), Effect::Write);
        assert_eq!(
            effect_gate(
                &deliver,
                EffectSignals {
                    tool_calls: 1,
                    receipt: false
                }
            ),
            EffectGate::NeedsOperator
        );
    }

    #[test]
    fn retry_waits_on_the_outbox_curve() {
        assert_eq!(retry_backoff_ms(1), 2_000);
        assert_eq!(retry_backoff_ms(2), 4_000);
        assert_eq!(retry_backoff_ms(3), 8_000);
        assert_eq!(retry_backoff_ms(10), 60_000);
    }

    #[test]
    fn retry_requires_a_retryable_outcome_and_a_remaining_budget() {
        let mut retrying = step(json!({"id": "fetch", "action": "agent", "prompt": "a",
                                       "retries": 1,
                                       "verify": [{"check": "final_message_present"}]}));
        retrying.retries = 1;
        let once = settled("fetch", 1, AttemptOutcome::Failed);
        assert_eq!(
            retry_decision(&retrying, &once, EffectSignals::default(), false),
            Retry::Later {
                attempt: 2,
                not_before_ms: 1_000 + 2_000
            }
        );

        let twice = StepFacts {
            step: "fetch".into(),
            settled: vec![
                AttemptFact {
                    attempt: 1,
                    outcome: AttemptOutcome::Failed,
                    worker: Some("w1".into()),
                    ended_ms: 1_000,
                },
                AttemptFact {
                    attempt: 2,
                    outcome: AttemptOutcome::Failed,
                    worker: Some("w2".into()),
                    ended_ms: 9_000,
                },
            ],
            in_flight: None,
        };
        assert_eq!(
            retry_decision(&retrying, &twice, EffectSignals::default(), false),
            Retry::Stop(RetryRefusal::Exhausted)
        );

        let succeeded = settled("fetch", 1, AttemptOutcome::Succeeded);
        assert_eq!(
            retry_decision(&retrying, &succeeded, EffectSignals::default(), false),
            Retry::Stop(RetryRefusal::NotRetryable)
        );
    }

    #[test]
    fn retry_refuses_an_unknown_effect_and_an_expired_deadline() {
        let write = step(json!({"id": "send", "action": "agent", "prompt": "a",
                                "effect": "write", "retries": 2,
                                "verify": [{"check": "final_message_present"}]}));
        let failed = settled("send", 1, AttemptOutcome::Failed);
        assert_eq!(
            retry_decision(
                &write,
                &failed,
                EffectSignals {
                    tool_calls: 1,
                    receipt: false
                },
                false
            ),
            Retry::Stop(RetryRefusal::EffectUnsafe)
        );

        let read = step(
            json!({"id": "fetch", "action": "agent", "prompt": "a", "retries": 1,
                               "verify": [{"check": "final_message_present"}]}),
        );
        assert_eq!(
            retry_decision(&read, &failed, EffectSignals::default(), true),
            Retry::Stop(RetryRefusal::DeadlinePassed)
        );
    }

    #[test]
    fn repeat_exclusion_names_the_failed_workers_only() {
        let facts = StepFacts {
            step: "fetch".into(),
            settled: vec![
                AttemptFact {
                    attempt: 1,
                    outcome: AttemptOutcome::Failed,
                    worker: Some("vultr".into()),
                    ended_ms: 1,
                },
                AttemptFact {
                    attempt: 2,
                    outcome: AttemptOutcome::TimedOut,
                    worker: Some("mac".into()),
                    ended_ms: 2,
                },
            ],
            in_flight: None,
        };
        assert_eq!(facts.exhausted_workers(), vec!["vultr", "mac"]);
    }

    #[test]
    fn a_cancelled_run_outranks_every_step_outcome() {
        let plan = plan();
        let facts = RunFacts::default();
        assert_eq!(run_outcome(&plan, &facts, true), RunOutcome::Cancelled);
    }

    #[test]
    fn a_failed_step_fails_the_run_and_an_optional_one_does_not() {
        let plan = plan();
        let fetch = settled("fetch", 1, AttemptOutcome::Failed);
        let summarise = settled("summarise", 1, AttemptOutcome::Succeeded);
        let facts = RunFacts {
            steps: &[fetch.clone(), summarise.clone()],
            ..RunFacts::default()
        };
        assert_eq!(run_outcome(&plan, &facts, false), RunOutcome::Failed);

        let optional_plan = JobPlan {
            id: "digest".into(),
            limits: Default::default(),
            steps: vec![
                step(
                    json!({"id": "fetch", "action": "agent", "prompt": "a", "optional": true,
                            "verify": [{"check": "final_message_present"}]}),
                ),
                agent("summarise"),
            ],
        };
        let facts = RunFacts {
            steps: &[fetch, summarise],
            ..RunFacts::default()
        };
        assert_eq!(
            run_outcome(&optional_plan, &facts, false),
            RunOutcome::Succeeded
        );
    }

    #[test]
    fn skipped_is_a_step_state_not_a_successful_dependency() {
        let plan = plan();
        let fetch = settled("fetch", 1, AttemptOutcome::Skipped);
        let summarise = settled("summarise", 1, AttemptOutcome::Succeeded);
        let facts = RunFacts {
            steps: &[fetch, summarise],
            ..RunFacts::default()
        };
        assert_eq!(run_outcome(&plan, &facts, false), RunOutcome::Failed);
    }

    #[test]
    fn needs_operator_is_fatal_even_when_its_step_is_optional() {
        let plan = JobPlan {
            id: "digest".into(),
            limits: Default::default(),
            steps: vec![step(
                json!({"id": "fetch", "action": "agent", "prompt": "a", "optional": true,
                        "on_failure": "continue",
                        "verify": [{"check": "final_message_present"}]}),
            )],
        };
        let facts = RunFacts {
            steps: &[settled("fetch", 1, AttemptOutcome::NeedsOperator)],
            ..RunFacts::default()
        };
        assert_eq!(run_outcome(&plan, &facts, false), RunOutcome::Failed);
    }

    #[test]
    fn accepted_unverified_steps_make_the_run_unverified_not_successful() {
        let plan = plan();
        let accepted = vec!["summarise".to_string()];
        let fetch = settled("fetch", 1, AttemptOutcome::Succeeded);
        let summarise = settled("summarise", 1, AttemptOutcome::Unverified);
        let facts = RunFacts {
            steps: &[fetch, summarise],
            accepted: &accepted,
            ..RunFacts::default()
        };
        assert_eq!(run_outcome(&plan, &facts, false), RunOutcome::Unverified);
    }

    #[test]
    fn a_run_that_never_finished_is_failed_not_succeeded() {
        let plan = plan();
        let fetch = settled("fetch", 1, AttemptOutcome::Succeeded);
        let facts = RunFacts {
            steps: std::slice::from_ref(&fetch),
            ..RunFacts::default()
        };
        assert_eq!(run_outcome(&plan, &facts, false), RunOutcome::Failed);

        let mut in_flight = settled("fetch", 1, AttemptOutcome::Succeeded);
        in_flight.step = "summarise".into();
        in_flight.in_flight = Some(2);
        let facts = RunFacts {
            steps: &[fetch, in_flight],
            ..RunFacts::default()
        };
        assert_eq!(run_outcome(&plan, &facts, false), RunOutcome::Failed);
    }

    #[test]
    fn a_halted_or_expired_run_is_failed() {
        let plan = plan();
        assert_eq!(
            run_outcome(
                &plan,
                &RunFacts {
                    halted: true,
                    ..RunFacts::default()
                },
                false
            ),
            RunOutcome::Failed
        );
        assert_eq!(
            run_outcome(
                &plan,
                &RunFacts {
                    deadline_passed: true,
                    ..RunFacts::default()
                },
                false
            ),
            RunOutcome::Failed
        );
    }

    #[test]
    fn a_complete_run_is_successful() {
        let plan = plan();
        let fetch = settled("fetch", 1, AttemptOutcome::Succeeded);
        let summarise = settled("summarise", 1, AttemptOutcome::Succeeded);
        let facts = RunFacts {
            steps: &[fetch, summarise],
            ..RunFacts::default()
        };
        assert_eq!(run_outcome(&plan, &facts, false), RunOutcome::Succeeded);
    }
}
