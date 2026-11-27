//! The run projection — everything an operator surface needs, derived by one
//! pass over the run's own stream (`design/JOB-MODEL.md` §15).
//!
//! Nothing here is a second source of truth: the view is a pure function of
//! committed events, disposable, and rebuildable after a restart (J1). The
//! same function backs `kian job status`, the alert text and the tests.

use std::collections::BTreeMap;

use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::SessionId;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::job::outcome::{
    ArtifactFact, AttemptFact, AttemptOutcome, RunFacts, RunOutcome, StepFacts, readiness,
    run_outcome,
};
use crate::job::plan::{JobPlan, StepAction};

/// The additive `job` key of the run's first `input/admitted` row (§5.2).
pub const JOB_KEY: &str = "job";

/// The tool name the conductor uses to record a produced artifact (§12.3).
pub const ARTIFACT_TOOL: &str = "job.artifact";

/// The gate name of the one verdict per run (§11.1). Step verdicts use
/// `step:<id>`; the run gate is spelled without a colon so the two can never
/// collide.
pub const RUN_GATE_NAME: &str = "run";

/// The gate name of one step's verdict (§11.1).
#[must_use]
pub fn step_gate_name(step: &str) -> String {
    format!("step:{step}")
}

/// Everything the run froze at admit (§5.2).
#[derive(Debug, Clone, PartialEq)]
pub struct JobMarker {
    /// `{job}@{slot}`.
    pub run_id: String,
    /// The declared job id.
    pub job_id: String,
    /// The slot that named this run.
    pub slot: String,
    /// `cron`, `manual` or `alert`.
    pub trigger: String,
    /// `sha256:…` over the frozen plan.
    pub plan_digest: String,
    /// The frozen plan itself.
    pub plan: JobPlan,
    /// The verified evidence pack a self-dev run was admitted over
    /// (SELF-EVOLUTION-MODEL §4, A11). Frozen with the marker, so every child
    /// of that run reads the same pack identity and no step can substitute a
    /// different one.
    pub selfdev: Option<Value>,
}

/// Why a frozen marker could not be trusted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkerError {
    /// The marker does not carry the frozen shape.
    Malformed(String),
    /// The declared digest and the declared plan disagree, so no step may be
    /// dispatched from these bytes.
    DigestMismatch {
        /// The digest the marker declares.
        declared: String,
        /// The digest recomputed from the plan it carries.
        computed: String,
    },
}

impl std::fmt::Display for MarkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed(why) => write!(f, "the job marker is malformed: {why}"),
            Self::DigestMismatch { declared, computed } => write!(
                f,
                "the job marker declares {declared} but its plan hashes to {computed}"
            ),
        }
    }
}

impl std::error::Error for MarkerError {}

/// The run's marker, if this stream carries one.
///
/// Scanned in reverse like the dream marker
/// (`crates/kian-memory/src/driver.rs`): the newest admitted row wins and the
/// message *text* is never consulted — the marker is a server-set key, not
/// something a model can forge (§5.2).
#[must_use]
pub fn marker(events: &[Event]) -> Option<Result<JobMarker, MarkerError>> {
    events
        .iter()
        .rev()
        .filter(|event| event.kind == EventKind::InputAdmitted)
        .find(|event| event.bytes.get(JOB_KEY).is_some())
        .map(|event| parse_marker(event.bytes.get(JOB_KEY).unwrap_or(&Value::Null)))
}

fn parse_marker(value: &Value) -> Result<JobMarker, MarkerError> {
    let plan_value = value
        .get("plan")
        .ok_or_else(|| MarkerError::Malformed("job.plan is present".into()))?;
    let plan: JobPlan = serde_json::from_value(plan_value.clone())
        .map_err(|error| MarkerError::Malformed(format!("job.plan: {error}")))?;

    let declared = text_field(value, "plan_digest")?;
    let computed = plan.digest();
    if declared != computed {
        return Err(MarkerError::DigestMismatch { declared, computed });
    }

    Ok(JobMarker {
        run_id: text_field(value, "run_id")?,
        job_id: text_field(value, "job_id")?,
        slot: text_field(value, "slot")?,
        trigger: text_field(value, "trigger")?,
        plan_digest: declared,
        plan,
        selfdev: value.get("selfdev").cloned(),
    })
}

fn text_field(value: &Value, key: &str) -> Result<String, MarkerError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| MarkerError::Malformed(format!("job.{key} is a string")))
}

/// The frozen marker a trigger writes into a run's first input row (§5.2).
#[must_use]
pub fn marker_payload(job_id: &str, slot: &str, trigger: &str, plan: &JobPlan) -> Value {
    marker_payload_with_selfdev(job_id, slot, trigger, plan, None)
}

/// The frozen marker of a run that carries self-dev facts alongside its plan.
///
/// The selfdev value was verified before admission, so anything that reads it
/// later reads a fact, not a claim. Its canonical bytes are what every child
/// of the run sees in its task text, so a re-drive builds identical input.
#[must_use]
pub fn marker_payload_with_selfdev(
    job_id: &str,
    slot: &str,
    trigger: &str,
    plan: &JobPlan,
    selfdev: Option<Value>,
) -> Value {
    let mut map = Map::new();
    map.insert(
        "run_id".into(),
        Value::String(crate::job::plan::run_id(job_id, slot)),
    );
    map.insert("job_id".into(), Value::String(job_id.to_string()));
    map.insert("slot".into(), Value::String(slot.to_string()));
    map.insert("trigger".into(), Value::String(trigger.to_string()));
    map.insert("plan_digest".into(), Value::String(plan.digest()));
    map.insert("plan".into(), plan.frozen_json());
    if let Some(selfdev) = selfdev {
        map.insert("selfdev".into(), selfdev);
    }
    Value::Object(map)
}

/// One dispatched attempt, as the run's own stream records it (§8.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchRecord {
    /// The step.
    pub step: String,
    /// The attempt number.
    pub attempt: u32,
    /// The worker id it was placed on (`conductor` for a deliver step).
    pub worker: String,
    /// The child thread.
    pub thread_id: Uuid,
    /// The child session.
    pub child: SessionId,
    /// When the record was written.
    pub at_ms: u64,
}

/// Every dispatch record on the run stream, in stream order.
#[must_use]
pub fn dispatches(events: &[Event]) -> Vec<DispatchRecord> {
    let mut out = Vec::new();
    for event in events {
        if event.kind != EventKind::ThreadStarted {
            continue;
        }
        let Some(job) = event.bytes.get(JOB_KEY) else {
            continue;
        };
        let (Some(step), Some(attempt)) = (
            job.get("step").and_then(Value::as_str),
            job.get("attempt").and_then(Value::as_u64),
        ) else {
            continue;
        };
        let Some(thread_id) = event
            .bytes
            .get("thread_id")
            .and_then(Value::as_str)
            .and_then(|raw| Uuid::parse_str(raw).ok())
        else {
            continue;
        };
        let Some(child) = event
            .bytes
            .get("worker")
            .and_then(Value::as_str)
            .and_then(|raw| Uuid::parse_str(raw).ok())
            .map(SessionId)
        else {
            continue;
        };
        let worker = event
            .bytes
            .get("exec_worker")
            .and_then(Value::as_str)
            .unwrap_or("conductor")
            .to_string();
        out.push(DispatchRecord {
            step: step.to_string(),
            attempt: attempt as u32,
            worker,
            thread_id,
            child,
            at_ms: event.ts,
        });
    }
    out
}

/// One step verdict the conductor wrote (§11.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerdictRecord {
    /// The step the verdict belongs to.
    pub step: String,
    /// Whether the declared evidence held.
    pub passed: bool,
    /// The attempt ordinal: the k-th verdict for a gate settles the k-th
    /// dispatch of that step, because attempts settle in order (§8.5).
    pub ordinal: u32,
    /// The outcome the conductor derived when it settled the attempt (§11.3).
    /// Read back from the verdict's additive `job` key, so a re-drive after a
    /// crash derives the same facts without re-deciding anything (J12).
    pub outcome: Option<AttemptOutcome>,
    /// When it was written.
    pub at_ms: u64,
}

/// Every step verdict on the run stream.
#[must_use]
pub fn verdicts(events: &[Event]) -> Vec<VerdictRecord> {
    let mut seen: BTreeMap<String, u32> = BTreeMap::new();
    let mut out = Vec::new();
    for event in events {
        if event.kind != EventKind::EvidenceVerdict {
            continue;
        }
        let Some(gate) = event.bytes.get("gate").and_then(Value::as_str) else {
            continue;
        };
        let Some(step) = gate.strip_prefix("step:") else {
            continue;
        };
        let passed = event
            .bytes
            .get("passed")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let outcome = event
            .bytes
            .get(JOB_KEY)
            .and_then(|job| job.get("outcome"))
            .and_then(Value::as_str)
            .and_then(AttemptOutcome::parse);
        let ordinal = seen.entry(step.to_string()).or_insert(0);
        *ordinal += 1;
        out.push(VerdictRecord {
            step: step.to_string(),
            passed,
            ordinal: *ordinal,
            outcome,
            at_ms: event.ts,
        });
    }
    out
}

/// Every artifact the run recorded, from its `job.artifact` intent (§12.3).
#[must_use]
pub fn artifacts(events: &[Event], plan: &JobPlan) -> Vec<ArtifactFact> {
    let mut out: Vec<ArtifactFact> = Vec::new();
    for event in events {
        if event.kind != EventKind::ToolCall {
            continue;
        }
        if event.bytes.get("tool").and_then(Value::as_str) != Some(ARTIFACT_TOOL) {
            continue;
        }
        let Some(artifact) = event
            .bytes
            .get("args")
            .and_then(|args| args.get("artifact"))
        else {
            continue;
        };
        let (Some(step), Some(name), Some(bytes)) = (
            artifact.get("step").and_then(Value::as_str),
            artifact.get("name").and_then(Value::as_str),
            artifact.get("bytes").and_then(Value::as_u64),
        ) else {
            continue;
        };
        let cap = plan
            .step(step)
            .and_then(|step| step.produces.as_ref())
            .map_or(u64::MAX, |produces| produces.max_bytes);
        let fact = ArtifactFact {
            step: step.to_string(),
            name: name.to_string(),
            bytes,
            within_cap: bytes <= cap,
        };
        match out
            .iter_mut()
            .find(|known| known.step == fact.step && known.name == fact.name)
        {
            Some(known) => *known = fact,
            None => out.push(fact),
        }
    }
    out
}

/// The one verdict per run (§11.1), and the run-level truth an operator
/// surface and the cron sweep read (§5.5).
#[derive(Debug, Clone, PartialEq)]
pub struct RunVerdict {
    /// The outcome the conductor derived.
    pub outcome: RunOutcome,
    /// The full payload (its additive `job` key carries the reason and the
    /// per-step outcomes).
    pub payload: Value,
    /// When it was written.
    pub at_ms: u64,
}

/// The run's verdict, if the conductor has settled it (§11.1).
///
/// The last one wins. A crash between the settle write and the end write is
/// healed by re-reading this: the conductor never writes a second verdict for
/// an outcome it already committed (J12, §17 W8).
#[must_use]
pub fn run_verdict(events: &[Event]) -> Option<RunVerdict> {
    let mut found = None;
    for event in events {
        if event.kind != EventKind::EvidenceVerdict {
            continue;
        }
        if event.bytes.get("gate").and_then(Value::as_str) != Some(RUN_GATE_NAME) {
            continue;
        }
        let Some(job) = event.bytes.get(JOB_KEY) else {
            continue;
        };
        let Some(outcome) = job
            .get("outcome")
            .and_then(Value::as_str)
            .and_then(RunOutcome::parse)
        else {
            continue;
        };
        found = Some(RunVerdict {
            outcome,
            payload: event.bytes.clone(),
            at_ms: event.ts,
        });
    }
    found
}

/// The payload of one step verdict (§11.1): the loop's verdict shape plus the
/// conductor's derived outcome under the additive `job` key. Written once per
/// settled attempt; a re-drive reads it back instead of re-deciding (J12).
#[must_use]
pub fn step_verdict_payload(
    step: &str,
    verdict: &kian_loop::evidence::GateVerdict,
    outcome: AttemptOutcome,
) -> Value {
    let mut payload = kian_loop::evidence::verdict_payload(&step_gate_name(step), verdict);
    payload[JOB_KEY] = serde_json::json!({
        "step": step,
        "attempt": verdict.attempt + 1,
        "outcome": outcome.as_str(),
    });
    payload
}

/// The payload of the run verdict (§11.1, §5.5). One check per plan step names
/// its outcome, so the run's evidence and the run's outcome are the same
/// object — never two records that can disagree.
#[must_use]
pub fn run_verdict_payload(
    outcome: RunOutcome,
    plan_digest: &str,
    steps: &[(String, AttemptOutcome)],
    failed_step: Option<&str>,
    reason: Option<&str>,
) -> Value {
    let checks: Vec<Value> = steps
        .iter()
        .map(|(step, outcome)| {
            serde_json::json!({
                "name": step,
                "passed": outcome.is_success(false),
                "detail": outcome.as_str(),
            })
        })
        .collect();
    let mut job = serde_json::json!({
        "outcome": outcome.as_str(),
        "plan_digest": plan_digest,
        "steps": steps
            .iter()
            .map(|(step, outcome)| serde_json::json!({
                "step": step,
                "outcome": outcome.as_str(),
            }))
            .collect::<Vec<_>>(),
    });
    if let Some(step) = failed_step {
        job["failed_step"] = Value::String(step.to_string());
    }
    if let Some(reason) = reason {
        job["reason"] = Value::String(reason.to_string());
    }
    serde_json::json!({
        "gate": RUN_GATE_NAME,
        "passed": outcome == RunOutcome::Succeeded,
        "checks": checks,
        "attempt": 0,
        JOB_KEY: job,
    })
}

/// The run's terminal outcome, if the run has ended (§5.5).
#[must_use]
pub fn ended(events: &[Event]) -> Option<RunOutcome> {
    // The conductor's own verdict is the job-level truth (§11.1): it is
    // written before the end and survives the engine owning the end write
    // (constraint 4 — a driver never writes `session/ended`).
    if let Some(verdict) = run_verdict(events) {
        return Some(verdict.outcome);
    }
    let mut final_outcome = None;
    for event in events {
        let EventKind::SessionEnded { reason } = &event.kind else {
            continue;
        };
        let declared = event
            .bytes
            .get(JOB_KEY)
            .and_then(|job| job.get("outcome"))
            .and_then(Value::as_str);
        final_outcome = Some(match declared {
            Some("succeeded") => RunOutcome::Succeeded,
            Some("unverified") => RunOutcome::Unverified,
            Some("cancelled") => RunOutcome::Cancelled,
            Some(_) => RunOutcome::Failed,
            None => match reason {
                EndReason::Cancelled => RunOutcome::Cancelled,
                EndReason::Completed => RunOutcome::Succeeded,
                _ => RunOutcome::Failed,
            },
        });
    }
    final_outcome
}

/// Whether the run's drain observed a cancellation (§5.4).
#[must_use]
pub fn cancelled(events: &[Event]) -> bool {
    events.iter().any(|event| {
        event.kind == EventKind::ControlInterrupt
            || matches!(
                &event.kind,
                EventKind::SessionEnded {
                    reason: EndReason::Cancelled
                }
            )
    })
}

/// A step's state as an operator sees it (§15.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepState {
    /// Waiting on a dependency, an input, or the scheduler.
    Pending,
    /// Everything it needs is in place; the next tick dispatches it.
    Ready,
    /// An attempt is running.
    Dispatched,
    /// The step finished and its outcome is final.
    Settled,
}

impl StepState {
    /// The spelling an operator sees.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Dispatched => "dispatched",
            Self::Settled => "settled",
        }
    }
}

/// One attempt as an operator sees it (§15.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptView {
    /// The attempt number.
    pub attempt: u32,
    /// Where it ran; `None` when the record does not name a worker.
    pub worker: Option<String>,
    /// When the dispatch record was written.
    pub started_ms: u64,
    /// When its verdict landed.
    pub ended_ms: Option<u64>,
    /// How it ended; `None` while it is still in flight.
    pub outcome: Option<AttemptOutcome>,
}

/// One step as an operator sees it (§15.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepView {
    /// The step id.
    pub id: String,
    /// What kind of step it is.
    pub action: StepAction,
    /// Its state.
    pub state: StepState,
    /// Its attempts, oldest first.
    pub attempts: Vec<AttemptView>,
}

/// One run as an operator sees it (§15.1).
#[derive(Debug, Clone, PartialEq)]
pub struct RunView {
    /// `{job}@{slot}`.
    pub run_id: String,
    /// The declared job.
    pub job_id: String,
    /// The slot.
    pub slot: String,
    /// The trigger that admitted it.
    pub trigger: String,
    /// `sha256:…` of the frozen plan.
    pub plan_digest: String,
    /// The run's outcome; `None` while it is still running.
    pub outcome: Option<RunOutcome>,
    /// The first event's timestamp.
    pub started_ms: u64,
    /// The end's timestamp.
    pub ended_ms: Option<u64>,
    /// Per-step progress, in plan order.
    pub steps: Vec<StepView>,
}

/// Assemble the view of one run from its own stream (§15.1).
///
/// `None` when the stream carries no job marker: a plain session is not a
/// run, and guessing would be a second source of truth.
#[must_use]
pub fn view(events: &[Event]) -> Option<RunView> {
    let marker = marker(events)?.ok()?;
    let plan = &marker.plan;
    let dispatch_log = dispatches(events);
    let verdict_log = verdicts(events);
    let artifact_log = artifacts(events, plan);
    let recorded_outcome = ended(events);

    let mut facts: Vec<StepFacts> = plan
        .steps
        .iter()
        .map(|step| StepFacts::pending(step.id.clone()))
        .collect();
    for dispatch in &dispatch_log {
        if let Some(facts) = facts.iter_mut().find(|facts| facts.step == dispatch.step) {
            facts.in_flight = Some(dispatch.attempt);
        }
    }
    for verdict in &verdict_log {
        let Some(facts) = facts.iter_mut().find(|facts| facts.step == verdict.step) else {
            continue;
        };
        if facts.in_flight == Some(verdict.ordinal) {
            facts.in_flight = None;
        }
        facts.settled.push(AttemptFact {
            attempt: verdict.ordinal,
            outcome: if verdict.passed {
                AttemptOutcome::Succeeded
            } else {
                AttemptOutcome::Failed
            },
            worker: dispatch_log
                .iter()
                .find(|dispatch| {
                    dispatch.step == verdict.step && dispatch.attempt == verdict.ordinal
                })
                .map(|dispatch| dispatch.worker.clone()),
            ended_ms: verdict.at_ms,
        });
    }

    let accepted = &plan.limits.accept_unverified;
    let run_facts = RunFacts {
        steps: &facts,
        artifacts: &artifact_log,
        accepted,
        halted: false,
        deadline_passed: false,
    };
    let outcome = recorded_outcome.or_else(|| cancelled(events).then_some(RunOutcome::Cancelled));

    let mut steps = Vec::with_capacity(plan.steps.len());
    for step in &plan.steps {
        let known = facts.iter().find(|facts| facts.step == step.id);
        let state = if known.is_some_and(|known| known.in_flight.is_some()) {
            StepState::Dispatched
        } else if known.is_some_and(StepFacts::is_settled) {
            StepState::Settled
        } else if outcome.is_some() {
            StepState::Pending
        } else if readiness(plan, &run_facts, step).is_ok() {
            StepState::Ready
        } else {
            StepState::Pending
        };
        let attempts = known
            .map(|known| {
                known
                    .settled
                    .iter()
                    .map(|attempt| AttemptView {
                        attempt: attempt.attempt,
                        worker: attempt.worker.clone(),
                        started_ms: dispatch_log
                            .iter()
                            .find(|dispatch| {
                                dispatch.step == step.id && dispatch.attempt == attempt.attempt
                            })
                            .map_or(0, |dispatch| dispatch.at_ms),
                        ended_ms: Some(attempt.ended_ms),
                        outcome: Some(attempt.outcome),
                    })
                    .collect()
            })
            .unwrap_or_default();
        steps.push(StepView {
            id: step.id.clone(),
            action: step.action,
            state,
            attempts,
        });
    }

    Some(RunView {
        run_id: marker.run_id,
        job_id: marker.job_id,
        slot: marker.slot,
        trigger: marker.trigger,
        plan_digest: marker.plan_digest,
        outcome,
        started_ms: events.first().map_or(0, |event| event.ts),
        ended_ms: events
            .iter()
            .rev()
            .find(|event| matches!(event.kind, EventKind::SessionEnded { .. }))
            .map(|event| event.ts),
        steps,
    })
}

/// The fold of a run whose end was not recorded. Tests and rendering only —
/// it is never written back as an outcome (§11.3).
#[must_use]
pub fn derived_outcome(plan: &JobPlan, events: &[Event]) -> RunOutcome {
    let artifact_log = artifacts(events, plan);
    let verdict_log = verdicts(events);
    let facts: Vec<StepFacts> = plan
        .steps
        .iter()
        .map(|step| {
            let mut facts = StepFacts::pending(step.id.clone());
            for verdict in verdict_log.iter().filter(|verdict| verdict.step == step.id) {
                facts.settled.push(AttemptFact {
                    attempt: verdict.ordinal,
                    outcome: if verdict.passed {
                        AttemptOutcome::Succeeded
                    } else {
                        AttemptOutcome::Failed
                    },
                    worker: None,
                    ended_ms: verdict.at_ms,
                });
            }
            facts
        })
        .collect();
    let run_facts = RunFacts {
        steps: &facts,
        artifacts: &artifact_log,
        accepted: &plan.limits.accept_unverified,
        halted: false,
        deadline_passed: false,
    };
    run_outcome(plan, &run_facts, cancelled(events))
}

/// Render a run for the operator (§15.2).
#[must_use]
pub fn render(view: &RunView) -> String {
    let mut out = String::new();
    out.push_str(&format!("run      {}\n", view.run_id));
    out.push_str(&format!(
        "job      {}   plan {}\n",
        view.job_id, view.plan_digest
    ));
    let status = view.outcome.map_or("running", RunOutcome::as_str);
    out.push_str(&format!("status   {status}\n"));
    out.push_str("steps\n");
    for step in &view.steps {
        let last = step.attempts.last();
        let worker = last
            .and_then(|attempt| attempt.worker.clone())
            .unwrap_or_else(|| "-".into());
        let outcome = last
            .and_then(|attempt| attempt.outcome)
            .map_or("-", AttemptOutcome::as_str);
        out.push_str(&format!(
            "  {:<10} {:<8} {:<12} {} attempt(s)  {}  {}\n",
            step.id,
            action_str(step.action),
            step.state.as_str(),
            step.attempts.len(),
            worker,
            outcome,
        ));
    }
    out
}

fn action_str(action: StepAction) -> &'static str {
    match action {
        StepAction::Agent => "agent",
        StepAction::Deliver => "deliver",
    }
}

// ---------------------------------------------------------------------------
// Tests (L1): the projection, the renderer, and the marker's integrity rules
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn plan() -> JobPlan {
        match serde_json::from_value(json!({
            "id": "digest",
            "steps": [
                {
                    "id": "fetch",
                    "prompt": "fetch the news",
                    "verify": [{"check": "final_message_present"}],
                    "produces": {"name": "raw", "max_bytes": 1024}
                },
                {
                    "id": "send",
                    "action": "deliver",
                    "depends_on": ["fetch"],
                    "inputs": [{"step": "fetch", "name": "raw"}],
                    "channel": "imessage",
                    "target": {"handle": "+15551234567"},
                    "content": {"from_artifact": {"step": "fetch", "name": "raw"}}
                }
            ]
        })) {
            Ok(plan) => plan,
            Err(error) => panic!("the fixture is a valid plan: {error}"),
        }
    }

    fn event(seq: u64, ts: u64, kind: EventKind, bytes: Value) -> Event {
        Event {
            seq,
            session: SessionId(Uuid::from_u128(1)),
            epoch: kian_core::model::Epoch(1),
            ts,
            kind,
            bytes,
            version: 0,
        }
    }

    fn admitted(seq: u64, ts: u64, plan: &JobPlan) -> Event {
        event(
            seq,
            ts,
            EventKind::InputAdmitted,
            json!({
                "role": "user",
                "content": "[kian-job] run=digest@slot step=- attempt=-",
                "job": marker_payload("digest", "slot", "cron", plan),
            }),
        )
    }

    fn dispatched(seq: u64, ts: u64, step: &str, attempt: u32, worker: &str) -> Event {
        event(
            seq,
            ts,
            EventKind::ThreadStarted,
            json!({
                "thread_id": Uuid::from_u128(100 + u128::from(attempt)).to_string(),
                "parent": Uuid::from_u128(1).to_string(),
                "worker": Uuid::from_u128(200 + u128::from(attempt)).to_string(),
                "pinned_version": 1,
                "task": {"role": "user", "content": "task"},
                "exec_worker": worker,
                "job": {"step": step, "attempt": attempt},
            }),
        )
    }

    fn verdict(seq: u64, ts: u64, step: &str, passed: bool) -> Event {
        event(
            seq,
            ts,
            EventKind::EvidenceVerdict,
            json!({
                "gate": format!("step:{step}"),
                "passed": passed,
                "checks": [],
                "attempt": 0,
            }),
        )
    }

    fn artifact(seq: u64, ts: u64, step: &str, bytes: u64) -> Event {
        event(
            seq,
            ts,
            EventKind::ToolCall,
            json!({
                "tool": ARTIFACT_TOOL,
                "call_id": format!("job:artifact:{step}:1"),
                "args": {
                    "artifact": {
                        "step": step,
                        "name": "raw",
                        "media": "text",
                        "bytes": bytes,
                        "sha256": "sha256:00",
                        "ref": {"session": Uuid::from_u128(1).to_string(), "seq": 3},
                    }
                },
            }),
        )
    }

    #[test]
    fn marker_round_trips_and_verifies_the_digest() {
        let plan = plan();
        let events = vec![admitted(1, 10, &plan)];
        let marker = match marker(&events) {
            Some(Ok(marker)) => marker,
            other => panic!("a valid marker: {other:?}"),
        };
        assert_eq!(marker.job_id, "digest");
        assert_eq!(marker.slot, "slot");
        assert_eq!(marker.trigger, "cron");
        assert_eq!(marker.run_id, crate::job::plan::run_id("digest", "slot"));
        assert_eq!(marker.plan_digest, plan.digest());
        assert_eq!(marker.plan, plan);
    }

    #[test]
    fn a_selfdev_marker_round_trips_its_frozen_value() {
        let plan = plan();
        let selfdev = json!({
            "pack": {"dir": "/tmp/pack", "manifest_sha256": "sha256:aa", "files_sha256": "sha256:bb"},
            "base_commit": "0".repeat(40),
            "kian_version": "0.3.0",
        });
        let payload =
            marker_payload_with_selfdev("digest", "slot", "manual", &plan, Some(selfdev.clone()));
        let events = vec![event(
            1,
            10,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "[kian-job] run=digest@slot", "job": payload}),
        )];
        let parsed = match marker(&events) {
            Some(Ok(parsed)) => parsed,
            other => panic!("a valid marker: {other:?}"),
        };
        assert_eq!(parsed.selfdev, Some(selfdev));

        let plain = marker_payload("digest", "slot", "cron", &plan);
        assert!(
            plain.get("selfdev").is_none(),
            "a plan-only marker carries no selfdev key"
        );
        let events = vec![event(
            1,
            10,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "[kian-job] run=digest@slot", "job": plain}),
        )];
        let parsed = match marker(&events) {
            Some(Ok(parsed)) => parsed,
            other => panic!("a valid marker: {other:?}"),
        };
        assert_eq!(parsed.selfdev, None);
    }

    #[test]
    fn marker_is_absent_on_a_stream_without_the_key() {
        let events = vec![event(
            1,
            10,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "hello"}),
        )];
        assert!(marker(&events).is_none());
    }

    #[test]
    fn marker_refuses_a_plan_that_disagrees_with_its_digest() {
        let plan = plan();
        let mut payload = marker_payload("digest", "slot", "cron", &plan);
        payload["plan"]["steps"][0]["prompt"] = json!("tampered");
        let events = vec![event(
            1,
            10,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "x", "job": payload}),
        )];
        match marker(&events) {
            Some(Err(MarkerError::DigestMismatch { declared, computed })) => {
                assert_ne!(declared, computed);
            }
            other => panic!("a digest mismatch: {other:?}"),
        }
    }

    #[test]
    fn dispatches_read_the_parent_side_record() {
        let events = vec![
            dispatched(2, 20, "fetch", 1, "vultr"),
            dispatched(3, 30, "fetch", 2, "laptop"),
        ];
        let records = dispatches(&events);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].step, "fetch");
        assert_eq!(records[0].attempt, 1);
        assert_eq!(records[0].worker, "vultr");
        assert_eq!(records[0].child, SessionId(Uuid::from_u128(201)));
        assert_eq!(records[0].thread_id, Uuid::from_u128(101));
        assert_eq!(records[1].worker, "laptop");
    }

    #[test]
    fn verdicts_number_attempts_per_step() {
        let events = vec![
            verdict(4, 40, "fetch", false),
            verdict(5, 50, "fetch", true),
            verdict(6, 60, "send", true),
        ];
        let records = verdicts(&events);
        assert_eq!(records.len(), 3);
        assert_eq!((records[0].step.as_str(), records[0].ordinal), ("fetch", 1));
        assert_eq!((records[1].step.as_str(), records[1].ordinal), ("fetch", 2));
        assert_eq!((records[2].step.as_str(), records[2].ordinal), ("send", 1));
        assert!(records[1].passed);
        assert!(!records[0].passed);
    }

    #[test]
    fn artifacts_flag_an_over_cap_production() {
        let plan = plan();
        let small = artifacts(&[artifact(3, 30, "fetch", 10)], &plan);
        assert_eq!(small.len(), 1);
        assert!(small[0].within_cap);

        let large = artifacts(&[artifact(3, 30, "fetch", 4096)], &plan);
        assert!(!large[0].within_cap);
    }

    #[test]
    fn view_tracks_pending_ready_dispatched_settled() {
        let plan = plan();
        let mut events = vec![admitted(1, 10, &plan)];

        let seen = view(&events).expect("a run view");
        assert_eq!(seen.steps[0].state, StepState::Ready);
        assert_eq!(seen.steps[1].state, StepState::Pending);
        assert!(seen.outcome.is_none());

        events.push(dispatched(2, 20, "fetch", 1, "vultr"));
        let seen = view(&events).expect("a run view");
        assert_eq!(seen.steps[0].state, StepState::Dispatched);

        events.push(artifact(3, 30, "fetch", 10));
        events.push(verdict(4, 40, "fetch", true));
        let seen = view(&events).expect("a run view");
        assert_eq!(seen.steps[0].state, StepState::Settled);
        assert_eq!(
            seen.steps[0].attempts[0].outcome,
            Some(AttemptOutcome::Succeeded)
        );
        assert_eq!(seen.steps[0].attempts[0].worker.as_deref(), Some("vultr"));
        assert_eq!(seen.steps[1].state, StepState::Ready);
    }

    #[test]
    fn view_is_none_for_a_plain_session() {
        let events = vec![event(1, 10, EventKind::SessionCreated, json!({}))];
        assert!(view(&events).is_none());
    }

    #[test]
    fn render_prints_one_line_per_step() {
        let plan = plan();
        let events = vec![
            admitted(1, 10, &plan),
            dispatched(2, 20, "fetch", 1, "vultr"),
            artifact(3, 30, "fetch", 10),
            verdict(4, 40, "fetch", true),
        ];
        let text = render(&view(&events).expect("a run view"));
        assert!(text.contains("run      digest@slot"));
        assert!(text.contains("status   running"));
        assert!(text.contains("fetch"));
        assert!(text.contains("vultr"));
        assert!(text.contains("agent"));
        assert!(text.contains("deliver"));
        assert_eq!(
            text.lines().filter(|line| line.starts_with("  ")).count(),
            2
        );
    }

    #[test]
    fn ended_reads_the_conductor_outcome() {
        let plan = plan();
        let events = vec![
            admitted(1, 10, &plan),
            event(
                5,
                50,
                EventKind::SessionEnded {
                    reason: EndReason::Completed,
                },
                json!({"job": {"outcome": "unverified", "plan_digest": plan.digest()}}),
            ),
        ];
        assert_eq!(ended(&events), Some(RunOutcome::Unverified));
        assert_eq!(
            view(&events).expect("a run view").outcome,
            Some(RunOutcome::Unverified)
        );
    }

    #[test]
    fn ended_falls_back_to_the_end_reason() {
        let events = vec![event(
            5,
            50,
            EventKind::SessionEnded {
                reason: EndReason::Failed,
            },
            json!({}),
        )];
        assert_eq!(ended(&events), Some(RunOutcome::Failed));
    }

    #[test]
    fn cancelled_detects_a_control_interrupt() {
        let events = vec![event(3, 30, EventKind::ControlInterrupt, json!({}))];
        assert!(cancelled(&events));
        assert!(!cancelled(&[event(
            1,
            10,
            EventKind::InputAdmitted,
            json!({})
        )]));
    }

    #[test]
    fn derived_outcome_needs_evidence_from_every_required_step() {
        let plan = plan();
        let mut events = vec![
            admitted(1, 10, &plan),
            dispatched(2, 20, "fetch", 1, "vultr"),
            artifact(3, 30, "fetch", 10),
            verdict(4, 40, "fetch", true),
        ];
        assert_eq!(derived_outcome(&plan, &events), RunOutcome::Failed);

        events.push(verdict(5, 50, "send", true));
        assert_eq!(derived_outcome(&plan, &events), RunOutcome::Succeeded);
    }
}
