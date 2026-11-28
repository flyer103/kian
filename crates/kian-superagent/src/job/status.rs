//! The run projection — everything an operator surface needs, derived by one
//! pass over the run's own stream (`design/JOB-MODEL.md` §15).
//!
//! Nothing here is a second source of truth: the view is a pure function of
//! committed events, disposable, and rebuildable after a restart (J1). The
//! same function backs `kian job status`, the alert text and the tests.

use std::collections::{BTreeMap, BTreeSet};

use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::SessionId;
use serde::Serialize;
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::job::outcome::{
    ArtifactFact, AttemptFact, AttemptOutcome, EffectSignals, Retry, RunFacts, RunOutcome,
    StepFacts, retry_decision, run_outcome,
};
use crate::job::plan::{JobPlan, StepAction};

/// The additive `job` key of the run's first `input/admitted` row (§5.2).
pub const JOB_KEY: &str = "job";

/// The v1 plan source written on newly admitted runs.
pub const PLAN_SOURCE_CONFIG: &str = "config";

/// The v1 plan author written on newly admitted runs.
pub const PLAN_AUTHOR_OPERATOR: &str = "operator";

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
    /// A second job-marked input tried to replace the run's frozen plan.
    Duplicate {
        /// The sequence of the first job-marked input.
        first_seq: u64,
        /// The sequence of the second job-marked input.
        second_seq: u64,
    },
    /// The job marker is not on the run's first admitted input.
    NotOnFirstInput {
        /// The sequence of the first admitted input.
        origin_seq: u64,
        /// The sequence of the job-marked input.
        marker_seq: u64,
    },
    /// A provenance value outside the v1 admission surface.
    InvalidProvenance {
        /// The field name.
        field: &'static str,
        /// The refused value.
        value: String,
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
            Self::Duplicate {
                first_seq,
                second_seq,
            } => write!(
                f,
                "the run has a second job marker at seq {second_seq} (first at seq {first_seq})"
            ),
            Self::NotOnFirstInput {
                origin_seq,
                marker_seq,
            } => write!(
                f,
                "the job marker is at seq {marker_seq}, but the run's first input is seq {origin_seq}"
            ),
            Self::InvalidProvenance { field, value } => write!(
                f,
                "job.{field} value {value:?} is not admitted in v1 (config/operator only)"
            ),
        }
    }
}

impl std::error::Error for MarkerError {}

/// A validated marker and its optional audit provenance.
#[derive(Debug, Clone, PartialEq)]
pub struct MarkerRecord {
    /// The frozen marker.
    pub marker: JobMarker,
    /// The source exactly as present on the row; absent on legacy rows.
    pub plan_source: Option<String>,
    /// The author exactly as present on the row; absent on legacy rows.
    pub plan_author: Option<String>,
}

/// Audit provenance as present on the first marker.
pub type MarkerProvenance = (Option<String>, Option<String>);

/// The first job-marked input plus an independently exposed duplicate.
#[derive(Debug, Clone, PartialEq)]
pub struct MarkerScan {
    /// The sequence of the run's first admitted input.
    pub first_input_seq: Option<u64>,
    /// The sequence of the first job-marked input.
    pub first_seq: Option<u64>,
    /// The first job-marked input, in stream order.
    pub first: Option<Result<MarkerRecord, MarkerError>>,
    /// The sequence of the second job-marked input, when one exists.
    pub duplicate_seq: Option<u64>,
}

/// Whether any admitted row carries the raw `job` key.
///
/// This deliberately does not parse: a malformed marker still belongs to the
/// conductor, which records `invalid_job_input` instead of falling through to
/// another driver.
#[must_use]
pub fn has_job_marker(events: &[Event]) -> bool {
    events
        .iter()
        .any(|event| event.kind == EventKind::InputAdmitted && event.bytes.get(JOB_KEY).is_some())
}

/// Scan run inputs in stream order.
///
/// The first job-marked row is the plan authority. A second job-marked row is
/// exposed separately so the conductor can settle the run invalid rather than
/// silently choosing either row. Message text is never consulted.
#[must_use]
pub fn scan_marker(events: &[Event]) -> MarkerScan {
    let mut first_input_seq = None;
    let mut first = None;
    let mut duplicate_seq = None;
    for event in events
        .iter()
        .filter(|event| event.kind == EventKind::InputAdmitted)
    {
        first_input_seq.get_or_insert(event.seq);
        let Some(value) = event.bytes.get(JOB_KEY) else {
            continue;
        };
        if first.is_none() {
            first = Some((event.seq, parse_marker_record(value)));
        } else {
            duplicate_seq = Some(event.seq);
            break;
        }
    }
    MarkerScan {
        first_input_seq,
        first_seq: first.as_ref().map(|(seq, _)| *seq),
        first: first.map(|(_, marker)| marker),
        duplicate_seq,
    }
}

/// The run's first marker, if this stream carries one.
///
/// A duplicate is an error, not a "newest wins" choice. The parser preserves
/// absent provenance for status; callers that need the default use
/// [`JobMarker::effective_plan_source`] or
/// [`JobMarker::effective_plan_author`].
#[must_use]
pub fn marker(events: &[Event]) -> Option<Result<JobMarker, MarkerError>> {
    let scan = scan_marker(events);
    match (scan.first, scan.duplicate_seq) {
        (None, _) => None,
        (Some(_), Some(second_seq)) => Some(Err(MarkerError::Duplicate {
            first_seq: scan.first_seq.unwrap_or(0),
            second_seq,
        })),
        (Some(record), None) => {
            if let (Some(origin_seq), Some(marker_seq)) = (scan.first_input_seq, scan.first_seq)
                && origin_seq != marker_seq
            {
                return Some(Err(MarkerError::NotOnFirstInput {
                    origin_seq,
                    marker_seq,
                }));
            }
            Some(record.map(|record| record.marker))
        }
    }
}

/// Parses and validates one frozen job marker.
pub fn parse_marker(value: &Value) -> Result<JobMarker, MarkerError> {
    parse_marker_record(value).map(|record| record.marker)
}

fn parse_marker_record(value: &Value) -> Result<MarkerRecord, MarkerError> {
    let plan_source = optional_text_field(value, "plan_source")?;
    if let Some(source) = plan_source.as_deref()
        && source != PLAN_SOURCE_CONFIG
    {
        return Err(MarkerError::InvalidProvenance {
            field: "plan_source",
            value: source.to_string(),
        });
    }
    let plan_author = optional_text_field(value, "plan_author")?;
    if let Some(author) = plan_author.as_deref()
        && author != PLAN_AUTHOR_OPERATOR
    {
        return Err(MarkerError::InvalidProvenance {
            field: "plan_author",
            value: author.to_string(),
        });
    }
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

    Ok(MarkerRecord {
        marker: JobMarker {
            run_id: text_field(value, "run_id")?,
            job_id: text_field(value, "job_id")?,
            slot: text_field(value, "slot")?,
            trigger: text_field(value, "trigger")?,
            plan_digest: declared,
            plan,
            selfdev: value.get("selfdev").cloned(),
        },
        plan_source,
        plan_author,
    })
}

impl MarkerRecord {
    /// The effective source, with the legacy default.
    #[must_use]
    pub fn effective_plan_source(&self) -> &str {
        self.plan_source.as_deref().unwrap_or(PLAN_SOURCE_CONFIG)
    }

    /// The effective author, with the legacy default.
    #[must_use]
    pub fn effective_plan_author(&self) -> &str {
        self.plan_author.as_deref().unwrap_or(PLAN_AUTHOR_OPERATOR)
    }
}

impl JobMarker {
    /// Whether two markers name the same frozen run and plan bytes.
    #[must_use]
    pub fn same_frozen_identity(&self, other: &Self) -> bool {
        self.run_id == other.run_id
            && self.job_id == other.job_id
            && self.slot == other.slot
            && self.trigger == other.trigger
            && self.plan_digest == other.plan_digest
            && self.plan == other.plan
            && self.selfdev == other.selfdev
    }
}

/// The first marker's provenance, preserving omission.
#[must_use]
pub fn provenance(events: &[Event]) -> Option<Result<MarkerProvenance, MarkerError>> {
    scan_marker(events)
        .first
        .map(|record| record.map(|record| (record.plan_source, record.plan_author)))
}

fn text_field(value: &Value, key: &str) -> Result<String, MarkerError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| MarkerError::Malformed(format!("job.{key} is a string")))
}

fn optional_text_field(value: &Value, key: &str) -> Result<Option<String>, MarkerError> {
    match value.get(key) {
        None => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(_) => Err(MarkerError::Malformed(format!("job.{key} is a string"))),
    }
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
    map.insert(
        "plan_source".into(),
        Value::String(PLAN_SOURCE_CONFIG.to_string()),
    );
    map.insert(
        "plan_author".into(),
        Value::String(PLAN_AUTHOR_OPERATOR.to_string()),
    );
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
    /// The frozen step digest named by the dispatch record.
    pub step_digest: Option<String>,
    /// The exact task message committed with the reservation.
    pub task: Value,
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
            step_digest: job
                .get("step_digest")
                .and_then(Value::as_str)
                .map(str::to_string),
            task: event.bytes.get("task").cloned().unwrap_or(Value::Null),
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

/// One first-wins step verdict as the status projection reads it.
#[derive(Debug, Clone, PartialEq)]
pub struct StepVerdictRecord {
    /// The step the verdict belongs to.
    pub step: String,
    /// The gate carried by the verdict.
    pub gate: String,
    /// The raw check results, preserved exactly as committed.
    pub checks: Option<Value>,
    /// The 1-based attempt named by the verdict.
    pub attempt: u32,
    /// The exact step outcome carried by A3.
    pub outcome: Option<AttemptOutcome>,
    /// Whether the declared evidence held.
    pub passed: bool,
    /// When the verdict committed.
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

/// Every step verdict in stream order, with the first verdict at each
/// `(step, attempt)` key retained and later rows ignored.
#[must_use]
pub fn step_verdicts(events: &[Event]) -> Vec<StepVerdictRecord> {
    let mut seen: BTreeSet<(String, u32)> = BTreeSet::new();
    let mut ordinals: BTreeMap<String, u32> = BTreeMap::new();
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
        let ordinal = ordinals.entry(step.to_string()).or_insert(0);
        *ordinal += 1;
        let attempt = event
            .bytes
            .get(JOB_KEY)
            .and_then(|job| job.get("attempt"))
            .and_then(Value::as_u64)
            .map(|attempt| attempt as u32)
            .or_else(|| {
                event
                    .bytes
                    .get("attempt")
                    .and_then(Value::as_u64)
                    .map(|attempt| attempt.saturating_add(1) as u32)
            })
            .unwrap_or(*ordinal);
        if !seen.insert((step.to_string(), attempt)) {
            continue;
        }
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
        out.push(StepVerdictRecord {
            step: step.to_string(),
            gate: gate.to_string(),
            checks: event.bytes.get("checks").cloned(),
            attempt,
            outcome,
            passed,
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
/// The first one wins. A crash between the settle write and the end write is
/// healed by re-reading this: the conductor never writes a second verdict for
/// an outcome it already committed (J12, §17 W8).
#[must_use]
pub fn run_verdict(events: &[Event]) -> Option<RunVerdict> {
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
        return Some(RunVerdict {
            outcome,
            payload: event.bytes.clone(),
            at_ms: event.ts,
        });
    }
    None
}

/// The payload of one step verdict (§11.1): the loop's verdict shape plus the
/// conductor's derived outcome under the additive `job` key. Written once per
/// settled attempt; a re-drive reads it back instead of re-deciding (J12).
#[must_use]
pub fn step_verdict_payload(
    run: &str,
    step: &str,
    verdict: &kian_loop::evidence::GateVerdict,
    outcome: AttemptOutcome,
) -> Value {
    let mut payload = kian_loop::evidence::verdict_payload(&step_gate_name(step), verdict);
    payload[JOB_KEY] = serde_json::json!({
        "run": run,
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
    run: Option<&str>,
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
    if let Some(run) = run {
        job["run"] = Value::String(run.to_string());
    }
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

/// The run's outcome as visible to a reader (§5.5, §15.1).
#[must_use]
pub fn ended(events: &[Event]) -> Option<RunOutcome> {
    if let Some(verdict) = run_verdict(events) {
        return Some(verdict.outcome);
    }
    let terminal = events.iter().rev().find_map(|event| match event.kind {
        EventKind::SessionEnded { reason } => Some(reason),
        _ => None,
    });
    if terminal.is_some_and(|reason| {
        matches!(
            reason,
            EndReason::Completed | EndReason::Cancelled | EndReason::Rotated
        )
    }) {
        return Some(RunOutcome::Failed);
    }
    None
}

/// Whether the run's drain observed a cancellation (§5.4).
#[must_use]
pub fn cancelled(events: &[Event]) -> bool {
    events
        .iter()
        .any(|event| event.kind == EventKind::ControlInterrupt)
}

/// The transient reason shown after a cancel row was consumed and before
/// the run's verdict commits the final cancellation.
pub const CANCELLATION_PENDING_REASON: &str = "cancellation_pending";

/// The persisted state an operator surface reports for one run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Running,
    Interrupted,
    Succeeded,
    Unverified,
    Failed,
    Cancelled,
}

impl RunStatus {
    /// The spelling an operator sees.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Interrupted => "interrupted",
            Self::Succeeded => "succeeded",
            Self::Unverified => "unverified",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    fn from_outcome(outcome: RunOutcome) -> Self {
        match outcome {
            RunOutcome::Succeeded => Self::Succeeded,
            RunOutcome::Unverified => Self::Unverified,
            RunOutcome::Failed => Self::Failed,
            RunOutcome::Cancelled => Self::Cancelled,
        }
    }
}

/// Whether a view used the optional runtime observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatusBasis {
    Runtime,
    StreamOnly,
}

impl StatusBasis {
    /// The spelling an operator sees.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::StreamOnly => "stream_only",
        }
    }
}

/// One observed runtime attempt. This is an observability hint, never an
/// execution or settlement authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeAttempt {
    pub step: String,
    pub attempt: u32,
}

/// Read-only process observations a status reader may blend into the pure
/// stream fold. No field is persisted and no execution path consumes it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunRuntimeFacts {
    /// Whether this process currently observes the run's drain as live.
    pub live_drain: bool,
    /// The attempt currently observed in flight, when known.
    pub in_flight: Option<RuntimeAttempt>,
    /// Per-step all-busy deferral deadlines from the live catalog/binding
    /// observation. Absent means the bound is unknown and must stay omitted.
    pub all_busy_next_due_ms: BTreeMap<String, u64>,
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
    /// The step will not be attempted in this run.
    Skipped,
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
            Self::Skipped => "skipped",
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
    /// The dispatch or delivery intent time, absent when the attempt
    /// settled before either record was written.
    pub started_ms: Option<u64>,
    /// When its verdict landed.
    pub ended_ms: Option<u64>,
    /// How it ended; `None` while it is still in flight.
    pub outcome: Option<AttemptOutcome>,
    /// The committing verdict's gate.
    pub gate: Option<String>,
    /// The committing verdict's checks, preserved exactly.
    pub checks: Option<Value>,
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
    /// Retry backoff or a runtime-observed all-busy deadline.
    pub next_due_ms: Option<u64>,
    /// The latest settled attempt's gate.
    pub gate: Option<String>,
    /// The latest settled attempt's checks.
    pub checks: Option<Value>,
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
    /// Audit provenance from A1; absent on legacy rows.
    pub plan_source: Option<String>,
    /// Audit provenance from A1; absent on legacy rows.
    pub plan_author: Option<String>,
    /// `sha256:…` of the frozen plan.
    pub plan_digest: String,
    /// The run's outcome; `None` while it is still running.
    pub outcome: Option<RunOutcome>,
    /// The operator status derived before any guessed liveness.
    pub status: RunStatus,
    /// Whether runtime observation was blended into the fold.
    pub status_basis: StatusBasis,
    /// The verdict reason, terminal fallback, or transient cancel note.
    pub reason: Option<String>,
    /// The first event's timestamp.
    pub started_ms: u64,
    /// The end's timestamp.
    pub ended_ms: Option<u64>,
    /// Per-step progress, in plan order.
    pub steps: Vec<StepView>,
    /// Every publish receipt the run recorded (§9.4 step 9).
    pub publish: Vec<PublishRecord>,
}

/// One publish receipt the run recorded (§9.4 step 9, A16).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRecord {
    /// The step that published.
    pub step: String,
    /// The attempt ordinal, read back from the call id.
    pub attempt: u32,
    /// The branch the commit landed on.
    pub branch: String,
    /// The full commit.
    pub commit: String,
    /// The gate's exit code, when one ran.
    pub gate_exit_code: Option<i64>,
    /// Whether the branch was pushed to its remote.
    pub pushed: bool,
    /// The pull request's URL, when one was opened.
    pub pull_request: Option<String>,
}

/// Assemble the view of one run from its own stream (§15.1).
///
/// `None` when the stream carries no job marker: a plain session is not a
/// run, and guessing would be a second source of truth.
#[must_use]
pub fn view(events: &[Event]) -> Option<RunView> {
    view_with_runtime(events, None)
}

/// Assemble the view of one run, optionally blending a read-only runtime
/// observation for liveness and all-busy deadlines. Runtime facts never
/// override a committed verdict and are never persisted.
#[must_use]
pub fn view_with_runtime(events: &[Event], runtime: Option<&RunRuntimeFacts>) -> Option<RunView> {
    let marker = scan_marker(events).first?.ok()?.marker;
    let (plan_source, plan_author) = match provenance(events) {
        Some(Ok(provenance)) => provenance,
        _ => (None, None),
    };
    let plan = &marker.plan;
    let dispatch_log = dispatches(events);
    let verdict_log = step_verdicts(events);
    let delivery_log = delivery_intents_with_time(events);
    let committed_verdict = run_verdict(events);
    let run_step_outcomes = committed_verdict
        .as_ref()
        .map(verdict_step_outcomes)
        .unwrap_or_default();

    let mut attempts: BTreeMap<(String, u32), AttemptView> = BTreeMap::new();
    for dispatch in &dispatch_log {
        attempts.insert(
            (dispatch.step.clone(), dispatch.attempt),
            AttemptView {
                attempt: dispatch.attempt,
                worker: Some(dispatch.worker.clone()),
                started_ms: Some(dispatch.at_ms),
                ended_ms: None,
                outcome: None,
                gate: None,
                checks: None,
            },
        );
    }
    for intent in &delivery_log {
        attempts
            .entry((intent.step.clone(), intent.attempt))
            .or_insert_with(|| AttemptView {
                attempt: intent.attempt,
                worker: None,
                started_ms: Some(intent.at_ms),
                ended_ms: None,
                outcome: None,
                gate: None,
                checks: None,
            });
    }
    for verdict in &verdict_log {
        let entry = attempts
            .entry((verdict.step.clone(), verdict.attempt))
            .or_insert_with(|| AttemptView {
                attempt: verdict.attempt,
                worker: None,
                started_ms: None,
                ended_ms: None,
                outcome: None,
                gate: None,
                checks: None,
            });
        if entry.ended_ms.is_none() {
            entry.ended_ms = Some(verdict.at_ms);
            entry.outcome = Some(verdict.outcome.unwrap_or(if verdict.passed {
                AttemptOutcome::Succeeded
            } else {
                AttemptOutcome::Failed
            }));
            entry.gate = Some(verdict.gate.clone());
            entry.checks = verdict.checks.clone();
        }
    }

    let mut steps = Vec::with_capacity(plan.steps.len());
    for step in &plan.steps {
        let step_attempts: Vec<AttemptView> = attempts
            .range((step.id.clone(), 0)..=(step.id.clone(), u32::MAX))
            .map(|(_, attempt)| attempt.clone())
            .collect();
        let in_flight = step_attempts
            .iter()
            .any(|attempt| attempt.ended_ms.is_none());
        let settled = step_attempts
            .iter()
            .any(|attempt| attempt.ended_ms.is_some());
        let retry_due = if in_flight {
            None
        } else {
            retry_due_ms(step, &step_attempts)
        };
        let runtime_due = (!settled)
            .then(|| runtime.and_then(|facts| facts.all_busy_next_due_ms.get(&step.id).copied()))
            .flatten();
        let (state, next_due_ms) = if in_flight {
            (StepState::Dispatched, None)
        } else if retry_due.is_some() || runtime_due.is_some() {
            (StepState::Ready, retry_due.or(runtime_due))
        } else if settled {
            (StepState::Settled, None)
        } else if committed_verdict.is_some()
            || run_step_outcomes.get(&step.id) == Some(&AttemptOutcome::Skipped)
            || dependency_skipped(plan, step, &attempts)
        {
            (StepState::Skipped, None)
        } else {
            (StepState::Pending, None)
        };
        let latest = step_attempts
            .iter()
            .filter(|attempt| attempt.ended_ms.is_some())
            .max_by_key(|attempt| attempt.attempt);
        let gate = latest.and_then(|attempt| attempt.gate.clone());
        let checks = latest.and_then(|attempt| attempt.checks.clone());
        steps.push(StepView {
            id: step.id.clone(),
            action: step.action,
            state,
            attempts: step_attempts,
            next_due_ms,
            gate,
            checks,
        });
    }

    let (status, reason) =
        derive_run_status(events, runtime, &attempts, committed_verdict.as_ref());
    let started_ms = scan_marker(events)
        .first_seq
        .and_then(|seq| events.iter().find(|event| event.seq == seq))
        .map_or_else(
            || events.first().map_or(0, |event| event.ts),
            |event| event.ts,
        );
    Some(RunView {
        run_id: marker.run_id,
        job_id: marker.job_id,
        slot: marker.slot,
        trigger: marker.trigger,
        plan_source,
        plan_author,
        plan_digest: marker.plan_digest,
        outcome: committed_verdict.as_ref().map(|verdict| verdict.outcome),
        status,
        status_basis: if runtime.is_some() {
            StatusBasis::Runtime
        } else {
            StatusBasis::StreamOnly
        },
        reason,
        started_ms,
        ended_ms: latest_end(events).map(|event| event.ts),
        steps,
        publish: publish_receipts(events),
    })
}

/// Build the pending view of an admitted run whose first input row has not
/// been consumed into the event stream yet. The row's marker is the same
/// frozen A1 payload, so identity and plan fields come from durable truth;
/// no execution has been observed.
#[must_use]
pub fn pending_view(
    marker: &JobMarker,
    plan_source: Option<String>,
    plan_author: Option<String>,
    admitted_at: u64,
) -> RunView {
    RunView {
        run_id: marker.run_id.clone(),
        job_id: marker.job_id.clone(),
        slot: marker.slot.clone(),
        trigger: marker.trigger.clone(),
        plan_source,
        plan_author,
        plan_digest: marker.plan_digest.clone(),
        outcome: None,
        status: RunStatus::Pending,
        status_basis: StatusBasis::StreamOnly,
        reason: None,
        started_ms: admitted_at,
        ended_ms: None,
        steps: marker
            .plan
            .steps
            .iter()
            .map(|step| StepView {
                id: step.id.clone(),
                action: step.action,
                state: StepState::Pending,
                attempts: Vec::new(),
                next_due_ms: None,
                gate: None,
                checks: None,
            })
            .collect(),
        publish: Vec::new(),
    }
}

#[derive(Debug, Clone)]
struct DeliveryIntentRecord {
    step: String,
    attempt: u32,
    at_ms: u64,
}

fn delivery_intents_with_time(events: &[Event]) -> Vec<DeliveryIntentRecord> {
    let mut out = Vec::new();
    for event in events {
        if event.kind != EventKind::ToolCall
            || event.bytes.get("tool").and_then(Value::as_str)
                != Some(crate::job::conduct::DELIVER_TOOL)
        {
            continue;
        }
        let Some(call_id) = event.bytes.get("call_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(rest) = call_id.strip_prefix(crate::job::conduct::DELIVER_CALL_PREFIX) else {
            continue;
        };
        let Some((step, attempt)) = rest.rsplit_once(':') else {
            continue;
        };
        let Ok(attempt) = attempt.parse::<u32>() else {
            continue;
        };
        out.push(DeliveryIntentRecord {
            step: step.to_string(),
            attempt,
            at_ms: event.ts,
        });
    }
    out
}

fn verdict_step_outcomes(verdict: &RunVerdict) -> BTreeMap<String, AttemptOutcome> {
    let mut out = BTreeMap::new();
    let Some(steps) = verdict
        .payload
        .get(JOB_KEY)
        .and_then(|job| job.get("steps"))
        .and_then(Value::as_array)
    else {
        return out;
    };
    for step in steps {
        let (Some(id), Some(outcome)) = (
            step.get("step").and_then(Value::as_str),
            step.get("outcome")
                .and_then(Value::as_str)
                .and_then(AttemptOutcome::parse),
        ) else {
            continue;
        };
        out.insert(id.to_string(), outcome);
    }
    out
}

fn retry_due_ms(step: &crate::job::plan::StepPlan, attempts: &[AttemptView]) -> Option<u64> {
    let facts = StepFacts {
        step: step.id.clone(),
        settled: attempts
            .iter()
            .filter_map(|attempt| {
                Some(AttemptFact {
                    attempt: attempt.attempt,
                    outcome: attempt.outcome?,
                    worker: attempt.worker.clone(),
                    ended_ms: attempt.ended_ms?,
                })
            })
            .collect(),
        in_flight: None,
    };
    match retry_decision(step, &facts, EffectSignals::default(), false) {
        Retry::Later { not_before_ms, .. } => Some(not_before_ms),
        Retry::Stop(_) => None,
    }
}

fn dependency_skipped(
    plan: &JobPlan,
    step: &crate::job::plan::StepPlan,
    attempts: &BTreeMap<(String, u32), AttemptView>,
) -> bool {
    let dependencies: BTreeSet<&str> = step
        .depends_on
        .iter()
        .map(String::as_str)
        .chain(step.inputs.iter().map(|input| input.step.as_str()))
        .collect();
    dependencies.into_iter().any(|dependency| {
        let outcome = attempts
            .range((dependency.to_string(), 0)..=(dependency.to_string(), u32::MAX))
            .filter_map(|(_, attempt)| attempt.outcome)
            .next_back();
        outcome.is_some_and(|outcome| {
            !outcome.is_success(
                plan.limits
                    .accept_unverified
                    .iter()
                    .any(|id| id == dependency),
            )
        })
    })
}

fn latest_end(events: &[Event]) -> Option<&Event> {
    events
        .iter()
        .rev()
        .find(|event| matches!(event.kind, EventKind::SessionEnded { .. }))
}

fn has_unended_drain(events: &[Event]) -> bool {
    let open_seq = events
        .iter()
        .rev()
        .find(|event| event.kind == EventKind::DrainClaimed)
        .map(|event| event.seq);
    let end_seq = latest_end(events).map(|event| event.seq);
    open_seq.is_some_and(|opened| end_seq.is_none_or(|end| opened > end))
}

fn derive_run_status(
    events: &[Event],
    runtime: Option<&RunRuntimeFacts>,
    attempts: &BTreeMap<(String, u32), AttemptView>,
    verdict: Option<&RunVerdict>,
) -> (RunStatus, Option<String>) {
    if let Some(verdict) = verdict {
        return (
            RunStatus::from_outcome(verdict.outcome),
            verdict
                .payload
                .get(JOB_KEY)
                .and_then(|job| job.get("reason"))
                .and_then(Value::as_str)
                .map(str::to_string),
        );
    }
    if let Some(end) = latest_end(events).filter(|end| {
        !has_unended_drain(events)
            || events.iter().any(|event| {
                matches!(
                    event.kind,
                    EventKind::SessionEnded {
                        reason: EndReason::Completed | EndReason::Cancelled | EndReason::Rotated
                    }
                ) && event.seq >= end.seq
            })
    }) {
        let reason = match end.kind {
            EventKind::SessionEnded {
                reason: EndReason::Completed | EndReason::Cancelled | EndReason::Rotated,
            } => RunStatus::Failed,
            EventKind::SessionEnded {
                reason:
                    EndReason::Interrupted
                    | EndReason::Failed
                    | EndReason::AwaitingInput
                    | EndReason::AwaitingBudget,
            } => RunStatus::Interrupted,
            _ => RunStatus::Pending,
        };
        let note = (reason == RunStatus::Failed)
            .then(|| crate::job::conduct::ENDED_WITHOUT_OUTCOME_REASON.to_string());
        return (reason, note);
    }
    if cancelled(events) {
        return (
            RunStatus::Running,
            Some(CANCELLATION_PENDING_REASON.to_string()),
        );
    }
    let has_live_attempt = attempts.values().any(|attempt| attempt.ended_ms.is_none());
    if let Some(runtime) = runtime {
        let live = runtime.live_drain || runtime.in_flight.is_some();
        return (
            if live {
                RunStatus::Running
            } else {
                RunStatus::Pending
            },
            None,
        );
    }
    if has_live_attempt || has_unended_drain(events) {
        (RunStatus::Running, None)
    } else {
        (RunStatus::Pending, None)
    }
}

/// Every successful publish receipt on the run stream (§9.4). A receipt with
/// `ok: false` published nothing a reader can follow, so it is not one.
#[must_use]
pub fn publish_receipts(events: &[Event]) -> Vec<PublishRecord> {
    let mut out = Vec::new();
    for event in events {
        if event.kind != EventKind::ToolResult {
            continue;
        }
        if event.bytes.get("ok").and_then(Value::as_bool) != Some(true) {
            continue;
        }
        let Some(call_id) = event.bytes.get("call_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(rest) = call_id.strip_prefix(crate::job::conduct::PUBLISH_CALL_PREFIX) else {
            continue;
        };
        let Some((step, attempt)) = rest.rsplit_once(':') else {
            continue;
        };
        let Ok(attempt) = attempt.parse::<u32>() else {
            continue;
        };
        let Some(receipt) = event.bytes.get("result") else {
            continue;
        };
        let (Some(branch), Some(commit)) = (
            receipt.get("branch").and_then(Value::as_str),
            receipt.get("commit").and_then(Value::as_str),
        ) else {
            continue;
        };
        out.push(PublishRecord {
            step: step.to_string(),
            attempt,
            branch: branch.to_string(),
            commit: commit.to_string(),
            gate_exit_code: receipt
                .get("gate")
                .and_then(|gate| gate.get("exit_code"))
                .and_then(Value::as_i64),
            pushed: receipt.get("push").is_some(),
            pull_request: receipt
                .get("pull_request")
                .and_then(|request| request.get("url"))
                .and_then(Value::as_str)
                .map(str::to_string),
        });
    }
    out
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
    out.push_str(&format!(
        "status   {} ({})\n",
        view.status.as_str(),
        view.status_basis.as_str()
    ));
    if let Some(reason) = &view.reason {
        out.push_str(&format!("reason   {reason}\n"));
    }
    for publish in &view.publish {
        let gate = publish
            .gate_exit_code
            .map_or_else(|| "off".to_string(), |code| format!("exit {code}"));
        let mut line = format!(
            "publish  {} attempt {}  {}  {}  gate {}",
            publish.step, publish.attempt, publish.branch, publish.commit, gate
        );
        if publish.pushed {
            line.push_str("  pushed");
        }
        if let Some(url) = &publish.pull_request {
            line.push_str(&format!("  pr {url}"));
        }
        line.push('\n');
        out.push_str(&line);
    }
    out.push_str("steps\n");
    for step in &view.steps {
        let last = step.attempts.last();
        let worker = last
            .and_then(|attempt| attempt.worker.clone())
            .unwrap_or_else(|| "-".into());
        let outcome = last
            .and_then(|attempt| attempt.outcome)
            .map_or("-", AttemptOutcome::as_str);
        let due = step
            .next_due_ms
            .map_or_else(String::new, |due| format!("  next_due_ms={due}"));
        out.push_str(&format!(
            "  {:<10} {:<8} {:<12} {} attempt(s)  {}  {}{}\n",
            step.id,
            action_str(step.action),
            step.state.as_str(),
            step.attempts.len(),
            worker,
            outcome,
            due,
        ));
    }
    out
}

fn action_str(action: StepAction) -> &'static str {
    match action {
        StepAction::Agent => "agent",
        StepAction::Deliver => "deliver",
        StepAction::Publish => "publish",
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

    fn step_verdict(
        seq: u64,
        ts: u64,
        step: &str,
        attempt: u32,
        outcome: AttemptOutcome,
        checks: Value,
    ) -> Event {
        event(
            seq,
            ts,
            EventKind::EvidenceVerdict,
            json!({
                "gate": format!("step:{step}"),
                "passed": outcome.is_success(true),
                "checks": checks,
                "attempt": attempt.saturating_sub(1),
                "job": {
                    "run": "digest@slot",
                    "step": step,
                    "attempt": attempt,
                    "outcome": outcome.as_str(),
                },
            }),
        )
    }

    fn run_verdict(
        seq: u64,
        ts: u64,
        plan: &JobPlan,
        outcome: RunOutcome,
        steps: &[(&str, AttemptOutcome)],
        reason: Option<&str>,
    ) -> Event {
        let mut job = json!({
            "outcome": outcome.as_str(),
            "plan_digest": plan.digest(),
            "steps": steps.iter().map(|(step, outcome)| json!({
                "step": step,
                "outcome": outcome.as_str(),
            })).collect::<Vec<Value>>(),
        });
        if let Some(reason) = reason {
            job["reason"] = json!(reason);
        }
        event(
            seq,
            ts,
            EventKind::EvidenceVerdict,
            json!({
                "gate": RUN_GATE_NAME,
                "passed": outcome == RunOutcome::Succeeded,
                "checks": [],
                "attempt": 0,
                "job": job,
            }),
        )
    }

    fn claimed(seq: u64, ts: u64) -> Event {
        event(
            seq,
            ts,
            EventKind::DrainClaimed,
            json!({"holder": "test", "epoch": 1}),
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
        let record = scan_marker(&events)
            .first
            .expect("the first marker")
            .expect("valid marker");
        assert_eq!(record.plan_source.as_deref(), Some(PLAN_SOURCE_CONFIG));
        assert_eq!(record.plan_author.as_deref(), Some(PLAN_AUTHOR_OPERATOR));
        assert_eq!(record.effective_plan_source(), PLAN_SOURCE_CONFIG);
        assert_eq!(record.effective_plan_author(), PLAN_AUTHOR_OPERATOR);
    }

    #[test]
    fn the_first_job_marked_input_is_authority_and_a_second_is_exposed() {
        let first_plan = plan();
        let mut second_plan = plan();
        second_plan.steps[0].prompt = Some("a later plan".to_string());
        let events = vec![
            admitted(1, 10, &first_plan),
            event(
                2,
                20,
                EventKind::InputAdmitted,
                json!({
                    "role": "user",
                    "content": "replace the plan",
                    "job": marker_payload("digest", "slot", "cron", &second_plan),
                }),
            ),
        ];
        let scan = scan_marker(&events);
        let first = scan
            .first
            .expect("the first marker")
            .expect("valid first marker");
        assert_eq!(first.marker.plan, first_plan);
        assert_eq!(scan.duplicate_seq, Some(2));
        assert!(matches!(
            marker(&events),
            Some(Err(MarkerError::Duplicate {
                first_seq: 1,
                second_seq: 2
            }))
        ));
    }

    #[test]
    fn malformed_job_key_still_classifies_as_job_marked() {
        let events = vec![event(
            1,
            10,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "x", "job": "not-an-object"}),
        )];
        assert!(has_job_marker(&events));
        assert!(matches!(
            marker(&events),
            Some(Err(MarkerError::Malformed(_)))
        ));
    }

    #[test]
    fn a_job_marker_after_the_first_input_is_not_authority() {
        let plan = plan();
        let mut late = admitted(2, 20, &plan);
        late.bytes["content"] = json!("run later");
        let events = vec![
            event(
                1,
                10,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "plain first"}),
            ),
            late,
        ];
        let scan = scan_marker(&events);
        assert_eq!(scan.first_input_seq, Some(1));
        assert_eq!(scan.first_seq, Some(2));
        assert!(matches!(
            marker(&events),
            Some(Err(MarkerError::NotOnFirstInput {
                origin_seq: 1,
                marker_seq: 2
            }))
        ));
    }

    #[test]
    fn legacy_provenance_remains_absent_but_has_v1_effective_values() {
        let plan = plan();
        let mut payload = marker_payload("digest", "slot", "cron", &plan);
        payload
            .as_object_mut()
            .expect("object")
            .remove("plan_source");
        payload
            .as_object_mut()
            .expect("object")
            .remove("plan_author");
        let events = vec![event(
            1,
            10,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "x", "job": payload}),
        )];
        let record = scan_marker(&events)
            .first
            .expect("legacy marker")
            .expect("valid legacy marker");
        assert_eq!(record.plan_source, None);
        assert_eq!(record.plan_author, None);
        assert_eq!(record.effective_plan_source(), PLAN_SOURCE_CONFIG);
        assert_eq!(record.effective_plan_author(), PLAN_AUTHOR_OPERATOR);
    }

    #[test]
    fn v1_refuses_other_provenance_values() {
        let plan = plan();
        for (field, value) in [
            ("plan_source", "inline"),
            ("plan_source", "router"),
            ("plan_source", "unknown"),
            ("plan_author", "router-session"),
            ("plan_author", "external-planner"),
        ] {
            let mut payload = marker_payload("digest", "slot", "cron", &plan);
            payload[field] = json!(value);
            let events = vec![event(
                1,
                10,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "x", "job": payload}),
            )];
            assert!(
                matches!(
                    marker(&events),
                    Some(Err(MarkerError::InvalidProvenance { field: refused, .. }))
                        if refused == field
                ),
                "{field}={value:?}"
            );
        }
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
        assert_eq!(seen.status, RunStatus::Pending);
        assert_eq!(seen.status_basis, StatusBasis::StreamOnly);
        assert_eq!(seen.steps[0].state, StepState::Pending);
        assert_eq!(seen.steps[1].state, StepState::Pending);
        assert!(seen.outcome.is_none());

        events.push(dispatched(2, 20, "fetch", 1, "vultr"));
        let seen = view(&events).expect("a run view");
        assert_eq!(seen.status, RunStatus::Running);
        assert_eq!(seen.steps[0].state, StepState::Dispatched);
        assert_eq!(seen.steps[0].attempts[0].started_ms, Some(20));
        assert_eq!(seen.steps[0].attempts[0].ended_ms, None);

        events.push(artifact(3, 30, "fetch", 10));
        events.push(verdict(4, 40, "fetch", true));
        let seen = view(&events).expect("a run view");
        assert_eq!(seen.steps[0].state, StepState::Settled);
        assert_eq!(
            seen.steps[0].attempts[0].outcome,
            Some(AttemptOutcome::Succeeded)
        );
        assert_eq!(seen.steps[0].attempts[0].worker.as_deref(), Some("vultr"));
        assert_eq!(seen.steps[1].state, StepState::Pending);
    }

    #[test]
    fn view_is_none_for_a_plain_session() {
        let events = vec![event(1, 10, EventKind::SessionCreated, json!({}))];
        assert!(view(&events).is_none());
    }

    #[test]
    fn a_duplicate_marker_still_renders_the_invalid_run() {
        let plan = plan();
        let mut second_plan = plan.clone();
        second_plan.steps[0].prompt = Some("a later plan".to_string());
        let events = vec![
            admitted(1, 10, &plan),
            event(
                2,
                20,
                EventKind::InputAdmitted,
                json!({
                    "role": "user",
                    "content": "replace the plan",
                    "job": marker_payload("digest", "slot", "cron", &second_plan),
                }),
            ),
            run_verdict(
                3,
                30,
                &plan,
                RunOutcome::Failed,
                &[],
                Some("invalid_job_input"),
            ),
        ];
        let seen = view(&events).expect("the first marker is still the run identity");
        assert_eq!(seen.outcome, Some(RunOutcome::Failed));
        assert_eq!(seen.status, RunStatus::Failed);
        assert_eq!(seen.reason.as_deref(), Some("invalid_job_input"));
    }

    #[test]
    fn status_follows_the_first_committed_run_verdict_reason() {
        let plan = plan();
        let events = vec![
            admitted(1, 10, &plan),
            claimed(2, 20),
            run_verdict(
                3,
                30,
                &plan,
                RunOutcome::Failed,
                &[
                    ("fetch", AttemptOutcome::Succeeded),
                    ("send", AttemptOutcome::TimedOut),
                ],
                Some("step_timed_out"),
            ),
            run_verdict(4, 40, &plan, RunOutcome::Succeeded, &[], None),
            event(
                5,
                50,
                EventKind::SessionEnded {
                    reason: EndReason::Completed,
                },
                json!({}),
            ),
        ];
        let seen = view(&events).expect("a run view");
        assert_eq!(seen.status, RunStatus::Failed);
        assert_eq!(seen.outcome, Some(RunOutcome::Failed));
        assert_eq!(seen.reason.as_deref(), Some("step_timed_out"));
        assert_eq!(seen.steps[0].attempts.first().map(|a| a.started_ms), None);
    }

    #[test]
    fn terminal_and_resumable_ends_follow_the_status_priority() {
        let plan = plan();
        let terminal = vec![
            admitted(1, 10, &plan),
            claimed(2, 20),
            event(
                3,
                30,
                EventKind::SessionEnded {
                    reason: EndReason::Completed,
                },
                json!({}),
            ),
        ];
        let seen = view(&terminal).expect("terminal view");
        assert_eq!(seen.status, RunStatus::Failed);
        assert_eq!(seen.outcome, None, "an end never invents a run verdict");
        assert_eq!(seen.reason.as_deref(), Some("ended_without_outcome"));

        let interrupted = vec![
            admitted(1, 10, &plan),
            claimed(2, 20),
            event(
                3,
                30,
                EventKind::SessionEnded {
                    reason: EndReason::Interrupted,
                },
                json!({}),
            ),
        ];
        let seen = view(&interrupted).expect("interrupted view");
        assert_eq!(seen.status, RunStatus::Interrupted);
        assert_eq!(seen.reason, None);
    }

    #[test]
    fn a_consumed_cancel_is_running_until_the_verdict() {
        let plan = plan();
        let events = vec![
            admitted(1, 10, &plan),
            claimed(2, 20),
            event(
                3,
                30,
                EventKind::ControlInterrupt,
                json!({"action": "interrupt", "reason": "test"}),
            ),
        ];
        let seen = view(&events).expect("a run view");
        assert_eq!(seen.status, RunStatus::Running);
        assert_eq!(seen.reason.as_deref(), Some(CANCELLATION_PENDING_REASON));
    }

    #[test]
    fn runtime_liveness_refines_an_unended_residency_without_guessing_from_age() {
        let plan = plan();
        let events = vec![admitted(1, 10, &plan), claimed(2, 20)];
        let stream_only = view(&events).expect("stream-only view");
        assert_eq!(stream_only.status, RunStatus::Running);
        assert_eq!(stream_only.status_basis, StatusBasis::StreamOnly);

        let dead = RunRuntimeFacts::default();
        let observed = view_with_runtime(&events, Some(&dead)).expect("runtime view");
        assert_eq!(observed.status, RunStatus::Pending);
        assert_eq!(observed.status_basis, StatusBasis::Runtime);

        let live = RunRuntimeFacts {
            live_drain: true,
            ..RunRuntimeFacts::default()
        };
        let observed = view_with_runtime(&events, Some(&live)).expect("live view");
        assert_eq!(observed.status, RunStatus::Running);
    }

    #[test]
    fn dependency_skip_and_retry_backoff_are_stream_derived() {
        let mut plan = plan();
        plan.steps[0].retries = 1;
        let events = vec![
            admitted(1, 10, &plan),
            claimed(2, 20),
            step_verdict(
                3,
                30,
                "fetch",
                1,
                AttemptOutcome::Failed,
                json!([{"check": "final_message_present", "passed": false}]),
            ),
        ];
        let seen = view(&events).expect("retry view");
        assert_eq!(seen.steps[0].state, StepState::Ready);
        assert_eq!(seen.steps[0].next_due_ms, Some(2_030));
        assert_eq!(seen.steps[1].state, StepState::Skipped);
        assert_eq!(seen.steps[0].gate.as_deref(), Some("step:fetch"));
        assert_eq!(
            seen.steps[0]
                .checks
                .as_ref()
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(1)
        );

        let committed = view(&[
            admitted(1, 10, &plan),
            claimed(2, 20),
            run_verdict(
                3,
                30,
                &plan,
                RunOutcome::Failed,
                &[
                    ("fetch", AttemptOutcome::Succeeded),
                    ("send", AttemptOutcome::Skipped),
                ],
                Some("dependency_failed"),
            ),
        ])
        .expect("committed view");
        assert_eq!(committed.steps[1].state, StepState::Skipped);
        assert!(committed.steps[1].attempts.is_empty());
    }

    #[test]
    fn step_verdicts_are_first_wins_and_keep_exact_outcomes_and_checks() {
        let plan = plan();
        let checks = json!([{"check": "final_message_present", "passed": true}]);
        let events = vec![
            admitted(1, 10, &plan),
            claimed(2, 20),
            step_verdict(
                3,
                30,
                "fetch",
                1,
                AttemptOutcome::InvalidOutput,
                checks.clone(),
            ),
            step_verdict(4, 40, "fetch", 1, AttemptOutcome::Succeeded, json!([])),
        ];
        let seen = view(&events).expect("a run view");
        assert_eq!(
            seen.steps[0].attempts[0].outcome,
            Some(AttemptOutcome::InvalidOutput)
        );
        assert_eq!(seen.steps[0].attempts[0].checks.as_ref(), Some(&checks));
        assert_eq!(seen.steps[0].attempts.len(), 1);
    }

    #[test]
    fn delivery_intent_is_an_in_flight_attempt_and_runtime_can_supply_all_busy_due() {
        let plan = plan();
        let delivery = event(
            3,
            30,
            EventKind::ToolCall,
            json!({
                "tool": crate::job::conduct::DELIVER_TOOL,
                "call_id": "job:deliver:send:1",
                "args": {"channel": "imessage", "target": {"handle": "+1"}, "content": {"text": "hi"}},
            }),
        );
        let events = vec![admitted(1, 10, &plan), claimed(2, 20), delivery];
        let seen = view(&events).expect("delivery view");
        assert_eq!(seen.steps[1].state, StepState::Dispatched);
        assert_eq!(seen.steps[1].attempts[0].started_ms, Some(30));
        assert_eq!(seen.steps[1].attempts[0].ended_ms, None);

        let mut all_busy = BTreeMap::new();
        all_busy.insert("fetch".to_string(), 9_999);
        let runtime = RunRuntimeFacts {
            live_drain: true,
            all_busy_next_due_ms: all_busy,
            ..RunRuntimeFacts::default()
        };
        let waiting =
            view_with_runtime(&[admitted(1, 10, &plan)], Some(&runtime)).expect("all-busy view");
        assert_eq!(waiting.steps[0].state, StepState::Ready);
        assert_eq!(waiting.steps[0].next_due_ms, Some(9_999));
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
        assert!(text.contains("status   pending (stream_only)"));
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
    fn run_verdict_is_truth_and_the_end_payload_is_ignored() {
        let plan = plan();
        let events = vec![
            admitted(1, 10, &plan),
            event(
                4,
                40,
                EventKind::EvidenceVerdict,
                json!({
                    "gate": "run",
                    "passed": false,
                    "checks": [],
                    "attempt": 0,
                    "job": {
                        "outcome": "unverified",
                        "plan_digest": plan.digest(),
                        "steps": []
                    }
                }),
            ),
            event(
                5,
                50,
                EventKind::SessionEnded {
                    reason: EndReason::Completed,
                },
                json!({"job": {"outcome": "succeeded"}}),
            ),
        ];
        assert_eq!(ended(&events), Some(RunOutcome::Unverified));
        assert_eq!(
            view(&events).expect("a run view").outcome,
            Some(RunOutcome::Unverified)
        );
    }

    #[test]
    fn the_first_committed_run_verdict_wins() {
        let plan = plan();
        let verdict = |seq, outcome: &str, passed| {
            event(
                seq,
                50 + seq,
                EventKind::EvidenceVerdict,
                json!({
                    "gate": "run",
                    "passed": passed,
                    "checks": [],
                    "attempt": 0,
                    "job": {
                        "outcome": outcome,
                        "plan_digest": plan.digest(),
                        "steps": []
                    }
                }),
            )
        };
        let events = vec![
            admitted(1, 10, &plan),
            verdict(2, "failed", false),
            verdict(3, "succeeded", true),
        ];
        assert_eq!(ended(&events), Some(RunOutcome::Failed));
    }

    #[test]
    fn a_terminal_end_without_a_run_verdict_is_failed() {
        let events = vec![event(
            5,
            50,
            EventKind::SessionEnded {
                reason: EndReason::Completed,
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

    #[test]
    fn a3_verdict_payloads_carry_the_run_identity() {
        let step = step_verdict_payload(
            "build@manual:01J0000000000000000000000",
            "fetch",
            &kian_loop::evidence::GateVerdict {
                passed: true,
                checks: Vec::new(),
                attempt: 2,
            },
            AttemptOutcome::Succeeded,
        );
        assert_eq!(
            step[JOB_KEY]["run"],
            json!("build@manual:01J0000000000000000000000")
        );
        assert_eq!(step[JOB_KEY]["attempt"], json!(3));

        let run = run_verdict_payload(
            Some("build@manual:01J0000000000000000000000"),
            RunOutcome::Failed,
            "sha256:abc",
            &[("fetch".to_string(), AttemptOutcome::Failed)],
            Some("fetch"),
            Some("step_failed"),
        );
        assert_eq!(
            run[JOB_KEY]["run"],
            json!("build@manual:01J0000000000000000000000")
        );
        assert_eq!(run[JOB_KEY]["reason"], json!("step_failed"));
    }
}
