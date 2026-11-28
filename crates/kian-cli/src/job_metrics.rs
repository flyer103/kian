//! Read-side job metrics derived from committed A3 verdicts (JOB-MODEL
//! §14.3). A rebuild recomputes the counters; no conductor increments them.

use std::collections::{BTreeMap, BTreeSet};

use kian_core::events::{Event, EventKind};
use kian_store::EventStore;
use kian_superagent::job::outcome::{AttemptOutcome, RunOutcome};
use kian_superagent::job::plan::StepAction;
use kian_superagent::job::status;
use serde_json::{Value, json};

/// The same bounded global kind read used by the run index.
pub const JOB_METRICS_SCAN_LIMIT: u64 = 100_000;

const RUN_OUTCOMES: [RunOutcome; 4] = [
    RunOutcome::Succeeded,
    RunOutcome::Unverified,
    RunOutcome::Failed,
    RunOutcome::Cancelled,
];

const ATTEMPT_OUTCOMES: [AttemptOutcome; 8] = [
    AttemptOutcome::Skipped,
    AttemptOutcome::Succeeded,
    AttemptOutcome::Unverified,
    AttemptOutcome::InvalidOutput,
    AttemptOutcome::TimedOut,
    AttemptOutcome::Interrupted,
    AttemptOutcome::NeedsOperator,
    AttemptOutcome::Failed,
];

/// One recomputed metric snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobMetrics {
    pub runs: BTreeMap<&'static str, u64>,
    pub steps: BTreeMap<&'static str, BTreeMap<&'static str, u64>>,
}

impl JobMetrics {
    fn empty() -> Self {
        let runs = RUN_OUTCOMES
            .iter()
            .map(|outcome| (outcome.as_str(), 0))
            .collect();
        let steps = ["agent", "deliver"]
            .into_iter()
            .map(|action| {
                (
                    action,
                    ATTEMPT_OUTCOMES
                        .iter()
                        .map(|outcome| (outcome.as_str(), 0))
                        .collect(),
                )
            })
            .collect();
        Self { runs, steps }
    }

    fn json(&self) -> Value {
        json!({
            "job_runs_total": self.runs,
            "job_step_attempts_total": self.steps,
        })
    }
}

/// Recompute the job metrics on a `/metrics` read. The evidence read is
/// kind-filtered; the admitted read only recovers each step's frozen action
/// because A3's step payload does not duplicate it.
pub async fn report(store: &EventStore) -> Value {
    let admitted = match store
        .scan_kind_across_sessions(EventKind::InputAdmitted, JOB_METRICS_SCAN_LIMIT)
        .await
    {
        Ok(events) => events,
        Err(error) => return json!({"error": error.to_string()}),
    };
    let verdicts = match store
        .scan_kind_across_sessions(EventKind::EvidenceVerdict, JOB_METRICS_SCAN_LIMIT)
        .await
    {
        Ok(events) => events,
        Err(error) => return json!({"error": error.to_string()}),
    };
    let mut report = fold(&admitted, &verdicts).json();
    report["truncated"] = json!(
        admitted.len() as u64 == JOB_METRICS_SCAN_LIMIT
            || verdicts.len() as u64 == JOB_METRICS_SCAN_LIMIT
    );
    report
}

fn fold(admitted: &[Event], verdicts: &[Event]) -> JobMetrics {
    let mut actions: BTreeMap<(String, String), &'static str> = BTreeMap::new();
    let mut seen_runs = BTreeSet::new();
    for event in admitted {
        if event.kind != EventKind::InputAdmitted || !seen_runs.insert(event.session.0.to_string())
        {
            continue;
        }
        let Some(marker) = event
            .bytes
            .get(status::JOB_KEY)
            .and_then(|value| status::parse_marker(value).ok())
        else {
            continue;
        };
        for step in marker.plan.steps {
            let action = match step.action {
                StepAction::Agent => Some("agent"),
                StepAction::Deliver => Some("deliver"),
                StepAction::Publish => None,
            };
            if let Some(action) = action {
                actions.insert((event.session.0.to_string(), step.id), action);
            }
        }
    }

    let mut metrics = JobMetrics::empty();
    let mut seen_run_verdicts = BTreeSet::new();
    let mut seen_step_verdicts = BTreeSet::new();
    for event in verdicts {
        if event.kind != EventKind::EvidenceVerdict {
            continue;
        }
        let Some(gate) = event.bytes.get("gate").and_then(Value::as_str) else {
            continue;
        };
        let Some(job) = event.bytes.get(status::JOB_KEY) else {
            continue;
        };
        if gate == status::RUN_GATE_NAME {
            let Some(outcome) = job
                .get("outcome")
                .and_then(Value::as_str)
                .and_then(RunOutcome::parse)
            else {
                continue;
            };
            let key = job
                .get("run")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| event.session.0.to_string());
            if !seen_run_verdicts.insert(key) {
                continue;
            }
            *metrics.runs.entry(outcome.as_str()).or_insert(0) += 1;
            continue;
        }
        let Some(step) = gate.strip_prefix("step:") else {
            continue;
        };
        let Some(attempt) = job.get("attempt").and_then(Value::as_u64) else {
            continue;
        };
        let run = job
            .get("run")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| event.session.0.to_string());
        let key = (run, step.to_string(), attempt);
        if !seen_step_verdicts.insert(key) {
            continue;
        }
        let Some(action) = actions
            .get(&(event.session.0.to_string(), step.to_string()))
            .copied()
        else {
            continue;
        };
        let Some(outcome) = job
            .get("outcome")
            .and_then(Value::as_str)
            .and_then(AttemptOutcome::parse)
        else {
            continue;
        };
        *metrics
            .steps
            .entry(action)
            .or_default()
            .entry(outcome.as_str())
            .or_insert(0) += 1;
    }
    metrics
}

#[cfg(test)]
mod tests {
    use super::*;
    use kian_core::events::SCHEMA_VERSION;
    use kian_core::model::{Epoch, SessionId};
    use kian_superagent::job::plan::JobPlan;
    use serde_json::json;
    use uuid::Uuid;

    fn event(session: SessionId, seq: u64, kind: EventKind, bytes: Value) -> Event {
        Event {
            seq,
            session,
            epoch: Epoch(1),
            ts: 1_761_900_000_000 + seq,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        }
    }

    fn plan() -> JobPlan {
        serde_json::from_value(json!({
            "id": "digest",
            "steps": [
                {
                    "id": "fetch",
                    "prompt": "fetch",
                    "verify": [{"check": "final_message_present"}]
                },
                {
                    "id": "send",
                    "action": "deliver",
                    "channel": "mock",
                    "target": {"handle": "owner"},
                    "content": {"text": "done"}
                }
            ]
        }))
        .expect("plan")
    }

    fn verdict(session: SessionId, seq: u64, gate: &str, job: Value) -> Event {
        event(
            session,
            seq,
            EventKind::EvidenceVerdict,
            json!({
                "gate": gate,
                "passed": false,
                "checks": [],
                "attempt": 0,
                "job": job,
            }),
        )
    }

    #[test]
    fn committed_verdicts_are_counted_once_first_wins() {
        let session = SessionId(Uuid::from_u128(1));
        let marker = status::marker_payload("digest", "slot", "manual", &plan());
        let admitted = vec![event(
            session,
            1,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "run", "job": marker}),
        )];
        let verdicts = vec![
            verdict(
                session,
                2,
                "step:fetch",
                json!({"run": "digest@slot", "step": "fetch", "attempt": 1, "outcome": "failed"}),
            ),
            verdict(
                session,
                3,
                "step:fetch",
                json!({"run": "digest@slot", "step": "fetch", "attempt": 1, "outcome": "succeeded"}),
            ),
            verdict(
                session,
                4,
                "step:send",
                json!({"run": "digest@slot", "step": "send", "attempt": 1, "outcome": "succeeded"}),
            ),
            verdict(
                session,
                5,
                "run",
                json!({"run": "digest@slot", "outcome": "failed"}),
            ),
            verdict(
                session,
                6,
                "run",
                json!({"run": "digest@slot", "outcome": "succeeded"}),
            ),
        ];
        let metrics = fold(&admitted, &verdicts);
        assert_eq!(metrics.runs["failed"], 1);
        assert_eq!(metrics.runs["succeeded"], 0);
        assert_eq!(metrics.steps["agent"]["failed"], 1);
        assert_eq!(metrics.steps["agent"]["succeeded"], 0);
        assert_eq!(metrics.steps["deliver"]["succeeded"], 1);
    }

    #[test]
    fn an_empty_projection_exports_defined_zeroes() {
        let value = JobMetrics::empty().json();
        assert_eq!(value["job_runs_total"]["succeeded"], 0);
        assert_eq!(value["job_runs_total"]["cancelled"], 0);
        assert_eq!(value["job_step_attempts_total"]["agent"]["failed"], 0);
        assert_eq!(value["job_step_attempts_total"]["deliver"]["timed_out"], 0);
    }
}
