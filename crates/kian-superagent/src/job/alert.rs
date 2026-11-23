//! Alert runs (JOB-MODEL §13.3): a failing run admits **one**
//! deliver-only run of a synthetic alert job.
//!
//! An alert is not a second mechanism: it is the same four axes with a
//! fixed plan — one `deliver` step whose only evidence is the channel
//! receipt. The idempotency key `kian:alert:<failed run id>` is what makes
//! a crash-looping conductor admit one alert per failed run instead of a
//! storm (§13.3), and an alert job can never declare `on_failure_of`
//! itself (V20), so alerts cannot chain.
//!
//! Pure: no I/O, no clock. The conductor composes an [`AlertAdmission`] and
//! hands it to its [`AlertPort`](crate::job::conduct::AlertPort).

use kian_core::model::SessionId;
use serde_json::{Value, json};

use crate::job::outcome::RunOutcome;
use crate::job::plan::{
    DEFAULT_STEP_TIMEOUT_MS, DeliverContent, Effect, JobCheck, JobLimits, JobPlan, OnFailure,
    StepAction, StepPlan, alert_run_id, alert_slot, run_session,
};
use crate::job::status::marker_payload;

/// The trigger token of an alert run (§3).
pub const TRIGGER_ALERT: &str = "alert";

/// The prefix every alert body carries, so an operator can route on it.
pub const ALERT_PREFIX: &str = "[kian-alert]";

/// One declared alert, as the conductor needs it: where to send, nothing
/// about how (§13.3).
#[derive(Debug, Clone, PartialEq)]
pub struct AlertSpec {
    /// The declared alert id; the synthetic job is `alert:<id>`.
    pub id: String,
    /// The delivery channel (already validated against the registry, V16).
    pub channel: String,
    /// The adapter-specific target, frozen with the plan.
    pub target: Value,
}

/// What an alert tells the operator (§13.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlertFacts<'a> {
    /// The failed run id.
    pub failed_run: &'a str,
    /// The run outcome — `failed` or `unverified` (§13.3); a cancelled run
    /// is deliberate and never alerts.
    pub outcome: RunOutcome,
    /// The step that failed, when one did.
    pub failed_step: Option<&'a str>,
    /// The reason, when one is known.
    pub detail: Option<&'a str>,
}

/// The synthetic job id of one alert: `alert:<id>`.
#[must_use]
pub fn alert_job_id(alert_id: &str) -> String {
    format!("alert:{alert_id}")
}

/// The alert body. Deterministic: the same facts produce the same bytes, so
/// a re-admitted alert (same idempotency key) is the same message.
#[must_use]
pub fn alert_content(facts: &AlertFacts<'_>) -> String {
    let mut lines = vec![format!(
        "{ALERT_PREFIX} run {} ended {}.",
        facts.failed_run,
        facts.outcome.as_str()
    )];
    if let Some(step) = facts.failed_step {
        lines.push(format!("step: {step}"));
    }
    if let Some(detail) = facts.detail {
        lines.push(format!("reason: {detail}"));
    }
    lines.push(format!("inspect: kian job status {}", facts.failed_run));
    lines.join("\n")
}

/// The synthetic deliver-only plan of one alert (§13.3): one `deliver` step
/// named `notify`, whose only evidence is the delivery receipt.
#[must_use]
pub fn alert_plan(spec: &AlertSpec, facts: &AlertFacts<'_>) -> JobPlan {
    let step = StepPlan {
        id: "notify".to_string(),
        action: StepAction::Deliver,
        prompt: None,
        requires: Vec::new(),
        pin: None,
        allow_offline: false,
        prefer: Vec::new(),
        depends_on: Vec::new(),
        inputs: Vec::new(),
        produces: None,
        output_schema: None,
        timeout_ms: DEFAULT_STEP_TIMEOUT_MS,
        retries: 0,
        effect: Effect::Write,
        idempotent: false,
        verify: vec![JobCheck::DeliveryReceipt {
            channel: spec.channel.clone(),
            target: spec.target.clone(),
        }],
        on_failure: OnFailure::Halt,
        optional: false,
        channel: Some(spec.channel.clone()),
        target: Some(spec.target.clone()),
        content: Some(DeliverContent {
            text: Some(alert_content(facts)),
            from_artifact: None,
        }),
    };
    JobPlan {
        id: alert_job_id(&spec.id),
        limits: JobLimits::default(),
        steps: vec![step],
    }
}

/// Everything needed to admit the alert run (§13.3): its session, its first
/// input payload (content + frozen plan under the `job` key), its
/// idempotency key and its run id.
#[derive(Debug, Clone, PartialEq)]
pub struct AlertAdmission {
    /// The alert run's session: `uuid5(job_ns("alert:<id>"), "run:alert:<failed>")`.
    pub session: SessionId,
    /// `alert:<id>@alert:<failed run id>`.
    pub run_id: String,
    /// `kian:alert:<failed run id>` — one alert per failed run, forever.
    pub key: String,
    /// The input row: `{"role","content","job":<frozen marker>}`.
    pub payload: Value,
}

/// Compose the alert admission for `spec` after `facts` (§13.3).
#[must_use]
pub fn admission(spec: &AlertSpec, facts: &AlertFacts<'_>) -> AlertAdmission {
    let job = alert_job_id(&spec.id);
    let slot = alert_slot(facts.failed_run);
    let plan = alert_plan(spec, facts);
    let marker = marker_payload(&job, &slot, TRIGGER_ALERT, &plan);
    let payload = json!({
        "content": format!(
            "Run alert job \"{job}\" for the failed run {}.",
            facts.failed_run
        ),
        "job": marker,
    });
    AlertAdmission {
        session: run_session(&job, &slot),
        run_id: alert_run_id(&job, facts.failed_run),
        key: format!("kian:alert:{}", facts.failed_run),
        payload,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ALERT_PREFIX, AlertFacts, AlertSpec, admission, alert_content, alert_job_id, alert_plan,
    };
    use crate::job::outcome::RunOutcome;
    use crate::job::plan::{Effect, JobCheck, StepAction};
    use crate::job::status;
    use kian_core::events::{Event, EventKind};
    use kian_core::model::{Epoch, SessionId};
    use serde_json::json;

    fn spec() -> AlertSpec {
        AlertSpec {
            id: "ops".to_string(),
            channel: "mail".to_string(),
            target: json!({"to": "owner@example.com"}),
        }
    }

    fn facts() -> AlertFacts<'static> {
        AlertFacts {
            failed_run: "digest@2026-11-23T09:00:00+08:00",
            outcome: RunOutcome::Failed,
            failed_step: Some("fetch"),
            detail: Some("timed out"),
        }
    }

    fn event(seq: u64, kind: EventKind, bytes: serde_json::Value) -> Event {
        Event {
            seq,
            session: SessionId::new(),
            epoch: Epoch(1),
            ts: 1_700_000_000_000 + seq,
            kind,
            bytes,
            version: 0,
        }
    }

    #[test]
    fn the_job_id_is_prefixed() {
        assert_eq!(alert_job_id("ops"), "alert:ops");
    }

    #[test]
    fn the_body_names_the_run_the_step_the_reason_and_the_command() {
        let text = alert_content(&facts());
        assert!(text.starts_with(ALERT_PREFIX), "{text}");
        assert!(text.contains("digest@2026-11-23T09:00:00+08:00"), "{text}");
        assert!(text.contains("ended failed"), "{text}");
        assert!(text.contains("step: fetch"), "{text}");
        assert!(text.contains("reason: timed out"), "{text}");
        assert!(text.contains("kian job status"), "{text}");
    }

    #[test]
    fn the_plan_is_one_deliver_step_whose_evidence_is_the_receipt() {
        let plan = alert_plan(&spec(), &facts());
        assert_eq!(plan.id, "alert:ops");
        assert_eq!(plan.steps.len(), 1);
        let step = &plan.steps[0];
        assert!(step.is_deliver());
        assert_eq!(step.id, "notify");
        assert_eq!(step.action, StepAction::Deliver);
        assert_eq!(step.channel.as_deref(), Some("mail"));
        assert_eq!(step.effect, Effect::Write);
        assert!(matches!(
            step.verify.as_slice(),
            [JobCheck::DeliveryReceipt { .. }]
        ));
        assert!(crate::job::plan::validate_plan(&plan).is_empty());
    }

    #[test]
    fn the_admission_carries_a_verifying_marker_and_one_key_per_failed_run() {
        let admitted = admission(&spec(), &facts());
        assert_eq!(
            admitted.run_id,
            "alert:ops@alert:digest@2026-11-23T09:00:00+08:00"
        );
        assert_eq!(admitted.key, "kian:alert:digest@2026-11-23T09:00:00+08:00");
        let job = admitted.payload.get("job").expect("the frozen marker");
        assert_eq!(job.get("trigger").and_then(|v| v.as_str()), Some("alert"));
        assert_eq!(
            job.get("job_id").and_then(|v| v.as_str()),
            Some("alert:ops")
        );
        let plan = alert_plan(&spec(), &facts());
        assert_eq!(
            job.get("plan_digest").and_then(|v| v.as_str()),
            Some(plan.digest().as_str())
        );
    }

    #[test]
    fn the_admitted_row_reads_back_through_the_marker_parser() {
        let admitted = admission(&spec(), &facts());
        let events = vec![event(1, EventKind::InputAdmitted, admitted.payload)];
        let marker = status::marker(&events)
            .expect("a marker")
            .expect("well formed");
        assert_eq!(marker.run_id, admitted.run_id);
        assert_eq!(marker.slot, "alert:digest@2026-11-23T09:00:00+08:00");
        assert_eq!(marker.trigger, "alert");
        assert_eq!(marker.plan.id, "alert:ops");
    }

    #[test]
    fn the_same_facts_derive_the_same_session_and_plan() {
        let first = admission(&spec(), &facts());
        let second = admission(&spec(), &facts());
        assert_eq!(first.session, second.session);
        assert_eq!(first.payload, second.payload);
    }
}
