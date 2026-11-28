//! The `jobs:` section (JOB-MODEL §7): declaration, validation and
//! lowering.
//!
//! Three lists, three authorities:
//!
//! - `jobs.jobs` declares a **plan** (a frozen `JobPlan`,
//!   `crates/kian-superagent/src/job/plan.rs`) and its limits. A job
//!   carries no trigger and holds no state.
//! - `jobs.triggers` names a job and a wall-clock expression. Each one
//!   is **lowered at load** into the existing cron runtime vector as a
//!   `CronRuntimeSchedule` whose target is the runtime-only
//!   `CronTarget::Job { job }` (§7.1a): one scheduler, one cursor, one
//!   claim table, no second timer.
//! - `jobs.alerts` names a job and an independent channel. Each entry
//!   becomes a synthetic deliver-only job `alert:<id>` (§13.3), admitted
//!   by the failing conductor, never by a timer.
//!
//! Everything here is load-time: parsing, the config-context half of the
//! V-rules (§7.4), and the frozen value a trigger embeds in its input row
//! (§5.2). The conductor never consults this module's config types; it reads
//! the frozen plan back out of its own stream.

use std::collections::{BTreeMap, BTreeSet};

use kian_superagent::job::plan::{
    JobCheck, JobLimits, JobPlan, StepAction, StepPlan, run_id as job_run_id, run_input_key,
    validate_plan,
};
use kian_superagent::job::status::marker_payload;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::{
    ChannelConfig, ConfigError, CronCatchUpDef, CronOverlap, CronRuntimeSchedule, CronTarget,
    DEFAULT_CRON_COALESCE_MS, DEFAULT_CRON_MAX_CONCURRENT, DEFAULT_CRON_TIMEOUT_MS,
};

/// The longest job or trigger id (§7.4 V1: the worker-capability shape).
pub const JOB_ID_MAX_LEN: usize = 64;

/// The trigger kind recorded in a run's marker (§6) for a wall-clock firing.
pub const TRIGGER_CRON: &str = "cron";
/// A run admitted by `kian job run`.
pub const TRIGGER_MANUAL: &str = "manual";
/// A run admitted by a failing conductor (§13.3).
pub const TRIGGER_ALERT: &str = "alert";

/// The `jobs:` section (§7.1).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobsSection {
    /// Declared plans, in declaration order.
    #[serde(default)]
    pub jobs: Vec<JobDef>,
    /// Wall-clock triggers, lowered into the cron runtime vector.
    #[serde(default)]
    pub triggers: Vec<JobTriggerDef>,
    /// Failure alerts, one synthetic deliver-only job each.
    #[serde(default)]
    pub alerts: Vec<JobAlertDef>,
}

impl JobsSection {
    /// Whether the section declares nothing at all (feature-off: no catalog
    /// is mounted and no cron schedule is lowered).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty() && self.triggers.is_empty() && self.alerts.is_empty()
    }
}

/// One declared job (§7.1): a plan plus its limits, and nothing else.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobDef {
    /// The job id (§7.4 V1).
    pub id: String,
    /// Run-wide limits and defaults (§7.1).
    #[serde(default)]
    pub limits: JobLimits,
    /// The steps, in declaration order (the dispatch order, §8.2).
    pub plan: Vec<StepPlan>,
}

/// One wall-clock trigger (§7.1a).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobTriggerDef {
    /// The schedule id — the durable cursor key. Defaults to the job id, so
    /// the one-trigger case needs no extra key; a second trigger for the
    /// same job must name itself (§7.4 V2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// The job this trigger fires.
    pub job: String,
    /// The cron expression (§7.4 V13: the cron contract's grammar).
    pub expr: String,
    /// The IANA timezone; absent = the host timezone frozen at load.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tz: Option<String>,
    /// Whether the trigger is armed; absent = true.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Misfire policy; the cron contract's values only (§7.4 V13).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catch_up: Option<CronCatchUpDef>,
    /// Overlap policy; v1 accepts `skip` only (§7.4 V15).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlap: Option<CronOverlap>,
    /// Lateness honored by `catch_up: latest`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coalesce_ms: Option<u64>,
    /// Firings of this trigger allowed in flight at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent: Option<u32>,
}

/// One failure alert (§13.3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobAlertDef {
    /// The alert id; the synthetic job is `alert:<id>`.
    pub id: String,
    /// The job whose failure triggers this alert (§7.4 V12/V20).
    pub on_failure_of: String,
    /// The delivery channel; must differ from every deliver channel of the
    /// alerted job (§7.4 V12).
    pub channel: String,
    /// The adapter-specific target, exactly as the adapter receives it.
    #[serde(default)]
    pub target: Value,
}

/// One resolved job: the frozen plan, its digest, and the trigger ids that
/// fire it (§7.3).
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedJob {
    /// The job id.
    pub id: String,
    /// The plan, exactly as declared (frozen per run at admit, §5.2).
    pub plan: JobPlan,
    /// `sha256:` + hex of the plan's canonical bytes (§6).
    pub digest: String,
    /// The schedule ids of this job's triggers, in declaration order.
    pub triggers: Vec<String>,
}

/// One resolved alert (§13.3): a synthetic deliver-only job.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedAlert {
    /// The alert id; the synthetic job is `alert:<id>`.
    pub id: String,
    /// The job whose failure admits this alert.
    pub on_failure_of: String,
    /// The delivery channel.
    pub channel: String,
    /// The adapter-specific target.
    pub target: Value,
}

impl ResolvedAlert {
    /// The synthetic job id a failing run of `on_failure_of` admits
    /// (§13.3).
    #[must_use]
    pub fn job_id(&self) -> String {
        format!("alert:{}", self.id)
    }
}

/// One run admission: everything a trigger needs to write the run's first
/// input row (§5.2) without consulting the config again.
#[derive(Debug, Clone, PartialEq)]
pub struct JobAdmission {
    /// `{job_id}@{slot_token}` (§3, §6).
    pub run_id: String,
    /// The run's session, derived from the job id and the slot (§6).
    pub session: kian_core::model::SessionId,
    /// The slot token this run occupies.
    pub slot: String,
    /// `cron` | `manual` | `alert`.
    pub trigger: String,
    /// The inbox idempotency key (`kian:job:run:{run_id}`).
    pub key: String,
    /// The human-readable content line. Never authority (§5.2).
    pub content: String,
    /// The additive `job` payload: the frozen plan and its digest.
    pub marker: Value,
}

/// The jobs a server can admit and the alerts it can fire (§7.1).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct JobCatalog {
    jobs: BTreeMap<String, ResolvedJob>,
    alerts: BTreeMap<String, Vec<ResolvedAlert>>,
}

impl JobCatalog {
    /// Build a catalog from already-resolved parts.
    #[must_use]
    pub fn new(
        jobs: BTreeMap<String, ResolvedJob>,
        alerts: BTreeMap<String, Vec<ResolvedAlert>>,
    ) -> Self {
        Self { jobs, alerts }
    }

    /// Whether the catalog declares no job (feature-off).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    /// How many jobs are declared.
    #[must_use]
    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    /// The job with this id.
    #[must_use]
    pub fn job(&self, id: &str) -> Option<&ResolvedJob> {
        self.jobs.get(id)
    }

    /// Every job, in id order.
    pub fn iter(&self) -> impl Iterator<Item = &ResolvedJob> {
        self.jobs.values()
    }

    /// The alerts armed for a job, in declaration order.
    #[must_use]
    pub fn alerts_for(&self, job: &str) -> &[ResolvedAlert] {
        self.alerts.get(job).map_or(&[], Vec::as_slice)
    }

    /// Every alert, flattened, in declaration order per job.
    pub fn alerts(&self) -> impl Iterator<Item = &ResolvedAlert> {
        self.alerts.values().flatten()
    }

    /// Everything a trigger needs to admit one run of `job` in
    /// `slot` (§5.2).
    ///
    /// `None` when the job is not declared: a scheduler holding a
    /// stale in-memory catalog must refuse the firing, not invent a plan.
    #[must_use]
    pub fn admission(&self, job: &str, slot: &str, trigger: &str) -> Option<JobAdmission> {
        let resolved = self.jobs.get(job)?;
        let run_id = job_run_id(job, slot);
        Some(JobAdmission {
            session: run_session_for(job, slot),
            key: run_input_key(&run_id),
            content: admission_content(job, slot, trigger),
            marker: marker_payload(job, slot, trigger, &resolved.plan),
            run_id,
            slot: slot.to_string(),
            trigger: trigger.to_string(),
        })
    }

    /// The synthetic deliver-only plan for one alert (§13.3).
    #[must_use]
    pub fn alert_plan(&self, alert: &ResolvedAlert, facts: &AlertFacts<'_>) -> JobPlan {
        alert_plan(alert, facts)
    }
}

/// The run session of `job` in `slot` (§6).
fn run_session_for(job: &str, slot: &str) -> kian_core::model::SessionId {
    kian_superagent::job::plan::run_session(job, slot)
}

/// The run's content line (§5.2). Informational only: the conductor derives
/// everything from the `job` marker, never from this text.
#[must_use]
pub fn admission_content(job: &str, slot: &str, trigger: &str) -> String {
    format!("Run job \"{job}\" for slot {slot} ({trigger}).")
}

/// What an alert tells the operator (§13.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AlertFacts<'a> {
    /// The failed run id.
    pub failed_run: &'a str,
    /// The run outcome (`failed` | `unverified` | `cancelled`).
    pub outcome: &'a str,
    /// The step that failed, when one did.
    pub failed_step: Option<&'a str>,
    /// The reason, when one is known.
    pub detail: Option<&'a str>,
}

/// The alert body. Deterministic: the same facts produce the same bytes, so
/// a re-admitted alert (same idempotency key) is the same message.
#[must_use]
pub fn alert_content(facts: &AlertFacts<'_>) -> String {
    let mut lines = vec![format!(
        "[kian-alert] run {} ended {}.",
        facts.failed_run, facts.outcome
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

/// The synthetic deliver-only plan of one alert (§13.3): one `deliver`
/// step named `notify`, whose only evidence is the delivery receipt.
#[must_use]
pub fn alert_plan(alert: &ResolvedAlert, facts: &AlertFacts<'_>) -> JobPlan {
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
        timeout_ms: kian_superagent::job::plan::DEFAULT_STEP_TIMEOUT_MS,
        retries: 0,
        effect: kian_superagent::job::plan::Effect::Write,
        idempotent: false,
        verify: vec![JobCheck::DeliveryReceipt {
            channel: alert.channel.clone(),
            target: alert.target.clone(),
        }],
        on_failure: kian_superagent::job::plan::OnFailure::Halt,
        optional: false,
        channel: Some(alert.channel.clone()),
        target: Some(alert.target.clone()),
        content: Some(kian_superagent::job::plan::DeliverContent {
            text: Some(alert_content(facts)),
            from_artifact: None,
        }),
        publish: None,
    };
    let plan = JobPlan {
        id: alert.job_id(),
        limits: JobLimits::default(),
        steps: vec![step],
    };
    debug_assert!(
        validate_plan(&plan).is_empty(),
        "the synthetic alert plan must satisfy the plan rules: {:?}",
        validate_plan(&plan)
    );
    plan
}

/// A resolved `jobs:` section: the cron schedules its triggers lowered
/// into, plus the catalog the dispatcher and the manual CLI admit from.
#[derive(Debug, Clone, Default)]
pub struct ResolvedJobs {
    /// The lowered triggers, in declaration order. Merged into the cron
    /// runtime vector by the assembly (one scheduler, §7.1a).
    pub schedules: Vec<CronRuntimeSchedule>,
    /// The plans and alerts a run can be admitted from.
    pub catalog: JobCatalog,
}

impl ResolvedJobs {
    /// An empty result: no jobs, no lowered schedules (feature-off).
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Whether the section resolved to nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.schedules.is_empty() && self.catalog.is_empty()
    }
}

/// Is `id` a legal schedule id, i.e. can it key a durable cursor
/// (§7.4 V1/V2: the lowered trigger id must be a cron schedule id)?
fn is_cron_shaped(id: &str) -> bool {
    let mut chars = id.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    // Mirrors crate::config::is_valid_cron_id: the trigger id keys the
    // durable cursor, so it must satisfy the cron schedule-id grammar.
    (first.is_ascii_lowercase() || first.is_ascii_digit())
        && id.len() <= 64
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

/// Is `target` a shape a channel adapter accepts (§13.2: an object or a
/// string, never null/empty)?
fn is_deliver_target(target: &Value) -> bool {
    match target {
        Value::Object(map) => !map.is_empty(),
        Value::String(text) => !text.trim().is_empty(),
        _ => false,
    }
}

/// §7.4 V18: does any prompt or schema carry a secret-shaped literal?
///
/// Uses the same redactor the runtime uses for exec output
/// (`kian_core::secrets::SecretRedactor::patterns_only`), so "looks like a
/// credential" means exactly what it means everywhere else. Returns the
/// human-readable location of the first hit.
fn secret_shaped(plan: &JobPlan) -> Option<String> {
    let redactor = kian_core::secrets::SecretRedactor::patterns_only();
    for step in &plan.steps {
        if let Some(prompt) = step.prompt.as_deref()
            && redactor.redact(prompt) != prompt
        {
            return Some(format!("step `{}` prompt", step.id));
        }
        if let Some(schema) = step.output_schema.as_ref() {
            let text = serde_json::to_string(schema).unwrap_or_default();
            if redactor.redact(&text) != text {
                return Some(format!("step `{}` output_schema", step.id));
            }
        }
    }
    None
}

impl JobsSection {
    /// Resolves the section: validates every declaration and lowers each
    /// trigger into the cron runtime vector (§7.1a), fail-closed on every
    /// degenerate field.
    ///
    /// `taken` is the set of schedule ids already claimed by
    /// `superagent.cron.schedules`, so a trigger can never shadow an
    /// operator's cron schedule (§7.4 V2). `channels` is the declared
    /// channel registry: a deliver step or an alert naming an undeclared
    /// channel refuses at load (§7.4 V12/V16).
    ///
    /// All declaration errors are collected and reported together, each
    /// naming its path and id, exactly like
    /// [`CronSection::to_runtime`](crate::config::CronSection::to_runtime).
    pub fn resolve(
        &self,
        channels: &BTreeMap<String, ChannelConfig>,
        taken: &BTreeSet<String>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<ResolvedJobs, ConfigError> {
        let (host_tz, host_name) = crate::config::host_timezone();
        let mut errors: Vec<String> = Vec::new();

        // ---- jobs (§7.2, plan-local rules + the config half) ----
        let mut jobs: BTreeMap<String, ResolvedJob> = BTreeMap::new();
        for (i, def) in self.jobs.iter().enumerate() {
            let mut messages: Vec<String> = Vec::new();
            if !kian_core::worker::valid_capability_name(&def.id) {
                messages.push(
                    "id must match [a-z0-9][a-z0-9._-]{0,63} (V1: the capability-name shape)"
                        .to_string(),
                );
            } else if jobs.contains_key(&def.id) {
                messages.push("duplicate job id (V2: a job id is unique)".to_string());
            }
            let plan = JobPlan {
                id: def.id.clone(),
                limits: def.limits.clone(),
                steps: def.plan.clone(),
            };
            messages.extend(validate_plan(&plan).into_iter().map(|v| v.to_string()));
            for step in &plan.steps {
                let Some(spec) = step.deliver() else {
                    continue;
                };
                if !channels.contains_key(spec.channel) {
                    messages.push(format!(
                        "step `{}`: channel `{}` is not declared in superagent.channels (V16)",
                        step.id, spec.channel
                    ));
                }
                if !is_deliver_target(spec.target) {
                    messages.push(format!(
                        "step `{}`: target must be a non-empty object or string (V16)",
                        step.id
                    ));
                }
            }
            if let Some(where_) = secret_shaped(&plan) {
                messages.push(format!(
                    "{where_} carries a secret-shaped literal (V18): a plan is config, and                      config never holds credentials"
                ));
            }
            let before = errors.len();
            errors.extend(
                messages
                    .into_iter()
                    .map(|m| format!("superagent.jobs.jobs[{i}] `{}`: {m}", def.id)),
            );
            if errors.len() == before {
                jobs.insert(
                    def.id.clone(),
                    ResolvedJob {
                        id: def.id.clone(),
                        digest: plan.digest(),
                        plan,
                        triggers: Vec::new(),
                    },
                );
            }
        }

        // ---- triggers (§7.1a: lowered, never a second scheduler) ----
        let mut schedules: Vec<CronRuntimeSchedule> = Vec::new();
        let mut seen: BTreeSet<String> = taken.clone();
        for (i, def) in self.triggers.iter().enumerate() {
            let mut messages: Vec<String> = Vec::new();
            if !jobs.contains_key(&def.job) {
                messages.push(format!(
                    "job `{}` is not declared in superagent.jobs.jobs (V13)",
                    def.job
                ));
            }
            let schedule_id = def.id.clone().unwrap_or_else(|| def.job.clone());
            if !is_cron_shaped(&schedule_id) {
                messages.push(format!(
                    "id `{schedule_id}` must match [a-z0-9][a-z0-9_-]{{0,63}} and key the durable cursor (V1) — set an explicit `id:` when the job id itself is not schedule-shaped"
                ));
            } else if !seen.insert(schedule_id.clone()) {
                messages.push(format!(
                    "duplicate schedule id `{schedule_id}` (V2: a trigger id is unique                      across superagent.jobs.triggers and superagent.cron.schedules)"
                ));
            } else if channels.contains_key(&schedule_id) {
                messages.push(format!(
                    "id `{schedule_id}` collides with a declared channel name (V2)"
                ));
            }
            let (tz, tz_name) = match def.tz.as_deref() {
                None => (host_tz, host_name.clone()),
                Some(raw) => {
                    let name = raw.trim();
                    if name.is_empty() {
                        messages.push(
                            "tz must be non-empty when set (omit the key to freeze the host                              timezone)"
                                .to_string(),
                        );
                        (host_tz, host_name.clone())
                    } else {
                        match kian_cron::parse_timezone(name) {
                            Ok(tz) => (tz, name.to_string()),
                            Err(e) => {
                                messages.push(format!("tz `{name}` does not resolve: {e}"));
                                (host_tz, host_name.clone())
                            }
                        }
                    }
                }
            };
            let expr = match kian_cron::CronExpr::parse(&def.expr) {
                Ok(expr) => match expr.next_fire_after(&tz, now) {
                    Ok(_) => Some(expr),
                    Err(e) => {
                        messages.push(format!(
                            "expr `{}` yields no fire time within the {} day horizon: {e}",
                            def.expr.trim(),
                            kian_cron::MAX_SCAN_DAYS
                        ));
                        None
                    }
                },
                Err(e) => {
                    messages.push(format!("expr `{}` is invalid: {e}", def.expr.trim()));
                    None
                }
            };
            let overlap = def.overlap.unwrap_or_default();
            if overlap != CronOverlap::Skip {
                messages.push(
                    "overlap: only `skip` is implemented in v1 (V15: one run per slot)".to_string(),
                );
            }
            let catch_up = match def.catch_up.unwrap_or_default() {
                CronCatchUpDef::Latest => kian_cron::CatchUp::Latest,
                CronCatchUpDef::Skip => kian_cron::CatchUp::Skip,
            };
            let coalesce_ms = def.coalesce_ms.unwrap_or(DEFAULT_CRON_COALESCE_MS);
            if coalesce_ms == 0 {
                messages.push("coalesce_ms must be > 0".to_string());
            }
            let max_concurrent = def.max_concurrent.unwrap_or(DEFAULT_CRON_MAX_CONCURRENT);
            if max_concurrent == 0 {
                messages.push("max_concurrent must be >= 1".to_string());
            }
            let before = errors.len();
            errors.extend(
                messages
                    .into_iter()
                    .map(|m| format!("superagent.jobs.triggers[{i}] `{}`: {m}", def.job)),
            );
            if errors.len() == before
                && let Some(expr) = expr
            {
                schedules.push(CronRuntimeSchedule {
                    id: schedule_id.clone(),
                    expr,
                    tz,
                    tz_name,
                    enabled: def.enabled.unwrap_or(true),
                    target: CronTarget::Job {
                        job: def.job.clone(),
                    },
                    prompt: admission_content(&def.job, "<slot>", TRIGGER_CRON),
                    overlap,
                    catch_up,
                    coalesce_ms,
                    max_concurrent,
                    timeout_ms: DEFAULT_CRON_TIMEOUT_MS,
                    worktree: None,
                    defer: crate::defer::DeferPolicy::Immediate,
                });
                if let Some(job) = jobs.get_mut(&def.job) {
                    job.triggers.push(schedule_id);
                }
            }
        }

        // ---- alerts (§13.3: an independent channel, never a chain) ----
        let declared: BTreeSet<&str> = self.jobs.iter().map(|j| j.id.as_str()).collect();
        let mut alerts: BTreeMap<String, Vec<ResolvedAlert>> = BTreeMap::new();
        for (i, def) in self.alerts.iter().enumerate() {
            let mut messages: Vec<String> = Vec::new();
            if !kian_core::worker::valid_capability_name(&def.id) {
                messages.push("id must match [a-z0-9][a-z0-9._-]{0,63} (V1)".to_string());
            }
            if !declared.contains(def.on_failure_of.as_str()) {
                messages.push(format!(
                    "on_failure_of `{}` is not declared in superagent.jobs.jobs (V20)",
                    def.on_failure_of
                ));
            }
            if !channels.contains_key(&def.channel) {
                messages.push(format!(
                    "channel `{}` is not declared in superagent.channels (V16)",
                    def.channel
                ));
            }
            if !is_deliver_target(&def.target) {
                messages.push("target must be a non-empty object or string (V16)".to_string());
            }
            if let Some(job) = jobs.get(&def.on_failure_of) {
                let collides = job
                    .plan
                    .steps
                    .iter()
                    .filter_map(|s| s.channel.as_deref())
                    .any(|c| c == def.channel);
                if collides {
                    messages.push(format!(
                        "channel `{}` is also a deliver channel of job `{}` (V12: an alert must not share the delivery path whose failure it reports)",
                        def.channel, def.on_failure_of
                    ));
                }
            }
            let before = errors.len();
            errors.extend(
                messages
                    .into_iter()
                    .map(|m| format!("superagent.jobs.alerts[{i}] `{}`: {m}", def.id)),
            );
            if errors.len() == before {
                alerts
                    .entry(def.on_failure_of.clone())
                    .or_default()
                    .push(ResolvedAlert {
                        id: def.id.clone(),
                        on_failure_of: def.on_failure_of.clone(),
                        channel: def.channel.clone(),
                        target: def.target.clone(),
                    });
            }
        }

        if !errors.is_empty() {
            return Err(ConfigError::Invalid(errors.join("; ")));
        }
        Ok(ResolvedJobs {
            schedules,
            catalog: JobCatalog::new(jobs, alerts),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn channels(names: &[&str]) -> BTreeMap<String, ChannelConfig> {
        let mut map = BTreeMap::new();
        for name in names {
            let cfg: ChannelConfig =
                serde_yaml::from_str("type: timer\ntimer:\n  interval_ms: 60000\n")
                    .expect("the test channel parses");
            map.insert((*name).to_string(), cfg);
        }
        map
    }

    fn resolve_with(
        yaml: &str,
        channels: &BTreeMap<String, ChannelConfig>,
    ) -> Result<ResolvedJobs, ConfigError> {
        let section: JobsSection = serde_yaml::from_str(yaml).expect("the jobs fixture parses");
        section.resolve(channels, &BTreeSet::new(), Utc::now())
    }

    fn resolve(yaml: &str) -> Result<ResolvedJobs, ConfigError> {
        resolve_with(yaml, &channels(&["imessage", "mail"]))
    }

    fn error_text(result: Result<ResolvedJobs, ConfigError>) -> String {
        match result {
            Ok(_) => panic!("expected a config error"),
            Err(ConfigError::Invalid(text)) => text,
            Err(other) => panic!("expected ConfigError::Invalid, got {other:?}"),
        }
    }

    const DIGEST: &str = r#"
jobs:
  - id: digest
    plan:
      - id: fetch
        prompt: gather the news
        requires: [net.unrestricted]
        verify:
          - check: min_tool_results
            n: 1
      - id: notify
        action: deliver
        channel: imessage
        target: { handle: owner }
        content: { text: done }
        depends_on: [fetch]
        verify:
          - check: delivery_receipt
            channel: imessage
triggers:
  - job: digest
    expr: "0 9 * * *"
    tz: Asia/Shanghai
"#;

    #[test]
    fn lowering_produces_one_job_targeted_schedule() {
        let resolved = resolve(DIGEST).expect("the digest job resolves");
        assert_eq!(resolved.schedules.len(), 1);
        let schedule = &resolved.schedules[0];
        assert_eq!(schedule.id, "digest");
        assert_eq!(
            schedule.target,
            CronTarget::Job {
                job: "digest".to_string()
            }
        );
        assert!(!schedule.prompt.trim().is_empty());
        assert_eq!(schedule.tz_name, "Asia/Shanghai");
        let job = resolved.catalog.job("digest").expect("the job resolved");
        assert_eq!(job.triggers, vec!["digest".to_string()]);
        assert_eq!(job.digest, job.plan.digest());
        assert_eq!(resolved.catalog.len(), 1);
    }

    #[test]
    fn a_second_trigger_for_one_job_needs_its_own_id() {
        let yaml = r#"
jobs:
  - id: digest
    plan:
      - id: fetch
        prompt: gather
        verify:
          - check: final_message_present
triggers:
  - job: digest
    expr: "0 9 * * *"
  - job: digest
    expr: "0 18 * * *"
"#;
        let text = error_text(resolve(yaml));
        assert!(text.contains("duplicate schedule id"), "{text}");
        assert!(text.contains("superagent.jobs.triggers[1]"), "{text}");
    }

    #[test]
    fn an_explicit_trigger_id_lowers_under_its_own_name() {
        let yaml = r#"
jobs:
  - id: digest
    plan:
      - id: fetch
        prompt: gather
        verify:
          - check: final_message_present
triggers:
  - job: digest
    expr: "0 9 * * *"
  - id: digest-evening
    job: digest
    expr: "0 18 * * *"
"#;
        let resolved = resolve(yaml).expect("two triggers resolve");
        assert_eq!(
            resolved
                .schedules
                .iter()
                .map(|s| s.id.as_str())
                .collect::<Vec<_>>(),
            vec!["digest", "digest-evening"]
        );
        let job = resolved.catalog.job("digest").unwrap();
        assert_eq!(job.triggers, vec!["digest", "digest-evening"]);
    }

    #[test]
    fn a_dotted_job_id_needs_an_explicit_trigger_id() {
        let yaml = r#"
jobs:
  - id: digest.v2
    plan:
      - id: fetch
        prompt: gather
        verify:
          - check: final_message_present
triggers:
  - job: digest.v2
    expr: "0 9 * * *"
"#;
        let text = error_text(resolve(yaml));
        assert!(
            text.contains("must match [a-z0-9][a-z0-9_-]{0,63}"),
            "{text}"
        );
        assert!(text.contains("explicit"), "{text}");
    }

    #[test]
    fn a_trigger_id_already_taken_by_cron_is_refused() {
        let yaml = r#"
jobs:
  - id: digest
    plan:
      - id: fetch
        prompt: gather
        verify:
          - check: final_message_present
triggers:
  - job: digest
    expr: "0 9 * * *"
"#;
        let section: JobsSection = serde_yaml::from_str(yaml).expect("parses");
        let mut taken = BTreeSet::new();
        taken.insert("digest".to_string());
        let text = match section.resolve(&channels(&["imessage"]), &taken, Utc::now()) {
            Ok(_) => panic!("expected a conflict"),
            Err(ConfigError::Invalid(text)) => text,
            Err(other) => panic!("{other:?}"),
        };
        assert!(text.contains("duplicate schedule id"), "{text}");
        assert!(text.contains("superagent.cron.schedules"), "{text}");
    }

    #[test]
    fn admission_carries_the_frozen_plan_under_the_job_key() {
        let resolved = resolve(DIGEST).expect("resolves");
        let slot = "2026-11-23T09:00:00+08:00";
        let admission = resolved
            .catalog
            .admission("digest", slot, TRIGGER_CRON)
            .expect("the job admits");
        assert_eq!(admission.run_id, "digest@2026-11-23T09:00:00+08:00");
        assert_eq!(
            admission.key,
            "kian:job:run:digest@2026-11-23T09:00:00+08:00"
        );
        assert!(admission.content.contains(slot));
        assert_eq!(admission.trigger, TRIGGER_CRON);
        let job = resolved.catalog.job("digest").unwrap();
        assert_eq!(
            admission.session,
            kian_superagent::job::plan::run_session("digest", slot)
        );
        // The marker is the frozen plan plus its digest: a later config edit
        // cannot change what this run executes (§5.2, §4 J2).
        let marker = &admission.marker;
        assert_eq!(marker["plan_digest"], serde_json::json!(job.digest));
        assert_eq!(marker["plan"], job.plan.frozen_json());
        assert_eq!(marker["run_id"], serde_json::json!(admission.run_id));
        assert_eq!(marker["slot"], serde_json::json!(slot));
        assert_eq!(marker["trigger"], serde_json::json!(TRIGGER_CRON));
    }

    #[test]
    fn an_undeclared_job_never_admits() {
        let resolved = resolve(DIGEST).expect("resolves");
        assert!(
            resolved
                .catalog
                .admission("nope", "slot", TRIGGER_CRON)
                .is_none()
        );
    }

    #[test]
    fn an_undeclared_deliver_channel_is_refused() {
        let yaml = r#"
jobs:
  - id: digest
    plan:
      - id: notify
        action: deliver
        channel: carrier-pigeon
        target: { handle: owner }
        content: { text: hi }
"#;
        let text = error_text(resolve(yaml));
        assert!(text.contains("carrier-pigeon"), "{text}");
        assert!(text.contains("(V16)"), "{text}");
    }

    #[test]
    fn a_trigger_naming_an_undeclared_job_is_refused() {
        let yaml = r#"
jobs:
  - id: digest
    plan:
      - id: fetch
        prompt: gather
        verify:
          - check: final_message_present
triggers:
  - job: ghost
    expr: "0 9 * * *"
"#;
        let text = error_text(resolve(yaml));
        assert!(text.contains("(V13)"), "{text}");
        assert!(text.contains("ghost"), "{text}");
    }

    #[test]
    fn a_plan_violation_is_reported_with_its_path() {
        let yaml = r#"
jobs:
  - id: digest
    plan:
      - id: publish
        prompt: publish
        depends_on: [missing]
        verify:
          - check: final_message_present
"#;
        let text = error_text(resolve(yaml));
        assert!(text.contains("superagent.jobs.jobs[0]"), "{text}");
        assert!(text.contains("(V4)"), "{text}");
        assert!(text.contains("missing"), "{text}");
    }

    #[test]
    fn a_secret_shaped_prompt_is_refused() {
        let yaml = r#"
jobs:
  - id: leaky
    plan:
      - id: fetch
        prompt: "curl -H X-Auth: ghp_abcdefghijklmnopqrstu"
        verify:
          - check: final_message_present
"#;
        let text = error_text(resolve(yaml));
        assert!(text.contains("secret-shaped literal"), "{text}");
        assert!(text.contains("(V18)"), "{text}");
    }

    #[test]
    fn an_alert_plan_is_one_deliver_step_with_a_receipt_check() {
        let yaml = r#"
jobs:
  - id: digest
    plan:
      - id: fetch
        prompt: gather
        verify:
          - check: final_message_present
alerts:
  - id: ops
    on_failure_of: digest
    channel: mail
    target: { to: owner@example.com }
"#;
        let resolved = resolve(yaml).expect("resolves");
        let alerts = resolved.catalog.alerts_for("digest");
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].job_id(), "alert:ops");
        let facts = AlertFacts {
            failed_run: "digest@2026-11-23T09:00:00+08:00",
            outcome: "failed",
            failed_step: Some("fetch"),
            detail: Some("timed out"),
        };
        let plan = resolved.catalog.alert_plan(&alerts[0], &facts);
        assert_eq!(plan.id, "alert:ops");
        assert_eq!(plan.steps.len(), 1);
        let step = &plan.steps[0];
        assert!(step.is_deliver());
        assert_eq!(step.id, "notify");
        assert_eq!(step.channel.as_deref(), Some("mail"));
        assert_eq!(step.effect, kian_superagent::job::plan::Effect::Write);
        assert!(matches!(
            step.verify.as_slice(),
            [JobCheck::DeliveryReceipt { .. }]
        ));
        let text = step
            .content
            .as_ref()
            .and_then(|c| c.text.clone())
            .expect("the alert carries literal text");
        assert!(text.contains("digest@2026-11-23T09:00:00+08:00"), "{text}");
        assert!(text.contains("kian job status"), "{text}");
    }

    #[test]
    fn an_alert_on_the_delivery_path_it_reports_is_refused() {
        let yaml = r#"
jobs:
  - id: digest
    plan:
      - id: notify
        action: deliver
        channel: imessage
        target: { handle: owner }
        content: { text: hi }
alerts:
  - id: ops
    on_failure_of: digest
    channel: imessage
    target: { handle: owner }
"#;
        let text = error_text(resolve(yaml));
        assert!(text.contains("V12"), "{text}");
    }

    #[test]
    fn an_alert_on_an_undeclared_job_is_refused() {
        let yaml = r#"
jobs:
  - id: digest
    plan:
      - id: fetch
        prompt: gather
        verify:
          - check: final_message_present
alerts:
  - id: ops
    on_failure_of: ghost
    channel: mail
    target: { to: owner@example.com }
"#;
        let text = error_text(resolve(yaml));
        assert!(text.contains("(V20)"), "{text}");
    }

    #[test]
    fn a_manual_only_job_is_valid_and_lowers_no_schedule() {
        let yaml = r#"
jobs:
  - id: digest
    plan:
      - id: fetch
        prompt: gather
        verify:
          - check: final_message_present
"#;
        let resolved = resolve(yaml).expect("a triggerless job resolves (V19)");
        assert!(resolved.schedules.is_empty());
        assert_eq!(resolved.catalog.len(), 1);
        assert!(
            resolved
                .catalog
                .admission("digest", "manual:01J", TRIGGER_MANUAL)
                .is_some()
        );
    }

    #[test]
    fn an_unknown_key_is_refused() {
        let yaml = r#"
jobs:
  - id: digest
    plan: []
    retries: 3
"#;
        assert!(serde_yaml::from_str::<JobsSection>(yaml).is_err());
    }

    #[test]
    fn the_default_section_resolves_to_nothing() {
        let section = JobsSection::default();
        assert!(section.is_empty());
        let resolved = section
            .resolve(&channels(&[]), &BTreeSet::new(), Utc::now())
            .expect("an empty section resolves");
        assert!(resolved.is_empty());
    }
}
