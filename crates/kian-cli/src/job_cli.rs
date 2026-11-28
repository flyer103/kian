//! `kian job list | status | run | cancel` (JOB-MODEL §15.3).
//!
//! The operator surface over runs. A run is an ordinary session, so every
//! read here is the same read the runtime does: rebuild the stream, derive
//! the view. There is no run table to query and no cached status to drift —
//! `status` is a pure projection of committed events (§1, J1).
//!
//! The two writes (`run`, `cancel`) admit ordinary inbox rows. A row is
//! durable before any wake exists, so a command that admits into a session
//! no live drain is serving stays honest: it says so instead of pretending
//! the work started.

use std::path::{Path, PathBuf};

use kian_core::model::InputRow;
use kian_store::EventStore;
use kian_superagent::job::{outcome::RunOutcome, plan, status};
use serde_json::{Value, json};

#[path = "job_status.rs"]
mod job_status;

/// The cancel row's content. The conductor treats a promoted row whose
/// content is this word as a cancel request (§5.4); any other message is an
/// operator note that mutates nothing.
pub const CANCEL_CONTENT: &str = "cancel";

/// The whole-job.selfdev bound (4.2): an over-long value is an admission
/// refusal, never a truncation.
pub const MAX_SELFDEV_BYTES: usize = 4096;

/// The job.selfdev.pack.dir bound (4.2). The directory is a reference, so
/// this is the only place its length is bounded.
pub const MAX_SELFDEV_DIR_BYTES: usize = 1024;

/// One `kian job` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobCommand {
    /// `kian job list [--config <path>] [--json]`
    List {
        /// The serve config.
        config: PathBuf,
        /// Machine-readable output.
        json: bool,
    },
    /// `kian job status <run|job> [--config <path>] [--json]`
    Status {
        /// A `{job}@{slot}` run id, or a declared job id.
        target: String,
        /// The serve config.
        config: PathBuf,
        /// Machine-readable output.
        json: bool,
    },
    /// `kian job run <job> [--pack <dir>] [--base <commit>] [--slot <token>] [--force] [--config <path>] [--json]`
    Run {
        /// The declared job id.
        job: String,
        /// The slot token; defaults to a fresh `manual:<token>`.
        slot: Option<String>,
        /// The evidence pack a self-dev job admits as its symptom (4.1).
        pack: Option<PathBuf>,
        /// The base commit, when the pack was taken outside a git checkout.
        base: Option<String>,
        /// Admit one run even when the consecutive-failure breaker is open
        /// (10.2).
        force: bool,
        /// The serve config.
        config: PathBuf,
        /// Machine-readable output.
        json: bool,
    },
    /// `kian job cancel <run> [--config <path>] [--json]`
    Cancel {
        /// The run id.
        run: String,
        /// The serve config.
        config: PathBuf,
        /// Machine-readable output.
        json: bool,
    },
}

/// A failed invocation: `Usage` is exit code 2, `Refused` is exit code 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobCliError {
    /// The argv did not parse.
    Usage(String),
    /// The operation itself was refused.
    Refused(String),
}

impl JobCliError {
    /// The operator-facing message, without the `kian: ` prefix.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::Usage(m) | Self::Refused(m) => m,
        }
    }

    /// The process exit code this error maps to.
    #[must_use]
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Usage(_) => 2,
            Self::Refused(_) => 1,
        }
    }
}

fn usage(message: impl Into<String>) -> JobCliError {
    JobCliError::Usage(message.into())
}

fn refused(message: impl Into<String>) -> JobCliError {
    JobCliError::Refused(message.into())
}

/// Runs a parsed command on a private runtime (the CLI's one blocking entry).
pub fn run(args: &[String]) -> Result<String, JobCliError> {
    let command = parse_args(args)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| refused(format!("tokio runtime: {error}")))?;
    runtime.block_on(execute(&command))
}

/// `kian job <subcommand> ...` (the argv after the verb group).
pub fn parse_args(args: &[String]) -> Result<JobCommand, JobCliError> {
    let Some((subcommand, rest)) = args.split_first() else {
        return Err(usage(
            "kian job <list|status|run|cancel> ... (see kian --help)",
        ));
    };
    match subcommand.as_str() {
        "list" => list_args(rest),
        "status" => {
            let (target, config, json) = positional_and_flags(rest, "status")?;
            Ok(JobCommand::Status {
                target,
                config,
                json,
            })
        }
        "run" => run_args(rest),
        "cancel" => {
            let (run, config, json) = positional_and_flags(rest, "cancel")?;
            Ok(JobCommand::Cancel { run, config, json })
        }
        other => Err(usage(format!(
            "unknown job subcommand {other} (list|status|run|cancel)"
        ))),
    }
}

fn list_args(rest: &[String]) -> Result<JobCommand, JobCliError> {
    let mut config = default_config();
    let mut json = false;
    let mut iter = rest.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => config = PathBuf::from(next_value(&mut iter, "--config")?),
            "--json" => json = true,
            other => return Err(usage(format!("unknown job list option {other}"))),
        }
    }
    Ok(JobCommand::List { config, json })
}

fn run_args(rest: &[String]) -> Result<JobCommand, JobCliError> {
    let mut job: Option<String> = None;
    let mut slot: Option<String> = None;
    let mut pack: Option<PathBuf> = None;
    let mut base: Option<String> = None;
    let mut force = false;
    let mut config = default_config();
    let mut json = false;
    let mut iter = rest.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--slot" => slot = Some(next_value(&mut iter, "--slot")?.to_string()),
            "--pack" => pack = Some(PathBuf::from(next_value(&mut iter, "--pack")?)),
            "--base" => base = Some(next_value(&mut iter, "--base")?.to_string()),
            "--force" => force = true,
            "--config" => config = PathBuf::from(next_value(&mut iter, "--config")?),
            "--json" => json = true,
            other if other.starts_with("--") => {
                return Err(usage(format!("unknown job run option {other}")));
            }
            other => {
                if job.replace(other.to_string()).is_some() {
                    return Err(usage("job run takes exactly one job id"));
                }
            }
        }
    }
    let Some(job) = job else {
        return Err(usage("job run requires a job id"));
    };
    Ok(JobCommand::Run {
        job,
        slot,
        pack,
        base,
        force,
        config,
        json,
    })
}

fn default_config() -> PathBuf {
    PathBuf::from("kian.yaml")
}

fn next_value<'a>(
    iter: &mut impl Iterator<Item = &'a String>,
    flag: &str,
) -> Result<&'a str, JobCliError> {
    iter.next()
        .map(String::as_str)
        .ok_or_else(|| usage(format!("{flag} requires a value")))
}

fn positional_and_flags(
    rest: &[String],
    verb: &str,
) -> Result<(String, PathBuf, bool), JobCliError> {
    let mut positional: Option<String> = None;
    let mut config = default_config();
    let mut json = false;
    let mut iter = rest.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => config = PathBuf::from(next_value(&mut iter, "--config")?),
            "--json" => json = true,
            other if other.starts_with("--") => {
                return Err(usage(format!("unknown job {verb} option {other}")));
            }
            other => {
                if positional.replace(other.to_string()).is_some() {
                    return Err(usage(format!("job {verb} takes exactly one argument")));
                }
            }
        }
    }
    let Some(positional) = positional else {
        return Err(usage(format!("job {verb} requires an argument")));
    };
    Ok((positional, config, json))
}

/// Executes one command. A usage error (exit 2) is distinct from a refusal
/// (exit 1): the verbs that can refuse before writing anything - an
/// unverified evidence pack, an open breaker - say so with the argv's code.
pub async fn execute(command: &JobCommand) -> Result<String, JobCliError> {
    match command {
        JobCommand::List { config, json } => list(config, *json).await.map_err(refused),
        JobCommand::Status {
            target,
            config,
            json,
        } => status_command(target, config, *json).await.map_err(refused),
        JobCommand::Run {
            job,
            slot,
            pack,
            base,
            force,
            config,
            json,
        } => {
            run_job(
                job,
                slot.as_deref(),
                config,
                *json,
                pack.as_deref(),
                base.as_deref(),
                *force,
            )
            .await
        }
        JobCommand::Cancel { run, config, json } => {
            cancel(run, config, *json).await.map_err(refused)
        }
    }
}

fn load_config(path: &Path) -> Result<crate::config::ServeConfig, String> {
    crate::config::ServeConfig::load(path)
        .map_err(|error| format!("config {}: {error}", path.display()))
}

/// The declared catalogue plus the trigger schedules it lowered to.
///
/// Both come from the same resolution the server arms with, so a run a
/// command admits is byte-identical to a run a firing admits (§7.1).
fn resolved(
    config: &crate::config::ServeConfig,
) -> Result<
    (
        Vec<crate::config::CronRuntimeSchedule>,
        crate::job_config::JobCatalog,
    ),
    String,
> {
    let Some(superagent) = &config.superagent else {
        return Ok((
            Vec::new(),
            crate::job_config::JobCatalog::new(Default::default(), Default::default()),
        ));
    };
    let cron = superagent
        .cron
        .to_runtime(&superagent.channels, chrono::Utc::now())
        .map_err(|error| format!("superagent.cron: {error}"))?;
    let taken: std::collections::BTreeSet<String> =
        cron.iter().map(|schedule| schedule.id.clone()).collect();
    let jobs = superagent
        .jobs
        .resolve(&superagent.channels, &taken, chrono::Utc::now())
        .map_err(|error| format!("superagent.jobs: {error}"))?;
    Ok((cron, jobs.catalog))
}

pub(crate) async fn open_store(
    config: &crate::config::ServeConfig,
    read_only: bool,
) -> Result<EventStore, String> {
    if config.storage.is_postgres() {
        let section = config
            .storage
            .postgres
            .as_ref()
            .ok_or_else(|| "storage: the postgres section is missing".to_string())?;
        let url_env = section.url_env.clone();
        let url = std::env::var(&url_env)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| format!("{url_env} (the postgres URL) is not set"))?;
        let opened = if read_only {
            EventStore::open_postgres_read_only(&url).await
        } else {
            EventStore::open_postgres(&url).await
        };
        opened.map_err(|error| format!("store: {error}"))
    } else if read_only {
        // A read-only open never migrates, so a fresh path fails to open at
        // all; one read-write open applies the migration, then the
        // read-only handle is retried (the cron CLI's rule, same reason).
        match EventStore::open_read_only(&config.storage.sqlite).await {
            Ok(store) => Ok(store),
            Err(first) => match EventStore::open(&config.storage.sqlite).await {
                Ok(migrator) => {
                    migrator.close().await;
                    EventStore::open_read_only(&config.storage.sqlite)
                        .await
                        .map_err(|error| format!("store: {error} (after migrating: {first})"))
                }
                Err(error) => Err(format!("store: {error} (migrating a fresh store: {first})")),
            },
        }
    } else {
        EventStore::open(&config.storage.sqlite)
            .await
            .map_err(|error| format!("store: {error}"))
    }
}

/// A manual run's slot token: the ULID shape the contract names
/// (`manual:<26-char token>`), so a manual slot sorts and parses like any
/// other idempotency-key component and never collides with a cron fire time.
#[must_use]
pub fn manual_slot() -> String {
    let now_ms = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0);
    let low = now_ms & 0x0000_ffff_ffff_ffff;
    let random = uuid::Uuid::new_v4();
    let mut bytes = [0u8; 16];
    bytes[..6].copy_from_slice(&low.to_be_bytes()[2..]);
    bytes[6..].copy_from_slice(&random.as_bytes()[..10]);
    format!("manual:{}", crockford_base32(&bytes))
}

fn crockford_base32(bytes: &[u8; 16]) -> String {
    const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    // Two leading padding bits make 130 = 26 x 5, the ULID layout.
    let mut acc: u32 = 0;
    let mut bits: u32 = 2;
    let mut symbols = String::with_capacity(26);
    for byte in bytes {
        acc = (acc << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            let index = usize::try_from((acc >> bits) & 0x1f).unwrap_or(0);
            symbols.push(char::from(ALPHABET[index]));
        }
        acc &= (1 << bits) - 1;
    }
    symbols
}

/// `{job}@{slot}` into its two halves. A token without `@` is not a run id;
/// a token with more than one is malformed rather than guessed.
#[must_use]
pub fn split_run_id(raw: &str) -> Option<(&str, &str)> {
    let (job, slot) = raw.split_once('@')?;
    if job.is_empty() || slot.is_empty() || slot.contains('@') {
        return None;
    }
    Some((job, slot))
}

/// Why an admitted manual run may not start immediately.
const MANUAL_NOTE: &str = "the row is durable; a serving process starts the \
run when its drain sees it (boot rebuilds the inbox, and a parked conductor \
promotes rows on its next tick) - sqlite has no cross-process wake";

/// Why a cancel request may not land immediately.
const CANCEL_NOTE: &str = "the row is durable; a parked conductor reads \
promoted rows on its next tick and ends the run cancelled - if no drain is \
serving the run, the request waits for the next boot";

async fn list(path: &Path, json_output: bool) -> Result<String, String> {
    let config = load_config(path)?;
    let (_schedules, catalog) = resolved(&config)?;
    let index = if catalog.is_empty() {
        job_status::RunIndex::default()
    } else {
        let store = open_store(&config, true).await?;
        let index = job_status::rebuild(&store).await?;
        store.close().await;
        index
    };
    if json_output {
        let jobs: Vec<Value> = catalog
            .iter()
            .map(|job| job_json(&catalog, job, index.last_run(&job.id)))
            .collect();
        return serde_json::to_string_pretty(&json!({
            "jobs": jobs,
            "index_truncated": index.truncated,
        }))
        .map_err(|error| error.to_string());
    }
    if catalog.is_empty() {
        return Ok("no jobs declared under superagent.jobs\n".to_string());
    }
    let mut out = String::new();
    for job in catalog.iter() {
        let triggers = if job.triggers.is_empty() {
            "manual".to_string()
        } else {
            job.triggers.join(",")
        };
        let alerts: Vec<String> = catalog
            .alerts_for(&job.id)
            .iter()
            .map(|alert| alert.id.clone())
            .collect();
        let last_run = index.last_run(&job.id).map_or_else(
            || "last_run=-".to_string(),
            |run| format!("last_run={} status={}", run.run_id, run.status.as_str()),
        );
        out.push_str(&format!(
            "job={} steps={} plan={} triggers={} alerts={} {}\n",
            job.id,
            job.plan.steps.len(),
            job.digest,
            triggers,
            if alerts.is_empty() {
                "-".to_string()
            } else {
                alerts.join(",")
            },
            last_run,
        ));
    }
    if index.truncated {
        out.push_str("note=run index scan reached its bound; older runs may be omitted\n");
    }
    Ok(out)
}

fn job_json(
    catalog: &crate::job_config::JobCatalog,
    job: &crate::job_config::ResolvedJob,
    last_run: Option<&job_status::RunIndexEntry>,
) -> Value {
    let alerts: Vec<String> = catalog
        .alerts_for(&job.id)
        .iter()
        .map(|alert| alert.id.clone())
        .collect();
    json!({
        "id": job.id,
        "plan_digest": job.digest,
        "steps": job.plan.steps.len(),
        "triggers": job.triggers,
        "alerts": alerts,
        "last_run": last_run.map(|run| json!({
            "run_id": run.run_id,
            "slot": run.slot,
            "started_ms": run.started_ms,
            "status": run.status.as_str(),
        })),
    })
}

async fn status_command(target: &str, path: &Path, json_output: bool) -> Result<String, String> {
    let config = load_config(path)?;
    let (_schedules, catalog) = resolved(&config)?;
    if let Some((job, slot)) = split_run_id(target) {
        let session = plan::run_session(job, slot);
        let store = open_store(&config, true).await?;
        let events = store
            .rebuild(session)
            .await
            .map_err(|error| format!("run {target}: {error}"))?;
        // A run row is durable before any drain consumes it, so a fresh run
        // has no stream and therefore no marker to read. The row's key is
        // the proof of admission: without it the run id names nothing
        // (§15.1).
        let pending_row = if events.is_empty() {
            let run = plan::run_id(job, slot);
            let key = plan::run_input_key(&run);
            let rows = store
                .input_rows(session)
                .await
                .map_err(|error| format!("run {target}: {error}"))?;
            rows.into_iter()
                .find(|row| row.idempotency_key.as_deref() == Some(key.as_str()))
        } else {
            None
        };
        store.close().await;
        if events.is_empty() {
            let Some(row) = pending_row else {
                return Err(format!(
                    "run {target} was never admitted: no inbox row carries its key                      (kian job run {job} admits one)"
                ));
            };
            let Some(marker_value) = row.payload.get(status::JOB_KEY) else {
                return Err(format!(
                    "run {target} carries no job marker: it is not a job run (session {})",
                    session.0
                ));
            };
            let marker = status::parse_marker(marker_value)
                .map_err(|error| format!("run {target} carries an invalid job marker: {error}"))?;
            if marker.run_id != target || marker.job_id != job || marker.slot != slot {
                return Err(run_identity_conflict(target, session));
            }
            let plan_source = marker_value
                .get("plan_source")
                .and_then(Value::as_str)
                .map(str::to_string);
            let plan_author = marker_value
                .get("plan_author")
                .and_then(Value::as_str)
                .map(str::to_string);
            let view = status::pending_view(&marker, plan_source, plan_author, row.admitted_at);
            if json_output {
                let mut value = view_json(&view);
                value["note"] = json!(MANUAL_NOTE);
                return serde_json::to_string_pretty(&value).map_err(|error| error.to_string());
            }
            return Ok(format!("{}\nnote={MANUAL_NOTE}\n", status::render(&view)));
        }
        let Some(view) = status::view(&events) else {
            return Err(format!(
                "run {target} carries no job marker: it is not a job run (session {})",
                session.0
            ));
        };
        if json_output {
            return serde_json::to_string_pretty(&view_json(&view))
                .map_err(|error| error.to_string());
        }
        return Ok(status::render(&view));
    }
    let Some(job) = catalog.job(target) else {
        return Err(unknown_job(&catalog, target));
    };
    let store = open_store(&config, true).await?;
    let index = job_status::rebuild(&store).await?;
    let Some(entry) = index.last_run(&job.id) else {
        store.close().await;
        if json_output {
            return serde_json::to_string_pretty(&json!({
                "job": job.id,
                "status": "no_runs",
            }))
            .map_err(|error| error.to_string());
        }
        return Ok(format!("job={}\nstatus=no_runs\n", job.id));
    };
    let session = entry.session;
    let events = store
        .rebuild(session)
        .await
        .map_err(|error| format!("run {}: {error}", entry.run_id))?;
    let pending = if events.is_empty() {
        let rows = store
            .input_rows(session)
            .await
            .map_err(|error| format!("run {}: {error}", entry.run_id))?;
        let key = plan::run_input_key(&entry.run_id);
        let row = rows
            .into_iter()
            .find(|row| row.idempotency_key.as_deref() == Some(key.as_str()))
            .ok_or_else(|| format!("run {} was never admitted", entry.run_id))?;
        let marker_value = row.payload.get(status::JOB_KEY).ok_or_else(|| {
            format!(
                "run {} carries no job marker (session {})",
                entry.run_id, session.0
            )
        })?;
        let marker = status::parse_marker(marker_value).map_err(|error| {
            format!(
                "run {} carries an invalid job marker: {error}",
                entry.run_id
            )
        })?;
        let plan_source = marker_value
            .get("plan_source")
            .and_then(Value::as_str)
            .map(str::to_string);
        let plan_author = marker_value
            .get("plan_author")
            .and_then(Value::as_str)
            .map(str::to_string);
        Some(status::pending_view(
            &marker,
            plan_source,
            plan_author,
            row.admitted_at,
        ))
    } else {
        None
    };
    store.close().await;
    let view = if let Some(view) = pending {
        view
    } else if let Some(view) = status::view(&events) {
        view
    } else {
        return Err(format!(
            "run {} carries no valid job marker (session {})",
            entry.run_id, session.0
        ));
    };
    if json_output {
        return serde_json::to_string_pretty(&view_json(&view)).map_err(|error| error.to_string());
    }
    Ok(status::render(&view))
}

fn unknown_job(catalog: &crate::job_config::JobCatalog, target: &str) -> String {
    let declared: Vec<&str> = catalog.iter().map(|job| job.id.as_str()).collect();
    format!(
        "unknown job {target} - not declared under superagent.jobs ({}) \
         (kian job list shows the declared ids)",
        if declared.is_empty() {
            "none declared".to_string()
        } else {
            declared.join(", ")
        }
    )
}

fn view_json(view: &status::RunView) -> Value {
    let mut object = serde_json::Map::new();
    object.insert("run_id".into(), json!(view.run_id));
    object.insert("job_id".into(), json!(view.job_id));
    object.insert("slot".into(), json!(view.slot));
    object.insert("trigger".into(), json!(view.trigger));
    if let Some(source) = &view.plan_source {
        object.insert("plan_source".into(), json!(source));
    }
    if let Some(author) = &view.plan_author {
        object.insert("plan_author".into(), json!(author));
    }
    object.insert("plan_digest".into(), json!(view.plan_digest));
    object.insert("status".into(), json!(view.status.as_str()));
    object.insert("status_basis".into(), json!(view.status_basis.as_str()));
    object.insert("reason".into(), json!(view.reason));
    object.insert(
        "outcome".into(),
        json!(view.outcome.as_ref().map(|outcome| outcome.as_str())),
    );
    object.insert("started_ms".into(), json!(view.started_ms));
    object.insert("ended_ms".into(), json!(view.ended_ms));
    object.insert(
        "steps".into(),
        json!(
            view.steps
                .iter()
                .map(|step| json!({
                    "id": step.id,
                    "action": step.action,
                    "state": step.state.as_str(),
                    "next_due_ms": step.next_due_ms,
                    "gate": step.gate,
                    "checks": step.checks,
                    "attempts": step.attempts.iter().map(|attempt| json!({
                        "attempt": attempt.attempt,
                        "worker": attempt.worker,
                        "started_ms": attempt.started_ms,
                        "ended_ms": attempt.ended_ms,
                        "outcome": attempt.outcome.as_ref().map(|outcome| outcome.as_str()),
                        "gate": attempt.gate,
                        "checks": attempt.checks,
                    })).collect::<Vec<Value>>(),
                }))
                .collect::<Vec<Value>>()
        ),
    );
    json!(object)
}

async fn run_job(
    job_id: &str,
    slot: Option<&str>,
    path: &Path,
    json_output: bool,
    pack: Option<&Path>,
    base: Option<&str>,
    force: bool,
) -> Result<String, JobCliError> {
    let config = load_config(path).map_err(refused)?;
    let (_schedules, catalog) = resolved(&config).map_err(refused)?;
    let Some(job) = catalog.job(job_id) else {
        return Err(refused(unknown_job(&catalog, job_id)));
    };
    let slot = match slot {
        Some(slot) => slot.to_string(),
        None => manual_slot(),
    };
    if slot.trim().is_empty() {
        return Err(usage("--slot must be a non-empty token"));
    }
    // V24: a plan that declares the surface cannot be admitted without a
    // symptom, and a plan that declares nothing takes no pack (14).
    let verified = match (job.plan.declares_surface(), pack) {
        (true, Some(dir)) => {
            // V30: a base commit is a full commit, never an abbreviation.
            if let Some(base) = base
                && !is_full_commit(base)
            {
                return Err(usage(format!(
                    "invalid base commit: {base:?} is not a full 40-hex commit"
                )));
            }
            Some(admission_selfdev(dir, base)?)
        }
        (true, None) => {
            return Err(usage(format!(
                "selfdev job requires an evidence pack: job {job_id} declares patch_paths_allowed (kian job run {job_id} --pack <dir>)"
            )));
        }
        (false, Some(_)) => {
            return Err(usage(format!(
                "job {job_id} declares no patch_paths_allowed check, so it takes no --pack"
            )));
        }
        (false, None) => {
            if base.is_some() {
                return Err(usage("--base needs --pack <dir>"));
            }
            None
        }
    };
    let session = plan::run_session(job_id, &slot);
    let run = plan::run_id(job_id, &slot);
    let content =
        crate::job_config::admission_content(job_id, &slot, crate::job_config::TRIGGER_MANUAL);
    let key = plan::run_input_key(&run);
    let store = open_store(&config, false).await.map_err(refused)?;
    // (10.2) the breaker is a trigger rule: it reads the last N settled runs
    // of this job through the operator projection and refuses before any row
    // exists, so an open circuit writes nothing.
    let streak = job.plan.limits.consecutive_failures;
    let circuit_open = if streak > 0 {
        match recent_run_outcomes(&store, job_id, streak).await {
            Ok(recent) => breaker_tripped(&recent, streak),
            Err(error) => {
                store.close().await;
                return Err(refused(error));
            }
        }
    } else {
        false
    };
    if circuit_open && !force {
        store.close().await;
        return Err(usage(format!(
            "selfdev_circuit_open: the last {streak} settled run(s) of {job_id} failed - no run was admitted (--force admits one run and records the override)"
        )));
    }
    let selfdev = verified.map(|mut value| {
        if force
            && circuit_open
            && let Some(object) = value.as_object_mut()
        {
            object.insert("circuit_overridden".to_string(), json!(true));
        }
        value
    });
    let marker = status::marker_payload_with_selfdev(
        job_id,
        &slot,
        crate::job_config::TRIGGER_MANUAL,
        &job.plan,
        selfdev,
    );
    if let Err(error) = ensure_run_identity(&store, session, &run, &marker).await {
        store.close().await;
        return Err(refused(error));
    }
    let now = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0);
    let admitted = store
        .admit(InputRow {
            id: uuid::Uuid::new_v4(),
            session,
            payload: json!({"content": content, "job": marker}),
            admitted_at: now,
            eligible_at: now,
            idempotency_key: Some(key),
        })
        .await
        .map_err(|error| refused(format!("admit run {run}: {error}")))?;
    store.close().await;
    if json_output {
        return serde_json::to_string_pretty(&json!({
            "run_id": run,
            "job_id": job_id,
            "slot": slot,
            "session": session.0.to_string(),
            "plan_digest": job.digest,
            "state": "admitted",
            "deduplicated": admitted.deduplicated,
            "note": MANUAL_NOTE,
        }))
        .map_err(|error| refused(error.to_string()));
    }
    Ok(format!(
        "run={run}\njob={}\nslot={slot}\nsession={}\nplan={}\nstate=admitted\ndeduplicated={}\nnote={MANUAL_NOTE}\n",
        job.id, session.0, job.digest, admitted.deduplicated
    ))
}

async fn ensure_run_identity(
    store: &EventStore,
    session: kian_core::model::SessionId,
    run: &str,
    expected: &Value,
) -> Result<(), String> {
    let expected = status::parse_marker(expected)
        .map_err(|error| format!("invalid admitted marker for {run}: {error}"))?;
    let events = store
        .rebuild(session)
        .await
        .map_err(|error| format!("run identity read {run}: {error}"))?;
    if !events.is_empty() {
        return match status::marker(&events) {
            Some(Ok(found)) if found.same_frozen_identity(&expected) => Ok(()),
            Some(Ok(_)) => Err(run_identity_conflict(run, session)),
            Some(Err(error)) => Err(format!(
                "run_identity_conflict: {run} carries an invalid marker: {error}"
            )),
            None => Err(run_identity_conflict(run, session)),
        };
    }
    let rows = store
        .input_rows(session)
        .await
        .map_err(|error| format!("run identity inbox read {run}: {error}"))?;
    let Some(row) = rows.first() else {
        return Ok(());
    };
    let Some(marker) = row.payload.get(status::JOB_KEY) else {
        return Err(run_identity_conflict(run, session));
    };
    let found = status::parse_marker(marker).map_err(|error| {
        format!("run_identity_conflict: {run} carries an invalid marker: {error}")
    })?;
    if found.same_frozen_identity(&expected) {
        Ok(())
    } else {
        Err(run_identity_conflict(run, session))
    }
}

/// The frozen selfdev value an admitted row carries (4.2), built from facts
/// the trigger re-derived rather than from the command line.
fn admission_selfdev(dir: &Path, base: Option<&str>) -> Result<Value, JobCliError> {
    let dir_text = dir.to_string_lossy().to_string();
    if dir_text.len() > MAX_SELFDEV_DIR_BYTES {
        return Err(pack_refusal(format!(
            "pack dir is {} bytes (the bound is {MAX_SELFDEV_DIR_BYTES})",
            dir_text.len()
        )));
    }
    let manifest = crate::self_evidence::verify_pack(dir).map_err(pack_refusal)?;
    let manifest_bytes = std::fs::read(dir.join("manifest.json"))
        .map_err(|error| pack_refusal(format!("read manifest.json: {error}")))?;
    let manifest_sha256 = crate::archive::sha256_hex(&manifest_bytes);
    let base_commit = base
        .map(str::to_string)
        .or_else(|| manifest.repo.commit.clone())
        .ok_or_else(|| {
            pack_refusal(
                "repo.json carries no commit (the pack was taken outside a git checkout) and --base was not given",
            )
        })?;
    let value = json!({
        "pack": {
            "dir": dir_text,
            "manifest_sha256": format!("sha256:{manifest_sha256}"),
            "files_sha256": format!("sha256:{}", manifest.files_sha256),
        },
        "base_commit": base_commit,
        "kian_version": env!("CARGO_PKG_VERSION"),
    });
    let len = serde_json::to_vec(&value).map_or(0, |bytes| bytes.len());
    if len > MAX_SELFDEV_BYTES {
        return Err(pack_refusal(format!(
            "job.selfdev is {len} bytes (the bound is {MAX_SELFDEV_BYTES})"
        )));
    }
    Ok(value)
}

fn is_full_commit(text: &str) -> bool {
    text.len() == 40 && text.chars().all(|c| c.is_ascii_hexdigit())
}

/// V26's refusal shape: an unverified pack is the whole message.
fn pack_refusal(reason: impl std::fmt::Display) -> JobCliError {
    usage(format!("evidence pack does not verify: {reason}"))
}

/// The breaker predicate (10.2), newest first: a full streak of settled
/// failed runs. Unverified and cancelled say something about a run, not
/// about a broken job, so neither extends the streak.
fn breaker_tripped(recent: &[RunOutcome], streak: u32) -> bool {
    let streak = usize::try_from(streak).unwrap_or(usize::MAX);
    streak > 0
        && recent.len() >= streak
        && recent[..streak]
            .iter()
            .all(|outcome| *outcome == RunOutcome::Failed)
}

/// The last N settled runs of one job, newest first, through the projection
/// the operator surface reads: a run is a session, its job identity is the
/// marked input row, and its outcome is the run verdict (11.1).
async fn recent_run_outcomes(
    store: &EventStore,
    job_id: &str,
    limit: u32,
) -> Result<Vec<RunOutcome>, String> {
    let sessions = crate::cache_metrics::sessions_with_events(store)
        .await
        .map_err(|error| format!("sessions: {error}"))?;
    let mut settled: Vec<(u64, RunOutcome)> = Vec::new();
    for session in sessions {
        let events = store
            .rebuild(session)
            .await
            .map_err(|error| format!("session {}: {error}", session.0))?;
        let Some(Ok(found)) = status::marker(&events) else {
            continue;
        };
        if found.job_id != job_id {
            continue;
        }
        let Some(verdict) = status::run_verdict(&events) else {
            continue;
        };
        settled.push((verdict.at_ms, verdict.outcome));
    }
    settled.sort_by_key(|settled| std::cmp::Reverse(settled.0));
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    settled.truncate(limit);
    Ok(settled.into_iter().map(|(_, outcome)| outcome).collect())
}

async fn cancel(run: &str, path: &Path, json_output: bool) -> Result<String, String> {
    let config = load_config(path)?;
    let Some((job, slot)) = split_run_id(run) else {
        return Err(format!("{run} is not a run id (expected <job>@<slot>)"));
    };
    let session = plan::run_session(job, slot);
    let catalog = resolved(&config)?.1;
    if catalog.job(job).is_none() {
        return Err(unknown_job(&catalog, job));
    }
    let store = open_store(&config, false).await?;
    // Only a run session may receive a cancel row: a row into an arbitrary
    // session would be an unexplained input, so the marker gates it. A run
    // admitted but not started is evidenced by its durable run row.
    let events = store
        .rebuild(session)
        .await
        .map_err(|error| format!("run {run}: {error}"))?;
    let pending_marker = if events.is_empty() {
        let rows = store
            .input_rows(session)
            .await
            .map_err(|error| format!("cancel run {run}: {error}"))?;
        let key = plan::run_input_key(run);
        let Some(row) = rows
            .into_iter()
            .find(|row| row.idempotency_key.as_deref() == Some(key.as_str()))
        else {
            store.close().await;
            return Err(format!("run {run} was never admitted"));
        };
        let Some(marker) = row.payload.get(status::JOB_KEY) else {
            store.close().await;
            return Err(run_identity_conflict(run, session));
        };
        let found = status::parse_marker(marker)
            .map_err(|error| format!("run {run} carries an invalid job marker: {error}"))?;
        Some(found)
    } else {
        None
    };
    let found = if let Some(marker) = pending_marker {
        marker
    } else {
        let Some(first) = events
            .iter()
            .find(|event| {
                event.kind == kian_core::events::EventKind::InputAdmitted
                    && event.bytes.get(status::JOB_KEY).is_some()
            })
            .and_then(|event| event.bytes.get(status::JOB_KEY))
        else {
            store.close().await;
            return Err(format!(
                "run {run} carries no job marker: it is not a job run (session {})",
                session.0
            ));
        };
        match status::parse_marker(first) {
            Ok(marker) => marker,
            Err(error) => {
                store.close().await;
                return Err(format!("run {run} carries an invalid job marker: {error}"));
            }
        }
    };
    if found.run_id != run || found.job_id != job || found.slot != slot {
        store.close().await;
        return Err(run_identity_conflict(run, session));
    }
    if let Some(outcome) = terminal_run_outcome(&events) {
        store.close().await;
        return Ok(already_terminal_output(
            run,
            session,
            Some(outcome),
            json_output,
        ));
    }
    let payload = json!({"role": "user", "content": CANCEL_CONTENT});
    let now = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0);
    let admitted = match store
        .admit(InputRow {
            id: uuid::Uuid::new_v4(),
            session,
            payload,
            admitted_at: now,
            eligible_at: now,
            idempotency_key: Some(format!("kian job:cancel:{run}")),
        })
        .await
    {
        Ok(admitted) => admitted,
        Err(kian_store::InboxError::SessionTerminal { .. }) => {
            let outcome = status::run_verdict(&events)
                .map(|verdict| verdict.outcome)
                .or(Some(RunOutcome::Failed));
            store.close().await;
            return Ok(already_terminal_output(run, session, outcome, json_output));
        }
        Err(error) => {
            store.close().await;
            return Err(format!("cancel run {run}: {error}"));
        }
    };
    store.close().await;
    if json_output {
        return serde_json::to_string_pretty(&json!({
            "run_id": run,
            "session": session.0.to_string(),
            "state": "cancel_requested",
            "deduplicated": admitted.deduplicated,
            "note": CANCEL_NOTE,
        }))
        .map_err(|error| error.to_string());
    }
    Ok(format!(
        "run={run}\nsession={}\nstate=cancel_requested\ndeduplicated={}\nnote={CANCEL_NOTE}\n",
        session.0, admitted.deduplicated
    ))
}

fn terminal_run_outcome(events: &[kian_core::events::Event]) -> Option<RunOutcome> {
    if let Some(verdict) = status::run_verdict(events) {
        return Some(verdict.outcome);
    }
    events
        .iter()
        .any(|event| {
            matches!(
                event.kind,
                kian_core::events::EventKind::SessionEnded { .. }
            )
        })
        .then_some(RunOutcome::Failed)
}

fn already_terminal_output(
    run: &str,
    session: kian_core::model::SessionId,
    outcome: Option<RunOutcome>,
    json_output: bool,
) -> String {
    if json_output {
        return serde_json::to_string_pretty(&json!({
            "run_id": run,
            "session": session.0.to_string(),
            "state": "already_terminal",
            "outcome": outcome.map(|outcome| outcome.as_str()),
        }))
        .expect("static terminal result serializes");
    }
    format!(
        "run={run}\nsession={}\nstate=already_terminal\noutcome={}\n",
        session.0,
        outcome.map_or("-", RunOutcome::as_str)
    )
}

fn run_identity_conflict(run: &str, session: kian_core::model::SessionId) -> String {
    format!(
        "run_identity_conflict: {run} derives session {} but that stream does not carry the same job marker",
        session.0
    )
}
#[cfg(test)]
mod tests {
    use super::*;

    fn trigger_of_slot(slot: &str) -> &'static str {
        if slot.starts_with("manual:") {
            crate::job_config::TRIGGER_MANUAL
        } else if slot.starts_with(plan::ALERT_SLOT_PREFIX) {
            crate::job_config::TRIGGER_ALERT
        } else {
            crate::job_config::TRIGGER_CRON
        }
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("a runtime")
            .block_on(future)
    }

    struct Temp(PathBuf);

    impl Temp {
        fn new(name: &str) -> Self {
            let base =
                std::env::temp_dir().join(format!("kian-job-cli-{name}-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&base).expect("temp dir");
            Self(base)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn write_config(dir: &Path, sqlite: &Path) -> PathBuf {
        let yaml = format!(
            r#"listen: "127.0.0.1:0"
storage:
  sqlite: "{}"
agent:
  id: job-cli-agent
  model_slots:
    main:
      provider: mock
      model: mock-model
  tools: []
providers:
  mock:
    base_url: "http://127.0.0.1:1"
    api_key_env: KIAN_JOB_CLI_KEY
tools:
  root: "{}"
  exec_timeout_ms: 30000
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
plugins: ["openai-compat", "local-tools", "superagent"]
agents:
  router:
    id: router
    version: 3
    model_slots:
      router:
        provider: mock
        model: mock-model
  worker:
    id: worker
    version: 7
    model_slots:
      main:
        provider: mock
        model: mock-model
superagent:
  router:
    session_id: "5b5b5b5b-5b5b-5b5b-5b5b-5b5b5b5b5b5b"
    agent: router
    slot: router
  worker:
    agent: worker
  channels:
    mock-im:
      type: mock-im
    alert-im:
      type: mock-im
  jobs:
    jobs:
      - id: digest
        plan:
          - id: fetch
            prompt: "fetch the news"
            requires: ["net.unrestricted"]
            produces:
              name: raw
            verify:
              - check: min_tool_results
                n: 1
          - id: notify
            action: deliver
            depends_on: ["fetch"]
            inputs:
              - step: fetch
                name: raw
            channel: mock-im
            target:
              handle: owner
            content:
              from_artifact:
                step: fetch
                name: raw
            verify:
              - check: delivery_receipt
                channel: mock-im
                target:
                  handle: owner
    triggers:
      - job: digest
        expr: "@daily"
    alerts:
      - id: ops
        on_failure_of: digest
        channel: alert-im
        target:
          handle: owner
"#,
            sqlite.display(),
            dir.display()
        );
        let path = dir.join("kian.yaml");
        std::fs::write(&path, yaml).expect("write kian.yaml");
        path
    }

    fn run_cli(args: &[&str]) -> Result<String, JobCliError> {
        let args: Vec<String> = args.iter().map(|arg| (*arg).to_string()).collect();
        run(&args)
    }

    fn fixture(name: &str) -> (Temp, PathBuf) {
        let temp = Temp::new(name);
        let config = write_config(temp.path(), &temp.path().join("events.sqlite"));
        (temp, config)
    }

    fn config_arg(config: &Path) -> String {
        config.to_str().expect("a utf8 path").to_string()
    }

    #[test]
    fn parse_list_takes_an_optional_config_and_json() {
        assert_eq!(
            parse_args(&["list".to_string()]).expect("a bare list"),
            JobCommand::List {
                config: PathBuf::from("kian.yaml"),
                json: false,
            }
        );
        assert_eq!(
            parse_args(&[
                "list".to_string(),
                "--config".to_string(),
                "a.yaml".to_string(),
                "--json".to_string(),
            ])
            .expect("list with flags"),
            JobCommand::List {
                config: PathBuf::from("a.yaml"),
                json: true,
            }
        );
    }

    #[test]
    fn parse_run_carries_the_slot_and_refuses_a_missing_argument() {
        assert_eq!(
            parse_args(&[
                "run".to_string(),
                "digest".to_string(),
                "--slot".to_string(),
                "manual:ABC".to_string(),
            ])
            .expect("run with a slot"),
            JobCommand::Run {
                job: "digest".to_string(),
                slot: Some("manual:ABC".to_string()),
                pack: None,
                base: None,
                force: false,
                config: PathBuf::from("kian.yaml"),
                json: false,
            }
        );
        for args in [["run"], ["status"], ["cancel"], ["nope"]] {
            let parsed = parse_args(&[args[0].to_string()]);
            assert!(
                matches!(parsed, Err(JobCliError::Usage(_))),
                "{args:?} must be a usage error"
            );
        }
        assert!(matches!(
            parse_args(&[
                "run".to_string(),
                "digest".to_string(),
                "--slot".to_string()
            ]),
            Err(JobCliError::Usage(_))
        ));
    }

    #[test]
    fn parse_run_carries_the_evidence_pack_flags() {
        assert_eq!(
            parse_args(&[
                "run".to_string(),
                "self-dev".to_string(),
                "--pack".to_string(),
                "/tmp/pack".to_string(),
                "--base".to_string(),
                "9f3c".to_string(),
                "--force".to_string(),
            ])
            .expect("run with a pack"),
            JobCommand::Run {
                job: "self-dev".to_string(),
                slot: None,
                pack: Some(PathBuf::from("/tmp/pack")),
                base: Some("9f3c".to_string()),
                force: true,
                config: PathBuf::from("kian.yaml"),
                json: false,
            }
        );
        assert!(matches!(
            parse_args(&[
                "run".to_string(),
                "digest".to_string(),
                "--pack".to_string()
            ]),
            Err(JobCliError::Usage(_))
        ));
        assert!(matches!(
            parse_args(&[
                "run".to_string(),
                "digest".to_string(),
                "--base".to_string()
            ]),
            Err(JobCliError::Usage(_))
        ));
    }

    #[test]
    fn the_breaker_needs_a_full_streak_of_failed_runs() {
        use RunOutcome::{Cancelled, Failed, Succeeded, Unverified};
        assert!(breaker_tripped(&[Failed, Failed, Failed], 3));
        assert!(breaker_tripped(&[Failed, Failed, Failed, Succeeded], 3));
        assert!(breaker_tripped(&[Failed], 1));
        assert!(!breaker_tripped(&[Failed, Failed], 3));
        assert!(!breaker_tripped(&[Failed, Succeeded, Failed], 3));
        assert!(!breaker_tripped(&[Failed, Unverified, Failed], 3));
        assert!(!breaker_tripped(&[Failed, Cancelled, Failed], 3));
        assert!(!breaker_tripped(&[Failed, Failed, Failed], 0));
    }

    #[test]
    fn a_base_commit_needs_a_pack() {
        let temp = Temp::new("selfdev-base-without-pack");
        let config = write_config(temp.path(), &temp.path().join("kian.sqlite"));
        let error = block_on(run_job(
            "digest",
            Some("manual:ABC"),
            &config,
            false,
            None,
            Some("9f3c"),
            false,
        ))
        .expect_err("a base commit is a claim about a pack");
        assert!(matches!(error, JobCliError::Usage(_)), "{error:?}");
        assert!(error.message().contains("--base needs --pack"), "{error:?}");
    }

    #[test]
    fn a_base_commit_is_a_full_commit() {
        assert!(is_full_commit(&"a".repeat(40)));
        assert!(is_full_commit("9F3C000000000000000000000000000000000000"));
        assert!(!is_full_commit("9f3c"));
        assert!(!is_full_commit(&"a".repeat(39)));
        assert!(!is_full_commit(&"g".repeat(40)));
    }

    #[test]
    fn a_manual_slot_is_a_ulid_shaped_token_and_never_repeats() {
        let first = manual_slot();
        let second = manual_slot();
        assert_ne!(first, second);
        let token = first.strip_prefix("manual:").expect("the manual prefix");
        assert_eq!(token.len(), 26, "{token}");
        assert!(
            token
                .chars()
                .all(|symbol| "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(symbol)),
            "{token}"
        );
    }

    #[test]
    fn crockford_encodes_the_ulid_layout() {
        assert_eq!(crockford_base32(&[0u8; 16]), "00000000000000000000000000");
        let mut bytes = [0u8; 16];
        bytes[15] = 1;
        assert_eq!(crockford_base32(&bytes), "00000000000000000000000001");
    }

    #[test]
    fn split_run_id_needs_exactly_one_separator() {
        assert_eq!(
            split_run_id("digest@2026-11-23T09:00:00+08:00"),
            Some(("digest", "2026-11-23T09:00:00+08:00"))
        );
        for raw in ["digest", "digest@", "@slot", "a@b@c"] {
            assert_eq!(split_run_id(raw), None, "{raw}");
        }
    }

    #[test]
    fn a_slot_token_names_its_trigger() {
        assert_eq!(
            trigger_of_slot("manual:ABC"),
            crate::job_config::TRIGGER_MANUAL
        );
        assert_eq!(
            trigger_of_slot("alert:digest@x"),
            crate::job_config::TRIGGER_ALERT
        );
        assert_eq!(
            trigger_of_slot("2026-11-23T09:00:00+08:00"),
            crate::job_config::TRIGGER_CRON
        );
    }

    #[test]
    fn list_shows_the_declared_job_its_trigger_and_its_alert() {
        let (_temp, config) = fixture("list");
        let out = run_cli(&["list", "--config", &config_arg(&config)]).expect("list");
        assert!(out.contains("job=digest"), "{out}");
        assert!(out.contains("steps=2"), "{out}");
        assert!(out.contains("plan=sha256:"), "{out}");
        assert!(out.contains("triggers=digest"), "{out}");
        assert!(out.contains("alerts=ops"), "{out}");
        assert!(out.contains("last_run=-"), "{out}");
    }

    #[test]
    fn list_says_so_when_no_job_is_declared() {
        let temp = Temp::new("list-empty");
        let source = write_config(temp.path(), &temp.path().join("events.sqlite"));
        let text = std::fs::read_to_string(&source).expect("read the fixture");
        let cut = text.find("  jobs:").expect("the jobs section");
        let path = temp.path().join("no-jobs.yaml");
        std::fs::write(&path, &text[..cut]).expect("write a jobs-free config");
        let out = run_cli(&["list", "--config", &config_arg(&path)]).expect("list");
        assert!(out.contains("no jobs declared"), "{out}");
    }

    #[test]
    fn run_admits_a_durable_row_and_dedupes_on_replay() {
        let (_temp, config) = fixture("run");
        let path = config_arg(&config);
        let first =
            run_cli(&["run", "digest", "--slot", "manual:TEST", "--config", &path]).expect("run");
        assert!(first.contains("run=digest@manual:TEST"), "{first}");
        assert!(first.contains("plan=sha256:"), "{first}");
        assert!(first.contains("state=admitted"), "{first}");
        assert!(first.contains("deduplicated=false"), "{first}");
        let replay = run_cli(&["run", "digest", "--slot", "manual:TEST", "--config", &path])
            .expect("replay");
        assert!(replay.contains("deduplicated=true"), "{replay}");
    }

    #[test]
    fn run_can_default_the_slot_to_a_fresh_manual_token() {
        let (_temp, config) = fixture("run-manual");
        let path = config_arg(&config);
        let first = run_cli(&["run", "digest", "--config", &path]).expect("run");
        assert!(first.contains("run=digest@manual:"), "{first}");
        let second = run_cli(&["run", "digest", "--config", &path]).expect("second run");
        assert_ne!(first, second, "each manual run gets its own slot");
    }

    #[test]
    fn run_refuses_an_undeclared_job_and_an_empty_slot() {
        let (_temp, config) = fixture("run-refusals");
        let path = config_arg(&config);
        let unknown = run_cli(&["run", "nope", "--config", &path]).expect_err("unknown job");
        assert!(
            unknown.message().contains("unknown job nope"),
            "{unknown:?}"
        );
        let empty =
            run_cli(&["run", "digest", "--slot", " ", "--config", &path]).expect_err("empty slot");
        assert!(empty.message().contains("non-empty token"), "{empty:?}");
    }

    #[test]
    fn run_json_names_the_run_and_its_dedup_state() {
        let (_temp, config) = fixture("run-json");
        let path = config_arg(&config);
        let out = run_cli(&[
            "run",
            "digest",
            "--slot",
            "manual:JSON",
            "--config",
            &path,
            "--json",
        ])
        .expect("run");
        let parsed: Value = serde_json::from_str(&out).expect("json output");
        assert_eq!(parsed["run_id"], "digest@manual:JSON");
        assert_eq!(parsed["state"], "admitted");
        assert_eq!(parsed["deduplicated"], false);
        assert!(
            parsed["plan_digest"]
                .as_str()
                .expect("a plan digest")
                .starts_with("sha256:")
        );
    }

    #[test]
    fn status_of_an_admitted_run_is_pending_not_unknown() {
        let (_temp, config) = fixture("status-pending");
        let path = config_arg(&config);
        run_cli(&["run", "digest", "--slot", "manual:PEND", "--config", &path]).expect("run");
        let out = run_cli(&["status", "digest@manual:PEND", "--config", &path]).expect("status");
        assert!(out.contains("status   pending (stream_only)"), "{out}");
        assert!(out.contains("plan sha256:"), "{out}");
    }

    #[test]
    fn status_refuses_a_run_that_was_never_admitted() {
        let (_temp, config) = fixture("status-absent");
        let path = config_arg(&config);
        let refused = run_cli(&["status", "digest@manual:NOPE", "--config", &path])
            .expect_err("a run with no row");
        assert!(refused.message().contains("never admitted"), "{refused:?}");
    }

    #[test]
    fn status_of_a_job_without_a_run_reports_no_runs() {
        let (_temp, config) = fixture("status-job");
        let path = config_arg(&config);
        let out = run_cli(&["status", "digest", "--config", &path]).expect("status");
        assert_eq!(out, "job=digest\nstatus=no_runs\n");
    }

    #[test]
    fn status_refuses_an_undeclared_job_id() {
        let (_temp, config) = fixture("status-unknown-job");
        let path = config_arg(&config);
        let refused =
            run_cli(&["status", "nope", "--config", &path]).expect_err("an unknown job id");
        assert!(
            refused.message().contains("unknown job nope"),
            "{refused:?}"
        );
    }

    #[test]
    fn cancel_of_a_pending_run_admits_one_ordinary_cancel_row() {
        let (_temp, config) = fixture("cancel-pending");
        let path = config_arg(&config);
        run_cli(&[
            "run",
            "digest",
            "--slot",
            "manual:CANCEL",
            "--config",
            &path,
        ])
        .expect("run");
        let cancelled =
            run_cli(&["cancel", "digest@manual:CANCEL", "--config", &path]).expect("cancel");
        assert!(cancelled.contains("state=cancel_requested"), "{cancelled}");
        assert!(cancelled.contains("deduplicated=false"), "{cancelled}");
        let replay =
            run_cli(&["cancel", "digest@manual:CANCEL", "--config", &path]).expect("cancel again");
        assert!(replay.contains("deduplicated=true"), "{replay}");
        let session = plan::run_session("digest", "manual:CANCEL");
        let store = block_on(open_store(
            &crate::config::ServeConfig::load(&config).expect("config"),
            true,
        ))
        .expect("store");
        let rows = block_on(store.input_rows(session)).expect("rows");
        block_on(store.close());
        assert_eq!(rows.len(), 2, "the run row and the cancel row");
        let cancel_rows: Vec<_> = rows
            .iter()
            .filter(|row| {
                row.payload.get("content").and_then(Value::as_str) == Some(CANCEL_CONTENT)
            })
            .collect();
        assert_eq!(cancel_rows.len(), 1, "exactly one cancel row");
        assert!(
            cancel_rows
                .iter()
                .all(|row| row.payload.get("job").is_none()),
            "a control row is never a second job-marked input"
        );
    }

    #[test]
    fn cancel_refuses_a_token_that_is_not_a_run_id() {
        let (_temp, config) = fixture("cancel-refusal");
        let path = config_arg(&config);
        let refused = run_cli(&["cancel", "digest", "--config", &path]).expect_err("not a run id");
        assert!(refused.message().contains("is not a run id"), "{refused:?}");
        let undeclared = run_cli(&["cancel", "nope@manual:X", "--config", &path])
            .expect_err("an undeclared job");
        assert!(
            undeclared.message().contains("unknown job nope"),
            "{undeclared:?}"
        );
    }

    #[test]
    fn cancel_of_a_terminal_run_reports_already_terminal() {
        let (_temp, config) = fixture("cancel-terminal");
        let path = config_arg(&config);
        run_cli(&[
            "run",
            "digest",
            "--slot",
            "manual:TERMINAL",
            "--config",
            &path,
        ])
        .expect("run");
        let run = "digest@manual:TERMINAL";
        let session = plan::run_session("digest", "manual:TERMINAL");
        let loaded = crate::config::ServeConfig::load(&config).expect("config");
        let (_, catalog) = resolved(&loaded).expect("jobs");
        let declared = catalog.job("digest").expect("declared job");
        let marker = status::marker_payload(
            "digest",
            "manual:TERMINAL",
            crate::job_config::TRIGGER_MANUAL,
            &declared.plan,
        );
        let store = block_on(open_store(&loaded, false)).expect("store");
        block_on(async {
            for event in [
                kian_core::events::Event {
                    seq: 1,
                    session,
                    epoch: kian_core::model::Epoch(1),
                    ts: 1,
                    kind: kian_core::events::EventKind::SessionCreated,
                    bytes: json!({}),
                    version: 0,
                },
                kian_core::events::Event {
                    seq: 2,
                    session,
                    epoch: kian_core::model::Epoch(1),
                    ts: 2,
                    kind: kian_core::events::EventKind::InputAdmitted,
                    bytes: json!({"role": "user", "content": "run", "job": marker}),
                    version: 0,
                },
                kian_core::events::Event {
                    seq: 3,
                    session,
                    epoch: kian_core::model::Epoch(1),
                    ts: 3,
                    kind: kian_core::events::EventKind::SessionEnded {
                        reason: kian_core::events::EndReason::Failed,
                    },
                    bytes: json!({}),
                    version: 0,
                },
            ] {
                let seq = event.seq;
                store
                    .append(
                        session,
                        event,
                        kian_store::AppendGuard {
                            epoch: kian_core::model::Epoch(1),
                            expected_seq: seq,
                        },
                    )
                    .await
                    .expect("seed terminal run");
            }
        });
        block_on(store.close());

        let output = run_cli(&["cancel", run, "--config", &path]).expect("already terminal");
        assert!(output.contains("state=already_terminal"), "{output}");
        assert!(output.contains("outcome=failed"), "{output}");
    }
}
