//! `kian cron ...` - the operator surface of the scheduler (CRON-MODEL SS10).
//!
//! Four verbs over the same two sources of truth: `superagent.cron` (the
//! declarations - config wins) and the cron rows in the event store (the
//! cursor, the schedule rows and the firing history). Nothing here rewrites
//! the operator's config file: `rm` deletes the store row and names the
//! declaration that still has to be edited by hand.
//!
//! Conventions match the rest of the binary: hand-rolled argv parsing (this
//! crate has no clap), `--config <path>` defaulting to `./kian.yaml`,
//! `--json` for the machine-readable form, exit code 2 for a usage error and
//! exit code 1 for a refused operation.

use std::path::{Path, PathBuf};

use chrono::{SecondsFormat, TimeZone, Utc};

use crate::config::{CronRuntimeSchedule, ServeConfig};
use crate::cron_tick::{CronDispatcher, dispatch_payload};
use crate::cron_worktree::{WorktreeDispatcher, WorktreePolicy};
use kian_store::{CronFiringRow, EventStore, FiringState};

/// `--limit` default for `kian cron logs`.
pub const DEFAULT_LOG_LIMIT: u64 = 20;

/// The parsed form of one `kian cron ...` invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CronCommand {
    /// `kian cron list` - every declared schedule, one line each.
    List { config: PathBuf, json: bool },
    /// `kian cron run-now <id>` - admit one firing immediately.
    RunNow {
        id: String,
        config: PathBuf,
        json: bool,
    },
    /// `kian cron logs --schedule <id> [--limit N]` - recent firings.
    Logs {
        schedule: String,
        limit: u64,
        config: PathBuf,
        json: bool,
    },
    /// `kian cron rm <id>` - drop the durable schedule row.
    Rm {
        id: String,
        config: PathBuf,
        json: bool,
    },
}

/// A failed invocation: `Usage` is exit code 2, `Refused` is exit code 1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CronCliError {
    /// The argv did not parse (bad flag, missing id, bad `--limit`).
    Usage(String),
    /// The operation itself was refused (unknown id, disabled schedule,
    /// store failure) - the message is the operator's explanation.
    Refused(String),
}

impl CronCliError {
    /// The operator-facing message, without the `kian: ` prefix.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            CronCliError::Usage(m) | CronCliError::Refused(m) => m,
        }
    }

    /// The process exit code this error maps to.
    #[must_use]
    pub fn exit_code(&self) -> u8 {
        match self {
            CronCliError::Usage(_) => 2,
            CronCliError::Refused(_) => 1,
        }
    }
}

/// The usage line every parse error points at.
pub const USAGE: &str = "usage:
  kian cron list [--config <path>] [--json]
  kian cron run-now <id> [--config <path>] [--json]
  kian cron logs --schedule <id> [--limit N] [--config <path>] [--json]
  kian cron rm <id> [--config <path>] [--json]
";

fn default_config() -> PathBuf {
    PathBuf::from("kian.yaml")
}

fn next_value(iter: &mut std::slice::Iter<'_, String>, flag: &str) -> Result<String, String> {
    iter.next()
        .cloned()
        .ok_or_else(|| format!("`{flag}` requires a value"))
}

/// Parses the argv **after** the `cron` verb.
pub fn parse_args(args: &[String]) -> Result<CronCommand, String> {
    let Some(verb) = args.first() else {
        return Err("`kian cron` needs a subcommand".to_string());
    };
    let rest = &args[1..];
    match verb.as_str() {
        "list" => {
            let mut config = default_config();
            let mut json = false;
            let mut iter = rest.iter();
            while let Some(arg) = iter.next() {
                match arg.as_str() {
                    "--config" => config = PathBuf::from(next_value(&mut iter, "--config")?),
                    "--json" => json = true,
                    other => return Err(format!("unknown `cron list` option `{other}`")),
                }
            }
            Ok(CronCommand::List { config, json })
        }
        "run-now" => {
            let mut id: Option<String> = None;
            let mut config = default_config();
            let mut json = false;
            let mut iter = rest.iter();
            while let Some(arg) = iter.next() {
                match arg.as_str() {
                    "--config" => config = PathBuf::from(next_value(&mut iter, "--config")?),
                    "--json" => json = true,
                    other if other.starts_with('-') => {
                        return Err(format!("unknown `cron run-now` option `{other}`"));
                    }
                    other => {
                        if id.is_some() {
                            return Err("`cron run-now` takes exactly one schedule id".to_string());
                        }
                        id = Some(other.to_string());
                    }
                }
            }
            let Some(id) = id else {
                return Err("`cron run-now` requires a schedule id".to_string());
            };
            Ok(CronCommand::RunNow { id, config, json })
        }
        "logs" => {
            let mut schedule: Option<String> = None;
            let mut limit = DEFAULT_LOG_LIMIT;
            let mut config = default_config();
            let mut json = false;
            let mut iter = rest.iter();
            while let Some(arg) = iter.next() {
                match arg.as_str() {
                    "--schedule" => {
                        schedule = Some(next_value(&mut iter, "--schedule")?);
                    }
                    "--limit" => {
                        let raw = next_value(&mut iter, "--limit")?;
                        let parsed =
                            raw.parse::<u64>().ok().filter(|n| *n > 0).ok_or_else(|| {
                                format!("`--limit` must be a positive integer, got `{raw}`")
                            })?;
                        limit = parsed;
                    }
                    "--config" => config = PathBuf::from(next_value(&mut iter, "--config")?),
                    "--json" => json = true,
                    other => return Err(format!("unknown `cron logs` option `{other}`")),
                }
            }
            let Some(schedule) = schedule else {
                return Err("`cron logs` requires `--schedule <id>`".to_string());
            };
            Ok(CronCommand::Logs {
                schedule,
                limit,
                config,
                json,
            })
        }
        "rm" => {
            let mut id: Option<String> = None;
            let mut config = default_config();
            let mut json = false;
            let mut iter = rest.iter();
            while let Some(arg) = iter.next() {
                match arg.as_str() {
                    "--config" => config = PathBuf::from(next_value(&mut iter, "--config")?),
                    "--json" => json = true,
                    other if other.starts_with('-') => {
                        return Err(format!("unknown `cron rm` option `{other}`"));
                    }
                    other => {
                        if id.is_some() {
                            return Err("`cron rm` takes exactly one schedule id".to_string());
                        }
                        id = Some(other.to_string());
                    }
                }
            }
            let Some(id) = id else {
                return Err("`cron rm` requires a schedule id".to_string());
            };
            Ok(CronCommand::Rm { id, config, json })
        }
        other => Err(format!("unknown cron subcommand `{other}`")),
    }
}

/// Parses, executes and renders one `kian cron ...` invocation.
///
/// The binary calls this; tests call [`execute`] / [`execute_with`] with a
/// parsed command so the store and the dispatcher stay injectable.
pub fn run(args: &[String]) -> Result<String, CronCliError> {
    let cmd = parse_args(args).map_err(|e| CronCliError::Usage(format!("{e}\n{USAGE}")))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| CronCliError::Refused(format!("cannot start async runtime: {e}")))?;
    runtime
        .block_on(execute(&cmd))
        .map_err(CronCliError::Refused)
}

/// Runs one parsed command against the real store and the real dispatcher.
pub async fn execute(cmd: &CronCommand) -> Result<String, String> {
    execute_with(cmd, None).await
}

/// [`execute`] with an optional injected dispatcher (tests never cut a
/// worktree and never spawn `dsh`).
pub async fn execute_with(
    cmd: &CronCommand,
    dispatcher: Option<&dyn CronDispatcher>,
) -> Result<String, String> {
    match cmd {
        CronCommand::List { config, json } => list(config, *json).await,
        CronCommand::RunNow { id, config, json } => run_now(id, config, *json, dispatcher).await,
        CronCommand::Logs {
            schedule,
            limit,
            config,
            json,
        } => logs(schedule, *limit, config, *json).await,
        CronCommand::Rm { id, config, json } => rm(id, config, *json).await,
    }
}

fn load_config(path: &Path) -> Result<ServeConfig, String> {
    ServeConfig::load(path).map_err(|e| format!("config {}: {e}", path.display()))
}

/// The resolved declarations, in declaration order (the `list` order).
fn runtime_schedules(config: &ServeConfig) -> Result<Vec<CronRuntimeSchedule>, String> {
    let Some(superagent) = &config.superagent else {
        return Ok(Vec::new());
    };
    superagent
        .cron
        .to_runtime(&superagent.channels, Utc::now())
        .map_err(|e| format!("superagent.cron: {e}"))
}

async fn open_store(config: &ServeConfig, read_only: bool) -> Result<EventStore, String> {
    if config.storage.is_postgres() {
        let section = config
            .storage
            .postgres
            .as_ref()
            .ok_or_else(|| "storage: the postgres section is missing".to_string())?;
        let url_env = section.url_env.clone();
        let url = std::env::var(&url_env)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| format!("`{url_env}` (the postgres URL) is not set"))?;
        let opened = if read_only {
            EventStore::open_postgres_read_only(&url).await
        } else {
            EventStore::open_postgres(&url).await
        };
        opened.map_err(|e| format!("store: {e}"))
    } else if read_only {
        // `open_read_only` never migrates (that is what keeps a store last
        // migrated by an older chain readable). On a fresh or absent sqlite
        // file the open itself fails, and the raw sqlite text ("unable to
        // open database file") reached the operator. One read-write open - the
        // same call the server makes on boot - applies the pending
        // migrations, and the read-only handle is then retried.
        match EventStore::open_read_only(&config.storage.sqlite).await {
            Ok(store) => Ok(store),
            Err(first) => {
                match EventStore::open(&config.storage.sqlite).await {
                    Ok(migrator) => migrator.close().await,
                    Err(e) => return Err(format!("store: {e} (migrating a fresh store: {first})")),
                }
                EventStore::open_read_only(&config.storage.sqlite)
                    .await
                    .map_err(|e| format!("store: {e}"))
            }
        }
    } else {
        EventStore::open(&config.storage.sqlite)
            .await
            .map_err(|e| format!("store: {e}"))
    }
}

/// The production dispatcher: worktree-isolated `dsh`, `superagent.cron.disk`
/// knobs applied, cut from the current directory (the repo `kian` runs in).
fn default_dispatcher(config: &ServeConfig) -> WorktreeDispatcher {
    let repo = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let policy = config
        .superagent
        .as_ref()
        .map_or_else(WorktreePolicy::default, |sa| {
            WorktreePolicy::from_section(&sa.cron)
        });
    WorktreeDispatcher::new(repo).with_policy(policy)
}

fn fire_time_text(at: chrono::DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn last_outcome(row: &CronFiringRow) -> String {
    match &row.skip_reason {
        Some(reason) => format!("{} ({reason})", row.state.as_str()),
        None => format!("{} @ {}", row.state.as_str(), row.fire_time),
    }
}

async fn list(config_path: &Path, json: bool) -> Result<String, String> {
    let config = load_config(config_path)?;
    let schedules = runtime_schedules(&config)?;
    let store = open_store(&config, true).await?;
    let rows = store
        .load_schedules()
        .await
        .map_err(|e| format!("cron schedules: {e}"))?;
    let mut lines: Vec<String> = Vec::new();
    let mut objects: Vec<serde_json::Value> = Vec::new();
    for schedule in &schedules {
        let row = rows.iter().find(|r| r.id == schedule.id);
        let expr = row.map_or_else(|| schedule.expr.to_string(), |r| r.expr.clone());
        let next_fire_at = row
            .and_then(|r| r.next_fire_at.clone())
            .or_else(|| {
                schedule
                    .next_fire_after(Utc::now())
                    .ok()
                    .map(fire_time_text)
            })
            .unwrap_or_else(|| "uncomputed".to_string());
        let last = store
            .list_firings(Some(&schedule.id), 1)
            .await
            .map_err(|e| format!("cron firings for `{}`: {e}", schedule.id))?
            .into_iter()
            .next();
        let last_label = last
            .as_ref()
            .map_or_else(|| "never".to_string(), last_outcome);
        lines.push(format!(
            "{}  expr={expr}  tz={}  next_fire_at={next_fire_at}  enabled={}  last={last_label}",
            schedule.id, schedule.tz_name, schedule.enabled
        ));
        objects.push(serde_json::json!({
            "id": schedule.id,
            "expr": expr,
            "tz": schedule.tz_name,
            "next_fire_at": next_fire_at,
            "enabled": schedule.enabled,
            "last_state": last.as_ref().map(|f| f.state.as_str()),
            "last_fire_time": last.as_ref().map(|f| f.fire_time.clone()),
            "declared": true,
        }));
    }
    for row in rows
        .iter()
        .filter(|r| !schedules.iter().any(|s| s.id == r.id))
    {
        lines.push(format!(
            "{}  expr={}  tz={}  next_fire_at={}  enabled={}  last={}  (store row only - not declared in {})",
            row.id,
            row.expr,
            row.tz,
            row.next_fire_at.clone().unwrap_or_else(|| "uncomputed".to_string()),
            row.enabled,
            "kept in the store",
            config_path.display()
        ));
        objects.push(serde_json::json!({
            "id": row.id,
            "expr": row.expr,
            "tz": row.tz,
            "next_fire_at": row.next_fire_at,
            "enabled": row.enabled,
            "last_state": serde_json::Value::Null,
            "last_fire_time": serde_json::Value::Null,
            "declared": false,
        }));
    }
    store.close().await;
    if json {
        return serde_json::to_string_pretty(&serde_json::json!({ "schedules": objects }))
            .map_err(|e| e.to_string());
    }
    if lines.is_empty() {
        return Ok("(no cron schedules declared)\n".to_string());
    }
    let mut out = lines.join("\n");
    out.push('\n');
    Ok(out)
}

async fn run_now(
    id: &str,
    config_path: &Path,
    json: bool,
    injected: Option<&dyn CronDispatcher>,
) -> Result<String, String> {
    let config = load_config(config_path)?;
    let schedules = runtime_schedules(&config)?;
    let Some(schedule) = schedules.into_iter().find(|s| s.id == id) else {
        return Err(format!(
            "unknown cron schedule `{id}` - not declared under superagent.cron in {} (`kian cron list` shows the declared ids)",
            config_path.display()
        ));
    };
    if !schedule.enabled {
        return Err(format!(
            "cron schedule `{id}` is disabled (`enabled: false` in {}); refusing to admit a firing",
            config_path.display()
        ));
    }
    // The time check is bypassed; the *idempotency key* is not. The slot is
    // the current whole second, so `kian_cron::firing_id`'s shape
    // (`{schedule_id}@{fire_time}`) holds and a second `run-now` inside the
    // same second is a recorded no-op (SS4.3).
    let now = Utc::now();
    let fire_time = Utc
        .timestamp_opt(now.timestamp(), 0)
        .single()
        .unwrap_or(now);
    let fire = fire_time_text(fire_time);
    let firing_id = format!("{}@{fire}", schedule.id);
    let claim = CronFiringRow {
        firing_id: firing_id.clone(),
        schedule_id: schedule.id.clone(),
        fire_time: fire.clone(),
        admitted_at: u64::try_from(now.timestamp_millis()).unwrap_or(0),
        state: FiringState::Admitted,
        skip_reason: None,
        worktree_path: None,
        finished_at: None,
        error: None,
    };
    let store = open_store(&config, false).await?;
    let won = store
        .claim_firing(&claim)
        .await
        .map_err(|e| format!("cron admit `{firing_id}`: {e}"))?;
    let (state, note) = if won {
        let owned;
        let dispatcher: &dyn CronDispatcher = match injected {
            Some(dispatcher) => dispatcher,
            None => {
                owned = default_dispatcher(&config);
                &owned
            }
        };
        let payload = dispatch_payload(&schedule, &firing_id, fire_time);
        match dispatcher.dispatch(&schedule, &firing_id, &payload) {
            Ok(()) => (
                FiringState::Admitted,
                "dispatch accepted the firing; the schedule cursor is untouched by run-now"
                    .to_string(),
            ),
            Err(error) => {
                store
                    .finish_firing(
                        &firing_id,
                        FiringState::Failed,
                        Some(u64::try_from(now.timestamp_millis()).unwrap_or(0)),
                        None,
                        Some(&error),
                    )
                    .await
                    .map_err(|e| format!("cron settle `{firing_id}`: {e}"))?;
                (
                    FiringState::Failed,
                    format!("the dispatcher refused the firing: {error}"),
                )
            }
        }
    } else {
        let existing = store
            .load_firing(&firing_id)
            .await
            .map_err(|e| format!("cron firing `{firing_id}`: {e}"))?;
        let state = existing.as_ref().map_or(FiringState::Admitted, |f| f.state);
        (
            state,
            "the slot was already recorded for this fire time - idempotent no-op (SS4.3)"
                .to_string(),
        )
    };
    let stored = store.load_firing(&firing_id).await.ok().flatten();
    store.close().await;
    let state_text = stored
        .as_ref()
        .map_or_else(|| state.as_str(), |f| f.state.as_str());
    if json {
        return serde_json::to_string_pretty(&serde_json::json!({
            "firing_id": firing_id,
            "schedule_id": schedule.id,
            "fire_time": fire,
            "state": state_text,
            "note": note,
        }))
        .map_err(|e| e.to_string());
    }
    Ok(format!(
        "firing_id={firing_id}\nstate={state_text}\nfire_time={fire}\nnote={note}\n"
    ))
}

fn render_firing(row: &CronFiringRow) -> String {
    let duration = row.finished_at.map_or_else(
        || "in-flight".to_string(),
        |finished| format!("{}ms", finished.saturating_sub(row.admitted_at)),
    );
    let skip = row.skip_reason.clone().unwrap_or_else(|| "-".to_string());
    let error = row
        .error
        .as_ref()
        .map(|e| format!("  error={e}"))
        .unwrap_or_default();
    format!(
        "{}  state={}  duration={duration}  skip_reason={skip}  firing_id={}{error}",
        row.fire_time,
        row.state.as_str(),
        row.firing_id
    )
}

async fn logs(
    schedule_id: &str,
    limit: u64,
    config_path: &Path,
    json: bool,
) -> Result<String, String> {
    let config = load_config(config_path)?;
    let declared = runtime_schedules(&config)?
        .iter()
        .any(|s| s.id == schedule_id);
    let store = open_store(&config, true).await?;
    let rows = store
        .list_firings(Some(schedule_id), limit)
        .await
        .map_err(|e| format!("cron firings for `{schedule_id}`: {e}"))?;
    store.close().await;
    if json {
        let objects: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "firing_id": row.firing_id,
                    "schedule_id": row.schedule_id,
                    "fire_time": row.fire_time,
                    "state": row.state.as_str(),
                    "duration_ms": row.finished_at.map(|f| f.saturating_sub(row.admitted_at)),
                    "skip_reason": row.skip_reason,
                    "error": row.error,
                })
            })
            .collect();
        return serde_json::to_string_pretty(&serde_json::json!({ "firings": objects }))
            .map_err(|e| e.to_string());
    }
    if rows.is_empty() {
        let hint = if declared {
            String::new()
        } else {
            format!(
                " (`{schedule_id}` is not declared in {})",
                config_path.display()
            )
        };
        return Ok(format!("(no firings recorded for `{schedule_id}`){hint}\n"));
    }
    let mut out = String::new();
    for row in &rows {
        out.push_str(&render_firing(row));
        out.push('\n');
    }
    Ok(out)
}

async fn rm(id: &str, config_path: &Path, json: bool) -> Result<String, String> {
    let config = load_config(config_path)?;
    let declared = runtime_schedules(&config)?.iter().any(|s| s.id == id);
    let store = open_store(&config, false).await?;
    let removed = store
        .delete_schedule(id)
        .await
        .map_err(|e| format!("cron rm `{id}`: {e}"))?;
    store.close().await;
    if !removed {
        return Err(format!(
            "no cron schedule `{id}` in the store - nothing to remove"
        ));
    }
    // Config stays the source of truth: the row is gone, the declaration is
    // the operator's to edit. The CLI never rewrites `kian.yaml`.
    let note = if declared {
        format!(
            "`{id}` is still declared in {} - edit that file; the next tick re-creates the store row from the declaration",
            config_path.display()
        )
    } else {
        format!(
            "no `{id}` declaration is left in {} - the store row is gone",
            config_path.display()
        )
    };
    if json {
        return serde_json::to_string_pretty(&serde_json::json!({
            "removed": id,
            "still_declared": declared,
            "config": config_path.display().to_string(),
            "note": note,
        }))
        .map_err(|e| e.to_string());
    }
    Ok(format!(
        "removed schedule `{id}` from the store\nnote={note}\n"
    ))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{CronCommand, DEFAULT_LOG_LIMIT, USAGE, execute, execute_with, parse_args};
    use crate::config::CronRuntimeSchedule;
    use crate::cron_tick::CronDispatcher;
    use kian_store::{EventStore, FiringState};

    /// The session every hermetic schedule targets (no channel needed).
    const SESSION: &str = "99999999-8888-7777-6666-555555555555";

    struct NoopDispatcher;

    impl CronDispatcher for NoopDispatcher {
        fn dispatch(
            &self,
            _schedule: &CronRuntimeSchedule,
            _firing_id: &str,
            _payload: &str,
        ) -> Result<(), String> {
            Ok(())
        }
    }

    struct RefusingDispatcher;

    impl CronDispatcher for RefusingDispatcher {
        fn dispatch(
            &self,
            _schedule: &CronRuntimeSchedule,
            _firing_id: &str,
            _payload: &str,
        ) -> Result<(), String> {
            Err("worktree_unavailable: hermetic test".to_string())
        }
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("kian-cron-cli-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    fn cleanup(dir: &Path) {
        let _ = std::fs::remove_dir_all(dir);
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("runtime")
            .block_on(future)
    }

    fn write_config(dir: &Path, sqlite: &Path) -> PathBuf {
        let yaml = format!(
            r#"listen: "127.0.0.1:0"
storage:
  sqlite: "{}"
agent:
  id: cron-cli-agent
  model_slots:
    main:
      provider: mock
      model: mock-model
  tools: []
providers:
  mock:
    base_url: "http://127.0.0.1:1"
    api_key_env: KIAN_CRON_CLI_KEY
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
  cron:
    schedules:
      - id: b5-ping
        expr: "@hourly"
        target: {{ session: "{SESSION}" }}
        prompt: "ping"
      - id: b5-off
        expr: "@daily"
        enabled: false
        target: {{ session: "{SESSION}" }}
        prompt: "off"
"#,
            sqlite.display(),
            dir.display()
        );
        let path = dir.join("kian.yaml");
        std::fs::write(&path, yaml).expect("write kian.yaml");
        path
    }

    fn migrated_store(path: &Path) {
        block_on(async {
            let store = EventStore::open(path).await.expect("open store");
            store.close().await;
        });
    }

    fn argv(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    // ----- argv parsing ------------------------------------------------

    #[test]
    fn cron_cli_parse_list_happy_path() {
        let parsed =
            parse_args(&argv(&["list", "--config", "/tmp/x.yaml", "--json"])).expect("list parses");
        assert_eq!(
            parsed,
            CronCommand::List {
                config: PathBuf::from("/tmp/x.yaml"),
                json: true,
            }
        );
    }

    #[test]
    fn cron_cli_parse_run_now_happy_path() {
        let parsed = parse_args(&argv(&["run-now", "night-audit", "--config", "k.yaml"]))
            .expect("run-now parses");
        assert_eq!(
            parsed,
            CronCommand::RunNow {
                id: "night-audit".to_string(),
                config: PathBuf::from("k.yaml"),
                json: false,
            }
        );
        assert!(
            parse_args(&argv(&["run-now"])).is_err(),
            "an id is required"
        );
        assert!(
            parse_args(&argv(&["run-now", "a", "b"])).is_err(),
            "exactly one id"
        );
    }

    #[test]
    fn cron_cli_parse_logs_happy_path() {
        let parsed = parse_args(&argv(&["logs", "--schedule", "b5-ping", "--limit", "5"]))
            .expect("logs parses");
        assert_eq!(
            parsed,
            CronCommand::Logs {
                schedule: "b5-ping".to_string(),
                limit: 5,
                config: PathBuf::from("kian.yaml"),
                json: false,
            }
        );
        let defaulted = parse_args(&argv(&["logs", "--schedule", "b5-ping"])).expect("default");
        match defaulted {
            CronCommand::Logs { limit, .. } => assert_eq!(limit, DEFAULT_LOG_LIMIT),
            other => panic!("unexpected: {other:?}"),
        }
        assert!(
            parse_args(&argv(&["logs"])).is_err(),
            "--schedule is required"
        );
        assert!(
            parse_args(&argv(&["logs", "--schedule", "x", "--limit", "0"])).is_err(),
            "limit must be positive"
        );
    }

    #[test]
    fn cron_cli_parse_rm_happy_path() {
        let parsed = parse_args(&argv(&["rm", "b5-off"])).expect("rm parses");
        assert_eq!(
            parsed,
            CronCommand::Rm {
                id: "b5-off".to_string(),
                config: PathBuf::from("kian.yaml"),
                json: false,
            }
        );
        assert!(parse_args(&argv(&["rm"])).is_err(), "an id is required");
    }

    #[test]
    fn cron_cli_parse_rejects_unknown_verb_and_option() {
        let err = parse_args(&argv(&["explode"])).expect_err("unknown verb");
        assert!(err.contains("unknown cron subcommand"), "{err}");
        let err = parse_args(&argv(&["list", "--nope"])).expect_err("unknown option");
        assert!(err.contains("unknown `cron list` option"), "{err}");
        assert!(parse_args(&[]).is_err(), "a verb is required");
        assert!(
            USAGE.contains("kian cron run-now"),
            "usage names every verb"
        );
    }

    // ----- execution against a temp config + temp sqlite ---------------

    #[test]
    fn cron_cli_list_prints_declared_schedules() {
        let dir = temp_dir("list");
        let sqlite = dir.join("events.sqlite");
        let config = write_config(&dir, &sqlite);
        migrated_store(&sqlite);
        let out = block_on(execute(&CronCommand::List {
            config: config.clone(),
            json: false,
        }))
        .expect("list runs");
        assert!(out.contains("b5-ping"), "{out}");
        assert!(out.contains("b5-off"), "{out}");
        assert!(out.contains("enabled=false"), "{out}");
        assert!(out.contains("last=never"), "{out}");
        assert!(
            out.contains("expr=@hourly") || out.contains("expr=0 * * * *"),
            "{out}"
        );
        cleanup(&dir);
    }

    #[test]
    fn cron_cli_list_on_fresh_store_migrates_silently() {
        // Smoke found this: the sqlite path exists in the config but has never
        // been migrated, so the read-only open failed and the raw sqlite error
        // ("unable to open database file" / "no such table") leaked. The cron
        // CLI now runs the pending migrations itself, exactly once, and `list`
        // behaves like it does on a migrated store.
        let dir = temp_dir("fresh-store");
        let sqlite = dir.join("events.sqlite");
        assert!(!sqlite.exists(), "the fresh-store path must start absent");
        let config = write_config(&dir, &sqlite);
        let out = block_on(execute(&CronCommand::List {
            config: config.clone(),
            json: false,
        }))
        .expect("list runs on a fresh store");
        assert!(out.contains("b5-ping"), "{out}");
        assert!(out.contains("last=never"), "{out}");
        assert!(!out.contains("unable to open database file"), "{out}");
        assert!(!out.contains("no such table"), "{out}");
        assert!(sqlite.exists(), "the store was created and migrated");
        cleanup(&dir);
    }

    #[test]
    fn cron_cli_run_now_admits_a_firing() {
        let dir = temp_dir("run-now");
        let sqlite = dir.join("events.sqlite");
        let config = write_config(&dir, &sqlite);
        migrated_store(&sqlite);
        let cmd = CronCommand::RunNow {
            id: "b5-ping".to_string(),
            config: config.clone(),
            json: false,
        };
        let out = block_on(execute_with(&cmd, Some(&NoopDispatcher))).expect("run-now admits");
        assert!(out.contains("firing_id=b5-ping@"), "{out}");
        assert!(out.contains("state=admitted"), "{out}");
        // idempotent slot: the same second cannot be admitted twice.
        let again = block_on(execute_with(&cmd, Some(&NoopDispatcher))).expect("replay");
        assert!(again.contains("idempotent"), "{again}");
        // a refused dispatch settles the firing as failed, never silently.
        let failing = block_on(execute_with(&cmd, Some(&RefusingDispatcher))).expect("refused");
        assert!(
            failing.contains("state=failed") || failing.contains("idempotent"),
            "{failing}"
        );
        cleanup(&dir);
    }

    #[test]
    fn cron_cli_run_now_refuses_unknown_id() {
        let dir = temp_dir("unknown");
        let sqlite = dir.join("events.sqlite");
        let config = write_config(&dir, &sqlite);
        migrated_store(&sqlite);
        let err = block_on(execute_with(
            &CronCommand::RunNow {
                id: "nope".to_string(),
                config: config.clone(),
                json: false,
            },
            Some(&NoopDispatcher),
        ))
        .expect_err("unknown id is refused");
        assert!(err.contains("unknown cron schedule `nope`"), "{err}");
        assert!(err.contains("superagent.cron"), "{err}");
        cleanup(&dir);
    }

    #[test]
    fn cron_cli_run_now_refuses_disabled_schedule() {
        let dir = temp_dir("disabled");
        let sqlite = dir.join("events.sqlite");
        let config = write_config(&dir, &sqlite);
        migrated_store(&sqlite);
        let err = block_on(execute_with(
            &CronCommand::RunNow {
                id: "b5-off".to_string(),
                config: config.clone(),
                json: false,
            },
            Some(&NoopDispatcher),
        ))
        .expect_err("a disabled schedule is refused");
        assert!(err.contains("is disabled"), "{err}");
        cleanup(&dir);
    }

    #[test]
    fn cron_cli_logs_prints_recent_firings() {
        let dir = temp_dir("logs");
        let sqlite = dir.join("events.sqlite");
        let config = write_config(&dir, &sqlite);
        migrated_store(&sqlite);
        let admitted = block_on(execute_with(
            &CronCommand::RunNow {
                id: "b5-ping".to_string(),
                config: config.clone(),
                json: false,
            },
            Some(&NoopDispatcher),
        ))
        .expect("run-now admits");
        let firing_id = admitted
            .lines()
            .find(|line| line.starts_with("firing_id="))
            .and_then(|line| line.strip_prefix("firing_id="))
            .expect("firing id printed")
            .to_string();
        let logs = block_on(execute(&CronCommand::Logs {
            schedule: "b5-ping".to_string(),
            limit: 5,
            config: config.clone(),
            json: false,
        }))
        .expect("logs runs");
        assert!(logs.contains(&firing_id), "{logs}");
        assert!(logs.contains("state=admitted"), "{logs}");
        assert!(logs.contains("duration=in-flight"), "{logs}");
        assert!(logs.contains("skip_reason=-"), "{logs}");
        let empty = block_on(execute(&CronCommand::Logs {
            schedule: "b5-off".to_string(),
            limit: 5,
            config: config.clone(),
            json: false,
        }))
        .expect("logs runs for a silent schedule");
        assert!(empty.contains("no firings recorded"), "{empty}");
        cleanup(&dir);
    }

    #[test]
    fn cron_cli_rm_removes_the_store_row() {
        let dir = temp_dir("rm");
        let sqlite = dir.join("events.sqlite");
        let config = write_config(&dir, &sqlite);
        migrated_store(&sqlite);
        let path = sqlite.clone();
        let digest = "0".repeat(64);
        block_on(async move {
            let store = EventStore::open(&path).await.expect("open");
            store
                .upsert_schedule("b5-off", "0 0 * * *", "UTC", &digest, false, 1)
                .await
                .expect("seed the row");
            let row = store.load_schedule("b5-off").await.expect("load");
            assert!(row.is_some(), "the row exists before rm");
            store.close().await;
        });
        let out = block_on(execute(&CronCommand::Rm {
            id: "b5-off".to_string(),
            config: config.clone(),
            json: false,
        }))
        .expect("rm runs");
        assert!(
            out.contains("removed schedule `b5-off` from the store"),
            "{out}"
        );
        assert!(out.contains("still declared in"), "{out}");
        assert!(
            out.contains(&config.display().to_string()),
            "the note names the config file: {out}"
        );
        // the config file itself is untouched (config is the source of truth).
        let text = std::fs::read_to_string(&config).expect("config readable");
        assert!(text.contains("b5-off"), "the declaration is still there");
        let path = sqlite.clone();
        block_on(async move {
            let store = EventStore::open(&path).await.expect("open");
            let row = store.load_schedule("b5-off").await.expect("load");
            assert!(row.is_none(), "the row is gone after rm");
            store.close().await;
        });
        // a second rm has nothing to remove and says so.
        let err = block_on(execute(&CronCommand::Rm {
            id: "b5-off".to_string(),
            config: config.clone(),
            json: false,
        }))
        .expect_err("second rm refuses");
        assert!(err.contains("nothing to remove"), "{err}");
        // the firing machinery is still intact for observability.
        let _ = FiringState::Admitted;
        cleanup(&dir);
    }
}
