//! Worktree-backed cron execution (CRON-MODEL §6) — the second
//! [`CronDispatcher`] implementation.
//!
//! [`cron_tick`](crate::cron_tick) owns the *decision* half of the scheduler
//! (decide, claim, advance); this module owns the *execution* half for the
//! isolated case: one fresh detached git worktree per firing, the scheduled
//! payload run as a turn inside it, the turn's output and the worktree's diff
//! captured onto the firing record, and the worktree removed again.
//!
//! The rules this module encodes, and where they come from:
//!
//! - **create → run → capture → cleanup** (§6.2), with the path
//!   `<root>/<schedule_id>/<YYYYMMDDTHHMMSSZ>-<hash8>` where `root` defaults
//!   to `<git-common-dir>/kian-worktrees` — the directory git itself reports
//!   (`rev-parse --git-common-dir`, falling back to `--git-dir`), because in a
//!   linked worktree `<repo>/.git` is an ASCII gitfile, not a directory, so
//!   the trees live inside the shared git dir (invisible to `git status`)
//!   and `hash8` is the first 8 hex chars of
//!   sha256(`firing_id`) (§6.3). The worktree is **detached**: this module
//!   never creates a branch, never commits and never pushes;
//! - **no fallback** (§6.5): if the worktree cannot be created the firing is
//!   `worktree_unavailable` with the git stderr and the payload is *not* run
//!   in the live checkout — isolation is the entire point;
//! - **a fatal turn is recorded, not swallowed** (§7): a non-zero exit is
//!   `failed`, a per-firing timeout is `timeout` and, when the runner kills
//!   it, the child's whole process group dies (§6.1);
//! - **disk is bounded** (§6.4): [`WorktreeDispatcher::gc`] evicts the oldest
//!   *non-running* worktrees until `disk_cap_mb` fits again and drops
//!   preserved trees older than `retain_hours`. It runs on the tick loop —
//!   [`WorktreeDispatcher::dispatch`] calls it every firing — deliberately not
//!   as its own cron schedule;
//! - **concurrency** (§5.4): a slot that would exceed the schedule's
//!   `max_concurrent` is recorded as skipped with the `skipped_overlap` token
//!   and never runs.
//!
//! The turn itself is behind the [`TurnRunner`] trait so the lifecycle is
//! testable with no `dsh` process, no network and no tokio runtime.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime};

use chrono::{DateTime, TimeDelta, Utc};
use sha2::{Digest, Sha256};

use crate::config::{CronHarnessSection, CronHarnessSettings, CronRuntimeSchedule};
use crate::cron_tick::{CronDispatcher, DispatchOutcome, FiringState};

/// Default retention for *preserved* worktrees (§6.4), hours.
pub const DEFAULT_RETAIN_HOURS: u64 = 72;
/// Default cap on the captured `git diff` (§6.2), KiB.
pub const DEFAULT_MAX_DIFF_KB: u64 = 512;
/// Default total cap across *all* cron worktrees (CRON-MODEL §6.4), MiB.
pub const DEFAULT_DISK_CAP_MB: u64 = 20480;
/// Default free-space preflight floor (§6.4), MiB (80 GiB, sized for Rust build
/// trees) — the effective floor is the larger of this and
/// [`DEFAULT_MIN_FREE_PERCENT`] (25%) of the filesystem.
pub const DEFAULT_MIN_FREE_MIB: u64 = 81920;
/// Default free-space preflight floor, percent of the filesystem (§6.4).
pub const DEFAULT_MIN_FREE_PERCENT: u64 = 25;
/// Default cap on a *single* worktree (§6.4), MiB.
pub const DEFAULT_MAX_WORKTREE_MIB: u64 = 2048;
/// Default bound on each stored stdout/stderr/diff of a firing (§6.2), KiB.
pub const DEFAULT_MAX_OUTPUT_KIB: u64 = 256;
/// The tick floor §6.4 ties housekeeping to, seconds — GC runs *with* the
/// tick, never as a cron schedule of its own.
pub const GC_INTERVAL_SECS: u64 = 15;

/// The `worktree:` policy of a schedule (§3.1/§6), resolved with §6's
/// defaults. `CronRuntimeSchedule` carries no policy of its own yet, so the
/// dispatcher owns the resolved copy and the future config plumbing feeds it
/// here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreePolicy {
    /// `worktree.root`; `None` means `<repo>/.git/kian-worktrees` (§6.3).
    pub root: Option<PathBuf>,
    /// `worktree.base`; `None` means `HEAD` of `worktree.repo` (§6.2).
    pub base: Option<String>,
    /// Leave a failed firing's worktree behind for inspection (§6.2).
    pub keep_on_failure: bool,
    /// Bound on the total size of `root`, MiB (§6.4).
    pub disk_cap_mb: u64,
    /// Age at which a preserved worktree is GC'd, hours (§6.4).
    pub retain_hours: u64,
    /// Cap on the captured diff, KiB (§6.2).
    pub max_diff_kb: u64,
    /// Free-space preflight floor (§6.4), MiB.
    pub min_free_mib: u64,
    /// Free-space preflight floor as a percent of the filesystem (§6.4).
    pub min_free_percent: u64,
    /// Cap on a single worktree (§6.4), MiB.
    pub max_worktree_mib: u64,
    /// Bound on each stored stdout/stderr/diff of a firing (§6.2), KiB.
    pub max_output_kib: u64,
}

impl Default for WorktreePolicy {
    fn default() -> Self {
        Self {
            root: None,
            base: None,
            keep_on_failure: true,
            disk_cap_mb: DEFAULT_DISK_CAP_MB,
            retain_hours: DEFAULT_RETAIN_HOURS,
            max_diff_kb: DEFAULT_MAX_DIFF_KB,
            min_free_mib: DEFAULT_MIN_FREE_MIB,
            min_free_percent: DEFAULT_MIN_FREE_PERCENT,
            max_worktree_mib: DEFAULT_MAX_WORKTREE_MIB,
            max_output_kib: DEFAULT_MAX_OUTPUT_KIB,
        }
    }
}

impl WorktreePolicy {
    /// The disk cap in bytes.
    pub fn disk_cap_bytes(&self) -> u64 {
        self.disk_cap_mb.saturating_mul(1024 * 1024)
    }

    /// The diff cap in bytes.
    pub fn max_diff_bytes(&self) -> usize {
        self.max_diff_kb.saturating_mul(1024) as usize
    }
}

/// The terminal outcome of one worktree-backed firing — the §7 `outcome`
/// column, and its stable machine token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FiringOutcome {
    /// The turn ran and exited zero.
    Ok,
    /// The turn ran and failed (non-zero exit, spawn error, panic).
    Failed,
    /// The per-firing ceiling expired; the process group was killed.
    Timeout,
    /// The slot was dropped for concurrency, before any worktree existed.
    SkippedOverlap,
    /// The worktree could not be created; nothing ran (§6.5).
    WorktreeUnavailable,
    /// The slot was dropped by the §6.4 disk guards: free space below the
    /// floor, or the total cap still exceeded after GC. Nothing ran.
    SkippedDiskPressure,
}

impl FiringOutcome {
    /// The `last_outcome` token of §4.1/§7.
    pub fn as_str(self) -> &'static str {
        match self {
            FiringOutcome::Ok => "ok",
            FiringOutcome::Failed => "failed",
            FiringOutcome::Timeout => "timeout",
            FiringOutcome::SkippedOverlap => "skipped_overlap",
            FiringOutcome::WorktreeUnavailable => "worktree_unavailable",
            FiringOutcome::SkippedDiskPressure => "skipped_disk_pressure",
        }
    }
}

/// One firing's record as the dispatcher holds it: the §7 columns the tick
/// loop's store does not carry yet (`worktree_path`, `diff_stat`, `diff`,
/// the child's tails), plus the housekeeping decision taken on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorktreeFiring {
    /// The slot key, `{schedule_id}@{fire_time}`.
    pub firing_id: String,
    /// The owning schedule.
    pub schedule_id: String,
    /// Where the turn ran, when a worktree was created.
    pub worktree_path: Option<PathBuf>,
    /// The §7 outcome token.
    pub outcome: FiringOutcome,
    /// The turn's exit code, when it ran to completion.
    pub exit_code: Option<i32>,
    /// Wall time of the turn, milliseconds.
    pub duration_ms: u64,
    /// Epoch ms the firing was recorded (the settlement's `finished_at`).
    pub finished_at_ms: u64,
    /// Tail of the turn's stdout.
    pub stdout_tail: String,
    /// Tail of the turn's stderr.
    pub stderr_tail: String,
    /// `git status --porcelain` of the worktree.
    pub status: String,
    /// `git diff --stat` of the worktree.
    pub diff_stat: String,
    /// The worktree's `git diff`, truncated at `max_diff_kb` (§6.2).
    pub diff: String,
    /// Whether the worktree was deliberately left behind for inspection.
    pub kept: bool,
    /// Bytes freed by the §6.2 post-run cleanup (the worktree `target/`
    /// removal), 0 when there was nothing to clean.
    pub freed_bytes: u64,
    /// The failure text: git stderr for `worktree_unavailable`, the timeout
    /// line for `timeout`, the spawn error for a failed run.
    pub error: Option<String>,
    /// Non-fatal notes: capture errors, cleanup errors (§6.2 — recorded,
    /// never fatal).
    pub notes: Vec<String>,
    /// Explicit truncation markers (§6.2): one entry per stored field the
    /// `max_output_kib` bound clipped, naming the field and both sizes.
    pub truncation_markers: Vec<String>,
}

impl WorktreeFiring {
    fn new(schedule_id: &str, firing_id: &str, outcome: FiringOutcome) -> Self {
        Self {
            firing_id: firing_id.to_owned(),
            schedule_id: schedule_id.to_owned(),
            worktree_path: None,
            outcome,
            exit_code: None,
            duration_ms: 0,
            finished_at_ms: 0,
            stdout_tail: String::new(),
            stderr_tail: String::new(),
            status: String::new(),
            diff_stat: String::new(),
            diff: String::new(),
            kept: false,
            freed_bytes: 0,
            error: None,
            notes: Vec::new(),
            truncation_markers: Vec::new(),
        }
    }
}

/// One turn request: where to run, what to run, how long it may take.
#[derive(Debug, Clone)]
pub struct TurnRequest {
    /// The fresh worktree — the turn's working directory, never the live
    /// checkout (§6.1).
    pub worktree: PathBuf,
    /// The scheduled payload as an opening user message (§3.1 `prompt`).
    pub prompt: String,
    /// The per-firing ceiling, from the schedule's `timeout_ms`.
    pub timeout: Duration,
    /// The owning schedule.
    pub schedule_id: String,
    /// The slot key.
    pub firing_id: String,
    /// The full dispatch payload JSON, for a harness that wants the routing
    /// fields too.
    pub payload: String,
    /// Cap on this run's worktree, bytes (§6.4): the runner kills the whole
    /// process group once the tree outgrows it. `None` = uncapped.
    pub max_worktree_bytes: Option<u64>,
}

/// What a turn produced, as the lifecycle needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnOutput {
    /// Captured stdout.
    pub stdout: String,
    /// Captured stderr.
    pub stderr: String,
    /// The exit code, `None` when the turn was killed.
    pub exit_code: Option<i32>,
    /// Set when the ceiling expired and the process group was killed.
    pub timed_out: bool,
    /// Set when the runner killed the turn for outgrowing the §6.4
    /// per-worktree cap — a `failed`/`worktree_too_large` firing, not a
    /// timeout.
    pub too_large: bool,
}

/// The injected turn executor: `dsh` in production, a closure in tests.
pub trait TurnRunner: Send + Sync {
    /// Run one turn. `Err` means the harness could not be started at all —
    /// an ordinary failed firing, recorded and never fatal to the tick.
    fn run(&self, request: &TurnRequest) -> Result<TurnOutput, String>;
}

/// The production runner: spawn the `dsh` harness in the worktree, argv-direct
/// (no shell), killing the whole process group on the per-firing ceiling.
#[derive(Debug, Clone)]
pub struct DshTurnRunner {
    /// The harness binary: `superagent.cron.harness.command`, else
    /// `KIAN_DSH_BIN`, else `dsh` on `PATH`.
    pub program: String,
    /// Fixed leading arguments (appended before the profile flags).
    pub args: Vec<String>,
    /// The dsh profile: argv becomes `args --profile <name> <payload>`, the
    /// documented `dsh [--profile] <name> [options] [app-args...]` shape whose
    /// one-shot example is `dsh headless "run the tests"`. `None` is the
    /// raw-command escape hatch (argv is `args <payload>`, no profile flag) and
    /// is only for a command that is not the `dsh` launcher.
    pub profile: Option<String>,
    /// Extra environment for the harness child (peer/model/session selects and
    /// credential references). Values are never logged nor put on a firing.
    pub env: BTreeMap<String, String>,
}

impl Default for DshTurnRunner {
    fn default() -> Self {
        Self::from_section(&CronHarnessSection::default())
    }
}

impl DshTurnRunner {
    /// A runner for the default `dsh` binary and the default profile.
    pub fn new() -> Self {
        Self::default()
    }

    /// The runner `superagent.cron.harness` resolves to: the configured binary
    /// (or `KIAN_DSH_BIN`, or `dsh`), the configured profile (default
    /// `headless`) and the configured child environment.
    pub fn from_section(section: &CronHarnessSection) -> Self {
        let settings: CronHarnessSettings = section.resolved();
        Self {
            program: settings.command,
            args: Vec::new(),
            profile: Some(settings.profile),
            env: settings.env,
        }
    }

    /// The documented argv: `dsh [--profile] <name> [options] [app-args...]`,
    /// with the scheduled payload delivered as the app's one positional task
    /// argument (dsh never reads the task from stdin).
    pub fn harness_argv(&self, prompt: &str) -> Vec<String> {
        let mut argv = self.args.clone();
        if let Some(profile) = &self.profile {
            argv.push("--profile".to_owned());
            argv.push(profile.clone());
        }
        argv.push(prompt.to_owned());
        argv
    }
}

impl TurnRunner for DshTurnRunner {
    fn run(&self, request: &TurnRequest) -> Result<TurnOutput, String> {
        // The child writes to files, not pipes: a polling timeout would
        // deadlock on a full 64 KiB pipe buffer otherwise.
        // Unique per invocation: two turns with the same firing id (a
        // concurrent re-run, or two tests in one process) must never share a
        // scratch dir, or one truncates the other's captured output.
        static SCRATCH_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let scratch = std::env::temp_dir().join(format!(
            "kian-cron-turn-{}-{}-{}",
            std::process::id(),
            hash8(&request.firing_id),
            SCRATCH_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::create_dir_all(&scratch).map_err(|e| format!("{}: {e}", scratch.display()))?;
        let stdout_path = scratch.join("stdout");
        let stderr_path = scratch.join("stderr");
        let stdout = fs::File::create(&stdout_path).map_err(|e| e.to_string())?;
        let stderr = fs::File::create(&stderr_path).map_err(|e| e.to_string())?;

        let mut command = Command::new(&self.program);
        command
            .args(self.harness_argv(&request.prompt))
            .envs(
                self.env
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str())),
            )
            // stdin is /dev/null on purpose: the payload is the app's positional
            // task argument, never stdin (dsh reads no task from it).
            // §6.2: the per-firing build tree lives *inside* the worktree, so
            // post-run cleanup deletes it with one `remove_dir_all` instead of
            // hunting a shared target dir across the host.
            .env("CARGO_TARGET_DIR", request.worktree.join("target"))
            .current_dir(&request.worktree)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        // Its own process group, so the timeout can kill descendants too.
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let mut child = command
            .spawn()
            .map_err(|error| format!("cannot spawn {}: {error}", self.program))?;
        let pid = child.id();

        let deadline = Instant::now() + request.timeout;
        let mut timed_out = false;
        let mut ticks: u32 = 0;
        let mut too_large = false;
        let mut exit_code = None;
        loop {
            match child.try_wait() {
                Ok(Some(status)) => {
                    exit_code = status.code();
                    break;
                }
                Ok(None) if Instant::now() >= deadline => {
                    timed_out = true;
                    kill_process_group(pid);
                    let _ = child.kill();
                    let _ = child.wait();
                    break;
                }
                Ok(None) => {
                    ticks = ticks.wrapping_add(1);
                    if ticks.is_multiple_of(20)
                        && size_cap_exceeded(&request.worktree, request.max_worktree_bytes)
                    {
                        too_large = true;
                        kill_process_group(pid);
                        let _ = child.kill();
                        let _ = child.wait();
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(25));
                }
                Err(error) => {
                    kill_process_group(pid);
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!("waiting for {}: {error}", self.program));
                }
            }
        }

        let read = |path: &Path| fs::read_to_string(path).unwrap_or_default();
        let output = TurnOutput {
            stdout: read(&stdout_path),
            stderr: read(&stderr_path),
            exit_code,
            too_large,
            timed_out,
        };
        let _ = fs::remove_dir_all(&scratch);
        Ok(output)
    }
}

/// Kill a whole process group (§6.1: a timeout kills the children too).
#[cfg(unix)]
fn kill_process_group(pid: u32) {
    // The child was spawned with `process_group(0)`, so its pgid is its pid;
    // a negative target signals the group.
    let _ = Command::new("kill")
        .arg("-KILL")
        .arg(format!("-{pid}"))
        .status();
}

/// Non-unix hosts have no process groups; the child alone is killed.
#[cfg(not(unix))]
fn kill_process_group(_pid: u32) {}

/// What one GC pass did (§6.4), for the tick loop's log/metrics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Worktrees evicted this pass, oldest first.
    pub evicted: Vec<PathBuf>,
    /// Worktrees kept because they are fresh and inside the cap.
    pub retained: Vec<PathBuf>,
    /// Worktrees skipped because a firing is running in them.
    pub running: Vec<PathBuf>,
    /// Total bytes under the root before the pass.
    pub bytes_before: u64,
    /// Total bytes under the root after the pass.
    pub bytes_after: u64,
    /// `true` when the cap is still exceeded after evicting what it could —
    /// the next create must fail with `worktree_unavailable`/`disk_cap`.
    pub refused: bool,
    /// Eviction failures, recorded and never fatal (§6.2).
    pub errors: Vec<String>,
}

/// The second [`CronDispatcher`]: one detached git worktree per firing, the
/// payload run inside it, then capture and cleanup.
///
/// `dispatch` is **synchronous**: it blocks for the length of the turn (up to
/// the schedule's `timeout_ms`) so that the firing record it fills in is
/// complete when it returns. The tick loop already runs on a dedicated
/// thread; a caller that must not block (the CLI's `run-now`) should wrap it
/// in `spawn_blocking`.
pub struct WorktreeDispatcher {
    repo: PathBuf,
    policy: WorktreePolicy,
    runner: Box<dyn TurnRunner>,
    ledger: Mutex<Vec<WorktreeFiring>>,
    running: Arc<Mutex<BTreeMap<String, u32>>>,
    active: Arc<Mutex<BTreeSet<PathBuf>>>,
    last_gc: Mutex<Option<Instant>>,
    /// The injected §6.4 free-space probe (preflight).
    free_space: FreeSpaceProbe,
    /// The injected §6.4 byte-count probe (per-worktree cap).
    size_probe: DirSizeProbe,
}

impl WorktreeDispatcher {
    /// A dispatcher over `repo` with §6's default policy and the real `dsh`
    /// runner.
    pub fn new(repo: impl Into<PathBuf>) -> Self {
        Self::with_parts(
            repo.into(),
            WorktreePolicy::default(),
            Box::new(DshTurnRunner::new()),
        )
    }

    fn with_parts(repo: PathBuf, policy: WorktreePolicy, runner: Box<dyn TurnRunner>) -> Self {
        Self {
            repo,
            policy,
            runner,
            ledger: Mutex::new(Vec::new()),
            running: Arc::new(Mutex::new(BTreeMap::new())),
            active: Arc::new(Mutex::new(BTreeSet::new())),
            last_gc: Mutex::new(None),
            free_space: Arc::new(df_free_space),
            size_probe: Arc::new(dir_size),
        }
    }

    /// Replace the resolved worktree policy.
    pub fn with_policy(mut self, policy: WorktreePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Install the runner `superagent.cron.harness` resolves to (profile,
    /// binary and child environment included).
    pub fn with_harness(mut self, section: &CronHarnessSection) -> Self {
        self.runner = Box::new(DshTurnRunner::from_section(section));
        self
    }

    /// Inject the turn runner (a closure in tests, `dsh` in production).
    pub fn with_runner(mut self, runner: Box<dyn TurnRunner>) -> Self {
        self.runner = runner;
        self
    }

    /// The resolved `worktree:` policy.
    pub fn policy(&self) -> &WorktreePolicy {
        &self.policy
    }

    /// The repository the worktrees are cut from.
    pub fn repo(&self) -> &Path {
        &self.repo
    }

    /// The worktree root: `worktree.root`, defaulting to
    /// `<git-common-dir>/kian-worktrees` (§6.3).
    ///
    /// Never panics and never fails: an unresolvable git dir falls back to the
    /// legacy `<repo>/.git/kian-worktrees` so GC and bookkeeping stay total; the
    /// *create* path uses [`Self::try_worktree_root`] and records the detailed
    /// `worktree_unavailable` failure instead.
    pub fn worktree_root(&self) -> PathBuf {
        self.try_worktree_root()
            .unwrap_or_else(|_| self.repo.join(".git").join("kian-worktrees"))
    }

    /// The §6.3 root, resolved by git itself.
    ///
    /// Rule: `worktree.root` when configured, otherwise ask git for the
    /// directory that owns the worktree registry —
    /// `rev-parse --path-format=absolute --git-common-dir`, which is the shared
    /// `<primary>/.git` even when `repo` is a linked worktree whose own `.git`
    /// is an ASCII gitfile. `--git-dir` is the fallback for gits without
    /// `--git-common-dir`/`--path-format` (older than 2.31) and for
    /// separate-git-dir layouts. A relative answer is made absolute against
    /// `repo`, and the result is canonicalised so symlinked checkouts
    /// (`/tmp` → `/private/tmp`) agree on one path; the trees always go in
    /// `<that dir>/kian-worktrees`, whether that dir is the repo's own `.git`
    /// or a common dir living elsewhere. Every attempt's failure is carried in
    /// the error text.
    pub fn try_worktree_root(&self) -> Result<PathBuf, String> {
        if let Some(root) = &self.policy.root {
            return Ok(root.clone());
        }
        let mut attempts: Vec<String> = Vec::new();
        for (flag, absolute) in [
            ("--git-common-dir", true),
            ("--git-common-dir", false),
            ("--git-dir", true),
            ("--git-dir", false),
        ] {
            let mut args = vec!["rev-parse"];
            if absolute {
                args.push("--path-format=absolute");
            }
            args.push(flag);
            let out = match run_git(&self.repo, &args) {
                Ok(out) => out,
                Err(error) => {
                    attempts.push(error);
                    continue;
                }
            };
            let raw = out.trim();
            if raw.is_empty() {
                attempts.push(format!("git {}: empty output", args.join(" ")));
                continue;
            }
            let mut dir = PathBuf::from(raw);
            if !dir.is_absolute() {
                dir = self.repo.join(dir);
            }
            match dir.canonicalize() {
                Ok(canonical) if canonical.is_dir() => {
                    return Ok(canonical.join("kian-worktrees"));
                }
                Ok(canonical) => attempts.push(format!(
                    "git {}: {} is not a directory",
                    args.join(" "),
                    canonical.display()
                )),
                Err(error) => attempts.push(format!(
                    "git {}: {}: {error}",
                    args.join(" "),
                    dir.display()
                )),
            }
        }
        Err(format!(
            "{}: cannot resolve the git dir ({})",
            self.repo.display(),
            attempts.join("; ")
        ))
    }

    /// [`Self::worktree_path`], but the root resolution error is returned
    /// instead of being flattened into the fallback.
    pub fn try_worktree_path(
        &self,
        schedule_id: &str,
        firing_id: &str,
        fire_time: DateTime<Utc>,
    ) -> Result<PathBuf, String> {
        let stamp = fire_time.format("%Y%m%dT%H%M%SZ").to_string();
        Ok(self
            .try_worktree_root()?
            .join(schedule_id)
            .join(format!("{stamp}-{}", hash8(firing_id))))
    }

    /// The §6.3 path of one firing's worktree.
    pub fn worktree_path(
        &self,
        schedule_id: &str,
        firing_id: &str,
        fire_time: DateTime<Utc>,
    ) -> PathBuf {
        let stamp = fire_time.format("%Y%m%dT%H%M%SZ").to_string();
        self.worktree_root()
            .join(schedule_id)
            .join(format!("{stamp}-{}", hash8(firing_id)))
    }

    /// Every firing this dispatcher has handled, in dispatch order.
    pub fn firings(&self) -> Vec<WorktreeFiring> {
        lock(&self.ledger).clone()
    }

    /// One firing's record, by slot key.
    pub fn firing(&self, firing_id: &str) -> Option<WorktreeFiring> {
        lock(&self.ledger)
            .iter()
            .rev()
            .find(|f| f.firing_id == firing_id)
            .cloned()
    }

    /// How many firings of `schedule_id` are running right now (§5.4).
    pub fn running(&self, schedule_id: &str) -> u32 {
        lock(&self.running).get(schedule_id).copied().unwrap_or(0)
    }

    /// Take one concurrency slot for `schedule_id`, or `None` when the
    /// ceiling is already reached — the caller records `skipped_overlap`
    /// (§5.4).
    pub fn try_lease(&self, schedule_id: &str, max_concurrent: u32) -> Option<SlotLease> {
        let ceiling = max_concurrent.max(1);
        let mut running = lock(&self.running);
        let current = running.get(schedule_id).copied().unwrap_or(0);
        if current >= ceiling {
            return None;
        }
        running.insert(schedule_id.to_owned(), current + 1);
        Some(SlotLease {
            schedule_id: schedule_id.to_owned(),
            running: Arc::clone(&self.running),
        })
    }

    /// One GC pass at `now` (§6.4): evict the oldest non-running worktrees
    /// until the cap fits, and every preserved worktree older than
    /// `retain_hours`.
    pub fn gc(&self, now: DateTime<Utc>) -> GcReport {
        self.gc_with_incoming(now, 0)
    }

    /// [`Self::gc`], reserving `incoming_bytes` for the worktree a firing is
    /// about to create (§6.4 measures *before* the create).
    pub fn gc_with_incoming(&self, now: DateTime<Utc>, incoming_bytes: u64) -> GcReport {
        let root = self.worktree_root();
        let active = lock(&self.active).clone();
        let cap = self.policy.disk_cap_bytes();
        let retain =
            TimeDelta::hours(i64::try_from(self.policy.retain_hours).unwrap_or(i64::MAX / 3600));

        let mut entries = list_worktrees(&root);
        let mut report = GcReport {
            bytes_before: entries.iter().map(|e| e.bytes).sum(),
            ..GcReport::default()
        };
        entries.retain(|entry| {
            if active.contains(&entry.path) {
                report.running.push(entry.path.clone());
                return false;
            }
            true
        });
        // Oldest first; the path tiebreak keeps eviction deterministic when
        // two worktrees were created inside the same filesystem tick.
        entries.sort_by(|a, b| {
            a.modified
                .cmp(&b.modified)
                .then_with(|| a.path.cmp(&b.path))
        });

        let mut total = report.bytes_before;
        let mut evicted_any = false;
        for entry in entries {
            let age = now.signed_duration_since(DateTime::<Utc>::from(entry.modified));
            let over_cap = total.saturating_add(incoming_bytes) > cap;
            let stale = age >= retain;
            if !over_cap && !stale {
                report.retained.push(entry.path.clone());
                continue;
            }
            match self.remove_worktree(&entry.path) {
                Ok(()) => {
                    total = total.saturating_sub(entry.bytes);
                    evicted_any = true;
                    report.evicted.push(entry.path.clone());
                }
                Err(error) => report.errors.push(error),
            }
        }
        if evicted_any {
            // Drop the administrative entries of the trees just removed.
            if let Err(error) = run_git(&self.repo, &["worktree", "prune"]) {
                report.errors.push(error);
            }
        }
        report.bytes_after = dir_size(&root);
        report.refused = report.bytes_after.saturating_add(incoming_bytes) > cap;
        *lock(&self.last_gc) = Some(Instant::now());
        report
    }

    /// The tick-loop housekeeping hook (§6.4): GC runs *on the tick*, never as
    /// its own cron schedule. Rate-limited to [`GC_INTERVAL_SECS`] so a fast
    /// tick loop does not re-walk the root every 100 ms.
    pub fn gc_if_due(&self, now: DateTime<Utc>) -> Option<GcReport> {
        {
            let last = lock(&self.last_gc);
            if matches!(*last, Some(at) if at.elapsed() < Duration::from_secs(GC_INTERVAL_SECS)) {
                return None;
            }
        }
        Some(self.gc(now))
    }

    fn record(&self, mut firing: WorktreeFiring) {
        firing.finished_at_ms = now_ms();
        lock(&self.ledger).push(firing);
    }

    fn ensure_capacity(&self, incoming_bytes: u64) -> Result<(), String> {
        let root = self.worktree_root();
        let total = dir_size(&root);
        let cap = self.policy.disk_cap_bytes();
        if total.saturating_add(incoming_bytes) > cap {
            return Err(format!(
                "disk_cap: {} + {} bytes would exceed the cap of {} bytes in {}",
                total,
                incoming_bytes,
                cap,
                root.display()
            ));
        }
        Ok(())
    }

    /// `git worktree add --detach` (§6.2 create). Any failure here is
    /// `worktree_unavailable` and nothing runs (§6.5).
    fn create_worktree(&self, path: &Path) -> Result<(), String> {
        run_git(&self.repo, &["rev-parse", "--git-dir"]).map_err(|error| {
            format!("{} is not a git repository ({error})", self.repo.display())
        })?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| format!("{}: {error}", parent.display()))?;
        }
        let base = self
            .policy
            .base
            .clone()
            .unwrap_or_else(|| "HEAD".to_owned());
        let path_str = path
            .to_str()
            .ok_or_else(|| format!("{}: path is not UTF-8", path.display()))?;
        run_git(
            &self.repo,
            &["worktree", "add", "--detach", path_str, &base],
        )?;
        lock(&self.active).insert(path.to_path_buf());
        Ok(())
    }

    /// §6.2 capture: status, diff stat and the (capped) diff. Capture errors
    /// are noted, never fatal to a turn that already ran.
    fn capture(&self, path: &Path, notes: &mut Vec<String>) -> (String, String, String) {
        let status = match run_git(path, &["status", "--porcelain"]) {
            Ok(out) => out,
            Err(error) => {
                notes.push(format!("capture status: {error}"));
                String::new()
            }
        };
        let diff_stat = match run_git(path, &["diff", "--stat"]) {
            Ok(out) => out,
            Err(error) => {
                notes.push(format!("capture diff --stat: {error}"));
                String::new()
            }
        };
        let diff = match run_git(path, &["diff"]) {
            Ok(out) => truncate(out, self.policy.max_diff_bytes()),
            Err(error) => {
                notes.push(format!("capture diff: {error}"));
                String::new()
            }
        };
        (status, diff_stat, diff)
    }

    /// §6.2 post-run cleanup: `cargo clean` semantics without a `cargo clean`
    /// subprocess — remove the worktree `target/` (where `CARGO_TARGET_DIR`
    /// pinned the per-firing build tree) and report the bytes freed. Rust build
    /// trees are GiB-sized, so this runs *before* `git worktree remove`: git
    /// never has to walk them, and a kept worktree stays small.
    fn clean_target_dir(&self, path: &Path) -> Result<u64, String> {
        let target = path.join("target");
        let Ok(meta) = fs::symlink_metadata(&target) else {
            return Ok(0);
        };
        let freed = if meta.is_dir() {
            (self.size_probe)(&target)
        } else {
            meta.len()
        };
        let result = if meta.is_dir() {
            fs::remove_dir_all(&target)
        } else {
            fs::remove_file(&target)
        };
        result.map_err(|error| format!("{}: {error}", target.display()))?;
        Ok(freed)
    }

    /// `git worktree remove --force` (§6.2 cleanup). Failure leaves the
    /// directory for GC and never fails the firing.
    fn remove_worktree(&self, path: &Path) -> Result<(), String> {
        let path_str = path
            .to_str()
            .ok_or_else(|| format!("{}: path is not UTF-8", path.display()))?;
        if run_git(&self.repo, &["worktree", "remove", "--force", path_str]).is_err() {
            // Not a registered worktree (or git refused): take the directory.
            fs::remove_dir_all(path).map_err(|error| format!("{}: {error}", path.display()))?;
        }
        // §6.2: drop the stale admin entry so `git worktree list` is clean.
        let _ = run_git(&self.repo, &["worktree", "prune"]);
        lock(&self.active).remove(path);
        Ok(())
    }
}

impl CronDispatcher for WorktreeDispatcher {
    fn dispatch(
        &self,
        schedule: &CronRuntimeSchedule,
        firing_id: &str,
        payload: &str,
    ) -> Result<(), String> {
        let now = Utc::now();
        // §6.4: housekeeping rides the tick that dispatches, not a schedule.
        let _ = self.gc_if_due(now);

        let fire_time = fire_time_from_payload(payload).unwrap_or(now);
        // §6.3 + §6.5: the root comes from git, and a root that cannot be
        // resolved fails the firing here — recorded, never a panic and never a
        // silent fallback to the live checkout.
        let path = match self.try_worktree_path(&schedule.id, firing_id, fire_time) {
            Ok(path) => path,
            Err(error) => {
                let mut record = WorktreeFiring::new(
                    &schedule.id,
                    firing_id,
                    FiringOutcome::WorktreeUnavailable,
                );
                record.error = Some(error);
                let message = record.error.clone().unwrap_or_default();
                self.record(record);
                return Err(format!("worktree_unavailable: {message}"));
            }
        };

        // §5.4: the ceiling is per schedule; an over-ceiling slot is a
        // recorded skip, not a truncation and not a queue.
        let Some(_lease) = self.try_lease(&schedule.id, schedule.max_concurrent) else {
            let mut record =
                WorktreeFiring::new(&schedule.id, firing_id, FiringOutcome::SkippedOverlap);
            record.error = Some(format!(
                "skipped_overlap: {} firing(s) already running, max_concurrent {}",
                self.running(&schedule.id),
                schedule.max_concurrent
            ));
            let message = record.error.clone().unwrap_or_default();
            self.record(record);
            return Err(message);
        };

        // §6.4 preflight: the host disk is never filled by cron. Free space
        // below the floor, or the total cap still exceeded after GC, skips
        // the firing — nothing is created and the slot is not retried.
        if let Err(error) = self.preflight(now) {
            let mut record =
                WorktreeFiring::new(&schedule.id, firing_id, FiringOutcome::SkippedDiskPressure);
            record.error = Some(error.clone());
            self.record(record);
            return Err(format!("disk_pressure: {error}"));
        }
        if let Err(error) = self.create_worktree(&path) {
            let mut record =
                WorktreeFiring::new(&schedule.id, firing_id, FiringOutcome::WorktreeUnavailable);
            record.error = Some(error);
            let message = record.error.clone().unwrap_or_default();
            self.record(record);
            // No fallback to the live checkout, and no same-slot retry
            // (§6.5): the caller settles the firing and moves on.
            return Err(format!("worktree_unavailable: {message}"));
        }

        let request = TurnRequest {
            worktree: path.clone(),
            prompt: schedule.prompt.clone(),
            timeout: Duration::from_millis(schedule.timeout_ms),
            schedule_id: schedule.id.clone(),
            firing_id: firing_id.to_owned(),
            payload: payload.to_owned(),
            max_worktree_bytes: Some(self.policy.max_worktree_bytes()),
        };
        let started = Instant::now();
        let turn = self.runner.run(&request);
        let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

        let mut record = WorktreeFiring::new(&schedule.id, firing_id, FiringOutcome::Failed);
        record.worktree_path = Some(path.clone());
        record.duration_ms = duration_ms;
        let mut failure: Option<String> = None;
        let mut too_large_flag = false;
        match turn {
            Ok(output) => {
                too_large_flag = output.too_large;
                let out_cap = self.policy.max_output_bytes();
                record.stdout_tail = bound_output(
                    &output.stdout,
                    out_cap,
                    "stdout",
                    &mut record.truncation_markers,
                );
                record.stderr_tail = bound_output(
                    &output.stderr,
                    out_cap,
                    "stderr",
                    &mut record.truncation_markers,
                );
                record.exit_code = output.exit_code;
                if output.timed_out {
                    record.outcome = FiringOutcome::Timeout;
                    failure = Some(format!(
                        "timeout: killed after {} ms (per-firing ceiling){}",
                        schedule.timeout_ms,
                        failure_excerpt(&output.stderr, &output.stdout, out_cap.min(8192))
                    ));
                } else if output.exit_code == Some(0) {
                    record.outcome = FiringOutcome::Ok;
                } else {
                    record.outcome = FiringOutcome::Failed;
                    failure = Some(format!(
                        "failed: turn exited {}{}",
                        output
                            .exit_code
                            .map_or_else(|| "on a signal".to_owned(), |code| code.to_string()),
                        failure_excerpt(&output.stderr, &output.stdout, out_cap.min(8192))
                    ));
                }
            }
            Err(error) => {
                record.outcome = FiringOutcome::Failed;
                failure = Some(format!("failed: {error}"));
            }
        }
        // §6.4 per-worktree cap: a run that outgrew its budget was killed by
        // the runner (whole process group) or is caught here; either way it
        // fails as `worktree_too_large` and is never kept for inspection.
        let max_tree = self.policy.max_worktree_bytes();
        let measured = (self.size_probe)(&path);
        let too_large = too_large_flag || measured > max_tree;
        if too_large {
            record.outcome = FiringOutcome::Failed;
            failure = Some(format!(
                "worktree_too_large: {} bytes exceeds the per-worktree cap of {} bytes",
                measured, max_tree
            ));
            if !too_large_flag {
                record.notes.push(
                    "worktree_too_large measured after the turn exited; nothing left to kill"
                        .to_owned(),
                );
            }
        }
        record.error = failure.clone();

        let (status, diff_stat, diff) = self.capture(&path, &mut record.notes);
        record.status = status;
        record.diff_stat = diff_stat;
        let diff_cap = self.policy.max_output_bytes();
        record.diff = bound_output(&diff, diff_cap, "diff", &mut record.truncation_markers);
        // §6.2 post-run cleanup: unconditional (ok, failed, kept or not) and
        // failure-tolerant — a cleanup error is a note on the firing, never a
        // failed tick. `cargo clean` semantics: the build tree goes.
        match self.clean_target_dir(&path) {
            Ok(freed) => record.freed_bytes = freed,
            Err(error) => record.notes.push(format!("cleanup target/: {error}")),
        }

        // §6.2 cleanup, unless the policy says to keep a failed turn's tree.
        // `--keep` (the CLI's flag) forces the same decision.
        record.kept =
            self.policy.keep_on_failure && record.outcome != FiringOutcome::Ok && !too_large;
        if record.kept {
            // Kept for inspection, but no longer running: it is GC's (§6.4).
            lock(&self.active).remove(&path);
        } else if let Err(error) = self.remove_worktree(&path) {
            record.notes.push(format!("cleanup: {error}"));
        }
        self.record(record);
        match failure {
            Some(message) => Err(message),
            None => Ok(()),
        }
    }

    fn take_outcome(&self, firing_id: &str) -> Option<DispatchOutcome> {
        let firing = self.firing(firing_id)?;
        Some(outcome_of(&firing))
    }

    fn drain_settled(&self) -> Vec<(String, DispatchOutcome)> {
        let mut ledger = lock(&self.ledger);
        ledger
            .drain(..)
            .map(|firing| (firing.firing_id.clone(), outcome_of(&firing)))
            .collect()
    }
}

/// One recorded firing's terminal outcome, as the ticker persists it.
fn outcome_of(firing: &WorktreeFiring) -> DispatchOutcome {
    let (state, skip_reason) = match firing.outcome {
        FiringOutcome::Ok => (FiringState::Succeeded, None),
        FiringOutcome::SkippedOverlap => {
            (FiringState::Skipped, Some("skipped_overlap".to_string()))
        }
        FiringOutcome::SkippedDiskPressure => (
            FiringState::Skipped,
            Some("skipped_disk_pressure".to_string()),
        ),
        FiringOutcome::Failed | FiringOutcome::Timeout | FiringOutcome::WorktreeUnavailable => {
            (FiringState::Failed, None)
        }
    };
    DispatchOutcome {
        state,
        worktree_path: firing
            .worktree_path
            .as_ref()
            .map(|path| path.display().to_string()),
        skip_reason,
        error: firing.error.clone(),
        finished_at_ms: (firing.finished_at_ms > 0).then_some(firing.finished_at_ms),
    }
}

/// Epoch milliseconds.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// One held concurrency slot (§5.4); releasing it is `Drop`.
pub struct SlotLease {
    schedule_id: String,
    running: Arc<Mutex<BTreeMap<String, u32>>>,
}

impl Drop for SlotLease {
    fn drop(&mut self) {
        let mut running = lock(&self.running);
        if let Some(count) = running.get_mut(&self.schedule_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                running.remove(&self.schedule_id);
            }
        }
    }
}

/// The first 8 hex chars of sha256(`firing_id`) (§6.3).
pub fn hash8(firing_id: &str) -> String {
    Sha256::digest(firing_id.as_bytes())
        .iter()
        .take(4)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn fire_time_from_payload(payload: &str) -> Option<DateTime<Utc>> {
    let value: serde_json::Value = serde_json::from_str(payload).ok()?;
    let raw = value.get("fire_time")?.as_str()?;
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|at| at.with_timezone(&Utc))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn truncate(mut text: String, max: usize) -> String {
    if text.len() <= max {
        return text;
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text.push_str("\n[cron: diff truncated at worktree.max_diff_kb]\n");
    text
}

/// A `git` invocation against `repo`, `-C`-rooted so it works for the primary
/// checkout and for a worktree alike. stderr is returned verbatim on failure —
/// §6.5 wants it captured on the firing.
fn run_git(repo: &Path, args: &[&str]) -> Result<String, String> {
    if args.is_empty() {
        return Err("empty git invocation".to_owned());
    }
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .output()
        .map_err(|error| format!("git {}: {error}", args.join(" ")))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(format!(
            "git {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

struct TreeEntry {
    path: PathBuf,
    bytes: u64,
    modified: SystemTime,
}

/// Every directory under `root/<schedule_id>/<leaf>` — the shape §6.3
/// creates, so a stray file in the root is never mistaken for a worktree.
fn list_worktrees(root: &Path) -> Vec<TreeEntry> {
    let mut found = Vec::new();
    let Ok(buckets) = fs::read_dir(root) else {
        return found;
    };
    for bucket in buckets.flatten() {
        if !bucket.path().is_dir() {
            continue;
        }
        let Ok(leaves) = fs::read_dir(bucket.path()) else {
            continue;
        };
        for leaf in leaves.flatten() {
            let path = leaf.path();
            if !path.is_dir() {
                continue;
            }
            let modified = leaf
                .metadata()
                .and_then(|meta| meta.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            found.push(TreeEntry {
                bytes: dir_size(&path),
                path,
                modified,
            });
        }
    }
    found
}

fn dir_size(path: &Path) -> u64 {
    let Ok(meta) = fs::symlink_metadata(path) else {
        return 0;
    };
    if meta.is_file() {
        return meta.len();
    }
    if !meta.is_dir() {
        return 0;
    }
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| dir_size(&entry.path()))
        .sum::<u64>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default();
            let path =
                std::env::temp_dir().join(format!("kian-b4-{tag}-{}-{nanos}", std::process::id()));
            fs::create_dir_all(&path).expect("temp dir");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// A throwaway repo with one commit — the `worktree.repo` of §6.2.
    fn init_repo(dir: &Path) {
        fs::create_dir_all(dir).expect("repo dir");
        run_git(dir, &["init", "-q"]).expect("git init");
        fs::write(dir.join("notes.txt"), "one\n").expect("write");
        run_git(
            dir,
            &[
                "-c",
                "user.email=cron@example.invalid",
                "-c",
                "user.name=cron",
                "add",
                "notes.txt",
            ],
        )
        .expect("git add");
        run_git(
            dir,
            &[
                "-c",
                "user.email=cron@example.invalid",
                "-c",
                "user.name=cron",
                "commit",
                "-q",
                "-m",
                "init",
            ],
        )
        .expect("git commit");
    }

    fn channels() -> BTreeMap<String, crate::config::ChannelConfig> {
        serde_yaml::from_str("cron-jobs:\n  type: timer\n  timer:\n    interval_ms: 60000\n")
            .expect("the cron test channel parses")
    }

    fn schedule(id: &str, max_concurrent: u32, timeout_ms: u64) -> CronRuntimeSchedule {
        let yaml = format!(
            "schedules:\n  - id: {id}\n    expr: \"* * * * *\"\n    \
             max_concurrent: {max_concurrent}\n    timeout_ms: {timeout_ms}\n    \
             worktree: true\n    \
             target: {{ channel: cron-jobs, topic: audit }}\n    prompt: audit .state/\n"
        );
        let section: crate::config::CronSection =
            serde_yaml::from_str(&yaml).expect("the cron fixture parses");
        section
            .to_runtime(&channels(), Utc::now())
            .expect("the cron fixture resolves")
            .into_iter()
            .next()
            .expect("exactly one schedule")
    }

    fn fire_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2024-06-03T12:00:00Z")
            .expect("fixture instant")
            .with_timezone(&Utc)
    }

    fn payload(schedule_id: &str, firing_id: &str) -> String {
        serde_json::json!({
            "schedule_id": schedule_id,
            "firing_id": firing_id,
            "fire_time": fire_time().to_rfc3339(),
            "prompt": "audit .state/",
            "target": { "channel": "cron-jobs", "topic": "audit" },
        })
        .to_string()
    }

    /// The injected runner: edits a tracked file, so the capture has a real
    /// diff, and counts its calls.
    struct ScriptedRunner {
        edit: bool,
        timed_out: bool,
        calls: Arc<AtomicUsize>,
    }

    impl TurnRunner for ScriptedRunner {
        fn run(&self, request: &TurnRequest) -> Result<TurnOutput, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(
                request.worktree.is_dir(),
                "the turn's cwd must be the worktree"
            );
            if self.edit {
                fs::write(request.worktree.join("notes.txt"), "one\ntwo\n")
                    .map_err(|error| error.to_string())?;
            }
            Ok(TurnOutput {
                stdout: "did the thing\n".to_owned(),
                stderr: "warn: nothing\n".to_owned(),
                exit_code: if self.timed_out { None } else { Some(0) },
                timed_out: self.timed_out,
                too_large: false,
            })
        }
    }

    /// Any directory left under the root, at any depth.
    fn residue(root: &Path) -> Vec<PathBuf> {
        list_worktrees(root).into_iter().map(|e| e.path).collect()
    }

    #[test]
    fn worktree_create_run_capture_cleanup_leaves_no_residue() {
        let tmp = TempDir::new("lifecycle");
        let repo = tmp.path().join("repo");
        init_repo(&repo);
        let calls = Arc::new(AtomicUsize::new(0));
        let dispatcher = WorktreeDispatcher::new(&repo)
            .with_free_space_probe(test_free_space())
            .with_runner(Box::new(ScriptedRunner {
                edit: true,
                timed_out: false,
                calls: Arc::clone(&calls),
            }));
        let sched = schedule("audit", 1, 60_000);
        let firing_id = kian_cron::firing_id("audit", fire_time());
        let path = dispatcher.worktree_path("audit", &firing_id, fire_time());

        dispatcher
            .dispatch(&sched, &firing_id, &payload("audit", &firing_id))
            .expect("the firing runs");

        let record = dispatcher.firing(&firing_id).expect("recorded");
        assert_eq!(record.outcome, FiringOutcome::Ok);
        assert_eq!(record.exit_code, Some(0));
        assert_eq!(record.stdout_tail.trim(), "did the thing");
        assert!(record.stderr_tail.contains("nothing"));
        assert!(record.status.contains("notes.txt"), "{}", record.status);
        assert!(record.diff.contains("+two"), "{}", record.diff);
        assert!(
            record.diff_stat.contains("notes.txt"),
            "{}",
            record.diff_stat
        );
        assert!(!record.kept);
        assert!(record.notes.is_empty(), "{:?}", record.notes);

        // §6.3 path scheme: <root>/<schedule_id>/<YYYYMMDDTHHMMSSZ>-<hash8>,
        // where a plain repo's root is <repo>/.git/kian-worktrees (the
        // resolution canonicalises, so the repo path is canonicalised too).
        let repo_real = repo.canonicalize().expect("repo realpath");
        assert_eq!(
            dispatcher.worktree_root(),
            repo_real.join(".git").join("kian-worktrees")
        );
        assert_eq!(
            path,
            repo_real
                .join(".git")
                .join("kian-worktrees")
                .join("audit")
                .join(format!("20240603T120000Z-{}", hash8(&firing_id)))
        );
        assert!(!path.exists(), "the worktree is removed");
        assert!(residue(&dispatcher.worktree_root()).is_empty());
        let listed = run_git(&repo, &["worktree", "list", "--porcelain"]).expect("worktree list");
        assert_eq!(
            listed.matches("worktree ").count(),
            1,
            "only the primary checkout remains: {listed}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the turn ran exactly once");
        assert_eq!(dispatcher.running("audit"), 0, "the slot was released");
        assert_eq!(dispatcher.firings().len(), 1);
    }

    #[test]
    fn linked_worktree_gitfile_resolves_under_common_dir() {
        let tmp = TempDir::new("gitfile");
        let repo = tmp.path().join("repoA");
        init_repo(&repo);
        // wtB is a linked worktree: its `.git` is an ASCII gitfile pointing at
        // <repoA>/.git/worktrees/wtB, so `<repoB>/.git/kian-worktrees` is not a
        // directory at all (the reported bug: os error 20).
        let wt = tmp.path().join("wtB");
        run_git(
            &repo,
            &[
                "worktree",
                "add",
                "-q",
                "--detach",
                wt.to_str().expect("utf8"),
                "HEAD",
            ],
        )
        .expect("git worktree add");
        assert!(
            wt.join(".git").is_file(),
            "the fixture is a gitfile worktree"
        );
        let common = run_git(
            &wt,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )
        .expect("git common dir");
        let common = PathBuf::from(common.trim())
            .canonicalize()
            .expect("common dir realpath");

        let calls = Arc::new(AtomicUsize::new(0));
        let dispatcher = WorktreeDispatcher::new(&wt)
            .with_free_space_probe(test_free_space())
            .with_runner(Box::new(ScriptedRunner {
                edit: true,
                timed_out: false,
                calls: Arc::clone(&calls),
            }));

        // The resolution rule: the base is the common dir, not wtB/.git.
        assert_eq!(dispatcher.worktree_root(), common.join("kian-worktrees"));
        assert!(!dispatcher.worktree_root().starts_with(wt.join(".git")));

        let sched = schedule("audit", 1, 60_000);
        let firing_id = kian_cron::firing_id("audit", fire_time());
        let path = dispatcher.worktree_path("audit", &firing_id, fire_time());
        assert!(
            path.starts_with(common.join("kian-worktrees")),
            "{}",
            path.display()
        );

        // A fake dispatch from inside wtB: create, run, capture, cleanup.
        dispatcher
            .dispatch(&sched, &firing_id, &payload("audit", &firing_id))
            .expect("a firing from a linked worktree runs");
        let record = dispatcher.firing(&firing_id).expect("recorded");
        assert_eq!(record.outcome, FiringOutcome::Ok, "{:?}", record.error);
        assert!(record.diff.contains("+two"), "{}", record.diff);
        assert!(!path.exists(), "the worktree is removed");
        assert!(residue(&dispatcher.worktree_root()).is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the turn ran exactly once");

        // wtB and the primary checkout survive; nothing else is registered.
        let listed = run_git(&repo, &["worktree", "list", "--porcelain"]).expect("worktree list");
        assert_eq!(listed.matches("worktree ").count(), 2, "{listed}");
    }

    #[test]
    fn non_repo_path_is_worktree_unavailable() {
        let tmp = TempDir::new("nonrepo");
        let calls = Arc::new(AtomicUsize::new(0));
        let dispatcher = WorktreeDispatcher::new(tmp.path())
            .with_free_space_probe(test_free_space())
            .with_runner(Box::new(ScriptedRunner {
                edit: true,
                timed_out: false,
                calls: Arc::clone(&calls),
            }));
        let sched = schedule("audit", 1, 60_000);
        let firing_id = kian_cron::firing_id("audit", fire_time());

        let error = dispatcher
            .dispatch(&sched, &firing_id, &payload("audit", &firing_id))
            .expect_err("a non-repo must fail the firing");

        assert!(error.starts_with("worktree_unavailable"), "{error}");
        let record = dispatcher.firing(&firing_id).expect("recorded");
        assert_eq!(record.outcome, FiringOutcome::WorktreeUnavailable);
        assert_eq!(record.outcome.as_str(), "worktree_unavailable");
        assert!(record.worktree_path.is_none(), "nothing was created");
        assert!(
            record
                .error
                .as_deref()
                .is_some_and(|e| e.contains("not a git repository"))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0, "no fallback run (§6.5)");
        assert!(residue(&dispatcher.worktree_root()).is_empty());
    }

    #[test]
    fn slot_over_max_concurrent_is_recorded_as_skipped_overlap() {
        let tmp = TempDir::new("concurrency");
        let repo = tmp.path().join("repo");
        init_repo(&repo);
        let calls = Arc::new(AtomicUsize::new(0));
        let dispatcher = WorktreeDispatcher::new(&repo)
            .with_free_space_probe(test_free_space())
            .with_runner(Box::new(ScriptedRunner {
                edit: false,
                timed_out: false,
                calls: Arc::clone(&calls),
            }));
        let sched = schedule("audit", 1, 60_000);
        let firing_id = kian_cron::firing_id("audit", fire_time());

        let lease = dispatcher.try_lease("audit", 1).expect("first slot");
        assert!(dispatcher.try_lease("audit", 1).is_none());
        let error = dispatcher
            .dispatch(&sched, &firing_id, &payload("audit", &firing_id))
            .expect_err("over the ceiling");
        assert!(error.starts_with("skipped_overlap"), "{error}");
        let record = dispatcher.firing(&firing_id).expect("recorded");
        assert_eq!(record.outcome, FiringOutcome::SkippedOverlap);
        assert_eq!(calls.load(Ordering::SeqCst), 0, "a skipped slot never runs");
        assert!(residue(&dispatcher.worktree_root()).is_empty());

        // Releasing the slot lets the very same firing run.
        drop(lease);
        assert_eq!(dispatcher.running("audit"), 0);
        dispatcher
            .dispatch(&sched, &firing_id, &payload("audit", &firing_id))
            .expect("the slot is free again");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(dispatcher.running("audit"), 0);
    }

    #[test]
    fn timeout_is_recorded_and_kept_for_inspection() {
        let tmp = TempDir::new("timeout");
        let repo = tmp.path().join("repo");
        init_repo(&repo);
        let calls = Arc::new(AtomicUsize::new(0));
        let dispatcher = WorktreeDispatcher::new(&repo)
            .with_free_space_probe(test_free_space())
            .with_runner(Box::new(ScriptedRunner {
                edit: true,
                timed_out: true,
                calls: Arc::clone(&calls),
            }));
        let sched = schedule("audit", 1, 60_000);
        let firing_id = kian_cron::firing_id("audit", fire_time());
        let path = dispatcher.worktree_path("audit", &firing_id, fire_time());

        let error = dispatcher
            .dispatch(&sched, &firing_id, &payload("audit", &firing_id))
            .expect_err("a timeout is a failed firing");
        assert!(error.starts_with("timeout:"), "{error}");

        let record = dispatcher.firing(&firing_id).expect("recorded");
        assert_eq!(record.outcome, FiringOutcome::Timeout);
        assert!(record.kept, "keep_on_failure preserves the tree");
        assert!(path.is_dir(), "the kept worktree survives");
        // The kept tree is not residue forever: retention GC takes it once it
        // is older than retain_hours (§6.4).
        let report = dispatcher.gc(Utc::now() + TimeDelta::hours(100));
        assert!(report.evicted.contains(&path), "{:?}", report.evicted);
        assert!(!path.exists());
        assert!(residue(&dispatcher.worktree_root()).is_empty());
    }

    #[test]
    fn gc_evicts_oldest_when_over_disk_cap() {
        let tmp = TempDir::new("gccap");
        let repo = tmp.path().join("repo");
        init_repo(&repo);
        // The root resolution canonicalises (macOS /var -> /private/var), so
        // the fixtures must be built on the canonical repo path too.
        let root = repo
            .canonicalize()
            .expect("repo realpath")
            .join(".git")
            .join("kian-worktrees");
        // Three preserved worktrees, ~600 KiB each: 1.8 MiB against a 1 MiB cap.
        let leaves = [
            "audit/20240101T000000Z-aaaaaaaa",
            "audit/20240102T000000Z-bbbbbbbb",
            "audit/20240103T000000Z-cccccccc",
        ];
        for leaf in leaves {
            let path = root.join(leaf);
            fs::create_dir_all(&path).expect("worktree dir");
            fs::write(path.join("blob"), vec![b'x'; 600 * 1024]).expect("blob");
        }
        let dispatcher = WorktreeDispatcher::new(&repo)
            .with_free_space_probe(test_free_space())
            .with_policy(WorktreePolicy {
                disk_cap_mb: 1,
                retain_hours: DEFAULT_RETAIN_HOURS,
                ..WorktreePolicy::default()
            })
            .with_runner(Box::new(ScriptedRunner {
                edit: false,
                timed_out: false,
                calls: Arc::new(AtomicUsize::new(0)),
            }));

        let report = dispatcher.gc(Utc::now());

        assert_eq!(report.evicted.len(), 2, "{report:?}");
        assert!(report.evicted[0].ends_with("20240101T000000Z-aaaaaaaa"));
        assert!(report.evicted[1].ends_with("20240102T000000Z-bbbbbbbb"));
        assert!(report.retained[0].ends_with("20240103T000000Z-cccccccc"));
        assert!(report.bytes_before > report.bytes_after);
        assert!(!report.refused, "the cap fits again: {report:?}");
        assert!(report.bytes_after <= dispatcher.policy().disk_cap_bytes());
        let left = residue(&root);
        assert_eq!(left.len(), 1, "{left:?}");
        assert!(left[0].ends_with("20240103T000000Z-cccccccc"));
    }

    #[test]
    fn gc_refuses_when_the_only_worktree_is_running() {
        let tmp = TempDir::new("gclive");
        let repo = tmp.path().join("repo");
        init_repo(&repo);
        // The root resolution canonicalises (macOS /var -> /private/var), so
        // the fixtures must be built on the canonical repo path too.
        let root = repo
            .canonicalize()
            .expect("repo realpath")
            .join(".git")
            .join("kian-worktrees");
        let live = root.join("audit/20240101T000000Z-aaaaaaaa");
        fs::create_dir_all(&live).expect("worktree dir");
        fs::write(live.join("blob"), vec![b'x'; 2 * 1024 * 1024]).expect("blob");
        let dispatcher = WorktreeDispatcher::new(&repo)
            .with_free_space_probe(test_free_space())
            .with_policy(WorktreePolicy {
                disk_cap_mb: 1,
                ..WorktreePolicy::default()
            });
        // A running firing's tree is pinned (§6.4: never the one running).
        lock(&dispatcher.active).insert(live.clone());

        let report = dispatcher.gc(Utc::now());

        assert!(report.evicted.is_empty(), "{report:?}");
        assert_eq!(report.running, vec![live.clone()]);
        assert!(report.refused);
        assert!(live.is_dir());
        // And the next create is skipped with `disk_pressure`.
        let sched = schedule("audit", 1, 60_000);
        let firing_id = kian_cron::firing_id("audit", fire_time());
        let error = dispatcher
            .dispatch(&sched, &firing_id, &payload("audit", &firing_id))
            .expect_err("the cap is exhausted");
        assert!(error.starts_with("disk_pressure"), "{error}");
        assert!(error.contains("over the total cap"), "{error}");
    }
}

// ---------------------------------------------------------------------------
// CRON-MODEL §6.4: the disk guards. Cron must never fill the host disk, so
// every byte bound is a policy knob, every measurement of the host is an
// injected probe (the preflight and the per-worktree cap are testable without
// writing to the real filesystem), and every refusal is a recorded skip.
// ---------------------------------------------------------------------------

/// Free and total bytes of the filesystem holding a path (§6.4).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FreeSpace {
    /// Bytes available to this user.
    pub available_bytes: u64,
    /// Bytes on the whole filesystem — the base of the §6.4 percent floor.
    pub total_bytes: u64,
}

/// The injected free-space probe (§6.4): `df` in production, a fixture in
/// tests, so the preflight never depends on — or fills — the host disk.
pub type FreeSpaceProbe = Arc<dyn Fn(&Path) -> Result<FreeSpace, String> + Send + Sync>;

/// The injected byte-count probe (§6.4): the per-worktree cap measures through
/// it, so a test can inject an over-cap reading instead of writing 2 GiB.
pub type DirSizeProbe = Arc<dyn Fn(&Path) -> u64 + Send + Sync>;

/// The production free-space probe (§6.4): `df -k`, the numbers the operator
/// sees, with no new dependency.
pub fn df_free_space(path: &Path) -> Result<FreeSpace, String> {
    let output = Command::new("df")
        .arg("-k")
        .arg(path)
        .output()
        .map_err(|error| format!("df: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "df {}: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let line = text
        .lines()
        .nth(1)
        .ok_or_else(|| "df: no data line".to_owned())?;
    let fields: Vec<&str> = line.split_whitespace().collect();
    if fields.len() < 4 {
        return Err(format!("df: unexpected line {line:?}"));
    }
    let total_kib: u64 = fields[1]
        .parse()
        .map_err(|_| format!("df: total block count {fields:?}"))?;
    let available_kib: u64 = fields[3]
        .parse()
        .map_err(|_| format!("df: available block count {fields:?}"))?;
    Ok(FreeSpace {
        available_bytes: available_kib.saturating_mul(1024),
        total_bytes: total_kib.saturating_mul(1024),
    })
}

/// The §6.4 test probe: a filesystem with room to spare, so no test depends on
/// the host's free space.
#[cfg(test)]
fn test_free_space() -> FreeSpaceProbe {
    Arc::new(|_path: &Path| {
        Ok(FreeSpace {
            available_bytes: 1 << 50,
            total_bytes: 1 << 50,
        })
    })
}

impl WorktreePolicy {
    /// The §6.4 per-worktree cap in bytes.
    pub fn max_worktree_bytes(&self) -> u64 {
        self.max_worktree_mib.saturating_mul(1024 * 1024)
    }

    /// The §6.2 bound on each stored stdout/stderr/diff, in bytes.
    pub fn max_output_bytes(&self) -> usize {
        self.max_output_kib.saturating_mul(1024) as usize
    }

    /// The §6.4 free-space floor: `min_free_mib`, or `min_free_percent` of
    /// `total_bytes`, whichever is larger.
    pub fn min_free_floor_bytes(&self, total_bytes: u64) -> u64 {
        let by_size = self.min_free_mib.saturating_mul(1024 * 1024);
        // One division, in u128: `total / 100 * percent` would drop the
        // remainder twice over and land the floor below the documented value.
        let by_percent = u64::try_from(
            u128::from(total_bytes) * u128::from(self.min_free_percent.min(100)) / 100,
        )
        .unwrap_or(u64::MAX);
        by_size.max(by_percent)
    }

    /// The policy the `superagent.cron.disk` knobs resolve to (§3.1/§6.4).
    pub fn from_section(section: &crate::config::CronSection) -> Self {
        let disk = section.disk.resolved();
        Self {
            disk_cap_mb: disk.total_cap_mib,
            min_free_mib: disk.min_free_gib.saturating_mul(1024),
            min_free_percent: disk.min_free_percent,
            max_worktree_mib: disk.max_worktree_mib,
            max_output_kib: disk.max_output_kib,
            ..Self::default()
        }
    }
}

impl WorktreeDispatcher {
    /// Inject the §6.4 free-space probe: tests never touch the host disk.
    pub fn with_free_space_probe(mut self, probe: FreeSpaceProbe) -> Self {
        self.free_space = probe;
        self
    }

    /// Inject the §6.4 byte-count probe used by the per-worktree cap.
    pub fn with_size_probe(mut self, probe: DirSizeProbe) -> Self {
        self.size_probe = probe;
        self
    }

    /// The §6.4 preflight, in order: free space on the repo's filesystem, then
    /// the total cap across all cron worktrees (GC first, oldest non-running
    /// evicted). `Err` is the `disk_pressure` reason: nothing is created and
    /// the slot is not retried.
    pub fn preflight(&self, now: DateTime<Utc>) -> Result<(), String> {
        // (a) probe the repo's filesystem, (b) compute the floor, (c) decide
        // against free space *minus* the worst case the next create could add.
        // A probe that cannot answer is a skip, never a bypass (fail-closed).
        let free = (self.free_space)(&self.repo)
            .map_err(|error| format!("free-space probe failed: {error}"))?;
        let floor = self.policy.min_free_floor_bytes(free.total_bytes);
        let incoming = self.largest_recent_worktree_bytes();
        if free.available_bytes.saturating_sub(incoming) < floor {
            // The next create could cross the floor: GC the oldest non-running
            // worktrees, then re-probe before refusing this slot.
            let _ = self.gc(now);
            let free = (self.free_space)(&self.repo)
                .map_err(|error| format!("free-space probe failed: {error}"))?;
            if free.available_bytes < floor {
                return Err(format!(
                    "only {} bytes free on the filesystem of {}, below the floor of {} bytes",
                    free.available_bytes,
                    self.repo.display(),
                    floor
                ));
            }
        }
        let report = self.gc_with_incoming(now, 0);
        if report.refused {
            return Err(format!(
                "{} bytes across cron worktrees, over the total cap of {} bytes even after GC",
                report.bytes_after,
                self.policy.disk_cap_bytes()
            ));
        }
        self.ensure_capacity(0)
    }

    /// The size of the largest worktree currently under the root (§6.4): the
    /// worst case the next create could add to the host. Measured through the
    /// injected size probe, so no test stats the host disk.
    fn largest_recent_worktree_bytes(&self) -> u64 {
        let root = self.worktree_root();
        list_worktrees(&root)
            .into_iter()
            .map(|entry| (self.size_probe)(&entry.path))
            .max()
            .unwrap_or(0)
    }

    /// The floor the next preflight will enforce, in bytes, given the host's
    /// filesystem size (§6.4).
    pub fn min_free_floor_bytes(&self) -> Result<u64, String> {
        let free = (self.free_space)(&self.repo)
            .map_err(|error| format!("free-space probe failed: {error}"))?;
        Ok(self.policy.min_free_floor_bytes(free.total_bytes))
    }
}

/// Bound one stored field of a firing (§6.2), recording an explicit marker so
/// an operator sees a truncation rather than a silently clipped log.
fn bound_output(text: &str, max: usize, label: &str, markers: &mut Vec<String>) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let mut end = max;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    markers.push(format!(
        "{label} truncated at the worktree.max_output_kib bound: {} bytes stored as {end} bytes",
        text.len()
    ));
    text[..end].to_owned()
}

/// The harness's own error text for a failing turn (§7): the child's stderr
/// (stdout when stderr is empty), bounded at `max` bytes and explicitly marked
/// when clipped — a firing records *why* dsh failed, never just the exit code.
fn failure_excerpt(stderr: &str, stdout: &str, max: usize) -> String {
    let (label, text) = if !stderr.trim().is_empty() {
        ("stderr", stderr.trim())
    } else if !stdout.trim().is_empty() {
        ("stdout", stdout.trim())
    } else {
        return String::new();
    };
    let mut end = text.len().min(max);
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    let mut body = text[..end].to_owned();
    if end < text.len() {
        body.push_str(&format!(
            "
[error excerpt truncated: {} bytes of {label} stored as {end}]",
            text.len()
        ));
    }
    format!(
        "
{label}: {body}"
    )
}

/// `true` when `worktree` has outgrown `cap` bytes (§6.4), measured on the host.
fn size_cap_exceeded(worktree: &Path, cap: Option<u64>) -> bool {
    cap.is_some_and(|max| dir_size(worktree) > max)
}

/// The §6.4 disk-guard tests: every host measurement is injected, and the only
/// bytes written are small `/tmp` fixtures.
#[cfg(test)]
mod tests_b4b {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default();
            let path =
                std::env::temp_dir().join(format!("kian-b4b-{tag}-{}-{nanos}", std::process::id()));
            fs::create_dir_all(&path).expect("temp dir");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn init_repo(dir: &Path) {
        fs::create_dir_all(dir).expect("repo dir");
        run_git(dir, &["init", "-q"]).expect("git init");
        fs::write(dir.join("notes.txt"), "one\n").expect("write");
        run_git(
            dir,
            &[
                "-c",
                "user.email=cron@example.invalid",
                "-c",
                "user.name=cron",
                "add",
                "notes.txt",
            ],
        )
        .expect("git add");
        run_git(
            dir,
            &[
                "-c",
                "user.email=cron@example.invalid",
                "-c",
                "user.name=cron",
                "commit",
                "-q",
                "-m",
                "one",
            ],
        )
        .expect("git commit");
    }

    fn channels() -> BTreeMap<String, crate::config::ChannelConfig> {
        serde_yaml::from_str("cron-jobs:\n  type: timer\n  timer:\n    interval_ms: 60000\n")
            .expect("the cron test channel parses")
    }

    fn schedule(id: &str) -> CronRuntimeSchedule {
        let yaml = format!(
            "schedules:\n  - id: {id}\n    expr: \"* * * * *\"\n    \
             max_concurrent: 1\n    timeout_ms: 60000\n    worktree: true\n    \
             target: {{ channel: cron-jobs, topic: audit }}\n    prompt: audit .state/\n"
        );
        let section: crate::config::CronSection =
            serde_yaml::from_str(&yaml).expect("the cron fixture parses");
        section
            .to_runtime(&channels(), Utc::now())
            .expect("the cron fixture resolves")
            .into_iter()
            .next()
            .expect("exactly one schedule")
    }

    fn fire_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2024-06-03T12:00:00Z")
            .expect("fixture instant")
            .with_timezone(&Utc)
    }

    fn payload(schedule_id: &str, firing_id: &str) -> String {
        serde_json::json!({
            "schedule_id": schedule_id,
            "firing_id": firing_id,
            "fire_time": fire_time().to_rfc3339(),
            "prompt": "audit .state/",
            "target": { "channel": "cron-jobs", "topic": "audit" },
        })
        .to_string()
    }

    /// The injected runner: spawns nothing, and reports the truncated output or
    /// the size-kill flag a guard test needs.
    struct UnitRunner {
        stdout: String,
        stderr: String,
        too_large: bool,
        calls: Arc<AtomicUsize>,
    }

    impl UnitRunner {
        fn new() -> Self {
            Self {
                stdout: "did the thing\n".to_owned(),
                stderr: "warn: nothing\n".to_owned(),
                too_large: false,
                calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl TurnRunner for UnitRunner {
        fn run(&self, request: &TurnRequest) -> Result<TurnOutput, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(
                request.worktree.is_dir(),
                "the turn's cwd must be the worktree"
            );
            Ok(TurnOutput {
                stdout: self.stdout.clone(),
                stderr: self.stderr.clone(),
                exit_code: if self.too_large { None } else { Some(0) },
                timed_out: false,
                too_large: self.too_large,
            })
        }
    }

    /// Both §6.4 free-space floors at zero: these tests drive the probes
    /// directly, so the host's numbers never leak into an assertion.
    fn quiet_policy() -> WorktreePolicy {
        WorktreePolicy {
            min_free_mib: 0,
            min_free_percent: 0,
            ..WorktreePolicy::default()
        }
    }

    /// 1 GiB free against a 4 TiB filesystem: below any sane floor.
    fn starved() -> FreeSpaceProbe {
        Arc::new(|_path: &Path| {
            Ok(FreeSpace {
                available_bytes: 1 << 30,
                total_bytes: 1 << 42,
            })
        })
    }

    fn residue(root: &Path) -> Vec<PathBuf> {
        list_worktrees(root).into_iter().map(|e| e.path).collect()
    }

    fn dispatch_once(dispatcher: &WorktreeDispatcher, id: &str) -> (String, Result<(), String>) {
        let sched = schedule(id);
        let firing_id = kian_cron::firing_id(id, fire_time());
        let outcome = dispatcher.dispatch(&sched, &firing_id, &payload(id, &firing_id));
        (firing_id, outcome)
    }

    #[test]
    fn preflight_below_floor_skips_with_disk_pressure() {
        let tmp = TempDir::new("preflight");
        let repo = tmp.path().join("repo");
        init_repo(&repo);
        let calls = Arc::new(AtomicUsize::new(0));
        let dispatcher = WorktreeDispatcher::new(&repo)
            .with_policy(WorktreePolicy::default())
            .with_free_space_probe(starved())
            .with_runner(Box::new(UnitRunner {
                calls: Arc::clone(&calls),
                ..UnitRunner::new()
            }));

        let (firing_id, outcome) = dispatch_once(&dispatcher, "audit");

        let error = outcome.expect_err("a starved filesystem must not run cron");
        assert!(error.starts_with("disk_pressure"), "{error}");
        let firing = dispatcher.firing(&firing_id).expect("the skip is recorded");
        assert_eq!(firing.outcome, FiringOutcome::SkippedDiskPressure);
        assert_eq!(firing.outcome.as_str(), "skipped_disk_pressure");
        assert!(
            firing.worktree_path.is_none(),
            "nothing was created: {firing:?}"
        );
        assert!(
            firing
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("below the floor"),
            "{firing:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0, "the turn never ran");
        assert!(residue(&dispatcher.worktree_root()).is_empty());
    }

    #[test]
    fn gc_evicts_oldest_when_total_exceeds_cap() {
        let tmp = TempDir::new("gcevict");
        let repo = tmp.path().join("repo");
        init_repo(&repo);
        // The root resolution canonicalises (macOS /var -> /private/var), so
        // the fixtures must be built on the canonical repo path too.
        let root = repo
            .canonicalize()
            .expect("repo realpath")
            .join(".git")
            .join("kian-worktrees");
        // Three preserved worktrees, ~600 KiB each: 1.8 MiB against a 1 MiB
        // total cap, so the preflight GC has to evict oldest-first.
        let leaves = [
            "audit/20240101T000000Z-aaaaaaaa",
            "audit/20240102T000000Z-bbbbbbbb",
            "audit/20240103T000000Z-cccccccc",
        ];
        for leaf in leaves {
            let path = root.join(leaf);
            fs::create_dir_all(&path).expect("worktree dir");
            fs::write(path.join("blob"), vec![b'x'; 600 * 1024]).expect("blob");
        }
        let dispatcher = WorktreeDispatcher::new(&repo)
            .with_policy(WorktreePolicy {
                disk_cap_mb: 1,
                ..quiet_policy()
            })
            .with_free_space_probe(test_free_space())
            .with_runner(Box::new(UnitRunner::new()));

        let (firing_id, outcome) = dispatch_once(&dispatcher, "audit");

        outcome.expect("GC made room, so the firing ran");
        let firing = dispatcher.firing(&firing_id).expect("recorded");
        assert_eq!(firing.outcome, FiringOutcome::Ok);
        let left: Vec<String> = residue(&root)
            .iter()
            .map(|path| path.display().to_string())
            .collect();
        assert!(
            !left
                .iter()
                .any(|path| path.contains("20240101T000000Z-aaaaaaaa")),
            "the oldest went first: {left:?}"
        );
        assert!(
            left.iter()
                .any(|path| path.contains("20240103T000000Z-cccccccc")),
            "the newest survived: {left:?}"
        );
        assert!(
            dir_size(&root) <= dispatcher.policy().disk_cap_bytes(),
            "{left:?}"
        );
    }

    #[test]
    fn size_cap_kills_and_records_worktree_too_large() {
        let tmp = TempDir::new("sizecap");
        let repo = tmp.path().join("repo");
        init_repo(&repo);
        let dispatcher = WorktreeDispatcher::new(&repo)
            .with_policy(WorktreePolicy {
                max_worktree_mib: 1,
                ..quiet_policy()
            })
            .with_free_space_probe(test_free_space())
            // The cap is measured through the probe: 3 GiB injected, never
            // written to the host.
            .with_size_probe(Arc::new(|_path: &Path| 3 * 1024 * 1024 * 1024))
            .with_runner(Box::new(UnitRunner {
                too_large: true,
                ..UnitRunner::new()
            }));

        let (firing_id, outcome) = dispatch_once(&dispatcher, "audit");

        let error = outcome.expect_err("a too-large run fails the firing");
        assert!(error.contains("worktree_too_large"), "{error}");
        let firing = dispatcher.firing(&firing_id).expect("recorded");
        assert_eq!(firing.outcome, FiringOutcome::Failed);
        assert!(
            firing
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("worktree_too_large"),
            "{firing:?}"
        );
        assert!(
            !firing.kept,
            "a too-large tree is never preserved: {firing:?}"
        );
        assert!(
            residue(&dispatcher.worktree_root()).is_empty(),
            "the tree was removed"
        );
    }

    #[test]
    fn output_truncation_marker_present() {
        let tmp = TempDir::new("truncate");
        let repo = tmp.path().join("repo");
        init_repo(&repo);
        let dispatcher = WorktreeDispatcher::new(&repo)
            .with_policy(WorktreePolicy {
                max_output_kib: 1,
                ..quiet_policy()
            })
            .with_free_space_probe(test_free_space())
            .with_runner(Box::new(UnitRunner {
                stdout: "x".repeat(4096),
                ..UnitRunner::new()
            }));

        let (firing_id, outcome) = dispatch_once(&dispatcher, "audit");

        outcome.expect("the turn ran");
        let firing = dispatcher.firing(&firing_id).expect("recorded");
        assert_eq!(
            firing.stdout_tail.len(),
            1024,
            "stdout is bounded to max_output_kib"
        );
        assert_eq!(
            firing.stderr_tail, "warn: nothing\n",
            "under the bound, untouched"
        );
        let markers = firing.truncation_markers.join(" | ");
        assert!(markers.contains("stdout"), "{markers}");
        assert!(
            markers.contains("4096"),
            "the marker states the original size: {markers}"
        );
        assert!(!markers.contains("stderr"), "{markers}");
    }

    #[test]
    fn dsh_runner_kills_a_turn_that_outgrows_the_worktree_cap() {
        let tmp = TempDir::new("runnerkill");
        let tree = tmp.path().join("tree");
        fs::create_dir_all(&tree).expect("tree");
        fs::write(tree.join("blob"), vec![b'x'; 1024 * 1024]).expect("blob");
        let runner = DshTurnRunner {
            program: "sh".to_owned(),
            args: vec!["-c".to_owned(), "sleep 30".to_owned()],
            profile: None,
            env: BTreeMap::new(),
        };
        let request = TurnRequest {
            worktree: tree.clone(),
            prompt: "audit .state/".to_owned(),
            timeout: Duration::from_secs(30),
            schedule_id: "audit".to_owned(),
            firing_id: "audit@2024-06-03T12:00:00Z".to_owned(),
            payload: "{}".to_owned(),
            max_worktree_bytes: Some(256 * 1024),
        };

        let started = Instant::now();
        let output = runner.run(&request).expect("the runner spawns the harness");

        assert!(output.too_large, "the size cap tripped: {output:?}");
        assert_eq!(output.exit_code, None, "a size kill is not a clean exit");
        assert!(!output.timed_out, "the per-firing ceiling did not expire");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the process group was killed, not waited out"
        );
    }

    /// A runner that leaves a fake cargo build tree in the worktree, like a turn
    /// that ran `cargo build` would. No real cargo build: tests stay cheap.
    struct BuildRunner {
        exit_code: i32,
        bytes: usize,
    }

    impl TurnRunner for BuildRunner {
        fn run(&self, request: &TurnRequest) -> Result<TurnOutput, String> {
            let deps = request.worktree.join("target").join("debug").join("deps");
            fs::create_dir_all(&deps).map_err(|error| error.to_string())?;
            fs::write(deps.join("kian-0123456789abcdef"), vec![120_u8; self.bytes])
                .map_err(|error| error.to_string())?;
            Ok(TurnOutput {
                stdout: "built\n".to_owned(),
                stderr: String::new(),
                exit_code: Some(self.exit_code),
                timed_out: false,
                too_large: false,
            })
        }
    }

    #[test]
    fn post_run_cleanup_removes_target_and_worktree() {
        // Success: the worktree goes, and its build tree goes with it.
        let tmp = TempDir::new("cleanup");
        let repo = tmp.path().join("repo");
        init_repo(&repo);
        let base = quiet_policy();
        let ok = WorktreeDispatcher::new(&repo)
            .with_policy(base.clone())
            .with_free_space_probe(test_free_space())
            .with_runner(Box::new(BuildRunner {
                exit_code: 0,
                bytes: 256 * 1024,
            }));

        let (firing_id, outcome) = dispatch_once(&ok, "audit");
        outcome.expect("the firing ran");
        let firing = ok.firing(&firing_id).expect("recorded");
        assert_eq!(firing.outcome, FiringOutcome::Ok);
        assert!(
            firing.freed_bytes >= 256 * 1024,
            "the freed bytes are recorded: {firing:?}"
        );
        assert!(
            !firing.notes.iter().any(|note| note.contains("cleanup")),
            "a clean cleanup logs nothing: {firing:?}"
        );
        let path = firing
            .worktree_path
            .clone()
            .expect("the turn had a worktree");
        assert!(
            !path.join("target").exists(),
            "target/ was removed: {}",
            path.display()
        );
        assert!(
            !path.exists(),
            "the worktree was removed: {}",
            path.display()
        );
        assert!(
            residue(&ok.worktree_root()).is_empty(),
            "no worktree behind"
        );

        // Failure under `keep_on_failure: true`: the worktree is kept for
        // inspection, but the cleanup is unconditional, so target/ still goes.
        let keep = WorktreeDispatcher::new(&repo)
            .with_policy(WorktreePolicy {
                keep_on_failure: true,
                ..base
            })
            .with_free_space_probe(test_free_space())
            .with_runner(Box::new(BuildRunner {
                exit_code: 1,
                bytes: 128 * 1024,
            }));

        let (firing_id, outcome) = dispatch_once(&keep, "audit");
        outcome.expect_err("a nonzero exit is a failed firing");
        let firing = keep.firing(&firing_id).expect("recorded");
        assert_eq!(firing.outcome, FiringOutcome::Failed);
        assert!(firing.kept, "keep_on_failure kept the tree: {firing:?}");
        assert!(firing.freed_bytes >= 128 * 1024, "{firing:?}");
        let path = firing.worktree_path.clone().expect("it ran");
        assert!(path.exists(), "the kept worktree stays: {}", path.display());
        assert!(
            !path.join("target").exists(),
            "its target/ is gone even when kept"
        );
    }

    #[test]
    fn knobs_default_values_match_spec() {
        let section: crate::config::CronSection =
            serde_yaml::from_str("schedules: []\n").expect("an empty cron section parses");
        let disk = section.disk.resolved();
        assert_eq!(disk.min_free_gib, 80);
        assert_eq!(disk.min_free_percent, 25);
        assert_eq!(disk.max_worktree_mib, 2048);
        assert_eq!(disk.total_cap_mib, 20480);
        assert_eq!(disk.max_output_kib, 256);

        let policy = WorktreePolicy::from_section(&section);
        assert_eq!(policy.disk_cap_bytes(), 20480 * 1024 * 1024);
        assert_eq!(policy.max_worktree_bytes(), 2 * 1024 * 1024 * 1024);
        assert_eq!(policy.max_output_bytes(), 256 * 1024);
        assert_eq!(policy.min_free_floor_bytes(100 << 30), 80 << 30);
        assert_eq!(policy.min_free_floor_bytes(1 << 50), (1 << 50) / 4);

        let wired: crate::config::CronSection = serde_yaml::from_str(
            "disk:\n  min_free_gib: 30\n  total_cap_mib: 64\n  max_output_kib: 8\n",
        )
        .expect("the disk knobs parse");
        let policy = WorktreePolicy::from_section(&wired);
        assert_eq!(policy.min_free_mib, 30 * 1024);
        assert_eq!(policy.disk_cap_mb, 64);
        assert_eq!(policy.max_output_bytes(), 8 * 1024);

        let bad: crate::config::CronSection =
            serde_yaml::from_str("disk:\n  total_cap_mib: 0\n").expect("it parses, then refuses");
        let error = bad
            .to_runtime(&channels(), chrono::Utc::now())
            .expect_err("a zero knob refuses boot");
        assert!(format!("{error}").contains("total_cap_mib"), "{error}");

        // The long knob name is the same knob (§6.4 `total_cap_mib`).
        let aliased: crate::config::CronSection =
            serde_yaml::from_str("disk:\n  max_total_worktree_mib: 4096\n")
                .expect("the long name parses");
        assert_eq!(aliased.disk.resolved().total_cap_mib, 4096);

        // The total cap bounds all worktrees: per-worktree above the total is
        // contradictory and refuses boot.
        let contradictory: crate::config::CronSection =
            serde_yaml::from_str("disk:\n  max_worktree_mib: 4096\n  total_cap_mib: 1024\n")
                .expect("it parses, then refuses");
        let error = contradictory
            .to_runtime(&channels(), chrono::Utc::now())
            .expect_err("a per-worktree cap above the total refuses boot");
        assert!(
            format!("{error}").contains("must be >= max_worktree_mib"),
            "{error}"
        );
    }
}

// ---------------------------------------------------------------------------
// The dsh invocation contract (CRON-MODEL \u00a76.2). dsh's own help is the
// authority: `dsh [--profile] <name> [options] [app-args...]`, one-shot form
// `dsh headless "run the tests"` \u2014 the profile selects the app and the
// scheduled payload is that app's one positional task argument. Spawning
// `dsh <prompt>` with no profile made dsh read the payload as the profile name
// (`profile "say hello" does not exist`) and exit 1. These tests pin the argv,
// the delivery channel (a positional argument, never stdin) and the stderr
// capture, against a fake `dsh` shell script in /tmp: no real harness, no
// network, no model.
// ---------------------------------------------------------------------------
#[cfg(all(test, unix))]
mod tests_dsh_contract {
    use super::*;
    use std::collections::BTreeMap;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default();
            let path =
                PathBuf::from("/tmp").join(format!("kian-b9-{tag}-{}-{nanos}", std::process::id()));
            fs::create_dir_all(&path).expect("temp dir");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn init_repo(dir: &Path) {
        fs::create_dir_all(dir).expect("repo dir");
        run_git(dir, &["init", "-q"]).expect("git init");
        fs::write(dir.join("notes.txt"), "one\n").expect("write");
        run_git(
            dir,
            &[
                "-c",
                "user.email=cron@example.invalid",
                "-c",
                "user.name=cron",
                "add",
                "notes.txt",
            ],
        )
        .expect("git add");
        run_git(
            dir,
            &[
                "-c",
                "user.email=cron@example.invalid",
                "-c",
                "user.name=cron",
                "commit",
                "-q",
                "-m",
                "init",
            ],
        )
        .expect("git commit");
    }

    fn channels() -> BTreeMap<String, crate::config::ChannelConfig> {
        serde_yaml::from_str("cron-jobs:\n  type: timer\n  timer:\n    interval_ms: 60000\n")
            .expect("the cron test channel parses")
    }

    fn schedule(id: &str) -> CronRuntimeSchedule {
        let yaml = format!(
            "schedules:\n  - id: {id}\n    expr: \"* * * * *\"\n    \
                    max_concurrent: 1\n    timeout_ms: 60000\n    worktree: true\n    \
                    target: {{ channel: cron-jobs, topic: audit }}\n    prompt: say hello\n"
        );
        let section: crate::config::CronSection =
            serde_yaml::from_str(&yaml).expect("the cron fixture parses");
        section
            .to_runtime(&channels(), Utc::now())
            .expect("the cron fixture resolves")
            .into_iter()
            .next()
            .expect("exactly one schedule")
    }

    fn fire_time() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2024-06-03T12:00:00Z")
            .expect("fixture instant")
            .with_timezone(&Utc)
    }

    fn payload(schedule_id: &str, firing_id: &str) -> String {
        serde_json::json!({
            "schedule_id": schedule_id,
            "firing_id": firing_id,
            "fire_time": fire_time().to_rfc3339(),
            "prompt": "say hello",
            "target": { "channel": "cron-jobs", "topic": "audit" },
        })
        .to_string()
    }

    /// A fake `dsh` in /tmp: records every argv element one per line, drains
    /// stdin to a file (so a test can prove the payload never arrives there),
    /// writes `stderr_text` to stderr, then exits `exit_code`.
    fn fake_dsh(dir: &Path, exit_code: i32, stderr_text: &str) -> PathBuf {
        let script = dir.join("dsh");
        let body = format!(
            "#!/bin/sh\n\
             : > \"$FAKE_DSH_ARGV\"\n\
             for arg in \"$@\"; do printf '%s\\n' \"$arg\" >> \"$FAKE_DSH_ARGV\"; done\n\
             cat > \"$FAKE_DSH_STDIN\"\n\
             printf '%s\\n' '{stderr_text}' >&2\n\
             printf '%s\\n' 'fake dsh turn'\n\
             exit {exit_code}\n"
        );
        fs::write(&script, body).expect("fake dsh script");
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&script).expect("script meta").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).expect("chmod 0755");
        script
    }

    fn runner(script: &Path, dir: &Path, profile: Option<&str>) -> DshTurnRunner {
        let argv_log = dir.join("argv.log");
        let stdin_log = dir.join("stdin.log");
        let mut env = BTreeMap::new();
        env.insert("FAKE_DSH_ARGV".to_owned(), argv_log.display().to_string());
        env.insert("FAKE_DSH_STDIN".to_owned(), stdin_log.display().to_string());
        DshTurnRunner {
            program: script.display().to_string(),
            args: Vec::new(),
            profile: profile.map(str::to_owned),
            env,
        }
    }

    fn argv_lines(dir: &Path) -> Vec<String> {
        fs::read_to_string(dir.join("argv.log"))
            .expect("the fake dsh recorded its argv")
            .lines()
            .map(str::to_owned)
            .collect()
    }

    #[test]
    fn dsh_argv_is_the_profile_flag_plus_a_positional_prompt() {
        let tmp = TempDir::new("argv");
        let script = fake_dsh(tmp.path(), 0, "");
        let runner = runner(&script, tmp.path(), Some("headless"));
        let tree = tmp.path().join("tree");
        fs::create_dir_all(&tree).expect("tree");
        let request = TurnRequest {
            worktree: tree,
            prompt: "say hello".to_owned(),
            timeout: Duration::from_secs(30),
            schedule_id: "audit".to_owned(),
            firing_id: "audit@2024-06-03T12:00:00Z".to_owned(),
            payload: "{}".to_owned(),
            max_worktree_bytes: None,
        };

        let output = runner.run(&request).expect("the fake dsh runs");

        assert_eq!(output.exit_code, Some(0), "{output:?}");
        assert!(output.stdout.contains("fake dsh turn"), "{output:?}");
        // (a) the payload arrives on the documented channel: the app's
        // positional task argument. (b) the profile flag is passed.
        assert_eq!(
            argv_lines(tmp.path()),
            vec!["--profile", "headless", "say hello"],
            "dsh [--profile] <name> [app-args...]: the profile picks the app, \
             the payload is its positional task argument"
        );
        let stdin = fs::read_to_string(tmp.path().join("stdin.log")).expect("stdin log");
        assert!(
            stdin.is_empty(),
            "the payload is never delivered on stdin: {stdin:?}"
        );
    }

    #[test]
    fn failing_dsh_stderr_is_captured_onto_the_firing() {
        let tmp = TempDir::new("fail");
        let repo = tmp.path().join("repo");
        init_repo(&repo);
        let harness_text = "profile \"say hello\" does not exist";
        let script = fake_dsh(tmp.path(), 1, harness_text);
        let dispatcher = WorktreeDispatcher::new(&repo)
            .with_free_space_probe(Arc::new(|_path: &Path| {
                Ok(FreeSpace {
                    available_bytes: 1 << 50,
                    total_bytes: 1 << 50,
                })
            }))
            .with_runner(Box::new(runner(&script, tmp.path(), Some("headless"))));
        let sched = schedule("audit");
        let firing_id = kian_cron::firing_id("audit", fire_time());

        let error = dispatcher
            .dispatch(&sched, &firing_id, &payload("audit", &firing_id))
            .expect_err("a nonzero exit is a failed firing");

        assert!(error.starts_with("failed:"), "{error}");
        let record = dispatcher.firing(&firing_id).expect("recorded");
        assert_eq!(record.outcome, FiringOutcome::Failed);
        assert_eq!(record.exit_code, Some(1));
        // (c) the harness's own error text, not just "turn exited 1".
        assert!(
            record.stderr_tail.contains(harness_text),
            "stderr tail: {:?}",
            record.stderr_tail
        );
        let recorded = record.error.as_deref().unwrap_or_default();
        assert!(
            recorded.contains(harness_text),
            "the firing error carries dsh's own text: {recorded}"
        );
        assert!(recorded.contains("turn exited 1"), "{recorded}");
        assert_eq!(
            argv_lines(tmp.path()),
            vec!["--profile", "headless", "say hello"],
            "the failing turn still used the documented argv"
        );
    }

    #[test]
    fn harness_section_resolves_profile_and_binary_defaults() {
        let empty: crate::config::CronHarnessSection =
            serde_yaml::from_str("{}\n").expect("an empty harness section parses");
        let settings = empty.resolved();
        assert_eq!(settings.profile, crate::config::CRON_DEFAULT_DSH_PROFILE);
        assert_eq!(settings.profile, "headless");
        assert!(settings.env.is_empty());

        let wired: crate::config::CronHarnessSection = serde_yaml::from_str(
            "profile: sdk\ncommand: /usr/local/bin/dsh\nenv:\n  KIAN_DSH_PEER: '1'\n",
        )
        .expect("the harness knobs parse");
        let settings = wired.resolved();
        assert_eq!(settings.profile, "sdk");
        assert_eq!(settings.command, "/usr/local/bin/dsh");
        assert_eq!(
            settings.env.get("KIAN_DSH_PEER").map(String::as_str),
            Some("1")
        );
        let runner = DshTurnRunner::from_section(&wired);
        assert_eq!(
            runner.harness_argv("say hello"),
            vec!["--profile", "sdk", "say hello"],
            "the configured profile, never the payload, is the profile name"
        );

        let mut errors = Vec::new();
        let blank: crate::config::CronHarnessSection =
            serde_yaml::from_str("profile: '  '\n").expect("it parses, then refuses");
        blank.collect_errors(&mut errors);
        assert!(
            errors.iter().any(|e| e.contains("cron.harness.profile")),
            "{errors:?}"
        );
    }
}
