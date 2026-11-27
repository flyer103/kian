//! `kian self inspect` — one window of the durable stream, read-only
//! (SPEC §3.1).
//!
//! The report answers what the runtime did in a bounded time window and
//! where it is failing: event volume by kind, session ends by reason,
//! turn outcomes and provider error classes, tool failures by tool,
//! media items with or without a transcript, evidence-gate verdicts and
//! the store/artifact footprint. It is a pure projection over the log —
//! no server, no write, no counter read outside `--db`. Content-bearing
//! payloads are counted, never printed: a finding names the session and
//! seq that evidence it, so the operator reads the row itself.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::SessionId;
use kian_store::EventStore;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::commands::{self, CmdResult};
use crate::config::ServeConfig;
use crate::{cache_metrics, usage_metrics};

/// Default window: the last hour.
pub const DEFAULT_WINDOW_MS: u64 = 60 * 60 * 1000;

/// The most findings one report carries.
pub const FINDINGS_CAP: usize = 50;

const TOP_FAILING_CAP: usize = 10;

/// A finding fires at this many occurrences (one-off noise stays out).
const FINDING_THRESHOLD: u64 = 3;

const DETAIL_CAP: usize = 200;

const MEDIA_SCAN_DEPTH: usize = 8;

/// The kinds the report reads. Everything else is still counted by the
/// volume query; only these carry the sections' semantics.
pub const INSPECT_KINDS: &[&str] = &[
    "input/admitted",
    "turn/end",
    "session/created",
    "session/ended",
    "tool/call",
    "tool/result",
    "evidence/verdict",
    "channel/delivered",
];

/// `--window` grammar: an integer plus a unit (`s`/`m`/`h`/`d`),
/// e.g. `30m`. A bare integer is refused: guessing a unit is exactly the
/// silent mistake this surface must not make.
pub fn parse_window(raw: &str) -> Result<u64, String> {
    let raw = raw.trim();
    let (digits, unit_ms) = if let Some(v) = raw.strip_suffix('s') {
        (v, 1_000u64)
    } else if let Some(v) = raw.strip_suffix('m') {
        (v, 60_000)
    } else if let Some(v) = raw.strip_suffix('h') {
        (v, 3_600_000)
    } else if let Some(v) = raw.strip_suffix('d') {
        (v, 86_400_000)
    } else {
        return Err(format!(
            "--window `{raw}` needs a unit (s, m, h, d), e.g. 30m or 24h"
        ));
    };
    let n: u64 = digits
        .parse()
        .map_err(|_| format!("--window `{raw}` has no integer before its unit"))?;
    if n == 0 {
        return Err("--window must be greater than zero".into());
    }
    n.checked_mul(unit_ms)
        .ok_or_else(|| format!("--window `{raw}` is too large"))
}

pub struct Options<'a> {
    pub config_path: &'a Path,
    pub db: Option<&'a Path>,
    pub since_ms: u64,
    pub until_ms: u64,
    pub session: Option<SessionId>,
}

/// Everything the report is a pure function of, plus the slice it folds.
pub struct Loaded {
    pub facts: Facts,
    pub events: Vec<Event>,
    pub counts: Vec<(String, u64)>,
}

/// `kian self inspect` — assembles the report (read-only embedded).
pub async fn run(opts: Options<'_>, json_out: bool) -> CmdResult<String> {
    let loaded = load(&opts).await?;
    let report = build_report(&loaded.facts, &loaded.events, &loaded.counts);
    if json_out {
        return serde_json::to_string_pretty(&report).map_err(|e| e.to_string());
    }
    Ok(render(&report))
}

/// The read half of [`run`]: open read-only, take the slice, close. The
/// evidence pack (SPEC §3.1) folds the same `Facts` over the same store, so
/// the aggregate view and the package cannot disagree about what the stream
/// says.
pub async fn load(opts: &Options<'_>) -> CmdResult<Loaded> {
    let db = commands::resolve_db(opts.db, opts.config_path)?;
    let cfg = ServeConfig::load(opts.config_path).ok();
    let store = crate::commands::open_reader(&db).await?;

    let (events, counts) = match opts.session {
        Some(session) => {
            let stream = store
                .rebuild(session)
                .await
                .map_err(|e| format!("read events for {}: {e}", session.0))?;
            if stream.is_empty() {
                return Err(format!(
                    "unknown session {} (no events in this store)",
                    session.0
                ));
            }
            let window: Vec<Event> = stream
                .into_iter()
                .filter(|e| e.ts >= opts.since_ms && e.ts <= opts.until_ms)
                .collect();
            let counts = kind_counts(&window);
            (window, counts)
        }
        None => {
            let events = store
                .events_in_window(INSPECT_KINDS, opts.since_ms, opts.until_ms)
                .await
                .map_err(|e| format!("read events in window: {e}"))?;
            let counts = store
                .event_kind_counts(opts.since_ms, opts.until_ms)
                .await
                .map_err(|e| format!("count events in window: {e}"))?;
            (events, counts)
        }
    };

    let facts = facts_for(
        &store,
        &db,
        opts.config_path,
        cfg.as_ref(),
        opts.session,
        opts.since_ms,
        opts.until_ms,
    )
    .await;
    store.close().await;
    Ok(Loaded {
        facts,
        events,
        counts,
    })
}

/// The kind volume of one event slice, in key order — `by_kind` in the
/// report, derived from the schema's own kind names.
pub fn kind_counts(events: &[Event]) -> Vec<(String, u64)> {
    let mut by_kind: BTreeMap<String, u64> = BTreeMap::new();
    for event in events {
        *by_kind.entry(event.kind.as_str().to_string()).or_default() += 1;
    }
    by_kind.into_iter().collect()
}

/// The facts a report is pure over: store/config identity, the window bounds
/// and the usage/cache projections. The caller keeps the store open across
/// the call and closes it afterwards.
pub async fn facts_for(
    store: &EventStore,
    db: &Path,
    config_path: &Path,
    cfg: Option<&ServeConfig>,
    session: Option<SessionId>,
    since_ms: u64,
    until_ms: u64,
) -> Facts {
    let views_enabled = cfg.map(|c| c.read_model.views.enabled).unwrap_or(false);
    let usage = match usage_metrics::usage_stats_report(store, session, views_enabled).await {
        Ok(report) => {
            serde_json::to_value(&report).unwrap_or_else(|e| json!({"error": e.to_string()}))
        }
        Err(e) => json!({"error": e.to_string()}),
    };
    let cache = match cache_metrics::cache_stats_report(store, session, views_enabled).await {
        Ok(report) => {
            serde_json::to_value(&report).unwrap_or_else(|e| json!({"error": e.to_string()}))
        }
        Err(e) => json!({"error": e.to_string()}),
    };

    let artifacts = cfg
        .and_then(|c| {
            c.artifacts
                .as_ref()
                .and_then(|a| a.resolve_dir(&c.storage).ok())
        })
        .filter(|dir| dir.exists())
        .map(|dir| {
            let (count, bytes) = dir_footprint(&dir);
            ArtifactFootprint {
                dir: dir.display().to_string(),
                count,
                bytes,
            }
        });

    Facts {
        version: env!("CARGO_PKG_VERSION").to_string(),
        store: "sqlite".to_string(),
        db_path: db.display().to_string(),
        config_path: config_path.display().to_string(),
        config_sha256: std::fs::read(config_path)
            .ok()
            .map(|bytes| hex(Sha256::digest(&bytes).as_slice())),
        since_ms,
        until_ms,
        storage: StorageSec {
            db_bytes: file_len(db),
            wal_bytes: file_len(&wal_path(db)),
            artifacts,
        },
        usage,
        cache,
    }
}

fn wal_path(db: &Path) -> PathBuf {
    let mut raw = db.as_os_str().to_os_string();
    raw.push("-wal");
    PathBuf::from(raw)
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn dir_footprint(dir: &Path) -> (u64, u64) {
    let mut count = 0u64;
    let mut bytes = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() {
                stack.push(entry.path());
            } else if kind.is_file() {
                count += 1;
                bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    (count, bytes)
}

/// The facts the aggregation is pure over — everything else comes from
/// the event slice. A test can build one without touching a store.
pub struct Facts {
    pub version: String,
    pub store: String,
    pub db_path: String,
    pub config_path: String,
    pub config_sha256: Option<String>,
    pub since_ms: u64,
    pub until_ms: u64,
    pub storage: StorageSec,
    pub usage: Value,
    pub cache: Value,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub window: WindowSec,
    pub identity: IdentitySec,
    pub events: EventsSec,
    pub sessions: SessionsSec,
    pub turns: TurnsSec,
    pub tools: ToolsSec,
    pub media: MediaSec,
    pub evidence: EvidenceSec,
    pub channels: ChannelsSec,
    pub lifetime: LifetimeSec,
    pub storage: StorageSec,
    pub findings: Vec<Finding>,
}

/// The existing usage/cache projections: lifetime totals, never windowed.
#[derive(Debug, Serialize)]
pub struct LifetimeSec {
    pub usage: Value,
    pub cache: Value,
}

#[derive(Debug, Serialize)]
pub struct WindowSec {
    pub since_ms: u64,
    pub until_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed: Option<Observed>,
}

/// The first/last `ts` the slice actually carried — the honest bound for
/// a diagnostic window (one query, not a snapshot).
#[derive(Debug, Serialize)]
pub struct Observed {
    pub first_ts_ms: u64,
    pub last_ts_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct IdentitySec {
    pub version: String,
    pub store: String,
    pub db_path: String,
    pub config_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_sha256: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct EventsSec {
    pub total: u64,
    pub by_kind: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize)]
pub struct SessionsSec {
    pub started: u64,
    pub ended: u64,
    pub ended_by_reason: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize)]
pub struct TurnsSec {
    pub outcomes: BTreeMap<String, u64>,
    pub error_classes: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize)]
pub struct ToolsSec {
    pub calls: u64,
    pub errors: u64,
    pub top_failing: Vec<ToolFailure>,
}

#[derive(Debug, Serialize)]
pub struct ToolFailure {
    pub tool: String,
    pub calls: u64,
    pub errors: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct MediaSec {
    pub items: u64,
    pub transcribed: u64,
    pub failed: u64,
}

#[derive(Debug, Serialize)]
pub struct EvidenceSec {
    pub gates: u64,
    pub failed: u64,
}

#[derive(Debug, Serialize)]
pub struct ChannelsSec {
    pub delivered: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageSec {
    pub db_bytes: u64,
    pub wal_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifacts: Option<ArtifactFootprint>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ArtifactFootprint {
    pub dir: String,
    pub count: u64,
    pub bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct Finding {
    pub code: String,
    pub severity: String,
    pub session: String,
    pub seq: u64,
    pub detail: String,
}

/// The v0 finding vocabulary (closed set: a new signal earns a code and
/// a test here, never an ad-hoc string).
pub const FINDING_MEDIA_ERROR: &str = "media_error";
pub const FINDING_TURN_ERROR: &str = "turn_error";
pub const FINDING_TOOL_ERROR: &str = "tool_error";
pub const FINDING_SESSION_FAILED: &str = "session_failed";
pub const FINDING_EVIDENCE_FAILED: &str = "evidence_failed";

/// Folds the window slice into the report — pure, deterministic.
pub fn build_report(facts: &Facts, events: &[Event], counts: &[(String, u64)]) -> Report {
    let mut sessions_started = 0u64;
    let mut ended_by_reason: BTreeMap<String, u64> = BTreeMap::new();
    let mut outcomes: BTreeMap<String, u64> = BTreeMap::new();
    let mut error_classes: BTreeMap<String, u64> = BTreeMap::new();
    let mut tool_calls = 0u64;
    let mut tool_errors = 0u64;
    let mut call_names: BTreeMap<String, String> = BTreeMap::new();
    let mut calls_by_tool: BTreeMap<String, u64> = BTreeMap::new();
    let mut errors_by_tool: BTreeMap<String, u64> = BTreeMap::new();
    let mut unresolved_tool_errors = 0u64;
    let mut media = MediaSec::default();
    let mut gates = 0u64;
    let mut gates_failed = 0u64;
    let mut delivered = 0u64;
    let mut findings: Vec<Finding> = Vec::new();

    for event in events {
        let session = event.session.0.to_string();
        match &event.kind {
            EventKind::SessionCreated => sessions_started += 1,
            EventKind::SessionEnded { reason } => {
                let name = end_reason_str(reason).to_string();
                *ended_by_reason.entry(name).or_default() += 1;
                if matches!(reason, EndReason::Failed) {
                    findings.push(Finding {
                        code: FINDING_SESSION_FAILED.into(),
                        severity: "error".into(),
                        session,
                        seq: event.seq,
                        detail: "session ended with reason `failed`".into(),
                    });
                }
            }
            EventKind::TurnEnd => {
                let outcome = event
                    .bytes
                    .get("outcome")
                    .and_then(Value::as_str)
                    .unwrap_or("unspecified");
                *outcomes.entry(outcome.to_string()).or_default() += 1;
                if let Some(class) = event
                    .bytes
                    .get("error")
                    .and_then(|error| error.get("class"))
                    .and_then(Value::as_str)
                {
                    *error_classes.entry(class.to_string()).or_default() += 1;
                }
            }
            EventKind::ToolCall => {
                tool_calls += 1;
                let content = event.bytes.get("content");
                let id = content.and_then(|c| c.get("id")).and_then(Value::as_str);
                let name = content.and_then(|c| c.get("name")).and_then(Value::as_str);
                if let (Some(id), Some(name)) = (id, name) {
                    call_names.insert(id.to_string(), name.to_string());
                    *calls_by_tool.entry(name.to_string()).or_default() += 1;
                }
            }
            EventKind::ToolResult => {
                let content = event.bytes.get("content");
                let failed = content
                    .and_then(|c| c.get("is_error"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if failed {
                    tool_errors += 1;
                    let call_id = content
                        .and_then(|c| c.get("call_id"))
                        .and_then(Value::as_str);
                    match call_id.and_then(|id| call_names.get(id)) {
                        Some(tool) => {
                            *errors_by_tool.entry(tool.clone()).or_default() += 1;
                        }
                        None => unresolved_tool_errors += 1,
                    }
                }
            }
            EventKind::InputAdmitted => {
                collect_media(
                    &event.bytes,
                    0,
                    &mut media,
                    &mut findings,
                    &session,
                    event.seq,
                );
            }
            EventKind::EvidenceVerdict => {
                gates += 1;
                let passed = event
                    .bytes
                    .get("passed")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if !passed {
                    gates_failed += 1;
                    let gate = event
                        .bytes
                        .get("gate")
                        .and_then(Value::as_str)
                        .unwrap_or("unnamed");
                    findings.push(Finding {
                        code: FINDING_EVIDENCE_FAILED.into(),
                        severity: "warn".into(),
                        session,
                        seq: event.seq,
                        detail: format!("evidence gate `{gate}` did not pass"),
                    });
                }
            }
            EventKind::ChannelDelivered => delivered += 1,
            _ => {}
        }
    }

    for (class, count) in &error_classes {
        if *count >= FINDING_THRESHOLD {
            findings.push(Finding {
                code: FINDING_TURN_ERROR.into(),
                severity: "warn".into(),
                session: String::new(),
                seq: 0,
                detail: format!("{count} turn(s) ended with provider error class `{class}`"),
            });
        }
    }
    for (tool, errors) in &errors_by_tool {
        if *errors >= FINDING_THRESHOLD {
            findings.push(Finding {
                code: FINDING_TOOL_ERROR.into(),
                severity: "warn".into(),
                session: String::new(),
                seq: 0,
                detail: format!("tool `{tool}` failed {errors} time(s)"),
            });
        }
    }
    if unresolved_tool_errors > 0 {
        findings.push(Finding {
            code: FINDING_TOOL_ERROR.into(),
            severity: "info".into(),
            session: String::new(),
            seq: 0,
            detail: format!(
                "{unresolved_tool_errors} failed tool result(s) name no call in the window"
            ),
        });
    }

    findings.sort_by(|a, b| {
        severity_rank(&a.severity)
            .cmp(&severity_rank(&b.severity))
            .then_with(|| a.session.cmp(&b.session))
            .then_with(|| a.seq.cmp(&b.seq))
            .then_with(|| a.code.cmp(&b.code))
    });
    findings.truncate(FINDINGS_CAP);

    let mut by_kind = BTreeMap::new();
    let mut total = 0u64;
    for (kind, count) in counts {
        if *count == 0 {
            continue;
        }
        *by_kind.entry(kind.clone()).or_default() += count;
        total += count;
    }

    let mut top_failing: Vec<ToolFailure> = errors_by_tool
        .iter()
        .map(|(tool, errors)| ToolFailure {
            tool: tool.clone(),
            calls: calls_by_tool.get(tool).copied().unwrap_or(0),
            errors: *errors,
        })
        .collect();
    top_failing.sort_by(|a, b| b.errors.cmp(&a.errors).then_with(|| a.tool.cmp(&b.tool)));
    top_failing.truncate(TOP_FAILING_CAP);

    let observed = events
        .iter()
        .map(|e| e.ts)
        .min()
        .zip(events.iter().map(|e| e.ts).max())
        .map(|(first_ts_ms, last_ts_ms)| Observed {
            first_ts_ms,
            last_ts_ms,
        });

    Report {
        window: WindowSec {
            since_ms: facts.since_ms,
            until_ms: facts.until_ms,
            observed,
        },
        identity: IdentitySec {
            version: facts.version.clone(),
            store: facts.store.clone(),
            db_path: facts.db_path.clone(),
            config_path: facts.config_path.clone(),
            config_sha256: facts.config_sha256.clone(),
        },
        events: EventsSec { total, by_kind },
        sessions: SessionsSec {
            started: sessions_started,
            ended: ended_by_reason.values().sum(),
            ended_by_reason,
        },
        turns: TurnsSec {
            outcomes,
            error_classes,
        },
        tools: ToolsSec {
            calls: tool_calls,
            errors: tool_errors,
            top_failing,
        },
        media,
        evidence: EvidenceSec {
            gates,
            failed: gates_failed,
        },
        channels: ChannelsSec { delivered },
        lifetime: LifetimeSec {
            usage: facts.usage.clone(),
            cache: facts.cache.clone(),
        },
        storage: StorageSec {
            db_bytes: facts.storage.db_bytes,
            wal_bytes: facts.storage.wal_bytes,
            artifacts: facts.storage.artifacts.clone(),
        },
        findings,
    }
}

fn severity_rank(severity: &str) -> u8 {
    match severity {
        "error" => 0,
        "warn" => 1,
        _ => 2,
    }
}

pub(crate) fn end_reason_str(reason: &EndReason) -> &'static str {
    match reason {
        EndReason::Completed => "completed",
        EndReason::Cancelled => "cancelled",
        EndReason::Interrupted => "interrupted",
        EndReason::Failed => "failed",
        EndReason::AwaitingInput => "awaiting_input",
        EndReason::AwaitingBudget => "awaiting_budget",
        EndReason::Rotated => "rotated",
    }
}

/// Collects `media` arrays anywhere in the payload (bounded depth) — the
/// channel edge nests them under its own namespace, the ADR-062 envelope
/// shape is `{kind, media_type, artifact, transcript|error}`.
fn collect_media(
    value: &Value,
    depth: usize,
    media: &mut MediaSec,
    findings: &mut Vec<Finding>,
    session: &str,
    seq: u64,
) {
    if depth > MEDIA_SCAN_DEPTH {
        return;
    }
    match value {
        Value::Object(map) => {
            if let Some(Value::Array(items)) = map.get("media") {
                for item in items {
                    if item.get("kind").and_then(Value::as_str).is_none() {
                        continue;
                    }
                    media.items += 1;
                    if let Some(error) = item.get("error").and_then(Value::as_str) {
                        media.failed += 1;
                        findings.push(Finding {
                            code: FINDING_MEDIA_ERROR.into(),
                            severity: "error".into(),
                            session: session.to_string(),
                            seq,
                            detail: truncate(error),
                        });
                    } else if item
                        .get("transcript")
                        .and_then(Value::as_str)
                        .is_some_and(|text| !text.trim().is_empty())
                    {
                        media.transcribed += 1;
                    }
                }
            }
            for inner in map.values() {
                collect_media(inner, depth + 1, media, findings, session, seq);
            }
        }
        Value::Array(items) => {
            for inner in items {
                collect_media(inner, depth + 1, media, findings, session, seq);
            }
        }
        _ => {}
    }
}

fn truncate(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.chars().count() <= DETAIL_CAP {
        return trimmed.to_string();
    }
    let head: String = trimmed.chars().take(DETAIL_CAP).collect();
    format!("{head}…")
}

pub(crate) fn iso(ms: u64) -> String {
    let millis = i64::try_from(ms).unwrap_or(i64::MAX);
    match DateTime::<Utc>::from_timestamp_millis(millis) {
        Some(at) => at.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        None => format!("{ms}ms"),
    }
}

pub(crate) fn human_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    let raw = bytes as f64;
    if raw >= KIB * KIB * KIB {
        format!("{:.1} GiB", raw / (KIB * KIB * KIB))
    } else if raw >= KIB * KIB {
        format!("{:.1} MiB", raw / (KIB * KIB))
    } else if raw >= KIB {
        format!("{:.1} KiB", raw / KIB)
    } else {
        format!("{bytes} B")
    }
}

fn map_line(map: &BTreeMap<String, u64>) -> String {
    if map.is_empty() {
        return "-".into();
    }
    map.iter()
        .map(|(key, count)| format!("{key}={count}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Renders the report for a terminal: one line per section, findings
/// last (a pure function of the report — the JSON stays the schema).
pub fn render(report: &Report) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "window      {} .. {} ({}ms)\n",
        iso(report.window.since_ms),
        iso(report.window.until_ms),
        report
            .window
            .until_ms
            .saturating_sub(report.window.since_ms)
    ));
    match &report.window.observed {
        Some(observed) => out.push_str(&format!(
            "observed    {} .. {}\n",
            iso(observed.first_ts_ms),
            iso(observed.last_ts_ms)
        )),
        None => out.push_str("observed    nothing recorded in the window\n"),
    }
    out.push_str(&format!(
        "identity    kian {} / {} / {}\n",
        report.identity.version, report.identity.store, report.identity.db_path
    ));
    match &report.identity.config_sha256 {
        Some(sha) => out.push_str(&format!(
            "config      {} (sha256 {})\n",
            report.identity.config_path,
            &sha[..12.min(sha.len())]
        )),
        None => out.push_str(&format!("config      {}\n", report.identity.config_path)),
    }
    out.push_str(&format!(
        "events      {} across {} kind(s): {}\n",
        report.events.total,
        report.events.by_kind.len(),
        map_line(&report.events.by_kind)
    ));
    out.push_str(&format!(
        "sessions    {} started, {} ended: {}\n",
        report.sessions.started,
        report.sessions.ended,
        map_line(&report.sessions.ended_by_reason)
    ));
    out.push_str(&format!(
        "turns       outcomes: {}; errors: {}\n",
        map_line(&report.turns.outcomes),
        map_line(&report.turns.error_classes)
    ));
    out.push_str(&format!(
        "tools       {} call(s), {} error(s)\n",
        report.tools.calls, report.tools.errors
    ));
    for failure in &report.tools.top_failing {
        out.push_str(&format!(
            "  {:<24} {} call(s), {} error(s)\n",
            failure.tool, failure.calls, failure.errors
        ));
    }
    out.push_str(&format!(
        "media       {} item(s), {} transcribed, {} failed\n",
        report.media.items, report.media.transcribed, report.media.failed
    ));
    out.push_str(&format!(
        "evidence    {} gate(s), {} failed\n",
        report.evidence.gates, report.evidence.failed
    ));
    out.push_str(&format!(
        "channels    {} delivered\n",
        report.channels.delivered
    ));
    match &report.storage.artifacts {
        Some(artifacts) => out.push_str(&format!(
            "storage     db {}, wal {}; artifacts {} file(s) in {} ({})\n",
            human_bytes(report.storage.db_bytes),
            human_bytes(report.storage.wal_bytes),
            artifacts.count,
            artifacts.dir,
            human_bytes(artifacts.bytes)
        )),
        None => out.push_str(&format!(
            "storage     db {}, wal {}\n",
            human_bytes(report.storage.db_bytes),
            human_bytes(report.storage.wal_bytes)
        )),
    }
    if report.findings.is_empty() {
        out.push_str("findings    none\n");
    } else {
        out.push_str(&format!("findings    {}\n", report.findings.len()));
        for finding in &report.findings {
            let where_ = if finding.session.is_empty() {
                "-".to_string()
            } else {
                format!("{} seq {}", finding.session, finding.seq)
            };
            out.push_str(&format!(
                "  {:<5} {:<16} {:<45} {}\n",
                finding.severity.to_uppercase(),
                finding.code,
                where_,
                finding.detail
            ));
        }
    }
    out
}
#[cfg(test)]
mod tests {
    use super::*;
    use kian_core::events::SCHEMA_VERSION;
    use kian_core::model::Epoch;
    use uuid::Uuid;

    fn event(seq: u64, ts: u64, kind: EventKind, bytes: Value) -> Event {
        Event {
            seq,
            session: SessionId(Uuid::nil()),
            epoch: Epoch(1),
            ts,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        }
    }

    fn facts() -> Facts {
        Facts {
            version: "test".into(),
            store: "sqlite".into(),
            db_path: "/tmp/kian.db".into(),
            config_path: "/tmp/kian.yaml".into(),
            config_sha256: None,
            since_ms: 1_000,
            until_ms: 2_000,
            storage: StorageSec {
                db_bytes: 10,
                wal_bytes: 0,
                artifacts: None,
            },
            usage: json!({"turns": 1}),
            cache: json!({"hit_rate": 0.5}),
        }
    }

    fn report_of(events: &[Event]) -> Report {
        build_report(&facts(), events, &[])
    }

    #[test]
    fn parse_window_accepts_units_and_refuses_bare_numbers() {
        assert_eq!(parse_window("30s"), Ok(30_000));
        assert_eq!(parse_window("5m"), Ok(300_000));
        assert_eq!(parse_window("2h"), Ok(7_200_000));
        assert_eq!(parse_window("1d"), Ok(86_400_000));
        assert!(parse_window("60").is_err());
        assert!(parse_window("0h").is_err());
        assert!(parse_window("h").is_err());
        assert!(parse_window("").is_err());
    }

    #[test]
    fn empty_window_reports_zeros() {
        let report = report_of(&[]);
        assert_eq!(report.events.total, 0);
        assert_eq!(report.sessions.ended, 0);
        assert!(report.turns.outcomes.is_empty());
        assert_eq!(report.tools.calls, 0);
        assert_eq!(report.media.items, 0);
        assert_eq!(report.channels.delivered, 0);
        assert!(report.findings.is_empty());
        assert!(report.window.observed.is_none());
    }

    #[test]
    fn counts_sections_from_the_slice() {
        let events = vec![
            event(1, 1_100, EventKind::SessionCreated, json!({})),
            event(
                2,
                1_200,
                EventKind::TurnEnd,
                json!({"outcome": "final_answer"}),
            ),
            event(
                3,
                1_300,
                EventKind::TurnEnd,
                json!({"iteration": 1, "error": {"class": "provider_down", "action": "retry"}}),
            ),
            event(
                4,
                1_400,
                EventKind::ToolCall,
                json!({"content": {"id": "c1", "name": "exec", "arguments": "{}"}}),
            ),
            event(
                5,
                1_450,
                EventKind::ToolResult,
                json!({"content": {"call_id": "c1", "is_error": true, "output": "boom"}}),
            ),
            event(
                6,
                1_500,
                EventKind::ChannelDelivered,
                json!({"channel": "imessage"}),
            ),
        ];
        let report = report_of(&events);
        assert_eq!(report.sessions.started, 1);
        assert_eq!(report.turns.outcomes.get("final_answer"), Some(&1));
        assert_eq!(report.turns.outcomes.get("unspecified"), Some(&1));
        assert_eq!(report.turns.error_classes.get("provider_down"), Some(&1));
        assert_eq!(report.tools.calls, 1);
        assert_eq!(report.tools.errors, 1);
        assert_eq!(report.channels.delivered, 1);
        assert_eq!(
            report.window.observed.as_ref().map(|o| o.first_ts_ms),
            Some(1_100)
        );
    }

    #[test]
    fn media_error_is_a_finding_and_transcripts_are_counted() {
        let events = vec![event(
            7,
            1_100,
            EventKind::InputAdmitted,
            json!({"content": {"text": "[image]",
            "kian": {"raw": {"media": [
                {"kind": "image", "media_type": "image/jpeg", "artifact": "a", "size": 10, "transcript": "a tower"},
                {"kind": "image", "media_type": "image/heic", "error": "image_ocr failed with HTTP 400"}
            ]}}}}),
        )];
        let report = report_of(&events);
        assert_eq!(report.media.items, 2);
        assert_eq!(report.media.transcribed, 1);
        assert_eq!(report.media.failed, 1);
        assert_eq!(report.findings.len(), 1);
        assert_eq!(report.findings[0].code, "media_error");
        assert_eq!(report.findings[0].severity, "error");
        assert_eq!(report.findings[0].seq, 7);
        assert_eq!(report.findings[0].detail, "image_ocr failed with HTTP 400");
    }

    #[test]
    fn session_failed_is_an_error_finding_but_interrupted_is_not() {
        let events = vec![
            event(
                8,
                1_100,
                EventKind::SessionEnded {
                    reason: EndReason::Failed,
                },
                json!({}),
            ),
            event(
                9,
                1_200,
                EventKind::SessionEnded {
                    reason: EndReason::Interrupted,
                },
                json!({}),
            ),
        ];
        let report = report_of(&events);
        assert_eq!(report.sessions.ended, 2);
        assert_eq!(report.sessions.ended_by_reason.get("failed"), Some(&1));
        assert_eq!(report.sessions.ended_by_reason.get("interrupted"), Some(&1));
        assert_eq!(report.findings.len(), 1);
        assert_eq!(report.findings[0].code, "session_failed");
    }

    #[test]
    fn repeated_turn_and_tool_errors_become_findings_at_the_threshold() {
        let mut events = Vec::new();
        for seq in 1..=3u64 {
            events.push(event(
                seq,
                1_000 + seq,
                EventKind::TurnEnd,
                json!({"error": {"class": "provider_down"}}),
            ));
            events.push(event(
                seq + 10,
                1_000 + seq,
                EventKind::ToolCall,
                json!({"content": {"id": format!("c{seq}"), "name": "exec"}}),
            ));
            events.push(event(
                seq + 20,
                1_000 + seq,
                EventKind::ToolResult,
                json!({"content": {"call_id": format!("c{seq}"), "is_error": true}}),
            ));
        }
        let report = report_of(&events);
        let codes: Vec<&str> = report.findings.iter().map(|f| f.code.as_str()).collect();
        assert_eq!(codes.iter().filter(|c| **c == "turn_error").count(), 1);
        assert_eq!(codes.iter().filter(|c| **c == "tool_error").count(), 1);
        assert_eq!(report.tools.top_failing.len(), 1);
        assert_eq!(report.tools.top_failing[0].tool, "exec");
        assert_eq!(report.tools.top_failing[0].errors, 3);

        let two = events
            .iter()
            .filter(|e| e.seq == 3)
            .cloned()
            .collect::<Vec<_>>();
        let report = report_of(&two);
        assert!(report.findings.iter().all(|f| f.code != "turn_error"));
    }

    #[test]
    fn findings_are_capped_and_error_first() {
        let mut events = Vec::new();
        for seq in 1..=80u64 {
            events.push(event(
                seq,
                1_000 + seq,
                EventKind::InputAdmitted,
                json!({"content": {"media": [{"kind": "audio", "error": "audio_transcribe failed"}]}}),
            ));
        }
        let report = report_of(&events);
        assert_eq!(report.findings.len(), FINDINGS_CAP);
        assert!(report.findings.iter().all(|f| f.severity == "error"));
        let seqs: Vec<u64> = report.findings.iter().map(|f| f.seq).collect();
        let mut sorted = seqs.clone();
        sorted.sort_unstable();
        assert_eq!(seqs, sorted);
    }

    #[test]
    fn evidence_verdicts_count_gates_and_failures() {
        let events = vec![
            event(
                1,
                1_100,
                EventKind::EvidenceVerdict,
                json!({"gate": "stop", "passed": true}),
            ),
            event(
                2,
                1_200,
                EventKind::EvidenceVerdict,
                json!({"gate": "stop", "passed": false}),
            ),
        ];
        let report = report_of(&events);
        assert_eq!(report.evidence.gates, 2);
        assert_eq!(report.evidence.failed, 1);
        assert_eq!(report.findings[0].code, "evidence_failed");
    }

    #[test]
    fn render_is_stable_and_never_prints_transcripts() {
        let events = vec![event(
            3,
            1_100,
            EventKind::InputAdmitted,
            json!({"content": {"media": [{"kind": "image", "transcript": "secret text"}]}}),
        )];
        let text = render(&report_of(&events));
        assert!(text.contains("media       1 item(s), 1 transcribed, 0 failed"));
        assert!(!text.contains("secret text"));
    }
}
