//! The operational session commands: `inspect` / `replay` /
//! `export` / `wake-now` / `lease inspect` / `recovery budget reset`
//! (SPEC §3.2), plus `tickets` — the Ticket stage board.
//!
//! Two access modes, by card design:
//!
//! - **Embedded reads** (`inspect`, `replay`, `export`, `lease
//!   inspect`): open the sqlite file **read-only in-process**
//!   ([`kian_store::EventStore::open_read_only`]) — the CLI never
//!   holds a write path. All state is derived from the event stream
//!   (constraint 1); nothing is cached.
//! - **Server actions** (`wake-now`, `recovery budget reset`): talk to
//!   a running `kian serve` over HTTP via [`crate::client`] —
//!   `POST /ops/rpc` (the operational plane). Mutations never touch
//!   the store directly from the CLI process.
//!
//! Resource resolution (fail-closed, both modes): an explicit `--db` /
//! `--url` wins; otherwise the value comes from `kian.yaml` (`--config`,
//! default `./kian.yaml` — `storage.sqlite` for reads, `listen` for
//! actions). A missing config is a clear error, never a guess.
//!
//! # `--json` output schemas (frozen for scripting)
//!
//! Every command prints human-readable output by default and a stable
//! JSON document with `--json`:
//!
//! **`session inspect <id>`**
//! ```json
//! {
//!   "session": "<uuid>",
//!   "state": "idle|running|recoverable|completed|cancelled|archived",
//!   "event_count": 12, "next_seq": 13, "epoch": 3, "last_event_ts": 1761900000000,
//!   "counters": { "inputs_admitted": 1, "drain_claims": 1, "turns": 1,
//!     "requests": 1, "assistant_messages": 1, "tool_calls": 0,
//!     "tool_results": 0, "compactions": 0, "budget_resets": 0,
//!     "pending_eligible_inputs": 0, "pending_control_inputs": 0 }
//! }
//! ```
//!
//! **`session replay <id>`** (re-derives everything from events; with
//! `--url` additionally diffs the derived state against the live
//! server projection)
//! ```json
//! {
//!   "session": "<uuid>",
//!   "event_count": 12,
//!   "derived_state": "completed",
//!   "live_state": "completed", // null without --url
//!   "state_matches_live": true, // null without --url
//!   "invariants_ok": true,
//!   "violations": [],
//!   "model_context": { // the A2 byte-identity check
//!     "requests_checked": 2, "byte_identical": 2,
//!     "mismatches": [] } // [{ "seq", "derived", "recorded" }]
//! }
//! ```
//!
//! **`session export <id> [--since S] [--until U] [--with-event-ids]`** —
//! JSONL, one frozen v0 event envelope per line (the exact stored
//! bytes), `since <= seq <= until` (inclusive; defaults: whole stream).
//! `--with-event-ids` (opt-) adds the derived `event_id` to each
//! line — presentation only, the envelope bytes and the default export
//! are unchanged, and import ignores the extra key. Import = one
//! [`kian_store::EventStore::append`] per line under each event's own
//! epoch/seq — a re-imported export projects the identical state.
//!
//! **`lease inspect`**
//! ```json
//! {
//!   "now_ms": 1761900000000,
//!   "leases": [ { "session": "<uuid>", "epoch": 3, "holder": "…",
//!                 "ttl_ms": 30000, "last_heartbeat_ms": 1761899990000,
//!                 "expires_in_ms": 22000, "expired": false } ]
//! }
//! ```
//!
//! **`tickets`** — one row per worker session, uuid-ascending
//! ```json
//! {
//!   "now_ms": 1761900000000,
//!   "tickets": [ { "session": "<uuid>", "thread": "<uuid>|null",
//!                  "topic": "pr-9|null", "stage": "working",
//!                  "thread_status": "outstanding",
//!                  "pending_inbox": 0, "age_ms": 45000, "events": 12 } ]
//! }
//! ```
//!
//! **`wake-now <id>`** → `{"session": "<uuid>", "outcome": "started|folded"}`
//!
//! **`recovery budget reset <id> [--reason S]`** →
//! `{"session": "<uuid>", "seq": 13, "epoch": 3, "ts": 1761900000000}`
//! (the appended `recovery/budget-reset` event's position; the reset
//! is an explicit event, never a silent counter wipe).

use std::path::{Path, PathBuf};

use kian_core::context::derive_model_context;
use kian_core::events::{Event, EventKind};
use kian_core::invariants;
use kian_core::model::{LeaseRecord, SessionId};
use kian_core::state::{SessionState, project};
use kian_loop::to_chat_message;
use kian_store::EventStore;
use serde_json::{Value, json};

use crate::client::KianClient;
use crate::config::ServeConfig;

/// Everything a command can fail with, as an honest printable string.
pub type CmdResult<T> = Result<T, String>;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Resource resolution (fail-closed)
// ---------------------------------------------------------------------------

/// Resolves the sqlite path: explicit `--db`, else the `--config`
/// file's `storage.sqlite`. Missing both is a clear error.
pub fn resolve_db(db: Option<&Path>, config_path: &Path) -> CmdResult<PathBuf> {
    if let Some(db) = db {
        if !db.exists() {
            return Err(format!(
                "database file `{}` does not exist (pass --db to a kian sqlite file)",
                db.display()
            ));
        }
        return Ok(db.to_path_buf());
    }
    let config = ServeConfig::load(config_path).map_err(|e| {
        format!(
            "no --db given and `{}` did not load: {e} \
             (pass --db <sqlite> or --config <kian.yaml>)",
            config_path.display()
        )
    })?;
    // postgres-mode configs carry no sqlite path (validation
    // refuses the mix) — these audit commands read sqlite files, so a
    // postgres config needs an explicit --db export.
    if config.storage.sqlite.trim().is_empty() {
        return Err(format!(
            "config `{}` selects storage.event_store: postgres and carries no sqlite path — \
             these audit commands read sqlite files; pass --db <sqlite> (or export one)",
            config_path.display()
        ));
    }
    Ok(PathBuf::from(config.storage.sqlite))
}

/// Resolves the server base URL: explicit `--url`, else the
/// `--config` file's `listen` address.
pub fn resolve_url(url: Option<&str>, config_path: &Path) -> CmdResult<String> {
    if let Some(url) = url {
        return Ok(url.trim_end_matches('/').to_string());
    }
    let config = ServeConfig::load(config_path).map_err(|e| {
        format!(
            "no --url given and `{}` did not load: {e} \
             (pass --url http://host:port or --config <kian.yaml>)",
            config_path.display()
        )
    })?;
    let addr = config
        .listen()
        .map_err(|e| format!("bad listen address in `{}`: {e}", config_path.display()))?;
    Ok(format!("http://{addr}"))
}

/// Parses a session id argument.
pub fn parse_session(raw: &str) -> CmdResult<SessionId> {
    raw.parse::<uuid::Uuid>()
        .map(SessionId)
        .map_err(|_| format!("`{raw}` is not a uuid"))
}

async fn open_reader(db: &Path) -> CmdResult<EventStore> {
    EventStore::open_read_only(db)
        .await
        .map_err(|e| format!("cannot open `{}` read-only: {e}", db.display()))
}

async fn rebuild_session(store: &EventStore, session: SessionId) -> CmdResult<Vec<Event>> {
    let events = store
        .rebuild(session)
        .await
        .map_err(|e| format!("read events for {session:?}: {e}"))?;
    if events.is_empty() {
        return Err(format!("unknown session {}", session.0));
    }
    Ok(events)
}

// ---------------------------------------------------------------------------
// session inspect — projection + counters
// ---------------------------------------------------------------------------

/// The `--json` document of `kian session inspect`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct InspectReport {
    pub session: SessionId,
    pub state: SessionState,
    pub event_count: usize,
    pub next_seq: u64,
    pub epoch: u64,
    pub last_event_ts: u64,
    pub counters: Value,
    /// usage totals, present only when the stream carries
    /// usage-bearing `turn/end` events (absent → no key, no text lines).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<crate::usage_metrics::SessionUsageReport>,
}

/// Event-kind counters, all derived from the stream (constraint 1).
fn counters(events: &[Event], pending: usize, pending_control: usize) -> Value {
    let mut c = std::collections::BTreeMap::new();
    let mut count = |name: &str, n: u64| {
        c.insert(name.to_string(), json!(n));
    };
    let kind_count = |k: &EventKind| events.iter().filter(|e| &e.kind == k).count() as u64;
    count("inputs_admitted", kind_count(&EventKind::InputAdmitted));
    count("drain_claims", kind_count(&EventKind::DrainClaimed));
    count("turns", kind_count(&EventKind::TurnStart));
    count("requests", kind_count(&EventKind::StepRequest));
    count(
        "assistant_messages",
        kind_count(&EventKind::AssistantMessage),
    );
    count("tool_calls", kind_count(&EventKind::ToolCall));
    count("tool_results", kind_count(&EventKind::ToolResult));
    count("compactions", kind_count(&EventKind::Compact));
    count("budget_resets", kind_count(&EventKind::RecoveryBudgetReset));
    count("pending_eligible_inputs", pending as u64);
    // unconsumed reserved control-envelope rows (durable interrupt
    // intents) — the "pending control" operator line.
    count("pending_control_inputs", pending_control as u64);
    json!(c)
}

/// `kian session inspect <id>`: projection + counters, read-only. An
/// archived session reports the archive marker plus the package path
/// (READ-MODEL §7) — an explicit `archived` answer, never an empty
/// projection.
pub async fn run_inspect(
    config_path: &Path,
    db: Option<&Path>,
    session: SessionId,
    json: bool,
) -> CmdResult<String> {
    let db = resolve_db(db, config_path)?;
    let store = open_reader(&db).await?;
    // Archived first: online rows are gone, so the ordinary path would
    // misreport the session as unknown.
    if let Some(m) = store
        .archived_session(session)
        .await
        .map_err(|e| format!("archive marker read: {e}"))?
    {
        store.close().await;
        let report = json!({
            "session": session.0.to_string(),
            "archived": true,
            "state": "archived",
            "package_path": m.package_path,
            "last_seq": m.last_seq,
            "event_count": m.event_count,
            "input_count": m.input_count,
            "archived_at": m.archived_at,
        });
        if json {
            return serde_json::to_string_pretty(&report).map_err(|e| e.to_string());
        }
        return Ok(format!(
            "session             {}\nstate               archived\nonline rows         removed (archived {})\npackage             {}\nevents              {} (seq 1..={})\ninputs              {}\n",
            session.0, m.archived_at, m.package_path, m.event_count, m.last_seq, m.input_count,
        ));
    }
    let events = rebuild_session(&store, session).await?;
    let state = project(&events).map_err(|e| format!("projection: {e}"))?;
    let pending_rows = store
        .eligible_inputs(session, now_ms())
        .await
        .map_err(|e| format!("inbox read: {e}"))?;
    let pending = pending_rows.len();
    // how many of the pending rows are durable interrupt intents.
    let pending_control = pending_rows
        .iter()
        .filter(|r| kian_core::control::InterruptIntent::is_control_envelope(&r.payload))
        .count();
    store.close().await;

    let last = events.last().expect("non-empty by construction");
    // usage lines only when observed (a pure projection over the
    // same events already read — no extra store round trip).
    let usage = {
        let report = crate::usage_metrics::session_usage_report(session, &events);
        (report.requests_with_usage > 0).then_some(report)
    };
    let report = InspectReport {
        session,
        state,
        event_count: events.len(),
        next_seq: last.seq + 1,
        epoch: last.epoch.0,
        last_event_ts: last.ts,
        counters: counters(&events, pending, pending_control),
        usage,
    };
    if json {
        return serde_json::to_string_pretty(&report).map_err(|e| e.to_string());
    }
    let c = &report.counters;
    let mut out = format!(
        "session             {}\nstate               {}\nevents              {} (next seq {}, epoch {})\nlast event ts       {}\ninputs admitted     {}\ndrain claims        {}\nturns               {}\nrequests            {}\nassistant messages  {}\ntool calls          {}\ntool results        {}\ncompactions         {}\nbudget resets       {}\npending inputs      {}\n",
        report.session.0,
        serde_json::to_value(report.state)
            .unwrap_or_default()
            .as_str()
            .unwrap_or("?"),
        report.event_count,
        report.next_seq,
        report.epoch,
        report.last_event_ts,
        c["inputs_admitted"],
        c["drain_claims"],
        c["turns"],
        c["requests"],
        c["assistant_messages"],
        c["tool_calls"],
        c["tool_results"],
        c["compactions"],
        c["budget_resets"],
        c["pending_eligible_inputs"],
    );
    // pending durable interrupt intents (read-only inbox view) —
    // rendered only when one exists.
    if pending_control > 0 {
        out.push_str(&format!("pending control     {pending_control}\n"));
    }
    // usage lines render only when the stream observed usage.
    if let Some(u) = &report.usage {
        out.push_str(&format!(
            "usage input tokens   {}\nusage output tokens  {}\nusage cache read     {}\nusage cache creation {}\nusage requests       {} ({} unknown)\n",
            u.input_tokens,
            u.output_tokens,
            u.cache_read_input_tokens,
            u.cache_creation_input_tokens,
            u.requests_with_usage,
            u.unknown_usage_requests,
        ));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// session replay — re-derive everything, diff against the live projection
// ---------------------------------------------------------------------------

/// One A2 byte-identity mismatch (derived vs recorded request).
#[derive(Debug, Clone, serde::Serialize)]
pub struct ContextMismatch {
    pub seq: u64,
    pub derived: String,
    pub recorded: String,
}

/// The A2 check over one session's stream.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ModelContextCheck {
    pub requests_checked: u64,
    pub byte_identical: u64,
    pub mismatches: Vec<ContextMismatch>,
}

/// For every `step/request` event, re-derives the model context from
/// the events *before* the request's `turn/start` bracket (exactly the
/// prefix the built-in loop derived, react.rs `run_iteration`) and
/// compares its serialized chat form byte-for-byte against the request
/// body's recorded `messages` (A2: the persisted bytes are the truth).
pub fn check_model_context(events: &[Event]) -> ModelContextCheck {
    let mut out = ModelContextCheck::default();
    for (i, ev) in events.iter().enumerate() {
        if !matches!(ev.kind, EventKind::StepRequest) {
            continue;
        }
        out.requests_checked += 1;
        // The loop derived from the stream as it stood BEFORE its
        // turn/start; find that bracket (the closest preceding one).
        let Some(bracket) = events[..i]
            .iter()
            .rposition(|e| matches!(e.kind, EventKind::TurnStart))
        else {
            out.mismatches.push(ContextMismatch {
                seq: ev.seq,
                derived: "(no turn/start bracket before request)".into(),
                recorded: String::new(),
            });
            continue;
        };
        let derived = match derive_model_context(&events[..bracket]) {
            Ok(msgs) => {
                // Dialect v2: tool transcripts map to the strict
                // wire shape; the per-call duplicates drop out.
                let chat: Vec<_> = msgs.iter().filter_map(to_chat_message).collect();
                // Canonical form only: the recorded body round-tripped
                // through `serde_json::Value` (lexicographic keys — the
                // frozen serialization), so both sides are compared
                // in that same canonical byte form.
                serde_json::to_value(&chat)
                    .and_then(|v| serde_json::to_string(&v))
                    .unwrap_or_default()
            }
            Err(e) => format!("(derive failed: {e})"),
        };
        let recorded = serde_json::to_string(
            &ev.bytes
                .get("body")
                .and_then(|b| b.get("messages"))
                .cloned()
                .unwrap_or(Value::Null),
        )
        .unwrap_or_default();
        if derived == recorded {
            out.byte_identical += 1;
        } else {
            out.mismatches.push(ContextMismatch {
                seq: ev.seq,
                derived,
                recorded,
            });
        }
    }
    out
}

/// Runs the invariant predicates that are stream-shaped (the ones
/// taking a single event slice); violations are collected as strings.
fn invariant_report(events: &[Event]) -> (bool, Vec<String>) {
    let mut violations = Vec::new();
    type StreamCheck = (
        &'static str,
        fn(&[Event]) -> Result<(), invariants::InvariantViolation>,
    );
    let checks: [StreamCheck; 5] = [
        ("no_half_terminated", invariants::no_half_terminated),
        ("durable_before_execute", invariants::durable_before_execute),
        (
            "promote_at_safe_boundaries",
            invariants::promote_at_safe_boundaries,
        ),
        (
            "drain_has_no_durable_identity",
            invariants::drain_has_no_durable_identity,
        ),
        (
            "interrupt_targets_active_chain",
            invariants::interrupt_targets_active_chain,
        ),
    ];
    for (name, check) in checks {
        if let Err(v) = check(events) {
            violations.push(format!("{name}: {v}"));
        }
    }
    (violations.is_empty(), violations)
}

/// `kian session replay <id>`: re-derive everything from events; with
/// `--url`, diff the derived state against the server's live
/// projection.
pub async fn run_replay(
    config_path: &Path,
    db: Option<&Path>,
    url: Option<&str>,
    session: SessionId,
    json: bool,
) -> CmdResult<String> {
    let db = resolve_db(db, config_path)?;
    let store = open_reader(&db).await?;
    // Archived: online replay is impossible (the rows are gone). Point
    // at import instead — the explicit guidance, never a silent
    // unknown-session (READ-MODEL §7).
    if let Some(m) = store
        .archived_session(session)
        .await
        .map_err(|e| format!("archive marker read: {e}"))?
    {
        store.close().await;
        return Err(format!(
            "session {} is archived (online rows removed; package {}): \
             restore it first — kian session import {} --db <fresh.sqlite>",
            session.0, m.package_path, m.package_path
        ));
    }
    let events = rebuild_session(&store, session).await?;
    store.close().await;

    let derived_state = project(&events).map_err(|e| format!("replay projection: {e}"))?;
    let model_context = check_model_context(&events);
    let (invariants_ok, violations) = invariant_report(&events);

    // Optional live diff: the server's projection vs the replayed one.
    let (live_state, state_matches_live) = match url {
        Some(url) => {
            let client = KianClient::new(resolve_url(Some(url), config_path)?);
            let live = client
                .rpc("projection", json!({"session": session.0.to_string()}))
                .await
                .map_err(|e| format!("live projection: {e}"))?;
            let live: SessionState = serde_json::from_value(
                live.get("state")
                    .cloned()
                    .ok_or("projection result missing `state`")?,
            )
            .map_err(|e| format!("decode live state: {e}"))?;
            let matches = live == derived_state;
            (Some(live), Some(matches))
        }
        None => (None, None),
    };

    let report = json!({
        "session": session.0.to_string(),
        "event_count": events.len(),
        "derived_state": derived_state,
        "live_state": live_state,
        "state_matches_live": state_matches_live,
        "invariants_ok": invariants_ok,
        "violations": violations,
        "model_context": model_context,
    });
    let ok =
        invariants_ok && model_context.mismatches.is_empty() && state_matches_live.unwrap_or(true);
    if json {
        return serde_json::to_string_pretty(&report).map_err(|e| e.to_string());
    }
    let live_line = match (&live_state, state_matches_live) {
        (Some(l), Some(m)) => format!(
            "live state          {} ({})",
            serde_json::to_value(l).unwrap_or_default(),
            if m { "matches" } else { "DIVERGES" }
        ),
        _ => "live state          (not fetched; pass --url to diff)".into(),
    };
    let invariants_line = if invariants_ok {
        "ok".to_string()
    } else {
        format!("VIOLATED ({})", violations.join("; "))
    };
    let mismatch_line = if model_context.mismatches.is_empty() {
        String::new()
    } else {
        format!(
            " — MISMATCH at seq {:?}",
            model_context
                .mismatches
                .iter()
                .map(|m| m.seq)
                .collect::<Vec<_>>()
        )
    };
    let body = format!(
        "session             {}\nevents              {}\nderived state       {}\n{live_line}\ninvariants          {invariants_line}\nmodel context       {} of {} requests byte-identical{mismatch_line}\n",
        session.0,
        events.len(),
        serde_json::to_value(derived_state).unwrap_or_default(),
        model_context.byte_identical,
        model_context.requests_checked,
    );
    Ok(if ok {
        body
    } else {
        format!("{body}replay verdict     DIVERGED\n")
    })
}

// ---------------------------------------------------------------------------
// session export — event interval → JSONL audit package
// ---------------------------------------------------------------------------

/// `kian session export <id> [--since S] [--until U] [--out F]
/// [--with-event-ids]`: writes one frozen v0 envelope per line (the
/// exact stored bytes, re-serialized identically from the envelope
/// column). `since`/`until` are inclusive seq bounds.
///
/// `--with-event-ids` (opt-) adds the derived `event_id`
/// ([`kian_core::events::event_id`]) to each line — a presentation key
/// for tooling, never part of the envelope. The default export's bytes
/// are unchanged; the flag only ever adds a key, and `import_jsonl`
/// parses the augmented lines unchanged (the envelope ignores unknown
/// keys), so the field is round-trip-safe, not export-only.
pub async fn run_export(
    config_path: &Path,
    db: Option<&Path>,
    session: SessionId,
    since: Option<u64>,
    until: Option<u64>,
    out: Option<&Path>,
    with_event_ids: bool,
) -> CmdResult<String> {
    let db = resolve_db(db, config_path)?;
    let store = open_reader(&db).await?;
    // Archived: the export MUST be byte-identical, so it is served
    // from the verified package (hashes re-checked on the way out)
    // rather than an online scan of empty tables (READ-MODEL §7).
    if let Some(m) = store
        .archived_session(session)
        .await
        .map_err(|e| format!("archive marker read: {e}"))?
    {
        store.close().await;
        return export_from_package(&m, session, since, until, out, with_event_ids).await;
    }
    let events = rebuild_session(&store, session).await?;
    store.close().await;

    let since = since.unwrap_or(1);
    let until = until.unwrap_or(u64::MAX);
    let selected: Vec<&Event> = events
        .iter()
        .filter(|e| e.seq >= since && e.seq <= until)
        .collect();
    if selected.is_empty() {
        return Err(format!(
            "no events in [{since}, {until_}] for session {sid}",
            until_ = until.min(events.iter().map(|e| e.seq).max().unwrap_or(0)),
            sid = session.0
        ));
    }

    let mut jsonl = String::new();
    for e in &selected {
        // The envelope column holds serde_json::to_string(event); a
        // canonical re-serialization of the same Event is byte-identical
        // (persist-what-you-send), so round-trips are lossless.
        // --with-event-ids re-serializes through a Value to add the
        // derived id next to the envelope fields — the default path
        // (the same bytes as before) is untouched.
        let line = if with_event_ids {
            let mut v = serde_json::to_value(e).map_err(|e| format!("serialize event: {e}"))?;
            v["event_id"] =
                serde_json::Value::String(kian_core::events::event_id(&e.session, e.seq));
            serde_json::to_string(&v).map_err(|e| format!("serialize event: {e}"))?
        } else {
            serde_json::to_string(e).map_err(|e| format!("serialize event: {e}"))?
        };
        jsonl.push_str(&line);
        jsonl.push('\n');
    }
    match out {
        Some(path) => {
            std::fs::write(path, &jsonl).map_err(|e| format!("write {}: {e}", path.display()))?;
            Ok(format!(
                "exported {} events (seq {}..={}) to {}",
                selected.len(),
                selected.first().map(|e| e.seq).unwrap_or(0),
                selected.last().map(|e| e.seq).unwrap_or(0),
                path.display()
            ))
        }
        None => {
            // JSONL to stdout: the caller (main) prints it verbatim.
            Ok(jsonl)
        }
    }
}

/// Serves `session export` for an archived session from its package:
/// the file's hashes are re-verified, then the requested `[since,
/// until]` lines are emitted with their stored bytes — byte-identical
/// to the pre-archive export by construction (the same lines, the same
/// serializer).
async fn export_from_package(
    m: &kian_store::ArchivedMarker,
    session: SessionId,
    since: Option<u64>,
    until: Option<u64>,
    out: Option<&Path>,
    with_event_ids: bool,
) -> CmdResult<String> {
    let dir = std::path::Path::new(&m.package_path);
    let manifest = crate::archive::verify_package(dir, Some(session))
        .await?
        .manifest;
    let since = since.unwrap_or(1);
    let until = until.unwrap_or(u64::MAX);
    let text = std::fs::read_to_string(dir.join("events.jsonl"))
        .map_err(|e| format!("read {}: {e}", dir.join("events.jsonl").display()))?;
    let selected: Vec<&str> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter(|line| {
            serde_json::from_str::<Event>(line)
                .map(|e| e.seq >= since && e.seq <= until)
                .unwrap_or(false)
        })
        .collect();
    if selected.is_empty() {
        return Err(format!(
            "no events in [{since}, {until_}] for session {sid} (archived range 1..={last})",
            until_ = until.min(manifest.last_seq),
            sid = session.0,
            last = manifest.last_seq
        ));
    }
    let mut jsonl = String::new();
    for &line in &selected {
        if with_event_ids {
            // Same augmentation as the online path: the derived
            // event_id is added next to the envelope fields, never
            // inside them.
            let mut v: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| format!("package line is not an envelope: {e}"))?;
            let seq = v
                .get("seq")
                .and_then(serde_json::Value::as_u64)
                .ok_or("package line carries no seq")?;
            v["event_id"] = serde_json::Value::String(kian_core::events::event_id(&session, seq));
            jsonl
                .push_str(&serde_json::to_string(&v).map_err(|e| format!("serialize event: {e}"))?);
        } else {
            jsonl.push_str(line);
        }
        jsonl.push('\n');
    }
    match out {
        Some(path) => {
            std::fs::write(path, &jsonl).map_err(|e| format!("write {}: {e}", path.display()))?;
            Ok(format!(
                "exported {} events (seq {}..={}) to {} (from archive package {})",
                selected.len(),
                since,
                until.min(manifest.last_seq),
                path.display(),
                m.package_path
            ))
        }
        None => Ok(jsonl),
    }
}

/// Re-imports a JSONL export into a **fresh** store (the round-trip
/// proof): appends each event under its own epoch/seq so the imported
/// stream projects the identical session state.
pub async fn import_jsonl(store: &EventStore, jsonl: &str) -> CmdResult<Vec<Event>> {
    let mut imported = Vec::new();
    for (line_no, line) in jsonl.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let event: Event = serde_json::from_str(line)
            .map_err(|e| format!("line {}: not an event envelope: {e}", line_no + 1))?;
        store
            .append(
                event.session,
                event.clone(),
                kian_store::AppendGuard {
                    epoch: event.epoch,
                    expected_seq: event.seq,
                },
            )
            .await
            .map_err(|e| format!("line {}: append: {e}", line_no + 1))?;
        imported.push(event);
    }
    Ok(imported)
}

// ---------------------------------------------------------------------------
// lease inspect
// ---------------------------------------------------------------------------

/// `kian lease inspect`: every claims row with epoch/TTL/holder (the
/// durable high-water marks; no liveness filtering — that is
/// `expired_leases`' job, this is the operator's view of the table).
pub async fn run_lease_inspect(
    config_path: &Path,
    db: Option<&Path>,
    json: bool,
) -> CmdResult<String> {
    let db = resolve_db(db, config_path)?;
    let store = open_reader(&db).await?;
    let leases = store
        .leases()
        .await
        .map_err(|e| format!("lease read: {e}"))?;
    store.close().await;

    let now = now_ms();
    let entries: Vec<Value> = leases
        .iter()
        .map(|l| {
            let LeaseRecord {
                session,
                epoch,
                holder,
                ttl_ms,
                last_heartbeat,
            } = l;
            let expires_in = last_heartbeat.saturating_add(*ttl_ms) as i64 - now as i64;
            json!({
                "session": session.0.to_string(),
                "epoch": epoch.0,
                "holder": holder,
                "ttl_ms": ttl_ms,
                "last_heartbeat_ms": last_heartbeat,
                "expires_in_ms": expires_in,
                "expired": expires_in < 0,
            })
        })
        .collect();
    let report = json!({"now_ms": now, "leases": entries});
    if json {
        return serde_json::to_string_pretty(&report).map_err(|e| e.to_string());
    }
    if leases.is_empty() {
        return Ok("no leases (no session has ever been claimed)\n".into());
    }
    let mut out = format!(
        "{:<38} {:>6} {:>10} {:>9} {:>14} {:>8}\n",
        "SESSION", "EPOCH", "HOLDER", "TTL(ms)", "LAST-HEARTBEAT", "EXPIRED"
    );
    for l in &leases {
        let expires_in = l.last_heartbeat.saturating_add(l.ttl_ms) as i64 - now as i64;
        out.push_str(&format!(
            "{:<38} {:>6} {:>10} {:>9} {:>14} {:>8}\n",
            l.session.0,
            l.epoch.0,
            l.holder,
            l.ttl_ms,
            l.last_heartbeat,
            if expires_in < 0 { "yes" } else { "no" }
        ));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// tickets — the Ticket stage board
// ---------------------------------------------------------------------------

/// One row of the `kian tickets` board: a worker session (one Ticket
/// v1) with its derived stage and the facts behind it.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct TicketRow {
    /// The worker session (= the Ticket v1 identity).
    pub session: SessionId,
    /// The thread this worker serves (from its `thread/started`).
    pub thread: Option<uuid::Uuid>,
    /// The spawning/served topic(s), if the channel wrapper named one.
    pub topic: Option<String>,
    /// The derived stage (pure projection, never persisted).
    pub stage: kian_superagent::TicketStage,
    /// Latest `thread/completed` status, or `outstanding`.
    pub thread_status: String,
    /// Eligible-but-unconsumed inbox rows (the queued arm's read side).
    pub pending_inbox: usize,
    /// Age in ms since the stream's first event.
    pub age_ms: u64,
    /// Event count of the worker stream.
    pub events: usize,
    /// the parked outbox deliveries named for this ticket
    /// (`<conversation> (parked after N attempts)` lines; empty when
    /// the outbox holds nothing for it).
    pub parked: Vec<String>,
}

/// `kian tickets [--db <sqlite>] [--json]` — the Ticket stage board
/// every worker session discovered by scanning `thread/started`
/// across streams (the [`kian_superagent::worker_sessions`] derivation),
/// each with its stage, topic, thread status, and age. Read-only
/// (`open_read_only`). Served from the fresh `ticket_stage` view when
/// one exists (READ-MODEL §6, P2a; `pending_inbox` and the stage's
/// dependence on it are then as of the last rebuild — the declared
/// staleness bound); the full per-stream derivation remains the honest
/// fallback. Deterministic order: session uuid ascending.
pub async fn run_tickets(config_path: &Path, db: Option<&Path>, json: bool) -> CmdResult<String> {
    let db = resolve_db(db, config_path)?;
    // The view gate (READ-MODEL §6): `read_model.views.enabled: false`
    // hides every view — full derivation only.
    let views_enabled = ServeConfig::load(config_path)
        .map(|cfg| cfg.read_model.views.enabled)
        .unwrap_or(false);
    let store = open_reader(&db).await?;
    let now = now_ms();

    let rows = if views_enabled {
        // Fresh view first; None on any doubt (stale/corrupt/absent,
        // or a row that fails to decode) falls through to the honest
        // derivation.
        match crate::views::fresh_tickets(&store).await {
            Some(entries) => match entries
                .iter()
                .map(|e| e.to_row(now))
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(rows) => rows,
                Err(_) => ticket_board(&store, now).await?,
            },
            None => ticket_board(&store, now).await?,
        }
    } else {
        ticket_board(&store, now).await?
    };
    store.close().await;

    if json {
        let report = json!({"now_ms": now, "tickets": rows});
        return serde_json::to_string_pretty(&report).map_err(|e| e.to_string());
    }
    Ok(render_tickets(&rows))
}

/// Renders the board (deterministic: a pure function of the rows).
fn render_tickets(rows: &[TicketRow]) -> String {
    if rows.is_empty() {
        return "no worker sessions (no thread/started lineage in this store)\n".into();
    }
    let mut out = format!(
        "{:<38} {:<16} {:<12} {:<16} {:>7} {:>8} {:>12}\n",
        "SESSION", "STAGE", "TOPIC", "THREAD", "PENDING", "EVENTS", "AGE"
    );
    for row in rows {
        out.push_str(&format!(
            "{:<38} {:<16} {:<12} {:<16} {:>7} {:>8} {:>12}\n",
            row.session.0,
            row.stage,
            row.topic.as_deref().unwrap_or("-"),
            row.thread
                .map(|t| t.to_string())
                .unwrap_or_else(|| "-".into()),
            row.pending_inbox,
            row.events,
            format_age(row.age_ms),
        ));
    }
    // the parked message named on the board — appended lines only
    // when something is parked (empty board output stays byte-identical
    // to the legacy shape).
    for row in rows.iter().filter(|row| !row.parked.is_empty()) {
        out.push_str(&format!(
            "parked: {} {} → {}\n",
            row.session.0,
            row.thread
                .map(|t| t.to_string())
                .unwrap_or_else(|| "-".into()),
            row.parked.join("; ")
        ));
    }
    out
}

/// The honest full-board derivation: every stream rebuilt, worker
/// discovery, stage projection per worker. This is both `kian tickets`
/// fallback **and** the `ticket_stage` view's build mechanism — one
/// function, so view == full derivation by construction (READ-MODEL
/// §6 rebuild-equivalence). Deterministic: rows in session uuid order.
/// (`pub` for the integration tests that pin that equivalence.)
pub async fn ticket_board(store: &EventStore, now_ms: u64) -> CmdResult<Vec<TicketRow>> {
    let sessions = crate::cache_metrics::sessions_with_events(store)
        .await
        .map_err(|e| format!("session listing: {e}"))?;
    let mut streams = std::collections::BTreeMap::new();
    for session in &sessions {
        let events = store
            .rebuild(*session)
            .await
            .map_err(|e| format!("read events for {session:?}: {e}"))?;
        streams.insert(*session, events);
    }
    ticket_rows(store, &streams, now_ms).await
}

/// The pure-ish core of the board derivation over already-read streams
/// (the only store access left is the per-worker pending-inbox read).
pub(crate) async fn ticket_rows(
    store: &EventStore,
    streams: &std::collections::BTreeMap<SessionId, Vec<Event>>,
    now_ms: u64,
) -> CmdResult<Vec<TicketRow>> {
    let mut all_events = Vec::new();
    for events in streams.values() {
        all_events.extend(events.iter().cloned());
    }
    // Worker discovery: `thread/started` across every stream (a
    // Router's dispatch side names the workers; worker-side lineage
    // names them too). BTreeSet ⇒ deterministic uuid order.
    let workers = kian_superagent::worker_sessions(&all_events);
    // Topic binding: the same disposable registry projection the
    // runtime rebuilds, inverted to worker → topics.
    let registry = kian_superagent::PassthroughRegistry::rebuild(&all_events);
    let mut worker_topics: std::collections::BTreeMap<SessionId, Vec<String>> =
        std::collections::BTreeMap::new();
    for topic in registry.topics() {
        for worker in registry.resolve(&topic) {
            worker_topics.entry(worker).or_default().push(topic.clone());
        }
    }

    // the real outbox view — the delivery-retry stage arm's input
    // ('s stub exemption: this construction replaces `NoOutbox`).
    // Ticket aliases per thread: the thread's own id string plus the
    // topics its worker owns (a delivery's `correlation` names either).
    // adds the conversation attribution: a live intent's ticket
    // key is the freeform correlation the model passed (the triggering
    // event id) and matches no alias — but the target conversation is
    // derivable from the worker's own stream (its admitted wrappers)
    // and its registry topics, so both paths feed the view.
    let outbox_status = kian_superagent::OutboxStatus::from_events(&all_events);
    let mut thread_aliases: std::collections::BTreeMap<uuid::Uuid, Vec<String>> =
        std::collections::BTreeMap::new();
    let mut worker_aliases: std::collections::BTreeMap<SessionId, Vec<String>> =
        std::collections::BTreeMap::new();
    let mut thread_conversations: std::collections::BTreeMap<uuid::Uuid, Vec<String>> =
        std::collections::BTreeMap::new();
    let mut worker_conversations: std::collections::BTreeMap<SessionId, Vec<String>> =
        std::collections::BTreeMap::new();
    for (worker, topics) in &worker_topics {
        let Some(events) = streams.get(worker) else {
            continue;
        };
        let thread = events
            .iter()
            .find(|e| e.kind == EventKind::ThreadStarted)
            .and_then(|e| e.bytes.get("thread_id"))
            .and_then(Value::as_str)
            .and_then(|raw| uuid::Uuid::parse_str(raw).ok());
        // Registry topics contribute both their alias form and the
        // conversations they name (a thread-key topic of the github
        // shape derives them; anything else derives none).
        let mut conversations: Vec<String> = topics
            .iter()
            .flat_map(|topic| kian_superagent::github::topic_conversation_keys(topic))
            .collect();
        // The worker's own stream is the registry-independent source:
        // every admitted wrapper names its topic on the durable log
        // (the live run had no registry binding at all —).
        conversations.extend(kian_superagent::worker_conversation_keys(events));
        conversations.sort();
        conversations.dedup();
        worker_conversations.insert(*worker, conversations.clone());
        if let Some(thread) = thread {
            let mut aliases = vec![thread.to_string()];
            aliases.extend(topics.iter().cloned());
            worker_aliases.insert(*worker, aliases.clone());
            thread_aliases.entry(thread).or_default().extend(aliases);
            thread_conversations
                .entry(thread)
                .or_default()
                .extend(conversations);
        }
    }
    // Workers the registry never bound (the live shape) still
    // get their conversations from their own streams.
    for worker in &workers {
        let entry = worker_conversations.entry(*worker).or_default();
        if let Some(events) = streams.get(worker) {
            entry.extend(kian_superagent::worker_conversation_keys(events));
            entry.sort();
            entry.dedup();
        }
        if let Some(thread) = streams.get(worker).and_then(|events| {
            events
                .iter()
                .find(|e| e.kind == EventKind::ThreadStarted)
                .and_then(|e| e.bytes.get("thread_id"))
                .and_then(Value::as_str)
                .and_then(|raw| uuid::Uuid::parse_str(raw).ok())
        }) {
            let convs = worker_conversations
                .get(worker)
                .cloned()
                .unwrap_or_default();
            thread_conversations
                .entry(thread)
                .or_default()
                .extend(convs);
        }
    }
    let outbox_view = kian_superagent::ThreadOutboxView::with_conversations(
        outbox_status.clone(),
        thread_aliases,
        thread_conversations,
    );
    let parked_lines = outbox_status.parked_lines();
    let mut rows = Vec::new();
    for worker in workers {
        let events = streams.get(&worker).cloned().unwrap_or_default();
        let pending = store
            .eligible_inputs(worker, now_ms)
            .await
            .map_err(|e| format!("inbox read: {e}"))?
            .len();
        let stage = kian_superagent::project_stage(&events, pending, &outbox_view);
        let thread = events
            .iter()
            .find(|e| e.kind == EventKind::ThreadStarted)
            .and_then(|e| e.bytes.get("thread_id"))
            .and_then(Value::as_str)
            .and_then(|raw| uuid::Uuid::parse_str(raw).ok());
        // The parked message named on the board: a worker whose
        // ticket has a pending/parked delivery shows its parked lines.
        // Attribution is alias-or-conversation: an alias hit
        // keeps the historical all-lines shape; otherwise the lines
        // are the parked deliveries of the conversations the worker's
        // own stream names — the live path, where the freeform
        // correlation ticket matches no alias.
        let alias_hit = worker_aliases
            .get(&worker)
            .is_some_and(|aliases| aliases.iter().any(|a| outbox_status.pending_for(a)));
        let parked = if alias_hit {
            parked_lines.clone()
        } else {
            let conversations: std::collections::BTreeSet<String> = worker_conversations
                .get(&worker)
                .map(|c| c.iter().cloned().collect())
                .unwrap_or_default();
            outbox_status.parked_lines_in(&conversations)
        };
        let thread_status = events
            .iter()
            .rev()
            .find(|e| e.kind == EventKind::ThreadCompleted)
            .and_then(|e| e.bytes.get("status"))
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| "outstanding".to_string());
        let first_ts = events.first().map(|e| e.ts).unwrap_or(0);
        rows.push(TicketRow {
            session: worker,
            thread,
            topic: worker_topics.get(&worker).map(|topics| topics.join(",")),
            stage,
            thread_status,
            pending_inbox: pending,
            age_ms: now_ms.saturating_sub(first_ts),
            events: events.len(),
            parked,
        });
    }
    Ok(rows)
}

/// Human age from ms, deterministic formatting: the two most
/// significant units (`2h13m`, `45s`, `0s`).
fn format_age(age_ms: u64) -> String {
    let secs = age_ms / 1000;
    let days = secs / 86_400;
    let hours = (secs % 86_400) / 3_600;
    let mins = (secs % 3_600) / 60;
    let s = secs % 60;
    if days > 0 {
        format!("{days}d{hours}h")
    } else if hours > 0 {
        format!("{hours}h{mins}m")
    } else if mins > 0 {
        format!("{mins}m{s}s")
    } else {
        format!("{s}s")
    }
}

// ---------------------------------------------------------------------------
// wake-now / recovery budget reset (via the running server)
// ---------------------------------------------------------------------------

/// `kian wake-now <id>`: operator wake via `POST /ops/rpc`.
pub async fn run_wake_now(
    config_path: &Path,
    url: Option<&str>,
    session: SessionId,
    json: bool,
) -> CmdResult<String> {
    let client = KianClient::new(resolve_url(url, config_path)?);
    let result = client
        .ops("wake_now", json!({"session": session.0.to_string()}))
        .await
        .map_err(|e| format!("wake-now: {e}"))?;
    let outcome = result
        .get("outcome")
        .and_then(Value::as_str)
        .unwrap_or("?")
        .to_string();
    if json {
        return serde_json::to_string_pretty(&json!({
            "session": session.0.to_string(),
            "outcome": outcome,
        }))
        .map_err(|e| e.to_string());
    }
    let meaning = match outcome.as_str() {
        "started" => "a drain was spawned",
        "folded" => "a drain was already in flight; the wake folded onto pendingWake",
        _ => "unknown outcome",
    };
    Ok(format!(
        "session             {}\noutcome             {outcome} ({meaning})\n",
        session.0
    ))
}

/// `kian mock-channel-send <channel> [--text S | --payload JSON]
/// [--topic T] [--url <base>] [--json]`: the demo/ injection —
/// one event onto a configured mock channel via `POST /ops/rpc`; the
/// running server's subscription engine routes it per the configured
/// rules.
pub async fn run_mock_channel_send(
    config_path: &Path,
    url: Option<&str>,
    channel: &str,
    topic: Option<&str>,
    payload: Value,
    json: bool,
) -> CmdResult<String> {
    let client = KianClient::new(resolve_url(url, config_path)?);
    let mut params = json!({"channel": channel, "payload": payload});
    if let Some(t) = topic {
        params["topic"] = json!(t);
    }
    let result = client
        .ops("mock_channel_send", params)
        .await
        .map_err(|e| format!("mock-channel-send: {e}"))?;
    if json {
        return serde_json::to_string_pretty(&result).map_err(|e| e.to_string());
    }
    let event_id = result
        .get("event_id")
        .and_then(Value::as_str)
        .unwrap_or("?");
    let topic = result.get("topic").and_then(Value::as_str).unwrap_or("-");
    Ok(format!(
        "channel              {channel}\ntopic                {topic}\nevent               {event_id}\nnext                the subscription engine routes it per kian.yaml\n"
    ))
}

/// `kian recovery budget reset <id> [--reason S]`: appends the
/// explicit `recovery/budget-reset` event via `POST /ops/rpc`.
pub async fn run_budget_reset(
    config_path: &Path,
    url: Option<&str>,
    session: SessionId,
    reason: Option<&str>,
    json: bool,
) -> CmdResult<String> {
    let client = KianClient::new(resolve_url(url, config_path)?);
    let mut params = json!({"session": session.0.to_string()});
    if let Some(r) = reason {
        params["reason"] = json!(r);
    }
    let result = client
        .ops("recovery_budget_reset", params)
        .await
        .map_err(|e| format!("budget reset: {e}"))?;
    let seq = result.get("seq").and_then(Value::as_u64).unwrap_or(0);
    let epoch = result.get("epoch").and_then(Value::as_u64).unwrap_or(0);
    let ts = result.get("ts").and_then(Value::as_u64).unwrap_or(0);
    if json {
        return serde_json::to_string_pretty(&json!({
            "session": session.0.to_string(),
            "seq": seq,
            "epoch": epoch,
            "ts": ts,
        }))
        .map_err(|e| e.to_string());
    }
    Ok(format!(
        "session             {}\nreset event         seq {seq} (epoch {epoch}, ts {ts})\nrecorded            recovery/budget-reset appended — no silent counter wipe\n",
        session.0
    ))
}

// ---------------------------------------------------------------------------
// memory verbs (round-8 plan, kian-87)
// ---------------------------------------------------------------------------

/// `kian memory create [--config <path>] [--json]`: the operator-owned
/// memory session — the existing `create_session` control method over
/// the running server, printed as the session id. Requires a
/// configured `memory:` section (the dreamer agent must exist); the
/// session is plain until a dream request lands on it.
pub async fn run_memory_create(config_path: &Path, json: bool) -> CmdResult<String> {
    let config = ServeConfig::load(config_path).map_err(|e| format!("memory create: {e}"))?;
    if config.memory.is_none() {
        return Err(
            "memory is not configured (no `memory:` section in kian.yaml) — \
             add one before creating memory sessions"
                .into(),
        );
    }
    let client = KianClient::new(resolve_url(None, config_path)?);
    let result = client
        .rpc("create_session", json!({}))
        .await
        .map_err(|e| format!("memory create: {e}"))?;
    let session = result
        .get("session")
        .and_then(Value::as_str)
        .unwrap_or("?")
        .to_string();
    if json {
        return serde_json::to_string_pretty(&json!({
            "session": session,
            "dreamer": config.memory.as_ref().expect("checked").dreamer.agent,
        }))
        .map_err(|e| e.to_string());
    }
    Ok(format!(
        "session             {session}\nnext                `kian memory dream --session {session} ...`\n"
    ))
}

/// `kian memory dream --session <uuid> --source <uuid> --from <n>
/// --to <n>`: one dream request via the `memory_dream` ops method on
/// the running server.
pub async fn run_memory_dream(
    config_path: &Path,
    url: Option<&str>,
    session: SessionId,
    source: SessionId,
    from_seq: u64,
    to_seq: u64,
    json: bool,
) -> CmdResult<String> {
    let client = KianClient::new(resolve_url(url, config_path)?);
    let result = client
        .ops(
            "memory_dream",
            json!({
                "session": session.0.to_string(),
                "source": source.0.to_string(),
                "from_seq": from_seq,
                "to_seq": to_seq,
            }),
        )
        .await
        .map_err(|e| format!("memory dream: {e}"))?;
    if json {
        return serde_json::to_string_pretty(&result).map_err(|e| e.to_string());
    }
    let deduplicated = result
        .get("deduplicated")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let note = if deduplicated {
        "deduplicated        the same range was already admitted — a no-op"
    } else {
        "admitted            one dream row (role user, the rendered prompt); the drain wakes"
    };
    Ok(format!(
        "session             {}\nsource              {}\nrange               [{from_seq},{to_seq})\nbytes read          {}\nprompt bytes        {}\n{note}\n",
        result.get("session").and_then(Value::as_str).unwrap_or("?"),
        result.get("source").and_then(Value::as_str).unwrap_or("?"),
        result
            .get("bytes_read")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        result
            .get("prompt_bytes")
            .and_then(Value::as_u64)
            .unwrap_or(0),
    ))
}

/// The projected note set of a memory session (the read side of
/// `list`/`show`/`verify` — one shared rebuild).
async fn memory_notes(
    config_path: &Path,
    db: Option<&Path>,
    session: SessionId,
) -> CmdResult<(EventStore, Vec<kian_core::memory::Note>)> {
    let db = resolve_db(db, config_path)?;
    let store = open_reader(&db).await?;
    let events = rebuild_session(&store, session).await?;
    Ok((store, kian_core::memory::project_notes(&events)))
}

/// `kian memory list --session <uuid>`: the projected notes, subject
/// order (the fold is deterministic).
pub async fn run_memory_list(
    config_path: &Path,
    db: Option<&Path>,
    session: SessionId,
    json: bool,
) -> CmdResult<String> {
    let (store, notes) = memory_notes(config_path, db, session).await?;
    store.close().await;
    if json {
        return serde_json::to_string_pretty(&json!({
            "session": session.0,
            "notes": notes,
        }))
        .map_err(|e| e.to_string());
    }
    let mut out = format!(
        "session             {}\nnotes               {}\n",
        session.0,
        notes.len()
    );
    for note in &notes {
        let ranges = note
            .derived_from
            .iter()
            .map(|r| format!("{}:[{},{}))", r.session.0, r.start_seq, r.end_seq))
            .collect::<Vec<_>>()
            .join(" ");
        out.push_str(&format!(
            "{:<20} {:<9} seq {:<4} {}\n",
            note.subject,
            status_str(note.status),
            note.seq,
            ranges
        ));
    }
    Ok(out)
}

fn status_str(status: kian_core::memory::NoteStatus) -> &'static str {
    match status {
        kian_core::memory::NoteStatus::Candidate => "candidate",
        kian_core::memory::NoteStatus::Verified => "verified",
        kian_core::memory::NoteStatus::Stale => "stale",
    }
}

/// `kian memory show --session <uuid> <event-id | subject>`: one note
/// by its carrying event's derived id, or by subject (the latest
/// revision the fold keeps).
pub async fn run_memory_show(
    config_path: &Path,
    db: Option<&Path>,
    session: SessionId,
    selector: &str,
    json: bool,
) -> CmdResult<String> {
    let db_path = resolve_db(db, config_path)?;
    let store = open_reader(&db_path).await?;
    let events = rebuild_session(&store, session).await?;

    // (a) a derived event id names the exact carrying event.
    let mut found: Option<(String, kian_core::memory::Note)> = None;
    for ev in &events {
        if !matches!(ev.kind, EventKind::MemoryNote) {
            continue;
        }
        let id = kian_core::events::event_id(&ev.session, ev.seq);
        if id == selector {
            if let Some(note) = kian_core::memory::parse_note(ev.kind, &ev.bytes) {
                let mut note = note;
                note.seq = ev.seq;
                found = Some((id, note));
            }
            break;
        }
    }
    // (b) else a subject names the fold's latest revision.
    if found.is_none() {
        let notes = kian_core::memory::project_notes(&events);
        found = notes
            .into_iter()
            .map(|note| (kian_core::events::event_id(&session, note.seq), note))
            .find(|(_, note)| note.subject == selector);
    }
    store.close().await;

    let Some((event_id, note)) = found else {
        return Err(format!(
            "no memory note matches `{selector}` (an event id or a note subject of session {})",
            session.0
        ));
    };
    if json {
        return serde_json::to_string_pretty(&json!({
            "session": session.0,
            "event_id": event_id,
            "note": note,
        }))
        .map_err(|e| e.to_string());
    }
    let ranges = note
        .derived_from
        .iter()
        .map(|r| format!("{}:[{},{}))", r.session.0, r.start_seq, r.end_seq))
        .collect::<Vec<_>>()
        .join(" ");
    let verified_by = note
        .verified_by
        .as_ref()
        .map(|v| format!("{} (ref {})", method_str(v), v.r#ref))
        .unwrap_or_else(|| "-".into());
    Ok(format!(
        "session             {}\nevent               {event_id} (seq {})\nsubject             {}\nstatus              {}\ntext                {}\nderived from        {ranges}\nverified by         {verified_by}\n",
        session.0,
        note.seq,
        note.subject,
        status_str(note.status),
        note.text
    ))
}

fn method_str(v: &kian_core::memory::VerifiedBy) -> &str {
    match v.method {
        kian_core::memory::VerifyMethod::Transcript => "transcript",
        kian_core::memory::VerifyMethod::EvidenceGate => "evidence_gate",
        kian_core::memory::VerifyMethod::Human => "human",
    }
}

/// One `kian memory verify` line.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NoteVerification {
    pub subject: String,
    pub status: &'static str,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// `kian memory verify --session <uuid>`: re-check every projected
/// note's `derived_from` ranges against the source streams (pure
/// [`kian_core::memory::verify_note`] over rebuilt streams — no
/// server needed).
pub async fn run_memory_verify(
    config_path: &Path,
    db: Option<&Path>,
    session: SessionId,
    json: bool,
) -> CmdResult<String> {
    let (store, notes) = memory_notes(config_path, db, session).await?;
    let mut results = Vec::with_capacity(notes.len());
    for note in &notes {
        // verify_note checks one note against ONE source stream; a
        // note with ranges across sessions verifies per source group.
        let mut ok = true;
        let mut error = None;
        let mut by_session: std::collections::BTreeMap<
            SessionId,
            Vec<kian_core::memory::DerivedRange>,
        > = std::collections::BTreeMap::new();
        for range in &note.derived_from {
            by_session.entry(range.session).or_default().push(*range);
        }
        for (source, ranges) in by_session {
            let events = store
                .rebuild(source)
                .await
                .map_err(|e| format!("verify: source {source:?} read: {e}"))?;
            let mut group = note.clone();
            group.derived_from = ranges;
            if let Err(e) = kian_core::memory::verify_note(&group, &events) {
                ok = false;
                error = Some(e.to_string());
                break;
            }
        }
        results.push(NoteVerification {
            subject: note.subject.clone(),
            status: status_str(note.status),
            ok,
            error,
        });
    }
    store.close().await;
    let verified = results.iter().filter(|r| r.ok).count();
    if json {
        return serde_json::to_string_pretty(&json!({
            "session": session.0,
            "verified": verified,
            "failed": results.len() - verified,
            "results": results,
        }))
        .map_err(|e| e.to_string());
    }
    let mut out = format!(
        "session             {}\nverified            {} of {}\n",
        session.0,
        verified,
        results.len()
    );
    for r in &results {
        let verdict = if r.ok {
            "ok".to_string()
        } else {
            format!("FAILED — {}", r.error.as_deref().unwrap_or("?"))
        };
        out.push_str(&format!("{:<20} {:<9} {verdict}\n", r.subject, r.status));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};
    use serde_json::json;

    use super::{check_model_context, import_jsonl, run_export};

    fn temp_db_path() -> PathBuf {
        std::env::temp_dir().join(format!("kian-c14-{}.sqlite", uuid::Uuid::new_v4()))
    }

    fn ev(seq: u64, epoch: u64, kind: EventKind, bytes: serde_json::Value) -> Event {
        Event {
            seq,
            session: SessionId(uuid::Uuid::nil()),
            epoch: Epoch(epoch),
            ts: 1_761_900_000_000 + seq,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        }
    }

    /// A minimal honest stream: created → input → turn → request, where
    /// the request body records exactly what `derive_model_context`
    /// yields for the pre-turn prefix (the A2 discipline).
    fn honest_stream(recorded_messages: serde_json::Value) -> Vec<Event> {
        vec![
            ev(1, 0, EventKind::SessionCreated, json!({})),
            ev(
                2,
                0,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "hello"}),
            ),
            // claim → Running: drain activity is legal from here on
            ev(3, 1, EventKind::DrainClaimed, json!({})),
            ev(4, 1, EventKind::TurnStart, json!({"iteration": 0})),
            ev(
                5,
                1,
                EventKind::StepRequest,
                json!({"body": {"messages": recorded_messages}}),
            ),
        ]
    }

    /// The A2 check passes when the recorded bytes equal the derived
    /// context (canonical form on both sides).
    #[test]
    fn model_context_check_passes_on_matching_bytes() {
        let events = honest_stream(json!([{"role": "user", "content": "hello"}]));
        let check = check_model_context(&events);
        assert_eq!(check.requests_checked, 1);
        assert_eq!(check.byte_identical, 1);
        assert!(check.mismatches.is_empty());
    }

    /// The A2 check FAILS loudly when history was rewritten or the
    /// recorded request diverges from the projection — this is the
    /// replay tool's whole point (constraint 3 detector).
    #[test]
    fn model_context_check_detects_divergence() {
        let events = honest_stream(json!([{"role": "user", "content": "REWRITTEN"}]));
        let check = check_model_context(&events);
        assert_eq!(check.requests_checked, 1);
        assert_eq!(check.byte_identical, 0);
        assert_eq!(check.mismatches.len(), 1);
        assert_eq!(check.mismatches[0].seq, 5);
        assert!(check.mismatches[0].derived.contains("hello"));
        assert!(check.mismatches[0].recorded.contains("REWRITTEN"));
    }

    /// Export → import round-trip at the unit level: an imported JSONL
    /// stream is byte-identical and projects the same state.
    #[tokio::test]
    async fn export_import_round_trip_is_lossless() {
        use kian_core::state::project;
        use kian_store::EventStore;

        let sid = SessionId::new();
        let mut events = honest_stream(json!([{"role": "user", "content": "hello"}]));
        events.push(ev(6, 1, EventKind::TurnEnd, json!({"outcome": "ok"})));
        for e in events.iter_mut() {
            e.session = sid;
        }

        let path = temp_db_path();
        let store = EventStore::open(&path).await.unwrap();
        for e in &events {
            store
                .append(
                    sid,
                    e.clone(),
                    kian_store::AppendGuard {
                        epoch: e.epoch,
                        expected_seq: e.seq,
                    },
                )
                .await
                .unwrap();
        }
        let jsonl: String = events
            .iter()
            .map(|e| serde_json::to_string(e).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        store.close().await;

        let fresh_path = temp_db_path();
        let fresh = EventStore::open(&fresh_path).await.unwrap();
        let imported = import_jsonl(&fresh, &jsonl).await.unwrap();
        assert_eq!(imported.len(), events.len());
        let rebuilt = fresh.rebuild(sid).await.unwrap();
        for (a, b) in events.iter().zip(rebuilt.iter()) {
            assert_eq!(
                serde_json::to_string(a).unwrap(),
                serde_json::to_string(b).unwrap(),
                "envelopes round-trip byte-identically"
            );
        }
        assert_eq!(project(&events).unwrap(), project(&rebuilt).unwrap());
        fresh.close().await;
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&fresh_path);
    }

    /// `import_jsonl` rejects malformed lines honestly.
    #[tokio::test]
    async fn import_rejects_malformed_lines() {
        use kian_store::EventStore;
        let path = temp_db_path();
        let store = EventStore::open(&path).await.unwrap();
        let err = import_jsonl(&store, "not json at all\n").await;
        assert!(err.is_err());
        store.close().await;
        let _ = std::fs::remove_file(&path);
    }

    /// the default export is byte-identical to the legacy format
    /// (one canonical `serde_json::to_string(event)` per line — no
    /// derived-id key anywhere).
    #[tokio::test]
    async fn default_export_bytes_are_unchanged_without_the_flag() {
        let sid = SessionId::new();
        let mut events = honest_stream(json!([{"role": "user", "content": "hello"}]));
        events.push(ev(6, 1, EventKind::TurnEnd, json!({"outcome": "ok"})));
        for e in events.iter_mut() {
            e.session = sid;
        }
        let path = temp_db_path();
        let store = kian_store::EventStore::open(&path).await.unwrap();
        for e in &events {
            store
                .append(
                    sid,
                    e.clone(),
                    kian_store::AppendGuard {
                        epoch: e.epoch,
                        expected_seq: e.seq,
                    },
                )
                .await
                .unwrap();
        }
        store.close().await;

        let jsonl = run_export(
            std::path::Path::new("kian.yaml"),
            Some(&path),
            sid,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();
        let expected: String = events
            .iter()
            .map(|e| serde_json::to_string(e).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        assert_eq!(jsonl, expected, "default export bytes must be unchanged");
        let _ = std::fs::remove_file(&path);
    }

    /// `--with-event-ids` adds the stable derived id to every line,
    /// deterministically, without touching any envelope field — and the
    /// augmented lines re-import into a fresh store whose default export
    /// is byte-identical to the original one (discipline; the field
    /// is round-trip-safe, not export-only).
    #[tokio::test]
    async fn with_event_ids_export_adds_stable_ids_and_round_trips() {
        use kian_store::EventStore;

        let sid = SessionId::new();
        let mut events = honest_stream(json!([{"role": "user", "content": "hello"}]));
        events.push(ev(6, 1, EventKind::TurnEnd, json!({"outcome": "ok"})));
        for e in events.iter_mut() {
            e.session = sid;
        }
        let path = temp_db_path();
        let store = EventStore::open(&path).await.unwrap();
        for e in &events {
            store
                .append(
                    sid,
                    e.clone(),
                    kian_store::AppendGuard {
                        epoch: e.epoch,
                        expected_seq: e.seq,
                    },
                )
                .await
                .unwrap();
        }
        store.close().await;

        let plain = run_export(
            std::path::Path::new("kian.yaml"),
            Some(&path),
            sid,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();
        let with_ids = run_export(
            std::path::Path::new("kian.yaml"),
            Some(&path),
            sid,
            None,
            None,
            None,
            true,
        )
        .await
        .unwrap();
        let with_ids_again = run_export(
            std::path::Path::new("kian.yaml"),
            Some(&path),
            sid,
            None,
            None,
            None,
            true,
        )
        .await
        .unwrap();
        assert_eq!(with_ids, with_ids_again, "derived ids are deterministic");

        // Line-by-line: the derived id is right, and every envelope
        // field survives the augmentation unchanged.
        assert_eq!(
            plain.lines().count(),
            with_ids.lines().count(),
            "the flag adds a key per line, never a line"
        );
        for (plain_line, id_line) in plain.lines().zip(with_ids.lines()) {
            let mut augmented: serde_json::Value =
                serde_json::from_str(plain_line).expect("plain line is an envelope");
            let line: serde_json::Value =
                serde_json::from_str(id_line).expect("augmented line still parses as an envelope");
            let seq = line["seq"].as_u64().unwrap();
            assert_eq!(
                line["event_id"].as_str().unwrap(),
                kian_core::events::event_id(&sid, seq),
                "event_id must equal the derived id for seq {seq}"
            );
            let object = augmented.as_object_mut().unwrap();
            object.insert("event_id".to_string(), line["event_id"].clone());
            assert_eq!(augmented, line, "no envelope field changed");
        }

        // The augmented JSONL re-imports into a fresh store (the
        // envelope ignores the extra key) and the fresh store's default
        // export is byte-identical to the original one.
        let fresh_path = temp_db_path();
        let fresh = EventStore::open(&fresh_path).await.unwrap();
        import_jsonl(&fresh, &with_ids).await.unwrap();
        fresh.close().await;
        let reexported = run_export(
            std::path::Path::new("kian.yaml"),
            Some(&fresh_path),
            sid,
            None,
            None,
            None,
            false,
        )
        .await
        .unwrap();
        assert_eq!(
            reexported, plain,
            "import ignores event_id; re-export is byte-identical"
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&fresh_path);
    }

    // -- memory verbs (round 8, kian-87) ---------------------------

    use super::{run_memory_list, run_memory_show, run_memory_verify};
    use kian_store::EventStore;

    /// Seeds one memory session (created + three note events, one a
    /// later revision of the same subject) and one source session
    /// whose stream covers seq [1,5); returns (db path, memory sid,
    /// source sid).
    async fn seed_memory_store() -> (PathBuf, SessionId, SessionId) {
        let memory_sid = SessionId::new();
        let source_sid = SessionId::new();
        let path = temp_db_path();
        let store = EventStore::open(&path).await.unwrap();

        // Per-session seq: append with the event's own seq as the
        // expectedSeq guard (events land in order).
        async fn append(
            store: &EventStore,
            sid: SessionId,
            seq: u64,
            epoch: Epoch,
            kind: EventKind,
            bytes: serde_json::Value,
        ) {
            let e = Event {
                seq,
                session: sid,
                epoch,
                ts: 1_761_900_000_000 + seq,
                kind,
                bytes,
                version: SCHEMA_VERSION,
            };
            store
                .append(
                    sid,
                    e,
                    kian_store::AppendGuard {
                        epoch,
                        expected_seq: seq,
                    },
                )
                .await
                .unwrap();
        }

        // The source session: seq 1..5 (created + input + answer +
        // turn end).
        append(
            &store,
            source_sid,
            1,
            Epoch::GENESIS,
            EventKind::SessionCreated,
            json!({}),
        )
        .await;
        append(
            &store,
            source_sid,
            2,
            Epoch(1),
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "the deploy broke"}),
        )
        .await;
        append(
            &store,
            source_sid,
            3,
            Epoch(1),
            EventKind::AssistantMessage,
            json!({"role": "assistant", "content": {"text": "rolled back"}}),
        )
        .await;
        append(
            &store,
            source_sid,
            4,
            Epoch(1),
            EventKind::TurnEnd,
            json!({"outcome": "ok"}),
        )
        .await;

        // The memory session: created + notes (seq 1..4).
        append(
            &store,
            memory_sid,
            1,
            Epoch::GENESIS,
            EventKind::SessionCreated,
            json!({}),
        )
        .await;
        append(
            &store,
            memory_sid,
            2,
            Epoch(1),
            EventKind::MemoryNote,
            json!({
                "subject": "deploys via dsh bridge",
                "text": "old text",
                "status": "candidate",
                "derived_from": [{"session": source_sid.0, "start_seq": 1, "end_seq": 5}],
            }),
        )
        .await;
        append(
            &store,
            memory_sid,
            3,
            Epoch(1),
            EventKind::MemoryNote,
            json!({
                "subject": "deploys via dsh bridge",
                "text": "Deploys go through the dsh bridge.",
                "status": "verified",
                "verified_by": {"method": "transcript", "ref": "3"},
                "derived_from": [{"session": source_sid.0, "start_seq": 1, "end_seq": 5}],
            }),
        )
        .await;
        append(
            &store,
            memory_sid,
            4,
            Epoch(1),
            EventKind::MemoryNote,
            json!({
                "subject": "broken range",
                "text": "Points outside the source stream.",
                "status": "candidate",
                "derived_from": [{"session": source_sid.0, "start_seq": 8, "end_seq": 12}],
            }),
        )
        .await;
        store.close().await;
        (path, memory_sid, source_sid)
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    #[tokio::test]
    async fn memory_list_reports_the_fold() {
        let (path, memory_sid, _source) = seed_memory_store().await;
        let out = run_memory_list(
            std::path::Path::new("kian.yaml"),
            Some(&path),
            memory_sid,
            false,
        )
        .await
        .unwrap();
        // Latest revision wins; subject-lexicographic order.
        assert!(out.contains("notes               2"), "{out}");
        assert!(out.contains("broken range"), "{out}");
        assert!(out.contains("deploys via dsh bridge"), "{out}");
        assert!(out.contains("verified"), "{out}");
        assert!(
            !out.contains("old text"),
            "stale revision must not surface: {out}"
        );

        let json_out = run_memory_list(
            std::path::Path::new("kian.yaml"),
            Some(&path),
            memory_sid,
            true,
        )
        .await
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(&json_out).unwrap();
        assert_eq!(value["notes"].as_array().unwrap().len(), 2);
        cleanup(&path);
    }

    #[tokio::test]
    async fn memory_show_resolves_subject_and_event_id() {
        let (path, memory_sid, _source) = seed_memory_store().await;
        let out = run_memory_show(
            std::path::Path::new("kian.yaml"),
            Some(&path),
            memory_sid,
            "deploys via dsh bridge",
            false,
        )
        .await
        .unwrap();
        assert!(out.contains("Deploys go through the dsh bridge."), "{out}");
        assert!(out.contains("verified"), "{out}");

        // The derived event id of the latest revision also resolves.
        let event_id = kian_core::events::event_id(&memory_sid, 4);
        let out = run_memory_show(
            std::path::Path::new("kian.yaml"),
            Some(&path),
            memory_sid,
            &event_id,
            false,
        )
        .await
        .unwrap();
        assert!(out.contains("seq 4"), "{out}");

        // Unknown selector is an honest error.
        let err = run_memory_show(
            std::path::Path::new("kian.yaml"),
            Some(&path),
            memory_sid,
            "no-such-subject",
            false,
        )
        .await
        .unwrap_err();
        assert!(err.contains("no memory note matches"), "{err}");
        cleanup(&path);
    }

    #[tokio::test]
    async fn memory_verify_rechecks_ranges_against_the_source() {
        let (path, memory_sid, _source) = seed_memory_store().await;
        let out = run_memory_verify(
            std::path::Path::new("kian.yaml"),
            Some(&path),
            memory_sid,
            false,
        )
        .await
        .unwrap();
        assert!(out.contains("verified            1 of 2"), "{out}");
        assert!(out.contains("broken range"), "{out}");
        assert!(
            out.contains("FAILED"),
            "the out-of-stream range must fail honestly: {out}"
        );

        let json_out = run_memory_verify(
            std::path::Path::new("kian.yaml"),
            Some(&path),
            memory_sid,
            true,
        )
        .await
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(&json_out).unwrap();
        assert_eq!(value["verified"], 1);
        assert_eq!(value["failed"], 1);
        cleanup(&path);
    }
}
