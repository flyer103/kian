//! The C14 operational session commands: `inspect` / `replay` /
//! `export` / `wake-now` / `lease inspect` / `recovery budget reset`
//! (SPEC §3.2), plus `tickets` — the C56 Ticket stage board.
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
//!     "pending_eligible_inputs": 0 }
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
//!   "live_state": "completed",           // null without --url
//!   "state_matches_live": true,          // null without --url
//!   "invariants_ok": true,
//!   "violations": [],
//!   "model_context": {                   // the A2 byte-identity check
//!     "requests_checked": 2, "byte_identical": 2,
//!     "mismatches": [] }                 // [{ "seq", "derived", "recorded" }]
//! }
//! ```
//!
//! **`session export <id> [--since S] [--until U]`** — JSONL, one
//! frozen v0 event envelope per line (the exact stored bytes), `since
//! <= seq <= until` (inclusive; defaults: whole stream). Import = one
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
//! **`tickets`** (C56) — one row per worker session, uuid-ascending
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
    // C51: postgres-mode configs carry no sqlite path (validation
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
}

/// Event-kind counters, all derived from the stream (constraint 1).
fn counters(events: &[Event], pending: usize) -> Value {
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
    json!(c)
}

/// `kian session inspect <id>`: projection + counters, read-only.
pub async fn run_inspect(
    config_path: &Path,
    db: Option<&Path>,
    session: SessionId,
    json: bool,
) -> CmdResult<String> {
    let db = resolve_db(db, config_path)?;
    let store = open_reader(&db).await?;
    let events = rebuild_session(&store, session).await?;
    let state = project(&events).map_err(|e| format!("projection: {e}"))?;
    let pending = store
        .eligible_inputs(session, now_ms())
        .await
        .map_err(|e| format!("inbox read: {e}"))?
        .len();
    store.close().await;

    let last = events.last().expect("non-empty by construction");
    let report = InspectReport {
        session,
        state,
        event_count: events.len(),
        next_seq: last.seq + 1,
        epoch: last.epoch.0,
        last_event_ts: last.ts,
        counters: counters(&events, pending),
    };
    if json {
        return serde_json::to_string_pretty(&report).map_err(|e| e.to_string());
    }
    let c = &report.counters;
    Ok(format!(
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
    ))
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
                // Dialect v2 (kian-61): tool transcripts map to the strict
                // wire shape; the per-call duplicates drop out.
                let chat: Vec<_> = msgs.iter().filter_map(to_chat_message).collect();
                // Canonical form only: the recorded body round-tripped
                // through `serde_json::Value` (lexicographic keys — the
                // frozen C8 serialization), so both sides are compared
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

/// Runs the C3 invariant predicates that are stream-shaped (the ones
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

/// `kian session export <id> [--since S] [--until U] [--out F]`:
/// writes one frozen v0 envelope per line (the exact stored bytes,
/// re-serialized identically from the envelope column). `since`/`until`
/// are inclusive seq bounds.
pub async fn run_export(
    config_path: &Path,
    db: Option<&Path>,
    session: SessionId,
    since: Option<u64>,
    until: Option<u64>,
    out: Option<&Path>,
) -> CmdResult<String> {
    let db = resolve_db(db, config_path)?;
    let store = open_reader(&db).await?;
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
        // (C5 persist-what-you-send), so round-trips are lossless.
        jsonl.push_str(&serde_json::to_string(e).map_err(|e| format!("serialize event: {e}"))?);
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
// tickets — the Ticket stage board (C56)
// ---------------------------------------------------------------------------

/// One row of the `kian tickets` board: a worker session (one Ticket
/// v1) with its derived stage and the facts behind it.
#[derive(Debug, Clone, serde::Serialize)]
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
    /// C62: the parked outbox deliveries named for this ticket
    /// (`<conversation> (parked after N attempts)` lines; empty when
    /// the outbox holds nothing for it).
    pub parked: Vec<String>,
}

/// `kian tickets [--db <sqlite>] [--json]` — the Ticket stage board
/// (C56): every worker session discovered by scanning `thread/started`
/// across streams (the [`kian_superagent::worker_sessions`] derivation),
/// each with its stage, topic, thread status, and age. Read-only
/// (`open_read_only`); the stage is a pure projection of the stream +
/// inbox, recomputed on every call — nothing is cached, nothing is
/// written. Deterministic order: session uuid ascending.
pub async fn run_tickets(config_path: &Path, db: Option<&Path>, json: bool) -> CmdResult<String> {
    let db = resolve_db(db, config_path)?;
    let store = open_reader(&db).await?;
    let sessions = crate::cache_metrics::sessions_with_events(&store)
        .await
        .map_err(|e| format!("session listing: {e}"))?;
    let mut streams = std::collections::BTreeMap::new();
    let mut all_events = Vec::new();
    for session in &sessions {
        let events = store
            .rebuild(*session)
            .await
            .map_err(|e| format!("read events for {session:?}: {e}"))?;
        all_events.extend(events.iter().cloned());
        streams.insert(*session, events);
    }
    // Worker discovery: `thread/started` across every stream (a
    // Router's dispatch side names the workers; worker-side lineage
    // names them too). BTreeSet ⇒ deterministic uuid order.
    let workers = kian_superagent::worker_sessions(&all_events);
    // Topic binding: the same disposable registry projection the
    // runtime rebuilds (C38), inverted to worker → topics.
    let registry = kian_superagent::PassthroughRegistry::rebuild(&all_events);
    let mut worker_topics: std::collections::BTreeMap<SessionId, Vec<String>> =
        std::collections::BTreeMap::new();
    for topic in registry.topics() {
        for worker in registry.resolve(&topic) {
            worker_topics.entry(worker).or_default().push(topic.clone());
        }
    }

    let now = now_ms();
    // C62: the real outbox view — the delivery-retry stage arm's input
    // (kian-64's stub exemption: this construction replaces `NoOutbox`).
    // Ticket aliases per thread: the thread's own id string plus the
    // topics its worker owns (a delivery's `correlation` names either).
    let outbox_status = kian_superagent::OutboxStatus::from_events(&all_events);
    let mut thread_aliases: std::collections::BTreeMap<uuid::Uuid, Vec<String>> =
        std::collections::BTreeMap::new();
    let mut worker_aliases: std::collections::BTreeMap<SessionId, Vec<String>> =
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
        if let Some(thread) = thread {
            let mut aliases = vec![thread.to_string()];
            aliases.extend(topics.iter().cloned());
            worker_aliases.insert(*worker, aliases.clone());
            thread_aliases.entry(thread).or_default().extend(aliases);
        }
    }
    let outbox_view = kian_superagent::ThreadOutboxView::new(outbox_status.clone(), thread_aliases);
    let parked_lines = outbox_status.parked_lines();
    let mut rows = Vec::new();
    for worker in workers {
        let events = streams.get(&worker).cloned().unwrap_or_default();
        let pending = store
            .eligible_inputs(worker, now)
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
        // The parked message named on the board (C62): a worker whose
        // ticket has a pending/parked delivery shows every parked line.
        let parked = worker_aliases
            .get(&worker)
            .map(|aliases| {
                if aliases.iter().any(|a| outbox_status.pending_for(a)) {
                    parked_lines.clone()
                } else {
                    Vec::new()
                }
            })
            .unwrap_or_default();
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
            age_ms: now.saturating_sub(first_ts),
            events: events.len(),
            parked,
        });
    }
    store.close().await;

    if json {
        let report = json!({"now_ms": now, "tickets": rows});
        return serde_json::to_string_pretty(&report).map_err(|e| e.to_string());
    }
    if rows.is_empty() {
        return Ok("no worker sessions (no thread/started lineage in this store)\n".into());
    }
    let mut out = format!(
        "{:<38} {:<16} {:<12} {:<16} {:>7} {:>8} {:>12}\n",
        "SESSION", "STAGE", "TOPIC", "THREAD", "PENDING", "EVENTS", "AGE"
    );
    for row in &rows {
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
    // C62: the parked message named on the board — appended lines only
    // when something is parked (empty board output stays byte-identical
    // to the pre-C62 shape).
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
    Ok(out)
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
/// [--topic T] [--url <base>] [--json]`: the C27 demo/S4 injection —
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};
    use serde_json::json;

    use super::{check_model_context, import_jsonl};

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
}
