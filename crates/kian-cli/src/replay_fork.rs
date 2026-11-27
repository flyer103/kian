//! Scripted fork replay (the session replay command with a fake provider):
//! re-drive one drain episode on a fork of the stream.
//!
//! The module is the offline half of the self-evolution loop: the recorded
//! stream is the fixture, the current code and config are the candidate, and
//! the report says whether the candidate reproduces the recording. It never
//! writes the source store — the episode is re-driven in a fork (a fresh
//! sqlite file seeded with the events before the episode boundary), so the
//! recorded stream stays the sole truth (constraint 1) and the replay cannot
//! mint a claim, a lease or an end write on a live session.
//!
//! Phase-1 contract:
//! - the anchor is an episode boundary (a drain/claimed row; default: the
//!   last one), and the prefix before it must project a non-terminal state,
//!   so the fork starts idle and the engine takes the ordinary claim path;
//! - the episode's input rows are re-admitted from its input/admitted events
//!   (the inverse of the orchestrator's consumption plan, round-trip
//!   checked); an episode that admits input after its first turn/start (a
//!   steer) is refused, never reordered;
//! - model answers come from the script (one NDJSON turn per provider call);
//!   tools are never executed — recorded tool/result rows are replayed by
//!   call id, then by tool name in stream order, and a call with no recorded
//!   result fails honestly and marks the report incomplete;
//! - the comparison walks the recorded and the replayed tail row by row:
//!   kind and payload bytes must match, with drain/claimed (lease
//!   bookkeeping) and the envelope's clock/fencing fields (ts, epoch)
//!   excepted;
//! - the verdict is reproduced, diverged or incomplete. All three exit 0 and
//!   print the report: a divergence is a result, not a CLI failure.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::future::{BoxFuture, FutureExt, ready};
use futures::stream::{self, BoxStream, StreamExt};
use kian_core::events::{Event, EventKind};
use kian_core::model::{AgentDef, InputRow, SessionId, SlotName};
use kian_core::seams::{
    ModelChunk, ModelProvider, ModelRequest, ProviderError, ToolCall, ToolError, ToolProvider,
    ToolResult, ToolSchema,
};
use kian_core::state::{self, SessionState};
use kian_llm::slots::spec_from_binding_payload;
use kian_llm::turn::ProviderRegistry;
use kian_loop::react::{ReActConfig, ReActLoop};
use kian_orchestrator::engine::{Engine, EngineConfig};
use kian_store::EventStore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::commands::{self, CmdResult};
use crate::config::ServeConfig;

/// How long the fork's drain may take to write its end row.
pub const DEFAULT_TIMEOUT_MS: u64 = 120_000;

const POLL_MS: u64 = 10;
const MAX_EXCERPT: usize = 280;
const ADMITTED_IGNORED: [&str; 1] = ["drain/claimed"];

/// Inputs of the scripted replay.
pub struct Options<'a> {
    pub config_path: &'a Path,
    pub db: Option<&'a Path>,
    pub session: SessionId,
    pub script: &'a Path,
    pub from_seq: Option<u64>,
    pub keep_fork: bool,
}

// ---------------------------------------------------------------------------
// report
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct Report {
    pub session: SessionId,
    pub provider: String,
    pub script: ScriptRef,
    pub anchor: AnchorRef,
    pub inputs: Vec<InputRef>,
    pub replay: ReplayRef,
    pub comparison: Comparison,
    pub fork: ForkRef,
    pub notes: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ScriptRef {
    pub path: String,
    pub turns: usize,
}

#[derive(Debug, Serialize)]
pub struct AnchorRef {
    pub seq: u64,
    pub kind: String,
    pub prefix_events: u64,
    pub prefix_state: String,
}

#[derive(Debug, Serialize)]
pub struct InputRef {
    pub seq: u64,
    pub keys: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ReplayRef {
    pub events: u64,
    pub end_reason: Option<String>,
    pub settled: bool,
    pub timeout_ms: u64,
}

#[derive(Debug, Serialize)]
pub struct Comparison {
    pub recorded_rows: u64,
    pub replayed_rows: u64,
    pub compared: u64,
    pub ignored: u64,
    pub ignored_kinds: Vec<String>,
    pub verdict: String,
    pub divergence: Option<Divergence>,
}

#[derive(Debug, Serialize)]
pub struct Divergence {
    pub reason: String,
    pub recorded_seq: Option<u64>,
    pub recorded_kind: Option<String>,
    pub replayed_seq: Option<u64>,
    pub replayed_kind: Option<String>,
    pub path: Option<String>,
    pub recorded_excerpt: Option<String>,
    pub replayed_excerpt: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ForkRef {
    pub path: String,
    pub kept: bool,
}

// ---------------------------------------------------------------------------
// entry point
// ---------------------------------------------------------------------------

/// Re-drives the anchored episode against the script and reports the verdict.
pub async fn run(opts: Options<'_>, json_out: bool) -> CmdResult<String> {
    let script = Script::load(opts.script)?;
    let cfg = ServeConfig::load(opts.config_path)
        .map_err(|e| format!("cannot load config {}: {e}", opts.config_path.display()))?;
    let db = commands::resolve_db(opts.db, opts.config_path)?;

    // The source store is opened read-only and never written: the whole point
    // of the fork is that a replay cannot touch the recorded truth.
    let source = commands::open_reader(&db).await?;
    refuse_live_lease(&source, opts.session, &db).await?;
    let events = source
        .rebuild(opts.session)
        .await
        .map_err(|e| format!("event read: {e}"))?;
    source.close().await;
    if events.is_empty() {
        return Err(format!(
            "unknown session {} (no events in this store)",
            opts.session.0
        ));
    }

    let anchor_idx = pick_anchor(&events, opts.from_seq)?;
    let anchor = &events[anchor_idx];
    let prefix = &events[..anchor_idx];
    let recorded_tail = &events[anchor_idx..];
    let prefix_state = state::project(prefix)
        .map_err(|e| format!("the prefix before seq {} does not project: {e}", anchor.seq))?;
    if prefix_state.is_terminal() {
        return Err(format!(
            "the prefix before seq {} projects terminal ({}); an episode boundary cannot sit behind a terminal end write",
            anchor.seq,
            state_label(&prefix_state)
        ));
    }

    let pending = episode_inputs(recorded_tail, anchor.seq)?;
    let inputs: Vec<InputRef> = pending
        .iter()
        .map(|p| InputRef {
            seq: p.seq,
            keys: p.admitted_keys.clone(),
        })
        .collect();
    let rows: Vec<InputRow> = pending.into_iter().map(|p| p.row).collect();

    let mut notes = Vec::new();
    let (fork, store) = make_fork(prefix, opts.session, &rows).await?;
    let outcome = drive(
        &cfg,
        &store,
        opts.session,
        &script,
        &events,
        prefix.len(),
        &mut notes,
    )
    .await;
    let (replayed, settled) = match outcome {
        Ok(pair) => pair,
        Err(e) => {
            store.close().await;
            remove_fork(&fork, false)?;
            return Err(e);
        }
    };
    let comparison = compare(recorded_tail, &replayed, &notes);
    store.close().await;

    let replayed_end = replayed.iter().rev().find_map(|e| match &e.kind {
        EventKind::SessionEnded { reason } => Some(crate::self_inspect::end_reason_str(reason)),
        _ => None,
    });
    let report = Report {
        session: opts.session,
        provider: "fake".into(),
        script: ScriptRef {
            path: opts.script.display().to_string(),
            turns: script.turns.len(),
        },
        anchor: AnchorRef {
            seq: anchor.seq,
            kind: kind_label(&anchor.kind),
            prefix_events: prefix.len() as u64,
            prefix_state: state_label(&prefix_state),
        },
        inputs,
        replay: ReplayRef {
            events: replayed.len() as u64,
            end_reason: replayed_end.map(str::to_string),
            settled,
            timeout_ms: DEFAULT_TIMEOUT_MS,
        },
        comparison,
        fork: ForkRef {
            path: fork.display().to_string(),
            kept: opts.keep_fork,
        },
        notes,
    };
    if !opts.keep_fork {
        remove_fork(&fork, false)?;
    }
    if json_out {
        serde_json::to_string_pretty(&report).map_err(|e| e.to_string())
    } else {
        Ok(render(&report))
    }
}

async fn refuse_live_lease(store: &EventStore, session: SessionId, db: &Path) -> CmdResult<()> {
    let leases = store
        .leases()
        .await
        .map_err(|e| format!("lease read: {e}"))?;
    let now = now_ms();
    if let Some(lease) = leases.iter().find(|l| l.session == session) {
        let expires_at = lease.last_heartbeat.saturating_add(lease.ttl_ms);
        if expires_at > now {
            return Err(format!(
                "a live drain holds this session (holder {}, epoch {}, lease expires in {} ms): replay refuses to fork a session that is still being driven — take a snapshot first: sqlite3 {} \".backup <copy>\" and pass --db <copy>",
                lease.holder,
                lease.epoch.0,
                expires_at - now,
                db.display()
            ));
        }
    }
    Ok(())
}

fn remove_fork(dir: &Path, kept: bool) -> CmdResult<()> {
    if kept {
        return Ok(());
    }
    match std::fs::remove_dir_all(dir) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("cannot remove the fork {}: {e}", dir.display())),
    }
}

// ---------------------------------------------------------------------------
// anchor and inputs
// ---------------------------------------------------------------------------

/// Index of the episode boundary to replay. A boundary is a drain/claimed
/// row: the claim opens the episode, so the prefix before it is the state the
/// drain saw when it claimed (idle, normally).
fn pick_anchor(events: &[Event], from_seq: Option<u64>) -> CmdResult<usize> {
    let boundaries: Vec<usize> = events
        .iter()
        .enumerate()
        .filter(|(_, e)| matches!(e.kind, EventKind::DrainClaimed))
        .map(|(i, _)| i)
        .collect();
    let Some(&last) = boundaries.last() else {
        return Err("no drain episode to replay (the stream has no drain/claimed row)".to_string());
    };
    let Some(seq) = from_seq else {
        return Ok(last);
    };
    if let Some(i) = boundaries.iter().copied().find(|&i| events[i].seq == seq) {
        return Ok(i);
    }
    let nearest = boundaries
        .iter()
        .rev()
        .map(|&i| events[i].seq)
        .find(|&s| s <= seq);
    Err(match nearest {
        Some(s) => format!(
            "--from-seq {seq} is not an episode boundary: a replay starts at a drain/claimed row (the nearest at or before it is seq {s})"
        ),
        None => format!(
            "--from-seq {seq} is not an episode boundary: the first episode starts at seq {}",
            events[boundaries[0]].seq
        ),
    })
}

/// One re-admitted input: the row plus the recorded evidence that names it
/// (its input/admitted seq and the payload keys the recording carried).
#[derive(Debug)]
struct EpisodeInput {
    seq: u64,
    admitted_keys: Vec<String>,
    row: InputRow,
}

/// Re-admits the episode's input rows, in recorded order.
fn episode_inputs(tail: &[Event], anchor_seq: u64) -> CmdResult<Vec<EpisodeInput>> {
    let first_turn = tail
        .iter()
        .find(|e| matches!(e.kind, EventKind::TurnStart))
        .map(|e| e.seq);
    let spilled = tail
        .iter()
        .find(|e| matches!(e.kind, EventKind::InputSpilled))
        .map(|e| e.seq);
    if let Some(seq) = spilled {
        return Err(format!(
            "the episode consumed a spilled input pair (input/spilled at seq {seq}); spill pairs are not replayable in phase 1"
        ));
    }
    let mut rows = Vec::new();
    for ev in tail
        .iter()
        .filter(|e| matches!(e.kind, EventKind::InputAdmitted))
    {
        if let Some(turn) = first_turn.filter(|turn| ev.seq > *turn) {
            return Err(format!(
                "the episode at seq {anchor_seq} admits input after its first turn/start (input/admitted at seq {}, turn/start at seq {turn}); a mid-episode steer is not replayable in phase 1 — replay an earlier episode",
                ev.seq
            ));
        }
        let payload = invert_admitted(&ev.bytes)
            .map_err(|e| format!("input/admitted seq {}: {e}", ev.seq))?;
        let row = InputRow {
            id: Uuid::new_v4(),
            session: ev.session,
            payload,
            admitted_at: ev.ts,
            eligible_at: ev.ts,
            idempotency_key: Some(format!("replay:{}", ev.seq)),
        };
        rows.push(EpisodeInput {
            seq: ev.seq,
            admitted_keys: row_keys(&ev.bytes),
            row,
        });
    }
    if rows.is_empty() {
        return Err(format!(
            "the episode at seq {anchor_seq} has no admitted input to replay (nothing for the model to answer)"
        ));
    }
    Ok(rows)
}

fn row_keys(payload: &Value) -> Vec<String> {
    match payload.as_object() {
        Some(obj) => {
            let mut keys: Vec<String> = obj.keys().cloned().collect();
            keys.sort();
            keys
        }
        None => Vec::new(),
    }
}

/// The inverse of the orchestrator's consumption plan: an input/admitted
/// payload back to the inbox row payload it was derived from. Every candidate
/// is round-trip checked, so a shape the plan would consume differently is
/// refused instead of silently reordered.
fn invert_admitted(admitted: &Value) -> Result<Value, String> {
    let Some(obj) = admitted.as_object() else {
        return Err("the payload is not an object".into());
    };
    let Some(role) = obj.get("role").and_then(Value::as_str) else {
        return Err("the payload lacks a role".into());
    };
    let Some(content) = obj.get("content") else {
        return Err("the payload lacks content".into());
    };
    let synthetic = obj.get("synthetic").and_then(Value::as_bool) == Some(true);
    let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
    keys.sort_unstable();
    let candidate = match (role, keys.as_slice()) {
        ("user", ["content", "role"]) => content.clone(),
        ("user", ["content", "role", "synthetic"]) if synthetic => {
            json!({"content": content, "synthetic": true})
        }
        ("user", ["content", "dream", "role"]) => {
            json!({"content": content, "dream": obj["dream"].clone()})
        }
        ("user", ["content", "cron", "role"]) => {
            json!({"content": content, "cron": obj["cron"].clone()})
        }
        ("user", ["content", "job", "role"]) => {
            json!({"content": content, "job": obj["job"].clone()})
        }
        ("user", ["content", "cron", "job", "role"]) => json!({
            "content": content,
            "cron": obj["cron"].clone(),
            "job": obj["job"].clone(),
        }),
        ("user", ["content", "cron", "defer", "role"]) => json!({
            "content": content,
            "cron": obj["cron"].clone(),
            "defer": obj["defer"].clone(),
        }),
        ("system", ["content", "memory", "role", "synthetic"]) if synthetic => {
            json!({"content": content, "memory": obj["memory"].clone()})
        }
        _ => {
            return Err(format!(
                "cannot invert an input/admitted payload with keys [{}] (role {role})",
                keys.join(", ")
            ));
        }
    };
    if candidate
        .as_object()
        .is_some_and(|o| o.len() == 2 && o.contains_key("content") && o.contains_key("spill"))
    {
        return Err("the payload inverts to a spill pair, which phase 1 refuses".into());
    }
    if wrap_row_payload(&candidate) != *admitted {
        return Err(format!(
            "cannot invert an input/admitted payload with keys [{}]: the round trip changes it",
            keys.join(", ")
        ));
    }
    Ok(candidate)
}

/// Mirrors the orchestrator's consumption plan (the un-spilled arms) so the
/// inverse above can be checked against the same rules.
fn wrap_row_payload(payload: &Value) -> Value {
    if let Some(obj) = payload.as_object() {
        if obj.len() == 2
            && obj.get("synthetic").and_then(Value::as_bool) == Some(true)
            && let Some(content) = obj.get("content")
        {
            return json!({"role": "user", "content": content, "synthetic": true});
        }
        if obj.len() == 2
            && let (Some(content), Some(dream)) = (obj.get("content"), obj.get("dream"))
        {
            return json!({"role": "user", "content": content, "dream": dream});
        }
        if obj.len() == 2
            && let (Some(content), Some(memory)) = (obj.get("content"), obj.get("memory"))
        {
            return json!({
                "role": "system",
                "content": content,
                "synthetic": true,
                "memory": memory,
            });
        }
        if obj.len() == 2
            && let (Some(content), Some(cron)) = (obj.get("content"), obj.get("cron"))
        {
            return json!({"role": "user", "content": content, "cron": cron});
        }
        if obj.len() == 2
            && let (Some(content), Some(job)) = (obj.get("content"), obj.get("job"))
        {
            return json!({"role": "user", "content": content, "job": job});
        }
        if obj.len() == 3
            && let (Some(content), Some(cron), Some(job)) =
                (obj.get("content"), obj.get("cron"), obj.get("job"))
        {
            return json!({"role": "user", "content": content, "cron": cron, "job": job});
        }
        if obj.len() == 3
            && let (Some(content), Some(cron), Some(defer)) =
                (obj.get("content"), obj.get("cron"), obj.get("defer"))
        {
            return json!({"role": "user", "content": content, "cron": cron, "defer": defer});
        }
    }
    json!({"role": "user", "content": payload})
}

// ---------------------------------------------------------------------------
// fork
// ---------------------------------------------------------------------------

async fn make_fork(
    prefix: &[Event],
    session: SessionId,
    rows: &[InputRow],
) -> CmdResult<(PathBuf, EventStore)> {
    let dir = std::env::temp_dir().join(format!(
        "kian-replay-{}-{}-{}",
        session.0,
        std::process::id(),
        now_ms()
    ));
    std::fs::create_dir_all(&dir)
        .map_err(|e| format!("cannot create the fork dir {}: {e}", dir.display()))?;
    let store = EventStore::open(dir.join("kian.db"))
        .await
        .map_err(|e| format!("cannot open the fork store in {}: {e}", dir.display()))?;
    if !prefix.is_empty() {
        let jsonl = crate::archive::export_bytes(prefix)
            .map_err(|e| format!("cannot serialize the prefix: {e}"))?;
        commands::import_jsonl(&store, &jsonl).await?;
    }
    for row in rows {
        store
            .admit(row.clone())
            .await
            .map_err(|e| format!("cannot admit the replay input: {e}"))?;
    }
    Ok((dir, store))
}

// ---------------------------------------------------------------------------
// driving the fork
// ---------------------------------------------------------------------------

async fn drive(
    cfg: &ServeConfig,
    store: &EventStore,
    session: SessionId,
    script: &Script,
    events: &[Event],
    prefix_len: usize,
    notes: &mut Vec<String>,
) -> CmdResult<(Vec<Event>, bool)> {
    let agent = cfg.agent_def();
    let scripted = Arc::new(ScriptedProvider::new(script.turns.clone()));
    let mut registry = ProviderRegistry::new();
    for name in provider_names(&agent, events) {
        registry.register(name, scripted.clone());
    }
    let tools = Arc::new(RecordedTools::new(events)?);
    let policy = Arc::new(
        cfg.policy()
            .map_err(|e| format!("config recovery policy: {e}"))?
            .table(),
    );
    let react = ReActLoop::with_evidence_gate(
        agent,
        Arc::new(registry),
        tools.clone(),
        policy,
        ReActConfig {
            slot: SlotName(cfg.drain.slot.clone()),
            max_iterations: cfg.drain.max_iterations,
            budget_scope: kian_loop::BudgetScope::Lifetime,
            permissions: cfg.agent.permissions.clone(),
            vision: kian_loop::VisionBudget::default(),
        },
        cfg.agent.evidence_gate.clone(),
    );
    let engine = Engine::new(
        store.clone(),
        Arc::new(react),
        EngineConfig {
            holder: "kian-replay".into(),
            ..EngineConfig::default()
        },
    );
    engine.wake(session);
    let (stream, settled) = wait_settled(store, session).await?;
    // The fork holds the imported prefix too: only the tail is this run's
    // output (the replayed episode), never the recording it was seeded with.
    let replayed: Vec<Event> = stream.into_iter().skip(prefix_len).collect();
    if !settled {
        notes.push(format!(
            "the replay did not write a session/ended within {} ms (last event {} at seq {})",
            DEFAULT_TIMEOUT_MS,
            replayed
                .last()
                .map(|e| kind_label(&e.kind))
                .unwrap_or_else(|| "none".into()),
            replayed.last().map(|e| e.seq).unwrap_or(0)
        ));
    }
    for note in tools.notes() {
        notes.push(note);
    }
    let exhausted = scripted.exhausted();
    if exhausted > 0 {
        notes.push(format!(
            "the script ran out: {exhausted} provider call(s) had no scripted turn and answered with an empty response"
        ));
    }
    Ok((replayed, settled))
}

async fn wait_settled(store: &EventStore, session: SessionId) -> CmdResult<(Vec<Event>, bool)> {
    let deadline =
        tokio::time::Instant::now() + std::time::Duration::from_millis(DEFAULT_TIMEOUT_MS);
    loop {
        let events = store
            .rebuild(session)
            .await
            .map_err(|e| format!("fork event read: {e}"))?;
        if matches!(
            events.last().map(|e| &e.kind),
            Some(EventKind::SessionEnded { .. })
        ) {
            return Ok((events, true));
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok((events, false));
        }
        tokio::time::sleep(std::time::Duration::from_millis(POLL_MS)).await;
    }
}

fn provider_names(agent: &AgentDef, events: &[Event]) -> Vec<String> {
    let mut names: BTreeSet<String> = BTreeSet::new();
    for spec in agent.model_slots.values() {
        names.insert(spec.provider.clone());
        for fallback in &spec.fallback {
            names.insert(fallback.provider.clone());
        }
    }
    for ev in events {
        let spec = match ev.kind {
            EventKind::SlotBinding => spec_from_binding_payload(&ev.bytes),
            EventKind::SlotFallback => ev
                .bytes
                .get("to")
                .and_then(|to| serde_json::from_value(to.clone()).ok()),
            _ => None,
        };
        if let Some(spec) = spec {
            names.insert(spec.provider.clone());
            for fallback in &spec.fallback {
                names.insert(fallback.provider.clone());
            }
        }
    }
    names.into_iter().collect()
}

// ---------------------------------------------------------------------------
// the scripted model
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScriptTurn {
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    usage: Option<Value>,
    #[serde(default)]
    tool_calls: Option<Vec<ScriptToolCall>>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    empty: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScriptToolCall {
    id: String,
    name: String,
    arguments: Value,
}

#[derive(Debug, Clone)]
struct Script {
    turns: Vec<ScriptTurn>,
}

impl Script {
    fn load(path: &Path) -> CmdResult<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read the script {}: {e}", path.display()))?;
        let mut turns = Vec::new();
        for (i, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let turn: ScriptTurn =
                serde_json::from_str(line).map_err(|e| format!("script line {}: {e}", i + 1))?;
            validate_turn(&turn).map_err(|e| format!("script line {}: {e}", i + 1))?;
            turns.push(turn);
        }
        if turns.is_empty() {
            return Err(format!(
                "the script {} has no turns (one JSON object per line)",
                path.display()
            ));
        }
        Ok(Self { turns })
    }
}

fn validate_turn(turn: &ScriptTurn) -> Result<(), String> {
    if turn.error.is_some() {
        if turn.text.is_some() || turn.tool_calls.is_some() || turn.empty {
            return Err("an error turn cannot also carry text, tool_calls or empty".into());
        }
        return Ok(());
    }
    if turn.empty {
        if turn.text.is_some() || turn.tool_calls.is_some() {
            return Err("an empty turn cannot also carry text or tool_calls".into());
        }
        return Ok(());
    }
    if turn.text.is_none() && turn.tool_calls.is_none() {
        return Err("a turn needs one of text, tool_calls, error or empty".into());
    }
    if let Some(calls) = &turn.tool_calls {
        if calls.is_empty() {
            return Err("tool_calls must not be empty (use empty: true for no answer)".into());
        }
        for call in calls {
            if call.id.is_empty() {
                return Err("every tool call needs an id".into());
            }
        }
    }
    Ok(())
}

fn provider_error(raw: &str) -> ProviderError {
    match raw {
        "rate_limited" => ProviderError::RateLimited,
        "context_overflow" => ProviderError::ContextOverflow,
        "empty_response" => ProviderError::EmptyResponse,
        "provider_down" => ProviderError::Unavailable("kian-replay: provider_down".into()),
        "transport" => ProviderError::Transport("kian-replay: transport".into()),
        "invalid_response" => {
            ProviderError::InvalidResponse("kian-replay: invalid_response".into())
        }
        other => ProviderError::Other(other.to_string()),
    }
}

struct ScriptedProvider {
    turns: Mutex<VecDeque<ScriptTurn>>,
    exhausted: AtomicUsize,
}

impl ScriptedProvider {
    fn new(turns: Vec<ScriptTurn>) -> Self {
        Self {
            turns: Mutex::new(turns.into()),
            exhausted: AtomicUsize::new(0),
        }
    }

    fn exhausted(&self) -> usize {
        self.exhausted.load(Ordering::Relaxed)
    }
}

impl ModelProvider for ScriptedProvider {
    fn complete(
        &self,
        _req: ModelRequest,
    ) -> BoxStream<'static, Result<ModelChunk, ProviderError>> {
        let turn = self.turns.lock().unwrap().pop_front();
        let Some(turn) = turn else {
            self.exhausted.fetch_add(1, Ordering::Relaxed);
            return stream::empty().boxed();
        };
        if let Some(error) = &turn.error {
            return stream::iter(vec![Err(provider_error(error))]).boxed();
        }
        if turn.empty {
            return stream::empty().boxed();
        }
        let mut chunks: Vec<Result<ModelChunk, ProviderError>> = Vec::new();
        if let Some(text) = &turn.text {
            chunks.push(Ok(ModelChunk::new(json!({ "content": text }))));
        }
        if let Some(calls) = &turn.tool_calls {
            let mapped: Vec<Value> = calls
                .iter()
                .enumerate()
                .map(|(index, call)| {
                    json!({
                        "index": index,
                        "id": call.id,
                        "function": {
                            "name": call.name,
                            "arguments": call.arguments.to_string(),
                        },
                    })
                })
                .collect();
            chunks.push(Ok(ModelChunk::new(json!({ "tool_calls": mapped }))));
        }
        if let Some(usage) = &turn.usage {
            chunks.push(Ok(ModelChunk::with_usage(Value::Null, usage.clone())));
        }
        stream::iter(chunks).boxed()
    }
}

// ---------------------------------------------------------------------------
// recorded tools
// ---------------------------------------------------------------------------

struct RecordedTools {
    schemas: Vec<ToolSchema>,
    by_id: HashMap<String, ToolResult>,
    named: Mutex<HashMap<String, VecDeque<ToolResult>>>,
    notes: Mutex<Vec<String>>,
}

impl RecordedTools {
    fn new(events: &[Event]) -> CmdResult<Self> {
        let schemas = recorded_schemas(events)?;
        let mut names: HashMap<String, String> = HashMap::new();
        let mut by_id = HashMap::new();
        let mut named: HashMap<String, VecDeque<ToolResult>> = HashMap::new();
        for ev in events {
            match &ev.kind {
                EventKind::ToolCall => {
                    if let (Some(id), Some(name)) = (
                        ev.bytes
                            .get("content")
                            .and_then(|c| c.get("id"))
                            .and_then(Value::as_str),
                        ev.bytes
                            .get("content")
                            .and_then(|c| c.get("name"))
                            .and_then(Value::as_str),
                    ) {
                        names.insert(id.to_string(), name.to_string());
                    }
                }
                EventKind::ToolResult => {
                    let Some(content) = ev.bytes.get("content") else {
                        continue;
                    };
                    let Some(call_id) = content.get("call_id").and_then(Value::as_str) else {
                        continue;
                    };
                    let result = ToolResult {
                        call_id: call_id.to_string(),
                        output: content.get("output").cloned().unwrap_or(Value::Null),
                        is_error: content
                            .get("is_error")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                        redactions: ev
                            .bytes
                            .get("redactions")
                            .and_then(|r| serde_json::from_value(r.clone()).ok())
                            .unwrap_or_default(),
                    };
                    by_id.insert(call_id.to_string(), result.clone());
                    if let Some(name) = names.get(call_id) {
                        named.entry(name.clone()).or_default().push_back(result);
                    }
                }
                _ => {}
            }
        }
        Ok(Self {
            schemas,
            by_id,
            named: Mutex::new(named),
            notes: Mutex::new(Vec::new()),
        })
    }

    fn notes(&self) -> Vec<String> {
        self.notes.lock().unwrap().clone()
    }

    fn take_named(&self, name: &str) -> Option<ToolResult> {
        self.named
            .lock()
            .unwrap()
            .get_mut(name)
            .and_then(|queue| queue.pop_front())
    }
}

impl ToolProvider for RecordedTools {
    fn schemas(&self) -> Vec<ToolSchema> {
        self.schemas.clone()
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        let recorded = self
            .by_id
            .get(&call.id)
            .cloned()
            .or_else(|| self.take_named(&call.name));
        let Some(recorded) = recorded else {
            self.notes.lock().unwrap().push(format!(
                "no recorded tool/result for call {} ({}); the replay answered it with an honest error",
                call.id, call.name
            ));
            return ready(Err(ToolError::Execution(format!(
                "kian-replay: no recorded result for tool {} (call {})",
                call.name, call.id
            ))))
            .boxed();
        };
        ready(Ok(ToolResult {
            call_id: call.id,
            output: recorded.output,
            is_error: recorded.is_error,
            redactions: recorded.redactions,
        }))
        .boxed()
    }
}

/// The tool schemas the recorded episode sent, read back from its first
/// step/request body (the body's tools array is the serialization of the same
/// ToolSchema values, so re-serializing them is byte-identical).
fn recorded_schemas(events: &[Event]) -> CmdResult<Vec<ToolSchema>> {
    let Some(ev) = events
        .iter()
        .find(|e| matches!(e.kind, EventKind::StepRequest))
    else {
        return Ok(Vec::new());
    };
    let Some(body) = ev.bytes.get("body") else {
        return Ok(Vec::new());
    };
    let Some(tools) = body.get("tools") else {
        return Ok(Vec::new());
    };
    let Some(array) = tools.as_array() else {
        return Err(format!(
            "step/request seq {}: the tools field is not an array",
            ev.seq
        ));
    };
    let mut schemas = Vec::new();
    for tool in array {
        let function = tool.get("function").unwrap_or(tool);
        let Some(name) = function.get("name").and_then(Value::as_str) else {
            return Err(format!("step/request seq {}: a tool has no name", ev.seq));
        };
        schemas.push(ToolSchema {
            name: name.to_string(),
            description: function
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            parameters: function.get("parameters").cloned().unwrap_or(Value::Null),
        });
    }
    Ok(schemas)
}

// ---------------------------------------------------------------------------
// comparison
// ---------------------------------------------------------------------------

fn compare(recorded: &[Event], replayed: &[Event], notes: &[String]) -> Comparison {
    let a: Vec<&Event> = recorded.iter().filter(|e| comparable(&e.kind)).collect();
    let b: Vec<&Event> = replayed.iter().filter(|e| comparable(&e.kind)).collect();
    let ignored = recorded.len() - a.len();
    let mut divergence: Option<Divergence> = None;
    let mut compared = 0u64;
    for i in 0..a.len().min(b.len()) {
        compared += 1;
        let left = a[i];
        let right = b[i];
        if left.kind != right.kind {
            divergence = Some(Divergence {
                reason: "kind_mismatch".into(),
                recorded_seq: Some(left.seq),
                recorded_kind: Some(kind_label(&left.kind)),
                replayed_seq: Some(right.seq),
                replayed_kind: Some(kind_label(&right.kind)),
                path: None,
                recorded_excerpt: Some(excerpt(&left.bytes)),
                replayed_excerpt: Some(excerpt(&right.bytes)),
            });
            break;
        }
        if left.bytes != right.bytes {
            divergence = Some(Divergence {
                reason: "payload_mismatch".into(),
                recorded_seq: Some(left.seq),
                recorded_kind: Some(kind_label(&left.kind)),
                replayed_seq: Some(right.seq),
                replayed_kind: Some(kind_label(&right.kind)),
                path: Some(diff_path(&left.bytes, &right.bytes, "")),
                recorded_excerpt: Some(excerpt(&left.bytes)),
                replayed_excerpt: Some(excerpt(&right.bytes)),
            });
            break;
        }
    }
    if divergence.is_none() && a.len() != b.len() {
        let (reason, recorded_seq, replayed_seq) = if a.len() > b.len() {
            (
                "recorded_rows_remain",
                a.get(b.len()).map(|e| e.seq),
                b.last().map(|e| e.seq),
            )
        } else {
            (
                "replayed_rows_extra",
                a.last().map(|e| e.seq),
                b.get(a.len()).map(|e| e.seq),
            )
        };
        divergence = Some(Divergence {
            reason: reason.into(),
            recorded_seq,
            recorded_kind: None,
            replayed_seq,
            replayed_kind: None,
            path: None,
            recorded_excerpt: None,
            replayed_excerpt: None,
        });
    }
    let verdict = if !notes.is_empty() {
        "incomplete"
    } else if divergence.is_some() {
        "diverged"
    } else {
        "reproduced"
    };
    Comparison {
        recorded_rows: a.len() as u64,
        replayed_rows: b.len() as u64,
        compared,
        ignored: ignored as u64,
        ignored_kinds: ADMITTED_IGNORED.iter().map(|s| (*s).to_string()).collect(),
        verdict: verdict.into(),
        divergence,
    }
}

fn comparable(kind: &EventKind) -> bool {
    !matches!(kind, EventKind::DrainClaimed)
}

/// The first JSON pointer at which two payloads differ (deterministic key
/// order: serde_json maps are sorted without the preserve_order feature).
fn diff_path(a: &Value, b: &Value, prefix: &str) -> String {
    match (a, b) {
        (Value::Object(x), Value::Object(y)) => {
            let mut keys: Vec<&String> = x.keys().collect();
            for key in y.keys() {
                if !x.contains_key(key) {
                    keys.push(key);
                }
            }
            keys.sort();
            for key in keys {
                let child = format!("{prefix}/{}", key.replace('~', "~0").replace('/', "~1"));
                match (x.get(key), y.get(key)) {
                    (Some(xv), Some(yv)) if xv != yv => return diff_path(xv, yv, &child),
                    (Some(_), None) | (None, Some(_)) => return child,
                    _ => {}
                }
            }
            prefix.to_string()
        }
        (Value::Array(x), Value::Array(y)) => {
            for i in 0..x.len().min(y.len()) {
                if x[i] != y[i] {
                    return diff_path(&x[i], &y[i], &format!("{prefix}/{i}"));
                }
            }
            format!("{prefix}/{}", x.len().min(y.len()))
        }
        _ => prefix.to_string(),
    }
}

fn excerpt(value: &Value) -> String {
    let text = serde_json::to_string(value).unwrap_or_default();
    truncate(&text, MAX_EXCERPT)
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let kept: String = text.chars().take(max).collect();
    format!("{kept}...")
}

fn kind_label(kind: &EventKind) -> String {
    match kind {
        EventKind::SessionEnded { reason } => {
            format!(
                "session/ended ({})",
                crate::self_inspect::end_reason_str(reason)
            )
        }
        other => other.as_str().to_string(),
    }
}

fn state_label(state: &SessionState) -> String {
    serde_json::to_value(state)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".into())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// rendering
// ---------------------------------------------------------------------------

pub fn render(report: &Report) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "{:<20} {}", "session", report.session.0);
    let _ = writeln!(
        out,
        "{:<20} {} (script: {} turn(s) at {})",
        "provider", report.provider, report.script.turns, report.script.path
    );
    let _ = writeln!(
        out,
        "{:<20} seq {} ({}); prefix {} event(s), state {}",
        "anchor",
        report.anchor.seq,
        report.anchor.kind,
        report.anchor.prefix_events,
        report.anchor.prefix_state
    );
    let inputs = report
        .inputs
        .iter()
        .map(|i| format!("{} [{}]", i.seq, i.keys.join(",")))
        .collect::<Vec<_>>()
        .join(", ");
    let _ = writeln!(out, "{:<20} {}", "inputs", inputs);
    let _ = writeln!(
        out,
        "{:<20} {} event(s), ended {}",
        "replay",
        report.replay.events,
        report.replay.end_reason.as_deref().unwrap_or("(unsettled)")
    );
    let _ = writeln!(
        out,
        "{:<20} {} compared, {} ignored ({})",
        "compare",
        report.comparison.compared,
        report.comparison.ignored,
        report.comparison.ignored_kinds.join(", ")
    );
    if let Some(d) = &report.comparison.divergence {
        let _ = writeln!(
            out,
            "{:<20} {}: recorded {} at seq {} vs replayed {} at seq {}",
            "first divergence",
            d.reason,
            d.recorded_kind.as_deref().unwrap_or("-"),
            d.recorded_seq
                .map(|s| s.to_string())
                .unwrap_or_else(|| "-".into()),
            d.replayed_kind.as_deref().unwrap_or("-"),
            d.replayed_seq
                .map(|s| s.to_string())
                .unwrap_or_else(|| "-".into()),
        );
        if let Some(path) = &d.path {
            let _ = writeln!(out, "{:<20} path {}", "", path);
        }
        if let Some(text) = &d.recorded_excerpt {
            let _ = writeln!(out, "{:<20} recorded {}", "", text);
        }
        if let Some(text) = &d.replayed_excerpt {
            let _ = writeln!(out, "{:<20} replayed {}", "", text);
        }
    }
    let _ = writeln!(
        out,
        "{:<20} {}",
        "replay verdict",
        report.comparison.verdict.to_uppercase()
    );
    let _ = writeln!(
        out,
        "{:<20} {} ({})",
        "fork",
        report.fork.path,
        if report.fork.kept { "kept" } else { "removed" }
    );
    for note in &report.notes {
        let _ = writeln!(out, "{:<20} {}", "note", note);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use kian_core::events::SCHEMA_VERSION;
    use kian_core::model::Epoch;

    fn ev(seq: u64, kind: EventKind, bytes: Value) -> Event {
        Event {
            seq,
            session: SessionId::new(),
            epoch: Epoch(1),
            ts: 1_700_000_000_000 + seq,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        }
    }

    fn claimed(seq: u64) -> Event {
        ev(
            seq,
            EventKind::DrainClaimed,
            json!({"holder": "h", "epoch": 1}),
        )
    }

    fn admitted(seq: u64, bytes: Value) -> Event {
        ev(seq, EventKind::InputAdmitted, bytes)
    }

    #[test]
    fn the_inverse_round_trips_every_unspilled_arm() {
        let arms = vec![
            (
                "plain",
                json!("hi"),
                json!({"role": "user", "content": "hi"}),
            ),
            (
                "synthetic",
                json!({"content": "hi", "synthetic": true}),
                json!({"role": "user", "content": "hi", "synthetic": true}),
            ),
            (
                "dream",
                json!({"content": "hi", "dream": {"night": 1}}),
                json!({"role": "user", "content": "hi", "dream": {"night": 1}}),
            ),
            (
                "cron",
                json!({"content": "hi", "cron": "nightly"}),
                json!({"role": "user", "content": "hi", "cron": "nightly"}),
            ),
            (
                "job",
                json!({"content": "hi", "job": {"plan": "p"}}),
                json!({"role": "user", "content": "hi", "job": {"plan": "p"}}),
            ),
            (
                "cron+job",
                json!({"content": "hi", "cron": "nightly", "job": {"plan": "p"}}),
                json!({"role": "user", "content": "hi", "cron": "nightly", "job": {"plan": "p"}}),
            ),
            (
                "cron+defer",
                json!({"content": "hi", "cron": "nightly", "defer": {"off_peak": true}}),
                json!({"role": "user", "content": "hi", "cron": "nightly", "defer": {"off_peak": true}}),
            ),
            (
                "memory",
                json!({"content": "remember", "memory": {"note": 7}}),
                json!({"role": "system", "content": "remember", "synthetic": true, "memory": {"note": 7}}),
            ),
        ];
        for (label, row, admitted) in arms {
            assert_eq!(wrap_row_payload(&row), admitted, "{label}: wrap");
            assert_eq!(invert_admitted(&admitted).unwrap(), row, "{label}: invert");
        }
    }

    #[test]
    fn the_inverse_refuses_an_unknown_shape() {
        let err =
            invert_admitted(&json!({"role": "user", "content": "x", "extra": 1})).unwrap_err();
        assert!(err.contains("cannot invert"), "{err}");
        let err = invert_admitted(&json!({"content": "x"})).unwrap_err();
        assert!(err.contains("lacks a role"), "{err}");
    }

    #[test]
    fn a_boundary_is_a_drain_claimed_row() {
        let events = vec![
            ev(1, EventKind::SessionCreated, json!({})),
            claimed(2),
            admitted(3, json!({"role": "user", "content": "a"})),
            ev(4, EventKind::TurnStart, json!({})),
            claimed(5),
            admitted(6, json!({"role": "user", "content": "b"})),
        ];
        assert_eq!(pick_anchor(&events, None).unwrap(), 4);
        assert_eq!(pick_anchor(&events, Some(2)).unwrap(), 1);
        let err = pick_anchor(&events, Some(3)).unwrap_err();
        assert!(err.contains("nearest at or before it is seq 2"), "{err}");
        let err = pick_anchor(&events, Some(1)).unwrap_err();
        assert!(err.contains("first episode starts at seq 2"), "{err}");
    }

    #[test]
    fn an_episode_without_a_claim_is_refused() {
        let events = vec![ev(1, EventKind::SessionCreated, json!({}))];
        let err = pick_anchor(&events, None).unwrap_err();
        assert!(err.contains("no drain episode"), "{err}");
    }

    #[test]
    fn episode_inputs_refuses_a_late_admit() {
        let tail = vec![
            claimed(10),
            admitted(11, json!({"role": "user", "content": "a"})),
            ev(12, EventKind::TurnStart, json!({})),
            admitted(13, json!({"role": "user", "content": "steer"})),
        ];
        let err = episode_inputs(&tail, 10).unwrap_err();
        assert!(err.contains("mid-episode steer"), "{err}");
    }

    #[test]
    fn episode_inputs_refuses_a_spill_pair() {
        let tail = vec![
            claimed(10),
            admitted(11, json!({"role": "user", "content": "a"})),
            ev(
                12,
                EventKind::InputSpilled,
                json!({"artifact": "x", "preview": "a"}),
            ),
        ];
        let err = episode_inputs(&tail, 10).unwrap_err();
        assert!(err.contains("spilled input pair"), "{err}");
    }

    #[test]
    fn episode_inputs_keeps_the_recorded_order_and_clock() {
        let tail = vec![
            claimed(10),
            admitted(
                11,
                json!({"role": "system", "content": "m", "synthetic": true, "memory": {"n": 1}}),
            ),
            admitted(12, json!({"role": "user", "content": "hi"})),
        ];
        let rows = episode_inputs(&tail, 10).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].seq, 11);
        assert_eq!(
            rows[0].admitted_keys,
            vec!["content", "memory", "role", "synthetic"]
        );
        assert_eq!(
            rows[0].row.payload,
            json!({"content": "m", "memory": {"n": 1}})
        );
        assert_eq!(rows[0].row.admitted_at, 1_700_000_000_011);
        assert_eq!(rows[0].row.idempotency_key.as_deref(), Some("replay:11"));
        assert_eq!(rows[1].row.payload, json!("hi"));
    }

    #[test]
    fn the_comparison_ignores_the_claim_and_reports_the_first_difference() {
        let recorded = vec![
            claimed(10),
            admitted(11, json!({"role": "user", "content": "a"})),
            ev(12, EventKind::TurnStart, json!({"iteration": 1})),
        ];
        let mut replayed = recorded.clone();
        replayed[2] = ev(12, EventKind::TurnStart, json!({"iteration": 2}));
        let notes: Vec<String> = Vec::new();
        let cmp = compare(&recorded, &replayed, &notes);
        assert_eq!(cmp.verdict, "diverged");
        assert_eq!(cmp.compared, 2);
        assert_eq!(cmp.ignored, 1);
        let d = cmp.divergence.unwrap();
        assert_eq!(d.reason, "payload_mismatch");
        assert_eq!(d.path.as_deref(), Some("/iteration"));
    }

    #[test]
    fn the_comparison_reports_extra_rows_and_notes() {
        let recorded = vec![
            claimed(10),
            admitted(11, json!({"role": "user", "content": "a"})),
        ];
        let mut replayed = recorded.clone();
        replayed.push(ev(
            12,
            EventKind::AssistantMessage,
            json!({"role": "assistant"}),
        ));
        let cmp = compare(&recorded, &replayed, &[]);
        assert_eq!(cmp.verdict, "diverged");
        assert_eq!(
            cmp.divergence.unwrap().reason.as_str(),
            "replayed_rows_extra"
        );
        let cmp = compare(&recorded, &recorded, &["a note".to_string()]);
        assert_eq!(cmp.verdict, "incomplete");
    }

    #[test]
    fn a_reproduced_stream_is_reproduced() {
        let events = vec![
            claimed(10),
            admitted(11, json!({"role": "user", "content": "a"})),
            ev(
                12,
                EventKind::SessionEnded {
                    reason: kian_core::events::EndReason::Completed,
                },
                json!({}),
            ),
        ];
        let cmp = compare(&events, &events, &[]);
        assert_eq!(cmp.verdict, "reproduced");
        assert!(cmp.divergence.is_none());
        assert_eq!(cmp.compared, 2);
    }

    #[test]
    fn an_end_reason_change_is_a_kind_mismatch() {
        let recorded = vec![ev(
            12,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Completed,
            },
            json!({}),
        )];
        let replayed = vec![ev(
            12,
            EventKind::SessionEnded {
                reason: kian_core::events::EndReason::Failed,
            },
            json!({}),
        )];
        let cmp = compare(&recorded, &replayed, &[]);
        let d = cmp.divergence.unwrap();
        assert_eq!(d.reason, "kind_mismatch");
        assert!(d.recorded_kind.unwrap().contains("completed"));
    }

    #[test]
    fn schemas_round_trip_out_of_a_request_body() {
        let body = json!({"model": "m", "tools": [
            {"type": "function", "function": {"name": "fs_read", "description": "read", "parameters": {"type": "object"}}}
        ]});
        let events = vec![ev(3, EventKind::StepRequest, json!({"body": body}))];
        let schemas = recorded_schemas(&events).unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].name, "fs_read");
        assert_eq!(schemas[0].parameters, json!({"type": "object"}));
        assert!(
            recorded_schemas(&[ev(1, EventKind::SessionCreated, json!({}))])
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn the_script_refuses_an_ambiguous_turn() {
        let turn = ScriptTurn {
            text: Some("x".into()),
            usage: None,
            tool_calls: None,
            error: Some("provider_down".into()),
            empty: false,
        };
        assert!(
            validate_turn(&turn)
                .unwrap_err()
                .contains("cannot also carry")
        );
        let turn = ScriptTurn {
            text: None,
            usage: None,
            tool_calls: None,
            error: None,
            empty: false,
        };
        assert!(validate_turn(&turn).unwrap_err().contains("needs one of"));
    }

    #[test]
    fn recorded_tools_replay_a_result_by_id_and_by_name() {
        let call = ev(
            4,
            EventKind::ToolCall,
            json!({"role": "assistant", "content": {"id": "c1", "name": "fs_read", "arguments": {}}}),
        );
        let result = ev(
            5,
            EventKind::ToolResult,
            json!({"role": "tool", "content": {"call_id": "c1", "output": {"text": "ok"}, "is_error": false}}),
        );
        let tools = RecordedTools::new(&[call, result]).unwrap();
        let first = futures::executor::block_on(tools.invoke(ToolCall {
            id: "c1".into(),
            name: "fs_read".into(),
            arguments: json!({}),
        }))
        .unwrap();
        assert_eq!(first.output, json!({"text": "ok"}));
        let second = futures::executor::block_on(tools.invoke(ToolCall {
            id: "c2".into(),
            name: "fs_read".into(),
            arguments: json!({}),
        }))
        .unwrap();
        assert_eq!(second.call_id, "c2");
        let err = futures::executor::block_on(tools.invoke(ToolCall {
            id: "c3".into(),
            name: "fs_write".into(),
            arguments: json!({}),
        }))
        .unwrap_err();
        assert!(err.to_string().contains("no recorded result"));
        assert_eq!(tools.notes().len(), 1);
    }
}
