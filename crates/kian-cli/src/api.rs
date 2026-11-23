//! Control-plane API (DESIGN §5 "Control-plane protocol").
//!
//! axum JSON-RPC-over-HTTP + SSE exposing the built-in set:
//! `create_session`, `submit`, `steer`, `interrupt`, `queue`,
//! `confirm`/`pending` (CONFIRMATION-MODEL, ADR-033), `events` (SSE
//! interval polling of the event store), `projection` read.
//!
//! # Layered iron rule (AGENTS.md constraint 9)
//!
//! This module is HTTP-only: it speaks JSON and SSE and calls **nothing
//! but** the [`ControlPlane`] trait — a surface that can only write the
//! inbox and read projections/events. It holds no engine, no driver, no
//! store handle, no event-append path. The iron-rule test
//! (`tests/iron_rule.rs`) enforces this textually: no execution symbol
//! may appear here.

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use kian_core::events::Event;
use kian_core::model::SessionId;
use kian_core::state::SessionState;
use serde::Serialize;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// The control-plane surface (the ONLY thing handlers may touch)
// ---------------------------------------------------------------------------

/// What the API layer may ask of the server: inbox writes and
/// projection/event reads. Anything execution-shaped (drains, claims,
/// event appends, loop driving) is structurally out of reach here.
pub trait ControlPlane: Send + Sync {
    fn create_session(&self) -> BoxFuture<'_, Result<SessionId, ControlError>>;
    fn submit(
        &self,
        session: SessionId,
        content: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<bool, ControlError>>;
    fn steer(
        &self,
        session: SessionId,
        content: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<bool, ControlError>>;
    fn queue(
        &self,
        session: SessionId,
        content: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<bool, ControlError>>;
    /// ADR-060: submits one turn with an explicit deferral policy. The
    /// row is durable before this returns, with its eligibility instant
    /// stamped on it; a control plane with no defer: section refuses.
    fn submit_deferred(
        &self,
        _session: SessionId,
        _content: Value,
        _idempotency_key: Option<String>,
        _policy: crate::defer::DeferPolicy,
    ) -> BoxFuture<'_, Result<DeferredAdmission, ControlError>> {
        async { Err(ControlError::DeferralUnavailable) }.boxed()
    }
    /// ADR-060: queues one turn with an explicit deferral policy. Same
    /// semantics as submit_deferred; the queue/steer marker is unchanged.
    fn queue_deferred(
        &self,
        _session: SessionId,
        _content: Value,
        _idempotency_key: Option<String>,
        _policy: crate::defer::DeferPolicy,
    ) -> BoxFuture<'_, Result<DeferredAdmission, ControlError>> {
        async { Err(ControlError::DeferralUnavailable) }.boxed()
    }
    fn interrupt(&self, session: SessionId) -> BoxFuture<'_, Result<String, ControlError>>;
    /// Answers one pending ask (CONFIRMATION-MODEL §5/§9): a durable
    /// reserved control row plus a wake. Never touches execution.
    fn confirm(
        &self,
        session: SessionId,
        call_id: String,
        decision: kian_core::control::ConfirmDecision,
        reason: Option<String>,
        requested_by: Option<String>,
    ) -> BoxFuture<'_, Result<ConfirmWireOutcome, ControlError>>;
    /// Raises a session's lifetime budget (ADR-055): a durable reserved
    /// control row plus a wake. Never touches execution — the drain
    /// consumes it into `budget/raised`.
    fn raise_budget(
        &self,
        session: SessionId,
        tokens: Option<u64>,
        wall_clock_ms: Option<u64>,
        reason: Option<String>,
        requested_by: Option<String>,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<BudgetWireOutcome, ControlError>>;
    /// The pending asks of one session with the TTL overlay: a pure read.
    fn pending(&self, session: SessionId) -> BoxFuture<'_, Result<Vec<PendingView>, ControlError>>;
    fn projection(&self, session: SessionId) -> BoxFuture<'_, Result<SessionState, ControlError>>;
    fn session_events(&self, session: SessionId)
    -> BoxFuture<'_, Result<Vec<Event>, ControlError>>;
    /// READ-MODEL §3 (P0): the versioned projection read. `since` is
    /// the caller's last version: equal to the current one answers
    /// `unchanged` with no state and no scan beyond the head check.
    fn projection_versioned(
        &self,
        session: SessionId,
        since: Option<u64>,
    ) -> BoxFuture<'_, Result<ProjectionRead, ControlError>>;
    /// READ-MODEL §4.1 (P0): the SSE tail read — events
    /// `(after, head]` plus the head version and the terminal probe,
    /// from one head query and one bounded scan (per-poll work is
    /// O(new events), never O(history)).
    fn session_events_after(
        &self,
        session: SessionId,
        after: u64,
    ) -> BoxFuture<'_, Result<EventTailRead, ControlError>>;
    /// READ-MODEL §4.2 (P3): the current head version of one session
    /// — the tail-only fan-in's start point. One indexed head query:
    /// the pre-connection history is never replayed and never scanned.
    /// `0` (no events) maps to [`ControlError::UnknownSession`].
    fn session_version(&self, session: SessionId) -> BoxFuture<'_, Result<u64, ControlError>>;
}

/// The versioned projection result (READ-MODEL §3). `unchanged`
/// responses carry no state (R3).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProjectionRead {
    /// The committed prefix the payload was derived from
    /// (`head.next_seq - 1`).
    pub version: u64,
    /// `since == version`: nothing changed, no state is carried.
    pub unchanged: bool,
    /// `Some(state)` iff `unchanged` is false.
    pub state: Option<SessionState>,
}

/// One SSE tail read (READ-MODEL §4.1).
#[derive(Debug, Clone, PartialEq)]
pub struct EventTailRead {
    /// The head version the read observed.
    pub version: u64,
    /// Exactly the events with `after < seq <= version`, seq order.
    pub events: Vec<Event>,
    /// The stream's last committed event is `session/ended` — the
    /// SSE `done` condition.
    pub terminal: bool,
}

/// The `confirm` wire outcome (CONFIRMATION-MODEL §9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmWireOutcome {
    /// The row is durable; the decision takes effect on the stream.
    Accepted,
    /// No unanswered ask with that `call_id`.
    NotPending,
    /// The call lapsed under `confirmation.ttl_ms`; a reject-expired
    /// row was admitted so the session resumes (the decision was not
    /// applied).
    Expired,
}

impl ConfirmWireOutcome {
    /// The wire spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::NotPending => "not_pending",
            Self::Expired => "expired",
        }
    }
}

/// The `raise_budget` wire outcome (ADR-055).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetWireOutcome {
    /// The row is durable; the projection adds it when a drain consumes it.
    Accepted,
    /// An exact replay of the same raise already has its durable row.
    Deduplicated,
    /// The session is terminal — a raise would be unconsumable inbox junk.
    Terminal,
}

impl BudgetWireOutcome {
    /// The wire spelling.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Deduplicated => "deduplicated",
            Self::Terminal => "terminal",
        }
    }
}

/// One pending confirmation as the `pending` read reports it
/// (CONFIRMATION-MODEL §9).
#[derive(Debug, Clone, PartialEq)]
pub struct PendingView {
    pub call_id: String,
    pub tool: String,
    pub arguments: Value,
    pub requested_at_ms: u64,
    pub expired: bool,
}

/// The deferred submit/queue outcome (ADR-060): the row is durable and
/// carries an eligibility instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeferredAdmission {
    /// An exact replay of the same row already exists.
    pub deduplicated: bool,
    /// The instant stamped on the durable row.
    pub eligible_at_ms: u64,
    /// True when that instant is in the future because a policy asked for
    /// it; false when the row is immediately eligible.
    pub deferred: bool,
}

/// Control-plane failures.
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("unknown session {session:?}")]
    UnknownSession { session: SessionId },
    /// READ-MODEL §3: `since` is ahead of the durable head — the
    /// client is ahead or pointed at another store (`-32602`).
    #[error("since {since} is ahead of session {session:?} version {version}")]
    SinceAhead {
        session: SessionId,
        since: u64,
        version: u64,
    },
    /// READ-MODEL §7 (P2b): the session's online range was archived —
    /// an explicit refusal naming the package, never a silent
    /// truncation (`-32007`).
    #[error(
        "session {session:?} is archived (online range removed at {archived_at}); use the archive package / session import"
    )]
    Archived {
        session: SessionId,
        archived_at: u64,
    },
    /// ADR-055: a raise with both amounts absent or zero is a client
    /// error (`-32602`), never a durable row.
    #[error("invalid budget raise: {reason}")]
    InvalidBudget { reason: String },
    /// ADR-060: the caller asked to defer but this server has no defer:
    /// section, so it cannot promise a future eligibility instant.
    #[error("deferral is not configured (no defer: section)")]
    DeferralUnavailable,
    #[error("{0}")]
    Internal(String),
}

/// JSON-RPC error code for an unknown session.
const CODE_UNKNOWN_SESSION: i64 = -32001;
/// JSON-RPC error code for server-side failures.
const CODE_INTERNAL: i64 = -32000;
/// JSON-RPC error code for an archived session (READ-MODEL §7). The
/// planned P2b code in API-CONTRACT §5.3 was `-32005`, but the
/// append-only registry had already given `-32005`/`-32006` to the
/// round-8 memory surface — `archived` is the next free code.
pub const CODE_ARCHIVED: i64 = -32007;
/// Standard JSON-RPC codes.
const CODE_METHOD_NOT_FOUND: i64 = -32601;
const CODE_INVALID_PARAMS: i64 = -32602;
const CODE_INVALID_REQUEST: i64 = -32600;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AppState {
    plane: Arc<dyn ControlPlane>,
    sse_poll_ms: u64,
    /// The fan-in metrics handle (READ-MODEL §4.2 / API-CONTRACT §6),
    /// present iff the route is mounted (`api.event_stream.enabled`).
    event_stream: Option<Arc<EventStreamMetrics>>,
}

/// Builds the built-in control-plane router.
pub fn router(plane: Arc<dyn ControlPlane>, sse_poll_ms: u64) -> Router {
    router_with_event_stream(
        plane,
        sse_poll_ms,
        Arc::new(EventStreamMetrics::new()),
        true,
    )
}

/// [`router`] with an explicit fan-in event-stream plan
/// (READ-MODEL §4.2, P3): the shared metrics handle `/metrics`
/// reports, and the `api.event_stream.enabled` gate. Disabled mounts
/// no `/events` route — the path 404s exactly like an unconfigured
/// webhook receiver.
pub fn router_with_event_stream(
    plane: Arc<dyn ControlPlane>,
    sse_poll_ms: u64,
    event_stream: Arc<EventStreamMetrics>,
    event_stream_enabled: bool,
) -> Router {
    let event_stream = event_stream_enabled.then_some(event_stream);
    let state = AppState {
        plane,
        sse_poll_ms,
        event_stream,
    };
    let mut router = Router::new()
        .route("/rpc", post(rpc))
        .route("/sessions/{id}/events", get(session_events_sse));
    if state.event_stream.is_some() {
        router = router.route("/events", get(fan_in_events_sse));
    }
    router.with_state(state)
}

// ---------------------------------------------------------------------------
// JSON-RPC over HTTP
// ---------------------------------------------------------------------------

async fn rpc(State(state): State<AppState>, Json(req): Json<Value>) -> Response {
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let Some(method) = req.get("method").and_then(Value::as_str) else {
        return rpc_error(id, CODE_INVALID_REQUEST, "missing `method`".into());
    };
    if req.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return rpc_error(id, CODE_INVALID_REQUEST, "`jsonrpc` must be \"2.0\"".into());
    }
    let params = req.get("params").cloned().unwrap_or(json!({}));
    match dispatch(&state, method, &params).await {
        Ok(result) => (
            StatusCode::OK,
            Json(json!({"jsonrpc": "2.0", "id": id, "result": result})),
        )
            .into_response(),
        Err((code, message)) => rpc_error(id, code, message),
    }
}

fn rpc_error(id: Value, code: i64, message: String) -> Response {
    (
        StatusCode::OK,
        Json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": code, "message": message}
        })),
    )
        .into_response()
}

/// Maps a [`ControlError`] onto JSON-RPC codes.
fn control_err(e: ControlError) -> (i64, String) {
    match e {
        ControlError::UnknownSession { session } => (
            CODE_UNKNOWN_SESSION,
            format!("unknown session {:?}", session.0),
        ),
        ControlError::SinceAhead {
            session,
            since,
            version,
        } => (
            CODE_INVALID_PARAMS,
            format!(
                "since {since} is ahead of session {:?} version {version} \
                 (client ahead or another store)",
                session.0
            ),
        ),
        // READ-MODEL §7: the explicit archived refusal, wire-coded
        // `-32007` with the package guidance in the message.
        ControlError::Archived {
            session,
            archived_at,
        } => (
            CODE_ARCHIVED,
            format!(
                "session {:?} is archived (online range removed at {archived_at}); \
                 use the archive package / `kian session import`",
                session.0
            ),
        ),
        ControlError::InvalidBudget { reason } => (CODE_INVALID_PARAMS, reason),
        ControlError::DeferralUnavailable => (
            CODE_INVALID_PARAMS,
            "deferral is not configured (no `defer:` section)".to_string(),
        ),
        ControlError::Internal(msg) => (CODE_INTERNAL, msg),
    }
}

async fn dispatch(state: &AppState, method: &str, params: &Value) -> Result<Value, (i64, String)> {
    match method {
        "create_session" => {
            expect_empty(params)?;
            let sid = state.plane.create_session().await.map_err(control_err)?;
            Ok(json!({"session": sid.0}))
        }
        "submit" => {
            let (session, content, key) = input_params(params)?;
            match defer_param(params)? {
                Some(policy) => {
                    let admission = state
                        .plane
                        .submit_deferred(session, content, key, policy)
                        .await
                        .map_err(control_err)?;
                    Ok(deferred_result(&admission))
                }
                None => {
                    let dedup = state
                        .plane
                        .submit(session, content, key)
                        .await
                        .map_err(control_err)?;
                    Ok(json!({"deduplicated": dedup}))
                }
            }
        }
        "steer" => {
            let (session, content, key) = input_params(params)?;
            reject_defer_param(params, "steer")?;
            let dedup = state
                .plane
                .steer(session, content, key)
                .await
                .map_err(control_err)?;
            Ok(json!({"deduplicated": dedup}))
        }
        "queue" => {
            let (session, content, key) = input_params(params)?;
            match defer_param(params)? {
                Some(policy) => {
                    let admission = state
                        .plane
                        .queue_deferred(session, content, key, policy)
                        .await
                        .map_err(control_err)?;
                    Ok(deferred_result(&admission))
                }
                None => {
                    let dedup = state
                        .plane
                        .queue(session, content, key)
                        .await
                        .map_err(control_err)?;
                    Ok(json!({"deduplicated": dedup}))
                }
            }
        }
        "interrupt" => {
            let session = session_params(params)?;
            let outcome = state.plane.interrupt(session).await.map_err(control_err)?;
            Ok(json!({"outcome": outcome}))
        }
        "confirm" => {
            let session = session_params(params)?;
            let call_id = params
                .get("call_id")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    (
                        CODE_INVALID_PARAMS,
                        "missing/non-string `call_id`".to_string(),
                    )
                })?
                .to_owned();
            let decision = params
                .get("decision")
                .and_then(Value::as_str)
                .and_then(kian_core::control::ConfirmDecision::parse)
                .ok_or_else(|| {
                    (
                        CODE_INVALID_PARAMS,
                        "`decision` must be \"approve\" or \"reject\"".to_string(),
                    )
                })?;
            let reason = optional_string(params, "reason")?;
            let requested_by = optional_string(params, "requested_by")?;
            let outcome = state
                .plane
                .confirm(session, call_id, decision, reason, requested_by)
                .await
                .map_err(control_err)?;
            Ok(json!({"outcome": outcome.as_str()}))
        }
        "raise_budget" => {
            let session = session_params(params)?;
            let tokens = optional_u64(params, "tokens")?;
            let wall_clock_ms = optional_u64(params, "wall_clock_ms")?;
            let reason = optional_string(params, "reason")?;
            let requested_by = optional_string(params, "requested_by")?;
            let idempotency_key = optional_string(params, "idempotency_key")?;
            let outcome = state
                .plane
                .raise_budget(
                    session,
                    tokens,
                    wall_clock_ms,
                    reason,
                    requested_by,
                    idempotency_key,
                )
                .await
                .map_err(control_err)?;
            Ok(json!({"outcome": outcome.as_str()}))
        }
        "pending" => {
            let session = session_params(params)?;
            let pending = state.plane.pending(session).await.map_err(control_err)?;
            let items: Vec<Value> = pending
                .into_iter()
                .map(|p| {
                    json!({
                        "call_id": p.call_id,
                        "tool": p.tool,
                        "arguments": p.arguments,
                        "requested_at_ms": p.requested_at_ms,
                        "expired": p.expired,
                    })
                })
                .collect();
            Ok(json!({"pending": items}))
        }
        "projection" => {
            let session = session_params(params)?;
            let since = since_param(params)?;
            let read = state
                .plane
                .projection_versioned(session, since)
                .await
                .map_err(control_err)?;
            if read.unchanged {
                // READ-MODEL §3/R3: no state on the unchanged path.
                Ok(json!({"version": read.version, "unchanged": true}))
            } else {
                Ok(json!({
                    "version": read.version,
                    "state": read.state.expect("a changed read carries state"),
                }))
            }
        }
        other => Err((
            CODE_METHOD_NOT_FOUND,
            format!(
                "unknown method `{other}` (baseline set: create_session, submit, steer, interrupt, queue, confirm, raise_budget, projection; events is the SSE endpoint GET /sessions/{{id}}/events)"
            ),
        )),
    }
}

fn expect_empty(params: &Value) -> Result<(), (i64, String)> {
    if params.as_object().is_some_and(|o| o.is_empty()) || params.is_null() {
        Ok(())
    } else {
        Err((CODE_INVALID_PARAMS, "method takes no params".into()))
    }
}

fn session_of(params: &Value) -> Result<SessionId, (i64, String)> {
    let s = params
        .get("session")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            (
                CODE_INVALID_PARAMS,
                "missing `session` (uuid string)".to_string(),
            )
        })?;
    let uuid: uuid::Uuid = s.parse().map_err(|_| {
        (
            CODE_INVALID_PARAMS,
            format!("`session` `{s}` is not a uuid"),
        )
    })?;
    Ok(SessionId(uuid))
}

/// Reserved row-envelope key sets (DESIGN §3.1, MEMORY-MODEL §5 D1 and
/// §6 I1): `{content, dream}` (the ops-plane dream admit),
/// `{content, spill}` (the Inline/Spill shape), `{synthetic, content}`
/// (the strippable steer marker) and `{content, memory}` (the
/// creation-time memory injection). Only the server's internal planes
/// produce these shapes — and the drain (`consumption_plan`) consumes
/// each envelope only at exactly two keys (kian-96, review N-1), so a
/// superset that passes this guard lands as ordinary nested content,
/// never as a client-minted marker. A public `submit`/`steer`/`queue`
/// carrying an exact reserved shape would let client content forge a
/// marker the server is documented to set itself (kian-94, review
/// B-2′; kian-99), so the wire refuses them (`-32602`).
fn is_reserved_envelope(content: &Value) -> bool {
    // CONFIRMATION-MODEL §5: the reserved control envelope
    // (`{"kian":{"type":"control",...}}`, any action) is a server-plane
    // shape — a public input carrying it would let client content mint a
    // control row (an interrupt or an approval).
    if kian_core::control::is_reserved_control_shape(content) {
        return true;
    }
    let Some(obj) = content.as_object() else {
        return false;
    };
    obj.len() == 2
        && obj.contains_key("content")
        && ["dream", "spill", "synthetic", "memory"]
            .iter()
            .any(|k| obj.contains_key(*k))
}

fn input_params(params: &Value) -> Result<(SessionId, Value, Option<String>), (i64, String)> {
    let session = session_of(params)?;
    let content = params
        .get("content")
        .cloned()
        .ok_or_else(|| (CODE_INVALID_PARAMS, "missing `content`".to_string()))?;
    if is_reserved_envelope(&content) {
        return Err((
            CODE_INVALID_PARAMS,
            "`content` is a reserved row-envelope key set ({content,dream} / {content,spill} \
             / {synthetic,content} / {content,memory} / the control envelope); only the \
             server's internal planes admit those rows"
                .to_string(),
        ));
    }
    let idempotency_key = params
        .get("idempotency_key")
        .and_then(Value::as_str)
        .map(str::to_owned);
    Ok((session, content, idempotency_key))
}

fn session_params(params: &Value) -> Result<SessionId, (i64, String)> {
    session_of(params)
}

/// The optional defer param (ADR-060): absent means immediate; the only
/// accepted value is "off_peak". Anything else is -32602, never silently
/// ignored.
fn defer_param(params: &Value) -> Result<Option<crate::defer::DeferPolicy>, (i64, String)> {
    match params.get("defer") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(raw)) => {
            crate::defer::DeferPolicy::parse(raw)
                .map(Some)
                .ok_or_else(|| {
                    (
                        CODE_INVALID_PARAMS,
                        "defer must be \"off_peak\" (the only deferral policy)".to_string(),
                    )
                })
        }
        Some(_) => Err((
            CODE_INVALID_PARAMS,
            "defer must be a string when present".to_string(),
        )),
    }
}

/// Steering is an interactive correction: it is never delayed (ADR-060).
fn reject_defer_param(params: &Value, method: &str) -> Result<(), (i64, String)> {
    if params.get("defer").is_some_and(|value| !value.is_null()) {
        return Err((
            CODE_INVALID_PARAMS,
            format!("{method} cannot defer: interactive steering is never delayed (ADR-060)"),
        ));
    }
    Ok(())
}

fn deferred_result(admission: &DeferredAdmission) -> Value {
    json!({
        "deduplicated": admission.deduplicated,
        "deferred": admission.deferred,
        "eligible_at_ms": admission.eligible_at_ms,
    })
}

/// An optional string param: absent/null → `None`; any other non-string
/// type is `-32602` (never silently ignored).
fn optional_u64(params: &Value, name: &str) -> Result<Option<u64>, (i64, String)> {
    match params.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(v) => v.as_u64().map(Some).ok_or_else(|| {
            (
                CODE_INVALID_PARAMS,
                format!("`{name}` must be a non-negative integer when present"),
            )
        }),
    }
}

fn optional_string(params: &Value, name: &str) -> Result<Option<String>, (i64, String)> {
    match params.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err((
            CODE_INVALID_PARAMS,
            format!("`{name}` must be a string when present"),
        )),
    }
}

/// The optional `projection.since` (READ-MODEL §3): absent → `None`;
/// present it must be a u64 — anything else (including explicit
/// `null`) is `-32602`, never silently treated as absent.
fn since_param(params: &Value) -> Result<Option<u64>, (i64, String)> {
    match params.get("since") {
        None => Ok(None),
        Some(v) => v.as_u64().map(Some).ok_or_else(|| {
            (
                CODE_INVALID_PARAMS,
                "`since` must be a u64 (the caller's last version)".to_string(),
            )
        }),
    }
}

// ---------------------------------------------------------------------------
// SSE `events` (interval polling of the event stream)
// ---------------------------------------------------------------------------

/// One `event: error` frame then close (READ-MODEL §4.1: an invalid
/// resume is an explicit error, never a silent restart).
fn sse_error_stream(message: String) -> Response {
    let stream = futures::stream::iter(vec![Ok::<_, Infallible>(
        SseEvent::default().event("error").data(message),
    )]);
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

async fn session_events_sse(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: axum::http::HeaderMap,
) -> Response {
    let Ok(uuid) = id.parse::<uuid::Uuid>() else {
        return (StatusCode::BAD_REQUEST, "session id must be a uuid").into_response();
    };
    let session = SessionId(uuid);
    let plane = Arc::clone(&state.plane);
    let poll = Duration::from_millis(state.sse_poll_ms);

    // READ-MODEL §4.1: a reconnect with `Last-Event-ID: <seq>` resumes
    // after that seq (frames before the resume point are never
    // re-emitted). A non-numeric resume id is an explicit error frame
    // followed by close.
    let start = match headers.get("last-event-id").map(|v| v.to_str()) {
        None => 0u64,
        Some(Ok(raw)) => match raw.trim().parse::<u64>() {
            Ok(seq) => seq,
            Err(_) => {
                return sse_error_stream(format!("Last-Event-ID `{raw}` is not a numeric seq"));
            }
        },
        Some(Err(_)) => return sse_error_stream("Last-Event-ID is not valid UTF-8".into()),
    };

    // Poll the tail every `poll` and emit events the client has not
    // seen yet (seq strictly increases; each frame carries `id: <seq>`
    // so a reconnect can resume). The stream ends after the
    // session/ended event — a terminal projection freezes the stream,
    // so there is nothing more to observe. Per-poll work is the
    // bounded tail read `(last, head]` (one head query + one scan) —
    // never a full-stream rebuild.
    let stream = futures::stream::unfold(
        (plane, session, start, false),
        move |(plane, session, mut last_seq, done)| {
            async move {
                if done {
                    return None;
                }
                // Batch of SSE frames for this poll cycle (empty while
                // nothing new arrived; the loop below waits an interval
                // and re-polls instead of yielding).
                let mut items: Vec<Result<SseEvent, Infallible>> = Vec::new();
                let mut done_next = false;
                loop {
                    match plane.session_events_after(session, last_seq).await {
                        Err(e) => {
                            let (_, msg) = control_err(e);
                            items.push(Ok(SseEvent::default().event("error").data(msg)));
                            done_next = true;
                            break;
                        }
                        Ok(tail) => {
                            if tail.events.is_empty() {
                                if tail.terminal {
                                    // At the head of a terminal stream
                                    // (e.g. resumed at the end write):
                                    // replayed, then done.
                                    items.push(Ok(SseEvent::default().event("done").data("ended")));
                                    done_next = true;
                                    break;
                                }
                                // Nothing new: wait one interval, poll again.
                                tokio::time::sleep(poll).await;
                                continue;
                            }
                            for e in &tail.events {
                                last_seq = e.seq;
                                items.push(Ok(SseEvent::default()
                                    .event("kian")
                                    .id(e.seq.to_string())
                                    .data(serde_json::to_string(e).unwrap_or_default())));
                            }
                            if tail.terminal {
                                items.push(Ok(SseEvent::default().event("done").data("ended")));
                                done_next = true;
                            }
                            break;
                        }
                    }
                }
                Some((
                    futures::stream::iter(items),
                    (plane, session, last_seq, done_next),
                ))
            }
        },
    )
    .flatten();

    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

// ---------------------------------------------------------------------------
// SSE fan-in `GET /events` (READ-MODEL §4.2, P3 — explicit-allowlist
// multi-session stream)
// ---------------------------------------------------------------------------

/// The fan-in allowlist cap (READ-MODEL §4.2 / API-CONTRACT §5.4): at
/// most 64 sessions per connection. A fixed contract constant, not a
/// tunable — more listed ids (before dedup) is HTTP 400.
pub const MAX_FAN_IN_SESSIONS: usize = 64;

/// The per-cycle frame cap (bounded backpressure, READ-MODEL §4.2): one
/// poll cycle emits at most this many frames before yielding to the
/// consumer, so a connection never buffers more than one bounded batch
/// however fast the watched sessions append. The watch state advances
/// only to the last emitted seq, so the next cycle continues exactly
/// where the batch stopped — nothing is dropped, nothing re-emitted.
const MAX_FAN_IN_BATCH: usize = 512;

/// The durable kind names (EVENT-MODEL §3 catalog) the optional
/// `kinds` allowlist may name. A listed kind outside this set is a
/// malformed request (HTTP 400), never a silently-empty filter. The
/// sync test below pins this table against `EventKind::as_str`.
const KNOWN_EVENT_KINDS: [&str; 22] = [
    "session/created",
    "input/admitted",
    "drain/claimed",
    "turn/start",
    "turn/end",
    "step/request",
    "assistant/message",
    "tool/call",
    "tool/result",
    "compact",
    "slot/binding",
    "slot/fallback",
    "slot/migration",
    "recovery/budget-reset",
    "thread/started",
    "thread/completed",
    "evidence/verdict",
    "channel/delivered",
    "input/spilled",
    "control/interrupt",
    "memory/note",
    "session/ended",
];

/// The parsed fan-in request: an explicit session allowlist (ordered,
/// deduped, ≤ [`MAX_FAN_IN_SESSIONS`]) and an optional kind allowlist.
#[derive(Debug)]
struct FanInQuery {
    sessions: Vec<SessionId>,
    kinds: Option<Vec<&'static str>>,
}

/// Parses the `GET /events` query pairs. `Err` is the HTTP 400 body —
/// malformed means: `sessions` missing/repeated, a non-UUID id
/// (including `*` — there is **no wildcard** in OSS), an empty entry,
/// over-cap, a repeated `kinds` parameter, an empty kind token, or a
/// kind outside the EVENT-MODEL catalog.
fn fan_in_query(pairs: &[(String, String)]) -> Result<FanInQuery, String> {
    let mut sessions_raw: Option<&str> = None;
    let mut kinds_raw: Option<&str> = None;
    for (key, value) in pairs {
        // `sessions`/`kinds` must appear exactly once; unknown keys are
        // ignored (forward compatibility, the same posture as unknown
        // JSON-RPC params).
        let slot = match key.as_str() {
            "sessions" => &mut sessions_raw,
            "kinds" => &mut kinds_raw,
            _ => continue,
        };
        if slot.replace(value.as_str()).is_some() {
            return Err(format!("`{key}` must appear exactly once"));
        }
    }
    let raw = sessions_raw.ok_or_else(|| {
        "`sessions` is required: GET /events?sessions=<uuid,...>[&kinds=<kind,...>]".to_string()
    })?;
    let mut sessions: Vec<SessionId> = Vec::new();
    let mut listed = 0usize;
    for token in raw.split(',') {
        let token = token.trim();
        listed += 1;
        if token.is_empty() {
            return Err("`sessions` contains an empty entry".into());
        }
        if token == "*" {
            return Err(
                "no wildcard: `sessions` must be an explicit allowlist of session uuids".into(),
            );
        }
        let uuid: uuid::Uuid = token.parse().map_err(|_| {
            format!("`sessions` entry `{token}` is not a uuid (no wildcard: explicit ids only)")
        })?;
        let session = SessionId(uuid);
        if !sessions.contains(&session) {
            sessions.push(session);
        }
    }
    if listed > MAX_FAN_IN_SESSIONS {
        return Err(format!(
            "`sessions` lists {listed} ids; the fan-in allowlist is capped at \
             {MAX_FAN_IN_SESSIONS}"
        ));
    }
    if sessions.is_empty() {
        return Err("`sessions` must list at least one session uuid".into());
    }
    let kinds = match kinds_raw {
        None => None,
        Some(raw) => {
            let mut kinds: Vec<&'static str> = Vec::new();
            for token in raw.split(',') {
                let token = token.trim();
                if token.is_empty() {
                    return Err("`kinds` contains an empty entry".into());
                }
                let known = KNOWN_EVENT_KINDS
                    .iter()
                    .find(|kind| **kind == token)
                    .copied()
                    .ok_or_else(|| {
                        format!(
                            "`kinds` entry `{token}` is not a known event kind \
                             (the EVENT-MODEL catalog)"
                        )
                    })?;
                if !kinds.contains(&known) {
                    kinds.push(known);
                }
            }
            if kinds.is_empty() {
                return Err("`kinds` must name at least one event kind".into());
            }
            Some(kinds)
        }
    };
    Ok(FanInQuery { sessions, kinds })
}

/// The fan-in metrics fragment (API-CONTRACT §6, the `event_stream`
/// object). `subscribers` and `sessions_watched` are live gauges
/// (open fan-in connections / sessions they still watch); `events_sent`
/// is a monotonic counter (process-local, like `engine`).
#[derive(Debug, Default)]
pub struct EventStreamMetrics {
    subscribers: AtomicU64,
    sessions_watched: AtomicU64,
    events_sent: AtomicU64,
}

/// The `/metrics` view of [`EventStreamMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct EventStreamSnapshot {
    pub subscribers: u64,
    pub sessions_watched: u64,
    pub events_sent: u64,
}

impl EventStreamMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// The current snapshot (Relaxed loads — these are observability
    /// counters, not synchronization).
    pub fn snapshot(&self) -> EventStreamSnapshot {
        EventStreamSnapshot {
            subscribers: self.subscribers.load(Ordering::Relaxed),
            sessions_watched: self.sessions_watched.load(Ordering::Relaxed),
            events_sent: self.events_sent.load(Ordering::Relaxed),
        }
    }
}

/// One watched session of an open fan-in connection: its uuid and the
/// last seq this connection emitted (its private tail cursor).
struct FanInWatch {
    session: SessionId,
    last: u64,
}

/// Connection-lifetime accounting: decrements the metrics gauges when
/// the stream is dropped (client disconnect or server-side close) —
/// including the sessions still watched at that moment.
struct FanInGuard {
    metrics: Arc<EventStreamMetrics>,
    watched: AtomicU64,
}

impl FanInGuard {
    fn session_added(&self) {
        self.watched.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .sessions_watched
            .fetch_add(1, Ordering::Relaxed);
    }

    fn session_dropped(&self) {
        self.watched.fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .sessions_watched
            .fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for FanInGuard {
    fn drop(&mut self) {
        self.metrics.subscribers.fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .sessions_watched
            .fetch_sub(self.watched.load(Ordering::Relaxed), Ordering::Relaxed);
    }
}

/// `GET /events?sessions=<uuid,...>[&kinds=<kind,...>]` — the
/// explicit-allowlist fan-in stream (READ-MODEL §4.2, API-CONTRACT
/// §5.4). Tail-only: the connection starts at each listed session's
/// current head (one version query per session — no history replay, no
/// history scan) and then polls bounded tails. There is no resume
/// header support in P3 — a `Last-Event-ID` header is ignored;
/// per-session resume stays on `GET /sessions/{id}/events`.
///
/// Frames: `event: kian` with `id: <session>:<seq>` (the data is the
/// event envelope, exactly the per-session stream's shape) and
/// `event: error` — one frame per failed listed session (unknown,
/// archived, or unreadable), naming it; the stream continues with the
/// others. No ordering across sessions is implied; per session the
/// seq is strictly increasing on the connection. A session that ends
/// is delivered through its `session/ended` event and then dropped
/// from the watch; the stream closes when every listed session has
/// ended or errored (there is no `done` frame — that is a per-session
/// concept).
async fn fan_in_events_sse(
    State(state): State<AppState>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> Response {
    let Some(metrics) = state.event_stream.clone() else {
        // Unreachable while mounted (the route is only mounted when
        // enabled); kept fail-closed anyway.
        return (StatusCode::NOT_FOUND, "event stream disabled").into_response();
    };
    let query = match fan_in_query(&pairs) {
        Ok(query) => query,
        Err(message) => return (StatusCode::BAD_REQUEST, message).into_response(),
    };
    let poll = Duration::from_millis(state.sse_poll_ms);
    metrics.subscribers.fetch_add(1, Ordering::Relaxed);
    let guard = Arc::new(FanInGuard {
        metrics: Arc::clone(&metrics),
        watched: AtomicU64::new(0),
    });
    let kinds = query.kinds.clone();

    // Tail-only start (READ-MODEL §4.2): one head query per listed
    // session, taken *here* — before the response is handed back — and
    // one error frame per unlistable session.
    //
    // Establishing the cursors lazily on the first poll cycle raced an
    // event appended immediately after the client opened the stream: the
    // lazily-read head was already past that event, so a tail-only
    // consumer silently missed it (the e2e caught this on a loaded CI
    // runner). Reading the head at accept time makes "tail from open"
    // mean what it says.
    let mut watch: Vec<FanInWatch> = Vec::new();
    let mut pending: Vec<SseEvent> = Vec::new();
    for session in query.sessions.iter().copied() {
        match state.plane.session_version(session).await {
            Ok(version) => {
                watch.push(FanInWatch {
                    session,
                    last: version,
                });
                guard.session_added();
            }
            Err(e) => {
                let (_, message) = control_err(e);
                pending.push(
                    SseEvent::default()
                        .event("error")
                        .data(format!("{message} — dropped from this stream")),
                );
            }
        }
    }
    let close_after_pending = watch.is_empty();

    // State machine per cycle: poll every watched session's bounded tail. Backpressure is
    // bounded twice over: the unfold future is only advanced when the
    // consumer reads (a slow or stalled consumer suspends polling
    // outright — the server never buffers on its behalf), and one
    // cycle emits at most MAX_FAN_IN_BATCH frames before yielding.
    let stream = futures::stream::unfold(
        (
            Arc::clone(&state.plane),
            watch,
            pending,
            kinds,
            metrics,
            guard,
            close_after_pending,
            false, // close after this yield?
        ),
        move |(
            plane,
            mut watch,
            mut pending,
            kinds,
            metrics,
            guard,
            close_after_pending,
            mut done,
        )| {
            async move {
                if done {
                    return None;
                }
                let mut items: Vec<Result<SseEvent, Infallible>> = Vec::new();
                if !pending.is_empty() {
                    for event in pending.drain(..) {
                        items.push(Ok(event));
                    }
                    if close_after_pending {
                        done = true;
                    }
                }
                if watch.is_empty() && pending.is_empty() {
                    done = true;
                } else {
                    loop {
                        let mut i = 0;
                        while i < watch.len() && items.len() < MAX_FAN_IN_BATCH {
                            let FanInWatch { session, last } = watch[i];
                            match plane.session_events_after(session, last).await {
                                Err(e) => {
                                    let (_, message) = control_err(e);
                                    items.push(Ok(SseEvent::default()
                                        .event("error")
                                        .data(format!("{message} — dropped from this stream"))));
                                    watch.remove(i);
                                    guard.session_dropped();
                                }
                                Ok(tail) => {
                                    // Emit up to the batch cap; the
                                    // cursor advances only to the last
                                    // emitted seq so the next cycle
                                    // resumes exactly there.
                                    let mut emitted = last;
                                    let mut capped = false;
                                    for event in &tail.events {
                                        if items.len() >= MAX_FAN_IN_BATCH {
                                            capped = true;
                                            break;
                                        }
                                        emitted = event.seq;
                                        let wanted = kinds.as_ref().is_none_or(|kinds| {
                                            kinds.contains(&event.kind.as_str())
                                        });
                                        if wanted {
                                            items.push(Ok(SseEvent::default()
                                                .event("kian")
                                                .id(format!("{}:{}", session.0, event.seq))
                                                .data(
                                                    serde_json::to_string(event)
                                                        .unwrap_or_default(),
                                                )));
                                            metrics.events_sent.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                    watch[i].last = emitted;
                                    if tail.terminal && !capped {
                                        // Frozen stream, fully
                                        // delivered: nothing more to
                                        // observe on this session.
                                        watch.remove(i);
                                        guard.session_dropped();
                                    } else {
                                        i += 1;
                                    }
                                }
                            }
                        }
                        if watch.is_empty() {
                            // Every listed session has ended or
                            // errored: close after this yield.
                            done = true;
                            break;
                        }
                        if items.is_empty() {
                            // Nothing new for anyone: wait one
                            // interval, poll again.
                            tokio::time::sleep(poll).await;
                            continue;
                        }
                        break;
                    }
                }
                Some((
                    futures::stream::iter(items),
                    (
                        plane,
                        watch,
                        pending,
                        kinds,
                        metrics,
                        guard,
                        close_after_pending,
                        done,
                    ),
                ))
            }
        },
    )
    .flatten();

    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{CODE_INVALID_PARAMS, input_params, is_reserved_envelope};

    /// Exactly the three reserved two-key shapes are refused; anything
    /// else (extra keys, single keys, other pairs, non-objects) stays
    /// ordinary client content.
    #[test]
    fn reserved_envelope_detection_is_exact() {
        for reserved in [
            json!({"content": "x", "dream": {"source_session": "ghost"}}),
            json!({"content": "x", "spill": {"raw": 1}}),
            json!({"synthetic": true, "content": "plan B"}),
        ] {
            assert!(is_reserved_envelope(&reserved), "{reserved}");
        }
        for ordinary in [
            json!("plain text"),
            json!({"content": "x"}),
            json!({"content": "x", "dream": 1, "extra": 2}),
            json!({"dream": 1, "spill": 2}),
            json!({"synthetic": true}),
            json!({"content": "x", "unrelated": {"dream": 1}}),
        ] {
            assert!(!is_reserved_envelope(&ordinary), "{ordinary}");
        }
    }

    /// CONFIRMATION-MODEL §5: the reserved control envelope (any action)
    /// is refused by the public wire — a client must not mint an
    /// interrupt or an approval — while a non-control `kian` object and
    /// a control envelope smuggled inside `content` stay ordinary.
    #[test]
    fn reserved_control_envelope_detection_is_exact() {
        for reserved in [
            json!({"kian": {"type": "control", "action": "interrupt"}}),
            json!({"kian": {"type": "control", "action": "confirm",
                            "call_id": "c", "decision": "approve"}}),
            json!({"kian": {"type": "control", "action": "future_action"}}),
        ] {
            assert!(is_reserved_envelope(&reserved), "{reserved}");
        }
        for ordinary in [
            json!({"kian": {"type": "async_result", "job_id": "j"}}),
            json!({"kian": {"type": "control", "action": "confirm"}, "extra": 1}),
            json!({"content": {"kian": {"type": "control", "action": "confirm"}}}),
            json!({"kian": "not an object"}),
        ] {
            assert!(!is_reserved_envelope(&ordinary), "{ordinary}");
        }
    }

    /// kian-99: the exact `{content, memory}` injection shape joins the
    /// reserved set — refused; a superset (`memory` + extra keys) and
    /// near-misses stay ordinary client content.
    #[test]
    fn reserved_memory_envelope_detection_is_exact() {
        assert!(is_reserved_envelope(&json!({
            "content": "rendered index",
            "memory": {"scope": "a", "notes": [], "bytes": 1, "items": 1,
                        "truncated": false, "source_seq": 1}
        })));
        for ordinary in [
            json!({"content": "x", "memory": {"scope": "a"}, "extra": true}),
            json!({"memory": {"scope": "a"}, "unrelated": 1}),
            json!({"content": "x", "unrelated": {"memory": 1}}),
        ] {
            assert!(!is_reserved_envelope(&ordinary), "{ordinary}");
        }
    }

    /// kian-99: a public `submit` whose `content` is the exact
    /// `{content, memory}` envelope is refused at the wire (`-32602`)
    /// — only the server's own creation path admits that shape.
    #[test]
    fn input_params_refuses_the_memory_envelope() {
        let params = json!({
            "session": "00000000-0000-0000-0000-000000000001",
            "content": {
                "content": "rendered index",
                "memory": {"scope": "ghost", "notes": [], "bytes": 1, "items": 1,
                            "truncated": false, "source_seq": 1}
            },
        });
        match input_params(&params) {
            Err((code, msg)) => {
                assert_eq!(code, CODE_INVALID_PARAMS);
                assert!(msg.contains("reserved"), "{msg}");
            }
            Ok(ok) => panic!("the memory envelope must be refused, got {ok:?}"),
        }
    }

    /// The wire-level guard (kian-94, review B-2′): `input_params` maps
    /// a reserved envelope to `-32602` and passes ordinary structured
    /// content through untouched.
    #[test]
    fn input_params_refuses_reserved_envelopes() {
        let params = json!({
            "session": "00000000-0000-0000-0000-000000000001",
            "content": {"content": "x", "dream": {"source_session": "00000000-0000-0000-0000-000000000002"}},
        });
        match input_params(&params) {
            Err((code, msg)) => {
                assert_eq!(code, CODE_INVALID_PARAMS);
                assert!(msg.contains("reserved"), "{msg}");
            }
            Ok(ok) => panic!("a reserved envelope must be refused, got {ok:?}"),
        }
        let ordinary = json!({
            "session": "00000000-0000-0000-0000-000000000001",
            "content": {"content": "x", "unrelated": true},
            "idempotency_key": "k",
        });
        let (session, content, key) = input_params(&ordinary).expect("ordinary object content");
        assert_eq!(
            session.0,
            "00000000-0000-0000-0000-000000000001"
                .parse::<uuid::Uuid>()
                .unwrap()
        );
        assert_eq!(content, json!({"content": "x", "unrelated": true}));
        assert_eq!(key.as_deref(), Some("k"));
    }

    /// READ-MODEL §7 (P2b): the archived session read error is the
    /// typed `-32007` refusal with package guidance in the message —
    /// never `unknown_session`, never a silent truncation.
    #[test]
    fn archived_error_maps_to_the_archived_code() {
        use super::{ControlError, control_err};
        use kian_core::model::SessionId;
        let session = SessionId(
            "00000000-0000-0000-0000-000000000009"
                .parse::<uuid::Uuid>()
                .unwrap(),
        );
        let (code, msg) = control_err(ControlError::Archived {
            session,
            archived_at: 42,
        });
        assert_eq!(code, -32007);
        assert!(msg.contains("archived"), "{msg}");
        assert!(msg.contains("session import"), "{msg}");
    }

    // READ-MODEL §4.2 (P3): the fan-in query parser.

    fn uuid_str(n: u8) -> String {
        format!("00000000-0000-0000-0000-{n:012x}")
    }

    /// The happy path parses an explicit allowlist (order preserved,
    /// duplicates collapsed) and an optional kind allowlist (known
    /// kinds only).
    #[test]
    fn fan_in_query_parses_allowlists() {
        use super::{MAX_FAN_IN_SESSIONS, fan_in_query};
        let a = uuid_str(1);
        let b = uuid_str(2);
        let query = fan_in_query(&[
            ("sessions".into(), format!("{a},{b},{a}")),
            ("kinds".into(), "input/admitted,turn/end".into()),
        ])
        .expect("valid query");
        assert_eq!(query.sessions.len(), 2, "duplicates collapse");
        assert_eq!(query.sessions[0].0.to_string(), a);
        assert_eq!(query.sessions[1].0.to_string(), b);
        assert_eq!(
            query.kinds.as_deref(),
            Some(&["input/admitted", "turn/end"][..])
        );
        // No kinds filter when the parameter is absent.
        let query = fan_in_query(&[("sessions".into(), a.clone())]).expect("valid query");
        assert!(query.kinds.is_none());
        // Unknown query keys are ignored.
        let query = fan_in_query(&[("sessions".into(), a), ("future".into(), "x".into())])
            .expect("unknown keys ignored");
        assert_eq!(query.sessions.len(), 1);
        // Exactly at the cap parses; one over is refused.
        let at_cap: Vec<(String, String)> = (0..MAX_FAN_IN_SESSIONS)
            .map(|n| ("sessions".into(), uuid_str(n as u8)))
            .collect();
        let joined = at_cap
            .iter()
            .map(|(_, v)| v.clone())
            .collect::<Vec<_>>()
            .join(",");
        fan_in_query(&[("sessions".into(), joined.clone())]).expect("cap is inclusive");
        let over = format!("{joined},{}", uuid_str(63)); // 65th listed entry
        let err = fan_in_query(&[("sessions".into(), over)]).unwrap_err();
        assert!(err.contains("capped at 64"), "{err}");
    }

    /// Malformed requests are refused: missing/repeated `sessions`,
    /// empty entries, non-UUID ids, the wildcard, an over-cap listing,
    /// repeated `kinds`, empty kind tokens, unknown kinds.
    #[test]
    fn fan_in_query_refuses_malformed_requests() {
        use super::fan_in_query;
        let a = uuid_str(1);
        for (pairs, why) in [
            (vec![], "sessions is required"),
            (
                vec![("sessions".into(), String::new())],
                "empty sessions value",
            ),
            (vec![("sessions".into(), format!("{a},"))], "empty entry"),
            (
                vec![("sessions".into(), format!("{a},not-a-uuid"))],
                "non-uuid entry",
            ),
            (vec![("sessions".into(), "*".into())], "no wildcard"),
            (
                vec![
                    ("sessions".into(), a.clone()),
                    ("sessions".into(), a.clone()),
                ],
                "repeated sessions parameter",
            ),
            (
                vec![("sessions".into(), a), ("kinds".into(), String::new())],
                "empty kinds value",
            ),
            (
                vec![
                    ("sessions".into(), uuid_str(1)),
                    ("kinds".into(), "input/admitted".into()),
                    ("kinds".into(), "turn/end".into()),
                ],
                "repeated kinds parameter",
            ),
            (
                vec![
                    ("sessions".into(), uuid_str(1)),
                    ("kinds".into(), "no/such-kind".into()),
                ],
                "unknown kind",
            ),
        ] {
            let err = fan_in_query(&pairs).map(|_| ()).unwrap_err();
            assert!(!err.is_empty(), "{why}: empty refusal");
            match why {
                "sessions is required" => assert!(err.contains("`sessions` is required")),
                "no wildcard" => assert!(err.contains("no wildcard"), "{err}"),
                "unknown kind" => assert!(err.contains("no/such-kind"), "{err}"),
                _ => {}
            }
        }
    }

    /// The `kinds` validation table is exactly the EVENT-MODEL §3
    /// catalog: every `EventKind` variant's durable name is present,
    /// and the table has no extra entries.
    #[test]
    fn known_event_kinds_table_matches_the_core_catalog() {
        use kian_core::events::{EndReason, EventKind};
        let variants = [
            EventKind::SessionCreated,
            EventKind::InputAdmitted,
            EventKind::DrainClaimed,
            EventKind::TurnStart,
            EventKind::TurnEnd,
            EventKind::StepRequest,
            EventKind::AssistantMessage,
            EventKind::ToolCall,
            EventKind::ToolResult,
            EventKind::Compact,
            EventKind::SlotBinding,
            EventKind::SlotFallback,
            EventKind::SlotMigration,
            EventKind::RecoveryBudgetReset,
            EventKind::ThreadStarted,
            EventKind::ThreadCompleted,
            EventKind::EvidenceVerdict,
            EventKind::ChannelDelivered,
            EventKind::InputSpilled,
            EventKind::ControlInterrupt,
            EventKind::MemoryNote,
            EventKind::SessionEnded {
                reason: EndReason::Completed,
            },
        ];
        let table = super::KNOWN_EVENT_KINDS;
        assert_eq!(table.len(), variants.len(), "table/variant count drift");
        for variant in &variants {
            assert!(
                table.contains(&variant.as_str()),
                "kind `{}` missing from the fan-in kinds table",
                variant.as_str()
            );
        }
    }

    /// The fan-in metrics gauges count subscribers and watched sessions
    /// symmetrically: adds and drops cancel, and the guard's drop
    /// settles whatever remained.
    #[test]
    fn fan_in_metrics_settle_to_zero_after_drop() {
        use std::sync::Arc;

        use super::{EventStreamMetrics, FanInGuard};
        let metrics = Arc::new(EventStreamMetrics::new());
        {
            let guard = Arc::new(FanInGuard {
                metrics: Arc::clone(&metrics),
                watched: std::sync::atomic::AtomicU64::new(0),
            });
            metrics
                .subscribers
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            guard.session_added();
            guard.session_added();
            guard.session_dropped();
            let mid = metrics.snapshot();
            assert_eq!(mid.subscribers, 1);
            assert_eq!(mid.sessions_watched, 1);
        }
        let end = metrics.snapshot();
        assert_eq!(end.subscribers, 0);
        assert_eq!(end.sessions_watched, 0);
        assert_eq!(end.events_sent, 0);
    }

    // -- deferred admissions (ADR-060, P4) ------------------------------

    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::future::BoxFuture;
    use kian_core::events::Event;
    use kian_core::model::SessionId;
    use kian_core::state::SessionState;
    use serde_json::Value;

    use super::{
        AppState, BudgetWireOutcome, ConfirmWireOutcome, ControlError, ControlPlane,
        DeferredAdmission, EventTailRead, ProjectionRead, control_err, dispatch,
    };

    /// A control plane that records the deferred calls; the methods these
    /// tests never reach are unreachable on purpose.
    #[derive(Default)]
    struct RecordingPlane {
        deferred: Mutex<Vec<(crate::defer::DeferPolicy, Option<String>)>>,
        submits: AtomicUsize,
    }

    impl ControlPlane for RecordingPlane {
        fn create_session(&self) -> BoxFuture<'_, Result<SessionId, ControlError>> {
            Box::pin(async { unreachable!("create_session is not exercised") })
        }
        fn submit(
            &self,
            _session: SessionId,
            _content: Value,
            _idempotency_key: Option<String>,
        ) -> BoxFuture<'_, Result<bool, ControlError>> {
            self.submits.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(false) })
        }
        fn submit_deferred(
            &self,
            _session: SessionId,
            _content: Value,
            idempotency_key: Option<String>,
            policy: crate::defer::DeferPolicy,
        ) -> BoxFuture<'_, Result<DeferredAdmission, ControlError>> {
            if let Ok(mut seen) = self.deferred.lock() {
                seen.push((policy, idempotency_key));
            }
            Box::pin(async {
                Ok(DeferredAdmission {
                    deduplicated: false,
                    eligible_at_ms: 1_800_000_000_000,
                    deferred: true,
                })
            })
        }
        fn queue_deferred(
            &self,
            _session: SessionId,
            _content: Value,
            idempotency_key: Option<String>,
            policy: crate::defer::DeferPolicy,
        ) -> BoxFuture<'_, Result<DeferredAdmission, ControlError>> {
            if let Ok(mut seen) = self.deferred.lock() {
                seen.push((policy, idempotency_key));
            }
            Box::pin(async {
                Ok(DeferredAdmission {
                    deduplicated: false,
                    eligible_at_ms: 1_800_000_000_000,
                    deferred: true,
                })
            })
        }
        fn steer(
            &self,
            _session: SessionId,
            _content: Value,
            _idempotency_key: Option<String>,
        ) -> BoxFuture<'_, Result<bool, ControlError>> {
            Box::pin(async { unreachable!("steer is not exercised") })
        }
        fn queue(
            &self,
            _session: SessionId,
            _content: Value,
            _idempotency_key: Option<String>,
        ) -> BoxFuture<'_, Result<bool, ControlError>> {
            Box::pin(async { unreachable!("queue is not exercised") })
        }
        fn interrupt(&self, _session: SessionId) -> BoxFuture<'_, Result<String, ControlError>> {
            Box::pin(async { unreachable!("interrupt is not exercised") })
        }
        fn confirm(
            &self,
            _session: SessionId,
            _call_id: String,
            _decision: kian_core::control::ConfirmDecision,
            _reason: Option<String>,
            _requested_by: Option<String>,
        ) -> BoxFuture<'_, Result<ConfirmWireOutcome, ControlError>> {
            Box::pin(async { unreachable!("confirm is not exercised") })
        }
        fn raise_budget(
            &self,
            _session: SessionId,
            _tokens: Option<u64>,
            _wall_clock_ms: Option<u64>,
            _reason: Option<String>,
            _requested_by: Option<String>,
            _idempotency_key: Option<String>,
        ) -> BoxFuture<'_, Result<BudgetWireOutcome, ControlError>> {
            Box::pin(async { unreachable!("raise_budget is not exercised") })
        }
        fn pending(
            &self,
            _session: SessionId,
        ) -> BoxFuture<'_, Result<Vec<super::PendingView>, ControlError>> {
            Box::pin(async { unreachable!("pending is not exercised") })
        }
        fn projection(
            &self,
            _session: SessionId,
        ) -> BoxFuture<'_, Result<SessionState, ControlError>> {
            Box::pin(async { Ok(SessionState::Idle) })
        }
        fn session_events(
            &self,
            _session: SessionId,
        ) -> BoxFuture<'_, Result<Vec<Event>, ControlError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn projection_versioned(
            &self,
            _session: SessionId,
            _since: Option<u64>,
        ) -> BoxFuture<'_, Result<ProjectionRead, ControlError>> {
            Box::pin(async {
                Ok(ProjectionRead {
                    version: 0,
                    unchanged: true,
                    state: None,
                })
            })
        }
        fn session_events_after(
            &self,
            _session: SessionId,
            _after: u64,
        ) -> BoxFuture<'_, Result<EventTailRead, ControlError>> {
            Box::pin(async {
                Ok(EventTailRead {
                    version: 0,
                    events: Vec::new(),
                    terminal: false,
                })
            })
        }
        fn session_version(&self, _session: SessionId) -> BoxFuture<'_, Result<u64, ControlError>> {
            Box::pin(async { Ok(0) })
        }
    }

    fn recording_state() -> (AppState, Arc<RecordingPlane>) {
        let plane = Arc::new(RecordingPlane::default());
        let plane_dyn: Arc<dyn ControlPlane> = plane.clone();
        (
            AppState {
                plane: plane_dyn,
                sse_poll_ms: 1_000,
                event_stream: None,
            },
            plane,
        )
    }

    #[tokio::test]
    async fn submit_with_defer_off_peak_returns_the_planned_instant() {
        let (state, plane) = recording_state();
        let params = json!({
            "session": uuid_str(1),
            "content": "batch job",
            "defer": "off_peak",
            "idempotency_key": "k1",
        });
        let out = dispatch(&state, "submit", &params)
            .await
            .expect("deferred submit");
        assert_eq!(
            out,
            json!({"deduplicated": false, "deferred": true, "eligible_at_ms": 1_800_000_000_000u64})
        );
        let seen = plane.deferred.lock().expect("deferred calls");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, crate::defer::DeferPolicy::OffPeak);
        assert_eq!(seen[0].1.as_deref(), Some("k1"));
        assert_eq!(
            plane.submits.load(Ordering::SeqCst),
            0,
            "a deferred submit never takes the immediate path"
        );
    }

    #[tokio::test]
    async fn an_undeferred_submit_keeps_the_old_response_shape() {
        let (state, plane) = recording_state();
        let params = json!({"session": uuid_str(1), "content": "now please"});
        let out = dispatch(&state, "submit", &params).await.expect("submit");
        assert_eq!(out, json!({"deduplicated": false}));
        assert_eq!(plane.submits.load(Ordering::SeqCst), 1);
        assert!(plane.deferred.lock().expect("calls").is_empty());
    }

    #[tokio::test]
    async fn invalid_defer_values_are_client_errors() {
        let (state, _plane) = recording_state();
        let bad = json!({"session": uuid_str(1), "content": "x", "defer": "offpeak"});
        let (code, message) = dispatch(&state, "submit", &bad)
            .await
            .expect_err("an unknown policy is refused");
        assert_eq!(code, CODE_INVALID_PARAMS);
        assert!(message.contains("off_peak"), "{message}");

        let wrong_type = json!({"session": uuid_str(1), "content": "x", "defer": 5});
        let (code, message) = dispatch(&state, "queue", &wrong_type)
            .await
            .expect_err("a non-string defer is refused");
        assert_eq!(code, CODE_INVALID_PARAMS);
        assert!(message.contains("string"), "{message}");
    }

    #[tokio::test]
    async fn steering_never_defers() {
        let (state, _plane) = recording_state();
        let params = json!({"session": uuid_str(1), "content": "x", "defer": "off_peak"});
        let (code, message) = dispatch(&state, "steer", &params)
            .await
            .expect_err("steer refuses deferral");
        assert_eq!(code, CODE_INVALID_PARAMS);
        assert!(message.contains("never delayed"), "{message}");
    }

    #[test]
    fn a_missing_defer_section_maps_to_invalid_params() {
        let (code, message) = control_err(ControlError::DeferralUnavailable);
        assert_eq!(code, CODE_INVALID_PARAMS);
        assert!(message.contains("not configured"), "{message}");
    }
}
