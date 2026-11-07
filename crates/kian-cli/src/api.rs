//! Control-plane API (DESIGN §5 "Control-plane protocol").
//!
//! axum JSON-RPC-over-HTTP + SSE exposing the built-in set:
//! `create_session`, `submit`, `steer`, `interrupt`, `queue`, `events`
//! (SSE interval polling of the event store), `projection` read.
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
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use futures::future::BoxFuture;
use kian_core::events::Event;
use kian_core::model::SessionId;
use kian_core::state::SessionState;
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
    fn interrupt(&self, session: SessionId) -> BoxFuture<'_, Result<String, ControlError>>;
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
}

/// Builds the built-in control-plane router.
pub fn router(plane: Arc<dyn ControlPlane>, sse_poll_ms: u64) -> Router {
    let state = AppState { plane, sse_poll_ms };
    Router::new()
        .route("/rpc", post(rpc))
        .route("/sessions/{id}/events", get(session_events_sse))
        .with_state(state)
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
            let dedup = state
                .plane
                .submit(session, content, key)
                .await
                .map_err(control_err)?;
            Ok(json!({"deduplicated": dedup}))
        }
        "steer" => {
            let (session, content, key) = input_params(params)?;
            let dedup = state
                .plane
                .steer(session, content, key)
                .await
                .map_err(control_err)?;
            Ok(json!({"deduplicated": dedup}))
        }
        "queue" => {
            let (session, content, key) = input_params(params)?;
            let dedup = state
                .plane
                .queue(session, content, key)
                .await
                .map_err(control_err)?;
            Ok(json!({"deduplicated": dedup}))
        }
        "interrupt" => {
            let session = session_params(params)?;
            let outcome = state.plane.interrupt(session).await.map_err(control_err)?;
            Ok(json!({"outcome": outcome}))
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
                "unknown method `{other}` (baseline set: create_session, submit, steer, interrupt, queue, projection; events is the SSE endpoint GET /sessions/{{id}}/events)"
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

/// Reserved row-envelope key sets (DESIGN §3.1, MEMORY-MODEL §5 D1):
/// `{content, dream}` (the ops-plane dream admit), `{content, spill}`
/// (the Inline/Spill shape) and `{synthetic, content}` (the strippable
/// steer marker). Only the server's internal planes produce these
/// shapes — and the drain (`consumption_plan`) consumes each envelope
/// only at exactly two keys (kian-96, review N-1), so a superset that
/// passes this guard lands as ordinary nested content, never as a
/// client-minted marker. A public `submit`/`steer`/`queue` carrying an
/// exact reserved shape would let client content forge a marker the
/// server is documented to set itself (kian-94, review B-2′), so the
/// wire refuses them (`-32602`).
fn is_reserved_envelope(content: &Value) -> bool {
    let Some(obj) = content.as_object() else {
        return false;
    };
    obj.len() == 2
        && obj.contains_key("content")
        && ["dream", "spill", "synthetic"]
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
             / {synthetic,content}); only the server's internal planes admit those rows"
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
}
