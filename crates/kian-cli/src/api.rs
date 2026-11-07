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
}

/// Control-plane failures.
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("unknown session {session:?}")]
    UnknownSession { session: SessionId },
    #[error("{0}")]
    Internal(String),
}

/// JSON-RPC error code for an unknown session.
const CODE_UNKNOWN_SESSION: i64 = -32001;
/// JSON-RPC error code for server-side failures.
const CODE_INTERNAL: i64 = -32000;
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
            let state = state.plane.projection(session).await.map_err(control_err)?;
            Ok(json!({"state": state}))
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

fn input_params(params: &Value) -> Result<(SessionId, Value, Option<String>), (i64, String)> {
    let session = session_of(params)?;
    let content = params
        .get("content")
        .cloned()
        .ok_or_else(|| (CODE_INVALID_PARAMS, "missing `content`".to_string()))?;
    let idempotency_key = params
        .get("idempotency_key")
        .and_then(Value::as_str)
        .map(str::to_owned);
    Ok((session, content, idempotency_key))
}

fn session_params(params: &Value) -> Result<SessionId, (i64, String)> {
    session_of(params)
}

// ---------------------------------------------------------------------------
// SSE `events` (interval polling of the event stream)
// ---------------------------------------------------------------------------

async fn session_events_sse(State(state): State<AppState>, Path(id): Path<String>) -> Response {
    let Ok(uuid) = id.parse::<uuid::Uuid>() else {
        return (StatusCode::BAD_REQUEST, "session id must be a uuid").into_response();
    };
    let session = SessionId(uuid);
    let plane = Arc::clone(&state.plane);
    let poll = Duration::from_millis(state.sse_poll_ms);

    // Poll the event store every `poll` and emit events the client has
    // not seen yet (seq strictly increases). The stream ends after the
    // session/ended event — a terminal projection freezes the stream, so
    // there is nothing more to observe.
    let stream = futures::stream::unfold(
        (plane, session, 0u64, false),
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
                    match plane.session_events(session).await {
                        Err(e) => {
                            let (_, msg) = control_err(e);
                            items.push(Ok(SseEvent::default().event("error").data(msg)));
                            done_next = true;
                            break;
                        }
                        Ok(events) => {
                            let fresh: Vec<&Event> =
                                events.iter().filter(|e| e.seq > last_seq).collect();
                            if fresh.is_empty() {
                                // Nothing new: wait one interval, poll again.
                                tokio::time::sleep(poll).await;
                                continue;
                            }
                            for e in fresh {
                                last_seq = e.seq;
                                items.push(Ok(SseEvent::default()
                                    .event("kian")
                                    .data(serde_json::to_string(e).unwrap_or_default())));
                            }
                            let terminal = events.last().is_some_and(|e| {
                                matches!(e.kind, kian_core::events::EventKind::SessionEnded { .. })
                            });
                            if terminal {
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
