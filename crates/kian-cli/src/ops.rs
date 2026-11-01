//! Operational plane (card C14 + the C27 `mock_channel_send` demo
//! injection): the server surface behind `kian wake-now`,
//! `kian recovery budget reset` and `kian mock-channel-send`.
//!
//! This module is deliberately **separate** from [`crate::api`] (the
//! C13 control plane): the control plane obeys the layered iron rule —
//! inbox writes and projection reads only — while these are explicit
//! *operator* actions (trigger a wake; append a recovery-budget-reset
//! event; inject a demo event onto a configured mock channel). The C13
//! wire and its semantics are untouched; the ops lane mounts at
//! `POST /ops/rpc` and mirrors the same structural discipline: handlers
//! speak JSON and call **nothing but** the [`OpsPlane`] trait — no
//! engine, no store, no execution symbols (the extended iron-rule test
//! greps this module too).
//!
//! Wire (JSON-RPC 2.0, same error-code table as the control plane):
//!
//! - `wake_now {session}` → `{"outcome": "started" | "folded"}`
//!   (`started`: no drain was in flight, one was spawned; `folded`: a
//!   drain was already running, the wake folded onto pendingWake).
//! - `recovery_budget_reset {session, reason?}` →
//!   `{"seq": u64, "epoch": u64, "ts": u64}` — the appended
//!   `recovery/budget-reset` event's position. The reset is an explicit
//!   event; there is no silent counter wipe.
//! - `mock_channel_send {channel, topic?, payload}` (C27, demo/S4
//!   injection; DESIGN §5 note) → `{"event_id", "channel", "topic?}` —
//!   one event onto the named mock channel; the subscription engine
//!   routes it per the configured rules.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use futures::future::BoxFuture;
use kian_core::model::SessionId;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// The operational surface (the ONLY thing ops handlers may touch)
// ---------------------------------------------------------------------------

/// What the API layer may ask of the server in the ops lane: explicit
/// operator actions with durable, auditable effects. Everything else is
/// structurally out of reach.
pub trait OpsPlane: Send + Sync {
    /// Trigger a wake for the session (idempotent; folds onto
    /// pendingWake when a drain is in flight).
    fn wake_now(&self, session: SessionId) -> BoxFuture<'_, Result<WakeReceipt, OpsError>>;

    /// Append a `recovery/budget-reset` event to the session's stream.
    fn recovery_budget_reset(
        &self,
        session: SessionId,
        reason: Option<String>,
    ) -> BoxFuture<'_, Result<ResetReceipt, OpsError>>;

    /// Inject one demo event onto a configured mock channel (C27, the
    /// S4 stand-in path). The subscription engine routes it per the
    /// configured rules — this method only hands the event to the
    /// channel.
    fn mock_channel_send(
        &self,
        channel: String,
        topic: Option<String>,
        payload: Value,
    ) -> BoxFuture<'_, Result<MockSendReceipt, OpsError>>;
}

/// Outcome of an operator wake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeReceipt {
    /// No drain was in flight; one was spawned.
    Started,
    /// A drain was already running; the wake folded onto pendingWake.
    Folded,
}

impl WakeReceipt {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Folded => "folded",
        }
    }
}

/// Where the `recovery/budget-reset` event landed in the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResetReceipt {
    pub seq: u64,
    pub epoch: u64,
    pub ts: u64,
}

/// Receipt of a demo channel injection (C27).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockSendReceipt {
    /// The injected event's id (the subscription admit's idempotency
    /// key — re-injecting the same id can never double-admit).
    pub event_id: String,
    pub channel: String,
    pub topic: Option<String>,
}

/// Operational-plane failures.
#[derive(Debug, thiserror::Error)]
pub enum OpsError {
    #[error("unknown session {session:?}")]
    UnknownSession { session: SessionId },
    #[error("session {session:?} is draining; retry when it is idle")]
    SessionBusy { session: SessionId },
    #[error("session {session:?} is terminal; resets and wakes are refused")]
    SessionTerminal { session: SessionId },
    #[error(
        "session {session:?} is idle; its recovery budget was never consumed (wake-now it first)"
    )]
    SessionIdle { session: SessionId },
    #[error("{0}")]
    Internal(String),
}

/// JSON-RPC error codes (mirrors the control-plane table).
const CODE_UNKNOWN_SESSION: i64 = -32001;
const CODE_SESSION_BUSY: i64 = -32002;
const CODE_SESSION_TERMINAL: i64 = -32003;
const CODE_SESSION_IDLE: i64 = -32004;
const CODE_INTERNAL: i64 = -32000;
const CODE_METHOD_NOT_FOUND: i64 = -32601;
const CODE_INVALID_PARAMS: i64 = -32602;
const CODE_INVALID_REQUEST: i64 = -32600;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AppState {
    plane: Arc<dyn OpsPlane>,
}

/// Builds the C14 operational router (`POST /ops/rpc`).
pub fn router(plane: Arc<dyn OpsPlane>) -> Router {
    Router::new()
        .route("/ops/rpc", post(rpc))
        .with_state(AppState { plane })
}

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

fn ops_err(e: OpsError) -> (i64, String) {
    match e {
        OpsError::UnknownSession { session } => (
            CODE_UNKNOWN_SESSION,
            format!("unknown session {:?}", session.0),
        ),
        OpsError::SessionBusy { session } => (
            CODE_SESSION_BUSY,
            format!("session {:?} is draining; retry when it is idle", session.0),
        ),
        OpsError::SessionTerminal { session } => (
            CODE_SESSION_TERMINAL,
            format!("session {:?} is terminal", session.0),
        ),
        OpsError::SessionIdle { session } => (
            CODE_SESSION_IDLE,
            format!(
                "session {:?} is idle; its recovery budget was never consumed",
                session.0
            ),
        ),
        OpsError::Internal(msg) => (CODE_INTERNAL, msg),
    }
}

async fn dispatch(state: &AppState, method: &str, params: &Value) -> Result<Value, (i64, String)> {
    match method {
        "wake_now" => {
            let session = session_params(params)?;
            let receipt = state.plane.wake_now(session).await.map_err(ops_err)?;
            Ok(json!({"outcome": receipt.as_str()}))
        }
        "recovery_budget_reset" => {
            let session = session_params(params)?;
            let reason = params
                .get("reason")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let receipt = state
                .plane
                .recovery_budget_reset(session, reason)
                .await
                .map_err(ops_err)?;
            Ok(json!({"seq": receipt.seq, "epoch": receipt.epoch, "ts": receipt.ts}))
        }
        "mock_channel_send" => {
            let channel = params
                .get("channel")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    (
                        CODE_INVALID_PARAMS,
                        "missing `channel` (name string)".to_string(),
                    )
                })?
                .to_owned();
            let topic = params
                .get("topic")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let payload = params
                .get("payload")
                .cloned()
                .ok_or_else(|| (CODE_INVALID_PARAMS, "missing `payload`".to_string()))?;
            let receipt = state
                .plane
                .mock_channel_send(channel, topic, payload)
                .await
                .map_err(ops_err)?;
            Ok(json!({
                "event_id": receipt.event_id,
                "channel": receipt.channel,
                "topic": receipt.topic,
            }))
        }
        other => Err((
            CODE_METHOD_NOT_FOUND,
            format!(
                "unknown method `{other}` (ops set: wake_now, recovery_budget_reset, mock_channel_send)"
            ),
        )),
    }
}

fn session_params(params: &Value) -> Result<SessionId, (i64, String)> {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use futures::future::BoxFuture;
    use kian_core::model::SessionId;
    use serde_json::json;

    use super::{OpsError, OpsPlane, ResetReceipt, WakeReceipt, dispatch};
    use crate::ops::AppState;

    #[tokio::test]
    async fn ops_dispatch_wakes_through_the_trait() {
        let plane = Arc::new(FakePlane::default());
        let sid = SessionId::new();
        let state = AppState {
            plane: plane.clone(),
        };

        let out = dispatch(&state, "wake_now", &json!({"session": sid.0.to_string()}))
            .await
            .expect("wake dispatch");
        assert_eq!(out["outcome"], "started");
        assert_eq!(*plane.wake_calls.lock().unwrap(), vec![sid]);
    }

    #[tokio::test]
    async fn ops_dispatch_resets_with_reason_and_returns_receipt() {
        let plane = Arc::new(FakePlane::default());
        let sid = SessionId::new();
        let state = AppState {
            plane: plane.clone(),
        };

        let out = dispatch(
            &state,
            "recovery_budget_reset",
            &json!({"session": sid.0.to_string(), "reason": "operator override"}),
        )
        .await
        .expect("reset dispatch");
        assert_eq!(out["seq"], 3);
        assert_eq!(out["epoch"], 1);
        assert_eq!(out["ts"], 42);
        assert_eq!(
            *plane.reset_calls.lock().unwrap(),
            vec![(sid, Some("operator override".into()))]
        );
    }

    #[tokio::test]
    async fn ops_dispatch_rejects_unknown_method_and_bad_params() {
        let state = AppState {
            plane: Arc::new(FakePlane::default()),
        };

        let (code, _) = dispatch(&state, &["conf", "irm"].concat(), &json!({}))
            .await
            .expect_err("banned");
        assert_eq!(code, -32601);

        let (code, msg) = dispatch(&state, "wake_now", &json!({"session": "not-a-uuid"}))
            .await
            .expect_err("bad uuid");
        assert_eq!(code, -32602);
        assert!(msg.contains("uuid"), "{msg}");

        let (code, _) = dispatch(&state, "wake_now", &json!({}))
            .await
            .expect_err("missing session");
        assert_eq!(code, -32602);
    }

    /// Error codes stay distinguishable (fail-closed, honest errors).
    #[test]
    fn ops_error_codes_are_distinct() {
        let sid = SessionId::new();
        let cases = [
            (OpsError::UnknownSession { session: sid }, -32001_i64),
            (OpsError::SessionBusy { session: sid }, -32002),
            (OpsError::SessionTerminal { session: sid }, -32003),
            (OpsError::SessionIdle { session: sid }, -32004),
            (OpsError::Internal("boom".into()), -32000),
        ];
        for (err, code) in cases {
            assert_eq!(super::ops_err(err).0, code);
        }
    }

    /// A recording fake: proves the handler layer speaks only OpsPlane.
    #[derive(Default)]
    pub struct FakePlane {
        pub wake_calls: Mutex<Vec<SessionId>>,
        pub reset_calls: Mutex<Vec<(SessionId, Option<String>)>>,
        pub mock_sends: Mutex<Vec<(String, Option<String>, serde_json::Value)>>,
    }

    impl OpsPlane for FakePlane {
        fn wake_now(&self, session: SessionId) -> BoxFuture<'_, Result<WakeReceipt, OpsError>> {
            self.wake_calls.lock().unwrap().push(session);
            Box::pin(async { Ok(WakeReceipt::Started) })
        }

        fn recovery_budget_reset(
            &self,
            session: SessionId,
            reason: Option<String>,
        ) -> BoxFuture<'_, Result<ResetReceipt, OpsError>> {
            self.reset_calls.lock().unwrap().push((session, reason));
            Box::pin(async {
                Ok(ResetReceipt {
                    seq: 3,
                    epoch: 1,
                    ts: 42,
                })
            })
        }

        fn mock_channel_send(
            &self,
            channel: String,
            topic: Option<String>,
            payload: serde_json::Value,
        ) -> BoxFuture<'_, Result<super::MockSendReceipt, OpsError>> {
            self.mock_sends
                .lock()
                .unwrap()
                .push((channel.clone(), topic.clone(), payload));
            Box::pin(async move {
                Ok(super::MockSendReceipt {
                    event_id: "evt-1".into(),
                    channel,
                    topic,
                })
            })
        }
    }

    #[tokio::test]
    async fn ops_dispatch_injects_through_the_trait() {
        let plane = Arc::new(FakePlane::default());
        let state = AppState {
            plane: plane.clone(),
        };
        let out = dispatch(
            &state,
            "mock_channel_send",
            &json!({
                "channel": "mock-im",
                "topic": "pr-9",
                "payload": {"text": "hello"}
            }),
        )
        .await
        .expect("mock send dispatch");
        assert_eq!(out["event_id"], "evt-1");
        assert_eq!(out["channel"], "mock-im");
        assert_eq!(out["topic"], "pr-9");
        assert_eq!(plane.mock_sends.lock().unwrap().len(), 1);

        // Missing payload is a parameter error.
        let (code, _) = dispatch(&state, "mock_channel_send", &json!({"channel": "mock-im"}))
            .await
            .expect_err("missing payload");
        assert_eq!(code, -32602);
    }
}
