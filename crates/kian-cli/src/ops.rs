//! Operational plane (+ the `mock_channel_send` demo
//! injection): the server surface behind `kian wake-now`,
//! `kian recovery budget reset`, `kian mock-channel-send`,
//! `kian memory dream` and `kian workers release`.
//!
//! This module is deliberately **separate** from [`crate::api`] (the
//! control plane): the control plane obeys the layered iron rule —
//! inbox writes and projection reads only — while these are explicit
//! *operator* actions (trigger a wake; append a recovery-budget-reset
//! event; inject a demo event onto a configured mock channel; admit
//! one dream request onto the memory session). The wire and its
//! semantics are untouched; the ops lane mounts at `POST /ops/rpc`
//! and mirrors the same structural discipline: handlers speak JSON
//! and call **nothing but** the [`OpsPlane`] trait — no engine, no
//! store, no execution symbols (the extended iron-rule test greps
//! this module too).
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
//! - `mock_channel_send {channel, topic?, payload}` (demo/
//!   injection; DESIGN §5 note) → `{"event_id", "channel", "topic?}` —
//!   one event onto the named mock channel; the subscription engine
//!   routes it per the configured rules.
//! - `memory_dream {session, source, from_seq, to_seq}` → the dream
//!   request of round-8 plan §2.3: the server reads the source range
//!   (refusing honestly over `max_bytes_read`), renders the prompt
//!   through `kian-memory` and admits ONE durable inbox row (role
//!   user, the rendered prompt) onto the memory session under the
//!   idempotency key `dream:<source>:<from>:<to>` — repeating the
//!   same range is a no-op. Memory must be configured (`memory:`
//!   section); otherwise `-32005`.
//! - `release_worker {worker, reason?}` → `{"worker", "outcome":
//!   "released" | "already_released", "session", "thread_id", "seq"}` —
//!   closes the live bound thread of a worker whose task already ended
//!   `interrupted`/`failed`, freeing the 1:1 slot (ADR-035). Refused
//!   `-32002` while the thread is running, holds eligible input or owes
//!   a live command; `-32008` for an unknown/unbound id.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use futures::future::BoxFuture;
use kian_core::model::SessionId;
use serde_json::{Value, json};

use crate::worker_release::ReleaseWorkerReceipt;

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

    /// Inject one demo event onto a configured mock channel (the
    /// stand-in path). The subscription engine routes it per the
    /// configured rules — this method only hands the event to the
    /// channel.
    fn mock_channel_send(
        &self,
        channel: String,
        topic: Option<String>,
        payload: Value,
    ) -> BoxFuture<'_, Result<MockSendReceipt, OpsError>>;

    /// Admits one dream request onto the memory session (round-8 plan
    /// §2.3): reads the source range `[from_seq, to_seq)`, renders the
    /// deterministic dream prompt and admits it as ONE durable inbox
    /// row (role user) under the idempotency key
    /// `dream:<source>:<from>:<to>` — a repeated range is a no-op
    /// (the existing inbox dedupe). The admit wakes the session.
    fn memory_dream(
        &self,
        session: SessionId,
        source: SessionId,
        from_seq: u64,
        to_seq: u64,
    ) -> BoxFuture<'_, Result<DreamAdmitReceipt, OpsError>>;

    /// Closes the live thread bound to one worker id and frees its 1:1
    /// slot (ADR-035). Refused while the thread is running, not ended,
    /// holding eligible input, or owing a live command row.
    fn release_worker(
        &self,
        worker: String,
        reason: Option<String>,
    ) -> BoxFuture<'_, Result<ReleaseWorkerReceipt, OpsError>>;
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

/// Receipt of a demo channel injection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MockSendReceipt {
    /// The injected event's id (the subscription admit's idempotency
    /// key — re-injecting the same id can never double-admit).
    pub event_id: String,
    pub channel: String,
    pub topic: Option<String>,
}

/// Receipt of one admitted dream request (the `memory_dream` result).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DreamAdmitReceipt {
    /// The memory session the dream row landed on.
    pub session: SessionId,
    /// The source session whose range is being consolidated.
    pub source: SessionId,
    /// The half-open range `[from_seq, to_seq)`.
    pub from_seq: u64,
    pub to_seq: u64,
    /// True when the idempotency key already existed — a repeated
    /// range admitted nothing (the honest no-op).
    pub deduplicated: bool,
    /// Bytes of source transcript actually read (under the cap).
    pub bytes_read: u64,
    /// Bytes of the whole rendered dream prompt.
    pub prompt_bytes: u64,
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
    #[error(
        "memory is not configured (no `memory:` section in kian.yaml) — memory_dream is refused"
    )]
    MemoryNotConfigured,
    #[error("unknown source session {source_session:?}")]
    UnknownSource { source_session: SessionId },
    #[error(
        "source range [{from_seq},{to_seq}) of {source_session:?} carries no events — nothing to dream over"
    )]
    EmptySourceRange {
        source_session: SessionId,
        from_seq: u64,
        to_seq: u64,
    },
    #[error(
        "source range [{from_seq},{to_seq}) of {source_session:?} renders {bytes} transcript bytes over the configured max_bytes_read {cap} — refused, not truncated"
    )]
    SourceRangeTooLarge {
        source_session: SessionId,
        from_seq: u64,
        to_seq: u64,
        bytes: u64,
        cap: u64,
    },
    /// A dream source range outside the source stream (kian-91, review
    /// B-1): `memory_dream` refuses unless the range satisfies
    /// `1 <= from_seq < to_seq <=` the source head — the source's last
    /// event seq plus one, read from the rebuilt stream. Refused at
    /// admission; never a silent truncation.
    #[error(
        "source range [{from_seq},{to_seq}) of {source_session:?} is out of bounds: must satisfy 1 <= from_seq < to_seq <= {head_next_seq} (the source head)"
    )]
    SourceRangeOutOfBounds {
        source_session: SessionId,
        from_seq: u64,
        to_seq: u64,
        head_next_seq: u64,
    },
    /// `release_worker` for an unknown worker id, no live binding or a
    /// never-started reservation (ADR-035, -32008).
    #[error("worker `{worker}` is not bound: {detail}")]
    WorkerNotBound { worker: String, detail: String },
    /// `release_worker` on a bound thread that cannot be closed right now
    /// (running, still holding eligible input, or owing a live command).
    #[error("worker `{worker}` (session {session:?}) cannot be released: {detail}")]
    WorkerBusy {
        worker: String,
        session: SessionId,
        detail: String,
    },
    #[error("{0}")]
    Internal(String),
}

/// JSON-RPC error codes (mirrors the control-plane table).
const CODE_UNKNOWN_SESSION: i64 = -32001;
const CODE_SESSION_BUSY: i64 = -32002;
const CODE_SESSION_TERMINAL: i64 = -32003;
const CODE_SESSION_IDLE: i64 = -32004;
/// Memory is not configured (`memory_dream` on a memory-off server).
const CODE_MEMORY_NOT_CONFIGURED: i64 = -32005;
/// A dream source range over the configured `max_bytes_read` cap
/// (refused honestly, never truncated).
const CODE_DREAM_RANGE_TOO_LARGE: i64 = -32006;
/// `release_worker` for an unknown/unbound worker id (ADR-035).
const CODE_WORKER_NOT_BOUND: i64 = -32008;
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

/// Builds the operational router (`POST /ops/rpc`).
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
        OpsError::MemoryNotConfigured => (
            CODE_MEMORY_NOT_CONFIGURED,
            "memory is not configured (no `memory:` section in kian.yaml)".into(),
        ),
        OpsError::UnknownSource { source_session } => (
            CODE_UNKNOWN_SESSION,
            format!("unknown source session {:?}", source_session.0),
        ),
        OpsError::EmptySourceRange {
            source_session,
            from_seq,
            to_seq,
        } => (
            CODE_INVALID_PARAMS,
            format!(
                "source range [{from_seq},{to_seq}) of {:?} carries no events",
                source_session.0
            ),
        ),
        OpsError::SourceRangeTooLarge {
            source_session,
            from_seq,
            to_seq,
            bytes,
            cap,
        } => (
            CODE_DREAM_RANGE_TOO_LARGE,
            format!(
                "source range [{from_seq},{to_seq}) of {:?} renders {bytes} transcript bytes \
                 over the configured max_bytes_read {cap} — refused, not truncated",
                source_session.0
            ),
        ),
        OpsError::SourceRangeOutOfBounds {
            source_session,
            from_seq,
            to_seq,
            head_next_seq,
        } => (
            CODE_INVALID_PARAMS,
            format!(
                "source range [{from_seq},{to_seq}) of {:?} is out of bounds: must satisfy \
                 1 <= from_seq < to_seq <= {head_next_seq} (the source head) — refused, not truncated",
                source_session.0
            ),
        ),
        OpsError::WorkerNotBound { worker, detail } => (
            CODE_WORKER_NOT_BOUND,
            format!("worker `{worker}` is not bound: {detail}"),
        ),
        OpsError::WorkerBusy {
            worker,
            session,
            detail,
        } => (
            CODE_SESSION_BUSY,
            format!(
                "worker `{worker}` (session {:?}) cannot be released: {detail}",
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
        "memory_dream" => {
            let session = session_params(params)?;
            let source = source_params(params)?;
            let (from_seq, to_seq) = range_params(params)?;
            let receipt = state
                .plane
                .memory_dream(session, source, from_seq, to_seq)
                .await
                .map_err(ops_err)?;
            Ok(json!({
                "session": receipt.session.0,
                "source": receipt.source.0,
                "from_seq": receipt.from_seq,
                "to_seq": receipt.to_seq,
                "deduplicated": receipt.deduplicated,
                "bytes_read": receipt.bytes_read,
                "prompt_bytes": receipt.prompt_bytes,
            }))
        }
        "release_worker" => {
            let worker = params
                .get("worker")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|w| !w.is_empty())
                .ok_or_else(|| {
                    (
                        CODE_INVALID_PARAMS,
                        "missing `worker` (non-empty worker id string)".to_string(),
                    )
                })?
                .to_owned();
            let reason = params
                .get("reason")
                .and_then(Value::as_str)
                .map(str::to_owned);
            let receipt = state
                .plane
                .release_worker(worker, reason)
                .await
                .map_err(ops_err)?;
            Ok(json!({
                "worker": receipt.worker,
                "outcome": receipt.outcome.as_str(),
                "session": receipt.session.0,
                "thread_id": receipt.thread_id.to_string(),
                "seq": receipt.seq,
            }))
        }
        other => Err((
            CODE_METHOD_NOT_FOUND,
            format!(
                "unknown method `{other}` (ops set: wake_now, recovery_budget_reset, mock_channel_send, memory_dream, release_worker)"
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

/// The `source` param of `memory_dream` (a uuid string).
fn source_params(params: &Value) -> Result<SessionId, (i64, String)> {
    let s = params
        .get("source")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            (
                CODE_INVALID_PARAMS,
                "missing `source` (uuid string)".to_string(),
            )
        })?;
    let uuid: uuid::Uuid = s
        .parse()
        .map_err(|_| (CODE_INVALID_PARAMS, format!("`source` `{s}` is not a uuid")))?;
    Ok(SessionId(uuid))
}

/// The `from_seq`/`to_seq` params of `memory_dream`: two u64s naming
/// the half-open range `[from_seq, to_seq)` — an inverted or empty
/// range is a parameter error (dreams are never over nothing).
fn range_params(params: &Value) -> Result<(u64, u64), (i64, String)> {
    let seq = |key: &str| -> Result<u64, (i64, String)> {
        params.get(key).and_then(Value::as_u64).ok_or_else(|| {
            (
                CODE_INVALID_PARAMS,
                format!("missing `{key}` (u64 seq number)"),
            )
        })
    };
    let from = seq("from_seq")?;
    let to = seq("to_seq")?;
    if from >= to {
        return Err((
            CODE_INVALID_PARAMS,
            format!("`from_seq` {from} must be < `to_seq` {to} (half-open range)"),
        ));
    }
    Ok((from, to))
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
    use crate::worker_release::{ReleaseWorkerOutcome, ReleaseWorkerReceipt};

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

    /// `memory_dream` dispatches through the trait and reports the
    /// receipt; malformed ranges are parameter errors.
    #[tokio::test]
    async fn ops_dispatch_dreams_through_the_trait() {
        let plane = Arc::new(FakePlane::default());
        let sid = SessionId::new();
        let source = SessionId::new();
        let state = AppState {
            plane: plane.clone(),
        };

        let out = dispatch(
            &state,
            "memory_dream",
            &json!({
                "session": sid.0.to_string(),
                "source": source.0.to_string(),
                "from_seq": 1,
                "to_seq": 4,
            }),
        )
        .await
        .expect("dream dispatch");
        assert_eq!(out["deduplicated"], false);
        assert_eq!(out["bytes_read"], 42);
        assert_eq!(
            *plane.dream_calls.lock().unwrap(),
            vec![(sid, source, 1, 4)]
        );

        // An inverted range never reaches the plane.
        let (code, _) = dispatch(
            &state,
            "memory_dream",
            &json!({
                "session": sid.0.to_string(),
                "source": source.0.to_string(),
                "from_seq": 4,
                "to_seq": 4,
            }),
        )
        .await
        .expect_err("inverted range");
        assert_eq!(code, -32602);

        // Missing source is a parameter error too.
        let (code, _) = dispatch(
            &state,
            "memory_dream",
            &json!({"session": sid.0.to_string(), "from_seq": 1, "to_seq": 4}),
        )
        .await
        .expect_err("missing source");
        assert_eq!(code, -32602);
    }

    /// `release_worker` dispatches through the trait and reports the
    /// receipt; a malformed/empty worker id never reaches the plane.
    #[tokio::test]
    async fn ops_dispatch_releases_a_worker_through_the_trait() {
        let plane = Arc::new(FakePlane::default());
        let state = AppState {
            plane: plane.clone(),
        };

        let out = dispatch(
            &state,
            "release_worker",
            &json!({"worker": "mac-mini", "reason": "stuck"}),
        )
        .await
        .expect("release dispatch");
        assert_eq!(out["worker"], "mac-mini");
        assert_eq!(out["outcome"], "released");
        assert_eq!(
            *plane.release_calls.lock().unwrap(),
            vec![("mac-mini".to_string(), Some("stuck".to_string()))]
        );

        let (code, _) = dispatch(&state, "release_worker", &json!({"worker": " "}))
            .await
            .expect_err("empty worker id");
        assert_eq!(code, -32602);
        let (code, _) = dispatch(&state, "release_worker", &json!({}))
            .await
            .expect_err("missing worker id");
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
            (
                OpsError::WorkerNotBound {
                    worker: "w".into(),
                    detail: "no binding".into(),
                },
                -32008,
            ),
            (
                OpsError::WorkerBusy {
                    worker: "w".into(),
                    session: sid,
                    detail: "running".into(),
                },
                -32002,
            ),
            (OpsError::Internal("boom".into()), -32000),
        ];
        for (err, code) in cases {
            assert_eq!(super::ops_err(err).0, code);
        }
    }

    /// The memory error codes (round 8): the two new closed refusals
    /// plus the reused codes the dream path maps onto.
    #[test]
    fn ops_memory_error_codes_are_distinct() {
        let sid = SessionId::new();
        let cases = [
            (OpsError::MemoryNotConfigured, -32005_i64),
            (
                OpsError::SourceRangeTooLarge {
                    source_session: sid,
                    from_seq: 1,
                    to_seq: 9,
                    bytes: 4096,
                    cap: 1024,
                },
                -32006,
            ),
            (
                OpsError::SourceRangeOutOfBounds {
                    source_session: sid,
                    from_seq: 2,
                    to_seq: 509,
                    head_next_seq: 10,
                },
                -32602,
            ),
            (
                OpsError::UnknownSource {
                    source_session: sid,
                },
                -32001,
            ),
            (
                OpsError::EmptySourceRange {
                    source_session: sid,
                    from_seq: 1,
                    to_seq: 2,
                },
                -32602,
            ),
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
        pub dream_calls: Mutex<Vec<(SessionId, SessionId, u64, u64)>>,
        pub release_calls: Mutex<Vec<(String, Option<String>)>>,
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
        ) -> BoxFuture<'static, Result<super::MockSendReceipt, OpsError>> {
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

        fn release_worker(
            &self,
            worker: String,
            reason: Option<String>,
        ) -> BoxFuture<'static, Result<ReleaseWorkerReceipt, OpsError>> {
            self.release_calls
                .lock()
                .unwrap()
                .push((worker.clone(), reason));
            Box::pin(async move {
                Ok(ReleaseWorkerReceipt {
                    worker,
                    outcome: ReleaseWorkerOutcome::Released,
                    session: SessionId::new(),
                    thread_id: uuid::Uuid::nil(),
                    seq: 9,
                })
            })
        }

        fn memory_dream(
            &self,
            session: SessionId,
            source: SessionId,
            from_seq: u64,
            to_seq: u64,
        ) -> BoxFuture<'static, Result<super::DreamAdmitReceipt, OpsError>> {
            self.dream_calls
                .lock()
                .unwrap()
                .push((session, source, from_seq, to_seq));
            Box::pin(async move {
                Ok(super::DreamAdmitReceipt {
                    session,
                    source,
                    from_seq,
                    to_seq,
                    deduplicated: false,
                    bytes_read: 42,
                    prompt_bytes: 128,
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
