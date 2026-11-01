//! `kian-sdk` — the wire-only embedded SDK for a running `kian serve`
//! (card C46, DESIGN §7's KSDK row).
//!
//! The crate speaks the frozen C13/C14 wire contract and nothing else:
//! JSON-RPC 2.0 over `POST /rpc` (control plane) and `POST /ops/rpc`
//! (operational plane), plus the `GET /sessions/{id}/events` SSE stream.
//! It depends on **no kian crate** — the SDK is embeddable anywhere a
//! Rust HTTP client can run.
//!
//! # Freeze discipline (the event-kinds rule applied to the API)
//!
//! This crate's public surface **is** the C13/C14 wire contract. After
//! card C46 it is **additive-only**: removing or reshaping a method,
//! param, response field, or error code is a breaking change and
//! requires a new plan round — the same governance as event kinds.
//!
//! # Layout
//!
//! - [`KianClient`] — the transport (`rpc`/`ops`) and the typed surface
//!   v1 (`create_session`, `submit`, `steer`, `queue`, `interrupt`,
//!   `projection`, the `events` SSE helper, and the ops wrappers).
//! - [`ClientError`] — typed, matchable errors (never string-parsed).
//! - [`SseFrame`] — one parsed frame of the C13 event stream.

pub mod error;
pub mod sse;
pub mod transport;
pub mod typed;

pub use error::{
    CODE_INTERNAL, CODE_SESSION_BUSY, CODE_SESSION_IDLE, CODE_SESSION_TERMINAL,
    CODE_UNKNOWN_SESSION, ClientError,
};
pub use sse::SseFrame;
pub use transport::KianClient;
pub use typed::{
    CreateSessionResult, InputAck, InterruptResult, MockSendReceipt, ProjectionResult,
    ResetReceipt, WakeOutcome,
};
