//! `kian-exec` — KiandProto's **server half**: the side that dials
//! execution endpoints (cards C19/C20, DESIGN §4.3/§7).
//!
//! The pairing has two independent halves sharing one wire contract:
//! - `kian-endpoint` (endpoint side): listens, executes commands, emits
//!   heartbeats;
//! - this crate (server side): [`connection::ConnectionManager`] dials a
//!   configured endpoint address, validates the hello/welcome handshake,
//!   tracks heartbeat freshness, pipelines commands and correlates results
//!   by command id, probes with ping, says bye on shutdown, and reconnects
//!   with bounded backoff.
//!
//! The wire types ([`proto`]) are this crate's own golden-pinned
//! declaration of KiandProto v0 — deliberately not shared code with the
//! endpoint crate. Any wire-frame change must update both crates' golden
//! tests and bump `PROTOCOL_VERSION` in both, in the same commit.
//!
//! C20 adds the durable half on top of the connection half (ADR-007):
//! - [`store`] — `endpoint_registry` + `exec_queue` tables in the server's
//!   `kian.sqlite` under this crate's own migrations. Queue rows are
//!   pre-event durable state (exactly like inbox rows): durable before any
//!   dispatch, and results reach session events only via the normal
//!   `tool/call`/`tool/result` loop path;
//! - [`backend`] — [`RemoteExecBackend`] (enqueue-first `ExecBackend`:
//!   park while offline, re-drive on connect/restart, honest
//!   `ExecError::Timeout` → `abandoned` at `queue_timeout`, at-least-once
//!   to the endpoint, deduped by command id);
//! - [`tool`] — [`RemoteExecTool`] (the same `exec` tool schema as the
//!   local built-in, routed through a configured endpoint);
//! - [`watchdog`] — [`HeartbeatWatchdog`], the v1 [`kian_core::seams::
//!   WatchdogSource`] impl: an edge-triggered heartbeat-freshness monitor
//!   over the registry projection (C21, DESIGN §4.3). It observes only —
//!   health events reach sessions exclusively via Admit.
//!
//! Brain–hand separation (DESIGN §4.3): this crate holds no session state.
//! Command ids are correlation ids; the durable ones derive from the
//! calling tool-call id (ADR-007).

pub mod backend;
pub mod connection;
pub mod error;
pub mod proto;
pub mod store;
pub mod tool;
pub mod watchdog;

pub use backend::{RemoteExecBackend, RemoteExecConfig};
pub use connection::{BackoffConfig, ConnectionConfig, ConnectionManager, EndpointStatus};
pub use error::ConnectionError;
pub use proto::PROTOCOL_VERSION;
pub use store::{
    CommandOutcome, EndpointRegistryRow, EnqueueOutcome, ExecStore, QueueStatus, QueueStoreError,
    QueuedCommand,
};
pub use tool::RemoteExecTool;
pub use watchdog::{
    Clock, HeartbeatWatchdog, ManualClock, SystemClock, WatchdogConfig, WatchdogMetrics,
};
