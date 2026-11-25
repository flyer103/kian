//! `kian-imessage-bridge` — the macOS-side iMessage bridge
//! (CHANNEL-IMESSAGE §3, ADR-042).
//!
//! Lives on the Mac signed into the bot Apple ID, in the logged-in user's
//! GUI session (a LaunchAgent) so that Full Disk Access and
//! Automation → Messages attach to a grantable binary path (§2.2). It
//! supervises one `imsg rpc` child (the only place macOS is touched) and
//! exposes a narrow loopback HTTP ABI to `kian serve`:
//!
//! - `GET  /v1/health`  — readiness: imsg/rpc, database.ready,
//!   the last-observed Automation state, the effective participation
//!   filter and the counters.
//! - `GET  /v1/updates` — bounded long poll over an exclusive `rowid`
//!   cursor, with the bridge's persisted tail replay (`since_rowid: 0`)
//!   and a `generation` that changes when `chat.db` is replaced.
//! - `GET  /v1/attachment/{rowid}/{index}` — one image by position,
//!   with an optional `as=jpeg` HEIC variant.
//! - `POST /v1/send`    — one text message, with the caller's
//!   `client_id` as the delivery idempotency key (a repeated id is
//!   answered from the observed-result cache, never sent twice).
//!
//! Everything here is deliberately kian-agnostic: no kian crate is a
//! dependency, so a different bridge implementation can replace this one
//! provided it honours the ABI (CHANNEL-IMESSAGE §15).

pub mod app;
pub mod attachment;
pub mod config;
pub mod imsg;
pub mod state;
