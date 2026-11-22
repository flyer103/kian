//! The declarative job kernel: a frozen plan, its canonical bytes and
//! digests, deterministic run/step identities, readiness, placement,
//! outcomes and the operator-facing run projection
//! (`design/JOB-MODEL.md`).
//!
//! Everything in this module is pure — no I/O, no clock, no store, no
//! ambient config. The conductor (`crate::job::conduct`, the run session's
//! driver) feeds these functions facts committed to the event stream,
//! so a re-driven drain reaches the same conclusions as the original one
//! (J1, J4, J12).

pub mod alert;
pub mod conduct;
pub mod outcome;
pub mod placement;
pub mod plan;
pub mod status;
