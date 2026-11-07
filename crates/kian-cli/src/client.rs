//! Thin re-export surface: the JSON-RPC/SSE client moved to
//! the wire-only `kian-sdk` crate so embedders get the frozen
//! surface without any kian server crate. [`KianClient`] keeps its
//! `crate::client` path — `commands.rs` is byte-identical.

pub use kian_sdk::{ClientError, KianClient};
