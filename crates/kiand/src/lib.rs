//! `kiand` — the endpoint daemon (card C22, DESIGN §4.3/§9):
//! `kian-endpoint`'s [`kian_endpoint::Endpoint`] over the built-in
//! [`kian_endpoint::LocalExecutor`], behind a thin, fail-closed main.
//!
//! The library half exists so the config surface (flags > environment >
//! optional `kiand.yaml` > defaults) and the daemon executor are
//! unit-testable without spawning a process; `main` is wiring only. The
//! config surface is deliberately **separate** from `kian-cli`'s
//! `kian.yaml` — a daemon and a server are configured by different
//! people in different places.
//!
//! Brain–hand separation (DESIGN §4.3) is inherited untouched: the daemon
//! holds zero session state; it executes commands and returns results.

pub mod config;
pub mod executor;
