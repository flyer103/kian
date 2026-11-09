//! `kian-endpoint` — endpoint SDK library: KiandProto's endpoint-side
//! implementation, embeddable in any process (DESIGN §4.3).
//!
//! Two access forms share one protocol:
//! - [`Endpoint::serve`] — accept commands over a stream transport (TCP),
//!   execute via a configurable [`ExecBackend`] under a concurrency cap,
//!   return results and emit heartbeats;
//! - [`Endpoint::embed`] — the same connection loop wired to an in-process
//!   peer, for embedding in any application.
//!
//! Brain–hand separation: the endpoint keeps **zero session state** — it
//! executes commands and returns results; session logs live in Kian's
//! EventStore. The server-side pair (`kian-exec`, bundle) is deliberately
//! absent; tests run against an in-process mock of that half ([`ServerPeer`]
//! doubles as the reference client). Wire format v0 is provisional — churn
//! is expected when bundle lands the real pair.
//!
//! Security boundary (DESIGN §4.4) is a future stake: no auth, no
//! data-egress tiering, no credential-less use here yet.
pub mod endpoint;
pub mod executor;
pub mod proto;
pub mod register;
pub mod tcp;

pub use endpoint::{Endpoint, EndpointError, HeartbeatInfo, HeartbeatTap, ServerPeer};
pub use executor::LocalExecutor;
pub use proto::{RefusalReason, RegisterToken};
pub use register::{RegisterBackoff, RegisterInfo, connect_registered, register_loop};

#[cfg(test)]
mod tests {
    /// Review assertion for the brain–hand separation (DESIGN §4.3): the
    /// endpoint stores zero session state — mechanically enforced by
    /// scanning every source file of this crate for references to the
    /// core's session identifier type, session-keyed fields, and session
    /// field access. Command ids are correlation ids scoped to a single
    /// in-flight execution; they are not session state.
    #[test]
    fn zero_session_state_review_assertion() {
        let sources = [
            ("src/lib.rs", include_str!("lib.rs")),
            ("src/proto.rs", include_str!("proto.rs")),
            ("src/endpoint.rs", include_str!("endpoint.rs")),
            ("src/executor.rs", include_str!("executor.rs")),
            ("src/tcp.rs", include_str!("tcp.rs")),
            ("src/register.rs", include_str!("register.rs")),
        ];
        // Assembled at runtime so this file itself stays free of the
        // literals it scans for.
        let core_session_type = ["S", "ession", "Id"].concat();
        let session_field = ["session", "_id"].concat();
        let session_access = [".", "session"].concat();
        for (name, src) in sources {
            for needle in [&core_session_type, &session_field, &session_access] {
                assert!(
                    !src.contains(needle.as_str()),
                    "{name} must not reference session identifiers (found `{needle}`)"
                );
            }
        }
    }
}
