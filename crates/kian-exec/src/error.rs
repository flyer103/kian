//! Server-side connection failures.
//!
//! Transport loss is *expected* (intermittent endpoint presence is the norm,
//! DESIGN §4.3): it fails the in-flight command futures honestly and triggers
//! a bounded-backoff reconnect — it is never mapped onto a command failure or
//! a fake result (honesty over completion rate).

use std::net::SocketAddr;

/// Failures of the server→endpoint connection itself (distinct from
/// [`crate::proto::CommandError`], which is the endpoint's honest report
/// about a command it did execute).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConnectionError {
    /// The configured endpoint address could not be dialed.
    #[error("dial {addr} failed: {message}")]
    Dial { addr: SocketAddr, message: String },
    /// The transport went away (connection reset, EOF, or peer closed).
    #[error("transport closed")]
    TransportClosed,
    /// The peer violated KiandProto (e.g. duplicate hello, non-first
    /// handshake frame).
    #[error("protocol violation: {0}")]
    Protocol(String),
    /// The endpoint's hello announced an incompatible protocol version.
    /// Terminal: reconnecting cannot fix a version gap, so the manager
    /// stops and every dispatch reports this error.
    #[error("protocol version mismatch: endpoint={endpoint} server={server}")]
    VersionMismatch { endpoint: u32, server: u32 },
    /// The manager was shut down (or its supervisor exited) before the
    /// operation could complete.
    #[error("connection manager is shut down")]
    Shutdown,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant has a non-empty display — errors must stay readable
    /// in logs.
    #[test]
    fn error_display_is_human_readable() {
        let errors = vec![
            ConnectionError::Dial {
                addr: "127.0.0.1:1".parse().unwrap(),
                message: "connection refused".into(),
            },
            ConnectionError::TransportClosed,
            ConnectionError::Protocol("duplicate hello".into()),
            ConnectionError::VersionMismatch {
                endpoint: 1,
                server: 0,
            },
            ConnectionError::Shutdown,
        ];
        for error in &errors {
            assert!(!error.to_string().is_empty());
        }
        assert_eq!(
            ConnectionError::VersionMismatch {
                endpoint: 1,
                server: 0
            }
            .to_string(),
            "protocol version mismatch: endpoint=1 server=0"
        );
    }
}
