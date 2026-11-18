//! Typed client errors (moved verbatim from `kian-cli/src/client.rs`,
//! ) plus the frozen error-code table.
//!
//! Errors are honest and structured: transport failures stay
//! [`ClientError::Http`]; a JSON-RPC error object becomes
//! [`ClientError::Rpc`] with the server's code and message so callers
//! can **match on the code** instead of parsing strings.

/// What a client call can fail with.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("transport: {0}")]
    Http(String),
    #[error("rpc error {code}: {message}")]
    Rpc { code: i64, message: String },
    #[error("malformed response: {0}")]
    Malformed(String),
}

// ---------------------------------------------------------------------------
// The frozen error-code table (matchable, never string-parsed)
// ---------------------------------------------------------------------------

/// JSON-RPC error code for server-side failures (-32000).
pub const CODE_INTERNAL: i64 = -32000;
/// JSON-RPC error code for an unknown session (-32001).
pub const CODE_UNKNOWN_SESSION: i64 = -32001;
/// JSON-RPC error code for a session that is draining (-32002).
pub const CODE_SESSION_BUSY: i64 = -32002;
/// JSON-RPC error code for a terminal session (-32003).
pub const CODE_SESSION_TERMINAL: i64 = -32003;
/// JSON-RPC error code for an idle session where the action makes no
/// sense (ops plane, -32004).
pub const CODE_SESSION_IDLE: i64 = -32004;
/// JSON-RPC error code for `release_worker` on an unknown/unbound worker
/// id (ops plane, -32008, ADR-035).
pub const CODE_WORKER_NOT_BOUND: i64 = -32008;

impl ClientError {
    /// The JSON-RPC error code when the server returned a structured
    /// error object, `None` for transport/malformed failures.
    pub fn rpc_code(&self) -> Option<i64> {
        match self {
            Self::Rpc { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// The server does not know this session (-32001).
    pub fn is_unknown_session(&self) -> bool {
        self.rpc_code() == Some(CODE_UNKNOWN_SESSION)
    }

    /// The session is draining; retry when it is idle (-32002).
    pub fn is_session_busy(&self) -> bool {
        self.rpc_code() == Some(CODE_SESSION_BUSY)
    }

    /// The session is terminal; writes and ops are refused (-32003).
    pub fn is_session_terminal(&self) -> bool {
        self.rpc_code() == Some(CODE_SESSION_TERMINAL)
    }

    /// The session is idle where the ops action needs consumption
    /// first (-32004).
    pub fn is_session_idle(&self) -> bool {
        self.rpc_code() == Some(CODE_SESSION_IDLE)
    }

    /// A server-side internal failure (-32000).
    pub fn is_internal(&self) -> bool {
        self.rpc_code() == Some(CODE_INTERNAL)
    }

    /// `release_worker` named a worker with no live binding (-32008).
    pub fn is_worker_not_bound(&self) -> bool {
        self.rpc_code() == Some(CODE_WORKER_NOT_BOUND)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CODE_INTERNAL, CODE_SESSION_BUSY, CODE_SESSION_IDLE, CODE_SESSION_TERMINAL,
        CODE_UNKNOWN_SESSION, CODE_WORKER_NOT_BOUND, ClientError,
    };

    /// The frozen code table is exactly the table.
    #[test]
    fn the_error_code_table_is_frozen() {
        assert_eq!(CODE_INTERNAL, -32000);
        assert_eq!(CODE_UNKNOWN_SESSION, -32001);
        assert_eq!(CODE_SESSION_BUSY, -32002);
        assert_eq!(CODE_SESSION_TERMINAL, -32003);
        assert_eq!(CODE_SESSION_IDLE, -32004);
        assert_eq!(CODE_WORKER_NOT_BOUND, -32008);
    }

    /// Helpers classify by code — never by message text.
    #[test]
    fn helpers_match_on_code_not_message() {
        let cases = [
            (
                ClientError::Rpc {
                    code: CODE_UNKNOWN_SESSION,
                    message: "any text".into(),
                },
                "unknown",
            ),
            (
                ClientError::Rpc {
                    code: CODE_SESSION_BUSY,
                    message: "x".into(),
                },
                "busy",
            ),
            (
                ClientError::Rpc {
                    code: CODE_SESSION_TERMINAL,
                    message: "x".into(),
                },
                "terminal",
            ),
            (
                ClientError::Rpc {
                    code: CODE_SESSION_IDLE,
                    message: "x".into(),
                },
                "idle",
            ),
            (
                ClientError::Rpc {
                    code: CODE_INTERNAL,
                    message: "x".into(),
                },
                "internal",
            ),
        ];
        for (err, _label) in &cases {
            assert!(err.rpc_code().is_some());
        }
        let unknown = &cases[0].0;
        assert!(unknown.is_unknown_session());
        assert!(!unknown.is_session_busy());
        let http = ClientError::Http("conn refused".into());
        assert_eq!(http.rpc_code(), None);
        assert!(!http.is_unknown_session());
    }
}
