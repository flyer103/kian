//! Job-specific inbox wording for admissions made by the run conductor.
//!
//! The generic bundle inbox is deliberately provider-agnostic. The job path
//! needs one distinction the bundle can act on: a session that is already
//! terminal is not a generic write failure.

use futures::future::BoxFuture;
use kian_core::model::SessionId;
use serde_json::Value;

/// The outcome of one durable job-child admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmitReceipt {
    /// Whether an existing same-key row supplied the result.
    pub deduplicated: bool,
}

/// Failures the job conductor can distinguish.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InboxWriteError {
    /// The target session ended before this new row could be admitted.
    #[error("session {session:?} is terminal; input admission refused")]
    SessionTerminal {
        /// The terminal session.
        session: SessionId,
    },
    /// Any other admission failure.
    #[error("inbox write failed: {0}")]
    Write(String),
}

/// The narrow admission port used by job execution.
pub trait InboxWriter: Send + Sync {
    /// Durably admits one row, retaining terminal refusal as a typed fact.
    fn admit(
        &self,
        session: SessionId,
        payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>>;
}

#[cfg(test)]
mod tests {
    use super::InboxWriteError;
    use kian_core::model::SessionId;

    #[test]
    fn terminal_refusal_is_distinct_from_a_generic_write_failure() {
        let session = SessionId::new();
        let terminal = InboxWriteError::SessionTerminal { session };
        assert!(terminal.to_string().contains("terminal"));
        assert_ne!(terminal, InboxWriteError::Write("terminal".into()));
    }
}
