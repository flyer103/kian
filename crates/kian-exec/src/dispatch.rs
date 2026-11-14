//! The correlated dispatch reply: one command id resolves to exactly one
//! family (exec or fs) — the two never mix on a connection because the
//! command id is minted per row and a row has one command family
//! (ADR-023).

use kian_core::fs::FsResult;
use kian_core::seams::ExecResult;

use crate::error::ConnectionError;
use crate::proto::CommandError;

/// What one dispatch resolved to.
pub enum DispatchReply {
    Exec(Result<ExecResult, CommandError>),
    Fs(Result<FsResult, CommandError>),
}

/// A dispatch outcome: transport failure, or a correlated command reply.
pub type DispatchOutcome = Result<DispatchReply, ConnectionError>;
