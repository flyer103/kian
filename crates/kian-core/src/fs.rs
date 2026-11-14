//! Worker filesystem command contract (KiandProto v2, ADR-023).
//!
//! Pure types plus the one lexical path rule shared by the server-side
//! `RemoteFsTool` and the worker-side executor. No I/O lives here: the
//! server enqueues a command, the worker performs it under its configured
//! workspace root, and the result travels back as a correlated frame.
//!
//! The contract is deliberately narrow — three text operations, a 1 MiB
//! bound each way, and the same "no `..`" guard the local built-in fs
//! tools apply (the root is a guard, not a sandbox; a worker's `exec`
//! already runs arbitrary programs as the same user).

use serde::{Deserialize, Serialize};

/// Largest file body the read path returns (bytes). A larger file is an
/// honest error result, never a silent truncation.
pub const FS_MAX_READ_BYTES: usize = 1 << 20;

/// Largest file body the write path accepts (bytes of UTF-8 content).
pub const FS_MAX_WRITE_BYTES: usize = 1 << 20;

/// One filesystem command sent to a bound worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum FsCommand {
    #[serde(rename = "fs_list")]
    List { path: String },
    #[serde(rename = "fs_read")]
    Read { path: String },
    #[serde(rename = "fs_write")]
    Write { path: String, content: String },
}

impl FsCommand {
    /// The workspace-relative path this command addresses.
    #[must_use]
    pub fn path(&self) -> &str {
        match self {
            Self::List { path } | Self::Read { path } | Self::Write { path, .. } => path,
        }
    }

    /// The tool name this command corresponds to (`fs_list` / `fs_read` /
    /// `fs_write`) — the same vocabulary on both sides.
    #[must_use]
    pub fn tool_name(&self) -> &'static str {
        match self {
            Self::List { .. } => "fs_list",
            Self::Read { .. } => "fs_read",
            Self::Write { .. } => "fs_write",
        }
    }

    /// The wire/log spelling of the command family.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::List { .. } => "fs_list",
            Self::Read { .. } => "fs_read",
            Self::Write { .. } => "fs_write",
        }
    }
}

/// The successful outcome of one filesystem command. The shape mirrors the
/// local built-in tools' output exactly (`{"entries"}`, `{"content"}`,
/// `{"written"}`), so a model sees the same result whether the tool ran
/// locally or on a worker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum FsResult {
    #[serde(rename = "fs_list")]
    List { entries: Vec<String> },
    #[serde(rename = "fs_read")]
    Read { content: String },
    #[serde(rename = "fs_write")]
    Write { written: bool },
}

impl FsResult {
    /// The tool-result body (the exact local-tool JSON shape).
    #[must_use]
    pub fn to_tool_output(&self) -> serde_json::Value {
        match self {
            Self::List { entries } => serde_json::json!({ "entries": entries }),
            Self::Read { content } => serde_json::json!({ "content": content }),
            Self::Write { written } => serde_json::json!({ "written": written }),
        }
    }
}

/// The one lexical rule both sides apply: a path must be non-empty and must
/// never climb upward. Absolute paths are handled by the caller's root join
/// exactly as the local tools handle them (the root is a guard, not a
/// sandbox — ADR-023 records the limitation).
pub fn confine_path(path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err("path must not be empty".to_string());
    }
    if std::path::Path::new(path)
        .components()
        .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(format!("path {path:?} must not contain '..'"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{FS_MAX_READ_BYTES, FsCommand, FsResult, confine_path};

    #[test]
    fn path_rule_rejects_upward_and_empty_only() {
        assert!(confine_path("notes/a.txt").is_ok());
        assert!(confine_path("a/../b").is_err());
        assert!(confine_path("..").is_err());
        assert!(confine_path("").is_err());
    }

    #[test]
    fn limits_are_the_documented_mebibyte() {
        assert_eq!(FS_MAX_READ_BYTES, 1 << 20);
        assert_eq!(super::FS_MAX_WRITE_BYTES, 1 << 20);
    }

    #[test]
    fn command_and_result_round_trip_with_a_kind_tag() {
        let list = FsCommand::List {
            path: "notes".into(),
        };
        let json = serde_json::to_value(&list).unwrap();
        assert_eq!(json["kind"], "fs_list");
        assert_eq!(serde_json::from_value::<FsCommand>(json).unwrap(), list);

        let read = FsResult::Read {
            content: "hello".into(),
        };
        assert_eq!(
            read.to_tool_output(),
            serde_json::json!({"content": "hello"})
        );
    }
}
