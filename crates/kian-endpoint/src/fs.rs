//! Worker-side filesystem execution (KiandProto v2, ADR-023).
//!
//! The endpoint performs the three text operations under its configured
//! workspace root. The same lexical `..` rule the server-side tool
//! applies is re-checked here (defence in depth, not a sandbox: `exec`
//! already runs arbitrary programs as the same user). Every failure is a
//! typed [`CommandError`] the server turns into an honest `{"error": …}`
//! tool result — never a fake success, never a truncated read.

use std::path::Path;

use kian_core::fs::{FS_MAX_READ_BYTES, FS_MAX_WRITE_BYTES, FsCommand, FsResult};

use crate::proto::{CommandError, ErrorCode};

/// Execute one filesystem command under `root`.
pub fn execute(root: &Path, cmd: &FsCommand) -> Result<FsResult, CommandError> {
    let path = cmd.path();
    kian_core::fs::confine_path(path).map_err(io_error)?;
    match cmd {
        FsCommand::List { .. } => {
            let dir = root.join(path);
            let mut entries: Vec<String> = std::fs::read_dir(&dir)
                .map_err(|e| io_error(format!("{path}: {e}")))?
                .map(|entry| {
                    entry
                        .map(|e| e.file_name().to_string_lossy().into_owned())
                        .map_err(|e| io_error(format!("{path}: {e}")))
                })
                .collect::<Result<Vec<_>, _>>()?;
            entries.sort();
            Ok(FsResult::List { entries })
        }
        FsCommand::Read { .. } => {
            let file = root.join(path);
            let meta = std::fs::metadata(&file).map_err(|e| io_error(format!("{path}: {e}")))?;
            if meta.len() > FS_MAX_READ_BYTES as u64 {
                return Err(io_error(format!(
                    "{path}: file is {} bytes; the read limit is {FS_MAX_READ_BYTES} bytes",
                    meta.len()
                )));
            }
            let content =
                std::fs::read_to_string(&file).map_err(|e| io_error(format!("{path}: {e}")))?;
            Ok(FsResult::Read { content })
        }
        FsCommand::Write { content, .. } => {
            if content.len() > FS_MAX_WRITE_BYTES {
                return Err(io_error(format!(
                    "{path}: content is {} bytes; the write limit is {FS_MAX_WRITE_BYTES} bytes",
                    content.len()
                )));
            }
            let file = root.join(path);
            if let Some(parent) = file.parent() {
                std::fs::create_dir_all(parent).map_err(|e| io_error(format!("{path}: {e}")))?;
            }
            std::fs::write(&file, content).map_err(|e| io_error(format!("{path}: {e}")))?;
            Ok(FsResult::Write { written: true })
        }
    }
}

fn io_error(message: impl Into<String>) -> CommandError {
    CommandError {
        code: ErrorCode::Io,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::execute;
    use kian_core::fs::{FS_MAX_READ_BYTES, FsCommand, FsResult};

    fn temp_root() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("kian-endpoint-fs-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn write_read_list_round_trip_under_the_root() {
        let root = temp_root();
        let write = execute(
            &root,
            &FsCommand::Write {
                path: "notes/a.txt".into(),
                content: "hello".into(),
            },
        )
        .unwrap();
        assert_eq!(write, FsResult::Write { written: true });
        let read = execute(
            &root,
            &FsCommand::Read {
                path: "notes/a.txt".into(),
            },
        )
        .unwrap();
        assert_eq!(
            read,
            FsResult::Read {
                content: "hello".into()
            }
        );
        let list = execute(
            &root,
            &FsCommand::List {
                path: "notes".into(),
            },
        )
        .unwrap();
        assert_eq!(
            list,
            FsResult::List {
                entries: vec!["a.txt".into()]
            }
        );
    }

    #[test]
    fn upward_paths_are_refused() {
        let root = temp_root();
        let err = execute(
            &root,
            &FsCommand::Read {
                path: "../escape".into(),
            },
        )
        .unwrap_err();
        assert!(err.message.contains(".."), "{err:?}");
    }

    #[test]
    fn oversized_reads_and_writes_are_honest_errors() {
        let root = temp_root();
        std::fs::write(root.join("big"), "x".repeat(FS_MAX_READ_BYTES + 1)).unwrap();
        let err = execute(&root, &FsCommand::Read { path: "big".into() }).unwrap_err();
        assert!(err.message.contains("read limit"), "{err:?}");
        let err = execute(
            &root,
            &FsCommand::Write {
                path: "too-big".into(),
                content: "x".repeat(FS_MAX_READ_BYTES + 1),
            },
        )
        .unwrap_err();
        assert!(err.message.contains("write limit"), "{err:?}");
    }
}
