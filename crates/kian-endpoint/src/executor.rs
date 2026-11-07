//! Built-in local executor (DESIGN §4.3).
//!
//! Executes [`ExecCommand`]s as local child processes: argv[0] is the
//! program (never a shell string), the child inherits this process's
//! environment plus the command's overrides, and `timeout_ms` kills the
//! child when it elapses. Data-egress tiering and credential-less
//! execution are future stakes (DESIGN §4.4) — deliberately not built.

use std::process::Stdio;
use std::time::Duration;

use futures::future::BoxFuture;
use kian_core::seams::{ExecBackend, ExecCommand, ExecError, ExecResult};

/// Local process executor: the default executor an endpoint is configured
/// with. All I/O is async (tokio process) — never blocks a runtime worker.
#[derive(Debug, Default, Clone, Copy)]
pub struct LocalExecutor;

impl ExecBackend for LocalExecutor {
    fn exec(&self, cmd: ExecCommand) -> BoxFuture<'static, Result<ExecResult, ExecError>> {
        Box::pin(async move {
            let program = cmd
                .argv
                .first()
                .cloned()
                .ok_or_else(|| ExecError::Spawn("empty argv".into()))?;
            let mut command = tokio::process::Command::new(program);
            command.args(&cmd.argv[1..]);
            command.envs(&cmd.env);
            if let Some(cwd) = &cmd.cwd {
                command.current_dir(cwd);
            }
            command
                .kill_on_drop(true) // a cancelled future must not orphan the child
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            let child = command
                .spawn()
                .map_err(|error| ExecError::Spawn(error.to_string()))?;

            match cmd.timeout_ms {
                None => child
                    .wait_with_output()
                    .await
                    .map(|output| ExecResult {
                        exit_code: output.status.code(),
                        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                        timed_out: false,
                        redactions: Vec::new(),
                    })
                    .map_err(|error| ExecError::Io(error.to_string())),
                Some(ms) => {
                    match tokio::time::timeout(Duration::from_millis(ms), child.wait_with_output())
                        .await
                    {
                        // The elapsed branch dropped the future, which
                        // dropped the child; kill_on_drop already fired,
                        // and tokio's orphan reaper collects the corpse.
                        Err(_elapsed) => Ok(ExecResult {
                            exit_code: None,
                            stdout: String::new(),
                            stderr: String::new(),
                            timed_out: true,
                            redactions: Vec::new(),
                        }),
                        Ok(output) => output
                            .map(|output| ExecResult {
                                exit_code: output.status.code(),
                                stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                                stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                                timed_out: false,
                                redactions: Vec::new(),
                            })
                            .map_err(|error| ExecError::Io(error.to_string())),
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(argv: &[&str], timeout_ms: Option<u64>) -> ExecCommand {
        ExecCommand {
            argv: argv.iter().map(|s| s.to_string()).collect(),
            env: Default::default(),
            cwd: None,
            timeout_ms,
        }
    }

    #[tokio::test]
    async fn local_exec_runs_command_and_captures_output() {
        let result = LocalExecutor
            .exec(cmd(&["echo", "hello"], None))
            .await
            .unwrap();
        assert_eq!(result.exit_code, Some(0));
        assert_eq!(result.stdout.trim(), "hello");
        assert_eq!(result.stderr, "");
        assert!(!result.timed_out);
    }

    #[tokio::test]
    async fn local_exec_timeout_kills_and_reports_honestly() {
        let result = LocalExecutor
            .exec(cmd(&["sleep", "5"], Some(100)))
            .await
            .unwrap();
        assert!(result.timed_out, "must be reported as timed out");
        assert_eq!(result.exit_code, None, "a killed child has no exit code");
    }

    #[tokio::test]
    async fn local_exec_empty_argv_is_a_spawn_error() {
        let err = LocalExecutor.exec(cmd(&[], None)).await.unwrap_err();
        assert!(matches!(err, ExecError::Spawn(_)));
    }
}
