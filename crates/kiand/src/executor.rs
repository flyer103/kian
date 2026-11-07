//! The daemon's executor: [`LocalExecutor`] plus the three things a
//! daemon needs on top of an embeddable SDK.
//!
//! - applies `exec_timeout_ms` as the default bound for commands that
//!   arrive without their own `timeout_ms` — the daemon never runs an
//!   unbounded command, which also bounds the SIGTERM drain wait;
//! - logs every command and its outcome at info (constraint 7 spirit:
//!   the log honestly records what ran and how it ended);
//! - tracks in-flight executions through a watch channel so the main
//!   task can drain them before exiting on SIGTERM/SIGINT.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::future::BoxFuture;
use kian_core::seams::{ExecBackend, ExecCommand, ExecError, ExecResult};
use kian_endpoint::LocalExecutor;
use tokio::sync::watch;

#[derive(Debug)]
pub struct DaemonExecutor {
    inner: LocalExecutor,
    default_timeout_ms: u64,
    inflight: watch::Sender<usize>,
    running: Arc<AtomicUsize>,
}

impl DaemonExecutor {
    /// New executor plus the in-flight receiver the main task drains on
    /// shutdown.
    pub fn new(default_timeout_ms: u64) -> (Arc<Self>, watch::Receiver<usize>) {
        let (inflight, inflight_rx) = watch::channel(0usize);
        (
            Arc::new(Self {
                inner: LocalExecutor,
                default_timeout_ms,
                inflight,
                running: Arc::new(AtomicUsize::new(0)),
            }),
            inflight_rx,
        )
    }

    /// The default bound applied to commands arriving without one.
    pub fn default_timeout_ms(&self) -> u64 {
        self.default_timeout_ms
    }

    /// Count one execution as in flight (synchronous: called before the
    /// exec future exists).
    fn enter(&self) {
        self.inflight
            .send_replace(self.running.fetch_add(1, Ordering::SeqCst) + 1);
    }
}

impl ExecBackend for DaemonExecutor {
    fn exec(&self, mut cmd: ExecCommand) -> BoxFuture<'static, Result<ExecResult, ExecError>> {
        if cmd.timeout_ms.is_none() {
            cmd.timeout_ms = Some(self.default_timeout_ms);
        }
        tracing::info!(command = %cmd.argv.join(" "), "executing command");
        // Count synchronously, before the future exists: the moment the
        // endpoint calls `exec` the command is on the daemon's books, so
        // the SIGTERM drain cannot observe a false zero in the window
        // between the endpoint's own in-flight counter and this task's
        // first poll.
        self.enter();
        // Owned clones into the 'static future (the seam returns a boxed
        // future that outlives this borrow).
        let inner = self.inner; // LocalExecutor is Copy
        let inflight = self.inflight.clone();
        let running = self.running.clone();
        Box::pin(async move {
            let result = inner.exec(cmd).await;
            inflight.send_replace(running.fetch_sub(1, Ordering::SeqCst) - 1);
            match &result {
                Ok(r) => tracing::info!(
                    exit_code = ?r.exit_code,
                    timed_out = r.timed_out,
                    "command finished"
                ),
                Err(e) => tracing::info!(error = %e, "command failed"),
            }
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn cmd(argv: &[&str], timeout_ms: Option<u64>) -> ExecCommand {
        ExecCommand {
            argv: argv.iter().map(|s| s.to_string()).collect(),
            env: Default::default(),
            cwd: None,
            timeout_ms,
        }
    }

    #[tokio::test]
    async fn default_timeout_binds_commands_that_arrive_without_one() {
        let (exec, _inflight) = DaemonExecutor::new(150);
        let started = Instant::now();
        let result = exec.exec(cmd(&["sleep", "5"], None)).await.unwrap();
        assert!(
            result.timed_out,
            "the daemon default must bound the command"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn explicit_command_timeout_is_not_overridden() {
        let (exec, _inflight) = DaemonExecutor::new(60_000);
        // Only the command's own 120ms bound can end this in time.
        let result = exec.exec(cmd(&["sleep", "5"], Some(120))).await.unwrap();
        assert!(result.timed_out);
    }

    #[tokio::test]
    async fn fast_commands_pass_through_unharmed() {
        let (exec, _inflight) = DaemonExecutor::new(30_000);
        let result = exec.exec(cmd(&["echo", "hi"], None)).await.unwrap();
        assert_eq!(result.exit_code, Some(0));
        assert_eq!(result.stdout.trim(), "hi");
        assert!(!result.timed_out);
    }

    #[tokio::test]
    async fn inflight_watch_rises_then_falls_to_zero() {
        let (exec, mut inflight) = DaemonExecutor::new(30_000);
        let task_exec = exec.clone();
        let task = tokio::spawn(async move {
            task_exec
                .exec(cmd(&["sleep", "1"], Some(5_000)))
                .await
                .unwrap()
        });
        inflight
            .wait_for(|&n| n == 1)
            .await
            .expect("command enters flight");
        let result = task.await.expect("task joins");
        assert_eq!(result.exit_code, Some(0));
        inflight
            .wait_for(|&n| n == 0)
            .await
            .expect("drains back to zero");
    }
}
