//! End-to-end daemon tests (acceptance): spawn the **real
//! `kiand` binary** (`CARGO_BIN_EXE_kiand`) on an ephemeral port and pair
//! it with the client (`kian_exec::ConnectionManager`) over real TCP.
//! No credentials, `NO_PROXY` honored (plan §1 rule 6).

use std::net::SocketAddr;
use std::process::Stdio;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::AsyncBufReadExt as _;

use kian_core::seams::ExecCommand;
use kian_endpoint::endpoint::DEFAULT_MAX_CONCURRENT;
use kian_exec::{ConnectionConfig, ConnectionManager};
use tokio::io::BufReader;
use tokio::process::{Child, Command};
use tokio::time::timeout;

/// Local e2e discipline: uniform across every suite even though raw TCP
/// ignores proxies.
fn no_proxy() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        // SAFETY: no other thread in this test binary reads the
        // environment, and the write happens exactly once before any
        // socket is opened by the calling test.
        unsafe { std::env::set_var("NO_PROXY", "127.0.0.1,localhost") };
    });
}

fn cmd(argv: &[&str]) -> ExecCommand {
    ExecCommand {
        argv: argv.iter().map(|s| s.to_string()).collect(),
        env: Default::default(),
        cwd: None,
        timeout_ms: None,
    }
}

/// Poll until `cond` holds; panics with `what` after ~15s.
async fn until(what: &str, cond: impl Fn() -> bool) {
    for _ in 0..1_500 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached within 15s: {what}");
}

/// The spawned daemon. Killed on drop so a failing test never leaks a
/// process.
struct Daemon {
    child: Child,
}

impl Daemon {
    fn spawn(args: &[&str], envs: &[(&str, &str)]) -> Daemon {
        no_proxy();
        let mut command = Command::new(env!("CARGO_BIN_EXE_kiand"));
        command
            .args(args)
            .env("NO_PROXY", "127.0.0.1,localhost")
            .envs(envs.iter().copied())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .kill_on_drop(true);
        Daemon {
            child: command.spawn().expect("spawn kiand"),
        }
    }

    /// Read stdout lines until the registration line; parse the bound
    /// address. Fails loudly if the daemon exits before announcing.
    async fn wait_listening(&mut self) -> SocketAddr {
        let stdout = self.child.stdout.take().expect("piped stdout");
        let mut lines = BufReader::new(stdout).lines();
        loop {
            let line = match timeout(Duration::from_secs(15), lines.next_line()).await {
                Ok(Ok(Some(line))) => line,
                Ok(Ok(None)) => panic!("kiand exited before printing its listening line"),
                Ok(Err(e)) => panic!("cannot read kiand stdout: {e}"),
                Err(_) => panic!("kiand did not print its listening line within 15s"),
            };
            if let Some(rest) = line.strip_prefix("kiand: endpoint ")
                && let Some((_id, addr)) = rest.split_once(" listening on ")
            {
                return addr.parse().expect("bound address parses");
            }
        }
    }

    async fn sigterm(&mut self) {
        let pid = self.child.id().expect("child pid while running");
        let status = std::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .expect("kill -TERM");
        assert!(status.success(), "kill -TERM must succeed");
    }

    async fn wait_exit(&mut self, bound_s: u64) -> std::process::ExitStatus {
        timeout(Duration::from_secs(bound_s), self.child.wait())
            .await
            .expect("kiand exits within the bound")
            .expect("child wait succeeds")
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// A running daemon + a connected manager: handshake done, heartbeat
/// observed.
async fn paired_daemon(heartbeat_ms: &str) -> (Daemon, ConnectionManager) {
    let mut daemon = Daemon::spawn(&["--heartbeat-ms", heartbeat_ms], &[]);
    let addr = daemon.wait_listening().await;
    let manager = ConnectionManager::new(ConnectionConfig::new(addr));
    manager.start();
    until("manager connects", || manager.status().connected).await;
    until("heartbeat observed", || manager.status().last_seen_ms > 0).await;
    (daemon, manager)
}

/// `--version` prints the workspace version.
#[test]
fn version_flag_prints_workspace_version() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_kiand"))
        .arg("--version")
        .env("NO_PROXY", "127.0.0.1,localhost")
        .output()
        .unwrap();
    assert!(out.status.success());
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        format!("kiand {}", env!("CARGO_PKG_VERSION"))
    );
}

/// The core round trip: default ephemeral port (announced on stdout),
/// hello/welcome handshake, heartbeats flowing, a real `echo` executing,
/// and its result returning through the manager.
#[tokio::test]
async fn handshake_heartbeat_command_result_round_trip() {
    let (mut daemon, manager) = paired_daemon("50").await;

    // The handshake announced the daemon's identity and concurrency cap.
    let status = manager.status();
    assert_eq!(status.endpoint_id, "kiand");
    assert_eq!(status.max_concurrent as usize, DEFAULT_MAX_CONCURRENT);

    // A command executes for real and its result returns.
    let outcome = timeout(
        Duration::from_secs(10),
        manager.dispatch(cmd(&["echo", "hello"])),
    )
    .await
    .expect("dispatch within bound")
    .expect("dispatch transport ok");
    let result = outcome.expect("command succeeded");
    assert_eq!(result.exit_code, Some(0));
    assert_eq!(result.stdout.trim(), "hello");
    assert!(!result.timed_out);

    manager.shutdown().await;
    daemon.sigterm().await;
    let status = daemon.wait_exit(10).await;
    assert_eq!(status.code(), Some(0), "SIGTERM exits 0 after drain");
}

/// SIGTERM mid-flight: the in-flight command still finishes, its result
/// is delivered, and only then does the daemon exit 0 (drain, then
/// transport close).
#[tokio::test]
async fn sigterm_drains_inflight_command_then_exits_zero() {
    let (mut daemon, manager) = paired_daemon("50").await;

    let dispatch = manager.dispatch(cmd(&["sleep", "1"]));
    tokio::pin!(dispatch);
    // Drive the dispatch (a future sends nothing until polled) while
    // giving the endpoint time to start the execution.
    tokio::select! {
        res = &mut dispatch => panic!("dispatch settled before SIGTERM: {res:?}"),
        _ = tokio::time::sleep(Duration::from_millis(300)) => {}
    }
    // The heartbeat projection must show the command in flight before the
    // signal lands — this is what makes the test a drain test.
    until("command is in flight", || manager.status().inflight == 1).await;

    daemon.sigterm().await;
    // The in-flight result must still be delivered before exit.
    let outcome = timeout(Duration::from_secs(15), dispatch.as_mut())
        .await
        .expect("in-flight result still delivered after SIGTERM")
        .expect("dispatch transport ok");
    let result = outcome.expect("slow command completed after drain");
    assert_eq!(result.exit_code, Some(0));
    assert!(!result.timed_out);

    let status = daemon.wait_exit(15).await;
    assert_eq!(
        status.code(),
        Some(0),
        "daemon exits 0 after finishing in-flight commands"
    );
    manager.shutdown().await;
}

/// Malformed config (bad value) refuses to start with a clear error.
#[test]
fn malformed_config_refuses_to_start_with_clear_error() {
    no_proxy();
    let dir =
        std::path::PathBuf::from(option_env!("CARGO_TARGET_TMPDIR").unwrap_or_else(|| "/tmp"));
    let file = dir.join("kiand-bad-value.yaml");
    std::fs::write(&file, "max_concurrent: definitely-not-a-number\n").unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_kiand"))
        .arg("--config")
        .arg(&file)
        .env("NO_PROXY", "127.0.0.1,localhost")
        .output()
        .unwrap();
    assert!(!out.status.success(), "must refuse to start");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("refusing to start"),
        "clear error on stderr, was: {stderr}"
    );
}

/// Unknown config keys are rejected (fail-closed, deny_unknown_fields).
#[test]
fn unknown_config_key_refuses_to_start() {
    no_proxy();
    let dir =
        std::path::PathBuf::from(option_env!("CARGO_TARGET_TMPDIR").unwrap_or_else(|| "/tmp"));
    let file = dir.join("kiand-unknown-key.yaml");
    std::fs::write(&file, "bogus: 1\n").unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_kiand"))
        .arg("--config")
        .arg(&file)
        .env("NO_PROXY", "127.0.0.1,localhost")
        .output()
        .unwrap();
    assert!(!out.status.success(), "must refuse to start");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("refusing to start"),
        "clear error on stderr"
    );
}

/// Malformed environment values refuse to start, naming the key.
#[test]
fn malformed_env_refuses_to_start() {
    no_proxy();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_kiand"))
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("KIAND_MAX_CONCURRENT", "abc")
        .output()
        .unwrap();
    assert!(!out.status.success(), "must refuse to start");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("KIAND_MAX_CONCURRENT"),
        "error names the offending key, was: {stderr}"
    );
}

/// Unknown flags are usage errors (exit 2), not silent starts.
#[test]
fn unknown_flag_is_a_usage_error() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_kiand"))
        .arg("--bogus")
        .env("NO_PROXY", "127.0.0.1,localhost")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}

/// preview: kill -9 (no drain, no bye), restart on the **same port**,
/// and the manager's reconnect loop re-handshakes and executes again.
#[tokio::test]
async fn kill9_restart_same_port_reconnects() {
    no_proxy();
    // Reserve an ephemeral port, then release it for the daemon.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let listen = format!("127.0.0.1:{port}");

    let mut daemon = Daemon::spawn(&["--listen", &listen, "--heartbeat-ms", "50"], &[]);
    let addr = daemon.wait_listening().await;
    assert_eq!(addr.port(), port);

    let manager = ConnectionManager::new(ConnectionConfig::new(addr));
    manager.start();
    until("manager connects", || manager.status().connected).await;
    let before = timeout(
        Duration::from_secs(10),
        manager.dispatch(cmd(&["echo", "before"])),
    )
    .await
    .expect("dispatch within bound")
    .expect("transport ok")
    .expect("command ok");
    assert_eq!(before.stdout.trim(), "before");

    // Hard crash: kill -9 — no drain, no bye frame.
    let _ = daemon.child.start_kill();
    let _ = daemon.child.wait().await;
    until("manager observes the crash", || !manager.status().connected).await;

    // Restart on the same port.
    let mut daemon2 = Daemon::spawn(&["--listen", &listen, "--heartbeat-ms", "50"], &[]);
    let addr2 = daemon2.wait_listening().await;
    assert_eq!(addr2, addr);

    // The manager reconnects on its own and the re-handshake works.
    until("manager reconnects after restart", || {
        manager.status().connected
    })
    .await;
    let after = timeout(
        Duration::from_secs(10),
        manager.dispatch(cmd(&["echo", "after"])),
    )
    .await
    .expect("dispatch within bound")
    .expect("transport ok")
    .expect("command ok");
    assert_eq!(after.stdout.trim(), "after");

    manager.shutdown().await;
    daemon2.sigterm().await;
    let status = daemon2.wait_exit(10).await;
    assert_eq!(status.code(), Some(0));
}
