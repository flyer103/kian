//! R12-03 L1 spawned-process tests for `kiand --register-to`: the real
//! daemon binary against a mock kian registration listener on loopback
//! (no real `kian serve` + kiand multi-process scenario — that is R12-06).
//!
//! Pins: absent token env refuses startup; malformed identity or
//! description refuses startup (exit 2, actionable message, no dial —
//! spec §2.3 shape rules); a terminal `bad_token` refusal exits
//! non-zero with an actionable, token-free error; SIGTERM drains and
//! sends a graceful `bye` before exit 0.

use std::time::Duration;

use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpListener;
use tokio::process::{Child, Command};

use kian_exec::proto::{
    EndpointFrame, PROTOCOL_VERSION, RefusalReason, ServerFrame, decode_endpoint_frame,
    encode_server_frame,
};

/// Fake credential material for loopback tests only.
const TOKEN: &str = "fake-test-token-0123456789abcdef";

fn kiand(addr: std::net::SocketAddr, token_env: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kiand"));
    command
        .arg("--register-to")
        .arg(addr.to_string())
        .arg("--endpoint-id")
        .arg("proc-box")
        .arg("--description")
        .arg("spawned test worker")
        .arg("--heartbeat-ms")
        .arg("60000")
        .env("KIAND_ENDPOINT_ID", "proc-box")
        .env("KIAND_DEDUPE", "0")
        .env(token_env, TOKEN)
        .kill_on_drop(true);
    command
}

async fn read_frame(
    lines: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> Option<EndpointFrame> {
    let mut line = String::new();
    match lines.read_line(&mut line).await {
        Ok(0) | Err(_) => None,
        Ok(_) => Some(decode_endpoint_frame(&line).expect("daemon emits decodable frames")),
    }
}

#[test]
fn absent_token_env_refuses_startup() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_kiand"))
        .args(["--register-to", "127.0.0.1:9", "--endpoint-id", "proc-box"])
        .env_remove("KIAN_WORKER_TOKEN")
        .output()
        .expect("kiand spawns");
    assert_eq!(
        output.status.code(),
        Some(2),
        "refuses to start: {output:?}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("KIAN_WORKER_TOKEN"),
        "names the missing variable: {stderr}"
    );
    assert!(!stderr.contains(TOKEN), "no token material in stderr");
}

// ---------------------------------------------------------------------------
// B2: register-mode identity/description shape validation (spec §2.3).
// A malformed identity or description refuses startup (exit 2, actionable
// message, no token material) before any dial: the server would otherwise
// answer `id_conflict` — retryable, re-sent forever — or silently drop the
// description (P11).
// ---------------------------------------------------------------------------

/// Spawn kiand against `addr` with an explicit id/description and capture
/// its output. The refusal must happen at resolve time, long before the
/// register deadline, so a short wait bound is honest.
async fn spawn_register(
    addr: std::net::SocketAddr,
    endpoint_id: &str,
    description: Option<&str>,
) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_kiand"));
    command
        .arg("--register-to")
        .arg(addr.to_string())
        .arg("--endpoint-id")
        .arg(endpoint_id)
        .env("KIAN_WORKER_TOKEN", TOKEN)
        .env("KIAND_DEDUPE", "0")
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    if let Some(description) = description {
        command.arg("--description").arg(description);
    }
    let child: Child = command.spawn().unwrap();
    tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .expect("a shape refusal exits promptly")
        .unwrap()
}

/// The registration listener must stay silent: a shape refusal happens
/// before any dial, so no connection ever arrives.
async fn assert_never_dials(listener: TcpListener) {
    let dialed = tokio::time::timeout(Duration::from_millis(500), listener.accept()).await;
    assert!(
        dialed.is_err(),
        "kiand must not dial kian when its identity/description is invalid"
    );
}

#[tokio::test]
async fn oversized_endpoint_id_refuses_startup_without_dialing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let output = spawn_register(addr, &"x".repeat(201), Some("build box")).await;
    assert_eq!(
        output.status.code(),
        Some(2),
        "shape refusal exits 2: {output:?}"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("endpoint_id"), "names the field: {stderr}");
    assert!(stderr.contains("1..=200"), "names the rule: {stderr}");
    assert!(!stderr.contains(TOKEN), "no token material in stderr");
    assert_never_dials(listener).await;
}

#[tokio::test]
async fn control_character_endpoint_id_refuses_startup() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let output = spawn_register(addr, "box\u{7}a", None).await;
    assert_eq!(output.status.code(), Some(2), "shape refusal exits 2");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("endpoint_id"), "names the field: {stderr}");
    assert!(stderr.contains("control"), "names the rule: {stderr}");
    assert!(!stderr.contains(TOKEN), "no token material in stderr");
    assert_never_dials(listener).await;
}

#[tokio::test]
async fn invalid_description_refuses_startup_without_dialing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    for description in [
        "   ".to_string(),           // empty after trim
        "d".repeat(201),             // over the 200-char bound
        "build\u{7}box".to_string(), // control character
    ] {
        let output = spawn_register(addr, "proc-box", Some(&description)).await;
        assert_eq!(
            output.status.code(),
            Some(2),
            "invalid description refuses startup: {output:?}"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("description"), "names the field: {stderr}");
        assert!(
            stderr.contains("200") || stderr.contains("non-empty") || stderr.contains("control"),
            "names the rule: {stderr}"
        );
        assert!(!stderr.contains(TOKEN), "no token material in stderr");
    }
    assert_never_dials(listener).await;
}

#[tokio::test]
async fn bad_token_refusal_exits_actionably_without_leaking() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (read, mut write) = stream.into_split();
        let mut lines = BufReader::new(read);
        // The register frame arrives; refuse with bad_token.
        let frame = read_frame(&mut lines).await.expect("register arrives");
        match frame {
            EndpointFrame::Register {
                endpoint_id,
                description,
                version,
                max_concurrent,
                ..
            } => {
                assert_eq!(endpoint_id, "proc-box");
                assert_eq!(description.as_deref(), Some("spawned test worker"));
                assert_eq!(version, PROTOCOL_VERSION);
                assert!(max_concurrent >= 1);
            }
            other => panic!("first frame must be register, got {other:?}"),
        }
        let mut line = encode_server_frame(&ServerFrame::Rejected {
            reason: RefusalReason::BadToken,
            detail: None,
        });
        line.push('\n');
        write.write_all(line.as_bytes()).await.unwrap();
        write.shutdown().await.unwrap();
    });
    let mut command = kiand(addr, "KIAN_WORKER_TOKEN");
    command.stderr(std::process::Stdio::piped());
    let child: Child = command.spawn().unwrap();
    let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .expect("terminal refusal exits promptly")
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "terminal refusal exits non-zero"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("bad_token"), "names the reason: {stderr}");
    assert!(
        stderr.contains("KIAN_WORKER_TOKEN"),
        "actionable hint names the variable: {stderr}"
    );
    assert!(!stderr.contains(TOKEN), "the token never lands in logs");
    server.await.unwrap();
}

#[tokio::test]
async fn sigterm_drains_sends_bye_and_exits_zero() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // Signals the test body once the daemon's first heartbeat arrives —
    // proof the welcome was processed and serving has started. SIGTERM is
    // only owed (and only tested) from that point on, so the test cannot
    // race the handshake under load.
    let (serving_tx, mut serving_rx) = tokio::sync::watch::channel(false);
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (read, mut write) = stream.into_split();
        let mut lines = BufReader::new(read);
        let frame = read_frame(&mut lines).await.expect("register arrives");
        assert!(matches!(frame, EndpointFrame::Register { .. }));
        // Direct a fast heartbeat so the "serving" signal arrives quickly.
        let mut welcome = encode_server_frame(&ServerFrame::Welcome {
            v: PROTOCOL_VERSION,
            heartbeat_ms: Some(200),
        });
        welcome.push('\n');
        write.write_all(welcome.as_bytes()).await.unwrap();
        // Serve until the connection closes; the graceful bye must be
        // among the frames seen.
        let mut saw_bye = false;
        while let Some(frame) = read_frame(&mut lines).await {
            match frame {
                EndpointFrame::Bye => saw_bye = true,
                EndpointFrame::Heartbeat { .. } => {
                    let _ = serving_tx.send(true);
                }
                _ => {}
            }
        }
        assert!(saw_bye, "SIGTERM produced a graceful bye");
    });
    let mut child = kiand(addr, "KIAN_WORKER_TOKEN").spawn().unwrap();
    // Wait until the daemon is provably serving (first heartbeat seen by
    // the mock server), then SIGTERM — no fixed sleep to race.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if *serving_rx.borrow_and_update() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "daemon never reached the serving state"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let pid = child.id().expect("child pid");
    let status = std::process::Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("kill -TERM");
    assert!(status.success());
    let exit = tokio::time::timeout(Duration::from_secs(10), child.wait())
        .await
        .expect("graceful shutdown is prompt")
        .unwrap();
    assert_eq!(exit.code(), Some(0), "graceful bye exits 0");
    tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("mock server finishes")
        .unwrap();
}
