//! (R12-07 / kian-133) Independent QA acceptance probes for the runtime
//! worker membership — L1, exec/protocol half.
//!
//! QA-authored probes with QA-authored fixtures only (nothing shared with
//! the implementers' in-crate or R12-06 assertions). Re-derived from the
//! brief §7 L1 rows and the locked spec (§1 frames, §2 config, §3 state
//! machine, §4 queue-first, §9 adversarial ledger):
//!
//! - KiandProto v1 frame goldens (value equality + line round-trip) and
//!   the redacting `RegisterToken`;
//! - the register refusal matrix over a real in-process listener
//!   (`bad_token` / `version_mismatch` / `duplicate_static` /
//!   `id_conflict` / protocol violations / `welcome` cadence P6);
//! - the token compare semantics + a source anchor on the constant-time
//!   accumulator (timing itself is not deterministically observable in
//!   process, so the red-witness anchor is structural);
//! - the per-peer failed-auth window bound (P9, clock injected);
//! - the fail-closed bind policy matrix (C2/C3);
//! - membership state edges through the live listener (replace,
//!   generation, bye, heartbeat freshness, M3);
//! - queue-first: park with no route, drain on attach, command-id dedupe,
//!   no cross-worker dispatch, honest timeout (Q1–Q4).
//!
//! Loopback TCP inside the test process only (the `tcp_pair` convention);
//! no external network, no child processes, fake token values only.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use kian_core::seams::{ExecCommand, ExecError, ExecResult};
use kian_exec::Clock as _;
use kian_exec::proto::{
    EndpointFrame, PROTOCOL_VERSION, RefusalReason, RegisterToken, ServerFrame,
    decode_endpoint_frame, decode_server_frame, encode_endpoint_frame, encode_server_frame,
};
use kian_exec::registration::{NoopSink, RegistrationConfig, RegistrationListener, WorkerToken};
use kian_exec::route::RouteTransport;
use kian_exec::store::QueueStatus;
use kian_exec::watchdog::ManualClock;
use kian_exec::{
    DefaultRoute, ExecStore, MemberState, MembershipConfig, RemoteExecBackend, RemoteExecConfig,
    WorkerDirectory, constant_time_eq, is_private_bind_addr, validate_bind_addr,
};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpStream;
use uuid::Uuid;

const QA_TOKEN: &str = "qa-fake-token-not-a-secret-0123456789";

// ---------------------------------------------------------------------------
// QA fixtures
// ---------------------------------------------------------------------------

/// A raw test-side client of the registration listener: line frames in
/// both directions with a bounded read (a hung server fails the test
/// instead of hanging it).
struct QaClient {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

impl QaClient {
    async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).await.expect("connect listener");
        stream.set_nodelay(true).unwrap();
        let (reader, writer) = stream.into_split();
        Self {
            reader: BufReader::new(reader),
            writer,
        }
    }

    async fn send(&mut self, frame: &EndpointFrame) {
        let mut line = encode_endpoint_frame(frame);
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await.unwrap();
        self.writer.flush().await.unwrap();
    }

    /// One server frame, or None on EOF; 5 s bound. A read error counts
    /// as close (a refused/reset peer sends RST — still a close, never
    /// a frame).
    async fn recv(&mut self) -> Option<ServerFrame> {
        let mut line = String::new();
        let n = match tokio::time::timeout(Duration::from_secs(5), self.reader.read_line(&mut line))
            .await
        {
            Err(_) => panic!("no server frame within 5s"),
            Ok(Err(_)) => return None,
            Ok(Ok(0)) => return None,
            Ok(Ok(n)) => n,
        };
        let _ = n;
        Some(decode_server_frame(line.trim_end()).expect("decodable server frame"))
    }

    async fn recv_eof_expected(&mut self) {
        let mut junk = [0u8; 64];
        match tokio::time::timeout(Duration::from_secs(5), self.reader.read(&mut junk)).await {
            Err(_) => panic!("connection still open after 5s (expected close)"),
            // A reset IS a close (the peer dropped unread data); real
            // bytes before the close would be a protocol bug.
            Ok(Ok(0)) => {}
            Ok(Err(_)) => {}
            Ok(Ok(n)) => panic!("unexpected bytes before close: {:?}", &junk[..n]),
        }
    }
}

fn qa_register(id: &str, token: &str, version: u32) -> EndpointFrame {
    EndpointFrame::Register {
        endpoint_id: id.into(),
        description: Some("qa probe worker".into()),
        token: RegisterToken::new(token.into()),
        version,
        max_concurrent: 3,
        capabilities: Vec::new(),
    }
}

/// Start a listener on an ephemeral loopback port with the QA token and
/// an injected clock; static pins optional.
async fn qa_listener(
    clock: Arc<ManualClock>,
    static_ids: Vec<&str>,
) -> (Arc<RegistrationListener>, SocketAddr, Arc<WorkerDirectory>) {
    let directory = WorkerDirectory::new();
    let mut config = RegistrationConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        WorkerToken::new(QA_TOKEN.into()),
        30_000,
    );
    config.static_ids = static_ids.into_iter().map(str::to_string).collect();
    let listener = Arc::new(RegistrationListener::new(
        config,
        clock,
        Arc::new(NoopSink),
        directory.clone(),
    ));
    let runner = listener.clone();
    tokio::spawn(async move {
        let _ = runner.run().await;
    });
    for _ in 0..500 {
        if let Some(addr) = listener.local_addr() {
            return (listener, addr, directory);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("listener never bound");
}

fn qa_cmd(argv: &[&str]) -> ExecCommand {
    ExecCommand {
        argv: argv.iter().map(|s| s.to_string()).collect(),
        env: BTreeMap::new(),
        cwd: None,
        timeout_ms: None,
    }
}

fn qa_ok_result() -> ExecResult {
    ExecResult {
        exit_code: Some(0),
        stdout: "qa-done".into(),
        stderr: String::new(),
        timed_out: false,
        redactions: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// L1: KiandProto v1 frame goldens (spec §1.2/§1.4)
// ---------------------------------------------------------------------------

#[test]
fn qa_frame_goldens_v1_value_equality_and_round_trip() {
    assert_eq!(PROTOCOL_VERSION, 3, "P1: server half is v3");

    // register, full: exact value shape (description present).
    let full = qa_register("builder-1", "t0ken-not-a-secret", PROTOCOL_VERSION);
    assert_eq!(
        serde_json::to_value(&full).unwrap(),
        serde_json::json!({
            "type": "register",
            "endpoint_id": "builder-1",
            "description": "qa probe worker",
            "token": "t0ken-not-a-secret",
            "version": 3,
            "max_concurrent": 3,
        })
    );
    // register without description: the key is ABSENT (no null form).
    let bare = EndpointFrame::Register {
        endpoint_id: "builder-1".into(),
        description: None,
        token: RegisterToken::new("t0ken-not-a-secret".into()),
        version: PROTOCOL_VERSION,
        max_concurrent: 4,
        capabilities: Vec::new(),
    };
    let bare_value = serde_json::to_value(&bare).unwrap();
    assert_eq!(
        bare_value,
        serde_json::json!({
            "type": "register",
            "endpoint_id": "builder-1",
            "token": "t0ken-not-a-secret",
            "version": 3,
            "max_concurrent": 4,
        })
    );
    assert!(bare_value.get("description").is_none());

    // bye: identical bytes both directions.
    assert_eq!(
        serde_json::to_value(&EndpointFrame::Bye).unwrap(),
        serde_json::json!({"type": "bye"})
    );

    // rejected: minimal refusal omits detail; reason is snake_case.
    assert_eq!(
        serde_json::to_value(&ServerFrame::Rejected {
            reason: RefusalReason::BadToken,
            detail: None,
        })
        .unwrap(),
        serde_json::json!({"type": "rejected", "reason": "bad_token"})
    );
    assert_eq!(
        serde_json::to_value(&ServerFrame::Rejected {
            reason: RefusalReason::VersionMismatch,
            detail: Some("server speaks KiandProto 3; worker announced 0".into()),
        })
        .unwrap(),
        serde_json::json!({
            "type": "rejected",
            "reason": "version_mismatch",
            "detail": "server speaks KiandProto 3; worker announced 0",
        })
    );
    for (reason, wire) in [
        (RefusalReason::VersionMismatch, "version_mismatch"),
        (RefusalReason::BadToken, "bad_token"),
        (RefusalReason::DuplicateStatic, "duplicate_static"),
        (RefusalReason::IdConflict, "id_conflict"),
        (RefusalReason::Disabled, "disabled"),
    ] {
        let encoded = format!("{{\"type\":\"rejected\",\"reason\":\"{wire}\"}}",);
        let decoded = decode_server_frame(&encoded).unwrap();
        assert_eq!(
            decoded,
            ServerFrame::Rejected {
                reason,
                detail: None
            },
            "{wire} decodes typed"
        );
        // Unknown reason strings never decode (closed set, no fallback).
        assert!(
            decode_server_frame(&format!("{{\"type\":\"rejected\",\"reason\":\"{wire}x\"}}"))
                .is_err(),
            "unknown reason must not decode"
        );
    }

    // welcome keeps the null heartbeat_ms form (frozen v0 byte shape).
    assert_eq!(
        serde_json::to_value(&ServerFrame::Welcome {
            v: PROTOCOL_VERSION,
            heartbeat_ms: None,
        })
        .unwrap(),
        serde_json::json!({"type": "welcome", "v": 3, "heartbeat_ms": null})
    );

    // encode → decode line round-trip over every endpoint-frame variant.
    let samples = vec![
        full,
        bare,
        EndpointFrame::Bye,
        EndpointFrame::Hello {
            v: PROTOCOL_VERSION,
            endpoint_id: "dev".into(),
            max_concurrent: 4,
        },
        EndpointFrame::Heartbeat {
            endpoint_id: "dev".into(),
            ts_ms: 7,
            inflight: 2,
        },
        EndpointFrame::command_ok(Uuid::nil(), qa_ok_result()),
        EndpointFrame::command_failed(
            Uuid::nil(),
            kian_exec::proto::CommandError {
                code: kian_exec::proto::ErrorCode::Timeout,
                message: "execution timed out".into(),
            },
        ),
    ];
    for frame in samples {
        let line = encode_endpoint_frame(&frame);
        assert_eq!(&decode_endpoint_frame(line.trim_end()).unwrap(), &frame);
    }
    let server_samples = vec![
        ServerFrame::Welcome {
            v: PROTOCOL_VERSION,
            heartbeat_ms: Some(10_000),
        },
        ServerFrame::Welcome {
            v: PROTOCOL_VERSION,
            heartbeat_ms: None,
        },
        ServerFrame::Command {
            id: Uuid::nil(),
            cmd: qa_cmd(&["ls"]),
        },
        ServerFrame::Ping,
        ServerFrame::Bye,
        ServerFrame::Rejected {
            reason: RefusalReason::Disabled,
            detail: None,
        },
    ];
    for frame in server_samples {
        let line = encode_server_frame(&frame);
        assert_eq!(&decode_server_frame(line.trim_end()).unwrap(), &frame);
    }
}

/// P3: the token type redacts on every accidental print path — the
/// `{:?}` of a whole register frame never carries the value.
#[test]
fn qa_register_token_redacts_on_every_print_path() {
    let token = RegisterToken::new("qa-secret-value-do-not-print".into());
    assert!(!format!("{token:?}").contains("qa-secret"));
    assert!(!format!("{token}").contains("qa-secret"));
    assert!(format!("{token:?}").contains("<redacted>"));
    let frame = qa_register("w", "qa-secret-value-do-not-print", PROTOCOL_VERSION);
    let debug = format!("{frame:?}");
    assert!(!debug.contains("qa-secret"), "frame debug leaks: {debug}");
    // Serialization still carries the bare value (it must — the wire).
    assert!(encode_endpoint_frame(&frame).contains("qa-secret-value-do-not-print"));
    // The server-side WorkerToken redacts the same way.
    let server = WorkerToken::new("qa-secret-value-do-not-print".into());
    assert!(!format!("{server:?}").contains("qa-secret"));
    assert!(!format!("{server}").contains("qa-secret"));
}

// ---------------------------------------------------------------------------
// L1: the register refusal matrix over a real in-process listener
// ---------------------------------------------------------------------------

#[tokio::test]
async fn qa_register_refusal_matrix_and_welcome_cadence() {
    let clock = Arc::new(ManualClock::new(1_000_000));
    let (_listener, addr, directory) = qa_listener(clock, vec!["qa-static-pin"]).await;

    // bad token: typed refusal, no detail, connection closed.
    let mut client = QaClient::connect(addr).await;
    client
        .send(&qa_register("qa-w1", "wrong-token", PROTOCOL_VERSION))
        .await;
    match client.recv().await {
        Some(ServerFrame::Rejected {
            reason: RefusalReason::BadToken,
            detail: None,
        }) => {}
        other => panic!("bad_token must be a typed minimal refusal, got {other:?}"),
    }
    client.recv_eof_expected().await;
    assert!(directory.snapshot().is_empty(), "no phantom member");

    // version 0 (the v0 peer): typed terminal refusal with the detail
    // template, never a crash, never a half-connection.
    let mut client = QaClient::connect(addr).await;
    client.send(&qa_register("qa-w1", QA_TOKEN, 0)).await;
    match client.recv().await {
        Some(ServerFrame::Rejected {
            reason: RefusalReason::VersionMismatch,
            detail: Some(text),
        }) => assert_eq!(
            text, "server speaks KiandProto 3; worker announced 0",
            "P10: fixed template, versions only"
        ),
        other => panic!("version_mismatch must be typed, got {other:?}"),
    }
    client.recv_eof_expected().await;

    // static pin collision: duplicate_static, static entry stays the pin.
    let mut client = QaClient::connect(addr).await;
    client
        .send(&qa_register("qa-static-pin", QA_TOKEN, PROTOCOL_VERSION))
        .await;
    match client.recv().await {
        Some(ServerFrame::Rejected {
            reason: RefusalReason::DuplicateStatic,
            ..
        }) => {}
        other => panic!("duplicate_static must be typed, got {other:?}"),
    }
    client.recv_eof_expected().await;

    // malformed id (control character): id_conflict (the shape defence).
    let mut client = QaClient::connect(addr).await;
    client
        .send(&qa_register("bad\u{1}id", QA_TOKEN, PROTOCOL_VERSION))
        .await;
    match client.recv().await {
        Some(ServerFrame::Rejected {
            reason: RefusalReason::IdConflict,
            ..
        }) => {}
        other => panic!("malformed id must answer id_conflict, got {other:?}"),
    }
    client.recv_eof_expected().await;

    // protocol violation: a non-register first frame closes with NO
    // reply frame and no half-registered state (P7).
    let mut client = QaClient::connect(addr).await;
    client
        .send(&EndpointFrame::Heartbeat {
            endpoint_id: "qa-w1".into(),
            ts_ms: 1,
            inflight: 0,
        })
        .await;
    client.recv_eof_expected().await;
    assert!(directory.snapshot().is_empty(), "still no phantom member");

    // the honest path: welcome at v1 with the P6 cadence
    // max(1, offline_after_ms / 3) = 10_000 for 30_000.
    let mut client = QaClient::connect(addr).await;
    client
        .send(&qa_register("qa-w1", QA_TOKEN, PROTOCOL_VERSION))
        .await;
    match client.recv().await {
        Some(ServerFrame::Welcome {
            v: PROTOCOL_VERSION,
            heartbeat_ms: Some(10_000),
        }) => {}
        other => panic!("welcome must carry the server-directed cadence, got {other:?}"),
    }
    let snapshot = directory.snapshot();
    assert_eq!(snapshot.len(), 1);
    let member = &snapshot["qa-w1"];
    assert!(member.connected);
    assert_eq!(member.generation, 1);
    assert_eq!(member.max_concurrent, 3);
    assert_eq!(
        kian_exec::membership::member_state(member, 1_000_000, 30_000),
        MemberState::Online
    );

    // A second register during steady state is a protocol violation:
    // the connection closes (never a half re-registration).
    client
        .send(&qa_register("qa-w1", QA_TOKEN, PROTOCOL_VERSION))
        .await;
    client.recv_eof_expected().await;
}

/// P8 + P11 through the live listener: `max_concurrent == 0` clamps
/// (accepted, warned); an invalid description drops with the
/// registration kept.
#[tokio::test]
async fn qa_clamp_and_description_drop_are_non_fatal() {
    let clock = Arc::new(ManualClock::new(1_000_000));
    let (_listener, addr, directory) = qa_listener(clock, vec![]).await;

    let mut client = QaClient::connect(addr).await;
    client
        .send(&EndpointFrame::Register {
            endpoint_id: "qa-w0".into(),
            description: Some("bad\u{7}description".into()),
            token: RegisterToken::new(QA_TOKEN.into()),
            version: PROTOCOL_VERSION,
            max_concurrent: 0,
            capabilities: Vec::new(),
        })
        .await;
    match client.recv().await {
        Some(ServerFrame::Welcome {
            v: PROTOCOL_VERSION,
            heartbeat_ms: Some(10_000),
        }) => {}
        other => panic!("registration must succeed, got {other:?}"),
    }
    let member = directory.snapshot()["qa-w0"].clone();
    assert_eq!(member.max_concurrent, 1, "0 clamps to 1 (P8)");
    assert_eq!(
        member.description, None,
        "invalid description dropped (P11)"
    );
}

// ---------------------------------------------------------------------------
// L1: token compare semantics + the constant-time source anchor
// ---------------------------------------------------------------------------

/// The semantic half: accept only exact equality; every near-miss shape
/// (first byte, last byte, prefix, length, empty) rejects.
#[test]
fn qa_constant_time_compare_semantics() {
    let secret = QA_TOKEN.as_bytes();
    assert!(constant_time_eq(secret, secret));
    assert!(constant_time_eq(b"", b""));
    assert!(!constant_time_eq(secret, b""));
    assert!(!constant_time_eq(b"", secret));
    // First-byte difference (the classic early-exit fast path).
    let mut first = secret.to_vec();
    first[0] ^= 1;
    assert!(!constant_time_eq(secret, &first));
    // LAST-byte difference (what an early-exit compare would accept if
    // it only scanned a prefix).
    let mut last = secret.to_vec();
    let final_index = last.len() - 1;
    last[final_index] ^= 1;
    assert!(!constant_time_eq(secret, &last));
    // Equal-length, differs in the middle.
    let mut mid = secret.to_vec();
    let mid_index = last.len() / 2;
    mid[mid_index] ^= 1;
    assert!(!constant_time_eq(secret, &mid));
    // Shared prefix, different lengths.
    assert!(!constant_time_eq(secret, &secret[..secret.len() - 1]));
    assert!(!constant_time_eq(secret, &[secret, b"x"].concat()));
}

/// The structural half (the red-witness anchor for the constant-time
/// pillar): timing is not deterministically observable in process, so
/// the compare's constant-time property is asserted structurally — the
/// fixed-scan accumulator must exist in the accept path's compare and
/// no early-exit comparison may be introduced inside it. If the
/// implementation is swapped for a `==`/early-exit compare this probe
/// goes red.
#[test]
fn qa_constant_time_compare_source_anchor() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/src/registration.rs");
    let source = std::fs::read_to_string(path).expect("registration.rs readable");
    let start = source
        .find("pub fn constant_time_eq")
        .expect("constant_time_eq declared");
    let body = &source[start
        ..source[start..]
            .find("\n}")
            .map(|end| start + end + 2)
            .unwrap_or(source.len())];
    assert!(
        body.contains("diff |= x ^ y"),
        "the constant-time accumulator must fold every byte (got: {body})"
    );
    assert!(
        !body.contains("return false"),
        "an early-exit return inside constant_time_eq is a timing oracle"
    );
    // The accept path must route the token through the compare.
    assert!(
        source[start..].contains("constant_time_eq(") || source.contains("constant_time_eq(\n"),
        "the accept path calls the compare"
    );
    assert!(
        source.matches("constant_time_eq(").count() >= 2,
        "declaration + at least one call site"
    );
}

// ---------------------------------------------------------------------------
// L1: the per-peer failed-auth window bound (P9, clock injected)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn qa_failed_auth_window_bound() {
    let clock = Arc::new(ManualClock::new(1_000_000));
    let (_listener, addr, _directory) = qa_listener(clock.clone(), vec![]).await;

    // Five refused attempts fill the window (each is one connection,
    // one attempt — the server closes after a refusal).
    for _ in 0..5 {
        let mut client = QaClient::connect(addr).await;
        client
            .send(&qa_register("qa-w", "wrong", PROTOCOL_VERSION))
            .await;
        match client.recv().await {
            Some(ServerFrame::Rejected {
                reason: RefusalReason::BadToken,
                ..
            }) => {}
            other => panic!("refused, got {other:?}"),
        }
        client.recv_eof_expected().await;
    }

    // The 6th connection from the same peer: closed WITHOUT reading a
    // frame — no refusal reply at all.
    let mut client = QaClient::connect(addr).await;
    client
        .send(&qa_register("qa-w", "wrong", PROTOCOL_VERSION))
        .await;
    client.recv_eof_expected().await;

    // Same peer, now the CORRECT token: still rate-limited inside the
    // window (the bound is per address, not per token value).
    let mut client = QaClient::connect(addr).await;
    client
        .send(&qa_register("qa-w", QA_TOKEN, PROTOCOL_VERSION))
        .await;
    client.recv_eof_expected().await;

    // Past the 60 s window the address is released (clock injected).
    clock.advance_to(1_000_000 + 61_000);
    let mut client = QaClient::connect(addr).await;
    client
        .send(&qa_register("qa-w", QA_TOKEN, PROTOCOL_VERSION))
        .await;
    match client.recv().await {
        Some(ServerFrame::Welcome {
            v: PROTOCOL_VERSION,
            ..
        }) => {}
        other => panic!("window expired: good token registers, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// L1: the fail-closed bind policy matrix (C2/C3)
// ---------------------------------------------------------------------------

#[test]
fn qa_bind_policy_matrix() {
    let private = [
        "127.0.0.1:7400",
        "127.8.8.8:7400",       // whole 127/8
        "10.0.0.5:7400",        // RFC1918 10/8
        "172.16.0.1:7400",      // RFC1918 172.16/12 low edge
        "172.31.255.255:7400",  // 172.16/12 high edge
        "192.168.1.4:7400",     // RFC1918 192.168/16
        "100.64.0.1:7400",      // CGNAT low edge
        "100.127.255.255:7400", // CGNAT high edge
        "[::1]:7400",           // IPv6 loopback
        "[fd12:3456::1]:7400",  // IPv6 ULA (fc00::/7)
        "[fc00::1]:7400",
    ];
    for text in private {
        let addr: SocketAddr = text.parse().unwrap();
        assert!(is_private_bind_addr(addr), "{text} is private");
        assert!(validate_bind_addr(addr, false).is_ok(), "{text} accepted");
    }
    let public = [
        "8.8.8.8:7400",
        "1.2.3.4:7400",
        "172.32.0.1:7400",  // just outside 172.16/12
        "100.128.0.1:7400", // just outside CGNAT
        "0.0.0.0:7400",     // wildcard counts public (fail-closed)
        "[::]:7400",        // wildcard counts public
        "[fe80::1]:7400",   // link-local counts public
        "[2606:4700::1]:7400",
    ];
    for text in public {
        let addr: SocketAddr = text.parse().unwrap();
        assert!(!is_private_bind_addr(addr), "{text} is public");
        let err = validate_bind_addr(addr, false).unwrap_err();
        assert!(
            err.contains("public"),
            "{text} refusal names the policy: {err}"
        );
        // The explicit override unblocks it.
        assert!(
            validate_bind_addr(addr, true).is_ok(),
            "{text} + allow_public"
        );
    }

    // RegistrationConfig::validate: blank token and offline_after_ms == 0
    // refuse fail-closed before any socket opens.
    let mut config = RegistrationConfig::new(
        "127.0.0.1:7400".parse().unwrap(),
        WorkerToken::new("   ".into()),
        30_000,
    );
    assert!(config.validate().is_err(), "blank token refuses");
    config.token = Some(WorkerToken::new(QA_TOKEN.into()));
    config.offline_after_ms = 0;
    assert!(config.validate().is_err(), "offline_after_ms == 0 refuses");
    config.offline_after_ms = 30_000;
    config.bind = "8.8.8.8:7400".parse().unwrap();
    assert!(config.validate().is_err(), "public bind refuses");
    config.allow_public = true;
    assert!(config.validate().is_ok(), "explicit override accepts");
}

// ---------------------------------------------------------------------------
// L1: membership state edges through the live listener (spec §3.2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn qa_membership_edges_replace_bye_and_freshness() {
    let clock = Arc::new(ManualClock::new(1_000_000));
    let (listener, addr, directory) = qa_listener(clock.clone(), vec![]).await;

    // Generation 1 joins.
    let mut a = QaClient::connect(addr).await;
    a.send(&qa_register("qa-w", QA_TOKEN, PROTOCOL_VERSION))
        .await;
    a.recv().await.expect("welcome gen 1");

    // Heartbeat freshness drives the computed state (M3): connected but
    // stale reads offline; a heartbeat brings it back.
    clock.advance_to(1_000_000 + 31_000);
    assert_eq!(
        listener.member_state("qa-w", clock.now_ms()).unwrap(),
        MemberState::Offline,
        "connected but stale computes offline (M3)"
    );
    a.send(&EndpointFrame::Heartbeat {
        endpoint_id: "qa-w".into(),
        ts_ms: 0, // the wire ts is ignored for dynamic liveness (M10)
        inflight: 0,
    })
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        listener.member_state("qa-w", clock.now_ms()).unwrap(),
        MemberState::Online,
        "a heartbeat refreshes on the server clock (M10)"
    );

    // A newer registration for the same id REPLACES the incumbent: the
    // old connection receives a server bye, the generation increments.
    let mut b = QaClient::connect(addr).await;
    b.send(&qa_register("qa-w", QA_TOKEN, PROTOCOL_VERSION))
        .await;
    match b.recv().await {
        Some(ServerFrame::Welcome {
            v: PROTOCOL_VERSION,
            ..
        }) => {}
        other => panic!("replacement registers, got {other:?}"),
    }
    match a.recv().await {
        Some(ServerFrame::Bye) => {}
        other => panic!("incumbent gets a server bye, got {other:?}"),
    }
    a.recv_eof_expected().await;
    let member = directory.snapshot()["qa-w"].clone();
    assert_eq!(member.generation, 2, "generation is monotone (M6)");
    assert!(member.connected, "the successor is live");

    // The replaced incumbent's late EOF cannot tear down its successor
    // (A15): dropping `a` again is a no-op — `b` stays online.
    drop(a);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        listener.member_state("qa-w", clock.now_ms()).unwrap(),
        MemberState::Online,
        "stale generation EOF does not touch the successor"
    );

    // Graceful bye: the member goes offline but is never pruned (M5).
    b.send(&EndpointFrame::Bye).await;
    b.recv_eof_expected().await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let member = directory.snapshot()["qa-w"].clone();
    assert!(!member.connected, "bye disconnects");
    assert!(
        directory.snapshot().contains_key("qa-w"),
        "no auto-prune (M5)"
    );

    // metrics: joined 2, replaced 1, left_bye 1.
    let metrics = listener.metrics();
    assert_eq!(metrics.joined.load(std::sync::atomic::Ordering::Relaxed), 2);
    assert_eq!(
        metrics.replaced.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        metrics.left_bye.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

// ---------------------------------------------------------------------------
// L1: queue-first (Q1–Q4) against a real sqlite store
// ---------------------------------------------------------------------------

/// A QA fake worker route: a channel-backed RegisteredTransport whose
/// "endpoint" is this test. The command stream is shared behind an
/// async mutex so `serve` and `assert_silent` observe the same channel.
struct QaFakeRoute {
    writer_rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<ServerFrame>>,
    inbound_tx: tokio::sync::mpsc::Sender<EndpointFrame>,
    _transport: Arc<kian_exec::route::RegisteredTransport>,
}

impl QaFakeRoute {
    fn build(id: &str) -> Self {
        let (writer_tx, writer_rx) = tokio::sync::mpsc::channel(16);
        let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(16);
        let worker = kian_exec::registration::RegisteredWorker {
            description: None,
            capabilities: Vec::new(),
            id: id.to_string(),
            generation: 1,
            peer_addr: "127.0.0.1:59999".parse().unwrap(),
            version: PROTOCOL_VERSION,
            max_concurrent: 2,
            writer: writer_tx.clone(),
            inbound: inbound_rx,
        };
        let transport = kian_exec::route::RegisteredTransport::new(worker);
        Self {
            writer_rx: tokio::sync::Mutex::new(writer_rx),
            inbound_tx,
            _transport: transport,
        }
    }

    /// Serve exactly `count` commands: receive each, answer ok.
    async fn serve(&self, count: usize, stdout: &str) {
        let mut writer_rx = self.writer_rx.lock().await;
        for _ in 0..count {
            let frame = tokio::time::timeout(Duration::from_secs(5), writer_rx.recv())
                .await
                .expect("command within 5s")
                .expect("route alive");
            let ServerFrame::Command { id, .. } = frame else {
                panic!("expected a command, got {frame:?}");
            };
            let mut result = qa_ok_result();
            result.stdout = stdout.to_string();
            self.inbound_tx
                .send(EndpointFrame::command_ok(id, result))
                .await
                .unwrap();
        }
    }

    /// No command may arrive within `ms` (cross-dispatch guard).
    async fn assert_silent(&self, ms: u64) {
        let mut writer_rx = self.writer_rx.lock().await;
        match tokio::time::timeout(Duration::from_millis(ms), writer_rx.recv()).await {
            Err(_) => {}
            Ok(frame) => panic!("unexpected dispatch: {frame:?}"),
        }
    }
}

#[tokio::test]
async fn qa_queue_first_park_drain_dedupe_no_cross_honest_timeout() {
    let dir = std::env::temp_dir().join(format!("kian-qa-exec-{}.sqlite", Uuid::new_v4()));
    let store = ExecStore::open(&dir).await.unwrap();
    let directory = WorkerDirectory::new();
    let backend = RemoteExecBackend::with_membership(
        store.clone(),
        Vec::new(), // zero static routes — workers: deployment shape
        DefaultRoute::Dynamic,
        RemoteExecConfig {
            queue_timeout: Duration::from_millis(600),
            poll_interval: Duration::from_millis(40),
            max_redrives: 1,
        },
        "qa-holder",
        Duration::from_secs(60),
        kian_core::secrets::SecretRedactor::patterns_only(),
        Some(MembershipConfig {
            directory: directory.clone(),
            offline_after_ms: 30_000,
        }),
    )
    .expect("zero static routes construct with membership (spec §4.1)");

    // (1) enqueue with NO route: the row parks pending; the caller is
    // parked too — never an error, never a dispatch elsewhere (Q1).
    let cmd1 = Uuid::new_v4();
    let backend1 = backend.clone();
    let waiter1 = tokio::spawn(async move {
        backend1
            .exec_tracked(cmd1, "qa-w1", qa_cmd(&["echo", "one"]), None)
            .await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    let row = store.get(cmd1).await.unwrap().expect("durable row");
    assert_eq!(row.status, QueueStatus::Pending, "parks pending");
    assert!(!waiter1.is_finished(), "caller parked");

    // (2) registration drains it: attach a route, the parked row
    // executes exactly once on that id.
    let route = Arc::new(QaFakeRoute::build("qa-w1"));
    backend
        .attach_route(
            "qa-w1",
            RouteTransport::Registered(route._transport.clone()),
            1,
        )
        .await;
    let route_c = route.clone();
    tokio::spawn(async move { route_c.serve(1, "qa-done").await });
    let result = waiter1
        .await
        .expect("task alive")
        .expect("drained after attach");
    assert_eq!(result.stdout, "qa-done");

    // (3) command-id dedupe: re-executing a settled id answers from the
    // row without a second dispatch (ADR-007).
    let route2 = Arc::new(QaFakeRoute::build("qa-w1"));
    backend
        .attach_route(
            "qa-w1",
            RouteTransport::Registered(route2._transport.clone()),
            2,
        )
        .await;
    route2.assert_silent(300).await;
    let replay = backend
        .exec_tracked(cmd1, "qa-w1", qa_cmd(&["echo", "one"]), None)
        .await
        .expect("settled id answers from the row");
    assert_eq!(replay.stdout, "qa-done");

    // (4) no cross-worker dispatch (Q4): a row for an id with no route
    // stays pending while a live route exists for another id.
    let cmd2 = Uuid::new_v4();
    let backend2 = backend.clone();
    let waiter2 = tokio::spawn(async move {
        backend2
            .exec_tracked(cmd2, "qa-w2", qa_cmd(&["echo", "two"]), None)
            .await
    });
    route2.assert_silent(250).await;
    let row = store.get(cmd2).await.unwrap().unwrap();
    assert_eq!(row.status, QueueStatus::Pending, "no route ⇒ parked");
    assert!(!waiter2.is_finished(), "caller parked for its own id");

    // (2b) attach for qa-w2 now: the parked row drains on the same id.
    let route3 = Arc::new(QaFakeRoute::build("qa-w2"));
    backend
        .attach_route(
            "qa-w2",
            RouteTransport::Registered(route3._transport.clone()),
            1,
        )
        .await;
    let route3_c = route3.clone();
    tokio::spawn(async move { route3_c.serve(1, "qa-done").await });
    let _ = waiter2
        .await
        .expect("task alive")
        .expect("parked row drains on its own id");

    // (5) honest timeout (Q1/Q3): no route ever appears for qa-w3 —
    // the caller gets the honest Timeout, and the error is NEVER the
    // old "no endpoint route registered".
    let err = backend
        .exec_tracked(Uuid::new_v4(), "qa-w3", qa_cmd(&["echo", "three"]), None)
        .await
        .unwrap_err();
    assert!(matches!(err, ExecError::Timeout), "honest timeout: {err}");
    assert!(!format!("{err}").contains("no endpoint route"));

    store.close().await;
    let _ = std::fs::remove_file(&dir);
}

/// The abandoned-row half of the honest timeout, inspected directly.
#[tokio::test]
async fn qa_timeout_abandons_the_row() {
    let dir = std::env::temp_dir().join(format!("kian-qa-exec-{}.sqlite", Uuid::new_v4()));
    let store = ExecStore::open(&dir).await.unwrap();
    let backend = RemoteExecBackend::with_membership(
        store.clone(),
        Vec::new(),
        DefaultRoute::Dynamic,
        RemoteExecConfig {
            queue_timeout: Duration::from_millis(200),
            poll_interval: Duration::from_millis(20),
            max_redrives: 1,
        },
        "qa-holder",
        Duration::from_secs(60),
        kian_core::secrets::SecretRedactor::patterns_only(),
        Some(MembershipConfig {
            directory: WorkerDirectory::new(),
            offline_after_ms: 30_000,
        }),
    )
    .unwrap();
    let id = Uuid::new_v4();
    let err = backend
        .exec_tracked(id, "qa-gone", qa_cmd(&["echo", "x"]), None)
        .await
        .unwrap_err();
    assert!(matches!(err, ExecError::Timeout));
    let row = store.get(id).await.unwrap().unwrap();
    assert_eq!(row.status, QueueStatus::Abandoned, "row honestly abandoned");
    // Outcome rows exist only for settled Done rows (the struct
    // contract): an abandoned row carries none.
    assert!(row.outcome.is_none());
    store.close().await;
    let _ = std::fs::remove_file(&dir);
}
