//! Golden wire tests: every typed SDK method round-trips
//! against a **scripted raw-TCP HTTP server** — no axum, no kian
//! crates — so the assertions are on the exact bytes that fly:
//!
//! - the request line and path match the frozen routes;
//! - the request body is byte-identical to `tests/fixtures/*.request.json`
//!   (the serialization-freeze discipline applied to the client);
//! - the canned response bytes (`*.response.json`) parse into the
//!   typed result structs.
//!
//! The SSE helper test pins the `kian`/`done`/`error` framing per,
//! and the error test proves the −32001/−32002/−32003/−32000 mapping
//! is matchable by code, never string-parsed.

use std::time::Duration;

use futures::StreamExt;
use kian_sdk::{
    CODE_INTERNAL, CODE_SESSION_BUSY, CODE_SESSION_TERMINAL, CODE_UNKNOWN_SESSION, ClientError,
    KianClient, SseFrame, WakeOutcome,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Fixed session id so golden fixture bytes are deterministic.
const SID: &str = "11111111-2222-3333-4444-555555555555";

fn fixture(name: &str) -> Vec<u8> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("fixture {name}: {e}"))
}

/// One scripted request/response exchange.
struct Exchange {
    request_line: &'static str,
    expect_body: Vec<u8>,
    respond_with: Vec<u8>,
}

/// Reads one full HTTP/1.1 request (headers + content-length body).
/// Returns (request line, content-type, body).
async fn read_request(stream: &mut TcpStream) -> (String, String, Vec<u8>) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        let n = stream.read(&mut chunk).await.expect("read request headers");
        assert!(n > 0, "client closed before sending a full request");
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let header = |name: &str| {
        head.lines()
            .find_map(|l| {
                let (k, v) = l.split_once(':')?;
                k.trim()
                    .eq_ignore_ascii_case(name)
                    .then(|| v.trim().to_string())
            })
            .unwrap_or_default()
    };
    let content_length: usize = header("content-length").parse().unwrap_or(0);
    while buf.len() < header_end + content_length {
        let n = stream.read(&mut chunk).await.expect("read request body");
        assert!(n > 0, "client closed mid-body");
        buf.extend_from_slice(&chunk[..n]);
    }
    let request_line = head.lines().next().unwrap_or_default().to_string();
    (
        request_line,
        header("content-type"),
        buf[header_end..].to_vec(),
    )
}

/// Serves the exchanges sequentially, one connection each (every reply
/// carries `connection: close`). The task panics — failing the test —
/// on any byte mismatch.
async fn serve(listener: TcpListener, exchanges: Vec<Exchange>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        for ex in exchanges {
            let (mut sock, _) = listener.accept().await.expect("accept");
            let (line, content_type, body) = read_request(&mut sock).await;
            assert_eq!(line, ex.request_line, "golden route mismatch (wire freeze)");
            assert!(
                content_type.starts_with("application/json"),
                "expected application/json, got {content_type}"
            );
            assert_eq!(
                body, ex.expect_body,
                "golden request body mismatch for `{line}` (serialization freeze)"
            );
            let head = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                ex.respond_with.len()
            );
            sock.write_all(head.as_bytes()).await.unwrap();
            sock.write_all(&ex.respond_with).await.unwrap();
            sock.shutdown().await.ok();
        }
    })
}

/// Serves one SSE stream: asserts the GET route, then writes raw frames
/// and closes (close-delimited body).
async fn serve_sse(listener: TcpListener, sid: &str, frames: String) {
    let (mut sock, _) = listener.accept().await.expect("accept");
    let (line, _ctype, body) = read_request(&mut sock).await;
    assert_eq!(line, format!("GET /sessions/{sid}/events HTTP/1.1"));
    assert_eq!(body, Vec::<u8>::new(), "SSE GET carries no body");
    let head = "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n";
    sock.write_all(head.as_bytes()).await.unwrap();
    sock.write_all(frames.as_bytes()).await.unwrap();
    sock.shutdown().await.ok();
}

async fn bind() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    (listener, base)
}

// ---------------------------------------------------------------------------
// Typed methods: golden request bytes + response parse
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_session_round_trips_golden_request_bytes() {
    let (listener, base) = bind().await;
    let server = serve(
        listener,
        vec![Exchange {
            request_line: "POST /rpc HTTP/1.1",
            expect_body: fixture("create_session.request.json"),
            respond_with: fixture("create_session.response.json"),
        }],
    )
    .await;
    let out = KianClient::new(&base).create_session().await.expect("call");
    assert_eq!(out.session, SID);
    server.await.unwrap();
}

#[tokio::test]
async fn submit_round_trips_golden_request_bytes() {
    let (listener, base) = bind().await;
    let server = serve(
        listener,
        vec![Exchange {
            request_line: "POST /rpc HTTP/1.1",
            expect_body: fixture("submit.request.json"),
            respond_with: fixture("submit.response.json"),
        }],
    )
    .await;
    let out = KianClient::new(&base)
        .submit(SID, json!({"text": "ship it"}), Some("k-1"))
        .await
        .expect("call");
    assert!(!out.deduplicated);
    server.await.unwrap();
}

#[tokio::test]
async fn steer_round_trips_golden_request_bytes() {
    let (listener, base) = bind().await;
    let server = serve(
        listener,
        vec![Exchange {
            request_line: "POST /rpc HTTP/1.1",
            expect_body: fixture("steer.request.json"),
            respond_with: fixture("steer.response.json"),
        }],
    )
    .await;
    let out = KianClient::new(&base)
        .steer(SID, json!("pivot now"), None)
        .await
        .expect("call");
    assert!(out.deduplicated);
    server.await.unwrap();
}

#[tokio::test]
async fn queue_round_trips_golden_request_bytes() {
    let (listener, base) = bind().await;
    let server = serve(
        listener,
        vec![Exchange {
            request_line: "POST /rpc HTTP/1.1",
            expect_body: fixture("queue.request.json"),
            respond_with: fixture("queue.response.json"),
        }],
    )
    .await;
    let out = KianClient::new(&base)
        .queue(SID, json!("next task"), None)
        .await
        .expect("call");
    assert!(!out.deduplicated);
    server.await.unwrap();
}

#[tokio::test]
async fn interrupt_round_trips_golden_request_bytes() {
    let (listener, base) = bind().await;
    let server = serve(
        listener,
        vec![Exchange {
            request_line: "POST /rpc HTTP/1.1",
            expect_body: fixture("interrupt.request.json"),
            respond_with: fixture("interrupt.response.json"),
        }],
    )
    .await;
    let out = KianClient::new(&base).interrupt(SID).await.expect("call");
    assert_eq!(out.outcome, "graceful");
    server.await.unwrap();
}

#[tokio::test]
async fn projection_round_trips_golden_request_bytes() {
    let (listener, base) = bind().await;
    let server = serve(
        listener,
        vec![Exchange {
            request_line: "POST /rpc HTTP/1.1",
            expect_body: fixture("projection.request.json"),
            respond_with: fixture("projection.response.json"),
        }],
    )
    .await;
    let out = KianClient::new(&base).projection(SID).await.expect("call");
    assert_eq!(out.state["phase"], "idle");
    assert_eq!(out.state["seq"], 7);
    server.await.unwrap();
}

#[tokio::test]
async fn wake_now_round_trips_golden_request_bytes() {
    let (listener, base) = bind().await;
    let server = serve(
        listener,
        vec![Exchange {
            request_line: "POST /ops/rpc HTTP/1.1",
            expect_body: fixture("wake_now.request.json"),
            respond_with: fixture("wake_now.response.json"),
        }],
    )
    .await;
    let out = KianClient::new(&base).wake_now(SID).await.expect("call");
    assert_eq!(out, WakeOutcome::Started);
    server.await.unwrap();
}

#[tokio::test]
async fn wake_now_folded_outcome_parses() {
    let (listener, base) = bind().await;
    let server = serve(
        listener,
        vec![Exchange {
            request_line: "POST /ops/rpc HTTP/1.1",
            expect_body: fixture("wake_now.request.json"),
            respond_with: br#"{"id":1,"jsonrpc":"2.0","result":{"outcome":"folded"}}"#.to_vec(),
        }],
    )
    .await;
    let out = KianClient::new(&base).wake_now(SID).await.expect("call");
    assert_eq!(out, WakeOutcome::Folded);
    server.await.unwrap();
}

#[tokio::test]
async fn wake_now_unknown_outcome_is_malformed_never_guessed() {
    let (listener, base) = bind().await;
    let server = serve(
        listener,
        vec![Exchange {
            request_line: "POST /ops/rpc HTTP/1.1",
            expect_body: fixture("wake_now.request.json"),
            respond_with: br#"{"id":1,"jsonrpc":"2.0","result":{"outcome":"shrug"}}"#.to_vec(),
        }],
    )
    .await;
    match KianClient::new(&base).wake_now(SID).await {
        Err(ClientError::Malformed(msg)) => assert!(msg.contains("shrug"), "{msg}"),
        other => panic!("expected Malformed, got {other:?}"),
    }
    server.await.unwrap();
}

#[tokio::test]
async fn recovery_budget_reset_round_trips_golden_request_bytes() {
    let (listener, base) = bind().await;
    let server = serve(
        listener,
        vec![Exchange {
            request_line: "POST /ops/rpc HTTP/1.1",
            expect_body: fixture("recovery_budget_reset.request.json"),
            respond_with: fixture("recovery_budget_reset.response.json"),
        }],
    )
    .await;
    let out = KianClient::new(&base)
        .recovery_budget_reset(SID, Some("operator override"))
        .await
        .expect("call");
    assert_eq!(out.seq, 9);
    assert_eq!(out.epoch, 2);
    assert_eq!(out.ts, 424242);
    server.await.unwrap();
}

#[tokio::test]
async fn recovery_budget_reset_without_reason_omits_the_field() {
    let (listener, base) = bind().await;
    // Golden: reason omitted when None — the server treats it as
    // absent, so the client must not send `"reason": null`.
    let mut expected = fixture("recovery_budget_reset.request.json");
    let removed = remove_json_field(&mut expected, b"\"reason\":\"operator override\",");
    assert!(removed, "fixture must carry the reason field to remove");
    let server = serve(
        listener,
        vec![Exchange {
            request_line: "POST /ops/rpc HTTP/1.1",
            expect_body: expected,
            respond_with: fixture("recovery_budget_reset.response.json"),
        }],
    )
    .await;
    let out = KianClient::new(&base)
        .recovery_budget_reset(SID, None)
        .await
        .expect("call");
    assert_eq!(out.seq, 9);
    server.await.unwrap();
}

#[tokio::test]
async fn mock_channel_send_round_trips_golden_request_bytes() {
    let (listener, base) = bind().await;
    let server = serve(
        listener,
        vec![Exchange {
            request_line: "POST /ops/rpc HTTP/1.1",
            expect_body: fixture("mock_channel_send.request.json"),
            respond_with: fixture("mock_channel_send.response.json"),
        }],
    )
    .await;
    let out = KianClient::new(&base)
        .mock_channel_send("mock-im", Some("pr-9"), json!({"text": "hello"}))
        .await
        .expect("call");
    assert_eq!(out.event_id, "evt-1");
    assert_eq!(out.channel, "mock-im");
    assert_eq!(out.topic.as_deref(), Some("pr-9"));
    server.await.unwrap();
}

/// Strips one exact field substring from a golden body (setup for the
/// omitted-reason variant).
fn remove_json_field(body: &mut Vec<u8>, field: &[u8]) -> bool {
    let Some(pos) = body.windows(field.len()).position(|w| w == field) else {
        return false;
    };
    body.drain(pos..pos + field.len());
    true
}

// ---------------------------------------------------------------------------
// SSE helper: kian / done / error framing per
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sse_helper_frames_kian_done_and_error_per_c13() {
    // Happy stream: two kian frames, keep-alive comment, done.
    let (listener, base) = bind().await;
    let stream = format!(
        "event: kian\ndata: {{\"seq\":1,\"kind\":\"session/created\",\"session\":\"{SID}\"}}\n\n\
         : keep-alive\n\n\
         event: kian\ndata: {{\"seq\":2,\"kind\":\"session/ended\",\"session\":\"{SID}\"}}\n\n\
         event: done\ndata: ended\n\n"
    );
    let server = tokio::spawn(serve_sse(listener, SID, stream));
    let client = KianClient::new(&base);
    let mut frames = client.events(SID).await.expect("sse open");
    let mut seen = Vec::new();
    while let Some(item) = tokio::time::timeout(Duration::from_secs(5), frames.next())
        .await
        .expect("sse frame timeout")
    {
        seen.push(item.expect("sse frame"));
        if matches!(seen.last(), Some(SseFrame::Done)) {
            break;
        }
    }
    server.await.unwrap();
    assert_eq!(seen.len(), 3, "keep-alive must be skipped: {seen:?}");
    match &seen[0] {
        SseFrame::Kian(ev) => {
            assert_eq!(ev["seq"], 1);
            assert_eq!(ev["kind"], "session/created");
        }
        other => panic!("expected kian frame, got {other:?}"),
    }
    assert_eq!(seen[2], SseFrame::Done);

    // Error stream: one error frame, then the server closes.
    let (listener, base) = bind().await;
    let server = tokio::spawn(serve_sse(
        listener,
        SID,
        "event: error\ndata: unknown session\n\n".to_string(),
    ));
    let mut frames = KianClient::new(&base).events(SID).await.expect("sse open");
    let item = tokio::time::timeout(Duration::from_secs(5), frames.next())
        .await
        .expect("sse frame timeout")
        .expect("stream ended before the error frame")
        .expect("frame");
    server.await.unwrap();
    assert_eq!(item, SseFrame::Error("unknown session".into()));
}

// ---------------------------------------------------------------------------
// Error mapping: matchable by code, never string-parsed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rpc_error_codes_are_matchable_never_string_parsed() {
    let cases: [(i64, String); 4] = [
        (CODE_UNKNOWN_SESSION, format!("unknown session \"{SID}\"")),
        (CODE_SESSION_BUSY, "session is draining".into()),
        (CODE_SESSION_TERMINAL, "session is terminal".into()),
        (CODE_INTERNAL, "boom".into()),
    ];
    let mut exchanges = Vec::new();
    for (code, message) in &cases {
        let body = serde_json::to_vec(&json!({
            "error": {"code": code, "message": message}
        }))
        .unwrap();
        exchanges.push(Exchange {
            request_line: "POST /rpc HTTP/1.1",
            expect_body: fixture("projection.request.json"),
            respond_with: body,
        });
    }
    let (listener, base) = bind().await;
    let server = serve(listener, exchanges).await;
    let client = KianClient::new(&base);

    let errs: [ClientError; 4] = [
        client.projection(SID).await.err().unwrap(),
        client.projection(SID).await.err().unwrap(),
        client.projection(SID).await.err().unwrap(),
        client.projection(SID).await.err().unwrap(),
    ];
    server.await.unwrap();

    // Pattern-match on the code constant — no message parsing.
    assert!(matches!(
        &errs[0],
        ClientError::Rpc { code, .. } if *code == CODE_UNKNOWN_SESSION
    ));
    assert!(matches!(
        &errs[1],
        ClientError::Rpc { code, .. } if *code == CODE_SESSION_BUSY
    ));
    assert!(matches!(
        &errs[2],
        ClientError::Rpc { code, .. } if *code == CODE_SESSION_TERMINAL
    ));
    assert!(matches!(
        &errs[3],
        ClientError::Rpc { code, .. } if *code == CODE_INTERNAL
    ));
    // Helper predicates agree.
    assert!(errs[0].is_unknown_session());
    assert!(errs[1].is_session_busy());
    assert!(errs[2].is_session_terminal());
    assert!(errs[3].is_internal());
}
