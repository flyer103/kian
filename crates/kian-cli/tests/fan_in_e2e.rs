//! Fan-in event-stream e2e (READ-MODEL P3, plan §4.5): the explicit
//! allowlist `GET /events?sessions=<uuid,...>[&kinds=<kind,...>]`
//! against the REAL assembled server — two-session interleave with
//! compound `id: <session>:<seq>`, tail-only start (no history replay),
//! the kind filter, the cap / wildcard / malformed 400s, the
//! unknown-id error frame that names and continues, terminal close,
//! per-session resume unaffected, the config gate, and the
//! `/metrics` `event_stream` gauges. Events are appended through a
//! direct store handle (the fan-in is a pure read surface; no drain
//! runs). `NO_PROXY` set per the AGENTS.md gotcha.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::{force_no_proxy, spawn_mock_provider, temp_dir, write_config};
use futures::StreamExt;
use kian_cli::server::Server;
use kian_core::events::{EndReason, Event, EventKind, SCHEMA_VERSION};
use kian_core::model::SessionId;
use kian_store::{AppendGuard, EventStore};

/// One parsed SSE frame (id kept raw: the fan-in id is `<session>:<seq>`).
#[derive(Debug, Clone, PartialEq)]
struct Frame {
    event: String,
    id: Option<String>,
    data: String,
}

#[derive(Default)]
struct FrameReader {
    buf: String,
}

impl FrameReader {
    fn feed(&mut self, chunk: &[u8]) -> Vec<Frame> {
        self.buf.push_str(&String::from_utf8_lossy(chunk));
        let mut out = Vec::new();
        while let Some(pos) = self.buf.find("\n\n") {
            let raw: String = self.buf.drain(..pos + 2).collect();
            if let Some(frame) = parse(&raw) {
                out.push(frame);
            }
        }
        out
    }
}

fn parse(raw: &str) -> Option<Frame> {
    let mut event = String::new();
    let mut id = None;
    let mut data = String::new();
    let mut saw_field = false;
    for line in raw.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        let (name, value) = match line.split_once(':') {
            Some((n, v)) => (n, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        match name {
            "event" => {
                event = value.to_owned();
                saw_field = true;
            }
            "id" => {
                id = Some(value.to_owned());
                saw_field = true;
            }
            "data" => {
                if !data.is_empty() {
                    data.push('\n');
                }
                data.push_str(value);
                saw_field = true;
            }
            _ => {}
        }
    }
    saw_field.then_some(Frame { event, id, data })
}

async fn rpc(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{base}/rpc"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .expect("rpc send");
    assert_eq!(resp.status(), 200);
    resp.json().await.expect("rpc json")
}

async fn spawn_server(config_path: &Path) -> (String, Arc<Server>) {
    let server = Arc::new(
        Server::from_config_file(config_path)
            .await
            .expect("assemble server")
            .0,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = server.router();
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    (format!("http://{addr}"), server)
}

/// Appends one event to `session` through a direct store handle and
/// returns its seq (the fan-in tests never run a drain: the stream is
/// a pure read surface).
async fn append(store: &EventStore, session: SessionId, kind: EventKind) -> u64 {
    let head = store.head(session).await.expect("head");
    let event = Event {
        seq: head.next_seq,
        session,
        epoch: head.epoch,
        ts: 1_762_000_000_000 + head.next_seq,
        kind,
        bytes: serde_json::json!({"e2e": head.next_seq}),
        version: SCHEMA_VERSION,
    };
    let seq = event.seq;
    store
        .append(
            session,
            event,
            AppendGuard {
                epoch: head.epoch,
                expected_seq: head.next_seq,
            },
        )
        .await
        .expect("append");
    seq
}

/// Collects fan-in frames from an already-open response stream until
/// `want` frames with `event: kian` have been seen or the deadline
/// passes (keep-alives are skipped by `parse`).
async fn collect_kian(
    stream: &mut (impl StreamExt<Item = reqwest::Result<Vec<u8>>> + Unpin),
    reader: &mut FrameReader,
    want: usize,
    deadline: Duration,
) -> Vec<Frame> {
    let mut seen = Vec::new();
    let until = tokio::time::Instant::now() + deadline;
    while seen.len() < want {
        let Some(chunk) = tokio::time::timeout_at(until, stream.next())
            .await
            .ok()
            .flatten()
        else {
            break;
        };
        let chunk = chunk.expect("sse chunk");
        for frame in reader.feed(&chunk) {
            if frame.event == "kian" {
                seen.push(frame);
            }
        }
    }
    seen
}

/// Wraps a response byte stream into owned chunks (`Vec<u8>`) so the
/// collector's item type stays nameable without a `bytes` dependency
/// (reqwest 0.12 does not re-export it).
fn byte_chunks(
    resp: reqwest::Response,
) -> impl futures::Stream<Item = reqwest::Result<Vec<u8>>> + Unpin {
    use futures::StreamExt as _;
    resp.bytes_stream()
        .map(|chunk| chunk.map(|bytes| bytes.to_vec()))
}

/// Opens `GET /events?…` and returns (status, response). `raw_query`
/// is appended verbatim.
async fn open_fan_in(
    client: &reqwest::Client,
    base: &str,
    raw_query: &str,
) -> (reqwest::StatusCode, Option<reqwest::Response>) {
    let resp = client
        .get(format!("{base}/events?{raw_query}"))
        .header("accept", "text/event-stream")
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .expect("fan-in open");
    let status = resp.status();
    if status.is_success() {
        (status, Some(resp))
    } else {
        (status, None)
    }
}

/// Two sessions interleave on one connection with compound ids; the
/// pre-connection history is NOT replayed (tail-only), and per session
/// the seq strictly increases.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fan_in_interleaves_two_sessions_tail_only() {
    force_no_proxy();
    let dir = temp_dir("fan-in-interleave");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:7341");

    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();
    let store = EventStore::open(&sqlite).await.expect("open store");

    // Two sessions, each with a pre-connect history beyond creation.
    let mut sessions = Vec::new();
    for _ in 0..2 {
        let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
        let sid = out["result"]["session"].as_str().expect("sid").to_owned();
        let sid: SessionId = SessionId(sid.parse().expect("uuid"));
        append(&store, sid, EventKind::InputAdmitted).await; // pre-connect seq 2
        sessions.push(sid);
    }
    let (a, b) = (sessions[0], sessions[1]);

    // Connect, then wait past one poll interval with nothing appended:
    // the pre-connect events (seq 2 of each) must never arrive.
    let query = format!("sessions={},{}", a.0, b.0);
    let (status, resp) = open_fan_in(&client, &base, &query).await;
    assert_eq!(status, 200);
    let resp = resp.expect("stream");
    let mut stream = byte_chunks(resp);
    let mut reader = FrameReader::default();
    let pre = collect_kian(&mut stream, &mut reader, 1, Duration::from_millis(400)).await;
    assert!(
        pre.is_empty(),
        "tail-only: pre-connect events must not be replayed, got {pre:?}"
    );

    // Interleave appends across the two sessions; both must arrive.
    append(&store, a, EventKind::TurnStart).await; // a:3
    append(&store, b, EventKind::TurnStart).await; // b:3
    append(&store, a, EventKind::TurnEnd).await; // a:4
    let frames = collect_kian(&mut stream, &mut reader, 3, Duration::from_secs(5)).await;
    assert_eq!(frames.len(), 3, "{frames:?}");
    let mut per_session: std::collections::BTreeMap<String, Vec<u64>> = Default::default();
    for frame in &frames {
        assert_eq!(frame.event, "kian");
        let id = frame.id.as_deref().expect("compound id");
        let (session, seq) = id.rsplit_once(':').expect("id: <session>:<seq>");
        let seq: u64 = seq.parse().expect("numeric seq");
        assert!(session == a.0.to_string() || session == b.0.to_string());
        let data: serde_json::Value = serde_json::from_str(&frame.data).expect("envelope JSON");
        assert_eq!(data["seq"].as_u64(), Some(seq), "id and data agree");
        per_session
            .entry(session.to_string())
            .or_default()
            .push(seq);
    }
    let a_seqs = per_session.get(&a.0.to_string()).expect("session A frames");
    assert_eq!(a_seqs, &vec![3, 4], "strictly increasing per session");
    assert_eq!(
        per_session.get(&b.0.to_string()).map(Vec::as_slice),
        Some(&[3u64][..]),
        "session B frames"
    );
}

/// The optional `kinds` allowlist filters frames server-side; the
/// stream closes once every watched session ends (terminal), even when
/// the end event itself is filtered out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fan_in_kind_filter_and_terminal_close() {
    force_no_proxy();
    let dir = temp_dir("fan-in-kinds");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:7342");

    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();
    let store = EventStore::open(&sqlite).await.expect("open store");
    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid: SessionId = SessionId(
        out["result"]["session"]
            .as_str()
            .expect("sid")
            .parse()
            .expect("uuid"),
    );

    let query = format!("sessions={}&kinds=input/admitted", sid.0);
    let (status, resp) = open_fan_in(&client, &base, &query).await;
    assert_eq!(status, 200);
    let resp = resp.expect("stream");
    let mut stream = byte_chunks(resp);
    let mut reader = FrameReader::default();

    append(&store, sid, EventKind::TurnStart).await; // filtered
    append(&store, sid, EventKind::InputAdmitted).await; // passes
    append(
        &store,
        sid,
        EventKind::SessionEnded {
            reason: EndReason::Completed,
        },
    )
    .await; // filtered, terminal

    let frames = collect_kian(&mut stream, &mut reader, 1, Duration::from_secs(5)).await;
    assert_eq!(frames.len(), 1, "{frames:?}");
    let data: serde_json::Value = serde_json::from_str(&frames[0].data).expect("envelope");
    assert_eq!(data["kind"], "input/admitted");

    // Every listed session ended: the server closes the stream (the
    // body ends) — there is no `done` frame on the fan-in.
    let mut ended = false;
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < until {
        match tokio::time::timeout_at(until, stream.next()).await {
            Ok(None) => {
                ended = true;
                break;
            }
            Ok(Some(chunk)) => {
                let chunk = chunk.expect("chunk");
                let unexpected = reader.feed(&chunk);
                assert!(
                    unexpected.is_empty(),
                    "no more frames expected after the filter, got {unexpected:?}"
                );
            }
            Err(_) => break,
        }
    }
    assert!(
        ended,
        "the fan-in must close once every listed session ended"
    );
}

/// Malformed and over-cap listings are HTTP 400 with a plain-text body;
/// there is no wildcard in OSS.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fan_in_refuses_malformed_over_cap_and_wildcard() {
    force_no_proxy();
    let dir = temp_dir("fan-in-400");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:7343");

    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();
    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid = out["result"]["session"].as_str().expect("sid").to_owned();

    let over_cap = {
        let ids: Vec<String> = (0..65)
            .map(|n| format!("00000000-0000-0000-0000-{n:012x}"))
            .collect();
        format!("sessions={}", ids.join(","))
    };
    let cases: Vec<(String, &str)> = vec![
        // Missing sessions entirely.
        (String::new(), "`sessions` is required"),
        // The wildcard is not a uuid and is refused by name.
        ("sessions=*".into(), "no wildcard"),
        // A non-uuid id.
        (format!("sessions={},not-a-uuid", sid), "not a uuid"),
        // Over-cap: 65 listed ids.
        (over_cap, "capped at 64"),
        // An unknown kind is malformed, never an empty filter.
        (
            format!("sessions={}&kinds=no/such-kind", sid),
            "not a known event kind",
        ),
    ];
    for (query, expected) in &cases {
        let resp = client
            .get(format!("{base}/events?{query}"))
            .header("accept", "text/event-stream")
            .send()
            .await
            .expect("get");
        assert_eq!(resp.status(), 400, "query `{query}`");
        let body = resp.text().await.expect("body");
        assert!(body.contains(expected), "query `{query}`: body `{body}`");
    }

    // Exactly at the cap is accepted (200, immediately closed only if
    // the sessions end — here unknown ids error and close).
    let at_cap: Vec<String> = (0..64)
        .map(|n| format!("00000000-0000-0000-0000-{n:012x}"))
        .collect();
    let (status, resp) =
        open_fan_in(&client, &base, &format!("sessions={}", at_cap.join(","))).await;
    assert_eq!(status, 200, "cap is inclusive");
    drop(resp);
}

/// An unknown listed session yields exactly ONE error frame naming it,
/// and the stream continues with the known sessions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fan_in_unknown_session_errors_once_and_continues() {
    force_no_proxy();
    let dir = temp_dir("fan-in-unknown");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:7344");

    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();
    let store = EventStore::open(&sqlite).await.expect("open store");
    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid: SessionId = SessionId(
        out["result"]["session"]
            .as_str()
            .expect("sid")
            .parse()
            .expect("uuid"),
    );
    let ghost = SessionId(uuid::Uuid::new_v4());

    let (status, resp) =
        open_fan_in(&client, &base, &format!("sessions={},{}", sid.0, ghost.0)).await;
    assert_eq!(status, 200);
    let resp = resp.expect("stream");
    let mut stream = byte_chunks(resp);
    let mut reader = FrameReader::default();

    // The error frame names the ghost exactly once, then events from
    // the known session keep flowing.
    append(&store, sid, EventKind::InputAdmitted).await;
    let mut error_frames = 0usize;
    let mut kian = 0usize;
    let until = tokio::time::Instant::now() + Duration::from_secs(5);
    while (error_frames == 0 || kian == 0) && tokio::time::Instant::now() < until {
        let Some(chunk) = tokio::time::timeout_at(until, stream.next())
            .await
            .ok()
            .flatten()
        else {
            break;
        };
        let chunk = chunk.expect("chunk");
        for frame in reader.feed(&chunk) {
            match frame.event.as_str() {
                "error" => {
                    error_frames += 1;
                    assert!(
                        frame.data.contains(&ghost.0.to_string()),
                        "the error frame must name the unknown id: {}",
                        frame.data
                    );
                }
                "kian" => kian += 1,
                other => panic!("unexpected frame {other}"),
            }
        }
    }
    assert_eq!(error_frames, 1, "exactly one error frame for the ghost");
    assert!(kian >= 1, "the known session's events still arrive");
}

/// The config gate: `api.event_stream.enabled: false` mounts no route
/// (404) and `/metrics` still reports the (zeroed) `event_stream` object.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fan_in_disabled_by_config_forty_fours() {
    force_no_proxy();
    let dir = temp_dir("fan-in-off");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:7345");
    let yaml = std::fs::read_to_string(&config).expect("config");
    std::fs::write(
        &config,
        yaml + "api:\n  event_stream:\n    enabled: false\n",
    )
    .expect("write");

    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "{base}/events?sessions=00000000-0000-0000-0000-000000000001"
        ))
        .send()
        .await
        .expect("get");
    assert_eq!(resp.status(), 404, "a disabled fan-in mounts no route");

    let metrics: serde_json::Value = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics")
        .json()
        .await
        .expect("metrics json");
    let event_stream = metrics.get("event_stream").expect("event_stream object");
    assert_eq!(event_stream["subscribers"], 0);
    assert_eq!(event_stream["sessions_watched"], 0);
    assert_eq!(event_stream["events_sent"], 0);
}

/// `/metrics` reports the live fan-in gauges while a connection is
/// open, and they settle back to zero once it drops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fan_in_metrics_track_the_open_connection() {
    force_no_proxy();
    let dir = temp_dir("fan-in-metrics");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:7346");

    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();
    let store = EventStore::open(&sqlite).await.expect("open store");
    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid: SessionId = SessionId(
        out["result"]["session"]
            .as_str()
            .expect("sid")
            .parse()
            .expect("uuid"),
    );

    let (status, resp) = open_fan_in(&client, &base, &format!("sessions={}", sid.0)).await;
    assert_eq!(status, 200);
    let mut stream = byte_chunks(resp.expect("stream"));
    let mut reader = FrameReader::default();

    let gauges = |client: &reqwest::Client, base: &str| {
        let client = client.clone();
        let base = base.to_string();
        async move {
            client
                .get(format!("{base}/metrics"))
                .send()
                .await
                .expect("metrics")
                .json::<serde_json::Value>()
                .await
                .expect("metrics json")
        }
    };
    // Subscribers and watched sessions appear once the stream is polled.
    let mut saw_open = false;
    for _ in 0..100 {
        let metrics = gauges(&client, &base).await;
        if metrics["event_stream"]["subscribers"].as_u64() == Some(1)
            && metrics["event_stream"]["sessions_watched"].as_u64() == Some(1)
        {
            saw_open = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(saw_open, "the open fan-in must show in /metrics");

    append(&store, sid, EventKind::InputAdmitted).await;
    let frames = collect_kian(&mut stream, &mut reader, 1, Duration::from_secs(5)).await;
    assert_eq!(frames.len(), 1);
    let mut saw_sent = false;
    for _ in 0..100 {
        let metrics = gauges(&client, &base).await;
        if metrics["event_stream"]["events_sent"].as_u64().unwrap_or(0) >= 1 {
            saw_sent = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(saw_sent, "events_sent must count emitted frames");

    // Drop the connection; the gauges settle back to zero (hyper drops
    // the body stream when the peer goes away; the guard's Drop does
    // the accounting).
    drop(stream);
    let mut settled = false;
    for _ in 0..600 {
        let metrics = gauges(&client, &base).await;
        if metrics["event_stream"]["subscribers"].as_u64() == Some(0)
            && metrics["event_stream"]["sessions_watched"].as_u64() == Some(0)
        {
            settled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(settled, "the gauges must settle to zero after disconnect");
}

/// Per-session resume is unaffected by the fan-in (READ-MODEL §4.1
/// stays the resume surface): a `Last-Event-ID` reconnect on
/// `/sessions/{id}/events` resumes after that seq and never re-emits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_session_stream_resume_unaffected_by_fan_in() {
    force_no_proxy();
    let dir = temp_dir("fan-in-resume");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_mock_provider(0).await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:7347");

    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();
    let store = EventStore::open(&sqlite).await.expect("open store");
    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid: SessionId = SessionId(
        out["result"]["session"]
            .as_str()
            .expect("sid")
            .parse()
            .expect("uuid"),
    );
    let seq2 = append(&store, sid, EventKind::InputAdmitted).await;
    assert_eq!(seq2, 2);

    // Open the fan-in first (proves both surfaces coexist), then the
    // resumed per-session stream.
    let (_status, fan) = open_fan_in(&client, &base, &format!("sessions={}", sid.0)).await;
    drop(fan);

    let resp = client
        .get(format!("{base}/sessions/{}/events", sid.0))
        .header("last-event-id", "1")
        .timeout(Duration::from_secs(10))
        .send()
        .await
        .expect("per-session sse open");
    assert_eq!(resp.status(), 200);
    let mut stream = byte_chunks(resp);
    let mut reader = FrameReader::default();
    let frames = collect_kian(&mut stream, &mut reader, 1, Duration::from_secs(5)).await;
    assert_eq!(frames.len(), 1, "{frames:?}");
    assert_eq!(frames[0].id.as_deref(), Some("2"), "resume after seq 1");
    let data: serde_json::Value = serde_json::from_str(&frames[0].data).expect("envelope");
    assert_eq!(data["kind"], "input/admitted");
}
