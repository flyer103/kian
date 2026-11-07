//! Read-path e2e (READ-MODEL P0, plan §4.1): versioned projection
//! reads (`version` / `since` / `unchanged`), SSE frames carrying
//! `id: <seq>`, `Last-Event-ID` resume across a reconnect with a
//! mid-session append, terminal replay, and the invalid-resume
//! refusals — against the REAL assembled server (scripted mock
//! provider over the openai-compat wire), with `NO_PROXY` set per the
//! AGENTS.md gotcha.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{
    force_no_proxy, spawn_scripted_provider, spawn_scripted_provider_gated_at, temp_dir,
    write_config,
};
use futures::StreamExt;
use kian_cli::server::Server;

/// One parsed SSE frame.
#[derive(Debug, Clone, PartialEq)]
struct Frame {
    event: String,
    id: Option<String>,
    data: String,
}

/// Incremental SSE frame parser: feed raw chunks, get complete frames.
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

/// Opens one SSE connection (optionally resuming after
/// `last_event_id`) and drains frames until the stream ends or the
/// deadline — whatever comes first. Returns every frame observed.
async fn drain_sse(
    client: &reqwest::Client,
    base: &str,
    sid: &str,
    last_event_id: Option<&str>,
    deadline: Duration,
) -> Vec<Frame> {
    let mut request = client
        .get(format!("{base}/sessions/{sid}/events"))
        .timeout(deadline + Duration::from_secs(5));
    if let Some(id) = last_event_id {
        request = request.header("last-event-id", id);
    }
    let resp = request.send().await.expect("sse open");
    assert_eq!(resp.status(), 200, "sse open status");
    let mut stream = resp.bytes_stream();
    let mut reader = FrameReader::default();
    let mut frames = Vec::new();
    let end = tokio::time::Instant::now() + deadline;
    loop {
        let chunk = match tokio::time::timeout_at(end, stream.next()).await {
            Err(_) => break, // deadline observed enough
            Ok(None) => break,
            Ok(Some(Err(e))) => panic!("sse chunk error: {e}"),
            Ok(Some(Ok(chunk))) => chunk,
        };
        let fresh = reader.feed(&chunk);
        let terminal = fresh
            .iter()
            .any(|f| f.event == "done" || f.event == "error");
        frames.extend(fresh);
        if terminal {
            break;
        }
    }
    frames
}

/// Creates a session, submits one input and waits (polling
/// `projection`) until the stream settles `completed`; returns
/// `(session id, final version)`.
async fn settle_session(client: &reqwest::Client, base: &str) -> (String, u64) {
    let out = rpc(client, base, "create_session", serde_json::json!({})).await;
    let sid = out["result"]["session"]
        .as_str()
        .expect("session id")
        .to_owned();
    let out = rpc(
        client,
        base,
        "submit",
        serde_json::json!({"session": sid, "content": "Say all done."}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "session never settled"
        );
        let out = rpc(
            client,
            base,
            "projection",
            serde_json::json!({"session": sid}),
        )
        .await;
        if out["result"]["state"] == "completed" {
            let version = out["result"]["version"].as_u64().expect("version present");
            return (sid, version);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn cleanup(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// §3 over the wire: `version` on every read, `since == version` →
/// `unchanged` with NO state, an older `since` → full read,
/// `since > version` → `-32602`, an event-less session stays
/// `-32001`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn projection_version_unchanged_delta_and_ahead_e2e() {
    force_no_proxy();
    let dir = temp_dir("read-path-projection");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) =
        spawn_scripted_provider(vec![common::ScriptedResponse::Text("All done.".into())], 0).await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:7331");
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    let (sid, version) = settle_session(&client, &base).await;
    assert!(version >= 3, "a settled session has at least created+ended");

    // Full read (no since): version + state.
    let out = rpc(
        &client,
        &base,
        "projection",
        serde_json::json!({"session": sid}),
    )
    .await;
    assert_eq!(out["result"]["version"].as_u64(), Some(version));
    assert_eq!(out["result"]["state"], "completed");

    // Unchanged: no state key at all (R3).
    let out = rpc(
        &client,
        &base,
        "projection",
        serde_json::json!({"session": sid, "since": version}),
    )
    .await;
    assert_eq!(
        out["result"],
        serde_json::json!({"version": version, "unchanged": true}),
        "the unchanged answer is exactly version+unchanged: {out}"
    );

    // An older since: the full read at the current version.
    let out = rpc(
        &client,
        &base,
        "projection",
        serde_json::json!({"session": sid, "since": version - 1}),
    )
    .await;
    assert_eq!(out["result"]["version"].as_u64(), Some(version));
    assert_eq!(out["result"]["state"], "completed");

    // Ahead of head: -32602, never a silent resync.
    let out = rpc(
        &client,
        &base,
        "projection",
        serde_json::json!({"session": sid, "since": version + 1}),
    )
    .await;
    assert_eq!(out["error"]["code"].as_i64(), Some(-32602), "{out}");

    // A non-u64 since is refused too.
    let out = rpc(
        &client,
        &base,
        "projection",
        serde_json::json!({"session": sid, "since": "seven"}),
    )
    .await;
    assert_eq!(out["error"]["code"].as_i64(), Some(-32602), "{out}");

    // An event-less session keeps its -32001 (unchanged behavior).
    let ghost = uuid::Uuid::new_v4().to_string();
    let out = rpc(
        &client,
        &base,
        "projection",
        serde_json::json!({"session": ghost, "since": 3}),
    )
    .await;
    assert_eq!(out["error"]["code"].as_i64(), Some(-32001), "{out}");

    cleanup(&dir);
}

/// §4.1 over the wire: `kian` frames carry `id: <seq>`; a terminal
/// stream replays then closes with `done`; `Last-Event-ID` resumes
/// after that seq without re-emitting anything earlier; an ahead or
/// non-numeric resume id is `event: error` + close.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_ids_terminal_replay_and_resume_refusals_e2e() {
    force_no_proxy();
    let dir = temp_dir("read-path-sse");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) =
        spawn_scripted_provider(vec![common::ScriptedResponse::Text("All done.".into())], 0).await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:7331");
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    let (sid, version) = settle_session(&client, &base).await;

    // Full replay: every kian frame carries id == its seq, strictly
    // increasing, ending with done.
    let frames = drain_sse(&client, &base, &sid, None, Duration::from_secs(10)).await;
    let kian: Vec<&Frame> = frames.iter().filter(|f| f.event == "kian").collect();
    assert!(!kian.is_empty(), "the replay must carry events: {frames:?}");
    let mut prev = 0u64;
    for frame in &kian {
        let id: u64 = frame
            .id
            .as_deref()
            .expect("kian frame id")
            .parse()
            .expect("numeric id");
        let ev: serde_json::Value = serde_json::from_str(&frame.data).expect("kian json");
        assert_eq!(
            ev["seq"].as_u64(),
            Some(id),
            "the frame id must equal the event seq"
        );
        assert!(id > prev, "per-connection ids strictly increase");
        prev = id;
    }
    assert_eq!(prev, version, "the replay ends at the head version");
    assert_eq!(frames.last().map(|f| f.event.as_str()), Some("done"));

    // Resume at head: nothing left to replay — the terminal stream
    // answers done immediately, re-emitting nothing.
    let frames = drain_sse(
        &client,
        &base,
        &sid,
        Some(&version.to_string()),
        Duration::from_secs(10),
    )
    .await;
    assert!(
        frames.iter().all(|f| f.event != "kian"),
        "a resume at head must not re-emit events: {frames:?}"
    );
    assert_eq!(frames.last().map(|f| f.event.as_str()), Some("done"));

    // Resume mid-stream: exactly the events after seq 1.
    let frames = drain_sse(&client, &base, &sid, Some("1"), Duration::from_secs(10)).await;
    let kian: Vec<&Frame> = frames.iter().filter(|f| f.event == "kian").collect();
    assert!(!kian.is_empty());
    let first_id: u64 = kian[0].id.as_deref().expect("id").parse().expect("numeric");
    assert_eq!(first_id, 2, "resume starts at seq+1, never re-emits");
    assert!(kian.iter().all(|f| f.id.as_deref() != Some("1")));

    // Ahead of head: one error frame, then close.
    let ahead = version + 5;
    let frames = drain_sse(
        &client,
        &base,
        &sid,
        Some(&ahead.to_string()),
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(
        frames.len(),
        1,
        "ahead resume: exactly one error frame: {frames:?}"
    );
    assert_eq!(frames[0].event, "error");
    assert!(frames[0].data.contains("ahead"), "{}", frames[0].data);

    // Non-numeric: one error frame, then close.
    let frames = drain_sse(&client, &base, &sid, Some("abc"), Duration::from_secs(10)).await;
    assert_eq!(frames.len(), 1, "non-numeric resume: {frames:?}");
    assert_eq!(frames[0].event, "error");

    cleanup(&dir);
}

/// §4.1 mid-session: the connection drops mid-drain (the provider
/// turn is held by a gate), the reconnect resumes after its last seen
/// seq, the held turn then completes (a mid-session append), and the
/// resumed stream delivers exactly the new events — never re-emitting
/// the pre-drop prefix.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_resume_across_reconnect_with_mid_session_append_e2e() {
    force_no_proxy();
    let dir = temp_dir("read-path-resume");
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, mock) = spawn_scripted_provider_gated_at(
        vec![common::ScriptedResponse::Text(
            "All done after the drop.".into(),
        )],
        0,
        1, // hold request 1 (the first provider turn) until released
    )
    .await;
    let config = write_config(&dir, &sqlite, &provider_base, "127.0.0.1:7331");
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid = out["result"]["session"]
        .as_str()
        .expect("session id")
        .to_owned();
    let out = rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({"session": sid, "content": "Say all done after the drop."}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false);

    // Connection #1: read frames until the drain reaches the held
    // provider turn (turn/start observed while the request is gated).
    let stream = client
        .get(format!("{base}/sessions/{sid}/events"))
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("sse open #1");
    assert_eq!(stream.status(), 200);
    let mut byte_stream = stream.bytes_stream();
    let mut reader = FrameReader::default();
    let mut frames: Vec<Frame> = Vec::new();
    let mut last_seq = 0u64;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    // Poll with short chunk timeouts so the break condition is
    // re-evaluated even while the held turn emits nothing.
    loop {
        assert!(
            tokio::time::Instant::now() < deadline,
            "turn/start never arrived"
        );
        let held =
            mock.requests.load(Ordering::SeqCst) >= 1 && mock.delivered.load(Ordering::SeqCst) < 1;
        if held && frames.iter().any(|f| f.data.contains("turn/start")) {
            break; // the drain is parked inside the held turn
        }
        match tokio::time::timeout(Duration::from_millis(100), byte_stream.next()).await {
            Err(_) => continue, // nothing new; re-check the gate
            Ok(None) => panic!("sse ended early #1"),
            Ok(Some(Err(e))) => panic!("sse chunk error #1: {e}"),
            Ok(Some(Ok(chunk))) => {
                for frame in reader.feed(&chunk) {
                    if frame.event == "kian" {
                        last_seq = frame.id.as_deref().expect("id").parse().expect("numeric");
                    }
                    frames.push(frame);
                }
            }
        }
    }
    let pre_drop: Vec<String> = frames
        .iter()
        .filter(|f| f.event == "kian")
        .filter_map(|f| f.id.clone())
        .collect();
    assert!(
        last_seq >= 3,
        "created+admitted+claimed at least: {pre_drop:?}"
    );
    drop(byte_stream); // connection #1 dies mid-drain (response dropped with it)

    // Connection #2: resume after the last seen seq.
    let stream2 = client
        .get(format!("{base}/sessions/{sid}/events"))
        .header("last-event-id", last_seq.to_string())
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("sse open #2");
    assert_eq!(stream2.status(), 200);

    // Release the held provider turn — the mid-session append.
    mock.released.store(true, Ordering::SeqCst);

    let mut byte_stream2 = stream2.bytes_stream();
    let mut reader2 = FrameReader::default();
    let mut resumed = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let chunk = match tokio::time::timeout_at(deadline, byte_stream2.next()).await {
            Err(_) => panic!("resumed stream never finished: {resumed:?}"),
            Ok(None) => break,
            Ok(Some(Err(e))) => panic!("sse chunk error #2: {e}"),
            Ok(Some(Ok(chunk))) => chunk,
        };
        let fresh = reader2.feed(&chunk);
        let done = fresh
            .iter()
            .any(|f| f.event == "done" || f.event == "error");
        resumed.extend(fresh);
        if done {
            break;
        }
    }

    // The resumed connection delivered ONLY post-drop events, strictly
    // increasing past the resume point.
    let kian: Vec<&Frame> = resumed.iter().filter(|f| f.event == "kian").collect();
    assert!(
        !kian.is_empty(),
        "the finished turn's events must arrive: {resumed:?}"
    );
    let ids: Vec<u64> = kian
        .iter()
        .map(|f| {
            f.id.as_deref()
                .expect("id")
                .parse::<u64>()
                .expect("numeric")
        })
        .collect();
    assert!(
        ids.iter().all(|id| *id > last_seq),
        "no re-emitted pre-drop events: resume was {last_seq}, got {ids:?}"
    );
    assert!(
        ids.windows(2).all(|w| w[0] < w[1]),
        "strictly increasing on the resumed connection: {ids:?}"
    );
    assert_eq!(resumed.last().map(|f| f.event.as_str()), Some("done"));

    // And the projection agrees: a read at the pre-drop version is
    // changed and now settled.
    let out = rpc(
        &client,
        &base,
        "projection",
        serde_json::json!({"session": sid, "since": last_seq}),
    )
    .await;
    assert_eq!(out["result"]["state"], "completed", "{out}");
    assert!(
        out["result"]["version"]
            .as_u64()
            .is_some_and(|v| v > last_seq)
    );

    cleanup(&dir);
}
