//! kian-77 serve-level acceptance: the G4 live leak, replayed end to
//! end against the real assembly. A session's agent runs `env` through
//! the built-in exec tool; the provider's API key (a configured secret
//! the assembly auto-collects from `api_key_env`) must appear **nowhere**
//! in the durable event stream — not in the `tool/result`, not in any
//! later `step/request` transcript envelope that replays it.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{
    ConfigTuning, KEY_ENV, ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir,
    write_config_with,
};
use futures::StreamExt;

/// Spawns the assembled server on an ephemeral port (the e2e_serve
/// harness, reused for this acceptance test).
async fn spawn_server(config_path: &std::path::Path) -> (String, Arc<kian_cli::server::Server>) {
    let server = Arc::new(
        kian_cli::server::Server::from_config_file(config_path)
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

/// The dummy secret the provider key env carries for this test
/// (the same env name `write_config*` wires into kian.yaml).
const SECRET_VALUE: &str = "sk-kian77-e2e-secret-value-42";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn env_dumping_session_persists_no_configured_secret() {
    force_no_proxy();
    // The secret exists in the server environment (assembly requires
    // it) — exactly the G4 precondition.
    // SAFETY: set once before the server (or any child) spawns; the
    // name is the shared e2e fixture key every test uses with the same
    // constant value.
    unsafe {
        std::env::set_var(KEY_ENV, SECRET_VALUE);
    }

    let dir = temp_dir("kian77");
    let sqlite = dir.join("kian.sqlite");
    // Script: turn 1 calls exec `env` (the environment dump), turn 2
    // settles with plain text so the drain ends cleanly.
    let (provider_base, _mock) = spawn_scripted_provider(
        vec![
            ScriptedResponse::ToolCall {
                id: "call-env-dump".into(),
                name: "exec".into(),
                arguments: serde_json::json!({"argv": ["bash", "-c", "env"]}),
            },
            ScriptedResponse::Text("done".into()),
        ],
        0,
    )
    .await;
    let config = write_config_with(
        &dir,
        "kian.yaml",
        &sqlite,
        &provider_base,
        "127.0.0.1:7331",
        &ConfigTuning {
            tools: vec!["exec".to_string()],
            ..ConfigTuning::default()
        },
    );
    let (base, _server) = spawn_server(&config).await;
    let client = reqwest::Client::new();

    let out = rpc(&client, &base, "create_session", serde_json::json!({})).await;
    let sid = out["result"]["session"]
        .as_str()
        .expect("session id")
        .to_owned();

    let sse = client
        .get(format!("{base}/sessions/{sid}/events"))
        .timeout(Duration::from_secs(20))
        .send()
        .await
        .expect("sse open");
    assert_eq!(sse.status(), 200);

    let out = rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({"session": sid, "content": "dump the environment"}),
    )
    .await;
    assert_eq!(out["result"]["deduplicated"], false);

    // Consume the whole trail (every raw frame kept — the assertion is
    // over the bytes a client could observe, which is the audit copy).
    let mut raw = String::new();
    let mut tool_results: Vec<serde_json::Value> = Vec::new();
    let mut saw_done = false;
    let mut stream = sse.bytes_stream();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !saw_done {
        let chunk = tokio::time::timeout_at(deadline, stream.next())
            .await
            .expect("sse timeout")
            .expect("sse ended early")
            .expect("sse chunk");
        raw.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(pos) = raw_tail_frame_start(&raw) {
            let frame: String = raw.drain(..pos).collect();
            let mut event_name = "";
            let mut data = String::new();
            for line in frame.lines() {
                if let Some(v) = line.strip_prefix("event: ") {
                    event_name = v.trim();
                } else if let Some(v) = line.strip_prefix("data: ") {
                    data.push_str(v.trim());
                }
            }
            match event_name {
                "kian" => {
                    if let Ok(ev) = serde_json::from_str::<serde_json::Value>(&data)
                        && ev["kind"] == "tool/result"
                    {
                        tool_results.push(ev);
                    }
                }
                "done" => saw_done = true,
                "error" => panic!("sse error frame: {data}"),
                _ => {}
            }
        }
    }

    // The exec ran: exactly one tool/result, and its stdout is the
    // environment dump — redacted.
    assert_eq!(tool_results.len(), 1, "exactly one exec tool result");
    let stdout = tool_results[0]["bytes"]["content"]["output"]["stdout"]
        .as_str()
        .expect("tool/result carries stdout");
    assert!(!stdout.contains(SECRET_VALUE), "stdout must be redacted");
    assert!(
        stdout.contains(&format!("[REDACTED:{KEY_ENV}]")),
        "the configured key env is redacted by name: {stdout}"
    );

    // The audit stream as a whole (every envelope, including the
    // step/request transcripts that replay tool output) holds no
    // verbatim secret.
    assert!(
        !raw.contains(SECRET_VALUE),
        "the secret leaked into the durable event stream"
    );
}

/// Returns the end offset of the next complete SSE frame (terminated by
/// a blank line) at the front of `buf`, if any.
fn raw_tail_frame_start(buf: &str) -> Option<usize> {
    buf.find("\n\n").map(|pos| pos + 2)
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
