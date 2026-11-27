//! Shared e2e fixtures: a mock openai-compat provider (axum) and a
//! `kian.yaml` writer. Tests run with `NO_PROXY=127.0.0.1,localhost`
//! (AGENTS.md: the macOS system proxy intercepts localhost).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;

/// One scripted mock response (openai-compat streaming shape).
#[derive(Debug, Clone)]
pub enum ScriptedResponse {
    /// Plain assistant text (a final answer).
    Text(String),
    /// A tool call the loop must dispatch (e.g. The long `exec` the
    /// chaos suite kills mid-tool). Not every test binary constructs
    /// this variant, hence the allow.
    #[allow(dead_code)]
    ToolCall {
        id: String,
        name: String,
        arguments: serde_json::Value,
    },
    /// A stream that dies mid-flight: HTTP 200, SSE
    /// content-type, one partial content frame — then the connection
    /// ends before the promised content-length is satisfied. The
    /// provider surfaces a mid-turn stream error (the honest soft
    /// failure), never a complete answer.
    #[allow(dead_code)]
    DieMidStream,
}

/// A mock openai-compat chat-completions endpoint. It speaks the real
/// wire format the `OpenAiCompatProvider` parses (SSE `data:` frames),
/// counts requests, serves one scripted response per request number
/// (the last script entry repeats), and can gate its first N requests
/// (the fault-injection point for the kill-and-restart tests).
pub struct MockProvider {
    pub requests: AtomicUsize,
    /// Responses actually sent (after any gate released) — the
    /// mid-provider-turn kill point observes request-arrived while
    /// `delivered < requests`.
    pub delivered: AtomicUsize,
    pub script: Vec<ScriptedResponse>,
    /// Requests with number <= `gated` hold until `released`.
    pub gated: usize,
    /// additionally hold exactly this request number until
    /// `released` (0 = off). Lets a test park a drain mid-provider-turn
    /// while an async job settles, then release on durable evidence —
    /// deterministic, no sleeps. Existing `gated` semantics unchanged.
    pub gate_at: usize,
    pub released: AtomicBool,
}

pub fn sse_completion_body(text: &str) -> String {
    format!(
        "data: {{\"choices\":[{{\"delta\":{{\"content\":\"{text}\"}}}}]}}\n\n\
         data: {{\"choices\":[{{\"delta\":{{}},\"finish_reason\":\"stop\"}}]}}\n\n"
    )
}

/// The SSE body carrying one tool call (openai streaming shape: the
/// `function.arguments` string is the JSON-encoded argument object).
/// Built through `serde_json` so argument quoting is always valid.
pub fn sse_tool_call_body(id: &str, name: &str, arguments: &str) -> String {
    let call = serde_json::json!({
        "index": 0,
        "id": id,
        "function": {"name": name, "arguments": arguments},
    });
    let head = serde_json::json!({"choices": [{"delta": {"tool_calls": [call]}}]});
    let tail = serde_json::json!({"choices": [{"delta": {}, "finish_reason": "tool_calls"}]});
    format!("data: {head}\n\ndata: {tail}\n\n")
}

async fn completions(
    State(mock): State<Arc<MockProvider>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let n = mock.requests.fetch_add(1, Ordering::SeqCst) + 1;
    let gated = n <= mock.gated || (mock.gate_at != 0 && n == mock.gate_at);
    if gated && !mock.released.load(Ordering::SeqCst) {
        // Hold (bounded well above any test timeout) until released or
        // the (soon-to-be-killed) client disconnects.
        for _ in 0..600 {
            if mock.released.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    let _ = body;
    let body: String = match mock
        .script
        .get(n - 1)
        .unwrap_or(mock.script.last().expect("non-empty script"))
    {
        ScriptedResponse::Text(text) => sse_completion_body(text),
        ScriptedResponse::ToolCall {
            id,
            name,
            arguments,
        } => sse_tool_call_body(
            id,
            name,
            &serde_json::to_string(arguments).expect("tool arguments serialize"),
        ),
        ScriptedResponse::DieMidStream => {
            // One partial frame, then the body aborts: hyper drops the
            // connection without the terminating chunk, which is the
            // truncation the provider must surface as a stream error.
            //
            // This used to promise a larger Content-Length than the body
            // it sent. hyper 1.12 panics on that mismatch ("payload claims
            // content-length of N, custom content-length header claims M")
            // — on a CI runner the panic poisons the keep-alive connection
            // and the recovered turn never reaches the mock, so the test
            // failed as a 2-vs-3 call count rather than as the thing it
            // asserts. An aborted chunked body is both truthful and the
            // same failure from the client's side.
            let partial = "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n";
            let chunks = futures::stream::iter(vec![
                Ok::<_, std::io::Error>(axum::body::Bytes::from_static(partial.as_bytes())),
                Err(std::io::Error::other("mock: the provider dies mid-stream")),
            ]);
            mock.delivered.fetch_add(1, Ordering::SeqCst);
            let mut response = axum::response::Response::new(axum::body::Body::from_stream(chunks));
            response.headers_mut().insert(
                "content-type",
                axum::http::HeaderValue::from_static("text/event-stream"),
            );
            return response;
        }
    };
    mock.delivered.fetch_add(1, Ordering::SeqCst);
    let mut response = (StatusCode::OK, body).into_response();
    response.headers_mut().insert(
        "content-type",
        axum::http::HeaderValue::from_static("text/event-stream"),
    );
    response
}

/// Spawns the mock provider on an ephemeral port; returns its base URL
/// (`http://127.0.0.1:PORT/v1`) and the shared state.
#[allow(dead_code)] // each integration test compiles this module separately
pub async fn spawn_mock_provider(gated: usize) -> (String, Arc<MockProvider>) {
    spawn_scripted_provider(vec![ScriptedResponse::Text("All done.".into())], gated).await
}

/// [`spawn_mock_provider`] with an explicit response script (request
/// number N gets script entry N-1; the last entry repeats).
pub async fn spawn_scripted_provider(
    script: Vec<ScriptedResponse>,
    gated: usize,
) -> (String, Arc<MockProvider>) {
    spawn_scripted_provider_gated_at(script, gated, 0).await
}

/// [`spawn_scripted_provider`] with an additional single-request gate
/// (`gate_at`, 0 = off) — the async-completion tests park one
/// provider turn until the completion input is durable.
#[allow(dead_code)] // each integration test compiles this module separately
pub async fn spawn_scripted_provider_gated_at(
    script: Vec<ScriptedResponse>,
    gated: usize,
    gate_at: usize,
) -> (String, Arc<MockProvider>) {
    assert!(!script.is_empty(), "mock script must be non-empty");
    let mock = Arc::new(MockProvider {
        requests: AtomicUsize::new(0),
        delivered: AtomicUsize::new(0),
        script,
        gated,
        gate_at,
        released: AtomicBool::new(false),
    });
    // The mock speaks the whole request body: the ADR-071 L2 grows the
    // context past a few MiB on purpose, and axum's default extractor
    // limit (2 MiB) would otherwise turn the mock itself into a 413.
    let app = Router::new()
        .route("/chat/completions", post(completions))
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024 * 1024))
        .with_state(Arc::clone(&mock));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock");
    let addr = listener.local_addr().expect("mock addr");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("mock provider serve");
    });
    (format!("http://{}", addr), mock)
}

/// The environment variable holding the (dummy) API key.
pub const KEY_ENV: &str = "KIAN_E2E_API_KEY";

/// Writes a kian.yaml pointing at the mock provider; returns its path.
#[allow(dead_code)] // each integration test compiles this module separately
pub fn write_config(dir: &Path, sqlite: &Path, provider_base: &str, listen: &str) -> PathBuf {
    write_config_with(
        dir,
        "kian.yaml",
        sqlite,
        provider_base,
        listen,
        &ConfigTuning::default(),
    )
}

/// Drain/lease tuning knobs the chaos suite overrides (defaults match
/// plain `write_config`).
pub struct ConfigTuning {
    pub lease_ttl_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub tools: Vec<String>,
    pub exec_timeout_ms: u64,
    /// ADR-068: the provider's own request-size bound (None = the
    /// provider config carries no key, the default today).
    pub max_request_bytes: Option<u64>,
}

impl Default for ConfigTuning {
    fn default() -> Self {
        Self {
            lease_ttl_ms: 30_000,
            heartbeat_interval_ms: 10_000,
            tools: Vec::new(),
            exec_timeout_ms: 30_000,
            max_request_bytes: None,
        }
    }
}

/// [`write_config`] with explicit drain/lease/tool tuning (chaos:
/// short lease TTL + heartbeat so recovery detection is observable)
/// and an explicit file name (boot #1 / boot #2 configs coexist).
pub fn write_config_with(
    dir: &Path,
    file: &str,
    sqlite: &Path,
    provider_base: &str,
    listen: &str,
    tuning: &ConfigTuning,
) -> PathBuf {
    let provider_bound = tuning
        .max_request_bytes
        .map(|limit| format!("    max_request_bytes: {limit}\n"))
        .unwrap_or_default();
    let yaml = format!(
        r#"listen: "{listen}"
storage:
  sqlite: "{}"
agent:
  id: e2e-agent
  model_slots:
    main:
      provider: mock
      model: mock-model
  tools: {:?}
providers:
  mock:
    base_url: "{provider_base}"
    api_key_env: {KEY_ENV}
{provider_bound}drain:
  max_iterations: 8
  lease_ttl_ms: {}
  heartbeat_interval_ms: {}
tools:
  root: "{}"
  exec_timeout_ms: {}
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#,
        sqlite.display(),
        // Debug-format the Vec<String> directly: `[]` / `["fs.read"]`
        // are valid YAML flow sequences. (serde_json::to_string would
        // inject quotes — `tools: "[]"` parses as a *string* and every
        // e2e config load fails.)
        tuning.tools,
        tuning.lease_ttl_ms,
        tuning.heartbeat_interval_ms,
        dir.display(),
        tuning.exec_timeout_ms,
    );
    let path = dir.join(file);
    std::fs::write(&path, yaml).expect("write kian.yaml");
    path
}

/// Forces the no-proxy environment for localhost (must run before any
/// reqwest client is built).
#[allow(dead_code)] // each integration test compiles this module separately
pub fn force_no_proxy() {
    unsafe {
        std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
        std::env::set_var("no_proxy", "127.0.0.1,localhost");
        std::env::set_var("KIAN_E2E_API_KEY", "test-key");
    }
}

/// A fresh temp dir (caller cleans up by dropping the guard).
pub fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("kian-c13-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

/// Polls `cond` every 25 ms until it holds or `timeout_ms` elapses.
#[allow(dead_code)] // each integration test compiles this module separately
pub async fn wait_for<F>(timeout_ms: u64, mut cond: F) -> bool
where
    F: FnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
