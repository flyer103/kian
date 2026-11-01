//! openai-compatible `ModelProvider` implementation (card C8).
//!
//! HTTP via reqwest with SSE streaming parsed into model chunks; request
//! bytes are serialized and handed to persistence before the request
//! flies. No code path may send bytes that were not first handed to
//! persistence.

use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::StreamExt as _;
use futures::TryStreamExt as _;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use kian_core::model::SlotName;
pub use kian_core::seams::ProviderError;
use kian_core::seams::{ModelChunk, ModelProvider, ModelRequest, ToolSchema};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Persist-before-send hook (A2)
// ---------------------------------------------------------------------------

/// The exact bytes of one model request, handed to the caller for durable
/// persistence as a `step/request` event *before* the HTTP request flies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedRequest {
    pub slot: SlotName,
    /// Provider name (registry key) the request is being sent to.
    pub provider: String,
    /// Model name extracted from the request body (`body["model"]`).
    pub model: String,
    /// The exact serialized request body — byte-for-byte what goes on the
    /// wire.
    pub body: Vec<u8>,
}

/// Persistence-hook failure. The provider maps it onto
/// [`ProviderError::Other`] and **never sends** the request.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PersistError {
    #[error("{0}")]
    Hook(String),
}

/// A2 hook: the sink the openai-compat provider hands every request's exact
/// bytes to before dialing. If `persist_request` fails, the request is not
/// sent and the turn surfaces an error.
///
/// A blanket impl covers plain async closures (`Fn(PersistedRequest) ->
/// Fut`), so callers can register a store append without a named type.
pub trait PersistSink: Send + Sync {
    fn persist_request(
        &self,
        record: PersistedRequest,
    ) -> BoxFuture<'static, Result<(), PersistError>>;
}

impl<F, Fut> PersistSink for F
where
    F: Fn(PersistedRequest) -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), PersistError>> + Send + 'static,
{
    fn persist_request(
        &self,
        record: PersistedRequest,
    ) -> BoxFuture<'static, Result<(), PersistError>> {
        Box::pin(self(record))
    }
}

// ---------------------------------------------------------------------------
// Frozen request serialization (constraint 3)
// ---------------------------------------------------------------------------

/// One chat message in the openai-compat request body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

/// SHA-256 of the golden request body asserted by
/// `same_logical_request_serializes_to_identical_bytes_golden_hash`.
///
/// **This constant is frozen stable content.** Changing the serialization
/// contract below (or this hash) invalidates provider prefix cache for
/// every session and is a constraint-3 event: it requires a DESIGN note
/// and an observable invalidation-cause record.
pub const FROZEN_CHAT_BODY_SHA256: &str =
    "4a1dad31d2e377be6f5268514841045a7b94b3210a344f75247e9ffbd284d7ce";

/// Build the openai-compat chat-completions request body.
///
/// # Frozen serialization contract (constraint 3)
///
/// The body is serialized with `serde_json::to_vec` under the workspace's
/// default feature set — i.e. **compact output with object keys in
/// lexicographic order** (`serde_json::Map` is a `BTreeMap`; the
/// `preserve_order` feature is NOT enabled). Consequences, all frozen:
///
/// - Object key order in the source of a `Value` is irrelevant; the same
///   logical body always serializes to identical bytes.
/// - Top-level fields: exactly `messages`, `model`, `stream` (always
///   `true`) and — only when at least one tool is supplied — `tools`.
/// - Message objects: exactly `content` and `role`.
/// - Tool entries: `{"function": {description, name, parameters},
///   "type": "function"}`. **The `tools` array preserves caller-supplied
///   order** and is part of the prompt prefix: callers must pass a
///   deterministic order (the built-in loop sorts by tool name).
/// - The workspace must never enable `serde_json/preserve_order`; doing so
///   would silently change every request's bytes.
///
/// Any change to this contract is a deliberate prefix-cache invalidation:
/// gate it like constraint 3 (observable event + golden-hash update).
pub fn build_chat_body(model: &str, messages: &[ChatMessage], tools: &[ToolSchema]) -> Value {
    let mut body = json!({
        "model": model,
        "messages": messages,
        "stream": true,
    });
    if !tools.is_empty() {
        body["tools"] = Value::Array(tools.iter().map(tool_entry).collect());
    }
    body
}

fn tool_entry(tool: &ToolSchema) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.parameters,
        }
    })
}

/// Deterministically serialize a request body to the exact bytes that are
/// both persisted and sent (the single source of both copies).
pub fn serialize_request_body(body: &Value) -> Result<Vec<u8>, ProviderError> {
    serde_json::to_vec(body)
        .map_err(|e| ProviderError::Other(format!("request serialization failed: {e}")))
}

// ---------------------------------------------------------------------------
// Provider
// ---------------------------------------------------------------------------

/// Endpoint configuration for one openai-compatible provider.
#[derive(Debug, Clone)]
pub struct OpenAiCompatConfig {
    /// Registry/provider name (`SlotTarget::provider`), e.g. `openai-compat`.
    pub name: String,
    /// Base URL without trailing slash, e.g. `https://api.example.com/v1`.
    pub base_url: String,
    pub api_key: String,
    /// Chat-completions path appended to `base_url`
    /// (default `/chat/completions`).
    pub chat_completions_path: String,
}

struct Inner {
    config: OpenAiCompatConfig,
    client: reqwest::Client,
    sink: Arc<dyn PersistSink>,
}

impl Inner {
    async fn post(&self, body: Vec<u8>) -> Result<reqwest::Response, ProviderError> {
        let cfg = &self.config;
        let url = format!(
            "{}{}",
            cfg.base_url.trim_end_matches('/'),
            cfg.chat_completions_path
        );
        self.client
            .post(&url)
            .bearer_auth(&cfg.api_key)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| ProviderError::Transport(e.to_string()))
    }
}

/// The openai-compatible [`ModelProvider`] (seam built-in, DESIGN §6.2).
///
/// Cheap to clone (`Arc` inside). Every `complete` call: serialize the
/// body → hand the exact bytes to the [`PersistSink`] → only on success
/// POST those same bytes and stream SSE back as [`ModelChunk`]s.
#[derive(Clone)]
pub struct OpenAiCompatProvider(Arc<Inner>);

impl OpenAiCompatProvider {
    pub fn new(config: OpenAiCompatConfig, sink: Arc<dyn PersistSink>) -> Self {
        // Builder configuration here is static; a build failure is a
        // process-level misconfiguration (no TLS backend), not a runtime
        // condition.
        let client = reqwest::Client::builder()
            .build()
            .expect("reqwest client construction cannot fail with rustls-tls");
        Self(Arc::new(Inner {
            config,
            client,
            sink,
        }))
    }

    pub fn config(&self) -> &OpenAiCompatConfig {
        &self.0.config
    }
}

impl ModelProvider for OpenAiCompatProvider {
    fn complete(&self, req: ModelRequest) -> BoxStream<'static, Result<ModelChunk, ProviderError>> {
        let inner = Arc::clone(&self.0);
        let model = req
            .body
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let bytes = serialize_request_body(&req.body);
        futures::stream::once(async move {
            let body = match bytes {
                Ok(b) => b,
                Err(e) => return error_stream(e),
            };
            let record = PersistedRequest {
                slot: req.slot.clone(),
                provider: inner.config.name.clone(),
                model,
                body: body.clone(),
            };
            // A2: persist-what-you-send. The persist hook completes before
            // the request is issued; on failure the request is never sent.
            if let Err(e) = inner.sink.persist_request(record).await {
                return error_stream(ProviderError::Other(format!(
                    "persist-before-send failed, request NOT sent: {e}"
                )));
            }
            match inner.post(body).await {
                Ok(resp) => response_stream(resp),
                Err(e) => error_stream(e),
            }
        })
        .flatten()
        .boxed()
    }
}

fn error_stream(e: ProviderError) -> BoxStream<'static, Result<ModelChunk, ProviderError>> {
    futures::stream::once(ready(Err(e))).boxed()
}

/// (saw_chunk, saw_error) liveness flags shared between the body stream and
/// the empty-response tail.
type StreamFlags = (AtomicBool, AtomicBool);

struct TailState {
    flags: Arc<StreamFlags>,
    /// Taken on the first tail poll to flush any trailing unterminated SSE
    /// frame at end-of-body.
    decoder: Option<Arc<std::sync::Mutex<SseDecoder>>>,
    queue: std::collections::VecDeque<Result<ModelChunk, ProviderError>>,
    done: bool,
}

fn response_stream(
    resp: reqwest::Response,
) -> BoxStream<'static, Result<ModelChunk, ProviderError>> {
    let status = resp.status();
    if !status.is_success() {
        return futures::stream::once(async move {
            let body = resp.text().await.unwrap_or_default();
            Err(map_error_status(status, &body))
        })
        .boxed();
    }
    let flags: Arc<StreamFlags> = Arc::new((AtomicBool::new(false), AtomicBool::new(false)));
    let decoder = Arc::new(std::sync::Mutex::new(SseDecoder::new()));
    let scan_flags = Arc::clone(&flags);
    let scan_dec = Arc::clone(&decoder);
    let main = resp
        .bytes_stream()
        .map_err(|e| ProviderError::Transport(e.to_string()))
        .scan((), move |(), res| {
            let mut items: Vec<Result<ModelChunk, ProviderError>> = Vec::new();
            match res {
                Ok(bytes) => {
                    let mut dec = scan_dec.lock().unwrap();
                    for frame in dec.feed(&bytes) {
                        if let SseFrame::Data(data) = frame {
                            items.extend(decode_data(&data, &scan_flags));
                        }
                    }
                }
                Err(e) => items.push(Err(e)),
            }
            if items.iter().any(|i| i.is_err()) {
                scan_flags.1.store(true, Ordering::SeqCst);
            }
            ready(Some(items))
        })
        .flat_map(futures::stream::iter);
    // Tail: flush a trailing unterminated frame, then apply the
    // constraint-7 empty check — a stream that produced neither a chunk
    // nor an error is a soft failure, never a silent success.
    let tail = futures::stream::unfold(
        TailState {
            flags: Arc::clone(&flags),
            decoder: Some(decoder),
            queue: std::collections::VecDeque::new(),
            done: false,
        },
        |mut st| {
            let next = loop {
                if st.done {
                    break None;
                }
                if let Some(item) = st.queue.pop_front() {
                    break Some(item);
                }
                if let Some(dec) = st.decoder.take() {
                    // End-of-body flush of a trailing unterminated frame.
                    let mut items = Vec::new();
                    for frame in dec.lock().unwrap().finish() {
                        if let SseFrame::Data(data) = frame {
                            items.extend(decode_data(&data, &st.flags));
                        }
                    }
                    if items.iter().any(|i| i.is_err()) {
                        st.flags.1.store(true, Ordering::SeqCst);
                    }
                    st.queue = items.into_iter().collect();
                    continue;
                }
                // Constraint 7: a stream that produced neither a chunk nor
                // an error is an empty response — a soft failure, never a
                // silent success.
                let terminal =
                    st.flags.0.load(Ordering::SeqCst) || st.flags.1.load(Ordering::SeqCst);
                if terminal {
                    break None;
                }
                st.done = true;
                break Some(Err(ProviderError::EmptyResponse));
            };
            ready(next.map(|item| (item, st)))
        },
    );
    main.chain(tail).boxed()
}

/// Decode one SSE `data:` payload into zero or more stream items.
fn decode_data(data: &str, flags: &StreamFlags) -> Vec<Result<ModelChunk, ProviderError>> {
    let v: Value = match serde_json::from_str(data) {
        Ok(v) => v,
        Err(e) => {
            return vec![Err(ProviderError::InvalidResponse(format!(
                "malformed SSE data frame: {e}"
            )))];
        }
    };
    if let Some(err) = v.get("error").filter(|e| !e.is_null()) {
        return vec![Err(map_error_payload(err))];
    }
    match v.pointer("/choices/0/delta") {
        Some(delta) if !delta.as_object().is_some_and(|o| o.is_empty()) => {
            flags.0.store(true, Ordering::SeqCst);
            vec![Ok(ModelChunk {
                delta: delta.clone(),
            })]
        }
        // Empty deltas (e.g. the trailing finish_reason marker) and
        // usage-only / keepalive frames carry nothing for the loop; a turn
        // with only those is an EmptyResponse soft failure (constraint 7).
        _ => Vec::new(),
    }
}

/// Map an HTTP error status (with body) onto the classifier-friendly
/// [`ProviderError`] classes.
fn map_error_status(status: reqwest::StatusCode, body: &str) -> ProviderError {
    let snippet: String = body.trim().chars().take(512).collect();
    let code = status.as_u16();
    match code {
        429 => ProviderError::RateLimited,
        408 => ProviderError::Transport(format!("HTTP {code}: {snippet}")),
        400 | 413 if is_context_overflow(body) => ProviderError::ContextOverflow,
        400 => ProviderError::InvalidResponse(format!("HTTP {code}: {snippet}")),
        _ => ProviderError::Unavailable(format!("HTTP {code}: {snippet}")),
    }
}

/// Map an in-band error object (SSE error frame) onto a [`ProviderError`].
fn map_error_payload(err: &Value) -> ProviderError {
    let msg = err
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("unknown provider error");
    let code = err
        .get("code")
        .and_then(Value::as_u64)
        .or_else(|| err.get("status").and_then(Value::as_u64));
    let typ = err.get("type").and_then(Value::as_str).unwrap_or("");
    if code == Some(429) || typ.contains("rate") {
        return ProviderError::RateLimited;
    }
    if is_context_overflow(msg) || typ.contains("context") {
        return ProviderError::ContextOverflow;
    }
    ProviderError::Unavailable(msg.to_string())
}

fn is_context_overflow(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("context length")
        || lower.contains("context window")
        || lower.contains("maximum context")
}

// ---------------------------------------------------------------------------
// SSE decoding (pure)
// ---------------------------------------------------------------------------

/// Incremental server-sent-events decoder: feed raw response bytes, get
/// complete frames back. Only the `data:` field is kept (the openai-compat
/// protocol uses no other field); `event:`/`id:`/comment lines are
/// ignored per the SSE spec.
pub(crate) mod sse {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum SseFrame {
        Data(String),
        Done,
    }

    #[derive(Debug, Default)]
    pub(crate) struct SseDecoder {
        buf: Vec<u8>,
        pending_data: Vec<String>,
    }

    impl SseDecoder {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        /// Consume bytes, returning every complete frame found so far.
        /// Lines may split arbitrarily across feeds.
        pub(crate) fn feed(&mut self, bytes: &[u8]) -> Vec<SseFrame> {
            self.buf.extend_from_slice(bytes);
            let mut out = Vec::new();
            while let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let mut line: Vec<u8> = self.buf.drain(..=pos).collect();
                line.pop(); // '\n'
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                if line.is_empty() {
                    // Empty line dispatches the pending event.
                    if !self.pending_data.is_empty() {
                        let data = std::mem::take(&mut self.pending_data).join("\n");
                        out.push(if data == "[DONE]" {
                            SseFrame::Done
                        } else {
                            SseFrame::Data(data)
                        });
                    }
                    continue;
                }
                if let Some(rest) = line.strip_prefix(b"data:") {
                    // Exactly one optional leading space after the colon.
                    let rest = if rest.first() == Some(&b' ') {
                        &rest[1..]
                    } else {
                        rest
                    };
                    self.pending_data
                        .push(String::from_utf8_lossy(rest).into_owned());
                }
            }
            out
        }

        /// Flush a trailing unterminated event at end-of-body. Idempotent.
        pub(crate) fn finish(&mut self) -> Vec<SseFrame> {
            self.feed(b"\n\n")
        }
    }
}
use sse::SseDecoder;
use sse::SseFrame;

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::{FutureExt as _, StreamExt, future::BoxFuture};
    use kian_core::model::SlotName;
    use kian_core::seams::{ModelChunk, ModelProvider, ModelRequest, ToolSchema};
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
    use tokio::sync::oneshot;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{
        ChatMessage, OpenAiCompatConfig, OpenAiCompatProvider, PersistError, PersistSink,
        PersistedRequest, ProviderError, build_chat_body, serialize_request_body,
    };

    /// AGENTS gotcha: the macOS system proxy intercepts localhost — every
    /// wiremock (local e2e) test must set NO_PROXY for 127.0.0.1/localhost.
    fn ensure_no_proxy() {
        // SAFETY: test-only; every test writes the same value before any
        // reqwest client is built.
        unsafe {
            std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
            std::env::set_var("no_proxy", "127.0.0.1,localhost");
        }
    }

    // ------------------------------------------------------------------
    // Test sinks
    // ------------------------------------------------------------------

    #[derive(Clone)]
    struct RecordingSink {
        log: Arc<Mutex<Vec<String>>>,
        bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl PersistSink for RecordingSink {
        fn persist_request(
            &self,
            record: PersistedRequest,
        ) -> BoxFuture<'static, Result<(), PersistError>> {
            let log = self.log.clone();
            let bodies = self.bodies.clone();
            async move {
                log.lock().unwrap().push(format!("req:{}", record.model));
                bodies.lock().unwrap().push(record.body);
                Ok(())
            }
            .boxed()
        }
    }

    /// A sink whose persist future blocks on a oneshot gate before
    /// completing — used to prove the HTTP request does not fly until
    /// persistence has completed.
    struct GatedSink {
        gate: Arc<tokio::sync::Mutex<Option<oneshot::Receiver<()>>>>,
        log: Arc<Mutex<Vec<String>>>,
    }

    impl PersistSink for GatedSink {
        fn persist_request(
            &self,
            _record: PersistedRequest,
        ) -> BoxFuture<'static, Result<(), PersistError>> {
            let gate = self.gate.clone();
            let log = self.log.clone();
            async move {
                log.lock().unwrap().push("persist".to_string());
                let rx = gate.lock().await.take();
                if let Some(rx) = rx {
                    rx.await
                        .map_err(|_| PersistError::Hook("gate dropped".into()))?;
                }
                Ok(())
            }
            .boxed()
        }
    }

    struct FailingSink;

    impl PersistSink for FailingSink {
        fn persist_request(
            &self,
            _record: PersistedRequest,
        ) -> BoxFuture<'static, Result<(), PersistError>> {
            async { Err(PersistError::Hook("db down".into())) }.boxed()
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    fn config(base: &str, chat_path: &str) -> OpenAiCompatConfig {
        OpenAiCompatConfig {
            name: "openai-compat".into(),
            base_url: base.into(),
            api_key: "test-key".into(),
            chat_completions_path: chat_path.into(),
        }
    }

    fn sse(frames: &[&str]) -> String {
        frames.iter().map(|f| format!("data: {f}\n\n")).collect()
    }

    fn sse_ok(frames: &[&str]) -> ResponseTemplate {
        ResponseTemplate::new(200)
            .set_body_string(sse(frames))
            .insert_header("content-type", "text/event-stream")
    }

    fn sample_request(model: &str) -> ModelRequest {
        ModelRequest {
            slot: SlotName("main".into()),
            body: build_chat_body(
                model,
                &[ChatMessage {
                    role: "user".into(),
                    content: "hello".into(),
                }],
                &[],
            ),
        }
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        let mut h = Sha256::new();
        h.update(bytes);
        format!("{:x}", h.finalize())
    }

    async fn collect(
        mut stream: futures::stream::BoxStream<'static, Result<ModelChunk, ProviderError>>,
    ) -> Vec<Result<ModelChunk, ProviderError>> {
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item);
        }
        out
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    /// A2 ordering: while the persist callback is pending, the mock server
    /// must receive zero requests; after it completes, exactly one —
    /// carrying byte-for-byte the persisted body.
    #[tokio::test]
    async fn persist_callback_completes_before_request_flies() {
        ensure_no_proxy();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(sse_ok(&[
                r#"{"choices":[{"delta":{"content":"hi"}}]}"#,
                "[DONE]",
            ]))
            .expect(1)
            .mount(&server)
            .await;

        let (tx, rx) = oneshot::channel::<()>();
        let log = Arc::new(Mutex::new(Vec::new()));
        let sink = GatedSink {
            gate: Arc::new(tokio::sync::Mutex::new(Some(rx))),
            log: log.clone(),
        };
        let provider =
            OpenAiCompatProvider::new(config(&server.uri(), "/chat/completions"), Arc::new(sink));
        let stream = provider.complete(sample_request("m1"));

        let handle = tokio::spawn(async move { collect(stream).await });
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            server.received_requests().await.unwrap().len(),
            0,
            "no request may fly before the persist callback completes"
        );

        tx.send(()).unwrap();
        let out = handle.await.unwrap();
        assert_eq!(out.len(), 1);
        assert!(out[0].is_ok());
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(log.lock().unwrap().as_slice(), ["persist"]);
    }

    /// A2 hard guarantee: when persistence fails the request is never sent.
    #[tokio::test]
    async fn persist_failure_prevents_any_request() {
        ensure_no_proxy();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(sse_ok(&["[DONE]"]))
            .expect(0)
            .mount(&server)
            .await;

        let provider = OpenAiCompatProvider::new(
            config(&server.uri(), "/chat/completions"),
            Arc::new(FailingSink),
        );
        let out = collect(provider.complete(sample_request("m1")).boxed()).await;
        assert_eq!(out.len(), 1);
        match &out[0] {
            Err(ProviderError::Other(msg)) => {
                assert!(msg.contains("db down"), "unexpected message: {msg}");
                assert!(
                    msg.contains("NOT sent"),
                    "must state the request was not sent: {msg}"
                );
            }
            other => panic!("expected Other error, got {other:?}"),
        }
        assert_eq!(server.received_requests().await.unwrap().len(), 0);
    }

    /// SSE deltas stream as chunks in arrival order; the wire body is the
    /// persisted body (byte-for-byte) and carries model + stream:true.
    #[tokio::test]
    async fn sse_deltas_stream_as_model_chunks() {
        ensure_no_proxy();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(sse_ok(&[
                r#"{"choices":[{"delta":{"content":"Hel"}}]}"#,
                r#"{"choices":[{"delta":{"content":"lo"}}]}"#,
                r#"{"choices":[{"finish_reason":"stop","delta":{}}]}"#,
                "[DONE]",
            ]))
            .expect(1)
            .mount(&server)
            .await;

        let sink = RecordingSink {
            log: Arc::new(Mutex::new(Vec::new())),
            bodies: Arc::new(Mutex::new(Vec::new())),
        };
        let bodies = sink.bodies.clone();
        let provider =
            OpenAiCompatProvider::new(config(&server.uri(), "/chat/completions"), Arc::new(sink));
        let out = collect(provider.complete(sample_request("m1")).boxed()).await;
        assert!(out.iter().all(|r| r.is_ok()));
        let deltas: Vec<Value> = out.into_iter().map(|r| r.unwrap().delta).collect();
        assert_eq!(
            deltas,
            vec![json!({"content": "Hel"}), json!({"content": "lo"})]
        );

        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].body,
            bodies.lock().unwrap()[0],
            "wire bytes must equal persisted bytes"
        );
        let wire: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(wire["model"], json!("m1"));
        assert_eq!(wire["stream"], json!(true));
        assert_eq!(
            requests[0].headers.get("authorization").unwrap(),
            "Bearer test-key"
        );
    }

    /// HTTP status → ProviderError classes used by the C10 classifier.
    #[tokio::test]
    async fn error_statuses_map_to_provider_error_classes() {
        ensure_no_proxy();
        let server = MockServer::start().await;
        for (p, template) in [
            (
                "/e429",
                ResponseTemplate::new(429).set_body_string(r#"{"error":{"message":"slow down"}}"#),
            ),
            ("/e500", ResponseTemplate::new(500).set_body_string("boom")),
            (
                "/e400",
                ResponseTemplate::new(400).set_body_string(
                    r#"{"error":{"message":"This model's maximum context length is 4096 tokens"}}"#,
                ),
            ),
        ] {
            Mock::given(method("POST"))
                .and(path(p))
                .respond_with(template)
                .expect(1)
                .mount(&server)
                .await;
        }

        let sink = RecordingSink {
            log: Arc::new(Mutex::new(Vec::new())),
            bodies: Arc::new(Mutex::new(Vec::new())),
        };
        for (p, expected) in [
            ("/e429", ProviderError::RateLimited),
            ("/e500", ProviderError::Unavailable("HTTP 500: boom".into())),
            ("/e400", ProviderError::ContextOverflow),
        ] {
            let provider =
                OpenAiCompatProvider::new(config(&server.uri(), p), Arc::new(sink.clone()));
            let out = collect(provider.complete(sample_request("m1")).boxed()).await;
            assert_eq!(out.len(), 1, "path {p}");
            assert_eq!(out[0].as_ref().unwrap_err(), &expected, "path {p}");
        }
    }

    /// Constraint 7: zero chunks is a soft failure (EmptyResponse), never a
    /// silent success.
    #[tokio::test]
    async fn zero_chunks_is_empty_response_soft_failure() {
        ensure_no_proxy();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(sse_ok(&["[DONE]"]))
            .expect(1)
            .mount(&server)
            .await;

        let sink = RecordingSink {
            log: Arc::new(Mutex::new(Vec::new())),
            bodies: Arc::new(Mutex::new(Vec::new())),
        };
        let provider =
            OpenAiCompatProvider::new(config(&server.uri(), "/chat/completions"), Arc::new(sink));
        let out = collect(provider.complete(sample_request("m1")).boxed()).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].as_ref().unwrap_err(), &ProviderError::EmptyResponse);
    }

    /// A mid-stream error frame maps onto the classifier classes too.
    #[tokio::test]
    async fn mid_stream_error_frame_maps_to_rate_limited() {
        ensure_no_proxy();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(sse_ok(&[
                r#"{"choices":[{"delta":{"content":"Hel"}}]}"#,
                r#"{"error":{"message":"quota exceeded","code":429}}"#,
            ]))
            .expect(1)
            .mount(&server)
            .await;

        let sink = RecordingSink {
            log: Arc::new(Mutex::new(Vec::new())),
            bodies: Arc::new(Mutex::new(Vec::new())),
        };
        let provider =
            OpenAiCompatProvider::new(config(&server.uri(), "/chat/completions"), Arc::new(sink));
        let out = collect(provider.complete(sample_request("m1")).boxed()).await;
        assert_eq!(out.len(), 2);
        assert!(out[0].is_ok());
        assert_eq!(out[1].as_ref().unwrap_err(), &ProviderError::RateLimited);
    }

    // ------------------------------------------------------------------
    // Frozen serialization (constraint 3)
    // ------------------------------------------------------------------

    /// The same logical request serializes to identical bytes twice, and
    /// those bytes match the golden hash. Changing the constant below is a
    /// constraint-3 event (prefix-cache invalidation for every session).
    #[test]
    fn same_logical_request_serializes_to_identical_bytes_golden_hash() {
        let tools = vec![
            ToolSchema {
                name: "fs.read".into(),
                description: "read a file".into(),
                parameters: json!({"type": "object"}),
            },
            ToolSchema {
                name: "fs.write".into(),
                description: "write a file".into(),
                parameters: json!({"type": "object"}),
            },
        ];
        let messages = vec![
            ChatMessage {
                role: "system".into(),
                content: "you are kian".into(),
            },
            ChatMessage {
                role: "user".into(),
                content: "hello".into(),
            },
        ];
        let a = build_chat_body("kimi-k2", &messages, &tools);
        let b = build_chat_body("kimi-k2", &messages, &tools);
        let bytes_a = serialize_request_body(&a).unwrap();
        let bytes_b = serialize_request_body(&b).unwrap();
        assert_eq!(bytes_a, bytes_b);
        assert_eq!(
            sha256_hex(&bytes_a),
            super::FROZEN_CHAT_BODY_SHA256,
            "request serialization drifted — this is a constraint-3 event"
        );
    }

    /// Serialization is independent of the source order the JSON value was
    /// assembled in (serde_json sorts object keys; arrays keep caller
    /// order).
    #[test]
    fn body_serialization_is_source_key_order_independent() {
        let built = build_chat_body(
            "m1",
            &[ChatMessage {
                role: "user".into(),
                content: "hi".into(),
            }],
            &[],
        );
        let hand_written = json!({
            "stream": true,
            "model": "m1",
            "messages": [{"content": "hi", "role": "user"}]
        });
        assert_eq!(
            serialize_request_body(&built).unwrap(),
            serialize_request_body(&hand_written).unwrap()
        );
    }

    // ------------------------------------------------------------------
    // SSE decoder (pure)
    // ------------------------------------------------------------------

    #[test]
    fn sse_decoder_handles_split_frames_crlf_comments_and_done() {
        use super::sse::{SseDecoder, SseFrame};

        let mut dec = SseDecoder::new();
        // One event fed byte-by-byte (frame split across feeds).
        let mut got = Vec::new();
        for b in b"data: {\"a\":1}\n\n" {
            got.extend(dec.feed(&[*b]));
        }
        assert_eq!(got, vec![SseFrame::Data("{\"a\":1}".into())]);

        // CRLF line endings, comment + ignored fields, then [DONE].
        let mut dec = SseDecoder::new();
        let got = dec.feed(
            b": keep-alive\r\nevent: message\r\ndata: x\r\ndata: y\r\n\r\ndata: [DONE]\r\n\r\n",
        );
        assert_eq!(got, vec![SseFrame::Data("x\ny".into()), SseFrame::Done]);

        // Trailing event without a blank line is flushed by finish().
        let mut dec = SseDecoder::new();
        dec.feed(b"data: tail");
        assert_eq!(dec.finish(), vec![SseFrame::Data("tail".into())]);
        assert!(dec.finish().is_empty(), "finish is idempotent");
    }
}
