//! openai-compatible `ModelProvider` implementation.
//!
//! HTTP via reqwest with SSE streaming parsed into model chunks; request
//! bytes are serialized and handed to persistence before the request
//! flies. No code path may send bytes that were not first handed to
//! persistence.

use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

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

/// One assistant tool call in the strict openai wire shape
/// (`messages[n].tool_calls[m]`, card / dialect v2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireFunctionCall {
    pub name: String,
    /// The arguments as a JSON-encoded string (the wire form; kian's
    /// event stream keeps them as an object and converts at the edge).
    pub arguments: String,
}

/// One tool-call entry: `{"id", "type": "function", "function"}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireToolCall {
    pub id: String,
    pub r#type: String,
    pub function: WireFunctionCall,
}

/// One chat message in the openai-compat request body.
///
/// Dialect v2: plain messages stay exactly `{"content", "role"}`
/// — byte-identical to dialect v1 — while tool round trips use the strict
/// openai wire shape: an assistant tool-call turn carries `tool_calls`
/// (and `content: null` when the turn had no text), and a `role:"tool"`
/// result carries `tool_call_id`. Strict OpenAI-compatible endpoints
/// (real DeepSeek verified, OpenAI documents the same shape) reject the
/// v1 dialect with HTTP 400/422 after any tool round trip.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    /// String content, or `null` for a purely tool-calling assistant turn.
    pub content: Option<String>,
    /// Present only on `role:"tool"` results (the wire `tool_call_id`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    /// Present only on assistant tool-call turns (the wire `tool_calls`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<WireToolCall>>,
    /// Thinking-mode reasoning the provider returned for this assistant
    /// turn. Replayed verbatim when present (DeepSeek V4 requires it);
    /// skipped entirely when absent, so legacy bodies stay byte-identical
    /// (ADR-022, DESIGN §3.2).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
}

impl ChatMessage {
    /// A plain `{role, content}` message (dialect v1 shape, unchanged).
    #[must_use]
    pub fn plain(role: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: role.into(),
            content: Some(content.into()),
            tool_call_id: None,
            tool_calls: None,
            reasoning_content: None,
        }
    }
}

/// SHA-256 of the golden request body asserted by
/// `same_logical_request_serializes_to_identical_bytes_golden_hash`.
///
/// **This constant is frozen stable content.** Changing the serialization
/// contract below (or this hash) invalidates provider prefix cache for
/// every session and is a constraint-3 event: it requires a DESIGN note
/// and an observable invalidation-cause record.
///
/// v2: strict openai tool wire shape — assistant `tool_calls`,
/// `role:"tool"` results with `tool_call_id`, dotless tool names. The v1
/// body (`4a1dad31…d7ce`) was rejected by strict OpenAI-compatible
/// endpoints (HTTP 400 dotted names / 422 missing `tool_call_id`) after
/// any tool round trip.
pub const FROZEN_CHAT_BODY_SHA256: &str =
    "b743584d30f65086566739871e1fc26f4967f587b1ed72923f7549f605207708";

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
/// - Message objects (dialect v2): `content` and `role` always;
///   `tool_calls` only on assistant tool-call turns; `tool_call_id` only
///   on `role:"tool"` results. Plain messages are byte-identical to the
///   v1 shape.
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

/// Default connect bound for one model call (ADR-041): TCP connect plus
/// the TLS handshake. `0` in the config disables the explicit bound.
pub const DEFAULT_PROVIDER_CONNECT_TIMEOUT_MS: u64 = 10_000;

/// Default idle bound for one model call (ADR-041): the wait for response
/// headers and each gap between streamed body chunks — never the total
/// request. `0` in the config disables the bound.
pub const DEFAULT_PROVIDER_IDLE_TIMEOUT_MS: u64 = 300_000;

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
    /// Connect bound in milliseconds (TCP + TLS). `0` = no explicit bound.
    pub connect_timeout_ms: u64,
    /// Idle bound in milliseconds: the wait for headers and each gap between
    /// body chunks, never the total request (ADR-041). `0` = disabled.
    pub idle_timeout_ms: u64,
}

struct Inner {
    config: OpenAiCompatConfig,
    client: reqwest::Client,
    sink: Arc<dyn PersistSink>,
    /// `idle_timeout_ms` as a duration; `None` when disabled (`0`).
    idle_timeout: Option<Duration>,
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
        let mut builder = reqwest::Client::builder();
        if config.connect_timeout_ms > 0 {
            builder = builder.connect_timeout(Duration::from_millis(config.connect_timeout_ms));
        }
        let client = builder
            .build()
            .expect("reqwest client construction cannot fail with rustls-tls");
        let idle_timeout =
            (config.idle_timeout_ms > 0).then(|| Duration::from_millis(config.idle_timeout_ms));
        Self(Arc::new(Inner {
            config,
            client,
            sink,
            idle_timeout,
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
            // ADR-030 rule for the model call: the wait for response
            // headers is bounded by the same idle bound as each body gap.
            let posted = match inner.idle_timeout {
                Some(idle) => match tokio::time::timeout(idle, inner.post(body)).await {
                    Ok(result) => result,
                    Err(_) => Err(ProviderError::Transport(format!(
                        "model request idle timeout: no response headers within {} ms",
                        idle.as_millis()
                    ))),
                },
                None => inner.post(body).await,
            };
            match posted {
                Ok(resp) => response_stream(resp, inner.idle_timeout),
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

/// Bounds every `next()` of the raw SSE byte stream by `idle` (ADR-041).
/// The bound is per gap, never a total-request cap: a stream that keeps
/// producing bytes is never cut. An expiry ends the stream with one
/// transport error (the `provider_down` class), so the turn settles
/// honestly instead of parking the drain.
fn idle_bounded<T, E>(
    stream: impl futures::Stream<Item = Result<T, E>> + Send + 'static,
    idle: Option<Duration>,
) -> BoxStream<'static, Result<T, ProviderError>>
where
    T: 'static,
    E: std::fmt::Display + 'static,
{
    let Some(idle) = idle else {
        return stream
            .map_err(|e| ProviderError::Transport(e.to_string()))
            .boxed();
    };
    futures::stream::unfold(Some(stream.boxed()), move |state| async move {
        let mut stream = state?;
        match tokio::time::timeout(idle, stream.next()).await {
            Ok(Some(Ok(item))) => Some((Ok(item), Some(stream))),
            Ok(Some(Err(e))) => Some((Err(ProviderError::Transport(e.to_string())), None)),
            Ok(None) => None,
            Err(_) => Some((
                Err(ProviderError::Transport(format!(
                    "model stream idle timeout: no chunk within {} ms",
                    idle.as_millis()
                ))),
                None,
            )),
        }
    })
    .boxed()
}

fn response_stream(
    resp: reqwest::Response,
    idle: Option<Duration>,
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
    let main = idle_bounded(resp.bytes_stream(), idle)
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
///
/// a frame carrying a `usage` object emits a chunk with
/// `delta: null` and the usage object **verbatim** (multiple frames all
/// surface; the consumer applies last-wins). A usage frame never flips
/// the saw-chunk liveness flag — a stream whose only frames are usage
/// frames is still an [`ProviderError::EmptyResponse`] soft failure
/// (constraint 7), while a usage frame after real content does not turn
/// the response empty.
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
    let mut items = Vec::new();
    match v.pointer("/choices/0/delta") {
        Some(delta) if !delta.as_object().is_some_and(|o| o.is_empty()) => {
            flags.0.store(true, Ordering::SeqCst);
            items.push(Ok(ModelChunk::new(delta.clone())));
        }
        // Empty deltas (e.g. The trailing finish_reason marker) and
        // keepalive frames carry no content for the loop.
        _ => {}
    }
    if let Some(usage) = v.get("usage").filter(|u| u.is_object()) {
        items.push(Ok(ModelChunk::with_usage(Value::Null, usage.clone())));
    }
    items
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
    use std::sync::atomic::{AtomicUsize, Ordering};
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
        ChatMessage, DEFAULT_PROVIDER_CONNECT_TIMEOUT_MS, DEFAULT_PROVIDER_IDLE_TIMEOUT_MS,
        OpenAiCompatConfig, OpenAiCompatProvider, PersistError, PersistSink, PersistedRequest,
        ProviderError, WireFunctionCall, WireToolCall, build_chat_body, serialize_request_body,
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
            connect_timeout_ms: DEFAULT_PROVIDER_CONNECT_TIMEOUT_MS,
            idle_timeout_ms: DEFAULT_PROVIDER_IDLE_TIMEOUT_MS,
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
            body: build_chat_body(model, &[ChatMessage::plain("user", "hello")], &[]),
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

    fn config_with_timeouts(
        base: &str,
        chat_path: &str,
        connect_ms: u64,
        idle_ms: u64,
    ) -> OpenAiCompatConfig {
        OpenAiCompatConfig {
            name: "openai-compat".into(),
            base_url: base.into(),
            api_key: "test-key".into(),
            chat_completions_path: chat_path.into(),
            connect_timeout_ms: connect_ms,
            idle_timeout_ms: idle_ms,
        }
    }

    /// How the raw stall server below behaves after accepting a request.
    #[derive(Clone, Copy)]
    enum StallScript {
        /// Never write a byte (the wait for response headers never ends).
        Silent,
        /// SSE headers + one partial content frame, then silence (a
        /// mid-stream stall after real progress).
        OneFrameThenSilence,
        /// Three frames 60 ms apart (180 ms total, above a 100 ms idle
        /// bound), then a clean close — slow but progressing.
        SlowButProgressing,
    }

    /// A raw HTTP/1.1 server for the ADR-041 stall tests: it reads the
    /// request, counts it, then performs the script. Deliberately raw
    /// (not wiremock) so a body can be *held open* with no further bytes.
    async fn spawn_stall_server(script: StallScript) -> (String, Arc<AtomicUsize>) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stall server");
        let addr = listener.local_addr().expect("stall server addr");
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&requests);
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let counter = Arc::clone(&counter);
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    let _ = socket.read(&mut buf).await;
                    counter.fetch_add(1, Ordering::SeqCst);
                    match script {
                        StallScript::Silent => {
                            tokio::time::sleep(Duration::from_secs(20)).await;
                        }
                        StallScript::OneFrameThenSilence => {
                            let _ = socket
                                .write_all(
                                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n",
                                )
                                .await;
                            let _ = socket
                                .write_all(
                                    b"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
                                )
                                .await;
                            let _ = socket.flush().await;
                            tokio::time::sleep(Duration::from_secs(20)).await;
                        }
                        StallScript::SlowButProgressing => {
                            let _ = socket
                                .write_all(
                                    b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\n\r\n",
                                )
                                .await;
                            for _ in 0..3 {
                                let _ = socket
                                    .write_all(
                                        b"data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n",
                                    )
                                    .await;
                                let _ = socket.flush().await;
                                tokio::time::sleep(Duration::from_millis(60)).await;
                            }
                        }
                    }
                });
            }
        });
        (format!("http://{addr}"), requests)
    }

    fn sink() -> Arc<dyn PersistSink> {
        Arc::new(|_record: PersistedRequest| async { Ok::<(), PersistError>(()) })
    }

    // -- ADR-041: the model call is bounded --------------------------------

    /// A provider that accepts the request and never sends response headers
    /// is ended by the idle bound instead of parking the caller forever.
    #[tokio::test]
    async fn an_idle_bound_ends_a_provider_that_never_sends_headers() {
        ensure_no_proxy();
        let (base, requests) = spawn_stall_server(StallScript::Silent).await;
        let provider = OpenAiCompatProvider::new(
            config_with_timeouts(&base, "/chat/completions", 1_000, 120),
            sink(),
        );
        let started = std::time::Instant::now();
        let out = collect(provider.complete(sample_request("m1"))).await;
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the idle bound must fire promptly"
        );
        assert_eq!(requests.load(Ordering::SeqCst), 1, "the request was sent");
        assert_eq!(out.len(), 1, "one terminal item: {out:?}");
        let err = format!("{:?}", out[0]);
        assert!(out[0].is_err(), "{err}");
        assert!(err.contains("idle timeout"), "{err}");
    }

    /// A stream that produced a chunk and then stalls is ended by the
    /// per-gap bound; the delivered chunk is not retracted.
    #[tokio::test]
    async fn an_idle_bound_ends_a_stream_that_stalls_mid_flight() {
        ensure_no_proxy();
        let (base, _requests) = spawn_stall_server(StallScript::OneFrameThenSilence).await;
        let provider = OpenAiCompatProvider::new(
            config_with_timeouts(&base, "/chat/completions", 1_000, 120),
            sink(),
        );
        let started = std::time::Instant::now();
        let out = collect(provider.complete(sample_request("m1"))).await;
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(
            out.iter().any(|item| item.is_ok()),
            "the first chunk must be delivered before the gap bound fires: {out:?}"
        );
        let last = out
            .last()
            .expect("terminal item")
            .as_ref()
            .expect_err("ends in an error");
        assert!(format!("{last:?}").contains("idle timeout"), "{last:?}");
    }

    /// The bound is per gap, not a total-request cap: a stream whose total
    /// duration exceeds the bound but never goes silent is not cut.
    #[tokio::test]
    async fn the_idle_bound_does_not_cut_a_slow_but_progressing_stream() {
        ensure_no_proxy();
        let (base, _requests) = spawn_stall_server(StallScript::SlowButProgressing).await;
        let provider = OpenAiCompatProvider::new(
            config_with_timeouts(&base, "/chat/completions", 1_000, 100),
            sink(),
        );
        let out = collect(provider.complete(sample_request("m1"))).await;
        assert!(
            out.iter().all(|item| item.is_ok()),
            "no error for a progressing stream: {out:?}"
        );
        assert!(
            out.len() >= 3,
            "all three frames are delivered (total > the 100 ms gap bound): {out:?}"
        );
    }

    /// `idle_timeout_ms: 0` is the explicit opt-out: the pre-ADR behavior,
    /// no idle bound is applied.
    #[tokio::test]
    async fn idle_timeout_zero_disables_the_bound() {
        ensure_no_proxy();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(sse_ok(&[
                r#"{"choices":[{"delta":{"content":"hi"}}]}"#,
                "[DONE]",
            ]))
            .mount(&server)
            .await;
        let provider = OpenAiCompatProvider::new(
            config_with_timeouts(&server.uri(), "/chat/completions", 1_000, 0),
            sink(),
        );
        let out = collect(provider.complete(sample_request("m1"))).await;
        assert_eq!(out.len(), 1, "{out:?}");
        assert!(out[0].is_ok(), "{out:?}");
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

    /// HTTP status → ProviderError classes used by the classifier.
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
    ///
    /// Dialect v2: the golden body now carries one full tool
    /// round trip in the strict openai wire shape — an assistant
    /// `tool_calls` turn (with `content: null`) followed by a
    /// `role:"tool"` result with `tool_call_id` — and dotless tool names
    /// (`fs_read`, `fs_write`), the shape strict OpenAI-compatible
    /// endpoints accept.
    #[test]
    fn same_logical_request_serializes_to_identical_bytes_golden_hash() {
        let tools = vec![
            ToolSchema {
                name: "fs_read".into(),
                description: "read a file".into(),
                parameters: json!({"type": "object"}),
            },
            ToolSchema {
                name: "fs_write".into(),
                description: "write a file".into(),
                parameters: json!({"type": "object"}),
            },
        ];
        let messages = vec![
            ChatMessage::plain("system", "you are kian"),
            ChatMessage::plain("user", "hello"),
            ChatMessage {
                role: "assistant".into(),
                content: None,
                tool_call_id: None,
                tool_calls: Some(vec![WireToolCall {
                    id: "call-1".into(),
                    r#type: "function".into(),
                    function: WireFunctionCall {
                        name: "fs_read".into(),
                        arguments: "{\"path\":\"notes.txt\"}".into(),
                    },
                }]),
                reasoning_content: None,
            },
            ChatMessage {
                role: "tool".into(),
                content: Some("{\"content\":\"hello kian\"}".into()),
                tool_call_id: Some("call-1".into()),
                tool_calls: None,
                reasoning_content: None,
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

    /// Dialect v2: the wire shape of a tool round trip is
    /// exactly the strict openai form — `tool_calls` with string
    /// `function.arguments` and a `tool_call_id` on the result — and
    /// plain messages stay byte-identical to the v1 shape (no extra
    /// keys).
    #[test]
    fn tool_round_trip_serializes_to_the_strict_openai_wire_shape() {
        let round_trip = vec![
            ChatMessage {
                role: "assistant".into(),
                content: None,
                tool_call_id: None,
                tool_calls: Some(vec![WireToolCall {
                    id: "call-9".into(),
                    r#type: "function".into(),
                    function: WireFunctionCall {
                        name: "fs_read".into(),
                        arguments: "{\"path\":\"a.txt\"}".into(),
                    },
                }]),
                reasoning_content: None,
            },
            ChatMessage {
                role: "tool".into(),
                content: Some("{\"content\":\"x\"}".into()),
                tool_call_id: Some("call-9".into()),
                tool_calls: None,
                reasoning_content: None,
            },
        ];
        let body = build_chat_body("m1", &round_trip, &[]);
        let wire = serialize_request_body(&body).unwrap();
        let v: Value = serde_json::from_slice(&wire).unwrap();
        let msgs = v["messages"].as_array().unwrap();
        assert_eq!(
            msgs[0]["tool_calls"][0]["function"]["arguments"],
            json!("{\"path\":\"a.txt\"}"),
            "wire arguments are a JSON-encoded string"
        );
        assert_eq!(msgs[0]["content"], Value::Null);
        assert_eq!(msgs[1]["tool_call_id"], json!("call-9"));
        // Plain messages carry exactly the two v1 keys.
        let plain = serialize_request_body(&build_chat_body(
            "m1",
            &[ChatMessage::plain("user", "hi")],
            &[],
        ))
        .unwrap();
        let v: Value = serde_json::from_slice(&plain).unwrap();
        assert_eq!(
            v["messages"][0]
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            vec!["content", "role"]
        );
    }

    /// Serialization is independent of the source order the JSON value was
    /// assembled in (serde_json sorts object keys; arrays keep caller
    /// order).
    #[test]
    fn body_serialization_is_source_key_order_independent() {
        let built = build_chat_body("m1", &[ChatMessage::plain("user", "hi")], &[]);
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
    // provider usage frames
    // ------------------------------------------------------------------

    /// A final usage frame after content deltas emits a usage-only chunk
    /// (Null delta, verbatim usage object) and the turn is NOT an
    /// EmptyResponse — the delta-bearing frames already flipped the
    /// liveness flag.
    #[tokio::test]
    async fn usage_frame_at_stream_end_is_parsed_verbatim_after_content() {
        ensure_no_proxy();
        let server = MockServer::start().await;
        let usage = json!({
            "prompt_tokens": 11,
            "completion_tokens": 7,
            "total_tokens": 18,
            "prompt_tokens_details": {"cached_tokens": 3}
        });
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(sse_ok(&[
                r#"{"choices":[{"delta":{"content":"hi"}}]}"#,
                r#"{"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18,"prompt_tokens_details":{"cached_tokens":3}}}"#,
                "[DONE]",
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
        assert_eq!(out.len(), 2, "{out:?}");
        assert!(out.iter().all(|r| r.is_ok()));
        assert_eq!(
            out[0].as_ref().unwrap(),
            &ModelChunk::new(json!({"content": "hi"}))
        );
        assert_eq!(
            out[1].as_ref().unwrap(),
            &ModelChunk::with_usage(Value::Null, usage.clone()),
            "the usage object rides verbatim with a Null delta"
        );
    }

    /// Usage may arrive mid-stream (before [DONE] and even before the
    /// final delta); multiple usage frames all surface as chunks — the
    /// consumer applies last-wins.
    #[tokio::test]
    async fn mid_stream_and_final_usage_frames_both_surface() {
        ensure_no_proxy();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(sse_ok(&[
                r#"{"usage":{"prompt_tokens":5}}"#,
                r#"{"choices":[{"delta":{"content":"lo"}}]}"#,
                r#"{"usage":{"prompt_tokens":9,"completion_tokens":2}}"#,
                "[DONE]",
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
        assert!(out.iter().all(|r| r.is_ok()));
        let usage_chunks: Vec<&ModelChunk> = out
            .iter()
            .map(|r| r.as_ref().unwrap())
            .filter(|c| c.usage.is_some())
            .collect();
        assert_eq!(usage_chunks.len(), 2);
        assert_eq!(usage_chunks[0].usage, Some(json!({"prompt_tokens": 5})));
        assert_eq!(
            usage_chunks[1].usage,
            Some(json!({"prompt_tokens": 9, "completion_tokens": 2})),
            "last-wins is the consumer's rule; the provider surfaces every frame"
        );
        // Usage chunks carry a Null delta (content never rides on them).
        assert!(usage_chunks.iter().all(|c| c.delta.is_null()));
    }

    /// Constraint 7 interaction: a usage frame alone flips nothing — the
    /// stream is still an EmptyResponse soft failure (the liveness flag
    /// counts delta-bearing frames only), and the usage chunk still
    /// surfaces before the error.
    #[tokio::test]
    async fn usage_only_stream_is_still_an_empty_response() {
        ensure_no_proxy();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(sse_ok(&[
                r#"{"usage":{"prompt_tokens":5,"completion_tokens":0}}"#,
                "[DONE]",
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
        assert_eq!(out.len(), 2, "{out:?}");
        assert_eq!(
            out[0].as_ref().unwrap(),
            &ModelChunk::with_usage(
                Value::Null,
                json!({"prompt_tokens": 5, "completion_tokens": 0})
            )
        );
        assert_eq!(out[1].as_ref().unwrap_err(), &ProviderError::EmptyResponse);
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
