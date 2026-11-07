//! Test-only fixtures for kian-loop (mock provider, mock tools, recording
//! policy). Compiled under `cfg(test)`; not part of the public surface.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use kian_core::model::{AgentDef, Budgets, SlotName, SlotSpec};
use kian_core::seams::{
    ModelChunk, ModelProvider, ModelRequest, PolicyRule, ProviderError, RecoveryPolicySource,
    ToolCall, ToolError, ToolProvider, ToolResult,
};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Agent definition
// ---------------------------------------------------------------------------

/// The agent definition every loop test binds: one `main` slot pointing
/// at the `mock` provider.
pub fn agent_def() -> AgentDef {
    AgentDef {
        id: "react-test".into(),
        version: 1,
        model_slots: BTreeMap::from([(
            SlotName("main".into()),
            SlotSpec {
                provider: "mock".into(),
                model: "mock-model".into(),
                fallback: Vec::new(),
            },
        )]),
        tools: vec!["fs_read".into(), "fs_write".into(), "exec".into()],
        budgets: Budgets {
            tokens: None,
            wall_clock_ms: None,
        },
    }
}

// ---------------------------------------------------------------------------
// No-op seam impls
// ---------------------------------------------------------------------------

pub struct NoopTools;
impl ToolProvider for NoopTools {
    fn schemas(&self) -> Vec<kian_core::seams::ToolSchema> {
        Vec::new()
    }
    fn invoke(&self, _call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        Box::pin(async { Err(ToolError::InvalidCall("noop".into())) })
    }
}

pub struct NoopPolicy;
impl RecoveryPolicySource for NoopPolicy {
    fn table(&self) -> Vec<PolicyRule> {
        Vec::new()
    }
}

// ---------------------------------------------------------------------------
// Scripted provider
// ---------------------------------------------------------------------------

/// One scripted provider turn: a list of stream items. An empty list is a
/// successful stream that yields nothing (the empty-response case).
pub type ScriptedChunks = Vec<Result<ModelChunk, ProviderError>>;

/// Async request hook: runs (awaited) at first poll of each turn's
/// stream, before any chunk is yielded.
type RequestHook = Arc<dyn Fn(usize) -> BoxFuture<'static, ()> + Send + Sync>;

/// A `ModelProvider` that replays scripted turns in order, records every
/// request body it saw, and optionally awaits a hook when a request is
/// built (fault-injection point — the hook runs before the first chunk
/// is polled, so interrupts/steers land deterministically mid-turn).
pub struct ScriptedProvider {
    turns: Mutex<VecDeque<ScriptedChunks>>,
    requests: Mutex<Vec<Value>>,
    hook: Mutex<Option<RequestHook>>,
    call_count: AtomicUsize,
}

impl ScriptedProvider {
    pub fn new(turns: Vec<ScriptedChunks>) -> Self {
        Self {
            turns: Mutex::new(turns.into()),
            requests: Mutex::new(Vec::new()),
            hook: Mutex::new(None),
            call_count: AtomicUsize::new(0),
        }
    }

    /// Installs a hook awaited (with the 1-based call number) when a
    /// request arrives — the injection point for interrupts/steers.
    pub fn on_request<F, Fut>(&self, hook: F)
    where
        F: Fn(usize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        *self.hook.lock().unwrap() = Some(Arc::new(move |n| Box::pin(hook(n))));
    }

    pub fn requests(&self) -> Vec<Value> {
        self.requests.lock().unwrap().clone()
    }
}

impl ModelProvider for ScriptedProvider {
    fn complete(&self, req: ModelRequest) -> BoxStream<'static, Result<ModelChunk, ProviderError>> {
        self.requests.lock().unwrap().push(req.body.clone());
        let n = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
        let hook = self.hook.lock().unwrap().clone();
        let turn = self
            .turns
            .lock()
            .unwrap()
            .pop_front()
            // Script exhausted: an honest empty response, never a hang.
            .unwrap_or_default();
        // The hook runs at first poll, before any chunk is yielded.
        let state = (hook, n, turn.into_iter(), false);
        futures::stream::unfold(state, |(hook, n, mut iter, mut hook_done)| async move {
            if !hook_done {
                hook_done = true;
                if let Some(hook) = &hook {
                    hook(n).await;
                }
            }
            iter.next().map(|item| (item, (hook, n, iter, hook_done)))
        })
        .boxed()
    }
}

/// Convenience: one chunk carrying plain content text.
pub fn text_chunk(text: &str) -> Result<ModelChunk, ProviderError> {
    Ok(ModelChunk::new(json!({"content": text})))
}

/// Convenience: one usage-only chunk — a Null delta plus the
/// provider's usage object verbatim.
pub fn usage_chunk(usage: Value) -> Result<ModelChunk, ProviderError> {
    Ok(ModelChunk::with_usage(Value::Null, usage))
}

/// Convenience: one chunk requesting a tool call.
pub fn tool_call_chunk(
    id: &str,
    name: &str,
    arguments: Value,
) -> Result<ModelChunk, ProviderError> {
    Ok(ModelChunk::new(json!({"tool_calls": [{
        "index": 0,
        "id": id,
        "function": {"name": name, "arguments": arguments.to_string()}
    }]})))
}

// ---------------------------------------------------------------------------
// Recording policy
// ---------------------------------------------------------------------------

/// A policy source that records that its table was consulted.
pub struct RecordingPolicy {
    pub rules: Vec<PolicyRule>,
    pub consulted: AtomicBool,
}

impl RecordingPolicy {
    pub fn with_empty_response_rule() -> Self {
        Self {
            rules: vec![PolicyRule {
                class: "empty_response".into(),
                action: kian_core::seams::RecoveryAction::InterruptAsk,
            }],
            consulted: AtomicBool::new(false),
        }
    }
}

impl RecoveryPolicySource for RecordingPolicy {
    fn table(&self) -> Vec<PolicyRule> {
        self.consulted.store(true, Ordering::SeqCst);
        self.rules.clone()
    }
}

// ---------------------------------------------------------------------------
// Hook tools
// ---------------------------------------------------------------------------

/// A `ToolProvider` whose every invocation runs an async test hook
/// returning the tool result (fault-injection and observation point —
/// the hook may await the store, spawn interrupts, …).
pub struct HookTools {
    pub hook: Arc<dyn Fn(ToolCall) -> BoxFuture<'static, ToolResult> + Send + Sync>,
}

impl HookTools {
    pub fn new(
        hook: Arc<dyn Fn(ToolCall) -> BoxFuture<'static, ToolResult> + Send + Sync>,
    ) -> Self {
        Self { hook }
    }

    /// Results that always succeed with a canned output; records the
    /// calls it served.
    pub fn ok(output: Value) -> (Self, Arc<Mutex<Vec<ToolCall>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = Arc::clone(&seen);
        let tools = Self::new(Arc::new(move |call| {
            seen2.lock().unwrap().push(call.clone());
            let output = output.clone();
            Box::pin(async move {
                let call_id = call.id;
                ToolResult {
                    call_id,
                    output,
                    is_error: false,
                    redactions: Vec::new(),
                }
            })
        }));
        (tools, seen)
    }
}

impl ToolProvider for HookTools {
    fn schemas(&self) -> Vec<kian_core::seams::ToolSchema> {
        Vec::new()
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        let fut = (self.hook)(call);
        Box::pin(async move { Ok(fut.await) })
    }
}
