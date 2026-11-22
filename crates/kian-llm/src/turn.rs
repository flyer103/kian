//! One provider turn: failover traversal driven by slot bindings.
//!
//! `complete_turn` runs the provider turn for one slot against a provider
//! registry. When the primary target fails with a failover-class error
//! *before any chunk was emitted*, the fallback cursor advances, one
//! `slot/fallback` payload is handed to persistence, and the next target
//! in the chain is tried. Interaction with execution is via the
//! `ModelProvider` seam only (layered iron rule).

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use futures::StreamExt as _;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use kian_core::model::{SlotName, SlotTarget};
use kian_core::seams::{ModelChunk, ModelProvider, ModelRequest, ProviderError};
use serde_json::{Value, json};

use crate::plan::RequestPlan;
use crate::slots::{FallbackCursor, SlotBinding};

/// Registry of named [`ModelProvider`] implementations — the seam surface
/// slots resolve against (`SlotTarget::provider` is the key).
#[derive(Default)]
pub struct ProviderRegistry {
    providers: BTreeMap<String, Arc<dyn ModelProvider>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, name: impl Into<String>, provider: Arc<dyn ModelProvider>) {
        self.providers.insert(name.into(), provider);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn ModelProvider>> {
        self.providers.get(name).cloned()
    }
}

/// Receives the `slot/fallback` payload for persistence *before* the
/// fallback provider is called (one event per switch, ADR-006). A blanket
/// impl covers plain async closures.
pub trait SlotEventSink: Send + Sync {
    fn persist_fallback(&self, payload: Value) -> BoxFuture<'static, Result<(), String>>;
}

impl<F, Fut> SlotEventSink for F
where
    F: Fn(Value) -> Fut + Send + Sync,
    Fut: Future<Output = Result<(), String>> + Send + 'static,
{
    fn persist_fallback(&self, payload: Value) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(self(payload))
    }
}

/// Failover-class errors: the endpoint is unusable right now (provider
/// down / rate limited / transport broken). `ContextOverflow` is
/// deliberately *not* failover — it routes to `compress_then_retry` in the
/// recovery policy; `EmptyResponse` is a soft failure, not a switch.
fn is_failover_class(e: &ProviderError) -> bool {
    matches!(
        e,
        ProviderError::Unavailable(_) | ProviderError::RateLimited | ProviderError::Transport(_)
    )
}

type RequestPlanBuilder =
    Arc<dyn Fn(&SlotTarget) -> Result<RequestPlan, ProviderError> + Send + Sync>;

struct TurnCtx {
    registry: Arc<ProviderRegistry>,
    slot: SlotName,
    binding: SlotBinding,
    build: RequestPlanBuilder,
    event_sink: Arc<dyn SlotEventSink>,
}

enum TurnState {
    Start,
    Running {
        inner: BoxStream<'static, Result<ModelChunk, ProviderError>>,
        cursor: FallbackCursor,
        emitted: bool,
    },
    Done,
}

/// Run one provider turn for `slot` with automatic fallback traversal.
///
/// Ordering guarantees (all observable):
/// - Every provider call persists its request bytes before dialing (the
///   provider's own A2 hook).
/// - Each fallback switch persists its `slot/fallback` event *before* the
///   fallback provider is called.
/// - Failover happens only when no chunk has been emitted yet; a stream
///   that already yielded chunks propagates its error instead (restarting
///   would duplicate output — honesty over completion rate).
pub fn complete_turn(
    registry: Arc<ProviderRegistry>,
    slot: SlotName,
    binding: SlotBinding,
    build: RequestPlanBuilder,
    event_sink: Arc<dyn SlotEventSink>,
) -> BoxStream<'static, Result<ModelChunk, ProviderError>> {
    let ctx = Arc::new(TurnCtx {
        registry,
        slot,
        binding,
        build,
        event_sink,
    });
    futures::stream::unfold(TurnState::Start, move |state| {
        let ctx = Arc::clone(&ctx);
        async move { turn_step(state, ctx).await }
    })
    .boxed()
}

async fn turn_step(
    mut state: TurnState,
    ctx: Arc<TurnCtx>,
) -> Option<(Result<ModelChunk, ProviderError>, TurnState)> {
    loop {
        match state {
            TurnState::Start => {
                let cursor = FallbackCursor::new(ctx.slot.clone(), &ctx.binding);
                let target = cursor.current().clone();
                match ctx.registry.get(&target.provider) {
                    None => {
                        return Some((Err(no_provider_error(&ctx, &target)), TurnState::Done));
                    }
                    Some(provider) => {
                        let plan = match (ctx.build)(&target) {
                            Ok(plan) => plan,
                            Err(e) => return Some((Err(e), TurnState::Done)),
                        };
                        let inner = provider.complete(ModelRequest {
                            slot: ctx.slot.clone(),
                            body: plan.body,
                        });
                        state = TurnState::Running {
                            inner,
                            cursor,
                            emitted: false,
                        };
                    }
                }
            }
            TurnState::Running {
                mut inner,
                mut cursor,
                mut emitted,
            } => match inner.next().await {
                Some(Ok(chunk)) => {
                    emitted = true;
                    return Some((
                        Ok(chunk),
                        TurnState::Running {
                            inner,
                            cursor,
                            emitted,
                        },
                    ));
                }
                Some(Err(e)) => {
                    if emitted || !is_failover_class(&e) {
                        return Some((Err(e), TurnState::Done));
                    }
                    let Some(mut switch) = cursor.advance() else {
                        return Some((Err(e), TurnState::Done));
                    };
                    // Build the exact fallback attempt before persisting:
                    // the event must carry the bytes the next call sends,
                    // so replay can reconstruct the retry (ADR-053).
                    let plan = match (ctx.build)(&switch.to) {
                        Ok(plan) => plan,
                        Err(err) => return Some((Err(err), TurnState::Done)),
                    };
                    attach_plan(&mut switch.payload, &plan);
                    // Persist the slot/fallback event before switching;
                    // if persistence fails we do NOT silently continue.
                    if let Err(err) = ctx
                        .event_sink
                        .persist_fallback(switch.payload.clone())
                        .await
                    {
                        return Some((
                            Err(ProviderError::Other(format!(
                                "slot/fallback persistence failed, not switching: {err}"
                            ))),
                            TurnState::Done,
                        ));
                    }
                    let target = switch.to.clone();
                    match ctx.registry.get(&target.provider) {
                        None => {
                            return Some((Err(no_provider_error(&ctx, &target)), TurnState::Done));
                        }
                        Some(provider) => {
                            let next = provider.complete(ModelRequest {
                                slot: ctx.slot.clone(),
                                body: plan.body,
                            });
                            state = TurnState::Running {
                                inner: next,
                                cursor,
                                emitted: false,
                            };
                        }
                    }
                }
                None => {
                    // The provider already surfaces an empty turn as
                    // EmptyResponse; this arm is the honest backstop.
                    return if emitted {
                        None
                    } else {
                        Some((Err(ProviderError::EmptyResponse), TurnState::Done))
                    };
                }
            },
            TurnState::Done => return None,
        }
    }
}

/// Adds the attempt metadata every persisted fallback carries. The body is
/// included because it is the exact byte copy the retry sends.
fn attach_plan(payload: &mut Value, plan: &RequestPlan) {
    payload["body"] = plan.body.clone();
    payload["body_version"] = json!(plan.body_version);
    payload["dialect"] = json!(plan.dialect);
    payload["body_sha256"] = json!(plan.body_sha256);
    payload["estimate"] = json!(plan.estimate);
    if let Some(digest) = &plan.profile_digest {
        payload["profile_digest"] = json!(digest);
    }
}

fn no_provider_error(ctx: &TurnCtx, target: &SlotTarget) -> ProviderError {
    ProviderError::Other(format!(
        "no provider registered for `{}` (slot `{}`)",
        target.provider, ctx.slot.0
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use futures::{FutureExt as _, StreamExt, future::BoxFuture};
    use kian_core::model::{AgentDef, Budgets, SessionId, SlotName, SlotSpec, SlotTarget};
    use kian_core::seams::ProviderError;
    use serde_json::Value;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{ProviderRegistry, SlotEventSink, complete_turn};
    use crate::provider::{
        ChatMessage, DEFAULT_PROVIDER_CONNECT_TIMEOUT_MS, DEFAULT_PROVIDER_IDLE_TIMEOUT_MS,
        OpenAiCompatConfig, OpenAiCompatProvider, PersistError, PersistSink, PersistedRequest,
    };
    use crate::slots::SlotResolver;

    fn ensure_no_proxy() {
        // SAFETY: test-only; every test writes the same value before any
        // reqwest client is built.
        unsafe {
            std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
            std::env::set_var("no_proxy", "127.0.0.1,localhost");
        }
    }

    /// Records the ordered sequence of persist callbacks across all
    /// providers: "req:<model>" for step/request, "fallback:<to-model>"
    /// for slot/fallback events.
    #[derive(Clone)]
    struct OrderingSink {
        log: Arc<Mutex<Vec<String>>>,
        payloads: Arc<Mutex<Vec<Value>>>,
    }

    impl PersistSink for OrderingSink {
        fn persist_request(
            &self,
            record: PersistedRequest,
        ) -> BoxFuture<'static, Result<(), PersistError>> {
            let log = self.log.clone();
            async move {
                log.lock().unwrap().push(format!("req:{}", record.model));
                Ok(())
            }
            .boxed()
        }
    }

    impl SlotEventSink for OrderingSink {
        fn persist_fallback(&self, payload: Value) -> BoxFuture<'static, Result<(), String>> {
            let log = self.log.clone();
            let payloads = self.payloads.clone();
            async move {
                log.lock().unwrap().push(format!(
                    "fallback:{}",
                    payload["to"]["model"].as_str().unwrap_or("?")
                ));
                payloads.lock().unwrap().push(payload);
                Ok(())
            }
            .boxed()
        }
    }

    fn sse_ok(frames: &[&str]) -> ResponseTemplate {
        let body: String = frames.iter().map(|f| format!("data: {f}\n\n")).collect();
        ResponseTemplate::new(200)
            .set_body_string(body)
            .insert_header("content-type", "text/event-stream")
    }

    fn provider_config(name: &str, base: &str, chat_path: &str) -> OpenAiCompatConfig {
        OpenAiCompatConfig {
            name: name.into(),
            base_url: base.into(),
            api_key: "test-key".into(),
            chat_completions_path: chat_path.into(),
            connect_timeout_ms: DEFAULT_PROVIDER_CONNECT_TIMEOUT_MS,
            idle_timeout_ms: DEFAULT_PROVIDER_IDLE_TIMEOUT_MS,
        }
    }

    fn resolver_for(provider: &str, model: &str, fallback: &[SlotTarget]) -> SlotResolver {
        let def = AgentDef {
            id: "test-agent".into(),
            version: 1,
            model_slots: BTreeMap::from([(
                SlotName("main".into()),
                SlotSpec {
                    provider: provider.into(),
                    model: model.into(),
                    fallback: fallback.to_vec(),
                    profile: None,
                },
            )]),
            tools: vec![],
            budgets: Budgets {
                tokens: None,
                wall_clock_ms: None,
            },
        };
        SlotResolver::from_agent_def(&def)
    }

    /// Primary 5xx → one `slot/fallback` event is persisted, then the
    /// fallback provider is called (with the fallback model) and the turn
    /// completes from it.
    #[tokio::test]
    async fn primary_5xx_emits_fallback_event_then_fallback_provider_succeeds() {
        ensure_no_proxy();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/a/chat/completions"))
            .respond_with(ResponseTemplate::new(500).set_body_string("primary down"))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/b/chat/completions"))
            .respond_with(sse_ok(&[
                r#"{"choices":[{"delta":{"content":"sa"}}]}"#,
                r#"{"choices":[{"delta":{"content":"ved"}}]}"#,
                "[DONE]",
            ]))
            .expect(1)
            .mount(&server)
            .await;

        let sink = OrderingSink {
            log: Arc::new(Mutex::new(Vec::new())),
            payloads: Arc::new(Mutex::new(Vec::new())),
        };
        let mut registry = ProviderRegistry::new();
        registry.register(
            "primary",
            Arc::new(OpenAiCompatProvider::new(
                provider_config("primary", &server.uri(), "/a/chat/completions"),
                Arc::new(sink.clone()),
            )),
        );
        registry.register(
            "backup",
            Arc::new(OpenAiCompatProvider::new(
                provider_config("backup", &server.uri(), "/b/chat/completions"),
                Arc::new(sink.clone()),
            )),
        );

        let resolver = resolver_for(
            "primary",
            "model-a",
            &[SlotTarget {
                provider: "backup".into(),
                model: "model-b".into(),
                profile: None,
            }],
        );
        let slots = resolver.bind_session(SessionId::new());
        let main = SlotName("main".into());
        let binding = slots.binding(&main).unwrap().clone();

        let build = Arc::new(|t: &SlotTarget| {
            crate::plan::build_request_plan(
                None,
                &t.model,
                &[ChatMessage::plain("user", "hello")],
                &[],
            )
        });

        let stream = complete_turn(
            Arc::new(registry),
            main.clone(),
            binding,
            build,
            Arc::new(sink.clone()),
        );
        let chunks = stream
            .filter(|r| futures::future::ready(r.is_ok()))
            .count()
            .await;
        assert_eq!(
            chunks, 2,
            "the turn must complete from the fallback provider"
        );

        // Exact durable ordering: primary step/request, slot/fallback
        // event, fallback step/request (whose own persist precedes its
        // send, proving the fallback call happened after the event).
        assert_eq!(
            sink.log.lock().unwrap().as_slice(),
            ["req:model-a", "fallback:model-b", "req:model-b"]
        );

        // The fallback event carries the exact attempt: body and metadata
        // are persisted before the fallback call (ADR-053).
        {
            let payloads = sink.payloads.lock().unwrap();
            let payload = payloads.first().expect("fallback payload");
            assert_eq!(payload["body"]["model"], serde_json::json!("model-b"));
            assert_eq!(payload["body_version"], serde_json::json!("v1"));
            assert_eq!(payload["dialect"], serde_json::json!("openai_chat"));
            assert_eq!(payload["body_sha256"].as_str().map(str::len), Some(64));
            assert!(payload["estimate"]["high"].as_u64().unwrap() > 0);
            assert_eq!(payload["to"]["model"], serde_json::json!("model-b"));
        }

        // Exactly one request hit each provider, and the fallback request
        // carried the fallback model.
        let requests = server.received_requests().await.unwrap();
        let to_a: Vec<_> = requests
            .iter()
            .filter(|r| r.url.path().starts_with("/a/"))
            .collect();
        let to_b: Vec<_> = requests
            .iter()
            .filter(|r| r.url.path().starts_with("/b/"))
            .collect();
        assert_eq!(to_a.len(), 1);
        assert_eq!(to_b.len(), 1);
        let wire: Value = serde_json::from_slice(&to_b[0].body).unwrap();
        assert_eq!(wire["model"], serde_json::json!("model-b"));
    }

    /// Chain exhausted: the last error propagates and no phantom fallback
    /// events appear.
    #[tokio::test]
    async fn fallback_exhaustion_returns_last_error_without_extra_events() {
        ensure_no_proxy();
        let server = MockServer::start().await;
        for p in ["/a/chat/completions", "/b/chat/completions"] {
            Mock::given(method("POST"))
                .and(path(p))
                .respond_with(ResponseTemplate::new(503).set_body_string("down"))
                .mount(&server)
                .await;
        }

        let sink = OrderingSink {
            log: Arc::new(Mutex::new(Vec::new())),
            payloads: Arc::new(Mutex::new(Vec::new())),
        };
        let mut registry = ProviderRegistry::new();
        registry.register(
            "primary",
            Arc::new(OpenAiCompatProvider::new(
                provider_config("primary", &server.uri(), "/a/chat/completions"),
                Arc::new(sink.clone()),
            )),
        );
        registry.register(
            "backup",
            Arc::new(OpenAiCompatProvider::new(
                provider_config("backup", &server.uri(), "/b/chat/completions"),
                Arc::new(sink.clone()),
            )),
        );

        let resolver = resolver_for(
            "primary",
            "model-a",
            &[SlotTarget {
                provider: "backup".into(),
                model: "model-b".into(),
                profile: None,
            }],
        );
        let slots = resolver.bind_session(SessionId::new());
        let main = SlotName("main".into());
        let binding = slots.binding(&main).unwrap().clone();

        let build = Arc::new(|t: &SlotTarget| {
            crate::plan::build_request_plan(
                None,
                &t.model,
                &[ChatMessage::plain("user", "hello")],
                &[],
            )
        });

        let stream = complete_turn(
            Arc::new(registry),
            main,
            binding,
            build,
            Arc::new(sink.clone()),
        );
        let mut last = None;
        let mut ok_chunks = 0;
        futures::pin_mut!(stream);
        while let Some(item) = stream.next().await {
            match item {
                Ok(_) => ok_chunks += 1,
                Err(e) => last = Some(e),
            }
        }
        assert_eq!(ok_chunks, 0);
        assert_eq!(
            last,
            Some(ProviderError::Unavailable("HTTP 503: down".into())),
            "the final error must surface"
        );
        assert_eq!(
            sink.log.lock().unwrap().as_slice(),
            ["req:model-a", "fallback:model-b", "req:model-b"],
            "exactly one fallback switch, no phantom retries"
        );
    }

    /// Non-failover errors (e.g. a malformed-request 400) propagate without
    /// touching the fallback chain.
    #[tokio::test]
    async fn non_failover_error_propagates_without_fallback() {
        ensure_no_proxy();
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/a/chat/completions"))
            .respond_with(
                ResponseTemplate::new(400).set_body_string(r#"{"error":{"message":"bad ask"}}"#),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/b/chat/completions"))
            .respond_with(sse_ok(&["[DONE]"]))
            .expect(0)
            .mount(&server)
            .await;

        let sink = OrderingSink {
            log: Arc::new(Mutex::new(Vec::new())),
            payloads: Arc::new(Mutex::new(Vec::new())),
        };
        let mut registry = ProviderRegistry::new();
        registry.register(
            "primary",
            Arc::new(OpenAiCompatProvider::new(
                provider_config("primary", &server.uri(), "/a/chat/completions"),
                Arc::new(sink.clone()),
            )),
        );
        registry.register(
            "backup",
            Arc::new(OpenAiCompatProvider::new(
                provider_config("backup", &server.uri(), "/b/chat/completions"),
                Arc::new(sink.clone()),
            )),
        );

        let resolver = resolver_for(
            "primary",
            "model-a",
            &[SlotTarget {
                provider: "backup".into(),
                model: "model-b".into(),
                profile: None,
            }],
        );
        let slots = resolver.bind_session(SessionId::new());
        let main = SlotName("main".into());
        let binding = slots.binding(&main).unwrap().clone();

        let build = Arc::new(|t: &SlotTarget| {
            crate::plan::build_request_plan(
                None,
                &t.model,
                &[ChatMessage::plain("user", "hello")],
                &[],
            )
        });

        let stream = complete_turn(
            Arc::new(registry),
            main,
            binding,
            build,
            Arc::new(sink.clone()),
        );
        let out: Vec<_> = stream.collect().await;
        assert_eq!(out.len(), 1);
        assert!(matches!(
            out[0].as_ref().unwrap_err(),
            ProviderError::InvalidResponse(_)
        ));
        assert_eq!(
            sink.log.lock().unwrap().as_slice(),
            ["req:model-a"],
            "no fallback may fire for a non-failover error"
        );
    }

    /// An unregistered provider name surfaces as an explicit error before
    /// any HTTP traffic.
    #[tokio::test]
    async fn unknown_provider_name_is_an_explicit_error() {
        let sink = OrderingSink {
            log: Arc::new(Mutex::new(Vec::new())),
            payloads: Arc::new(Mutex::new(Vec::new())),
        };
        let registry = ProviderRegistry::new();

        let resolver = resolver_for("ghost", "model-a", &[]);
        let slots = resolver.bind_session(SessionId::new());
        let main = SlotName("main".into());
        let binding = slots.binding(&main).unwrap().clone();

        let build = Arc::new(|t: &SlotTarget| {
            crate::plan::build_request_plan(
                None,
                &t.model,
                &[ChatMessage::plain("user", "hello")],
                &[],
            )
        });

        let out: Vec<_> = complete_turn(
            Arc::new(registry),
            main,
            binding,
            build,
            Arc::new(sink.clone()),
        )
        .collect()
        .await;
        assert_eq!(out.len(), 1);
        match out[0].as_ref().unwrap_err() {
            ProviderError::Other(msg) => assert!(msg.contains("ghost"), "{msg}"),
            other => panic!("expected Other, got {other:?}"),
        }
        assert!(sink.log.lock().unwrap().is_empty());
    }
}
