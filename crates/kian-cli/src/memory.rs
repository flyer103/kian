//! The agent-memory assembly (MEMORY-MODEL Phase 1, round-8 plan
//! kian-87): the config-gated driver routing and the dream-request
//! rendering behind `POST /ops/rpc memory_dream` and the `kian memory`
//! verbs.
//!
//! Two pieces:
//!
//! - [`MemoryLoopDriver`] — the config-gated replacement of
//!   [`ServeLoopDriver::Plain`](crate::superagent::ServeLoopDriver::Plain)
//!   when a `memory:` section is configured. Ordinary sessions keep
//!   the **same** [`ReActLoop`] handle (byte-identical request paths,
//!   constraint 3); a session whose stream carries a `dream`-marked
//!   `input/admitted` — the durable marker of the memory session, set
//!   only by the `memory_dream` ops admit and read from the stream
//!   alone (constraint 1; kian-91 review B-2: never inferred from
//!   message text) — runs the configured dreamer agent's
//!   [`DreamDriver`].
//! - [`render_transcript`] — the deterministic source-range rendering
//!   `memory_dream` quotes into the dream prompt (the transcript is
//!   the model's only ground truth; the same range always renders the
//!   same bytes).
//!
//! With no `memory:` section none of this is assembled (memory off,
//! byte-identical behavior).

use std::sync::Arc;

use futures::StreamExt;
use futures::stream::BoxStream;
use kian_core::events::Event;
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_loop::ReActLoop;
use kian_memory::{DreamDriver, MemoryConfig, dream_request_in};
use kian_orchestrator::DrainCtx;

/// The dream-routing driver: every ordinary session keeps the plain
/// loop; the memory session (a stream carrying a `dream`-marked
/// `input/admitted` row) runs the dreamer's [`DreamDriver`].
///
/// The classification happens per drain from the session's own event
/// stream (the engine appends the `input/admitted` consumption events
/// before it calls the driver), so a restarted server classifies a
/// takeover hold identically — no resident routing state (constraint
/// 1). The marker is server-set (kian-91 review B-2): a forged
/// `[kian-dream: ...]` header in user text is ordinary content and
/// routes to the plain side. `DreamDriver` itself is a passenger that
/// no-ops honestly on streams without a dream marker, so an ordinary
/// session that reaches the dream side by operator error still settles
/// normally.
pub struct MemoryLoopDriver {
    plain: Arc<ReActLoop>,
    dream: Arc<DreamDriver>,
}

impl MemoryLoopDriver {
    /// Builds the router over the two sides. `plain` is the exact loop
    /// the built-in assembly would have run (unchanged for ordinary
    /// sessions); `dream` wraps the configured dreamer agent's loop.
    #[must_use]
    pub fn new(plain: Arc<ReActLoop>, dream: Arc<DreamDriver>) -> Self {
        Self { plain, dream }
    }

    /// The plain side (every ordinary session).
    #[must_use]
    pub fn plain(&self) -> &Arc<ReActLoop> {
        &self.plain
    }

    /// The dream side (the memory session).
    #[must_use]
    pub fn dream(&self) -> &Arc<DreamDriver> {
        &self.dream
    }
}

impl LoopDriver for MemoryLoopDriver {
    type Ctx = DrainCtx;

    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        // Classify by stream content before the first provider turn
        // (the SuperAgentDriver pattern): a read failure defaults to
        // the plain side — the plain loop would fail the same read
        // honestly one step later, never silently.
        let plain = Arc::clone(&self.plain);
        let dream = Arc::clone(&self.dream);
        futures::stream::once(async move {
            let events = ctx.events().await.unwrap_or_default();
            if dream_request_in(&events).is_some() {
                dream.drive(ctx)
            } else {
                plain.drive(ctx)
            }
        })
        .flatten()
        .boxed()
    }
}

// ---------------------------------------------------------------------------
// The assembled memory plan (what the ops plane and /metrics see)
// ---------------------------------------------------------------------------

/// What a `memory:` section assembles: the scope, the caps in force
/// and the shared dream driver (its counters are the `memory` metrics
/// fragment). Cheap to share; carried by [`crate::server::Server`].
pub struct MemoryAssembly {
    scope: String,
    config: MemoryConfig,
    dream: Arc<DreamDriver>,
}

impl MemoryAssembly {
    /// Builds the plan over the router's dream side.
    #[must_use]
    pub fn new(scope: String, config: MemoryConfig, dream: Arc<DreamDriver>) -> Self {
        Self {
            scope,
            config,
            dream,
        }
    }

    /// The memory scope (rides the dream prompt header).
    #[must_use]
    pub fn scope(&self) -> &str {
        &self.scope
    }

    /// The caps in force.
    #[must_use]
    pub fn config(&self) -> &MemoryConfig {
        &self.config
    }

    /// The `GET /metrics` `memory` fragment: the bundle's dream
    /// counters (pure reads of shared atomics).
    #[must_use]
    pub fn metrics_report(&self) -> serde_json::Value {
        let counters = self.dream.counters();
        serde_json::json!({
            "scope": self.scope,
            "dreams": counters.dreams(),
            "read_failures": counters.read_failures(),
            "parse_failures": counters.parse_failures(),
            "notes_appended": counters.notes_appended(),
            "notes_deduped": counters.notes_deduped(),
            "append_failures": counters.append_failures(),
        })
    }
}

// ---------------------------------------------------------------------------
// The source transcript
// ---------------------------------------------------------------------------

/// Renders the source range's events as the transcript quoted into the
/// dream prompt: one deterministic line per event,
/// `seq <n> <kind>: <payload json>`. Same events → same bytes (the
/// rendered prompt is model-visible frozen content, constraint 3);
/// [`kian_memory::render_dream_prompt`] embeds it verbatim.
#[must_use]
pub fn render_transcript(events: &[Event]) -> String {
    let mut out = String::new();
    for ev in events {
        out.push_str(&format!(
            "seq {} {}: {}\n",
            ev.seq,
            ev.kind.as_str(),
            serde_json::to_string(&ev.bytes).unwrap_or_else(|_| "{}".into())
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use futures::StreamExt;
    use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{AgentDef, Budgets, Epoch, SessionId, SlotName, SlotSpec};
    use kian_core::seams::{
        LoopDriver, LoopOutcome, ModelChunk, ModelProvider, ModelRequest, PolicyRule,
        ProviderError, RecoveryPolicySource, ToolError, ToolProvider, ToolResult,
    };
    use kian_core::state::SessionState;
    use kian_llm::turn::ProviderRegistry;
    use kian_loop::ReActLoop;
    use kian_memory::{MemoryConfig, render_dream_prompt};
    use kian_orchestrator::{DrainCtx, ManualClock, Metrics};
    use kian_store::EventStore;
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{MemoryLoopDriver, render_transcript};

    const T0: u64 = 1_761_900_000_000;

    // -- transcript rendering ------------------------------------------

    fn ev(seq: u64, kind: EventKind, bytes: Value) -> Event {
        Event {
            seq,
            session: SessionId(Uuid::nil()),
            epoch: Epoch(1),
            ts: T0 + seq,
            kind,
            bytes,
            version: SCHEMA_VERSION,
        }
    }

    /// Same events render the same bytes; each event is one line with
    /// seq, kind and payload.
    #[test]
    fn transcript_rendering_is_deterministic() {
        let events = vec![
            ev(
                1,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "the deploy broke"}),
            ),
            ev(
                2,
                EventKind::AssistantMessage,
                json!({"role": "assistant", "content": {"text": "rolled back"}}),
            ),
        ];
        let rendered = render_transcript(&events);
        assert_eq!(rendered, render_transcript(&events));
        assert_eq!(
            rendered,
            "seq 1 input/admitted: {\"content\":\"the deploy broke\",\"role\":\"user\"}\n\
             seq 2 assistant/message: {\"content\":{\"text\":\"rolled back\"},\"role\":\"assistant\"}\n"
        );
        assert!(rendered.ends_with('\n'));
    }

    // -- routing + the request-bytes witness ----------------------------

    /// A scripted model that records every request it sees (the
    /// request-bytes witness).
    struct RecordingModel {
        seen: std::sync::Mutex<Vec<String>>,
        answer: String,
    }

    impl ModelProvider for RecordingModel {
        fn complete(
            &self,
            req: ModelRequest,
        ) -> futures::stream::BoxStream<'static, Result<ModelChunk, ProviderError>> {
            self.seen
                .lock()
                .unwrap()
                .push(serde_json::to_string(&req).expect("serialize request"));
            let answer = self.answer.clone();
            futures::stream::iter(vec![Ok(ModelChunk::new(json!({"content": answer})))]).boxed()
        }
    }

    struct NoopTools;
    impl ToolProvider for NoopTools {
        fn schemas(&self) -> Vec<kian_core::seams::ToolSchema> {
            Vec::new()
        }
        fn invoke(
            &self,
            _call: kian_core::seams::ToolCall,
        ) -> futures::future::BoxFuture<'static, Result<ToolResult, ToolError>> {
            Box::pin(async { Err(ToolError::InvalidCall("noop".into())) })
        }
    }

    struct NoopPolicy;
    impl RecoveryPolicySource for NoopPolicy {
        fn table(&self) -> Vec<PolicyRule> {
            Vec::new()
        }
    }

    fn agent_def(id: &str) -> AgentDef {
        AgentDef {
            id: id.into(),
            version: 1,
            model_slots: BTreeMap::from([(
                SlotName("main".into()),
                SlotSpec {
                    provider: "mock".into(),
                    model: "mock-model".into(),
                    fallback: Vec::new(),
                },
            )]),
            tools: vec![],
            budgets: Budgets {
                tokens: None,
                wall_clock_ms: None,
            },
        }
    }

    fn react_loop(agent_id: &str, model: Arc<RecordingModel>) -> Arc<ReActLoop> {
        let mut registry = ProviderRegistry::new();
        registry.register("mock", model);
        Arc::new(ReActLoop::with_config(
            agent_def(agent_id),
            Arc::new(registry),
            Arc::new(NoopTools),
            Arc::new(NoopPolicy),
            kian_loop::ReActConfig::default(),
        ))
    }

    fn temp_db_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("kian-cli-memory-{}.sqlite", Uuid::new_v4()))
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    /// A session whose stream carries a dream prompt routes to the
    /// dream side; an ordinary session routes to the plain side — the
    /// SAME `Arc<ReActLoop>` the built-in assembly would run, so its
    /// model request bytes are byte-identical with memory off (the
    /// witness).
    #[tokio::test]
    async fn ordinary_sessions_keep_the_plain_loop_and_request_bytes() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open store");

        // One shared recording model answers both sides' turns.
        let model = Arc::new(RecordingModel {
            seen: std::sync::Mutex::new(Vec::new()),
            answer: "[]".into(),
        });
        let plain = react_loop("plain-agent", Arc::clone(&model));
        let dreamer = react_loop("dreamer-agent", Arc::clone(&model));
        let router = MemoryLoopDriver::new(
            Arc::clone(&plain),
            Arc::new(kian_memory::DreamDriver::new(
                (*dreamer).clone(),
                MemoryConfig::default(),
            )),
        );

        // (a) an ordinary session: plain user row only.
        let sid = SessionId::new();
        store
            .admit(kian_core::model::InputRow {
                id: Uuid::new_v4(),
                session: sid,
                payload: Value::String("hello".into()),
                admitted_at: T0,
                eligible_at: T0,
                idempotency_key: None,
            })
            .await
            .expect("admit plain row");
        bootstrap_claim(&store, sid).await;
        let ctx = ctx_of(&store, sid).await;
        let outcomes: Vec<LoopOutcome> = router.drive(ctx).collect().await;
        assert_eq!(outcomes, [LoopOutcome::Settled]);
        let events = store.rebuild(sid).await.expect("rebuild");
        assert!(kian_memory::dream_request_in(&events).is_none());
        // Exactly one model request, produced by the plain loop.
        assert_eq!(model.seen.lock().unwrap().len(), 1);
        let plain_request = model.seen.lock().unwrap()[0].clone();

        // (b) the same ordinary session driven by the plain loop
        // directly (memory off) produces the SAME request bytes —
        // the constraint-3 witness.
        let sid2 = SessionId::new();
        store
            .admit(kian_core::model::InputRow {
                id: Uuid::new_v4(),
                session: sid2,
                payload: Value::String("hello".into()),
                admitted_at: T0,
                eligible_at: T0,
                idempotency_key: None,
            })
            .await
            .expect("admit plain row 2");
        bootstrap_claim(&store, sid2).await;
        let ctx2 = ctx_of(&store, sid2).await;
        let outcomes: Vec<LoopOutcome> = plain.drive(ctx2).collect().await;
        assert_eq!(outcomes, [LoopOutcome::Settled]);
        assert_eq!(model.seen.lock().unwrap().len(), 2);
        assert_eq!(
            model.seen.lock().unwrap()[1],
            plain_request,
            "ordinary-session request bytes must be unchanged by the memory assembly"
        );

        // (c) a dream session routes to the dream side and harvests.
        // kian-91 (review B-2): the dream row is the ops-plane
        // envelope — prompt content plus the trusted `dream` marker
        // (exactly two keys); consumption keeps the marker on the
        // admitted event, which is what routes the stream.
        let source = SessionId::new();
        let prompt =
            render_dream_prompt("plain-agent", source, 1, 3, "seq 1 user: hi\nseq 2 a: ho\n");
        let dream_payload = kian_memory::dream_admit_payload("plain-agent", source, 1, 3, &prompt);
        let sid3 = SessionId::new();
        store
            .admit(kian_core::model::InputRow {
                id: Uuid::new_v4(),
                session: sid3,
                payload: dream_payload,
                admitted_at: T0,
                eligible_at: T0,
                idempotency_key: None,
            })
            .await
            .expect("admit dream row");
        bootstrap_claim(&store, sid3).await;
        let ctx3 = ctx_of(&store, sid3).await;
        let outcomes: Vec<LoopOutcome> = router.drive(ctx3).collect().await;
        assert_eq!(outcomes, [LoopOutcome::Settled]);
        let events = store.rebuild(sid3).await.expect("rebuild");
        assert!(kian_memory::dream_request_in(&events).is_some());
        // "[]" is an honest zero: the dream ran (request count grew)
        // but no note events landed.
        assert_eq!(model.seen.lock().unwrap().len(), 3);
        assert!(
            !events
                .iter()
                .any(|ev| matches!(ev.kind, EventKind::MemoryNote))
        );

        store.close().await;
        cleanup(&path);
    }

    /// Appends `session/created` (if the stream is empty) plus a
    /// `drain/claimed` under epoch 1 — the minimum the ctx expects.
    async fn bootstrap_claim(store: &EventStore, sid: SessionId) {
        let head = store.head(sid).await.expect("head");
        if head.next_seq == 1 {
            store
                .append(
                    sid,
                    Event {
                        seq: 1,
                        session: sid,
                        epoch: Epoch::GENESIS,
                        ts: T0,
                        kind: EventKind::SessionCreated,
                        bytes: json!({}),
                        version: SCHEMA_VERSION,
                    },
                    kian_store::AppendGuard {
                        epoch: Epoch::GENESIS,
                        expected_seq: 1,
                    },
                )
                .await
                .expect("bootstrap session/created");
        }
    }

    async fn ctx_of(store: &EventStore, sid: SessionId) -> DrainCtx {
        let ctx = DrainCtx::new(
            store.clone(),
            sid,
            Epoch(1),
            Arc::new(ManualClock::new(T0)),
            Arc::new(Metrics::default()),
        )
        .await
        .expect("ctx");
        ctx.append(EventKind::DrainClaimed, json!({"holder": "test"}))
            .await
            .expect("claim");
        ctx
    }

    /// The claim path leaves a projectable stream (sanity for the
    /// fixtures above).
    #[tokio::test]
    async fn fixtures_project() {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open store");
        let sid = SessionId::new();
        bootstrap_claim(&store, sid).await;
        let events = store.rebuild(sid).await.expect("rebuild");
        assert!(matches!(
            kian_core::state::project(&events).unwrap(),
            SessionState::Idle
        ));
        store.close().await;
        cleanup(&path);
    }
}
