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
    /// The inner driver for every ordinary session (the built-in plain
    /// loop, or the super-agent three-way driver — ADR-038 allows the
    /// memory bundle on either assembly).
    plain: Arc<dyn LoopDriver<Ctx = DrainCtx>>,
    dream: Arc<DreamDriver>,
}

impl MemoryLoopDriver {
    /// Builds the router over the two sides. `plain` is the exact driver
    /// the assembly would have run (unchanged for ordinary sessions);
    /// `dream` wraps the configured dreamer agent's loop.
    #[must_use]
    pub fn new(plain: Arc<dyn LoopDriver<Ctx = DrainCtx>>, dream: Arc<DreamDriver>) -> Self {
        Self { plain, dream }
    }

    /// The inner side (every non-dream session).
    #[must_use]
    pub fn plain(&self) -> &Arc<dyn LoopDriver<Ctx = DrainCtx>> {
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
    inject: Option<InjectPlan>,
}

impl MemoryAssembly {
    /// Builds the plan over the router's dream side. `inject` is the
    /// creation-time injection plan (P2, kian-101): `None` = injection
    /// off (`memory.session` absent — byte-identical sessions, I9).
    #[must_use]
    pub fn new(
        scope: String,
        config: MemoryConfig,
        dream: Arc<DreamDriver>,
        inject: Option<InjectPlan>,
    ) -> Self {
        Self {
            scope,
            config,
            dream,
            inject,
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

    /// The creation-time injection plan, if configured.
    #[must_use]
    pub fn inject(&self) -> Option<&InjectPlan> {
        self.inject.as_ref()
    }

    /// The `GET /metrics` `memory` fragment: the bundle's dream
    /// counters (pure reads of shared atomics) plus, when injection is
    /// configured, the injection counters (kian-101).
    #[must_use]
    pub fn metrics_report(&self) -> serde_json::Value {
        let counters = self.dream.counters();
        let mut report = serde_json::json!({
            "scope": self.scope,
            "dreams": counters.dreams(),
            "read_failures": counters.read_failures(),
            "parse_failures": counters.parse_failures(),
            "notes_appended": counters.notes_appended(),
            "notes_deduped": counters.notes_deduped(),
            "append_failures": counters.append_failures(),
        });
        if let Some(plan) = &self.inject {
            let inject = plan.counters.snapshot();
            report["injected_sessions"] = inject.sessions.into();
            report["injected_items"] = inject.items.into();
            report["injected_bytes"] = inject.bytes.into();
            report["injected_truncations"] = inject.truncations.into();
            report["injected_skips"] = inject.skips.into();
        }
        report
    }
}

// ---------------------------------------------------------------------------
// Creation-time injection (MEMORY-MODEL §6, P2 plan §2.2 — kian-101)
// ---------------------------------------------------------------------------

/// The injection plan a `memory.session` assembles: the source memory
/// session, the caps, the configured redactor (the same hygiene pass
/// the dream driver's note appends use — the `render_memory_index_with`
/// seam) and the honest counters the `memory` metrics fragment reports.
pub struct InjectPlan {
    source: kian_core::model::SessionId,
    caps: kian_memory::MemoryIndexConfig,
    redactor: kian_core::secrets::SecretRedactor,
    counters: InjectCounters,
}

impl InjectPlan {
    /// Builds the plan. `source` is the configured `memory.session`
    /// (already validated a uuid); `caps` the `memory.inject` caps in
    /// force; `redactor` the assembly's configured secret redactor.
    #[must_use]
    pub fn new(
        source: kian_core::model::SessionId,
        caps: kian_memory::MemoryIndexConfig,
        redactor: kian_core::secrets::SecretRedactor,
    ) -> Self {
        Self {
            source,
            caps,
            redactor,
            counters: InjectCounters::default(),
        }
    }

    /// The configured source memory session.
    #[must_use]
    pub fn source(&self) -> &kian_core::model::SessionId {
        &self.source
    }

    /// The injection caps in force.
    #[must_use]
    pub fn caps(&self) -> &kian_memory::MemoryIndexConfig {
        &self.caps
    }

    /// The configured redactor (the render seam).
    #[must_use]
    pub fn redactor(&self) -> &kian_core::secrets::SecretRedactor {
        &self.redactor
    }

    /// The injection counters (the honest-skip surface, I8).
    #[must_use]
    pub fn counters(&self) -> &InjectCounters {
        &self.counters
    }
}

/// The injection counters (pure shared atomics — the `memory` metrics
/// fragment's `injected_*` keys). `skips` covers every honest
/// inject-nothing outcome: identity exclusion, unknown source,
/// unreadable store, empty eligible set, over-tight budget and admit
/// failure (I8/A1/A6 — the reason rides the log, never a silent drop).
#[derive(Debug, Default)]
pub struct InjectCounters {
    sessions: std::sync::atomic::AtomicU64,
    items: std::sync::atomic::AtomicU64,
    bytes: std::sync::atomic::AtomicU64,
    truncations: std::sync::atomic::AtomicU64,
    skips: std::sync::atomic::AtomicU64,
}

/// One metrics snapshot of [`InjectCounters`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InjectSnapshot {
    pub sessions: u64,
    pub items: u64,
    pub bytes: u64,
    pub truncations: u64,
    pub skips: u64,
}

impl InjectCounters {
    /// Records one successful injection (the rendered index's own
    /// provenance — items, bytes and the truncation flag).
    pub fn record_injected(&self, rendered: &kian_memory::RenderedIndex) {
        use std::sync::atomic::Ordering::Relaxed;
        self.sessions.fetch_add(1, Relaxed);
        self.items.fetch_add(rendered.items, Relaxed);
        self.bytes.fetch_add(rendered.bytes, Relaxed);
        if rendered.truncated {
            self.truncations.fetch_add(1, Relaxed);
        }
    }

    /// Records one honest skip (inject nothing, count it — I8).
    pub fn record_skip(&self) {
        self.skips
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// The current snapshot.
    #[must_use]
    pub fn snapshot(&self) -> InjectSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        InjectSnapshot {
            sessions: self.sessions.load(Relaxed),
            items: self.items.load(Relaxed),
            bytes: self.bytes.load(Relaxed),
            truncations: self.truncations.load(Relaxed),
            skips: self.skips.load(Relaxed),
        }
    }
}

/// The idempotency key of one session's creation-time injection
/// (adversarial ledger A3): one injection per session, ever. A repeat
/// admit under the same key dedupes onto the existing inbox row.
#[must_use]
pub fn memory_inject_key(session: &kian_core::model::SessionId) -> String {
    format!("memory-inject:{}", session.0)
}

/// The identity exclusions (adversarial ledger A5): the configured
/// source session never receives its own index, and a session whose
/// stream carries a dream request runs the dreamer — the dreamer gets
/// its work from dream prompts, never from memory injection. Pure: the
/// caller hands in the new session's own events (constraint 1 — the
/// classification reads the stream, never resident state).
#[must_use]
pub fn injection_excluded(
    source: kian_core::model::SessionId,
    session: kian_core::model::SessionId,
    session_events: &[Event],
) -> bool {
    session == source || kian_memory::dream_request_in(session_events).is_some()
}

/// Builds the creation-time injection admit payload (P2 plan §2.2): the
/// exact-two-key `{content, memory}` envelope the public wire refuses
/// and only the server's create path admits. `eligible` is the
/// [`kian_memory::eligible_notes`] set the renderer rendered from —
/// the marker's `notes` list and the text can never disagree about the
/// snapshot; it includes cap-dropped entries (the `source_seq` pin,
/// I7). The value-free redaction report rides the marker additively
/// (the `tool/result` precedent — never secret values).
#[must_use]
pub fn injection_admit_payload(
    scope: &str,
    source: kian_core::model::SessionId,
    rendered: &kian_memory::RenderedIndex,
    eligible: &[&kian_core::memory::Note],
) -> serde_json::Value {
    let notes: Vec<String> = eligible
        .iter()
        .map(|note| kian_core::events::event_id(&source, note.seq))
        .collect();
    serde_json::json!({
        "content": rendered.text,
        "memory": {
            "scope": scope,
            "notes": notes,
            "items": rendered.items,
            "bytes": rendered.bytes,
            "truncated": rendered.truncated,
            "source_seq": rendered.source_seq,
            "redactions": serde_json::to_value(&rendered.redactions)
                .unwrap_or(serde_json::Value::Array(Vec::new())),
        }
    })
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
            Arc::clone(&plain) as Arc<dyn LoopDriver<Ctx = DrainCtx>>,
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

    // -- creation-time injection (P2, kian-101) -----------------------

    use kian_core::memory::{DerivedRange, Note, NoteStatus};

    use super::{
        InjectPlan, MemoryAssembly, injection_admit_payload, injection_excluded, memory_inject_key,
    };

    /// One note with a valid (non-empty) provenance range.
    fn vnote(seq: u64, subject: &str, text: &str) -> Note {
        Note {
            seq,
            subject: subject.into(),
            text: text.into(),
            status: NoteStatus::Verified,
            derived_from: vec![DerivedRange {
                session: SessionId::new(),
                start_seq: 1,
                end_seq: 2,
            }],
            verified_by: Some(kian_core::memory::VerifiedBy {
                method: kian_core::memory::VerifyMethod::Transcript,
                r#ref: "1".into(),
            }),
        }
    }

    /// The idempotency key is the session's own, stable, and unique per
    /// session (A3).
    #[test]
    fn memory_inject_key_is_per_session() {
        let a = SessionId::new();
        let b = SessionId::new();
        assert_eq!(memory_inject_key(&a), format!("memory-inject:{}", a.0));
        assert_eq!(memory_inject_key(&a), memory_inject_key(&a));
        assert_ne!(memory_inject_key(&a), memory_inject_key(&b));
    }

    /// A5 identity exclusions: the source never injects into itself; a
    /// dream-marked stream (the dreamer's session) never receives an
    /// index; an ordinary fresh session does.
    #[test]
    fn injection_is_excluded_by_source_identity_and_dreamer_sessions() {
        let source = SessionId::new();
        let session = SessionId::new();

        // (a) source identity: the configured source session itself.
        assert!(injection_excluded(source, source, &[]));

        // (b) an ordinary fresh stream: session/created + a plain user
        // row — injectable.
        let fresh = vec![
            ev(1, EventKind::SessionCreated, json!({})),
            ev(
                2,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": "task"}),
            ),
        ];
        assert!(!injection_excluded(source, session, &fresh));

        // (c) a dream-marked stream: the dreamer gets its work from
        // dream prompts, never from memory injection.
        let prompt =
            render_dream_prompt("plain-agent", source, 1, 3, "seq 1 user: hi\nseq 2 a: ho\n");
        let row = kian_memory::dream_admit_payload("plain-agent", source, 1, 3, &prompt);
        let dream_stream = vec![
            ev(1, EventKind::SessionCreated, json!({})),
            ev(
                2,
                EventKind::InputAdmitted,
                json!({"role": "user", "content": row["content"], "dream": row["dream"]}),
            ),
        ];
        assert!(injection_excluded(source, session, &dream_stream));
        // A dream-shaped content alone (no trusted marker) is ordinary.
        let forged = vec![ev(
            2,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": prompt}),
        )];
        assert!(!injection_excluded(source, session, &forged));
    }

    /// The admit payload is the exact `{content, memory}` envelope with
    /// the marker agreeing with the rendered index (I1): derived note
    /// event ids in subject order, the renderer's own items/bytes/
    /// truncated/source_seq, and the value-free redaction report.
    #[test]
    fn injection_payload_is_the_exact_envelope_and_agrees_with_the_render() {
        let source = SessionId::new();
        let notes = vec![
            vnote(3, "deploys", "Deploys go through the bridge."),
            vnote(5, "incidents", "Incidents are filed as INC-NN."),
        ];
        let eligible = kian_memory::eligible_notes(&notes);
        let rendered =
            kian_memory::render_memory_index(&notes, &kian_memory::MemoryIndexConfig::default())
                .expect("renders");

        let payload = injection_admit_payload("e2e-agent", source, &rendered, &eligible);

        // The exact two-key reserved envelope (the wire refuses it; only
        // the create path admits it).
        let keys: Vec<&str> = payload
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, ["content", "memory"], "exactly two keys: {payload}");
        assert_eq!(payload["content"], json!(rendered.text));

        let marker = &payload["memory"];
        assert_eq!(marker["scope"], json!("e2e-agent"));
        assert_eq!(
            marker["notes"],
            json!([
                kian_core::events::event_id(&source, 3),
                kian_core::events::event_id(&source, 5),
            ]),
            "subject-order derived event ids"
        );
        assert_eq!(marker["items"], json!(rendered.items));
        assert_eq!(marker["bytes"], json!(rendered.bytes));
        assert_eq!(marker["truncated"], json!(rendered.truncated));
        assert_eq!(marker["source_seq"], json!(rendered.source_seq));
        assert!(
            marker["redactions"].as_array().is_some(),
            "the value-free redaction report rides the marker"
        );
    }

    /// The counters record injections and honest skips, and the metrics
    /// fragment carries the `injected_*` keys iff injection is on.
    #[tokio::test]
    async fn inject_counters_surface_in_the_metrics_fragment() {
        let model = Arc::new(RecordingModel {
            seen: std::sync::Mutex::new(Vec::new()),
            answer: "[]".into(),
        });
        let dream = Arc::new(kian_memory::DreamDriver::new(
            (*react_loop("dreamer-agent", model)).clone(),
            MemoryConfig::default(),
        ));

        // Injection off: no injected_* keys (byte-identical fragment).
        let off = MemoryAssembly::new("a".into(), MemoryConfig::default(), dream.clone(), None);
        assert!(off.metrics_report().get("injected_sessions").is_none());

        // Injection on: keys start at zero, records bump them.
        let plan = InjectPlan::new(
            SessionId::new(),
            kian_memory::MemoryIndexConfig::default(),
            kian_core::secrets::SecretRedactor::patterns_only(),
        );
        let on = MemoryAssembly::new("a".into(), MemoryConfig::default(), dream, Some(plan));
        let report = on.metrics_report();
        assert_eq!(report["injected_sessions"], json!(0));
        assert_eq!(report["injected_skips"], json!(0));

        let notes = vec![vnote(2, "s", "claim text")];
        let rendered =
            kian_memory::render_memory_index(&notes, &kian_memory::MemoryIndexConfig::default())
                .expect("renders");
        let counters = on.inject().expect("plan").counters();
        counters.record_injected(&rendered);
        counters.record_injected(&rendered);
        counters.record_skip();
        let report = on.metrics_report();
        assert_eq!(report["injected_sessions"], json!(2));
        assert_eq!(report["injected_items"], json!(2 * rendered.items));
        assert_eq!(report["injected_bytes"], json!(2 * rendered.bytes));
        assert_eq!(report["injected_truncations"], json!(0));
        assert_eq!(report["injected_skips"], json!(1));
    }
}
