//! `DreamDriver` — the dreaming drain (MEMORY-MODEL §5, round-8 plan
//! §2.4).
//!
//! A thin wrapper around the built-in [`ReActLoop`]: every outcome is
//! forwarded **unchanged** (the loop owns termination honesty); on
//! `Settled` — and only then — the driver harvests notes from the
//! session's own event stream:
//!
//! 1. locate the **last** dream request — the last `input/admitted`
//!    row carrying the trusted, server-set `dream` marker (kian-91,
//!    review B-2: never inferred from message text),
//! 2. locate the **last assistant message after it** (the dream's
//!    answer),
//! 3. parse it fail-closed ([`parse_notes`]) — a parse failure appends
//!    nothing and counts (honest zero, never a fabricated note),
//! 4. fold the answer's drafts by subject keeping the LAST entry
//!    (kian-91, review M-2 — the write-end twin of the fold's
//!    latest-per-subject rule), then skip drafts whose
//!    `(subject, derived_from)` already exist in the stream — the
//!    crash-resume dedupe: a takeover drain that re-runs a dream whose
//!    notes already landed appends nothing twice,
//! 5. redact each note's `text` through the [`kian_core::secrets`]
//!    redactor, attaching the aggregate report on the additive
//!    `redactions` key (kian-91, review M-1 / MEMORY-MODEL N4),
//! 6. append one `memory/note` per remaining draft through the fenced
//!    [`DrainCtx`] path, then yield `Settled`.
//!
//! The driver is a passenger (layered iron rule): it only appends
//! events and reads the stream; it never mutates projections. Notes are
//! appended *before* the engine's end write, so a completed dream
//! always carries its notes.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::stream::{BoxStream, StreamExt};
use kian_core::events::{Event, EventKind};
use kian_core::memory::{self, DerivedRange};
use kian_core::seams::{LoopDriver, LoopOutcome};
use kian_core::secrets::SecretRedactor;
use kian_loop::ReActLoop;
use kian_orchestrator::DrainCtx;
use serde_json::Value;

use crate::config::MemoryConfig;
use crate::parse::parse_notes;
use crate::prompt::{DreamRequest, parse_dream_marker};

/// Bundle-owned counters for the dreaming drain (the metrics surface
/// the assembly wires into `GET /metrics`). All plain atomics — a
/// dropped dream counts honestly, nothing more.
#[derive(Debug, Default)]
pub struct DreamCounters {
    dreams: AtomicU64,
    read_failures: AtomicU64,
    parse_failures: AtomicU64,
    notes_appended: AtomicU64,
    notes_deduped: AtomicU64,
    append_failures: AtomicU64,
}

impl DreamCounters {
    fn bump(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Dreams harvested (a dream header was located and an answer
    /// extraction attempted).
    #[must_use]
    pub fn dreams(&self) -> u64 {
        self.dreams.load(Ordering::Relaxed)
    }

    /// Event-stream reads that failed during harvest.
    #[must_use]
    pub fn read_failures(&self) -> u64 {
        self.read_failures.load(Ordering::Relaxed)
    }

    /// Answers that did not parse (or no answer exists) — honest zeros.
    #[must_use]
    pub fn parse_failures(&self) -> u64 {
        self.parse_failures.load(Ordering::Relaxed)
    }

    /// `memory/note` events appended.
    #[must_use]
    pub fn notes_appended(&self) -> u64 {
        self.notes_appended.load(Ordering::Relaxed)
    }

    /// Drafts skipped because `(subject, derived_from)` already exists
    /// in the stream (crash-resume dedupe).
    #[must_use]
    pub fn notes_deduped(&self) -> u64 {
        self.notes_deduped.load(Ordering::Relaxed)
    }

    /// Note appends that failed (store/fence errors).
    #[must_use]
    pub fn append_failures(&self) -> u64 {
        self.append_failures.load(Ordering::Relaxed)
    }
}

/// The dreaming drain: the built-in ReAct loop plus the note harvest on
/// `Settled`. Cheap to clone; the counters are shared across clones.
#[derive(Clone)]
pub struct DreamDriver {
    inner: ReActLoop,
    config: MemoryConfig,
    counters: Arc<DreamCounters>,
    /// Redacts note `text` before append (kian-91, review M-1 /
    /// MEMORY-MODEL N4): a note is a durable claim, so the same
    /// [`kian_core::secrets`] hygiene exec output passes applies to it.
    redactor: SecretRedactor,
}

impl DreamDriver {
    /// Wraps `inner`; `config.max_notes` caps every harvest. The
    /// redactor defaults to pattern scanning (the safe default for a
    /// surface with no configured secret names); the assembly injects
    /// the configured one via [`Self::with_redactor`].
    #[must_use]
    pub fn new(inner: ReActLoop, config: MemoryConfig) -> Self {
        Self {
            inner,
            config,
            counters: Arc::new(DreamCounters::default()),
            redactor: SecretRedactor::patterns_only(),
        }
    }

    /// Installs the assembly's configured redactor (known secret env
    /// values + patterns) for note-text redaction.
    #[must_use]
    pub fn with_redactor(mut self, redactor: SecretRedactor) -> Self {
        self.redactor = redactor;
        self
    }

    /// The wrapped loop (assembly surface).
    #[must_use]
    pub fn inner(&self) -> &ReActLoop {
        &self.inner
    }

    /// The caps in force.
    #[must_use]
    pub fn config(&self) -> &MemoryConfig {
        &self.config
    }

    /// The shared dream counters.
    #[must_use]
    pub fn counters(&self) -> &DreamCounters {
        &self.counters
    }

    /// The `Settled` harvest (the whole DreamDriver delta): read the
    /// stream, locate the last dream header + answer, parse, dedupe,
    /// append. Every failure path is an honest zero with a counter.
    async fn harvest(&self, ctx: &DrainCtx) {
        let events = match ctx.events().await {
            Ok(events) => events,
            Err(e) => {
                tracing::warn!(
                    session = %ctx.session().0,
                    error = %e,
                    "dream harvest: stream read failed"
                );
                self.counters.bump(&self.counters.read_failures);
                return;
            }
        };

        let Some((header_seq, header)) = events.iter().rev().find_map(dream_header_of) else {
            // No dream request in the stream: nothing to harvest (an
            // honest no-op — a memory session drained without a dream).
            tracing::debug!(
                session = %ctx.session().0,
                "dream harvest: no dream header in the stream"
            );
            return;
        };
        self.counters.bump(&self.counters.dreams);

        let Some(answer) = last_answer_after(&events, header_seq) else {
            tracing::warn!(
                session = %ctx.session().0,
                "dream harvest: no assistant answer after the dream prompt"
            );
            self.counters.bump(&self.counters.parse_failures);
            return;
        };

        let request = DreamRequest::from_header(header, self.config.max_notes);
        let drafts = match parse_notes(answer, &request) {
            Ok(drafts) => drafts,
            Err(e) => {
                // Honest zero (round-8 plan §2.4): nothing is appended,
                // a note is never fabricated, the failure is counted.
                tracing::warn!(
                    session = %ctx.session().0,
                    error = %e,
                    "dream harvest: answer did not parse — appending nothing"
                );
                self.counters.bump(&self.counters.parse_failures);
                return;
            }
        };

        // Within-answer last-wins (kian-91, review M-2 — the write-end
        // twin of the fold's latest-per-subject rule): one answer
        // carrying two revisions of a subject lands the LAST, the
        // earlier draft superseded in place — never silently kept while
        // the later revision is dropped as a "duplicate".
        let drafts = fold_last_per_subject(drafts);

        // Crash-resume dedupe: a draft whose (subject, derived_from)
        // already exists in the stream is a note this session already
        // carries — a re-driven dream (takeover, folded wake) appends
        // it exactly once.
        let mut existing = existing_note_keys(&events);
        for draft in drafts {
            let key = (draft.subject.clone(), draft.derived_from.clone());
            if existing.contains(&key) {
                tracing::debug!(
                    session = %ctx.session().0,
                    subject = %draft.subject,
                    "dream harvest: duplicate (subject, derived_from) skipped"
                );
                self.counters.bump(&self.counters.notes_deduped);
                continue;
            }
            // N4 redaction (kian-91, review M-1): the note's `text`
            // passes the secret redactor before the append is durable;
            // the aggregate report rides the additive `redactions` key
            // (same shape as `tool/result`) and never carries a value.
            let (redacted_text, report) = self.redactor.redact_with_report(&draft.text);
            let mut payload = draft.to_payload();
            if let Some(obj) = payload.as_object_mut() {
                obj.insert("text".into(), Value::String(redacted_text));
                if !report.is_empty() {
                    obj.insert(
                        "redactions".into(),
                        serde_json::to_value(&report).expect("redaction report serializes"),
                    );
                }
            }
            match ctx.append(EventKind::MemoryNote, payload).await {
                Ok(_) => {
                    self.counters.bump(&self.counters.notes_appended);
                    // A repeated subject inside one answer is not a
                    // second note of the same key.
                    existing.insert(key);
                }
                Err(e) => {
                    // Fenced out or a store failure: stop — a fenced
                    // drain never fights the fence for more writes.
                    tracing::warn!(
                        session = %ctx.session().0,
                        subject = %draft.subject,
                        error = %e,
                        "dream harvest: note append failed"
                    );
                    self.counters.bump(&self.counters.append_failures);
                    break;
                }
            }
        }
    }
}

impl LoopDriver for DreamDriver {
    type Ctx = DrainCtx;

    /// Forwards the inner loop's outcomes unchanged; on `Settled`,
    /// harvests notes before yielding it (so the engine's end write
    /// lands after the notes).
    fn drive(&self, ctx: DrainCtx) -> BoxStream<'static, LoopOutcome> {
        let this = self.clone();
        let dream_ctx = ctx.clone();
        let inner = self.inner.drive(ctx);
        futures::stream::unfold(
            State {
                this,
                dream_ctx,
                inner,
                done: false,
            },
            |mut st| async move {
                if st.done {
                    return None;
                }
                let outcome = st.inner.next().await?;
                if matches!(outcome, LoopOutcome::Settled) {
                    // The inner stream is finished (Settled is its
                    // terminal outcome); harvest, then forward.
                    st.done = true;
                    st.this.harvest(&st.dream_ctx).await;
                }
                Some((outcome, st))
            },
        )
        .boxed()
    }
}

/// Unfold state: the shared dream context plus the inner loop's
/// outcome stream. `done` ends the wrapper right after the forwarded
/// terminal outcome (mirroring the inner loop's own `done` flag).
struct State {
    this: DreamDriver,
    dream_ctx: DrainCtx,
    inner: BoxStream<'static, LoopOutcome>,
    done: bool,
}

/// The last dream request carried by a stream, if any — the assembly's
/// routing classification (kian-87, marker-based as of kian-91/review
/// B-2): a session whose stream holds a `dream`-marked
/// `input/admitted` row is the memory session and runs
/// [`DreamDriver`]. The marker is the trusted, server-set source of
/// routing and provenance — the `[kian-dream: ...]` header line in the
/// message content is never consulted (untrusted text must not flip a
/// session onto the dreamer). Derived from the stream alone
/// (constraint 1), so a restarted server classifies a takeover hold
/// identically.
#[must_use]
pub fn dream_request_in(events: &[Event]) -> Option<crate::prompt::DreamHeader> {
    events
        .iter()
        .rev()
        .find_map(dream_header_of)
        .map(|(_, header)| header)
}

/// Whether an `input/admitted` event carries the trusted dream marker:
/// a user message with an additive `dream` key that parses. Returns the
/// carrying event's seq (the answer search bound) plus the header. The
/// message TEXT is never inspected (B-2).
fn dream_header_of(ev: &Event) -> Option<(u64, crate::prompt::DreamHeader)> {
    if !matches!(ev.kind, EventKind::InputAdmitted) {
        return None;
    }
    if ev.bytes.get("role").and_then(Value::as_str) != Some("user") {
        return None;
    }
    let header = parse_dream_marker(ev.bytes.get("dream")?)?;
    Some((ev.seq, header))
}

/// Folds parsed drafts by subject keeping the LAST entry (kian-91,
/// review M-2), preserving first-occurrence order for the subjects that
/// survive. Pure; deterministic.
fn fold_last_per_subject(drafts: Vec<crate::parse::NoteDraft>) -> Vec<crate::parse::NoteDraft> {
    let mut folded: Vec<crate::parse::NoteDraft> = Vec::with_capacity(drafts.len());
    let mut position: HashMap<String, usize> = HashMap::with_capacity(drafts.len());
    for draft in drafts {
        match position.get(&draft.subject) {
            Some(&index) => folded[index] = draft,
            None => {
                position.insert(draft.subject.clone(), folded.len());
                folded.push(draft);
            }
        }
    }
    folded
}

/// The last assistant answer text after `seq`: the newest
/// `assistant/message` carrying a non-empty `content.text` (a
/// tool-call-only assistant message is not an answer).
fn last_answer_after(events: &[Event], seq: u64) -> Option<&str> {
    events
        .iter()
        .rev()
        .filter(|ev| ev.seq > seq && matches!(ev.kind, EventKind::AssistantMessage))
        .find_map(|ev| {
            ev.bytes
                .pointer("/content/text")
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
        })
}

/// The `(subject, derived_from)` keys already in the stream (via the
/// core fold parser — invalid note payloads never surface as keys).
fn existing_note_keys(events: &[Event]) -> HashSet<(String, Vec<DerivedRange>)> {
    events
        .iter()
        .filter(|ev| matches!(ev.kind, EventKind::MemoryNote))
        .filter_map(|ev| memory::parse_note(ev.kind, &ev.bytes))
        .map(|note| (note.subject, note.derived_from))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use kian_core::memory::NoteStatus;
    use kian_core::model::{AgentDef, Budgets, Epoch, InputRow, SessionId, SlotName, SlotSpec};
    use kian_core::seams::{
        ModelChunk, ModelProvider, ModelRequest, PolicyRule, ProviderError, RecoveryPolicySource,
        ToolCall, ToolError, ToolProvider, ToolResult,
    };
    use kian_core::{events as core_events, memory as core_memory};
    use kian_llm::turn::ProviderRegistry;
    use kian_orchestrator::{DrainCtx, ManualClock, Metrics};
    use kian_store::EventStore;
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::*;
    use crate::config::MemoryConfig;
    use crate::prompt::{dream_idempotency_key, render_dream_prompt};

    const T0: u64 = 1_761_900_000_000;
    const SOURCE: &str = "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b";

    // -- fixtures -----------------------------------------------------------

    fn source_id() -> SessionId {
        SessionId(Uuid::parse_str(SOURCE).unwrap())
    }

    /// A scripted model (no network): each call replays one turn's
    /// chunks; text chunks assemble into the assistant message.
    struct ScriptedModel {
        turns: std::sync::Mutex<std::collections::VecDeque<Vec<Result<ModelChunk, ProviderError>>>>,
    }

    impl ScriptedModel {
        fn text_turns(answers: &[&str]) -> Arc<Self> {
            Arc::new(Self {
                turns: std::sync::Mutex::new(
                    answers
                        .iter()
                        .map(|a| vec![Ok(ModelChunk::new(json!({"content": *a})))])
                        .collect(),
                ),
            })
        }
    }

    impl ModelProvider for ScriptedModel {
        fn complete(
            &self,
            _req: ModelRequest,
        ) -> futures::stream::BoxStream<'static, Result<ModelChunk, ProviderError>> {
            let turn = self.turns.lock().unwrap().pop_front().unwrap_or_default();
            let items: Vec<Result<ModelChunk, ProviderError>> = turn;
            futures::stream::iter(items).boxed()
        }
    }

    struct NoopTools;
    impl ToolProvider for NoopTools {
        fn schemas(&self) -> Vec<kian_core::seams::ToolSchema> {
            Vec::new()
        }
        fn invoke(
            &self,
            _call: ToolCall,
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

    fn agent_def() -> AgentDef {
        AgentDef {
            id: "memory-dreamer".into(),
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

    fn dream_driver(model: Arc<ScriptedModel>) -> DreamDriver {
        let mut registry = ProviderRegistry::new();
        registry.register("mock", model);
        let react = ReActLoop::with_config(
            agent_def(),
            Arc::new(registry),
            Arc::new(NoopTools),
            Arc::new(NoopPolicy),
            kian_loop::ReActConfig::default(),
        );
        DreamDriver::new(react, MemoryConfig::default())
    }

    fn temp_db_path() -> PathBuf {
        std::env::temp_dir().join(format!("kian-memory-test-{}.sqlite", Uuid::new_v4()))
    }

    fn cleanup(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    /// Opens a store, bootstraps a memory session (session/created at
    /// genesis) and returns (store, session, ctx under epoch 1, path).
    async fn memory_session() -> (EventStore, SessionId, DrainCtx, PathBuf) {
        let path = temp_db_path();
        let store = EventStore::open(&path).await.expect("open store");
        let sid = SessionId::new();
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
                    version: core_events::SCHEMA_VERSION,
                },
                kian_store::AppendGuard {
                    epoch: Epoch::GENESIS,
                    expected_seq: 1,
                },
            )
            .await
            .expect("bootstrap session/created");
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
        (store, sid, ctx, path)
    }

    fn dream_row(sid: SessionId, from: u64, to: u64, prompt: String) -> InputRow {
        // kian-91 (review B-2): the ops-plane envelope — the prompt as
        // content plus the trusted dream marker (exactly two keys).
        InputRow {
            id: Uuid::new_v4(),
            session: sid,
            payload: crate::prompt::dream_admit_payload(
                "ops-agent",
                source_id(),
                from,
                to,
                &prompt,
            ),
            admitted_at: T0,
            eligible_at: T0,
            idempotency_key: Some(dream_idempotency_key(source_id(), from, to)),
        }
    }

    fn prompt(transcript: &str) -> String {
        render_dream_prompt("ops-agent", source_id(), 1, 4, transcript)
    }

    const TRANSCRIPT: &str = "seq 1 user: the deploy pipeline broke on staging\n\
         seq 2 assistant: rolled back and filed INC-42\n\
         seq 3 user: deploys go through the dsh bridge from now on";

    async fn drive_to_end(driver: &DreamDriver, ctx: DrainCtx) -> Vec<LoopOutcome> {
        driver.drive(ctx).collect::<Vec<LoopOutcome>>().await
    }

    fn note_events(events: &[Event]) -> Vec<&Event> {
        events
            .iter()
            .filter(|ev| matches!(ev.kind, EventKind::MemoryNote))
            .collect()
    }

    // -- the driver ---------------------------------------------------------

    /// A settled dream appends one `memory/note` per parsed draft, with
    /// header-supplied provenance, and the outcomes are exactly the
    /// loop's.
    #[tokio::test]
    async fn settled_dream_appends_notes_from_the_answer() {
        let (store, sid, ctx, path) = memory_session().await;
        store
            .admit(dream_row(sid, 1, 4, prompt(TRANSCRIPT)))
            .await
            .expect("admit dream row");

        let answer = r#"[
            {"subject": "deploys via dsh bridge", "text": "Deploys go through the dsh bridge.", "status": "verified", "verified_by": {"method": "transcript", "ref": "3"}},
            {"subject": "incident process", "text": "Incidents are filed as INC-NN."}
        ]"#;
        let driver = dream_driver(ScriptedModel::text_turns(&[answer]));
        let outcomes = drive_to_end(&driver, ctx).await;
        assert_eq!(outcomes, [LoopOutcome::Settled]);

        let events = store.rebuild(sid).await.expect("rebuild");
        let notes = note_events(&events);
        assert_eq!(notes.len(), 2, "one event per draft");
        let projected = core_memory::project_notes(&events);
        assert_eq!(projected.len(), 2);
        let deploys = projected
            .iter()
            .find(|n| n.subject == "deploys via dsh bridge")
            .expect("note present");
        assert_eq!(deploys.status, NoteStatus::Verified);
        assert_eq!(
            deploys.derived_from,
            vec![core_memory::DerivedRange {
                session: source_id(),
                start_seq: 1,
                end_seq: 4,
            }]
        );
        assert_eq!(driver.counters().notes_appended(), 2);
        assert_eq!(driver.counters().parse_failures(), 0);
        cleanup(&path);
    }

    /// A garbage answer appends nothing — honest zero, counted, never
    /// a fabricated note.
    #[tokio::test]
    async fn parse_failure_appends_nothing_and_counts() {
        let (store, sid, ctx, path) = memory_session().await;
        store
            .admit(dream_row(sid, 1, 4, prompt(TRANSCRIPT)))
            .await
            .expect("admit dream row");

        let driver = dream_driver(ScriptedModel::text_turns(&[
            "I cannot answer in that format.",
        ]));
        let outcomes = drive_to_end(&driver, ctx).await;
        assert_eq!(outcomes, [LoopOutcome::Settled]);

        let events = store.rebuild(sid).await.expect("rebuild");
        assert!(note_events(&events).is_empty(), "honest zero");
        assert_eq!(driver.counters().parse_failures(), 1);
        assert_eq!(driver.counters().notes_appended(), 0);
        cleanup(&path);
    }

    /// Crash-resume dedupe: a draft whose `(subject, derived_from)`
    /// already exists in the stream is skipped; a fresh subject still
    /// lands.
    #[tokio::test]
    async fn crash_resume_dedupes_on_subject_and_range() {
        let (store, sid, ctx, path) = memory_session().await;
        store
            .admit(dream_row(sid, 1, 4, prompt(TRANSCRIPT)))
            .await
            .expect("admit dream row");
        // A previous (crashed) drain already landed this note for the
        // same subject + range.
        ctx.append(
            EventKind::MemoryNote,
            json!({
                "subject": "deploys via dsh bridge",
                "text": "Deploys go through the dsh bridge.",
                "status": "candidate",
                "derived_from": [{"session": SOURCE, "start_seq": 1, "end_seq": 4}],
            }),
        )
        .await
        .expect("pre-existing note");

        let answer = r#"[
            {"subject": "deploys via dsh bridge", "text": "Deploys go through the dsh bridge."},
            {"subject": "incident process", "text": "Incidents are filed as INC-NN."}
        ]"#;
        let driver = dream_driver(ScriptedModel::text_turns(&[answer]));
        let outcomes = drive_to_end(&driver, ctx).await;
        assert_eq!(outcomes, [LoopOutcome::Settled]);

        let events = store.rebuild(sid).await.expect("rebuild");
        // Exactly one new note: the duplicate subject/range skipped.
        assert_eq!(note_events(&events).len(), 2);
        assert_eq!(driver.counters().notes_deduped(), 1);
        assert_eq!(driver.counters().notes_appended(), 1);
        let projected = core_memory::project_notes(&events);
        assert_eq!(projected.len(), 2);
        cleanup(&path);
    }

    /// Within ONE answer, the LATER revision of a subject wins (kian-91,
    /// review M-2): an answer carrying a candidate then a verified
    /// revision of the same subject appends exactly one note — the
    /// verified one. (Pre-fix, the earlier draft won and the verified
    /// revision was dropped as a "duplicate".)
    #[tokio::test]
    async fn within_answer_last_revision_wins() {
        let (store, sid, ctx, path) = memory_session().await;
        store
            .admit(dream_row(sid, 1, 4, prompt(TRANSCRIPT)))
            .await
            .expect("admit dream row");

        let answer = r#"[
            {"subject": "deploys via dsh bridge", "text": "FIRST revision", "status": "candidate"},
            {"subject": "deploys via dsh bridge", "text": "SECOND revision", "status": "verified",
             "verified_by": {"method": "transcript", "ref": "2"}}
        ]"#;
        let driver = dream_driver(ScriptedModel::text_turns(&[answer]));
        let outcomes = drive_to_end(&driver, ctx).await;
        assert_eq!(outcomes, [LoopOutcome::Settled]);

        let events = store.rebuild(sid).await.expect("rebuild");
        let notes = note_events(&events);
        assert_eq!(notes.len(), 1, "exactly one note for one subject");
        let projected = core_memory::project_notes(&events);
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].text, "SECOND revision", "the LAST draft wins");
        assert_eq!(projected[0].status, NoteStatus::Verified);
        assert_eq!(driver.counters().notes_appended(), 1);
        assert_eq!(driver.counters().notes_deduped(), 0);
        cleanup(&path);
    }

    /// Note text passes the secret redactor before append (kian-91,
    /// review M-1 / MEMORY-MODEL N4): a token-shaped secret in the
    /// model's note text is redacted in the durable event, the
    /// aggregate report rides the additive `redactions` key with only
    /// `{kind,name,count}`, and no secret value appears anywhere in the
    /// payload.
    #[tokio::test]
    async fn note_text_is_redacted_with_a_value_free_report() {
        let (store, sid, ctx, path) = memory_session().await;
        store
            .admit(dream_row(sid, 1, 4, prompt(TRANSCRIPT)))
            .await
            .expect("admit dream row");

        // A well-formed GitHub classic token (pattern-pass redaction —
        // no config needed, exactly the unknown-secret-of-known-shape
        // case N4 exists for).
        let secret = "ghp_abcdefghijklmnopqrstuvwxyz012345";
        let answer = format!(
            r#"[{{"subject": "leaked key", "text": "the agent's key is {secret} and it repeats {secret}"}}]"#
        );
        let driver = dream_driver(ScriptedModel::text_turns(&[&answer]));
        let outcomes = drive_to_end(&driver, ctx).await;
        assert_eq!(outcomes, [LoopOutcome::Settled]);

        let events = store.rebuild(sid).await.expect("rebuild");
        let notes = note_events(&events);
        assert_eq!(notes.len(), 1);
        let payload_str = notes[0].bytes.to_string();
        assert!(
            !payload_str.contains(secret),
            "the secret value must not appear in the event payload"
        );
        assert!(
            notes[0].bytes["text"]
                .as_str()
                .unwrap()
                .contains("REDACTED")
        );
        // The report: aggregate counts only, exactly the tool/result
        // shape — {kind, name, count} — with the token prefix as the
        // name and both occurrences counted.
        let report = notes[0]
            .bytes
            .get("redactions")
            .expect("the redaction report rides the additive key");
        assert_eq!(
            report,
            &serde_json::json!([{ "kind": "token_pattern", "name": "ghp_", "count": 2 }]),
            "the report carries only kind/name/count"
        );
        // The note still folds (the additive key is not context).
        let projected = core_memory::project_notes(&events);
        assert_eq!(projected.len(), 1);
        assert!(!projected[0].text.contains(secret));
        cleanup(&path);
    }

    /// Non-settled outcomes are forwarded unchanged (here: a tool-call
    /// turn, then the final answer), and the harvest still lands.
    #[tokio::test]
    async fn forwards_non_settled_outcomes_unchanged() {
        let (store, sid, ctx, path) = memory_session().await;
        store
            .admit(dream_row(sid, 1, 4, prompt(TRANSCRIPT)))
            .await
            .expect("admit dream row");

        let answer = r#"[{"subject": "s", "text": "t"}]"#;
        let model = ScriptedModel::text_turns(&[answer]);
        // Turn 1 asks for a tool (exec fails honestly in NoopTools),
        // turn 2 is the final answer.
        let tool_call = ModelChunk::new(json!({
            "tool_calls": [{
                "index": 0,
                "id": "call-1",
                "function": {"name": "exec", "arguments": "{\"cmd\":\"true\"}"}
            }]
        }));
        model.turns.lock().unwrap().push_front(vec![Ok(tool_call)]);
        let driver = dream_driver(model);
        let outcomes = drive_to_end(&driver, ctx).await;
        assert_eq!(
            outcomes,
            [LoopOutcome::TurnDone, LoopOutcome::Settled],
            "outcomes forwarded unchanged"
        );

        let events = store.rebuild(sid).await.expect("rebuild");
        assert_eq!(note_events(&events).len(), 1);
        cleanup(&path);
    }

    /// No dream header in the stream (a memory session drained without
    /// a dream request): an honest no-op, no notes, no parse failure.
    #[tokio::test]
    async fn no_dream_header_is_an_honest_noop() {
        let (store, sid, ctx, path) = memory_session().await;
        // A plain (non-dream) user row.
        store
            .admit(InputRow {
                id: Uuid::new_v4(),
                session: sid,
                payload: Value::String("hello".into()),
                admitted_at: T0,
                eligible_at: T0,
                idempotency_key: None,
            })
            .await
            .expect("admit plain row");

        let driver = dream_driver(ScriptedModel::text_turns(&["hi there"]));
        let outcomes = drive_to_end(&driver, ctx).await;
        assert_eq!(outcomes, [LoopOutcome::Settled]);

        let events = store.rebuild(sid).await.expect("rebuild");
        assert!(note_events(&events).is_empty());
        assert_eq!(driver.counters().parse_failures(), 0);
        assert_eq!(driver.counters().dreams(), 0);
        cleanup(&path);
    }

    /// A later dream in the same session harvests against the LATEST
    /// header (disjoint ranges interleave without cross-contamination).
    #[tokio::test]
    async fn latest_header_wins_for_a_second_dream() {
        let (store, sid, ctx, path) = memory_session().await;
        let first = render_dream_prompt("ops-agent", source_id(), 1, 4, TRANSCRIPT);
        store
            .admit(dream_row(sid, 1, 4, first))
            .await
            .expect("dream 1");
        let second = render_dream_prompt("ops-agent", source_id(), 5, 9, "seq 5 …");
        store
            .admit(dream_row(sid, 5, 9, second))
            .await
            .expect("dream 2");

        let answer = r#"[{"subject": "later note", "text": "t"}]"#;
        let driver = dream_driver(ScriptedModel::text_turns(&[answer]));
        let _ = drive_to_end(&driver, ctx).await;

        let events = store.rebuild(sid).await.expect("rebuild");
        let projected = core_memory::project_notes(&events);
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].derived_from[0].start_seq, 5);
        assert_eq!(projected[0].derived_from[0].end_seq, 9);
        cleanup(&path);
    }
}
