//! `kian-superagent` — the super-agent bundle (ADR-003, card C23 slice 1:
//! subscription rules, channel framework, first-class MockChannel).
//!
//! DESIGN §7 assigns this crate: "super-agent bundle: Router/Worker
//! templates, subscription rules, channel output policies". It is a
//! *bundle* on top of the core, never core behavior: everything here is
//! Footprint-Ladder level 1–2 (config + plugin-hosted code over the
//! `ChannelAdapter` seam from `kian_core::seams`).
//!
//! The layered iron rule (AGENTS.md constraint 9) shapes the crate: the
//! bundle never touches execution. Its **only** request surface toward
//! the server is the [`InboxWriter`] port — write the inbox, then wake.
//! Assembly (kian-cli) implements the port over the real engine; this
//! crate never names an execution API. `tests/iron_rule.rs` enforces
//! that textually (the C13 pattern).
//!
//! Modules:
//! - [`inbox`] — the `InboxWriter` port (the bundle's whole server surface)
//! - [`rules`] — declarative `SubscriptionRule` set (config, DESIGN §4.1)
//! - [`reasons`] — the closed decision-reason vocabularies (C57):
//!   `IntakeReason` audit/counter codes for every intake decision, and
//!   the `DecisionKind` judge-side decision-tag convention
//! - [`provenance`] — the channel-event wrapper riding inside the input
//!   payload's `content` (the frozen C4 payload contract stays untouched)
//! - [`registry`] — the passthrough worker registry (C38: topic →
//!   *set* of worker sessions, the many-to-many fan-out projection):
//!   disposable, rebuilt from event streams (constraint 1)
//! - [`stage`] — the Ticket stage projection (C56): "what is it
//!   waiting for" — a pure first-match table over the worker's own
//!   stream plus two read-side inputs (pending inbox rows, an outbox
//!   view); a disposable projection, never persisted
//! - [`subscriptions`] — the subscription service consuming
//!   `ChannelAdapter` streams, matching rules, admitting + waking —
//!   one durable row per passthrough subscriber (C38)
//! - [`intake`] — the Thread/Correlation intake projection (C61,
//!   ADR-013 draft): config-owned canonical thread keys derived from
//!   provenance/payload fields (`<channel>:<group>#<key>` — after
//!   derivation the key *is* a topic; the rule matcher is unchanged),
//!   stream-derived source read positions (lark message-id offsets;
//!   GitHub REST `since` timestamps) with an honest idempotent
//!   re-poll bound where no cursor derives, and the neighbors-block
//!   surface that carries the key as context — correlation never
//!   routes, and fan-out grants no authority (bounded by Mandate,
//!   ADR-012)
//! - [`mock`] — `MockChannel`: the production-quality in-memory channel
//!   (the S4 IM stand-in and the framework's reference implementation)
//! - [`lark`] — `LarkChannel`: the first *real* channel adapter (C44),
//!   over the Feishu/Lark open APIs — chat-history poll receive (the
//!   adapter-side, webhook-free ingestion), IM send, env-gated
//!   credentials filled by assembly
//! - [`github`] — `GitHubChannel`: the second *real* channel adapter
//!   (C48), the S4 "group-chat message + CI events" family —
//!   webhook-shaped receive (delivery id = the idempotency key;
//!   verification lives in the C49 receiver surface, the adapter maps
//!   only verified deliveries), REST-API send (PR review comment /
//!   issue comment / PR description, resolving only on acceptance),
//!   token env-only (`KIAN_GITHUB_TOKEN`; receive needs none),
//!   untrusted by default (C43)
//! - [`dsh`] — the S6 dsh mapping contract (C53, ADR-010, DESIGN §16):
//!   dsh durable session events → the existing Kian durable-event subset
//!   (bundle-level; no `kian-core` kind changes), the generation rule
//!   (decode-before-map; foreign generations refused), the pre-flight
//!   persist-what-you-send rule across the process boundary (request
//!   bytes before the response is offered, or the bridge stops at an
//!   honest negative result), and the steer/interrupt control
//!   translation — the contract C54's bridge implements and any
//!   dsh-side plugin must satisfy
//! - [`worker`] — Thread delegation (C25): `WorkerSpawner`,
//!   `WorkerLoopDriver` (a `LoopDriver` decorator adding thread lineage
//!   and the evidence-gated completion report), the delegation wrapper,
//!   and the delegation-pair audit helper
//! - [`templates`] — the Router/Worker `AgentDef` templates and the
//!   structural profile rules (C25; workers never hold channel tools)
//! - [`policy`] — the DESIGN §4.2 channel output policies (C26):
//!   `ImPolicy` (≤100-word rule) and `StructuredPolicy` (section
//!   contract, honest rejection) — bundle-only placement
//! - [`router`] — the resident Router profile (C26): judge prompt +
//!   bootstrap, `RouterTools` (spawn_worker / reply /
//!   deliver_structured), the `RouterDriver` residency decorator, and
//!   the channel registry
//! - [`metrics`] — `RouterMetrics` (C41): the article-§7.2 batching
//!   observability over the Router drain — per judge turn, batch size,
//!   wall duration and amortized per-decision latency; counters + a
//!   bounded batch histogram, `RouterDriver`-recorded, assembly-snapshotted
//! - [`recall`] — the `RecallPort` read port (C34): cross-session
//!   reads (`read_events`, `list_threads`) plus the disposable spill
//!   index — assembly-implemented over the store's public scan; reads
//!   only, the bundle names no store
//! - [`toolpack`] — the recall toolpack (C35): `recall_event` /
//!   `recall_neighbors` / `recall_thread` as one read-only
//!   `ToolProvider` over `RecallPort`; every output Inline/Spill-shaped
//!   under its own caps so multi-hop recall composes (the Chain of
//!   Recall)
//! - [`timer`] — `TimerChannel` (C40): the timer input source as a
//!   production channel adapter — numbered ticks on a configurable
//!   cadence (clock-injected, `ManualTurnClock`-testable), inbound-only
//! - [`watchdog`] — Watchdog Session v2 (C40): the model-in-the-loop
//!   progress judge — a resident worker-template agent whose tools are
//!   exactly the read/intervene surface (`list_threads`,
//!   `read_progress` over `RecallPort`; `intervene` as a steer row via
//!   `InboxWriter`; `escalate` under the IM policy)

pub mod dsh;
pub mod github;
pub mod inbox;
pub mod intake;
pub mod lark;
pub mod metrics;
pub mod mock;
pub mod policy;
pub mod provenance;
pub mod rearm;
pub mod reasons;
pub mod recall;
pub mod registry;
pub mod router;
pub mod rules;
pub mod stage;
pub mod subscriptions;
pub mod templates;
pub mod timer;
pub mod toolpack;
pub mod watchdog;
pub mod worker;

pub use dsh::{
    BRIDGE_CONTRACT_VERSION, ContractError, ControlFrame, DshEvent, DshMapper, MapCtx, Mapped,
    PeerFrame, PeerSession, SUPPORTED_LOGICAL_FORMAT, control_line, parse_peer_frame,
};
pub use github::{
    API_VERSION, DEFAULT_API_BASE as GITHUB_DEFAULT_API_BASE, GITHUB_CHANNEL_TYPE, GitHubChannel,
    GitHubConfig, GitHubError, SUBSCRIBER_CAPACITY as GITHUB_SUBSCRIBER_CAPACITY,
    SUPPORTED_EVENT_TYPES, payload_since_cursor,
};
pub use inbox::{AdmitReceipt, InboxWriteError, InboxWriter};
pub use intake::{
    GROUP_FIELD, ReadPosition, SourceCursor, THREAD_KEY_FIELD, ThreadKey, ThreadKeyRule,
    ThreadKeySet, derive_thread_key, neighbors_with_thread_key, read_position,
};
pub use lark::{
    DEFAULT_API_BASE, DEFAULT_POLL_INTERVAL, LARK_CHANNEL_TYPE, LarkChannel, LarkConfig, LarkError,
    SUBSCRIBER_CAPACITY, payload_message_cursor,
};
pub use metrics::{
    ManualTurnClock, ROUTER_BATCH_BUCKETS, RouterMetrics, RouterTurnRecorder, SpeakMetrics,
    SystemTurnClock, TurnClock, TurnRecord,
};
pub use mock::{CapturedDelivery, MockChannel};
pub use policy::{
    DEFAULT_MAX_WORDS, DEFAULT_SECTIONS, ELLIPSIS, EnforcedReply, IM_POLICY_NAME, ImPolicy,
    PolicyError, STRUCTURED_POLICY_NAME, SpeakCondition, SpeakContext, SpeakKind, SpeakPolicy,
    SpeakRule, SpeakTracker, SpeakVerdict, StructuredPolicy, count_words, im_delivered_payload,
    speak_key, structured_delivered_payload,
};
pub use provenance::{
    CHANNEL_EVENT_TYPE, ChannelEventMarker, ShapingConfig, channel_event_untrusted,
    channel_input_payload, is_untrusted_view, parse_channel_marker, raw_view, shape_channel_input,
    shape_channel_input_marked, wrap_channel_event, wrap_channel_event_marked,
};
pub use reasons::{DecisionKind, IntakeReason, outcome_reasons, routed_reason};
pub use recall::{RecallPort, RecallPortError, SpillIndex, thread_bindings};
pub use registry::{PassthroughRegistry, WorkerStatus, worker_sessions};
pub use router::{
    ChannelRegistry, DEFAULT_PARK_TICK_MS, DELIVER_STRUCTURED_TOOL, JUDGE_PROMPT_TEMPLATE,
    JudgePrompt, REPLY_TOOL, ROUTER_BOOTSTRAP_KEY, RouterDriver, RouterHold, RouterInbox,
    RouterProfile, RouterTools, SILENT_REPRO_TEMPLATE, SpeculationPolicy, enforce_speculation_cap,
    outstanding_threads,
};
pub use rules::{
    AdmissionClass, AdmissionClassRule, AdmissionClassTable, CorrelateBy, Route, RuleMatcher,
    SubscriptionRule, SubscriptionRuleSet, event_has_explicit_signal,
};
pub use stage::{NoOutbox, OutboxView, TicketStage, project_stage};
pub use subscriptions::{FanoutAdmission, RoutingOutcome, SubscriptionService};
pub use templates::{
    ASYNC_TOOLS, CHANNEL_TOOLS, ProfileError, ROUTER_SLOT, SPAWN_WORKER_TOOL, SuperAgentProfile,
    WORKER_TOOLS, empty_tool_parameters, is_channel_tool, mount_async_tools, mount_recall_tools,
    mount_watchdog_tools, router_template, worker_template,
};
pub use timer::{
    DEFAULT_TIMER_TOPIC, TIMER_CHANNEL, TIMER_QUEUE_CAPACITY, TimerChannel, TimerConfig,
};
pub use toolpack::{
    DEFAULT_NEIGHBOR_WINDOW, DEFAULT_OUTPUT_CAP_BYTES, RECALL_EVENT_TOOL, RECALL_NEIGHBORS_TOOL,
    RECALL_THREAD_TOOL, RECALL_TOOLS, RecallToolConfig, RecallTools,
};
pub use watchdog::{
    ESCALATE_TOOL, INTERVENE_TOOL, LIST_THREADS_TOOL, READ_PROGRESS_TOOL, WATCHDOG_BOOTSTRAP_KEY,
    WATCHDOG_PROMPT_TEMPLATE, WATCHDOG_TOOLS, WatchdogProfile, WatchdogPrompt, WatchdogTools,
    watchdog_template,
};
pub use worker::{
    Delegation, DelegationPair, LineagePlan, PortFor, REPORT_STATE_KEY, ReportState, SpawnError,
    SpawnedWorker, StreamPort, StreamPortError, ThreadReport, WORKER_GATE_NAME, WorkerLoopConfig,
    WorkerLoopDriver, WorkerSpawner, completion_summary, delegation_content, delegation_pairs,
    lineage_plan, parse_delegation, parse_thread_report, thread_completed_payload,
    thread_report_content, thread_report_content_stated, thread_started_payload,
};
