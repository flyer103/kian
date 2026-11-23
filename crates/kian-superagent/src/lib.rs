//! `kian-superagent` — the super-agent bundle (ADR-003, slice 1:
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
//! that textually (the pattern).
//!
//! Modules:
//! - [`inbox`] — the `InboxWriter` port (the bundle's whole server surface)
//! - [`rules`] — declarative `SubscriptionRule` set (config, DESIGN §4.1)
//! - [`reasons`] — the closed decision-reason vocabularies:
//!   `IntakeReason` audit/counter codes for every intake decision, and
//!   the `DecisionKind` judge-side decision-tag convention
//! - [`provenance`] — the channel-event wrapper riding inside the input
//!   payload's `content` (the frozen payload contract stays untouched)
//! - [`registry`] — the passthrough worker registry (topic →
//!   *set* of worker sessions, the many-to-many fan-out projection):
//!   disposable, rebuilt from event streams (constraint 1)
//! - [`stage`] — the Ticket stage projection: "what is it
//!   waiting for" — a pure first-match table over the worker's own
//!   stream plus two read-side inputs (pending inbox rows, an outbox
//!   view); a disposable projection, never persisted
//! - [`subscriptions`] — the subscription service consuming
//!   `ChannelAdapter` streams, matching rules, admitting + waking —
//!   one durable row per passthrough subscriber
//! - [`intake`] — the Thread/Correlation intake projection (
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
//!   (the IM stand-in and the framework's reference implementation)
//! - [`lark`] — `LarkChannel`: the first *real* channel adapter,
//!   over the Feishu/Lark open APIs — chat-history poll receive (the
//!   adapter-side, webhook-free ingestion), IM send, env-gated
//!   credentials filled by assembly
//! - [`github`] — `GitHubChannel`: the second *real* channel adapter
//!   , the "group-chat message + CI events" family —
//!   webhook-shaped receive (delivery id = the idempotency key;
//!   verification lives in the receiver surface, the adapter maps
//!   only verified deliveries), REST-API send (PR review comment /
//!   issue comment / PR description, resolving only on acceptance),
//!   token env-only (`KIAN_GITHUB_TOKEN`; receive needs none),
//!   untrusted by default
//! - [`dsh`] — the dsh mapping contract (ADR-010, DESIGN §16):
//!   dsh durable session events → the existing Kian durable-event subset
//!   (bundle-level; no `kian-core` kind changes), the generation rule
//!   (decode-before-map; foreign generations refused), the pre-flight
//!   persist-what-you-send rule across the process boundary (request
//!   bytes before the response is offered, or the bridge stops at an
//!   honest negative result), and the steer/interrupt control
//!   translation — the contract 's bridge implements and any
//!   dsh-side plugin must satisfy
//! - [`worker`] — Thread delegation: `WorkerSpawner`,
//!   `WorkerLoopDriver` (a `LoopDriver` decorator adding thread lineage
//!   and the evidence-gated completion report), the delegation wrapper,
//!   and the delegation-pair audit helper
//! - [`templates`] — the Router/Worker `AgentDef` templates and the
//!   structural profile rules (workers never hold channel tools)
//! - [`policy`] — the DESIGN §4.2 channel output policies:
//!   `ImPolicy` (the unit-budget rule, plus the details-request full-delivery
//!   mode derived from the event stream) and `StructuredPolicy`
//!   (section contract, honest rejection) — bundle-only placement
//! - [`router`] — the resident Router profile: judge prompt +
//!   bootstrap, `RouterTools` (spawn_worker / reply /
//!   deliver_structured), the `RouterDriver` residency decorator, and
//!   the channel registry
//! - [`metrics`] — `RouterMetrics`: the article-§7.2 batching
//!   observability over the Router drain — per judge turn, batch size,
//!   wall duration and amortized per-decision latency; counters + a
//!   bounded batch histogram, `RouterDriver`-recorded, assembly-snapshotted
//! - [`recall`] — the `RecallPort` read port: cross-session
//!   reads (`read_events`, `list_threads`) plus the disposable spill
//!   index — assembly-implemented over the store's public scan; reads
//!   only, the bundle names no store
//! - [`toolpack`] — the recall toolpack: `recall_event` /
//!   `recall_neighbors` / `recall_thread` as one read-only
//!   `ToolProvider` over `RecallPort`; every output Inline/Spill-shaped
//!   under its own caps so multi-hop recall composes (the Chain of
//!   Recall)
//! - [`timer`] — `TimerChannel`: the timer input source as a
//!   production channel adapter — numbered ticks on a configurable
//!   cadence (clock-injected, `ManualTurnClock`-testable), inbound-only
//! - [`watchdog`] — Watchdog Session v2: the model-in-the-loop
//!   progress judge — a resident worker-template agent whose tools are
//!   exactly the read/intervene surface (`list_threads`,
//!   `read_progress` over `RecallPort`; `intervene` as a steer row via
//!   `InboxWriter`; `escalate` under the IM policy)

pub mod answers;
pub mod audit;
pub mod binding;
pub mod bootstrap;
pub mod directory;
pub mod dsh;
pub mod github;
pub mod imessage;
pub mod inbox;
pub mod intake;
pub mod job;
pub mod lark;
pub mod metrics;
pub mod mock;
pub mod multiagent;
pub mod outbox;
pub mod policy;
pub mod policy_version;
pub mod provenance;
pub mod rearm;
pub mod reasons;
pub mod recall;
pub mod registry;
pub mod roster;
pub mod router;
pub mod rules;
pub mod stage;
pub mod subscriptions;
pub mod templates;
pub mod timeout;
pub mod timer;
pub mod toolpack;
pub mod watchdog;
pub mod wechat;
pub mod worker;

pub use audit::{
    EvidenceRef, MULTIAGENT_KEY, ParentThreadAudit, attempt_of, child_thread_started_payload,
    parent_thread_started_payload, thread_completed_payload_keyed,
};
pub use binding::ResidentBinding;
pub use bootstrap::SessionBootstrap;
pub use directory::{MemberState, MemberView, WorkerDirectoryError, WorkerDirectoryPort};
pub use dsh::{
    BRIDGE_CONTRACT_VERSION, ContractError, ControlFrame, DshEvent, DshMapper, MapCtx, Mapped,
    PeerFrame, PeerSession, SUPPORTED_LOGICAL_FORMAT, control_line, parse_peer_frame,
};
pub use github::{
    API_VERSION, DEFAULT_API_BASE as GITHUB_DEFAULT_API_BASE, GITHUB_CHANNEL_TYPE, GitHubChannel,
    GitHubConfig, GitHubError, POLL_SINCE_FIELD, PollConfig, PollKind,
    SUBSCRIBER_CAPACITY as GITHUB_SUBSCRIBER_CAPACITY, SUPPORTED_EVENT_TYPES, payload_since_cursor,
    poll_author_is_human, poll_comment_body, poll_event_id, poll_issue_body, poll_item_since,
    poll_items_to_events, poll_thread_key_rule, rfc3339_ms,
};
pub use imessage::{
    BRIDGE_TOKEN_ENV as IMESSAGE_BRIDGE_TOKEN_ENV,
    DEFAULT_POLL_TIMEOUT as IMESSAGE_DEFAULT_POLL_TIMEOUT, ECHO_RING_CAPACITY,
    IMESSAGE_CHANNEL_TYPE, ImessageChannel, ImessageConfig, ImessageDrop, ImessageError,
    ImessageService, SEND_SPLIT_CHARS as IMESSAGE_SEND_SPLIT_CHARS,
    SUBSCRIBER_CAPACITY as IMESSAGE_SUBSCRIBER_CAPACITY, delivery_client_id, is_http_base,
    map_message as imessage_map_message, message_to_event as imessage_message_to_event,
    split_text as imessage_split_text,
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
pub use multiagent::{
    BUDGET_EXHAUSTED_CLASS, BoardGating, CHILD_TIMEOUT_REQUESTED_BY, CapRefusal,
    DEFAULT_CHILD_TIMEOUT_MS, DEFAULT_MAX_CHILDREN, DEFAULT_MAX_DEPTH, DepReadiness,
    EMPTY_RESPONSE_CLASS, MultiagentLimits, Reconcile, SUCCESS_SET, SpawnIdentity, TaskOutcome,
    cap_refusal, child_session_id, child_thread_id, depth_of, derive_outcome, holding_children,
    join_target, legacy_report_key, outcome_evidence_ref, outcome_for_completion, ready_deps,
    reconcile_completion, report_key, report_key_for, spawn_uid, task_id_for_key, task_input_key,
};
pub use outbox::{
    DEFAULT_BACKOFF_CAP_MS, DEFAULT_BACKOFF_START_MS, DEFAULT_MAX_ATTEMPTS, Delivered,
    ManualRetryClock, OutboxConfig, OutboxDriver, OutboxEntry, OutboxMetrics, OutboxPark,
    OutboxState, OutboxStatus, PARK_ERROR_PREFIX, RetryClock, SystemRetryClock, ThreadOutboxView,
    conversation_key, intent_ticket, outbox_pending, reconstruct_delivery, reply_detail_mode,
    with_attempt_keys, worker_conversation_keys,
};
pub use policy::{
    DEFAULT_MAX_WORDS, DEFAULT_SECTIONS, DETAIL_REQUEST_TEXTS, ELLIPSIS, EnforcedReply,
    IM_POLICY_NAME, ImPolicy, PolicyError, STRUCTURED_POLICY_NAME, SpeakCondition, SpeakContext,
    SpeakKind, SpeakPolicy, SpeakRule, SpeakTracker, SpeakVerdict, StructuredPolicy, count_words,
    im_delivered_payload, im_delivered_payload_full, is_detail_request, speak_key,
    structured_delivered_payload,
};
pub use policy_version::{
    DriftMarking, DriftRecord, NO_POLICY_LABEL, PolicyVersion, drift, notify_drift,
    parse_policy_header, persisted_bootstrap_prompt, policy_header, policy_marker_key,
    run_drift_marking, scan_drift, with_policy_header,
};
pub use provenance::{
    CHANNEL_EVENT_TYPE, ChannelEventMarker, ShapingConfig, channel_event_untrusted,
    channel_input_payload, channel_input_text, is_untrusted_view, latest_channel_input_text_before,
    parse_channel_marker, raw_plain_text, raw_view, shape_channel_input,
    shape_channel_input_marked, tool_call_seq, wrap_channel_event, wrap_channel_event_marked,
};
pub use reasons::{DecisionKind, IntakeReason, outcome_reasons, routed_reason};
pub use recall::{RecallPort, RecallPortError, SpillIndex, thread_bindings};
pub use registry::{PassthroughRegistry, WorkerStatus, worker_sessions};
pub use roster::{
    AgentSurface, CheckOutcome, MandateStrength, MandateTemplate, OUTPUT_SCHEMA_MAX_BYTES,
    ResolvedAgent, Roster, RosterEntry, RosterError, RosterLimits, SUPPORTED_SCHEMA_KEYWORDS,
    SUPPORTED_TYPE_NAMES, SchemaError, default_roster_max_children, validate_instance,
    validate_output_schema, validate_schema,
};
pub use router::{
    ChannelRegistry, DEFAULT_PARK_TICK_MS, DEFAULT_WORKER_QUEUE_TIMEOUT_MS,
    DELIVER_STRUCTURED_TOOL, JUDGE_PROMPT_TEMPLATE, JudgePrompt, LIST_AGENTS_DESCRIPTION,
    LIST_AGENTS_TOOL, LIST_WORKERS_DESCRIPTION, LIST_WORKERS_TOOL, MandatePolicy, REPLY_TOOL,
    ROUTER_BOOTSTRAP_KEY, RouterDriver, RouterHold, RouterInbox, RouterProfile, RouterTools,
    SILENT_REPRO_TEMPLATE, SpeculationPolicy, WORKER_MEMBERSHIP_PROMPT_BLOCK, WorkerEntry,
    build_mandate, enforce_speculation_cap, outstanding_threads,
};
pub use rules::{
    AdmissionClass, AdmissionClassRule, AdmissionClassTable, CorrelateBy, Route, RuleMatcher,
    SubscriptionRule, SubscriptionRuleSet, event_has_explicit_signal,
};
pub use stage::{NoOutbox, OutboxView, TicketStage, project_stage};
pub use subscriptions::{FanoutAdmission, RoutingOutcome, SubscriptionService};
pub use templates::{
    ASYNC_TOOLS, CHANNEL_TOOLS, CONFIRM_ANSWER_PROMPT_LINE, ProfileError,
    ROSTER_INSTRUCTION_PROMPT_LINE, ROUTER_SLOT, SPAWN_WORKER_TOOL, SuperAgentProfile,
    WORKER_PROMPT_TEMPLATE, WORKER_TOOLS, empty_tool_parameters, is_channel_tool,
    mount_async_tools, mount_recall_tools, mount_watchdog_tools, render_worker_prompt,
    router_template, worker_mandate_line, worker_template,
};
pub use timeout::{ChildTick, due_interrupts};
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
pub use wechat::{
    DEFAULT_API_BASE as WECHAT_DEFAULT_API_BASE, SEND_SPLIT_CHARS,
    SUBSCRIBER_CAPACITY as WECHAT_SUBSCRIBER_CAPACITY, WECHAT_CHANNEL_TYPE, WechatChannel,
    WechatConfig, WechatError, message_to_event, split_text,
};
pub use worker::{
    BoundThread, BusyOwner, Delegation, DelegationPair, EXEC_WORKER_KEY, LineagePlan, MANDATE_KEY,
    MandateRevalidation, PortFor, REPORT_STATE_KEY, ReportState, SCHEMA_GATE_NAME, SchemaGatePlan,
    SpawnError, SpawnRequest, SpawnSessionWriter, SpawnedWorker, StreamPort, StreamPortError,
    ThreadLiveness, ThreadReport, WORKER_GATE_NAME, WorkerBinding, WorkerLoopConfig,
    WorkerLoopDriver, WorkerSpawner, WorkerStreamsError, WorkerStreamsPort, bound_workers,
    busy_owner, completion_summary, delegation_content, delegation_is_speculative,
    delegation_mandate, delegation_pairs, delegation_worker_binding, final_assistant_answer,
    lineage_plan, mandate_delegation_content, parse_delegation, parse_thread_report,
    revalidate_mandate, schema_correction_content, schema_correction_count, schema_correction_key,
    schema_gate_checks, schema_gate_plan, speculative_delegation_content, stream_assignee_of,
    thread_completed_payload, thread_liveness, thread_report_content, thread_report_content_stated,
    thread_started_payload, thread_started_payload_bound, thread_started_worker_binding,
    worker_binding_of,
};
