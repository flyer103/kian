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
//! - [`provenance`] — the channel-event wrapper riding inside the input
//!   payload's `content` (the frozen C4 payload contract stays untouched)
//! - [`registry`] — the passthrough worker registry: a disposable
//!   projection rebuilt from event streams (constraint 1)
//! - [`subscriptions`] — the subscription service consuming
//!   `ChannelAdapter` streams, matching rules, admitting + waking
//! - [`mock`] — `MockChannel`: the production-quality in-memory channel
//!   (the S4 IM stand-in and the framework's reference implementation)
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

pub mod inbox;
pub mod mock;
pub mod policy;
pub mod provenance;
pub mod registry;
pub mod router;
pub mod rules;
pub mod subscriptions;
pub mod templates;
pub mod worker;

pub use inbox::{AdmitReceipt, InboxWriteError, InboxWriter};
pub use mock::{CapturedDelivery, MockChannel};
pub use policy::{
    DEFAULT_MAX_WORDS, DEFAULT_SECTIONS, ELLIPSIS, EnforcedReply, IM_POLICY_NAME, ImPolicy,
    PolicyError, STRUCTURED_POLICY_NAME, StructuredPolicy, count_words, im_delivered_payload,
    structured_delivered_payload,
};
pub use provenance::{
    CHANNEL_EVENT_TYPE, ChannelEventMarker, channel_input_payload, parse_channel_marker,
};
pub use registry::{PassthroughRegistry, worker_sessions};
pub use router::{
    ChannelRegistry, DEFAULT_PARK_TICK_MS, DELIVER_STRUCTURED_TOOL, JUDGE_PROMPT_TEMPLATE,
    JudgePrompt, REPLY_TOOL, ROUTER_BOOTSTRAP_KEY, RouterDriver, RouterHold, RouterInbox,
    RouterProfile, RouterTools, outstanding_threads,
};
pub use rules::{CorrelateBy, Route, RuleMatcher, SubscriptionRule, SubscriptionRuleSet};
pub use subscriptions::{RoutingOutcome, SubscriptionService};
pub use templates::{
    CHANNEL_TOOLS, ProfileError, ROUTER_SLOT, SPAWN_WORKER_TOOL, SuperAgentProfile, WORKER_TOOLS,
    empty_tool_parameters, is_channel_tool, router_template, worker_template,
};
pub use worker::{
    Delegation, DelegationPair, LineagePlan, PortFor, SpawnError, SpawnedWorker, StreamPort,
    StreamPortError, ThreadReport, WORKER_GATE_NAME, WorkerLoopConfig, WorkerLoopDriver,
    WorkerSpawner, completion_summary, delegation_content, delegation_pairs, lineage_plan,
    parse_delegation, parse_thread_report, thread_completed_payload, thread_report_content,
    thread_started_payload,
};
