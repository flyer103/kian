//! `kian-llm` — ModelProvider seam, slot resolution, sent-bytes persistence
//! hooks.
//!
//! DESIGN §7 assigns this crate the key interfaces `ModelProvider` and
//! `SlotResolver`. It implements the [`kian_core::seams::ModelProvider`]
//! seam for openai-compatible endpoints.
//!
//! Persist-what-you-send (AGENTS.md constraint 2): the exact request bytes
//! are handed to the caller for persistence *before* the HTTP request
//! flies. Request serialization — including tool-schema ordering — is
//! frozen stable content (constraint 3): changing it invalidates provider
//! prefix cache for every session.
//!
//! Modules:
//! - [`provider`] — openai-compat ModelProvider over reqwest/SSE
//! - [`slots`] — slot resolution and fallback chains (ADR-006)
//! - [`plan`] — one request plan shared by sender, persistence and budget
//! - [`turn`] — one provider turn: failover traversal driven by slots
pub mod plan;
pub mod provider;
pub mod slots;
pub mod turn;

pub use kian_core::seams::{ModelChunk, ModelProvider, ModelRequest, ProviderError, ToolSchema};

pub use plan::{
    AuditError, BodyForm, BodyVersion, ESTIMATOR_SOURCE, ESTIMATOR_VERSION, Expansion, RequestPlan,
    TokenEstimate, build_request_plan, expand_audit_body,
};
pub use provider::{
    ChatMessage, FROZEN_CHAT_BODY_SHA256, OpenAiCompatConfig, OpenAiCompatProvider, PersistError,
    PersistSink, PersistedRequest, WireFunctionCall, WireToolCall,
};
pub use slots::{
    FallbackCursor, FallbackSwitch, SessionSlots, SlotBinding, SlotError, SlotMigration,
    SlotResolver,
};
pub use turn::{ProviderRegistry, SlotEventSink, complete_turn};
