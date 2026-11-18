//! `kian-plugin::hooks` — the hook-plugin core (PLUGIN-SPEC v2.2,
//! ADR-028): the closed hook vocabulary, the §3.2 payload views, the
//! §5 grant model, the hook-scoped tier-0 registry and the `HookBus`
//! dispatch core. P3a adds the sidecar tier's runner/ABI/manifest
//! ([`sidecar`]); the config surface, per-worker resolution and the
//! set digest are P3b.
//!
//! The bus never writes events, never touches projections and never
//! imports a harness (§10.7 / ADR-028 D4): it is called from control
//! flow and returns verdicts. Nothing in this module can append an event.
//!
//! Module map (K1 — later phases grow `sidecar/`, `resolve.rs`,
//! `digest.rs` without moving these):
//! - [`ids`] — the closed §3.1 catalog (`HookId`, `HookMode`,
//!   `HookClass`), the empty harness registry (§3.6)
//! - [`views`] — the §3.2 payload structs and the verdict types
//! - [`grants`] — the closed §5.1 grant set and the reply → required
//!   grant map (§3.3)
//! - [`registry`] — hook-scoped tier-0 registration (§5.3) over the
//!   existing `Disposer`
//! - [`failure`] — failure classification and the grant-keyed class
//!   defaults (§11.1); [`failure::resolve_outcome`] is the only place a
//!   reply becomes a verdict (K6)
//! - [`bus`] — the `HookBus` façade: §8.3 dispatch order, the §4.1
//!   admission grid, `admit_post`, the §4.6 settle decisioning, the
//!   §10.6 audit lines and the K7 veto marker
//! - [`ports`] — the seven port methods (§4, verbatim) and `SettleCtl`
//! - [`wire`] — the §3.2/§7.6 bounds and digest helpers the P2 call
//!   sites build their views with (one `sha2` edge, K10)
//! - [`sidecar`] — the P3a sidecar tier: §6 manifest, §7 stdio ABI,
//!   the spawn-hardened runner with §6.4/§6.5 counters and quarantine

pub mod bus;
pub mod digest;
pub mod failure;
pub mod grants;
pub mod ids;
pub mod ports;
pub mod registry;
pub mod resolve;
pub mod session;
pub mod sidecar;
pub mod views;
pub mod wire;

pub use bus::{
    DEFAULT_MAX_SETTLE_VETOS, HookBus, VETO_MARKER_PREFIX, is_veto_marker, veto_marker,
    veto_marker_prefix,
};
pub use digest::{process_sha256, set_digest};
pub use failure::{Cause, ClassDefault, FailureClass, Outcome, SpeechCode, resolve_outcome};
pub use grants::{
    Grant, GrantError, GrantSet, ReplyShape, observe_grant_for_class, reply_required_grant,
    veto_grant_for_class,
};
pub use ids::{
    HARNESS_REGISTRY, HarnessPoint, HookClass, HookId, HookMode, PayloadKind, catalog,
    harness_declared,
};
pub use ports::{HookPorts, SettleCtl};
pub use registry::{
    HandlerId, HookFn, HookPayload, HookReply, Registry, ReplyErrCode, SessionScope,
    global_registry, register, register_scoped,
};
pub use resolve::{
    EffectivePlugin, InstanceDecl, LimitOverrides, Resolution, ResolveError, ResolveErrorCode,
    ScopesConfig, SidecarConfig, WorkerScope, resolve, validate, validate_shape,
};
pub use session::{SessionSetGuard, SidecarRuntime};
pub use views::{
    AdmissionInfo, AdmitDraft, AdmitInput, AdmitPostInput, AdmitReceiptView, AdmitTarget,
    AdmitVerdict, ChannelProvenance, EXEC_WORKER_KEY, FrameInfo, Origin, PermissionView,
    RecoveryView, RedactionReport, RequestMeta, ResponseView, SettleVerdict, SettleView,
    ToolCallView, ToolPostView, ToolVerdict, TurnErrorAudit, durable_worker_binding,
};
pub use wire::{
    FRAME_PAYLOAD_MAX_BYTES, PREVIEW_MAX_BYTES, bounded_value, preview, serialized, sha256_hex,
};
