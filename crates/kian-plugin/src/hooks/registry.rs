//! The hook-scoped tier-0 registry (PLUGIN-SPEC §5.3, ADR-028 R13).
//!
//! The existing `Ctx` dispatch keys handlers by kind and `TypeId` and
//! carries no plugin/hook/session metadata, so the hook bus keeps its own
//! registry here. Registration is reversible through the existing
//! [`Disposer`](crate::ctx::Disposer) — disposal order is
//! registration-reverse, identical to today's semantics. Raw
//! `Ctx::on`/`on_waterfall`/`on_bail` stay exactly as they are for
//! non-hook typed events.
//!
//! [`register`]/[`register_scoped`] are free functions over the
//! process-global registry (§5.3's sketch); [`Registry`] instances are
//! also constructible directly so [`HookBus`](super::bus::HookBus) tests
//! — and later the assembly — can hold isolated registries.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use futures::future::BoxFuture;
use kian_core::model::SessionId;

use super::grants::GrantSet;
use super::ids::HookId;
use super::views::{
    AdmitDraft, AdmitReceiptView, FrameInfo, RecoveryView, RequestMeta, ResponseView, SettleView,
    ToolCallView, ToolPostView,
};
use crate::ctx::Disposer;

/// Registration scope (§1 "Session scope vs server scope"): a tier-0
/// registration made at process start is **server scope** (applies to
/// every session); a `register_scoped` registration applies only to
/// sessions whose effective set contains it — at tier 0, approximated by
/// the dispatch's target session id (K5: exactly two variants in v1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionScope {
    Server,
    Session(SessionId),
}

impl SessionScope {
    /// Whether a dispatch with this frame identity falls inside the scope.
    /// A dispatch with no frame identity can only match server scope —
    /// firing a session-scoped handler without knowing the session would
    /// be a guess, and the contract forbids implicit identity (R11).
    fn matches(&self, frame: Option<&FrameInfo>) -> bool {
        match self {
            SessionScope::Server => true,
            SessionScope::Session(scoped) => frame.is_some_and(|f| f.session == *scoped),
        }
    }
}

/// A tier-0 handler registration's identity (§5.3's sketch, verbatim).
#[derive(Debug, Clone)]
pub struct HandlerId {
    pub plugin: &'static str,
    pub hook: HookId,
    pub grants: GrantSet,
    /// Dispatch order within the hook; ties break by declaration index
    /// (§8.3.1 — the sidecar `[plugin].order`/name tiebreak is the
    /// sidecar tier's own rule).
    pub order: i32,
}

/// One dispatched payload, wrapping the §3.2 view of the hook being
/// dispatched. Handlers receive the same Rust struct the tier-1 ABI
/// serializes, so both tiers observe identical fields (§1 "View").
#[derive(Debug, Clone)]
pub enum HookPayload {
    AdmitDraft(AdmitDraft),
    AdmitReceipt(AdmitReceiptView),
    TurnRequest(RequestMeta),
    TurnResponse(ResponseView),
    ToolPre(ToolCallView),
    ToolPost(ToolPostView),
    Recovery(RecoveryView),
    Settle(SettleView),
}

impl HookPayload {
    /// The hook this payload dispatches to.
    pub fn hook(&self) -> HookId {
        match self {
            HookPayload::AdmitDraft(_) => HookId::AdmitPre,
            HookPayload::AdmitReceipt(_) => HookId::AdmitPost,
            HookPayload::TurnRequest(_) => HookId::TurnRequest,
            HookPayload::TurnResponse(_) => HookId::TurnResponse,
            HookPayload::ToolPre(_) => HookId::ToolPre,
            HookPayload::ToolPost(_) => HookId::ToolPost,
            HookPayload::Recovery(_) => HookId::RecoverDecided,
            HookPayload::Settle(_) => HookId::SettlePre,
        }
    }

    /// The embedded dispatch identity (wire-skipped; §7.3 frame metadata).
    pub fn frame(&self) -> Option<&FrameInfo> {
        match self {
            HookPayload::AdmitDraft(d) => d.frames.first(),
            HookPayload::AdmitReceipt(v) => v.frame.as_ref(),
            HookPayload::TurnRequest(v) => v.frame.as_ref(),
            HookPayload::TurnResponse(v) => v.frame.as_ref(),
            HookPayload::ToolPre(v) => v.frame.as_ref(),
            HookPayload::ToolPost(v) => v.frame.as_ref(),
            HookPayload::Recovery(v) => v.frame.as_ref(),
            HookPayload::Settle(v) => v.frame.as_ref(),
        }
    }
}

/// A tier-0 handler's answer, shaped by the §3.3 reply vocabulary. The
/// `Err` variant mirrors the ABI's closed code set (§7.2): `deadline` and
/// `crashed` are self-declared unreachability, everything else is a
/// speech defect (§11.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookReply {
    Ack { note: Option<String> },
    FlowNext,
    FlowStop { reason: String },
    BailNone,
    BailDeny { reason: String },
    Err { code: ReplyErrCode, message: String },
}

/// The closed `err.code` set of the ABI (§7.2). A code outside the set is
/// itself `malformed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyErrCode {
    Deadline,
    Crashed,
    Malformed,
    Unauthorized,
    UnsupportedHook,
    ProtocolViolation,
}

impl ReplyErrCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ReplyErrCode::Deadline => "deadline",
            ReplyErrCode::Crashed => "crashed",
            ReplyErrCode::Malformed => "malformed",
            ReplyErrCode::Unauthorized => "unauthorized",
            ReplyErrCode::UnsupportedHook => "unsupported_hook",
            ReplyErrCode::ProtocolViolation => "protocol_violation",
        }
    }
}

/// A tier-0 hook handler: an async function over the dispatched payload.
pub type HookFn = Arc<dyn for<'a> Fn(&'a HookPayload) -> BoxFuture<'a, HookReply> + Send + Sync>;

/// The hook registry: a list of reversible registrations, snapshotted per
/// dispatch in §8.3 order.
#[derive(Default)]
pub struct Registry {
    next_id: AtomicU64,
    entries: RwLock<Vec<Option<Arc<Entry>>>>,
}

/// One live registration.
pub(crate) struct Entry {
    pub(crate) id: u64,
    pub(crate) handler: HandlerId,
    pub(crate) scope: SessionScope,
    pub(crate) f: HookFn,
}

impl std::fmt::Debug for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("id", &self.id)
            .field("plugin", &self.handler.plugin)
            .field("hook", &self.handler.hook)
            .field("scope", &self.scope)
            .finish()
    }
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a server-scope handler (applies to every session, §5.3)
    /// and returns the [`Disposer`] removing exactly this registration.
    pub fn register(self: &Arc<Self>, handler: HandlerId, f: HookFn) -> Disposer {
        self.register_scoped(SessionScope::Server, handler, f)
    }

    /// Registers a session-scoped handler (§5.3): it participates only in
    /// dispatches whose frame targets that session.
    pub fn register_scoped(
        self: &Arc<Self>,
        scope: SessionScope,
        handler: HandlerId,
        f: HookFn,
    ) -> Disposer {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let entry = Arc::new(Entry {
            id,
            handler,
            scope,
            f,
        });
        {
            let mut entries = self
                .entries
                .write()
                .expect("kian-plugin: hook registry lock poisoned");
            entries.push(Some(Arc::clone(&entry)));
        }
        let registry = Arc::clone(self);
        Disposer::from_fn(move || {
            let mut entries = registry
                .entries
                .write()
                .expect("kian-plugin: hook registry lock poisoned");
            // Remove only this registration's own slot — a stale disposer
            // must never unregister a replacement.
            for slot in entries.iter_mut() {
                if let Some(live) = slot
                    && live.id == id
                {
                    *slot = None;
                }
            }
        })
    }

    /// The handlers subscribed to `hook` whose scope matches the
    /// dispatch's frame identity, in §8.3 tier-0 order: ascending
    /// `order`, tiebroken by declaration index.
    pub(crate) fn snapshot(&self, hook: &HookId, frame: Option<&FrameInfo>) -> Vec<Arc<Entry>> {
        let entries = self
            .entries
            .read()
            .expect("kian-plugin: hook registry lock poisoned");
        let mut matched: Vec<(i32, usize, Arc<Entry>)> = entries
            .iter()
            .enumerate()
            .filter_map(|(index, slot)| {
                let entry = slot.as_ref()?;
                (entry.handler.hook == *hook && entry.scope.matches(frame))
                    .then(|| (entry.handler.order, index, Arc::clone(entry)))
            })
            .collect();
        matched.sort_by_key(|(order, index, _)| (*order, *index));
        matched.into_iter().map(|(_, _, entry)| entry).collect()
    }
}

static GLOBAL: OnceLock<Arc<Registry>> = OnceLock::new();

/// The process-global registry backing the §5.3 free functions.
pub fn global_registry() -> &'static Arc<Registry> {
    GLOBAL.get_or_init(|| Arc::new(Registry::new()))
}

/// Server-scope registration over the process-global registry (§5.3).
pub fn register(handler: HandlerId, f: HookFn) -> Disposer {
    global_registry().register(handler, f)
}

/// Session-scoped registration over the process-global registry (§5.3).
pub fn register_scoped(scope: SessionScope, handler: HandlerId, f: HookFn) -> Disposer {
    global_registry().register_scoped(scope, handler, f)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use futures::FutureExt;

    use super::super::grants::Grant;

    fn recording_handler(
        plugin: &'static str,
        grants: &[Grant],
        reply: HookReply,
        log: Arc<Mutex<Vec<String>>>,
    ) -> (HandlerId, HookFn) {
        let handler = HandlerId {
            plugin,
            hook: HookId::ToolPre,
            grants: GrantSet::from_grants(grants.iter().copied()),
            order: 0,
        };
        let f: HookFn = Arc::new(move |_payload: &HookPayload| {
            let log = Arc::clone(&log);
            let reply = reply.clone();
            async move {
                log.lock().unwrap().push(plugin.to_string());
                reply
            }
            .boxed()
        });
        (handler, f)
    }

    fn tool_pre_payload(frame: Option<FrameInfo>) -> HookPayload {
        HookPayload::ToolPre(ToolCallView {
            call_id: "call_1".to_string(),
            name: "exec".to_string(),
            iteration: 1,
            arguments: serde_json::json!({}),
            arguments_bytes: 2,
            arguments_sha256: "ab".to_string(),
            arguments_truncated: false,
            permission: super::super::views::PermissionView {
                evaluated: "allow".to_string(),
                policy: "default".to_string(),
                matched: "exec".to_string(),
                reason: None,
            },
            surface: "react".to_string(),
            frame,
        })
    }

    #[test]
    fn disposer_reverses_registration() {
        let registry = Arc::new(Registry::new());
        let log = Arc::new(Mutex::new(Vec::new()));

        let (id_a, f_a) = recording_handler(
            "a",
            &[Grant::ObserveTool],
            HookReply::Ack { note: None },
            Arc::clone(&log),
        );
        let (id_b, f_b) = recording_handler(
            "b",
            &[Grant::ObserveTool],
            HookReply::Ack { note: None },
            Arc::clone(&log),
        );
        let keep_a = registry.register(id_a, f_a);
        let keep_b = registry.register(id_b, f_b);

        // Merged disposal unwinds in reverse registration order (the
        // existing Disposer semantics): `later.extend` runs the later
        // registration's teardown first.
        let merged = Disposer::new().extend(keep_a).extend(keep_b);
        assert_eq!(registry.snapshot(&HookId::ToolPre, None).len(), 2);
        merged.dispose();
        assert!(
            registry.snapshot(&HookId::ToolPre, None).is_empty(),
            "disposal removes exactly its own registrations"
        );

        // A stale disposer never unregisters a replacement (the same
        // guard `Ctx::provide` carries).
        let (id_c, f_c) = recording_handler(
            "c",
            &[Grant::ObserveTool],
            HookReply::Ack { note: None },
            Arc::clone(&log),
        );
        let stale = {
            let (id_x, f_x) = recording_handler(
                "x",
                &[Grant::ObserveTool],
                HookReply::Ack { note: None },
                Arc::clone(&log),
            );
            registry.register(id_x, f_x)
        };
        let _keep_c = registry.register(id_c, f_c);
        stale.dispose();
        assert_eq!(
            registry.snapshot(&HookId::ToolPre, None).len(),
            1,
            "the stale disposer removed its own slot only"
        );
    }

    #[tokio::test]
    async fn scoped_registration_is_session_filtered() {
        let registry = Arc::new(Registry::new());
        let hits = Arc::new(Mutex::new(Vec::new()));

        let session_a: SessionId =
            serde_json::from_str("\"0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b\"").unwrap();
        let session_b: SessionId =
            serde_json::from_str("\"1f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b\"").unwrap();

        let scoped_hits = Arc::clone(&hits);
        let _keep_scoped = registry.register_scoped(
            SessionScope::Session(session_a),
            HandlerId {
                plugin: "scoped",
                hook: HookId::ToolPre,
                grants: GrantSet::from_grants([Grant::ObserveTool]),
                order: 0,
            },
            Arc::new(move |_payload: &HookPayload| {
                let hits = Arc::clone(&scoped_hits);
                async move {
                    hits.lock().unwrap().push("scoped".to_string());
                    HookReply::Ack { note: None }
                }
                .boxed()
            }),
        );
        let server_hits = Arc::clone(&hits);
        let _keep_server = registry.register(
            HandlerId {
                plugin: "server",
                hook: HookId::ToolPre,
                grants: GrantSet::from_grants([Grant::ObserveTool]),
                order: 0,
            },
            Arc::new(move |_payload: &HookPayload| {
                let hits = Arc::clone(&server_hits);
                async move {
                    hits.lock().unwrap().push("server".to_string());
                    HookReply::Ack { note: None }
                }
                .boxed()
            }),
        );

        // Dispatch at session B: only the server-scope handler fires.
        let payload = tool_pre_payload(Some(FrameInfo::new(session_b)));
        for entry in registry.snapshot(&payload.hook(), payload.frame()) {
            (entry.f)(&payload).await;
        }
        assert_eq!(*hits.lock().unwrap(), vec!["server".to_string()]);

        // Dispatch at session A: both fire — scoped was declared first, so
        // the declaration-index tiebreak puts it first (§8.3.1).
        hits.lock().unwrap().clear();
        let payload = tool_pre_payload(Some(FrameInfo::new(session_a)));
        for entry in registry.snapshot(&payload.hook(), payload.frame()) {
            (entry.f)(&payload).await;
        }
        assert_eq!(
            *hits.lock().unwrap(),
            vec!["scoped".to_string(), "server".to_string()]
        );

        // A dispatch with no frame identity matches server scope only —
        // never a guess at the session (R11).
        hits.lock().unwrap().clear();
        let payload = tool_pre_payload(None);
        for entry in registry.snapshot(&payload.hook(), payload.frame()) {
            (entry.f)(&payload).await;
        }
        assert_eq!(*hits.lock().unwrap(), vec!["server".to_string()]);
    }
}
