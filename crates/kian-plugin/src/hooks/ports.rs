//! The seven interception port methods (PLUGIN-SPEC §4, verbatim) and
//! [`SettleCtl`] — the one port that needs an effect (§4.6), implemented
//! by the caller (`kian-superagent`) with the `InboxWriter` it already
//! holds.
//!
//! Ports are plain-data interfaces: harnesses never see plugin types and
//! plugins never see harness types. No port receives a `DrainCtx` and no
//! port may append an event; [`HookBus`](super::bus::HookBus) implements
//! the trait, and the `admit/post` hook is **not** an eighth method — it
//! is dispatched by the concrete `HookBus::admit_post` the bundle calls
//! after each durable row (K3, §4.1 step 4).

use std::sync::Arc;

use futures::future::BoxFuture;

use super::bus::HookBus;
use super::views::{
    AdmitDraft, AdmitVerdict, RecoveryView, RequestMeta, ResponseView, SettleVerdict, SettleView,
    ToolCallView, ToolPostView, ToolVerdict,
};

/// The settle gate's two operations (§4.6): admit one synthetic input
/// through the caller's inbox, and read the veto count for a plugin from
/// the session's stream. Implemented by the settle caller; the bus holds
/// no store handle and never appends events itself (§10.7).
pub trait SettleCtl: Send + Sync {
    /// Admits the veto-marker content as the existing, honest steer path
    /// would; `Err` carries the append error (§4.6's three-step failure
    /// path begins here). Async because the caller's `InboxWriter` is
    /// async and the gate consults inside a drain — a sync surface
    /// would force `block_on` on a tokio worker (amended in P2; the
    /// two operations are unchanged, §4).
    fn admit_synthetic(
        &self,
        content: String,
    ) -> futures::future::BoxFuture<'static, Result<(), String>>;

    /// The number of veto-marker rows naming `plugin` already in this
    /// session's stream (§4.6's budget rule — a projection of the
    /// durable stream, not side state).
    fn veto_count(&self, plugin: &str) -> u64;
}

/// The seven port methods at their nine call sites (§4's table; the
/// `tool_post` method's second, dsh site goes through a crate-local port
/// in `kian-dsh` and never through this trait). Shapes are plain serde
/// structs; injected as `Arc<dyn HookPorts>` into `kian-loop` and
/// `Arc<HookBus>` into the bundle (K3).
pub trait HookPorts: Send + Sync {
    fn admit(&self, draft: &AdmitDraft) -> BoxFuture<'static, AdmitVerdict>;
    /// Observe (`turn/request`, §4.2).
    fn request(&self, meta: &RequestMeta) -> BoxFuture<'static, ()>;
    /// Observe (`turn/response`, §4.3).
    fn response(&self, view: &ResponseView) -> BoxFuture<'static, ()>;
    fn tool_pre(&self, call: &ToolCallView) -> BoxFuture<'static, ToolVerdict>;
    /// Observe (`tool/post`, §4.4 step 5).
    fn tool_post(&self, view: &ToolPostView) -> BoxFuture<'static, ()>;
    /// Observe (`recover/decided`, §4.5).
    fn recover(&self, view: &RecoveryView) -> BoxFuture<'static, ()>;
    /// The completion gate (§4.6). The `SettleCtl` rides an `Arc` so the
    /// `'static` future the §4 sketch fixes can own it — a `&dyn` borrow
    /// cannot cross the boxing boundary (a compiler-forced deviation,
    /// recorded in the P1 report).
    fn settle(
        &self,
        view: &SettleView,
        ctl: Arc<dyn SettleCtl>,
    ) -> BoxFuture<'static, SettleVerdict>;
}

impl HookPorts for HookBus {
    fn admit(&self, draft: &AdmitDraft) -> BoxFuture<'static, AdmitVerdict> {
        let bus = self.clone();
        let draft = draft.clone();
        Box::pin(async move { bus.admit(&draft).await })
    }

    fn request(&self, meta: &RequestMeta) -> BoxFuture<'static, ()> {
        let bus = self.clone();
        let meta = meta.clone();
        Box::pin(async move { bus.request(&meta).await })
    }

    fn response(&self, view: &ResponseView) -> BoxFuture<'static, ()> {
        let bus = self.clone();
        let view = view.clone();
        Box::pin(async move { bus.response(&view).await })
    }

    fn tool_pre(&self, call: &ToolCallView) -> BoxFuture<'static, ToolVerdict> {
        let bus = self.clone();
        let call = call.clone();
        Box::pin(async move { bus.tool_pre(&call).await })
    }

    fn tool_post(&self, view: &ToolPostView) -> BoxFuture<'static, ()> {
        let bus = self.clone();
        let view = view.clone();
        Box::pin(async move { bus.tool_post(&view).await })
    }

    fn recover(&self, view: &RecoveryView) -> BoxFuture<'static, ()> {
        let bus = self.clone();
        let view = view.clone();
        Box::pin(async move { bus.recover(&view).await })
    }

    fn settle(
        &self,
        view: &SettleView,
        ctl: Arc<dyn SettleCtl>,
    ) -> BoxFuture<'static, SettleVerdict> {
        let bus = self.clone();
        let view = view.clone();
        Box::pin(async move { bus.settle(&view, ctl.as_ref()).await })
    }
}
