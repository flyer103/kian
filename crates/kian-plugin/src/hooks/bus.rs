//! The `HookBus` dispatch core (PLUGIN-SPEC §8.3, §4.1, §4.6; ADR-028
//! D13, D14, D20).
//!
//! Tier-0 only in P1: handlers come from the hook-scoped registry, in
//! §8.3 order (ascending `order`, declaration index tiebreak), and a
//! `bail`/`flow` hook stops at the first valid deny/stop — one audit line
//! records `winner`, the `skipped` count and the reason. Replies are
//! resolved through [`resolve_outcome`](super::failure::resolve_outcome)
//! only: out-of-grant and wrong-mode speech is discarded as-if-absent,
//! unreachability takes the grant-keyed class default (§5.2, §11.1).
//!
//! The bus holds **no** `DrainCtx`, no store handle and no write API:
//! nothing in this module can append an event (§10.7). Its only effect
//! channel is the [`SettleCtl`](super::ports::SettleCtl) the settle
//! caller injects (§4.6).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use kian_core::model::SessionId;

use super::failure::{
    Cause, ClassDefault, Outcome, SpeechCode, grant_keyed_default, resolve_outcome,
};
use super::grants::Grant;
use super::ids::HookId;
use super::ports::SettleCtl;
use super::registry::{Entry, HookPayload, HookReply, Registry, global_registry};
use super::resolve;
use super::session;
use super::views::{
    AdmitDraft, AdmitReceiptView, AdmitTarget, AdmitVerdict, FrameInfo, Origin, RecoveryView,
    RequestMeta, ResponseView, SettleVerdict, SettleView, ToolCallView, ToolPostView, ToolVerdict,
};

/// The settle-veto marker prefix, owned here so the writer
/// ([`veto_marker`]) and the counter ([`is_veto_marker`]) cannot drift
/// (K7). The counting prefix includes the trailing space after the plugin
/// name, so `plugin=foo` never matches `plugin=foobar` (§3.2.8).
pub const VETO_MARKER_PREFIX: &str = "[kian:hook-veto plugin=";

/// The tier-0 settle budget: the manifest default `max_settle_vetoes = 3`
/// (§6.1). Tier-0 has no manifest and must not invent one (K8).
pub const DEFAULT_MAX_SETTLE_VETOS: u64 = 3;

/// The synthetic input a settle veto admits (§4.6 step 3's template,
/// verbatim): persisted before the model can see it, strippable via the
/// existing `synthetic` marker.
pub fn veto_marker(plugin: &str, hook: &str, reason: &str) -> String {
    format!(
        "[kian:hook-veto plugin={plugin} hook={hook} reason={reason}] {reason} — address this before finishing."
    )
}

/// The content prefix a veto-minted row must start with to count against
/// `plugin`'s budget (§3.2.8 / §4.6).
pub fn veto_marker_prefix(plugin: &str) -> String {
    format!("{VETO_MARKER_PREFIX}{plugin} ")
}

/// The budget predicate: `synthetic` + content prefix, and **never**
/// `role` (§4.6's trust boundary — a `steer`-capable caller can mint such
/// a row, which is the documented behaviour, not a control).
pub fn is_veto_marker(plugin: &str, synthetic: bool, content: &str) -> bool {
    synthetic && content.starts_with(&veto_marker_prefix(plugin))
}

/// One discarded reply, for the `hook-discard` audit line's fields.
/// The runtime audit line is emitted when the defect is classified;
/// this record is the in-process accounting tests assert on (K9:
/// `kian-plugin` asserts outcomes and counters, never log text).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct Discard {
    pub plugin: String,
    pub code: SpeechCode,
    pub grant: Option<Grant>,
}

/// What one dispatch resolved to — the outcome, the skip accounting and
/// the audit-adjacent records (§8.3). Kept `pub(crate)`: tests assert
/// through it; the public surface is the verdict types.
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub(crate) struct DispatchRecord {
    pub invoked: usize,
    pub winner: Option<String>,
    pub reason: Option<String>,
    pub skipped: usize,
    pub discards: Vec<Discard>,
    pub defaults: Vec<(String, ClassDefault)>,
}

impl DispatchRecord {
    /// A denial exists — a valid veto reply or a fail-closed class
    /// default (§11.1 consequence 1: an unreachable veto holder denies at
    /// any position).
    fn denied(&self) -> bool {
        self.winner.is_some()
    }
}

struct BusInner {
    registry: Arc<Registry>,
    next_dispatch_id: AtomicU64,
    /// In-process settle-veto attempts whose marker append failed, keyed
    /// by (session, plugin) — the §4.6 append-failure rule's counter, so
    /// `vetoes_used = max(rows_naming_this_plugin, this_gate's_attempts)`
    /// stays bounded when an inbox persistently fails.
    settle_failed_admits: Mutex<HashMap<(SessionId, String), u32>>,
    /// The sidecar runtime (§8–§9), installed once at boot by the
    /// assembly; `None` (the `sidecars:` section absent) leaves every
    /// consultation byte-identical to a build without the feature
    /// (§13.3).
    sidecars: session::SharedRuntime,
}

/// The hook bus façade. Cheap to clone (an `Arc` inside); a default bus
/// reads the process-global registry (§5.3), and an isolated one can be
/// built over a private [`Registry`] for tests and assemblies.
#[derive(Clone)]
pub struct HookBus {
    inner: Arc<BusInner>,
}

impl Default for HookBus {
    fn default() -> Self {
        Self::new()
    }
}

impl HookBus {
    /// The bus over the process-global tier-0 registry.
    pub fn new() -> Self {
        Self::with_registry(Arc::clone(global_registry()))
    }

    /// The bus over an explicit registry.
    pub fn with_registry(registry: Arc<Registry>) -> Self {
        Self {
            inner: Arc::new(BusInner {
                registry,
                next_dispatch_id: AtomicU64::new(0),
                settle_failed_admits: Mutex::new(HashMap::new()),
                sidecars: std::sync::RwLock::new(None),
            }),
        }
    }

    /// Installs the sidecar runtime (§8–§9) — the boot-time write the
    /// assembly performs once, after the inbox rebuild (§13.1). The
    /// manifests are the successfully mounted instances (refused
    /// optional instances are absent, §6.2). A second install is the
    /// purity break §9.2 step 4 exists to catch, so it is refused.
    pub fn install_sidecars(
        &self,
        cfg: resolve::SidecarConfig,
        manifests: std::collections::BTreeMap<String, super::sidecar::manifest::Manifest>,
    ) -> Result<(), String> {
        let mut slot = self
            .inner
            .sidecars
            .write()
            .expect("kian-plugin: sidecar runtime lock poisoned");
        if slot.is_some() {
            return Err(
                "sidecar runtime already installed — no live reload in v1 (§13.1); restart \
                 `kian serve` to change the set"
                    .to_string(),
            );
        }
        *slot = Some(Arc::new(session::SidecarRuntime::new(cfg, manifests)));
        Ok(())
    }

    /// The §9.2 guard at one drain begin: record the set digest at
    /// first sight (`hook-set` line), recompute and compare at every
    /// later claim served by this process (`hook-set-mismatch` line +
    /// session quarantine on inequality). A no-op without an installed
    /// runtime — byte-identical (§13.3). (Named `session_begin`, not
    /// `drain_begin`: the superagent iron-rule grep forbids the
    /// `drain_` prefix in bundle sources, and the boundary this marks
    /// is the session's, whichever harness drives it.)
    pub fn session_begin(&self, frame: FrameInfo) {
        let runtime = self.sidecar_runtime();
        if let Some(runtime) = runtime {
            runtime.drain_begin(&frame);
        }
    }

    /// The digest this process recorded for `session` (§9.2's audit
    /// record; `None` before the first open or without a runtime).
    pub fn session_set_digest(&self, session: &SessionId) -> Option<String> {
        self.sidecar_runtime()
            .and_then(|rt| rt.recorded_digest(session))
    }

    /// Whether `session` is under §9.3's session-lifetime quarantine
    /// (sidecar hooks take their class defaults; tier-0 unaffected).
    pub fn session_quarantined(&self, session: &SessionId) -> bool {
        self.sidecar_runtime()
            .is_some_and(|rt| rt.is_quarantined(session))
    }

    fn sidecar_runtime(&self) -> Option<Arc<session::SidecarRuntime>> {
        self.inner
            .sidecars
            .read()
            .expect("kian-plugin: sidecar runtime lock poisoned")
            .clone()
    }

    /// The registry this bus dispatches through.
    pub fn registry(&self) -> &Arc<Registry> {
        &self.inner.registry
    }

    /// One bus consultation of one hook for one event and one target
    /// session (§1 "Dispatch"): tier-0 handlers in §8.3 order, the first
    /// valid deny/stop (or fail-closed default) wins, the remaining
    /// subscribers are skipped and counted, and speech defects are
    /// discarded as-if-absent with one `hook-discard` audit line each.
    ///
    /// `targets` is the fan-out size the `hook-veto` line reports (`1` for
    /// every hook except an `admit/pre` grid, §10.6).
    async fn dispatch(
        &self,
        hook: HookId,
        payload: &HookPayload,
        origin: Origin,
        targets: usize,
    ) -> DispatchRecord {
        self.dispatch_per(hook, payload.frame(), origin, targets, |_| payload.clone())
            .await
    }

    /// [`Self::dispatch`] with a per-subscriber payload: `payload_for`
    /// rebuilds the view for each subscriber before its call — the
    /// §3.2.8 `settle/pre` path, whose `vetoes_used`/`vetoes_remaining`
    /// are **per plugin** (a second gate has its own numbers). Every
    /// other hook dispatches one uniform payload.
    async fn dispatch_per<F>(
        &self,
        hook: HookId,
        frame: Option<&FrameInfo>,
        origin: Origin,
        targets: usize,
        payload_for: F,
    ) -> DispatchRecord
    where
        F: Fn(&Entry) -> HookPayload,
    {
        let subscribers = self.inner.registry.snapshot(&hook, frame);
        let mut record = DispatchRecord {
            invoked: subscribers.len(),
            ..DispatchRecord::default()
        };
        // §8.2/§9.3 dispatch-time sidecar filter: with a runtime
        // installed, a sidecar-tier subscriber participates only in
        // sessions whose effective set contains it (the drop was
        // audited at resolution), and a session under §9.3 quarantine
        // sees its sidecar hooks take the grant-keyed class default —
        // tier-0 handlers are never affected (§9.2 step 6).
        let sidecars = self.sidecar_runtime();
        let session_state = sidecars.as_ref().map(|rt| rt.state_for(frame));
        // §10.6 `skipped` counts subscribers that would have been
        // consulted — a name the session's set dropped was never a
        // candidate, so it is not "skipped" (m-corrected: the count is
        // over the eligible tail, not the raw tail).
        let eligible = |subscriber: &Arc<Entry>| {
            let sidecar = sidecars
                .as_ref()
                .is_some_and(|rt| rt.is_sidecar(subscriber.handler.plugin));
            if sidecar && let Some(session::SessionState::Set(set)) = &session_state {
                set.names.contains(subscriber.handler.plugin)
            } else {
                true
            }
        };
        for (position, subscriber) in subscribers.iter().enumerate() {
            let dispatch_id = format!(
                "t0-{}",
                self.inner.next_dispatch_id.fetch_add(1, Ordering::Relaxed)
            );
            let started = Instant::now();
            let sidecar = sidecars
                .as_ref()
                .is_some_and(|rt| rt.is_sidecar(subscriber.handler.plugin));
            if !eligible(subscriber) {
                // Not this session's set: the name is simply absent
                // here — one audit line at resolution already named the
                // drop (§8.2).
                record.invoked -= 1;
                continue;
            }
            let quarantined =
                sidecar && matches!(session_state, Some(session::SessionState::Quarantined));
            let payload = payload_for(subscriber);
            let outcome = if quarantined {
                // §9.3: a quarantined sidecar is unreachable. The
                // default is synthesized with cause `quarantine` (never
                // `crashed`) through the same grant-keyed table.
                Outcome::Default(grant_keyed_default(
                    &subscriber.handler.grants,
                    &hook,
                    origin,
                    Cause::Quarantine,
                ))
            } else {
                let reply: HookReply = (subscriber.f)(&payload).await;
                resolve_outcome(&reply, &subscriber.handler.grants, &hook, origin)
            };
            let elapsed_ms = started.elapsed().as_millis() as u64;
            let plugin = subscriber.handler.plugin;
            match outcome {
                Outcome::AsIfAbsent => {
                    audit_dispatch(&hook, plugin, frame, &dispatch_id, "observed", elapsed_ms);
                }
                Outcome::Denied { reason } => {
                    audit_dispatch(&hook, plugin, frame, &dispatch_id, "deny", elapsed_ms);
                    record.skipped = subscribers[position + 1..]
                        .iter()
                        .filter(|s| eligible(s))
                        .count();
                    audit_veto(
                        &hook,
                        plugin,
                        frame,
                        &dispatch_id,
                        &reason,
                        record.skipped,
                        targets,
                    );
                    record.winner = Some(plugin.to_string());
                    record.reason = Some(reason);
                    break;
                }
                Outcome::Discarded { code, grant } => {
                    audit_dispatch(&hook, plugin, frame, &dispatch_id, "discarded", elapsed_ms);
                    audit_discard(&hook, plugin, code, grant);
                    record.discards.push(Discard {
                        plugin: plugin.to_string(),
                        code,
                        grant,
                    });
                }
                Outcome::Default(class_default) => match class_default {
                    ClassDefault::Deny { cause } | ClassDefault::RefuseAdmission { cause } => {
                        // An unreachable veto-capable subscriber is itself
                        // a fail-closed denial, at any position (§11.1
                        // consequence 1) — and the honest-error reason
                        // says exactly that (§11.1 row 1).
                        let reason = format!("hook unavailable ({})", cause.as_str());
                        audit_dispatch(
                            &hook,
                            plugin,
                            frame,
                            &dispatch_id,
                            "default-deny",
                            elapsed_ms,
                        );
                        audit_default(&hook, plugin, Some(cause));
                        record.skipped = subscribers[position + 1..]
                            .iter()
                            .filter(|s| eligible(s))
                            .count();
                        audit_veto(
                            &hook,
                            plugin,
                            frame,
                            &dispatch_id,
                            &reason,
                            record.skipped,
                            targets,
                        );
                        record.winner = Some(plugin.to_string());
                        record.reason = Some(reason);
                        record.defaults.push((plugin.to_string(), class_default));
                        break;
                    }
                    ClassDefault::Abstain | ClassDefault::Lapse => {
                        audit_dispatch(
                            &hook,
                            plugin,
                            frame,
                            &dispatch_id,
                            "default-lapse",
                            elapsed_ms,
                        );
                        audit_default(&hook, plugin, None);
                        record.defaults.push((plugin.to_string(), class_default));
                    }
                },
            }
        }
        record
    }

    /// The `admit/pre` evaluation (§4.1, K16): **one evaluation, N
    /// frames** — the view is produced once per inbound event, and the
    /// bus dispatches once per (subscriber, target session), each frame
    /// carrying that target's identity. The first valid `stop` (or
    /// fail-closed default of a `veto:admit` holder at channel origin)
    /// refuses the event for **every** target; frames not yet sent are
    /// not sent and are counted as skipped. The write loop starts only
    /// after the grid resolves — the caller's responsibility (P2).
    pub async fn admit(&self, draft: &AdmitDraft) -> AdmitVerdict {
        // Per-target (target, frame) pairs: the caller fills `frames`
        // aligned with `targets`; a shorter list gets §7.3's floor
        // values (no worker binding, no events yet). One Vec sorted by
        // target session id — "no frame mixes identities" is structural
        // (§4.1), not an invariant two independent sorts have to keep
        // re-agreeing on.
        let mut grid: Vec<(AdmitTarget, FrameInfo)> = if draft.frames.len() == draft.targets.len() {
            draft
                .targets
                .iter()
                .cloned()
                .zip(draft.frames.iter().cloned())
                .collect()
        } else {
            draft
                .targets
                .iter()
                .map(|t| (t.clone(), FrameInfo::new(t.session)))
                .collect()
        };
        // Deterministic grid order: target session id ascending (§4.1).
        grid.sort_by_key(|(target, _)| target.session);

        let origin = draft.origin;
        let mut stop: Option<(String, String)> = None;
        for (_target, frame) in &grid {
            if stop.is_some() {
                // A stop has already refused the event for every target:
                // this target's frames are not sent (§4.1).
                continue;
            }
            let mut framed = draft.clone();
            // §4.1: the view is produced once per event — the complete
            // fan-out set rides every dispatch; only the frame is
            // per-target (the payload's identity is the frame's).
            framed.frames = vec![frame.clone()];
            let record = self
                .dispatch(
                    HookId::AdmitPre,
                    &HookPayload::AdmitDraft(framed),
                    origin,
                    draft.targets.len(),
                )
                .await;
            if record.denied() {
                stop = record.winner.zip(record.reason);
            }
        }
        match stop {
            Some((plugin, reason)) => AdmitVerdict::Stop { plugin, reason },
            None => AdmitVerdict::Next,
        }
    }

    /// `admit/post` (§4.1 step 4): fires per admitted target after the
    /// row is durable — single-target by construction, `ack`-only mode.
    /// This is the concrete post-row method the bundle calls (K3); it is
    /// deliberately not an eighth port method.
    pub async fn admit_post(&self, receipt: &AdmitReceiptView) {
        let _ = self
            .dispatch(
                HookId::AdmitPost,
                &HookPayload::AdmitReceipt(receipt.clone()),
                receipt.origin,
                1,
            )
            .await;
    }

    /// `turn/request` (§4.2): observe-only, once per provider turn on the
    /// persisted primary body.
    pub async fn request(&self, meta: &RequestMeta) {
        let _ = self
            .dispatch(
                HookId::TurnRequest,
                &HookPayload::TurnRequest(meta.clone()),
                Origin::Api,
                1,
            )
            .await;
    }

    /// `turn/response` (§4.3): observe-only, once per settled turn.
    pub async fn response(&self, view: &ResponseView) {
        let _ = self
            .dispatch(
                HookId::TurnResponse,
                &HookPayload::TurnResponse(view.clone()),
                Origin::Api,
                1,
            )
            .await;
    }

    /// `tool/pre` (§4.4): `bail` mode — the first valid `deny` from a
    /// `veto:tool` holder, or the fail-closed class default of an
    /// unreachable one, denies the call. The declarative gate's decision
    /// stands otherwise.
    pub async fn tool_pre(&self, call: &ToolCallView) -> ToolVerdict {
        let record = self
            .dispatch(
                HookId::ToolPre,
                &HookPayload::ToolPre(call.clone()),
                Origin::Api,
                1,
            )
            .await;
        match (record.winner, record.reason) {
            (Some(plugin), Some(reason)) => ToolVerdict::Deny { plugin, reason },
            _ => ToolVerdict::Allow,
        }
    }

    /// `tool/post` (§4.4 step 5): observe-only after the `tool/result`
    /// append.
    pub async fn tool_post(&self, view: &ToolPostView) {
        let _ = self
            .dispatch(
                HookId::ToolPost,
                &HookPayload::ToolPost(view.clone()),
                Origin::Api,
                1,
            )
            .await;
    }

    /// `recover/decided` (§4.5): observe-only; a hook may not veto or
    /// alter recovery.
    pub async fn recover(&self, view: &RecoveryView) {
        let _ = self
            .dispatch(
                HookId::RecoverDecided,
                &HookPayload::Recovery(view.clone()),
                Origin::Api,
                1,
            )
            .await;
    }

    /// `settle/pre` (§4.6): the completion gate's decisioning. On a deny
    /// with budget remaining the marker row is admitted through the
    /// caller-injected [`SettleCtl`] (step 3); on budget exhaustion the
    /// veto is recorded but settlement proceeds (step 4); a failed marker
    /// append does **not** settle and consumes the budget anyway via the
    /// in-process counter (the three-step failure path).
    ///
    /// §3.2.8: `vetoes_used`/`vetoes_remaining` are **per plugin** —
    /// each subscriber's view is rebuilt with its own numbers and its
    /// own limit (§6.1's manifest `max_settle_vetoes` after worker
    /// `limits:` narrowing; `0` disables the gate for that plugin, so
    /// its deny is always an Override). Tier-0 subscribers have no
    /// manifest and must not invent one (K8): the tier-0 default of 3.
    pub async fn settle(&self, view: &SettleView, ctl: &dyn SettleCtl) -> SettleVerdict {
        // The per-plugin budget table, resolved before dispatch so the
        // payload builder is pure (§3.2.8's numbers and the §4.6
        // decision read the same row).
        let subscribers = self
            .inner
            .registry
            .snapshot(&HookId::SettlePre, view.frame.as_ref());
        let mut used_of: HashMap<&str, u64> = HashMap::with_capacity(subscribers.len());
        let mut limit_of: HashMap<&str, u64> = HashMap::with_capacity(subscribers.len());
        for subscriber in &subscribers {
            let plugin = subscriber.handler.plugin;
            let stream_used = ctl.veto_count(plugin);
            let failed_admits = self.failed_settle_admits(view, plugin);
            used_of.insert(plugin, stream_used.max(u64::from(failed_admits)));
            let limit = self
                .sidecar_runtime()
                .map_or(DEFAULT_MAX_SETTLE_VETOS, |rt| {
                    rt.settle_veto_limit(view.frame.as_ref(), plugin)
                });
            limit_of.insert(plugin, limit);
        }
        let record = self
            .dispatch_per(
                HookId::SettlePre,
                view.frame.as_ref(),
                Origin::Api,
                1,
                |subscriber| {
                    let plugin = subscriber.handler.plugin;
                    let used = used_of.get(plugin).copied().unwrap_or(0);
                    let limit = limit_of
                        .get(plugin)
                        .copied()
                        .unwrap_or(DEFAULT_MAX_SETTLE_VETOS);
                    let mut per_plugin = view.clone();
                    per_plugin.vetoes_used = used;
                    per_plugin.vetoes_remaining = limit.saturating_sub(used);
                    HookPayload::Settle(per_plugin)
                },
            )
            .await;
        let (plugin, reason) = match (record.winner, record.reason) {
            (Some(plugin), Some(reason)) => (plugin, reason),
            _ => return SettleVerdict::Allow,
        };

        // Budget (§4.6): per plugin, per session. The stream count is the
        // caller's (durable); the in-process counter covers append
        // failures the stream cannot see. The limit is the winner's own
        // (§6.1) — `0` disables its gate outright, so the check is the
        // same `used >= limit` either way.
        let used = used_of.get(plugin.as_str()).copied().unwrap_or(0);
        let limit = limit_of
            .get(plugin.as_str())
            .copied()
            .unwrap_or(DEFAULT_MAX_SETTLE_VETOS);
        if used >= limit {
            audit_override(&plugin, view, used, &reason);
            return SettleVerdict::Override {
                plugin,
                reason,
                vetoes_used: used,
            };
        }

        let marker = veto_marker(&plugin, "settle/pre", &reason);
        if let Err(error) = ctl.admit_synthetic(marker).await {
            self.note_failed_settle_admit(view, &plugin);
            let vetoes_used = used + 1;
            audit_veto_admit_failed(&plugin, view, vetoes_used, &error, &reason);
            return SettleVerdict::AdmitFailed {
                plugin,
                reason,
                error,
                vetoes_used,
            };
        }
        SettleVerdict::Deny { plugin, reason }
    }

    fn failed_settle_admits(&self, view: &SettleView, plugin: &str) -> u32 {
        let Some(frame) = view.frame.as_ref() else {
            return 0;
        };
        self.inner
            .settle_failed_admits
            .lock()
            .expect("kian-plugin: settle-attempts lock poisoned")
            .get(&(frame.session, plugin.to_string()))
            .copied()
            .unwrap_or(0)
    }

    fn note_failed_settle_admit(&self, view: &SettleView, plugin: &str) {
        let Some(frame) = view.frame.as_ref() else {
            return;
        };
        *self
            .inner
            .settle_failed_admits
            .lock()
            .expect("kian-plugin: settle-attempts lock poisoned")
            .entry((frame.session, plugin.to_string()))
            .or_insert(0) += 1;
    }
}

// §10.6 audit lines — `tracing` events on the existing sinks, target
// `kian::hook`, field names exactly per the catalog. Aggregate facts
// only: identity, hook id, counters and codes (§10.5).

fn audit_dispatch(
    hook: &HookId,
    plugin: &str,
    frame: Option<&FrameInfo>,
    dispatch: &str,
    outcome: &str,
    elapsed_ms: u64,
) {
    match frame {
        Some(f) => {
            tracing::info!(
                target: "kian::hook",
                hook = %hook,
                plugin = plugin,
                session = %f.session.0,
                worker = %f.worker,
                seq = f.seq,
                epoch = ?f.epoch,
                dispatch = dispatch,
                outcome = outcome,
                elapsed_ms = elapsed_ms,
                "hook-dispatch"
            );
        }
        None => {
            tracing::info!(
                target: "kian::hook",
                hook = %hook,
                plugin = plugin,
                dispatch = dispatch,
                outcome = outcome,
                elapsed_ms = elapsed_ms,
                "hook-dispatch"
            );
        }
    }
}

fn audit_veto(
    hook: &HookId,
    plugin: &str,
    frame: Option<&FrameInfo>,
    dispatch: &str,
    reason: &str,
    skipped: usize,
    targets: usize,
) {
    let session = frame.map(|f| f.session.0.to_string()).unwrap_or_default();
    tracing::info!(
        target: "kian::hook",
        hook = %hook,
        plugin = plugin,
        session = %session,
        dispatch = dispatch,
        reason = reason,
        skipped = skipped,
        // §10.6's `winner` carries its useful value — the winning
        // plugin's name (a constant `true` said nothing the `plugin`
        // field had not already said).
        winner = plugin,
        targets = targets,
        "hook-veto"
    );
}

fn audit_default(hook: &HookId, plugin: &str, cause: Option<super::failure::Cause>) {
    // §10.6 `hook-default` — `reason` is the class-default cause when one
    // exists; an abstain/lapse default carries none.
    match cause {
        Some(cause) => {
            tracing::info!(
                target: "kian::hook",
                hook = %hook,
                plugin = plugin,
                reason = cause.as_str(),
                "hook-default"
            );
        }
        None => {
            tracing::info!(
                target: "kian::hook",
                hook = %hook,
                plugin = plugin,
                "hook-default"
            );
        }
    }
}

fn audit_discard(hook: &HookId, plugin: &str, code: SpeechCode, grant: Option<Grant>) {
    let grant = grant.map(|g| g.as_str().to_string()).unwrap_or_default();
    tracing::info!(
        target: "kian::hook",
        hook = %hook,
        plugin = plugin,
        code = code.as_str(),
        grant = %grant,
        outcome = "discarded",
        "hook-discard"
    );
}

fn audit_override(plugin: &str, view: &SettleView, vetoes: u64, reason: &str) {
    let session = view
        .frame
        .as_ref()
        .map(|f| f.session.0.to_string())
        .unwrap_or_default();
    tracing::info!(
        target: "kian::hook",
        hook = "settle/pre",
        plugin = plugin,
        session = %session,
        vetoes = vetoes,
        reason = reason,
        "hook-override"
    );
}

fn audit_veto_admit_failed(
    plugin: &str,
    view: &SettleView,
    vetoes: u64,
    error: &str,
    reason: &str,
) {
    let session = view
        .frame
        .as_ref()
        .map(|f| f.session.0.to_string())
        .unwrap_or_default();
    tracing::info!(
        target: "kian::hook",
        hook = "settle/pre",
        plugin = plugin,
        session = %session,
        vetoes = vetoes,
        error = error,
        reason = reason,
        "hook-veto-admit-failed"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::FutureExt;

    use super::super::grants::GrantSet;
    use super::super::registry::{HandlerId, SessionScope};
    use super::*;

    fn bus() -> HookBus {
        HookBus::with_registry(Arc::new(Registry::new()))
    }

    /// Registers a `tool/pre` handler recording invocations into `log`
    /// and returning `reply`; the returned [`Disposer`] must be kept by
    /// the caller (dropping it unregisters — §5.3).
    fn tool_pre_handler(
        bus: &HookBus,
        plugin: &'static str,
        grants: &[&str],
        order: i32,
        reply: HookReply,
        log: Arc<Mutex<Vec<String>>>,
    ) -> crate::ctx::Disposer {
        let handler = HandlerId {
            plugin,
            hook: HookId::ToolPre,
            grants: GrantSet::from_names(grants).unwrap(),
            order,
        };
        bus.registry().register(
            handler,
            Arc::new(move |_payload: &HookPayload| {
                let log = Arc::clone(&log);
                let reply = reply.clone();
                async move {
                    log.lock().unwrap().push(plugin.to_string());
                    reply
                }
                .boxed()
            }),
        )
    }

    fn tool_pre_view(frame: Option<FrameInfo>) -> ToolCallView {
        ToolCallView {
            call_id: "call_1".to_string(),
            name: "exec".to_string(),
            iteration: 1,
            arguments: serde_json::json!({"cmd": ["ls"]}),
            arguments_bytes: 12,
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
        }
    }

    fn frame() -> Option<FrameInfo> {
        Some(FrameInfo::new(
            serde_json::from_str("\"0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b\"").unwrap(),
        ))
    }

    #[tokio::test]
    async fn out_of_grant_reply_is_discarded_as_if_absent() {
        // §5.2: the only subscriber, `tool/pre`, replying `deny` with
        // `observe:tool` only — the call proceeds on the declarative
        // gate's decision, and the defection is recorded, not resolved to
        // the fail-closed class default (K6).
        let bus = bus();
        let log = Arc::new(Mutex::new(Vec::new()));
        let _keep = tool_pre_handler(
            &bus,
            "audit-only",
            &["observe:tool"],
            0,
            HookReply::BailDeny {
                reason: "I deny anyway".to_string(),
            },
            Arc::clone(&log),
        );

        let verdict = bus.tool_pre(&tool_pre_view(frame())).await;
        assert_eq!(
            verdict,
            ToolVerdict::Allow,
            "an out-of-grant deny is discarded — never a denial, never the class default"
        );
        assert_eq!(*log.lock().unwrap(), vec!["audit-only".to_string()]);

        let record = bus
            .dispatch(
                HookId::ToolPre,
                &HookPayload::ToolPre(tool_pre_view(frame())),
                Origin::Api,
                1,
            )
            .await;
        assert_eq!(record.discards.len(), 1);
        assert_eq!(record.discards[0].plugin, "audit-only");
        assert_eq!(record.discards[0].code, SpeechCode::Unauthorized);
        assert_eq!(record.discards[0].grant, Some(Grant::VetoTool));
    }

    #[tokio::test]
    async fn wrong_mode_reply_is_discarded() {
        // §7.4/§5.2: an `ack` at a `bail` hook is `err:malformed`-shaped
        // speech — discarded, as-if-absent.
        let bus = bus();
        let log = Arc::new(Mutex::new(Vec::new()));
        let _keep = tool_pre_handler(
            &bus,
            "confused",
            &["observe:tool", "veto:tool"],
            0,
            HookReply::Ack { note: None },
            Arc::clone(&log),
        );

        let verdict = bus.tool_pre(&tool_pre_view(frame())).await;
        assert_eq!(verdict, ToolVerdict::Allow);
        assert_eq!(*log.lock().unwrap(), vec!["confused".to_string()]);
        log.lock().unwrap().clear();

        let record = bus
            .dispatch(
                HookId::ToolPre,
                &HookPayload::ToolPre(tool_pre_view(frame())),
                Origin::Api,
                1,
            )
            .await;
        assert_eq!(record.discards.len(), 1);
        assert_eq!(record.discards[0].code, SpeechCode::Malformed);
        assert_eq!(*log.lock().unwrap(), vec!["confused".to_string()]);
    }

    #[tokio::test]
    async fn first_deny_wins_and_skips_rest() {
        // §8.3.3: the first valid deny wins, remaining subscribers are
        // skipped for that dispatch, and the record carries the count.
        let bus = bus();
        let log = Arc::new(Mutex::new(Vec::new()));
        let _keep = tool_pre_handler(
            &bus,
            "gate",
            &["observe:tool", "veto:tool"],
            0,
            HookReply::BailDeny {
                reason: "blocked".to_string(),
            },
            Arc::clone(&log),
        );
        let _keep = tool_pre_handler(
            &bus,
            "observer",
            &["observe:tool"],
            1,
            HookReply::BailNone,
            Arc::clone(&log),
        );

        let verdict = bus.tool_pre(&tool_pre_view(frame())).await;
        assert_eq!(
            verdict,
            ToolVerdict::Deny {
                plugin: "gate".to_string(),
                reason: "blocked".to_string()
            }
        );
        assert_eq!(
            *log.lock().unwrap(),
            vec!["gate".to_string()],
            "subscribers after the first deny are not consulted"
        );

        let record = bus
            .dispatch(
                HookId::ToolPre,
                &HookPayload::ToolPre(tool_pre_view(frame())),
                Origin::Api,
                1,
            )
            .await;
        assert_eq!(
            record.skipped, 1,
            "the skipped count names the not-consulted subscriber"
        );
        assert_eq!(record.winner.as_deref(), Some("gate"));
    }

    #[tokio::test]
    async fn handler_order_tiebreak_is_declaration_index() {
        // §8.3.1: tier-0 handlers run in ascending `order`, ties broken
        // by declaration index — not the sidecar `[plugin].order`/name
        // rule (that is C3's).
        let bus = bus();
        let log = Arc::new(Mutex::new(Vec::new()));
        // Declared first, same order: runs first.
        let _keep = tool_pre_handler(
            &bus,
            "first-declared",
            &["observe:tool"],
            0,
            HookReply::BailNone,
            Arc::clone(&log),
        );
        let _keep = tool_pre_handler(
            &bus,
            "second-declared",
            &["observe:tool"],
            0,
            HookReply::BailNone,
            Arc::clone(&log),
        );
        // Declared last but ordered lower: runs before both.
        let _keep = tool_pre_handler(
            &bus,
            "ordered-early",
            &["observe:tool"],
            -1,
            HookReply::BailNone,
            Arc::clone(&log),
        );

        bus.tool_pre(&tool_pre_view(frame())).await;
        assert_eq!(
            *log.lock().unwrap(),
            vec![
                "ordered-early".to_string(),
                "first-declared".to_string(),
                "second-declared".to_string(),
            ],
            "order ascends; ties break by declaration index"
        );
    }

    #[tokio::test]
    async fn unreachable_veto_denies_at_any_order_position() {
        // §11.1 consequence 1: an abstaining subscriber ordered first does
        // not mask an unreachable veto holder ordered last — the outcome
        // must not depend on position.
        let bus = bus();
        let log = Arc::new(Mutex::new(Vec::new()));
        let _keep = tool_pre_handler(
            &bus,
            "abstainer",
            &["observe:tool", "veto:tool"],
            0,
            HookReply::BailNone,
            Arc::clone(&log),
        );
        let _keep = tool_pre_handler(
            &bus,
            "wedged-gate",
            &["observe:tool", "veto:tool"],
            1,
            HookReply::Err {
                code: super::super::registry::ReplyErrCode::Deadline,
                message: "over deadline".to_string(),
            },
            Arc::clone(&log),
        );

        let verdict = bus.tool_pre(&tool_pre_view(frame())).await;
        assert_eq!(
            verdict,
            ToolVerdict::Deny {
                plugin: "wedged-gate".to_string(),
                reason: "hook unavailable (deadline)".to_string(),
            },
            "the fail-closed default's reason is the honest-error tail"
        );
        assert_eq!(
            *log.lock().unwrap(),
            vec!["abstainer".to_string(), "wedged-gate".to_string()]
        );
    }

    #[tokio::test]
    async fn scoped_handler_joins_only_its_session_dispatch() {
        // §5.3 through the bus: a session-scoped registration participates
        // only in dispatches targeting that session.
        let bus = bus();
        let hits = Arc::new(Mutex::new(0u32));
        let scoped: SessionId =
            serde_json::from_str("\"0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b\"").unwrap();
        let other: SessionId =
            serde_json::from_str("\"1f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b\"").unwrap();

        let counter = Arc::clone(&hits);
        let _keep = bus.registry().register_scoped(
            SessionScope::Session(scoped),
            HandlerId {
                plugin: "scoped-gate",
                hook: HookId::ToolPre,
                grants: GrantSet::from_names(&["observe:tool", "veto:tool"]).unwrap(),
                order: 0,
            },
            Arc::new(move |_payload: &HookPayload| {
                let counter = Arc::clone(&counter);
                async move {
                    *counter.lock().unwrap() += 1;
                    HookReply::BailNone
                }
                .boxed()
            }),
        );

        bus.tool_pre(&tool_pre_view(Some(FrameInfo::new(other))))
            .await;
        assert_eq!(*hits.lock().unwrap(), 0, "not this session's handler");
        bus.tool_pre(&tool_pre_view(Some(FrameInfo::new(scoped))))
            .await;
        assert_eq!(*hits.lock().unwrap(), 1);
    }

    // ------------------------------------------------------------------
    // §8.2/§9.3 — the sidecar runtime's dispatch-time behaviour
    // ------------------------------------------------------------------

    mod sidecar_dispatch {
        use super::*;

        fn sidecar_manifest(name: &str) -> super::super::super::sidecar::manifest::Manifest {
            let text = format!(
                "[plugin]\nname = \"{name}\"\nversion = \"0.1.0\"\nabi = 1\n\
                 [process]\nargv = [\"/bin/cat\"]\nsha256 = \"{}\"\n\
                 [grants]\nhooks = [\"tool/pre\"]\ncapabilities = [\"observe:tool\"]\n\
                 [limits]\ndeadline_ms = 500\n",
                "a".repeat(64)
            );
            let ctx = super::super::super::sidecar::ValidationCtx {
                instance_key: name,
                ..Default::default()
            };
            super::super::super::sidecar::parse_manifest(&text, &ctx)
                .expect("fixture manifest parses")
        }

        fn sidecar_cfg(server: &[&str]) -> resolve::SidecarConfig {
            serde_json::from_value(serde_json::json!({
                "plugins": {"gate": {"manifest": "/etc/kian/plugins/gate.toml"}},
                "scopes": {"server": server}
            }))
            .unwrap()
        }

        fn installed_sidecars(bus: &HookBus, server: &[&str]) {
            let mut manifests = std::collections::BTreeMap::new();
            manifests.insert("gate".to_string(), sidecar_manifest("gate"));
            bus.install_sidecars(sidecar_cfg(server), manifests)
                .expect("boot-once install");
        }

        fn worker_frame(worker: &str, session: &str) -> Option<FrameInfo> {
            let session: SessionId =
                serde_json::from_str(&format!("\"{session}\"")).expect("valid uuid literal");
            Some(FrameInfo {
                session,
                worker: worker.to_string(),
                seq: 0,
                epoch: Some(1),
            })
        }

        #[tokio::test]
        async fn mismatch_quarantines_sidecars_tier0_keeps_working() {
            // §9.2 step 4 + §9.3: the session's sidecar hooks take the
            // grant-keyed class default WITHOUT consulting the handler
            // (a veto:tool holder fail-closes with cause `quarantine`);
            // tier-0 handlers keep working (step 6).
            let bus = bus();
            installed_sidecars(&bus, &["gate"]);
            let log = Arc::new(Mutex::new(Vec::new()));
            // The tier-0 watcher sits BEFORE the gate in dispatch order:
            // §8.3's first-deny-wins still skips subscribers after a
            // denial, so this placement is what shows tier-0 is not
            // disabled by the quarantine (§9.2 step 6).
            let _tier0 = tool_pre_handler(
                &bus,
                "watcher",
                &["observe:tool"],
                0,
                HookReply::Ack { note: None },
                Arc::clone(&log),
            );
            let _gate = tool_pre_handler(
                &bus,
                "gate",
                &["observe:tool", "veto:tool"],
                1,
                HookReply::BailNone,
                Arc::clone(&log),
            );

            let frame = worker_frame("", "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b");
            // Open the session (§9.2 step 1): both subscribers consulted,
            // tier-0 first (order 0 before the gate's 1).
            bus.session_begin(frame.clone().unwrap());
            let verdict = bus.tool_pre(&tool_pre_view(frame.clone())).await;
            assert_eq!(verdict, ToolVerdict::Allow);
            assert_eq!(*log.lock().unwrap(), vec!["watcher", "gate"]);

            // The purity break §9.2 step 4 names: resolution stops being
            // a function of the boot snapshot (the reload path v1 does
            // not have; the in-crate seam simulates exactly it).
            bus.inner
                .sidecars
                .read()
                .expect("sidecar runtime lock poisoned")
                .as_ref()
                .expect("runtime installed")
                .reconfigure(sidecar_cfg(&[]));
            bus.session_begin(frame.clone().unwrap());
            assert!(bus.session_quarantined(&frame.as_ref().unwrap().session));

            let verdict = bus.tool_pre(&tool_pre_view(frame.clone())).await;
            assert_eq!(
                verdict,
                ToolVerdict::Deny {
                    plugin: "gate".to_string(),
                    reason: "hook unavailable (quarantine)".to_string(),
                },
                "the quarantined veto:tool holder fails closed without being consulted"
            );
            assert_eq!(
                *log.lock().unwrap(),
                vec!["watcher", "gate", "watcher"],
                "the sidecar handler was NOT invoked; the tier-0 handler was"
            );
        }

        #[tokio::test]
        async fn per_worker_use_filters_the_sidecar_from_dispatch() {
            // §8.2: a name the worker's `use:` omits is simply absent —
            // no consultation, no error, one audit line at resolution.
            let bus = bus();
            let mut cfg = sidecar_cfg(&["gate"]);
            cfg.scopes.worker.insert(
                "node-zulu".to_string(),
                resolve::WorkerScope {
                    use_: Some(vec![]),
                    limits: Default::default(),
                },
            );
            let mut manifests = std::collections::BTreeMap::new();
            manifests.insert("gate".to_string(), sidecar_manifest("gate"));
            bus.install_sidecars(cfg, manifests).expect("install");

            let log = Arc::new(Mutex::new(Vec::new()));
            let _gate = tool_pre_handler(
                &bus,
                "gate",
                &["observe:tool"],
                0,
                HookReply::BailNone,
                Arc::clone(&log),
            );

            let zulu = worker_frame("node-zulu", "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b");
            bus.session_begin(zulu.clone().unwrap());
            let verdict = bus.tool_pre(&tool_pre_view(zulu.clone())).await;
            assert_eq!(verdict, ToolVerdict::Allow);
            assert_eq!(
                *log.lock().unwrap(),
                Vec::<String>::new(),
                "not in zulu's set"
            );

            let yankee = worker_frame("", "99999999-8888-7777-6666-555555555555");
            bus.session_begin(yankee.clone().unwrap());
            let verdict = bus.tool_pre(&tool_pre_view(yankee.clone())).await;
            assert_eq!(verdict, ToolVerdict::Allow);
            assert_eq!(
                *log.lock().unwrap(),
                vec!["gate"],
                "in the server scope's set"
            );
        }

        #[tokio::test]
        async fn no_runtime_means_no_sidecar_behaviour() {
            // §13.3: with the `sidecars:` section absent, every guard
            // surface is a no-op and dispatch is the pre-feature
            // behaviour (the §9 accessor surface reads as "nothing").
            let bus = bus();
            let session: SessionId =
                serde_json::from_str("\"0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b\"").unwrap();
            bus.session_begin(FrameInfo::new(session));
            assert_eq!(bus.session_set_digest(&session), None);
            assert!(!bus.session_quarantined(&session));

            let log = Arc::new(Mutex::new(Vec::new()));
            let _tier0 = tool_pre_handler(
                &bus,
                "watcher",
                &["observe:tool"],
                0,
                HookReply::Ack { note: None },
                Arc::clone(&log),
            );
            let verdict = bus
                .tool_pre(&tool_pre_view(worker_frame(
                    "",
                    "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b",
                )))
                .await;
            assert_eq!(verdict, ToolVerdict::Allow);
            assert_eq!(*log.lock().unwrap(), vec!["watcher"]);
        }

        #[tokio::test]
        async fn second_install_is_refused() {
            // §13.1: no live reload — the boot-once write is the only
            // one; a second install is the purity break the guard
            // exists to catch, so it refuses rather than comply.
            let bus = bus();
            installed_sidecars(&bus, &["gate"]);
            assert!(
                bus.install_sidecars(sidecar_cfg(&["gate"]), Default::default())
                    .is_err()
            );
        }
    }

    // ------------------------------------------------------------------
    // §4.1/§11.1 — port-level admission fail-closed / observe-only (M4-g)
    // ------------------------------------------------------------------

    fn admit_draft(origin: Origin, targets: &[&str]) -> AdmitDraft {
        let targets: Vec<AdmitTarget> = targets
            .iter()
            .map(|&s| {
                let session: SessionId =
                    serde_json::from_str(&format!("\"{s}\"")).expect("valid uuid literal");
                AdmitTarget {
                    session,
                    router: false,
                }
            })
            .collect();
        let frames = targets.iter().map(|t| FrameInfo::new(t.session)).collect();
        AdmitDraft {
            origin,
            targets,
            input: super::super::views::AdmitInput {
                role: "user".to_string(),
                synthetic: false,
                content_preview: "go".to_string(),
                content_bytes: 2,
                content_sha256: "ab".to_string(),
                content_truncated: false,
                inline_view: false,
            },
            channel: None,
            admission: super::super::views::AdmissionInfo {
                policy: "none".to_string(),
                spilled: false,
            },
            frames,
        }
    }

    fn admit_handler(
        bus: &HookBus,
        plugin: &'static str,
        grants: &[&str],
        reply: HookReply,
        log: Arc<Mutex<Vec<String>>>,
    ) -> crate::ctx::Disposer {
        let handler = HandlerId {
            plugin,
            hook: HookId::AdmitPre,
            grants: GrantSet::from_names(grants).unwrap(),
            order: 0,
        };
        bus.registry().register(
            handler,
            Arc::new(move |_payload: &HookPayload| {
                let log = Arc::clone(&log);
                let reply = reply.clone();
                async move {
                    log.lock().unwrap().push(plugin.to_string());
                    reply
                }
                .boxed()
            }),
        )
    }

    /// §11.1 row 2 at the PORT level: an unreachable `veto:admit`
    /// holder's grant-keyed default (`RefuseAdmission`) refuses the
    /// event for **every** target at channel origin — the first
    /// target's stop short-circuits the grid, no later frame is sent.
    #[tokio::test]
    async fn unreachable_veto_admit_refuses_all_targets_at_channel_origin() {
        let bus = bus();
        let log = Arc::new(Mutex::new(Vec::new()));
        let _keep = admit_handler(
            &bus,
            "border-guard",
            &["observe:admit", "veto:admit"],
            HookReply::Err {
                code: super::super::registry::ReplyErrCode::Deadline,
                message: "wedged".to_string(),
            },
            Arc::clone(&log),
        );

        let verdict = bus
            .admit(&admit_draft(
                Origin::Channel,
                &[
                    "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b",
                    "1f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b",
                ],
            ))
            .await;
        assert_eq!(
            verdict,
            AdmitVerdict::Stop {
                plugin: "border-guard".to_string(),
                reason: "hook unavailable (deadline)".to_string(),
            },
            "the fail-closed class default refuses the whole grid"
        );
        assert_eq!(
            *log.lock().unwrap(),
            vec!["border-guard".to_string()],
            "the wedged holder was consulted exactly once — the second \
             target's frame was never sent after the stop"
        );
    }

    /// §3.4.3 at the PORT level: API-origin admission is observe-only —
    /// the same unreachable holder never refuses there (its default at
    /// API origin is the observe-only lapse, and even an explicit deny
    /// reply is discarded as out-of-origin speech).
    #[tokio::test]
    async fn api_origin_admission_never_refuses() {
        let bus = bus();
        let log = Arc::new(Mutex::new(Vec::new()));
        let _keep = admit_handler(
            &bus,
            "border-guard",
            &["observe:admit", "veto:admit"],
            HookReply::BailDeny {
                reason: "I deny anyway".to_string(),
            },
            Arc::clone(&log),
        );

        let verdict = bus
            .admit(&admit_draft(
                Origin::Api,
                &["0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b"],
            ))
            .await;
        assert_eq!(
            verdict,
            AdmitVerdict::Next,
            "an API-origin deny is out-of-origin speech — discarded, never a stop"
        );
        assert_eq!(*log.lock().unwrap(), vec!["border-guard".to_string()]);
    }

    // ------------------------------------------------------------------
    // §4.6/§6.1/§3.2.8 — the settle budget (M1/M2)
    // ------------------------------------------------------------------

    /// A caller-supplied [`SettleCtl`] with settable per-plugin stream
    /// counts and a recording marker admit.
    struct TestCtl {
        counts: std::collections::HashMap<String, u64>,
        admits: Arc<Mutex<Vec<String>>>,
    }

    impl SettleCtl for TestCtl {
        fn admit_synthetic(
            &self,
            content: String,
        ) -> futures::future::BoxFuture<'static, Result<(), String>> {
            self.admits.lock().unwrap().push(content);
            Box::pin(async { Ok(()) })
        }
        fn veto_count(&self, plugin: &str) -> u64 {
            self.counts.get(plugin).copied().unwrap_or(0)
        }
    }

    fn settle_view(frame: Option<FrameInfo>) -> SettleView {
        SettleView {
            reason: "completed".to_string(),
            turns: 1,
            tool_calls: 0,
            evidence: "none".to_string(),
            vetoes_used: 0,
            vetoes_remaining: 0,
            episodes: 0,
            frame,
        }
    }

    /// A settle subscriber that records the §3.2.8 budget fields of the
    /// view it received, then answers `reply`.
    fn settle_handler(
        bus: &HookBus,
        plugin: &'static str,
        reply: HookReply,
        seen: Arc<Mutex<Vec<(String, u64, u64)>>>,
    ) -> crate::ctx::Disposer {
        let handler = HandlerId {
            plugin,
            hook: HookId::SettlePre,
            grants: GrantSet::from_names(&["observe:settle", "veto:settle"]).unwrap(),
            order: 0,
        };
        bus.registry().register(
            handler,
            Arc::new(move |payload: &HookPayload| {
                let seen = Arc::clone(&seen);
                let reply = reply.clone();
                async move {
                    if let HookPayload::Settle(view) = payload {
                        seen.lock().unwrap().push((
                            plugin.to_string(),
                            view.vetoes_used,
                            view.vetoes_remaining,
                        ));
                    }
                    reply
                }
                .boxed()
            }),
        )
    }

    fn sidecar_cfg_named(name: &str) -> resolve::SidecarConfig {
        serde_json::from_value(serde_json::json!({
            "plugins": {name: {"manifest": "/etc/kian/plugins/x.toml"}},
            "scopes": {"server": [name]}
        }))
        .unwrap()
    }

    fn sidecar_manifest_with_veto_limit(
        name: &str,
        max_settle_vetoes: u64,
    ) -> super::super::sidecar::manifest::Manifest {
        let text = format!(
            "[plugin]\nname = \"{name}\"\nversion = \"0.1.0\"\nabi = 1\n\
             [process]\nargv = [\"/bin/cat\"]\nsha256 = \"{}\"\n\
             [grants]\nhooks = [\"settle/pre\"]\ncapabilities = [\"observe:settle\", \"veto:settle\"]\n\
             [limits]\ndeadline_ms = 500\nmax_settle_vetoes = {max_settle_vetoes}\n",
            "a".repeat(64)
        );
        let ctx = super::super::sidecar::ValidationCtx {
            instance_key: name,
            ..Default::default()
        };
        super::super::sidecar::parse_manifest(&text, &ctx).expect("fixture manifest parses")
    }

    /// §6.1 (M1): `max_settle_vetoes = 0` **disables** the gate — the
    /// sidecar's own manifest limit is applied at the settle decision,
    /// so its deny is an Override before any marker is admitted, while
    /// a tier-0 subscriber with no manifest keeps the default budget
    /// of 3 and still denies.
    #[tokio::test]
    async fn manifest_limit_zero_disables_the_gate_for_that_plugin() {
        let bus = bus();
        let mut manifests = std::collections::BTreeMap::new();
        manifests.insert(
            "gated".to_string(),
            sidecar_manifest_with_veto_limit("gated", 0),
        );
        let cfg = sidecar_cfg_named("gated");
        bus.install_sidecars(cfg, manifests).expect("install");

        let seen: Arc<Mutex<Vec<(String, u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let _keep = settle_handler(
            &bus,
            "gated",
            HookReply::BailDeny {
                reason: "not done".to_string(),
            },
            Arc::clone(&seen),
        );
        let admits = Arc::new(Mutex::new(Vec::new()));
        let ctl = TestCtl {
            counts: std::collections::HashMap::new(),
            admits: Arc::clone(&admits),
        };

        let verdict = bus.settle(&settle_view(frame()), &ctl).await;
        assert_eq!(
            verdict,
            SettleVerdict::Override {
                plugin: "gated".to_string(),
                reason: "not done".to_string(),
                vetoes_used: 0,
            },
            "§6.1: 0 disables the gate outright — the deny is recorded, settlement proceeds"
        );
        assert!(
            admits.lock().unwrap().is_empty(),
            "a disabled gate admits no marker"
        );
    }

    /// §6.1 (M1): a manifest limit WIDER than the tier-0 default is
    /// honored, not silently capped at 3.
    #[tokio::test]
    async fn manifest_limit_wider_than_default_is_honored() {
        let bus = bus();
        let mut manifests = std::collections::BTreeMap::new();
        manifests.insert(
            "gated".to_string(),
            sidecar_manifest_with_veto_limit("gated", 5),
        );
        let cfg = sidecar_cfg_named("gated");
        bus.install_sidecars(cfg, manifests).expect("install");

        let seen: Arc<Mutex<Vec<(String, u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let _keep = settle_handler(
            &bus,
            "gated",
            HookReply::BailDeny {
                reason: "not done".to_string(),
            },
            Arc::clone(&seen),
        );
        let admits = Arc::new(Mutex::new(Vec::new()));
        let mut counts = std::collections::HashMap::new();
        counts.insert("gated".to_string(), 3);
        let ctl = TestCtl {
            counts,
            admits: Arc::clone(&admits),
        };

        // 3 of 5 spent: the deny still lands (a marker is admitted) —
        // the old constant would have overridden here.
        let verdict = bus.settle(&settle_view(frame()), &ctl).await;
        assert!(matches!(verdict, SettleVerdict::Deny { .. }), "{verdict:?}");
        assert_eq!(admits.lock().unwrap().len(), 1);

        // 5 of 5 spent: exhausted — the Override.
        let mut counts = std::collections::HashMap::new();
        counts.insert("gated".to_string(), 5);
        let ctl = TestCtl {
            counts,
            admits: Arc::clone(&admits),
        };
        let verdict = bus.settle(&settle_view(frame()), &ctl).await;
        assert!(
            matches!(verdict, SettleVerdict::Override { vetoes_used: 5, .. }),
            "exactly the manifest's 5, not the tier-0 3: {verdict:?}"
        );
    }

    /// §3.2.8 (M2): `vetoes_used`/`vetoes_remaining` are per plugin —
    /// each settle subscriber's view carries its own numbers, not one
    /// cloned view for all. Both subscribers abstain so both are
    /// dispatched and both views are observable.
    #[tokio::test]
    async fn settle_budget_fields_are_per_plugin() {
        let bus = bus();
        let seen: Arc<Mutex<Vec<(String, u64, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let _keep_a = settle_handler(&bus, "gate-a", HookReply::BailNone, Arc::clone(&seen));
        let _keep_b = settle_handler(&bus, "gate-b", HookReply::BailNone, Arc::clone(&seen));
        let mut counts = std::collections::HashMap::new();
        counts.insert("gate-a".to_string(), 1);
        counts.insert("gate-b".to_string(), 0);
        let ctl = TestCtl {
            counts,
            admits: Arc::new(Mutex::new(Vec::new())),
        };

        let _ = bus.settle(&settle_view(frame()), &ctl).await;
        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![("gate-a".to_string(), 1, 2), ("gate-b".to_string(), 0, 3),],
            "gate-a reads its own 1-of-3; gate-b reads its own 0-of-3 — \
             no shared constants"
        );
    }

    /// §4.6's trust boundary (M4-f, the predicate half): the budget
    /// predicate is `synthetic` + prefix and **never** `role` — a
    /// steer-capable caller can mint a marker-shaped row, and it
    /// counts. Forgeability is the documented behaviour.
    #[test]
    fn steer_minted_marker_matches_the_budget_predicate_regardless_of_role() {
        let content = veto_marker("quality-gate", "settle/pre", "not done yet");
        assert!(is_veto_marker("quality-gate", true, &content));
        // The neighboring-plugin confusion the trailing space forbids.
        assert!(!is_veto_marker("quality", true, &content));
        // A non-synthetic row never counts, whatever its content.
        assert!(!is_veto_marker("quality-gate", false, &content));
    }
}
