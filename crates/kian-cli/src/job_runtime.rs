//! Assembly adapters for the job conductor (design/JOB-MODEL.md §5.3,
//! §13.1, §13.3).
//!
//! These carry no semantics of their own: they translate the bundle's
//! ports onto the drain, the mounted inbox and the resolved job catalogue.
//! The conductor's decisions (readiness, placement, retries, verdicts) live
//! in `kian_superagent::job::conduct` and stay there.
//!
//! - `DrainRunPort` — the conductor's write path: its own drain's append
//!   and cached read, the interrupt flag, and the rows it promotes. Nothing
//!   here can reach another session's drain (constraint 9, J5).
//! - `JobAlertPort` — §13.3's alert admission. The catalogue is resolved
//!   after the driver is assembled (the plugin runs before the engine), so
//!   it is installed into this port at boot by `Server::from_config`; an
//!   unmounted catalogue yields no alerts, never a fabricated one.
//! - `EmptyDirectory` — a deployment with no worker catalogue. Placement
//!   then refuses with `no_capable_worker`, which is the honest answer: a
//!   capability nobody declared is not a capability the runtime may assume.

use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::{BoxStream, StreamExt};
use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_core::seams::LoopOutcome;
use kian_orchestrator::DrainCtx;
use kian_superagent::job::alert::AlertSpec;
use kian_superagent::job::conduct::{AlertPort, DeliverPort, RunPort, RunPortFor};
use kian_superagent::{InboxWriter, MemberView, WorkerDirectoryError, WorkerDirectoryPort};
use serde_json::Value;

/// The server clock, adapted to the conductor's own clock trait.
///
/// The trait is declared by the bundle (JOB-MODEL §5.4) so the conductor can
/// read a monotonic millisecond clock without naming an orchestrator type; the
/// server's real clock is this adapter's input.
pub struct JobClock(Arc<dyn kian_orchestrator::Clock>);

impl JobClock {
    pub fn new(clock: Arc<dyn kian_orchestrator::Clock>) -> Self {
        Self(clock)
    }
}

impl kian_superagent::job::conduct::Clock for JobClock {
    fn now_ms(&self) -> u64 {
        self.0.now_ms()
    }
}

/// The conductor's `RunPort` over one drain hold (JOB-MODEL §5.3).
///
/// Writes carry the hold's epoch, so a stale port is fenced out by the store
/// exactly like every other drain write (constraint 5).
struct DrainRunPort(DrainCtx);

impl RunPort for DrainRunPort {
    fn session(&self) -> SessionId {
        self.0.session()
    }

    fn emit(&self, kind: EventKind, bytes: Value) -> BoxFuture<'static, Result<(), String>> {
        let ctx = self.0.clone();
        Box::pin(async move {
            ctx.append(kind, bytes)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    }

    fn read_stream(&self) -> BoxFuture<'static, Result<Vec<Event>, String>> {
        let ctx = self.0.clone();
        Box::pin(async move { ctx.events_vec().await.map_err(|error| error.to_string()) })
    }

    fn interrupt_requested(&self) -> bool {
        self.0.interrupt_requested()
    }

    fn take_inputs(&self) -> BoxFuture<'static, Result<Vec<Value>, String>> {
        let ctx = self.0.clone();
        Box::pin(async move {
            ctx.take_eligible_inputs()
                .await
                .map(|rows| rows.into_iter().map(|row| row.payload).collect())
                .map_err(|error| error.to_string())
        })
    }
}

/// The per-hold `RunPort` factory (the `PortFor` shape of the other
/// decorators).
#[must_use]
pub fn run_port_for_ctx() -> RunPortFor<DrainCtx> {
    Arc::new(|ctx: &DrainCtx| Arc::new(DrainRunPort(ctx.clone())) as Arc<dyn RunPort>)
}

/// §13.3's alert port: the declared alerts of a job, admitted as ordinary
/// input rows through the mounted inbox (durable row first, then the
/// advisory wake — constraint 6).
pub struct JobAlertPort {
    inbox: Arc<dyn InboxWriter>,
    catalog: std::sync::OnceLock<Arc<crate::job_config::JobCatalog>>,
}

impl JobAlertPort {
    /// A port with no catalogue yet — the plugin assembles before the
    /// config-resolved catalogue exists.
    #[must_use]
    pub fn new(inbox: Arc<dyn InboxWriter>) -> Arc<Self> {
        Arc::new(Self {
            inbox,
            catalog: std::sync::OnceLock::new(),
        })
    }

    /// Installs the boot-resolved catalogue. Returns `false` when one was
    /// already installed (the first boot resolution wins; a reload never
    /// silently re-points a mounted port).
    pub fn install(&self, catalog: Arc<crate::job_config::JobCatalog>) -> bool {
        self.catalog.set(catalog).is_ok()
    }

    /// Whether a catalogue is mounted (an unmounted port admits nothing —
    /// the run still ends and its failure is still recorded).
    #[must_use]
    pub fn mounted(&self) -> bool {
        self.catalog.get().is_some()
    }
}

impl AlertPort for JobAlertPort {
    fn alerts_for(&self, job_id: &str) -> Vec<AlertSpec> {
        let Some(catalog) = self.catalog.get() else {
            return Vec::new();
        };
        catalog
            .alerts_for(job_id)
            .iter()
            .map(|alert| AlertSpec {
                id: alert.id.clone(),
                channel: alert.channel.clone(),
                target: alert.target.clone(),
            })
            .collect()
    }

    fn raise(
        &self,
        session: SessionId,
        payload: Value,
        key: String,
    ) -> BoxFuture<'static, Result<(), String>> {
        let inbox = Arc::clone(&self.inbox);
        Box::pin(async move {
            let marker = payload
                .get(kian_superagent::job::status::JOB_KEY)
                .ok_or_else(|| "alert input carries no job marker".to_string())?;
            let marker = kian_superagent::job::status::parse_marker(marker)
                .map_err(|error| format!("invalid alert marker: {error}"))?;
            let failed_run = marker
                .slot
                .strip_prefix("alert:")
                .filter(|suffix| !suffix.is_empty())
                .ok_or_else(|| "alert slot does not name a failed run".to_string())?;
            if marker.trigger != kian_superagent::job::alert::TRIGGER_ALERT
                || !(marker.job_id.starts_with("alert:") || marker.job_id.starts_with("alert."))
                || marker.run_id
                    != kian_superagent::job::plan::alert_run_id(&marker.job_id, failed_run)
                || key != format!("kian:alert:{failed_run}")
            {
                return Err("alert run identity does not match its failed run".to_string());
            }
            let violations = kian_superagent::job::plan::validate_plan(&marker.plan);
            if !violations.is_empty() {
                return Err(format!(
                    "invalid alert plan: {}",
                    violations
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("; ")
                ));
            }
            inbox
                .admit(session, payload, Some(key))
                .await
                .map_err(|error| error.to_string())?;
            let _ = inbox.wake(session);
            Ok(())
        })
    }
}

/// A deployment with no worker catalogue: every placement refuses with
/// `no_capable_worker` — never a guessed worker, never a silent local
/// fallback (JOB-MODEL §9.2).
pub struct EmptyDirectory;

impl WorkerDirectoryPort for EmptyDirectory {
    fn known_ids(&self) -> BoxFuture<'static, Result<Vec<String>, WorkerDirectoryError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn members(&self) -> BoxFuture<'static, Result<Vec<MemberView>, WorkerDirectoryError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn member(
        &self,
        _id: &str,
    ) -> BoxFuture<'static, Result<Option<MemberView>, WorkerDirectoryError>> {
        Box::pin(async { Ok(None) })
    }
}

/// The stream a run gets when the conductor is not mounted in this process
/// (a bundle without job deps, or a test server).
///
/// It records the honest failure and then settles the run: a run whose steps
/// were never dispatched must not look like a run that succeeded, and it must
/// not fall back onto a plain agent loop over the marker payload either.
pub fn unmounted_run(
    ctx: DrainCtx,
    plan_digest: Option<String>,
) -> BoxStream<'static, LoopOutcome> {
    futures::stream::once(async move {
        let payload = kian_superagent::job::status::run_verdict_payload(
            None,
            kian_superagent::job::outcome::RunOutcome::Failed,
            plan_digest.as_deref().unwrap_or(""),
            &[],
            None,
            Some("effect_unknown"),
        );
        if let Err(error) = ctx.append(EventKind::EvidenceVerdict, payload).await {
            tracing::warn!(
                session = %ctx.session().0,
                %error,
                "job run: cannot record the unmounted verdict"
            );
        }
        LoopOutcome::SoftFailed {
            reason: "job runs are not mounted in this process".to_string(),
        }
    })
    .boxed()
}

/// The delivery port over the one mounted channel registry (§13.1): the
/// conductor names the channel the plan froze; an unmounted name is an
/// honest `Unavailable`.
#[must_use]
pub fn deliver_port(channels: &kian_superagent::ChannelRegistry) -> Arc<dyn DeliverPort> {
    Arc::new(channels.clone())
}
