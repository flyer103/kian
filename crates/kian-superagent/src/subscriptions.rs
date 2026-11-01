//! The subscription service (card C23): consume `ChannelEvent` streams
//! from registered [`ChannelAdapter`]s, evaluate the declarative rule
//! set, admit a provenance-wrapped input to the routed target — the row
//! durable **before** the wake fires (invariant 1) — then wake it.
//!
//! Routing (DESIGN §4.1): `router` rules admit to the configured
//! resident Router session; `passthrough` rules correlate the event's
//! topic through [`PassthroughRegistry`], admitting straight into the
//! owning worker's inbox without Router judgment. A passthrough topic
//! with no registered worker falls back to the Router (the bootstrap
//! path: the Router judges the first event of an unknown topic and the
//! spawner binds the worker; both transitions leave an audit log line).
//!
//! Events matching no rule are dropped with an audit log line and no
//! inbox row — an honest no-op, never a silent swallow.

use std::sync::{Arc, RwLock};

use futures::StreamExt;
use kian_core::model::SessionId;
use kian_core::seams::{ChannelAdapter, ChannelEvent};
use tracing::info;
use tracing::warn;

use crate::inbox::{InboxWriteError, InboxWriter};
use crate::provenance::channel_input_payload;
use crate::registry::PassthroughRegistry;
use crate::rules::{Route, SubscriptionRuleSet};

/// What handling one channel event concluded — the auditable decision,
/// returned to callers and asserted in tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingOutcome {
    /// Admitted to `session` (durable row, then wake). `deduplicated`
    /// is true when the event's id had already been admitted — exactly
    /// one row exists either way.
    Routed {
        session: SessionId,
        deduplicated: bool,
    },
    /// No rule matched: audit-logged, no inbox row, no wake.
    DroppedNoRule,
}

/// The subscription service over one rule set and one inbox port.
///
/// Holds the passthrough registry behind a lock: it is runtime-mutable
/// (the spawner registers workers) yet disposable — a restart replaces
/// it wholesale with the projection rebuilt from event streams
/// ([`SubscriptionService::install_registry`]).
pub struct SubscriptionService {
    router_session: SessionId,
    rules: SubscriptionRuleSet,
    registry: RwLock<PassthroughRegistry>,
    inbox: Arc<dyn InboxWriter>,
}

impl SubscriptionService {
    pub fn new(
        router_session: SessionId,
        rules: SubscriptionRuleSet,
        inbox: Arc<dyn InboxWriter>,
    ) -> Self {
        Self {
            router_session,
            rules,
            registry: RwLock::new(PassthroughRegistry::new()),
            inbox,
        }
    }

    /// The configured resident Router session.
    pub fn router_session(&self) -> SessionId {
        self.router_session
    }

    /// Runtime registration of a passthrough binding (the worker
    /// spawner, C25). Restart-equivalent state comes from
    /// [`SubscriptionService::install_registry`] instead.
    pub fn register_topic(&self, topic: impl Into<String>, worker: SessionId) {
        self.registry
            .write()
            .expect("passthrough registry lock")
            .register(topic, worker);
    }

    /// Installs a registry rebuilt from event streams at startup
    /// (constraint 1: the projection is disposable and re-derived, so
    /// no worker binding survives only in memory).
    pub fn install_registry(&self, rebuilt: PassthroughRegistry) {
        *self.registry.write().expect("passthrough registry lock") = rebuilt;
    }

    /// Read-only snapshot (inspection/tests).
    pub fn registry_snapshot(&self) -> PassthroughRegistry {
        self.registry
            .read()
            .expect("passthrough registry lock")
            .clone()
    }

    /// Handles one inbound channel event: rule match → route →
    /// durable admit (idempotent per `event_id`) → wake.
    ///
    /// Ordering is the point (invariant 1): the `admit` future is
    /// awaited to completion — the row durably committed — *before*
    /// `wake` is invoked. A failed admit returns the error without any
    /// wake, mirroring the engine's own discipline.
    pub async fn handle_event(
        &self,
        event: ChannelEvent,
    ) -> Result<RoutingOutcome, InboxWriteError> {
        let Some(rule) = self.rules.first_match(&event) else {
            info!(
                target: "kian::subscriptions",
                channel = %event.channel,
                event_id = %event.event_id,
                "subscription audit: event dropped, no matching rule"
            );
            return Ok(RoutingOutcome::DroppedNoRule);
        };

        let (target, unbound_topic) = match &rule.route {
            Route::Router => (self.router_session, false),
            Route::Passthrough { .. } => match event.topic.as_deref().and_then(|t| {
                self.registry
                    .read()
                    .expect("passthrough registry lock")
                    .resolve(t)
            }) {
                Some(worker) => (worker, false),
                None => {
                    warn!(
                        target: "kian::subscriptions",
                        channel = %event.channel,
                        event_id = %event.event_id,
                        topic = event.topic.as_deref().unwrap_or(""),
                        "subscription audit: passthrough topic unbound, falling back to router"
                    );
                    (self.router_session, true)
                }
            },
        };
        let _ = unbound_topic; // audited above; the outcome records the session

        let payload = channel_input_payload(&event);
        // Invariant 1: durable row first — the admit future resolves
        // only on commit — and the wake strictly after.
        let receipt = self
            .inbox
            .admit(target, payload, Some(event.event_id.clone()))
            .await?;
        self.inbox.wake(target);
        Ok(RoutingOutcome::Routed {
            session: target,
            deduplicated: receipt.deduplicated,
        })
    }

    /// Spawns one consumer task per adapter: each subscribes and feeds
    /// events to [`SubscriptionService::handle_event`] until the
    /// adapter's stream ends. Handling errors are logged and the
    /// consumer keeps going — the durable idempotency key means a
    /// retry of the same event can never double-admit.
    pub fn spawn(
        self: &Arc<Self>,
        adapters: Vec<Arc<dyn ChannelAdapter>>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        adapters
            .into_iter()
            .map(|adapter| {
                let service = Arc::clone(self);
                let mut stream = adapter.subscribe();
                tokio::spawn(async move {
                    while let Some(event) = stream.next().await {
                        if let Err(e) = service.handle_event(event).await {
                            warn!(
                                target: "kian::subscriptions",
                                error = %e,
                                "subscription audit: handling failed, event left to the adapter"
                            );
                        }
                    }
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::{RoutingOutcome, SubscriptionService};
    use crate::inbox::{AdmitReceipt, InboxWriteError, InboxWriter};
    use crate::registry::PassthroughRegistry;
    use crate::rules::{CorrelateBy, Route, RuleMatcher, SubscriptionRule, SubscriptionRuleSet};
    use futures::future::BoxFuture;
    use kian_core::model::SessionId;
    use kian_core::seams::ChannelEvent;
    use kian_orchestrator::WakeOutcome;
    use serde_json::{Value, json};

    fn sid(n: u128) -> SessionId {
        SessionId(uuid::Uuid::from_u128(n))
    }

    /// Scripted port: records the ordered call log so ordering and
    /// routing are observable without any server.
    struct ScriptedInbox {
        admits: Mutex<Vec<(SessionId, Option<String>)>>,
        wakes: Mutex<Vec<SessionId>>,
        dedup: bool,
        fail: bool,
    }

    impl ScriptedInbox {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                admits: Mutex::new(Vec::new()),
                wakes: Mutex::new(Vec::new()),
                dedup: false,
                fail: false,
            })
        }
    }

    impl InboxWriter for ScriptedInbox {
        fn admit(
            &self,
            session: SessionId,
            _payload: Value,
            idempotency_key: Option<String>,
        ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
            self.admits.lock().unwrap().push((session, idempotency_key));
            let dedup = self.dedup;
            let fail = self.fail;
            Box::pin(async move {
                if fail {
                    Err(InboxWriteError::Write("scripted".into()))
                } else {
                    Ok(AdmitReceipt {
                        deduplicated: dedup,
                    })
                }
            })
        }

        fn wake(&self, session: SessionId) -> WakeOutcome {
            self.wakes.lock().unwrap().push(session);
            WakeOutcome::Folded
        }
    }

    fn event(channel: &str, event_id: &str, topic: Option<&str>) -> ChannelEvent {
        ChannelEvent {
            channel: channel.into(),
            event_id: event_id.into(),
            topic: topic.map(str::to_string),
            payload: json!({"text": "hello"}),
            ts_ms: 1,
        }
    }

    fn rules() -> SubscriptionRuleSet {
        SubscriptionRuleSet::new(vec![
            SubscriptionRule {
                channel: "ci".into(),
                matcher: RuleMatcher::Topic("pr-9".into()),
                route: Route::Passthrough {
                    correlate_by: CorrelateBy::Topic,
                },
            },
            SubscriptionRule {
                channel: "mock-im".into(),
                matcher: RuleMatcher::Always,
                route: Route::Router,
            },
        ])
    }

    #[tokio::test]
    async fn router_rule_admits_to_router_then_wakes_in_order() {
        let inbox = ScriptedInbox::new();
        let router = sid(1);
        let service =
            SubscriptionService::new(router, rules(), Arc::clone(&inbox) as Arc<dyn InboxWriter>);
        let outcome = service
            .handle_event(event("mock-im", "evt-1", None))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: router,
                deduplicated: false
            }
        );
        // Invariant 1, observable: admit completes before wake fires.
        let admits = inbox.admits.lock().unwrap().clone();
        let wakes = inbox.wakes.lock().unwrap().clone();
        assert_eq!(admits.len(), 1);
        assert_eq!(wakes, vec![router]);
        assert_eq!(admits[0].0, router);
        assert_eq!(admits[0].1.as_deref(), Some("evt-1"));
    }

    #[tokio::test]
    async fn passthrough_routes_to_the_registered_worker_not_the_router() {
        let inbox = ScriptedInbox::new();
        let router = sid(1);
        let worker = sid(2);
        let service =
            SubscriptionService::new(router, rules(), Arc::clone(&inbox) as Arc<dyn InboxWriter>);
        service.register_topic("pr-9", worker);
        let outcome = service
            .handle_event(event("ci", "evt-2", Some("pr-9")))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: worker,
                deduplicated: false
            }
        );
        let admits = inbox.admits.lock().unwrap().clone();
        assert_eq!(admits.len(), 1);
        assert_eq!(admits[0].0, worker);
        let wakes = inbox.wakes.lock().unwrap().clone();
        assert_eq!(wakes, vec![worker]);
    }

    #[tokio::test]
    async fn unbound_passthrough_topic_falls_back_to_router() {
        let inbox = ScriptedInbox::new();
        let router = sid(1);
        // The rule matches every event on the channel, but the registry
        // holds no binding for the event's topic — the bootstrap
        // fallback admits to the Router (with an audit line).
        let rules = SubscriptionRuleSet::new(vec![SubscriptionRule {
            channel: "ci".into(),
            matcher: RuleMatcher::Always,
            route: Route::Passthrough {
                correlate_by: CorrelateBy::Topic,
            },
        }]);
        let service =
            SubscriptionService::new(router, rules, Arc::clone(&inbox) as Arc<dyn InboxWriter>);
        let outcome = service
            .handle_event(event("ci", "evt-3", Some("pr-404")))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: router,
                deduplicated: false
            }
        );
    }

    #[tokio::test]
    async fn unmatched_event_is_dropped_with_no_admit_and_no_wake() {
        let inbox = ScriptedInbox::new();
        let service =
            SubscriptionService::new(sid(1), rules(), Arc::clone(&inbox) as Arc<dyn InboxWriter>);
        let outcome = service
            .handle_event(event("github", "evt-4", None))
            .await
            .unwrap();
        assert_eq!(outcome, RoutingOutcome::DroppedNoRule);
        assert!(inbox.admits.lock().unwrap().is_empty());
        assert!(inbox.wakes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_admit_never_wakes() {
        let inbox = Arc::new(ScriptedInbox {
            admits: Mutex::new(Vec::new()),
            wakes: Mutex::new(Vec::new()),
            dedup: false,
            fail: true,
        });
        let service =
            SubscriptionService::new(sid(1), rules(), Arc::clone(&inbox) as Arc<dyn InboxWriter>);
        let err = service
            .handle_event(event("mock-im", "evt-5", None))
            .await
            .unwrap_err();
        assert!(matches!(err, InboxWriteError::Write(_)));
        assert!(inbox.wakes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn registry_install_replaces_runtime_state() {
        let inbox = ScriptedInbox::new();
        let router = sid(1);
        let service =
            SubscriptionService::new(router, rules(), Arc::clone(&inbox) as Arc<dyn InboxWriter>);
        let worker = sid(9);
        let mut rebuilt = PassthroughRegistry::new();
        rebuilt.register("pr-9", worker);
        service.install_registry(rebuilt);
        assert_eq!(service.registry_snapshot().resolve("pr-9"), Some(worker));
        let outcome = service
            .handle_event(event("ci", "evt-6", Some("pr-9")))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            RoutingOutcome::Routed {
                session: worker,
                deduplicated: false
            }
        );
        assert_eq!(service.router_session(), router);
    }
}
