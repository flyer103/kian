//! `HeartbeatWatchdog` — the v1 [`WatchdogSource`] impl (
//! DESIGN §4.3): an edge-triggered health monitor over the endpoint
//! registry projection.
//!
//! v1 is deliberately **server-side**: a dead daemon cannot report its own
//! death, so the authoritative view is the server's heartbeat-absence
//! observation — `now - last_heartbeat > offline_after` marks an endpoint
//! offline, a fresh heartbeat marks it online again (DESIGN §4.3;).
//! Endpoint-side self-checks (anomaly self-healing) remain deferred.
//!
//! Semantics:
//!
//! - **Freshness rules, not the `state` column.** The registry's `state`
//!   string mirrors the connection manager's transport view; the watchdog
//!   trusts only `last_heartbeat_ms`. A silent-but-connected endpoint
//!   (transport up, no heartbeat frames) is offline as far as health is
//!   concerned.
//! - **Edge-triggered, no flapping.** One [`HealthEvent`] per state
//!   *change*, never per poll. A healthy endpoint never emits: its first
//!   observation establishes the baseline silently. A first observation
//!   that is already stale *does* emit offline — the registry's previous
//!   word was online, and the change is real and actionable.
//! - **Observes only.** The watchdog never writes sessions or events;
//!   health events reach sessions exclusively via Admit (wired ) —
//!   the layered iron rule.
//!
//! Exposure: a `tokio::sync::watch` channel carries the live state map
//! (a late subscriber reads the current state immediately), a broadcast
//! channel fans the edge events out to any number of [`WatchdogSource`]
//! consumers, and [`WatchdogMetrics`] counts transitions and offline
//! detection latency.
//!
//! Freshness compares the server clock against the endpoint-*reported*
//! heartbeat timestamp (the v0 registry semantics — own that write
//! path and this change does not touch it); clock-skew hardening
//! (server-receive timestamps) is deferred.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream::{BoxStream, StreamExt};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

use kian_core::seams::{EndpointHealth, HealthEvent, WatchdogSource};

use crate::store::{EndpointRegistryRow, ExecStore};

/// How many edge events may queue per subscriber before the broadcast
/// channel reports lag (logged; the subscriber keeps the latest state via
/// its own dedupe map, so a lagging subscriber drops noise, not truth).
const EVENT_CHANNEL_CAP: usize = 256;

// ---------------------------------------------------------------------------
// Clock (injectable so watchdog tests are deterministic)
// ---------------------------------------------------------------------------

/// Millisecond clock, injectable for deterministic tests (the same shape
/// as `kian-orchestrator`'s `Clock`; kept local because kian-exec must not
/// depend on the orchestrator — DESIGN §7).
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// The real wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        crate::store::now_ms().max(0) as u64
    }
}

/// A manually advanced clock for deterministic freshness tests.
#[derive(Debug)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    #[must_use]
    pub fn new(start_ms: u64) -> Self {
        Self(AtomicU64::new(start_ms))
    }

    /// Jump the clock to `ms` (monotone: never backwards).
    pub fn advance_to(&self, ms: u64) {
        let mut current = self.0.load(Ordering::Relaxed);
        while ms > current {
            match self
                .0
                .compare_exchange_weak(current, ms, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    /// Advance the clock by `ms` and return the new reading.
    pub fn advance_by(&self, ms: u64) -> u64 {
        self.0.fetch_add(ms, Ordering::Relaxed) + ms
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Config + metrics
// ---------------------------------------------------------------------------

/// Tuning of the heartbeat watchdog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WatchdogConfig {
    /// How stale a heartbeat may be before the endpoint is offline
    /// (`offline_after_ms` in the plan): offline iff
    /// `now - last_heartbeat > offline_after`.
    pub offline_after: Duration,
    /// Registry polling cadence. The detection bound is
    /// `offline_after + poll_interval`.
    pub poll_interval: Duration,
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            offline_after: Duration::from_secs(30),
            poll_interval: Duration::from_secs(1),
        }
    }
}

/// Watchdog counters for the metrics surface (wires them into
/// `/metrics`). All fields are plain atomics; read them cheaply.
#[derive(Debug, Default)]
pub struct WatchdogMetrics {
    transitions_to_offline: AtomicU64,
    transitions_to_online: AtomicU64,
    detection_latency_ms_total: AtomicU64,
    detection_latency_samples: AtomicU64,
}

impl WatchdogMetrics {
    /// Offline transitions emitted so far.
    pub fn offline_transitions(&self) -> u64 {
        self.transitions_to_offline.load(Ordering::Relaxed)
    }

    /// Online (reconnect) transitions emitted so far.
    pub fn online_transitions(&self) -> u64 {
        self.transitions_to_online.load(Ordering::Relaxed)
    }

    /// Total state transitions emitted so far.
    pub fn transitions(&self) -> u64 {
        self.offline_transitions() + self.online_transitions()
    }

    /// Mean offline detection latency in ms: how far past the
    /// `offline_after` deadline the transition was observed. Bounded by
    /// `poll_interval` in steady operation.
    pub fn mean_detection_latency_ms(&self) -> Option<u64> {
        let samples = self.detection_latency_samples.load(Ordering::Relaxed);
        (samples > 0).then(|| self.detection_latency_ms_total.load(Ordering::Relaxed) / samples)
    }

    fn record_offline(&self, latency_ms: u64) {
        self.transitions_to_offline.fetch_add(1, Ordering::Relaxed);
        self.detection_latency_ms_total
            .fetch_add(latency_ms, Ordering::Relaxed);
        self.detection_latency_samples
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_online(&self) {
        self.transitions_to_online.fetch_add(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// HeartbeatWatchdog
// ---------------------------------------------------------------------------

struct Inner {
    store: ExecStore,
    config: WatchdogConfig,
    clock: Arc<dyn Clock>,
    /// Registry heartbeats older than this boot cannot establish online
    /// membership: they are display cache, not a live connection.
    started_at_ms: u64,
    /// Live per-endpoint state map (the watch-backed current-state view).
    states: watch::Sender<Arc<HashMap<String, EndpointHealth>>>,
    /// Edge events fanned out to [`WatchdogSource`] consumers.
    events: broadcast::Sender<HealthEvent>,
    metrics: WatchdogMetrics,
    shutdown: watch::Sender<bool>,
}

/// The v1 server-side [`WatchdogSource`]: an edge-triggered heartbeat
/// monitor over the endpoint registry (DESIGN §4.3). Build with
/// [`HeartbeatWatchdog::start`]; share one per process.
pub struct HeartbeatWatchdog {
    inner: Arc<Inner>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl HeartbeatWatchdog {
    /// Spawns the monitor over a migrated [`ExecStore`]. The registry is a
    /// disposable projection owned by the queue drivers; the watchdog only
    /// ever reads it.
    pub fn start(store: ExecStore, config: WatchdogConfig, clock: Arc<dyn Clock>) -> Arc<Self> {
        let (states, _) = watch::channel(Arc::new(HashMap::new()));
        let (events, _) = broadcast::channel(EVENT_CHANNEL_CAP);
        let (shutdown, _) = watch::channel(false);
        let started_at_ms = clock.now_ms();
        let inner = Arc::new(Inner {
            store,
            config,
            clock,
            started_at_ms,
            states,
            events,
            metrics: WatchdogMetrics::default(),
            shutdown,
        });
        let task = tokio::spawn(run(inner.clone()));
        Arc::new(Self {
            inner,
            task: Mutex::new(Some(task)),
        })
    }

    /// The live state map. A late subscriber reads the current state
    /// immediately (`borrow_and_update`), then sees every change.
    pub fn subscribe(&self) -> watch::Receiver<Arc<HashMap<String, EndpointHealth>>> {
        self.inner.states.subscribe()
    }

    /// A copy of the current state map.
    pub fn snapshot(&self) -> HashMap<String, EndpointHealth> {
        (*self.inner.states.borrow()).as_ref().clone()
    }

    /// Counters (transitions, detection latency) for the metrics surface.
    pub fn metrics(&self) -> &WatchdogMetrics {
        &self.inner.metrics
    }

    /// Stops the monitor and joins it.
    pub async fn shutdown(&self) {
        self.inner.shutdown.send_replace(true);
        let task = self.task.lock().unwrap().take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

impl WatchdogSource for HeartbeatWatchdog {
    /// The edge stream. Late subscribers are live: the stream opens with
    /// the current state of every endpoint that is **offline** right now
    /// (online is the absence of signal — healthy endpoints stay silent),
    /// then carries exactly one event per future state change. A
    /// subscriber-local dedupe map keeps the hand-off between the opening
    /// snapshot and the live edges exactly-once even if an edge races the
    /// subscription.
    fn health(&self) -> BoxStream<'static, HealthEvent> {
        let now = self.inner.clock.now_ms();
        let rx = self.inner.events.subscribe();
        let snapshot = self.snapshot();
        let initial: Vec<HealthEvent> = snapshot
            .iter()
            .filter(|(_, health)| **health == EndpointHealth::Offline)
            .map(|(endpoint_id, _)| HealthEvent {
                endpoint_id: endpoint_id.clone(),
                status: EndpointHealth::Offline,
                ts_ms: now,
                detail: Some("offline at subscribe (current state)".into()),
            })
            .collect();
        // The dedupe map rides in the unfold state (not captured by the
        // async block), so it survives across polls and stays owned.
        let tail = futures::stream::unfold((rx, snapshot), |(mut rx, mut seen)| async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if seen.get(&event.endpoint_id) == Some(&event.status) {
                            continue; // already implied by this subscriber's state
                        }
                        seen.insert(event.endpoint_id.clone(), event.status);
                        return Some((event, (rx, seen)));
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        // The subscriber fell behind on edge *events*; the
                        // dedupe map keeps its state view correct, so dropped
                        // duplicates are noise, not truth.
                        tracing::warn!(skipped, "watchdog subscriber lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
        futures::stream::iter(initial).chain(tail).boxed()
    }
}

/// The monitor loop: poll the registry, detect freshness edges, publish.
async fn run(inner: Arc<Inner>) {
    let mut shutdown = inner.shutdown.subscribe();
    let mut states: HashMap<String, EndpointHealth> = HashMap::new();
    loop {
        if *inner.shutdown.borrow() {
            break;
        }
        let now = inner.clock.now_ms();
        match inner.store.endpoints().await {
            Ok(rows) => {
                let (events, changed) = observe(&inner, &mut states, &rows, now);
                if changed {
                    inner.states.send_replace(Arc::new(states.clone()));
                }
                for event in events {
                    // No live subscribers is not an error.
                    let _ = inner.events.send(event);
                }
            }
            Err(error) => {
                tracing::warn!(%error, "watchdog registry read failed");
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(inner.config.poll_interval) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
            }
        }
    }
    tracing::debug!("heartbeat watchdog stopped");
}

/// One poll's edge detection over the registry rows. Returns the emitted
/// events and whether the state map changed (for the watch channel).
///
/// Freshness rules: offline iff `now - last_heartbeat > offline_after`
/// (saturating — a `last_heartbeat_ms` in the future, e.g. from endpoint
/// clock skew, counts as fresh). The registry's `state` column is
/// deliberately ignored (transport view ≠ health view).
fn observe(
    inner: &Inner,
    states: &mut HashMap<String, EndpointHealth>,
    rows: &[EndpointRegistryRow],
    now: u64,
) -> (Vec<HealthEvent>, bool) {
    let offline_after_ms = inner
        .config
        .offline_after
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    let mut events = Vec::new();
    let mut changed = false;
    for row in rows {
        let age = now.saturating_sub(row.last_heartbeat_ms);
        let health = if age > offline_after_ms || row.last_heartbeat_ms <= inner.started_at_ms {
            EndpointHealth::Offline
        } else {
            EndpointHealth::Online
        };
        match states.insert(row.endpoint_id.clone(), health) {
            // Unchanged: edge-triggered means silence — no flapping.
            Some(prev) if prev == health => {}
            // A state transition: emit exactly one edge.
            Some(prev) => {
                let event = match health {
                    EndpointHealth::Offline => {
                        // Detection latency: how far past the deadline the
                        // transition was observed (≤ poll_interval in
                        // steady operation).
                        let latency = age.saturating_sub(offline_after_ms);
                        inner.metrics.record_offline(latency);
                        HealthEvent {
                            endpoint_id: row.endpoint_id.clone(),
                            status: EndpointHealth::Offline,
                            ts_ms: now,
                            detail: Some(format!(
                                "no heartbeat for {age} ms (offline_after {offline_after_ms} ms); was {prev:?}"
                            )),
                        }
                    }
                    EndpointHealth::Online => {
                        inner.metrics.record_online();
                        HealthEvent {
                            endpoint_id: row.endpoint_id.clone(),
                            status: EndpointHealth::Online,
                            ts_ms: now,
                            detail: Some("heartbeat resumed".into()),
                        }
                    }
                };
                events.push(event);
                changed = true;
            }
            // First observation: a healthy endpoint establishes its
            // baseline silently (it never emits); an already-stale one was
            // online in the registry's previous word — report the change.
            None => {
                changed = true;
                if health == EndpointHealth::Offline {
                    let latency = age.saturating_sub(offline_after_ms);
                    inner.metrics.record_offline(latency);
                    events.push(HealthEvent {
                        endpoint_id: row.endpoint_id.clone(),
                        status: EndpointHealth::Offline,
                        ts_ms: now,
                        detail: Some(format!(
                            "registered stale: no heartbeat for {age} ms (offline_after {offline_after_ms} ms)"
                        )),
                    });
                }
            }
        }
    }
    (events, changed)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Clock, ManualClock, WatchdogConfig};

    /// The manual clock never moves backwards and `advance_by` reports the
    /// new reading.
    #[test]
    fn manual_clock_is_monotone() {
        let clock = ManualClock::new(1_000);
        assert_eq!(clock.now_ms(), 1_000);
        assert_eq!(clock.advance_by(100), 1_100);
        clock.advance_to(500); // backwards is ignored
        assert_eq!(clock.now_ms(), 1_100);
        clock.advance_to(2_000);
        assert_eq!(clock.now_ms(), 2_000);
    }

    /// The default config states the documented detection bound.
    #[test]
    fn default_config_detection_bound() {
        let config = WatchdogConfig::default();
        assert_eq!(config.offline_after, Duration::from_secs(30));
        assert_eq!(config.poll_interval, Duration::from_secs(1));
    }
}
