//! Watchdog acceptance tests: the v1 server-side
//! [`WatchdogSource`] over the registry's heartbeat freshness.
//!
//! Every test drives the real sqlite registry (rows written through the
//! public `upsert_endpoint` projection API, exactly as the queue drivers
//! do) and a [`ManualClock`], so freshness arithmetic is deterministic;
//! only the poll cadence runs on real time. No credentials, no network.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use futures::StreamExt;
use kian_core::seams::{EndpointHealth, HealthEvent, WatchdogSource};
use kian_exec::{Clock, ExecStore, HeartbeatWatchdog, ManualClock, WatchdogConfig};
use uuid::Uuid;

/// Local e2e discipline (plan §1 rule 6).
fn no_proxy() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        // SAFETY: no other thread in this test binary reads the
        // environment, and the write happens exactly once before any
        // socket is opened by the calling test.
        unsafe { std::env::set_var("NO_PROXY", "127.0.0.1,localhost") };
    });
}

fn temp_db() -> PathBuf {
    std::env::temp_dir().join(format!("kian-exec-watchdog-{}.sqlite", Uuid::new_v4()))
}

/// Fast, deterministic tuning: 1000 ms freshness deadline, 25 ms polls.
fn config() -> WatchdogConfig {
    WatchdogConfig {
        offline_after: Duration::from_millis(1_000),
        poll_interval: Duration::from_millis(25),
    }
}

/// Register one endpoint with the given heartbeat timestamp (the queue
/// drivers' projection write; the watchdog only ever reads it).
async fn heartbeat(store: &ExecStore, endpoint: &str, ts_ms: u64, state: &str) {
    store
        .upsert_endpoint(endpoint, "127.0.0.1:9000", ts_ms, state)
        .await
        .unwrap();
}

async fn next_event(
    stream: &mut (impl StreamExt<Item = HealthEvent> + Unpin),
    what: &str,
) -> HealthEvent {
    match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
        Ok(Some(event)) => event,
        other => panic!("expected a health event ({what}), got {other:?}"),
    }
}

/// Advance the manual clock past the deadline in poll-sized steps (so the
/// observed detection clock cannot run far ahead of the deadline) and wait
/// for the next event.
async fn advance_until_event(
    clock: &ManualClock,
    poll: Duration,
    stream: &mut (impl StreamExt<Item = HealthEvent> + Unpin),
    what: &str,
) -> HealthEvent {
    let mut waited = Duration::ZERO;
    while waited < Duration::from_secs(5) {
        clock.advance_by(poll.as_millis() as u64);
        if let Ok(Some(event)) = tokio::time::timeout(poll, stream.next()).await {
            return event;
        }
        waited += poll;
    }
    panic!("no event while advancing the clock ({what})");
}

/// Offline is detected on the first poll after the freshness deadline,
/// within the documented `offline_after + poll_interval` bound.
#[tokio::test]
async fn offline_detected_within_offline_after_plus_poll() {
    no_proxy();
    let store = ExecStore::open(temp_db()).await.unwrap();
    let clock = Arc::new(ManualClock::new(10_000));
    heartbeat(&store, "dev-box", 10_000, "online").await;

    let watchdog = HeartbeatWatchdog::start(
        store.clone(),
        config(),
        clock.clone() as Arc<dyn kian_exec::Clock>,
    );
    let mut health = watchdog.health();

    // Deadline is 10_000 + 1_000 = 11_000. Advance in poll-sized steps,
    // mimicking real time, until the deadline is crossed.
    let event = advance_until_event(&clock, config().poll_interval, &mut health, "offline").await;

    assert_eq!(event.endpoint_id, "dev-box");
    assert_eq!(event.status, EndpointHealth::Offline);
    // Detection clock is within one poll of the deadline.
    assert!(
        event.ts_ms <= 11_000 + config().poll_interval.as_millis() as u64,
        "detection at {} ran past the deadline + poll bound",
        event.ts_ms
    );
    assert!(event.detail.is_some(), "offline events carry a detail");
    // The transition and its latency landed in the metrics.
    assert_eq!(watchdog.metrics().offline_transitions(), 1);
    let latency = watchdog
        .metrics()
        .mean_detection_latency_ms()
        .expect("one latency sample recorded");
    assert!(
        latency <= config().poll_interval.as_millis() as u64,
        "detection latency {latency} exceeds one poll"
    );

    watchdog.shutdown().await;
}

/// A reconnect emits `online` exactly once — no flapping on later polls
/// that still see a fresh heartbeat.
#[tokio::test]
async fn reconnect_emits_online_exactly_once() {
    no_proxy();
    let store = ExecStore::open(temp_db()).await.unwrap();
    let clock = Arc::new(ManualClock::new(10_000));
    heartbeat(&store, "dev-box", 10_000, "online").await;

    let watchdog = HeartbeatWatchdog::start(
        store.clone(),
        config(),
        clock.clone() as Arc<dyn kian_exec::Clock>,
    );
    let mut health = watchdog.health();

    // Go offline.
    let offline = advance_until_event(&clock, config().poll_interval, &mut health, "offline").await;
    assert_eq!(offline.status, EndpointHealth::Offline);

    // Reconnect: a fresh heartbeat at the current clock.
    let now = clock.now_ms();
    heartbeat(&store, "dev-box", now, "online").await;
    let online = next_event(&mut health, "online").await;
    assert_eq!(online.endpoint_id, "dev-box");
    assert_eq!(online.status, EndpointHealth::Online);
    assert_eq!(watchdog.metrics().online_transitions(), 1);

    // Keep the heartbeat fresh and the clock running: exactly-once means
    // silence from here on.
    for _ in 0..6 {
        clock.advance_by(config().poll_interval.as_millis() as u64);
        heartbeat(&store, "dev-box", clock.now_ms(), "online").await;
        tokio::time::sleep(config().poll_interval).await;
    }
    assert!(
        tokio::time::timeout(3 * config().poll_interval, health.next())
            .await
            .is_err(),
        "a steady online endpoint must not emit again"
    );
    assert_eq!(watchdog.metrics().transitions(), 2);

    watchdog.shutdown().await;
}

/// A silent-but-connected endpoint (transport up per the registry's state
/// column, but no heartbeat frames) goes offline: freshness rules, not the
/// connection view.
#[tokio::test]
async fn silent_but_connected_endpoint_goes_offline() {
    no_proxy();
    let store = ExecStore::open(temp_db()).await.unwrap();
    let clock = Arc::new(ManualClock::new(10_000));
    // The registry says "online" (transport up) with a heartbeat at the
    // current clock; from here on the endpoint stays connected but silent.
    heartbeat(&store, "dev-box", 10_000, "online").await;

    let watchdog = HeartbeatWatchdog::start(
        store.clone(),
        config(),
        clock.clone() as Arc<dyn kian_exec::Clock>,
    );
    let mut health = watchdog.health();

    // No further heartbeats — but the queue driver keeps refreshing the
    // row's state column as "online" (its transport view).
    for _ in 0..3 {
        clock.advance_by(400);
        heartbeat(&store, "dev-box", 10_000, "online").await;
    }
    let event = advance_until_event(&clock, config().poll_interval, &mut health, "offline").await;
    assert_eq!(event.endpoint_id, "dev-box");
    assert_eq!(event.status, EndpointHealth::Offline);
    assert_eq!(
        watchdog.snapshot().get("dev-box"),
        Some(&EndpointHealth::Offline)
    );

    watchdog.shutdown().await;
}

/// A healthy endpoint never emits: the baseline is silent and steady
/// freshness stays silent.
#[tokio::test]
async fn healthy_endpoint_never_emits() {
    no_proxy();
    let store = ExecStore::open(temp_db()).await.unwrap();
    let clock = Arc::new(ManualClock::new(10_000));
    heartbeat(&store, "dev-box", 10_000, "online").await;
    heartbeat(&store, "ci-runner", 9_900, "online").await;

    let watchdog = HeartbeatWatchdog::start(
        store.clone(),
        config(),
        clock.clone() as Arc<dyn kian_exec::Clock>,
    );
    let mut health = watchdog.health();

    // Heartbeats keep flowing while the clock advances well past a whole
    // freshness window, several times over.
    for _ in 0..4 {
        for _ in 0..10 {
            clock.advance_by(150);
            heartbeat(&store, "dev-box", clock.now_ms(), "online").await;
            heartbeat(&store, "ci-runner", clock.now_ms(), "online").await;
        }
        tokio::time::sleep(2 * config().poll_interval).await;
    }
    assert!(
        tokio::time::timeout(3 * config().poll_interval, health.next())
            .await
            .is_err(),
        "a healthy endpoint must never emit a health event"
    );
    assert_eq!(watchdog.metrics().transitions(), 0);
    // Current state is still live and correct.
    assert_eq!(
        watchdog.snapshot().get("dev-box"),
        Some(&EndpointHealth::Online)
    );

    watchdog.shutdown().await;
}

/// The stream is live: a subscriber that arrives late sees the current
/// state (offline endpoints reported; healthy ones silent).
#[tokio::test]
async fn late_subscriber_sees_current_state() {
    no_proxy();
    let store = ExecStore::open(temp_db()).await.unwrap();
    let clock = Arc::new(ManualClock::new(10_000));
    // Only dev-box exists during detection (registry rows are read in
    // endpoint-id order; a same-deadline second endpoint would win the
    // first edge). ci-runner registers fresh only after dev-box is offline.
    heartbeat(&store, "dev-box", 10_000, "online").await;

    let watchdog = HeartbeatWatchdog::start(
        store.clone(),
        config(),
        clock.clone() as Arc<dyn kian_exec::Clock>,
    );

    // dev-box dies (and is detected) before the late subscriber arrives.
    // ci-runner only registers now, fresh — it stays healthy throughout.
    let mut first = watchdog.health();
    let offline = advance_until_event(&clock, config().poll_interval, &mut first, "offline").await;
    assert_eq!(offline.endpoint_id, "dev-box");
    heartbeat(&store, "ci-runner", clock.now_ms(), "online").await;
    tokio::time::sleep(2 * config().poll_interval).await;

    // The late subscriber opens now and must immediately learn dev-box's
    // current offline state — and nothing about healthy ci-runner.
    let mut late = watchdog.health();
    let current = next_event(&mut late, "current offline state").await;
    assert_eq!(current.endpoint_id, "dev-box");
    assert_eq!(current.status, EndpointHealth::Offline);
    assert!(
        tokio::time::timeout(3 * config().poll_interval, late.next())
            .await
            .is_err(),
        "healthy endpoints stay silent for late subscribers too"
    );

    // The watch channel carries the same live state.
    let mut snapshot = watchdog.subscribe();
    let states = snapshot.borrow_and_update().clone();
    assert_eq!(states.get("dev-box"), Some(&EndpointHealth::Offline));
    assert_eq!(states.get("ci-runner"), Some(&EndpointHealth::Online));

    watchdog.shutdown().await;
}
