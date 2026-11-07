//! `TimerChannel` — the timer input source (DESIGN §4.3).
//!
//! The v2 watchdog needs periodic ticks, and kian's one admission path
//! is the channel pipeline — so the timer is **a channel like any
//! other**: a production [`ChannelAdapter`] (beside
//! [`MockChannel`](crate::mock::MockChannel)) that emits
//! `ChannelEvent{channel:"timer", topic:Some("watchdog"),
//! event_id:"tick-<n>", payload:{"tick":n}}` on a configurable cadence.
//!
//! Why a channel (the resolved design question): an in-process tokio
//! interval admitting rows directly was rejected — ticks would leave no
//! durable audit of what the watchdog was asked to judge, would bypass
//! the subscription pipeline's `event_id` idempotency, and would fork
//! the admission path; a `TimerSource` seam in core was rejected —
//! constraint 10 (nothing generic justifies a seam). As a channel, a
//! tick is admitted like any event: provenance-wrapped, idempotent by
//! `event_id` (a restart that re-emits `tick-3` dedupes against the
//! committed row — the tick counter is per-process), and audited in the
//! target session's stream. Ticks missed while the server is down
//! simply never happened — the stream honestly shows no check ran.
//! `emit_on_start: true` triggers one immediate tick after startup.
//!
//! The cadence clock is [`crate::metrics::TurnClock`] (the bundle's
//! `now_ms` port): assembly injects the system clock, tests a
//! [`ManualTurnClock`](crate::metrics::ManualTurnClock) so cadence
//! assertions advance time explicitly, never by sleeping. The run loop
//! polls the clock on a short wall interval (no busy wait).
//!
//! Outbound `send` is an honest [`ChannelError::Unavailable`]: the
//! timer is inbound-only — escalation goes through a real channel.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use kian_core::seams::{ChannelAdapter, ChannelError, ChannelEvent, SendReceipt};
use serde_json::json;
use tracing::warn;

use crate::metrics::TurnClock;

/// The channel name timer adapters report by convention (config may
/// use any name; subscription rules key on whatever the adapter
/// reports).
pub const TIMER_CHANNEL: &str = "timer";

/// The default routing topic of timer events (the watchdog's).
pub const DEFAULT_TIMER_TOPIC: &str = "watchdog";

/// Default per-subscriber queue bound.
pub const TIMER_QUEUE_CAPACITY: usize = 64;

/// The timer's cadence. `interval_ms` must be > 0
/// (validated by the config layer, fail-closed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerConfig {
    /// Milliseconds between ticks.
    pub interval_ms: u64,
    /// Routing topic every tick carries (subscription rules match on
    /// it; default [`DEFAULT_TIMER_TOPIC`]).
    pub topic: String,
    /// Emit one tick immediately at startup (an immediate first
    /// progress check after a restart). Default false.
    pub emit_on_start: bool,
}

impl TimerConfig {
    /// The config with the default topic and no startup tick.
    #[must_use]
    pub fn new(interval_ms: u64) -> Self {
        Self {
            interval_ms,
            topic: DEFAULT_TIMER_TOPIC.to_string(),
            emit_on_start: false,
        }
    }
}

/// The timer as a channel: emits numbered ticks on cadence. The tick
/// counter starts at 1 per process — durability and cross-restart
/// dedupe live in admission (`event_id`), never here (constraint 1).
pub struct TimerChannel {
    channel: String,
    config: TimerConfig,
    clock: Arc<dyn TurnClock>,
    subscribers: Mutex<Vec<tokio::sync::mpsc::Sender<ChannelEvent>>>,
    /// Ticks emitted so far (the next tick is `emitted + 1`).
    emitted: AtomicU64,
    dropped_full: AtomicU64,
}

impl TimerChannel {
    /// A timer adapter named `channel` (convention [`TIMER_CHANNEL`])
    /// with `config`'s cadence, reading time from `clock`.
    pub fn new(channel: impl Into<String>, config: TimerConfig, clock: Arc<dyn TurnClock>) -> Self {
        Self {
            channel: channel.into(),
            config,
            clock,
            subscribers: Mutex::new(Vec::new()),
            emitted: AtomicU64::new(0),
            dropped_full: AtomicU64::new(0),
        }
    }

    /// The channel name this adapter reports on its events.
    pub fn channel_name(&self) -> &str {
        &self.channel
    }

    /// The cadence configuration.
    pub fn config(&self) -> &TimerConfig {
        &self.config
    }

    /// Ticks emitted so far (the emitted counter, not the wake state —
    /// admission may have deduped any of them).
    pub fn emitted(&self) -> u64 {
        self.emitted.load(Ordering::Relaxed)
    }

    /// How many tick copies were dropped because a subscriber queue
    /// was full (the bounded-queue observability point).
    pub fn dropped_full(&self) -> u64 {
        self.dropped_full.load(Ordering::Relaxed)
    }

    /// The event id of tick number `n` (the admission idempotency key).
    pub fn tick_event_id(n: u64) -> String {
        format!("tick-{n}")
    }

    /// Builds tick number `n`'s event (the payload shape the watchdog
    /// judge sees inside the provenance wrapper).
    #[must_use]
    pub fn tick_event(&self, n: u64) -> ChannelEvent {
        ChannelEvent {
            channel: self.channel.clone(),
            event_id: Self::tick_event_id(n),
            topic: Some(self.config.topic.clone()),
            sender: None,
            group: None,
            payload: json!({"tick": n}),
            ts_ms: self.clock.now_ms(),
        }
    }

    /// Fans one event out to every live subscriber (bounded `try_send`
    /// — a full queue drops that subscriber's copy with an audit line,
    /// exactly the `MockChannel` discipline).
    fn broadcast(&self, event: ChannelEvent) {
        let mut subscribers = self.subscribers.lock().expect("timer channel lock");
        subscribers.retain(|tx| !tx.is_closed());
        for tx in subscribers.iter() {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    self.dropped_full.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        target: "kian::timer_channel",
                        channel = %self.channel,
                        event_id = %event.event_id,
                        "timer channel audit: subscriber queue full, tick copy dropped"
                    );
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
    }

    /// The run loop: emits tick 1, 2, … on cadence until the task is
    /// dropped. Time comes from the injected clock; the loop sleeps
    /// `poll` of *wall* time between checks (a `ManualTurnClock`
    /// therefore drives ticks by `advance`, never by waiting).
    ///
    /// Deadlines anchor at the loop's start: tick `k` fires at
    /// `start + (k - 1) * interval` when `emit_on_start` (tick 1 is
    /// immediate), else at `start + k * interval`. A clock that jumps
    /// past several deadlines emits the missed ticks (each a distinct
    /// id — admission decides which ones are new work).
    pub async fn run(&self, poll: Duration) {
        let start = self.clock.now_ms();
        let interval = self.config.interval_ms.max(1);
        let offset = u64::from(!self.config.emit_on_start);
        loop {
            let emitted = self.emitted.load(Ordering::Acquire);
            // Tick number `emitted + 1` fires at start + emitted*interval
            // (emit_on_start) or start + (emitted+1)*interval (otherwise).
            let deadline = start + (emitted + offset).saturating_mul(interval);
            let now = self.clock.now_ms();
            if now < deadline {
                tokio::time::sleep(poll).await;
                continue;
            }
            let next = emitted + 1;
            self.broadcast(self.tick_event(next));
            self.emitted.store(next, Ordering::Release);
        }
    }

    /// Spawns [`TimerChannel::run`] as a task (assembly mounts the
    /// returned handle for the server's lifetime).
    pub fn spawn(self: &Arc<Self>, poll: Duration) -> tokio::task::JoinHandle<()> {
        let timer = Arc::clone(self);
        tokio::spawn(async move {
            timer.run(poll).await;
        })
    }
}

impl ChannelAdapter for TimerChannel {
    fn subscribe(&self) -> BoxStream<'static, ChannelEvent> {
        let (tx, rx) = tokio::sync::mpsc::channel(TIMER_QUEUE_CAPACITY);
        self.subscribers
            .lock()
            .expect("timer channel lock")
            .push(tx);
        futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        })
        .boxed()
    }

    fn send(
        &self,
        _out: kian_core::seams::ChannelDelivery,
    ) -> BoxFuture<'static, Result<SendReceipt, ChannelError>> {
        // Inbound-only by design: escalation is a reply on a real
        // channel through the IM policy, never a "delivery" on time.
        let channel = self.channel.clone();
        Box::pin(async move {
            Err(ChannelError::Unavailable(format!(
                "the timer channel `{channel}` is inbound-only — send on a real channel instead"
            )))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use kian_core::seams::ChannelAdapter;

    use super::{DEFAULT_TIMER_TOPIC, TimerChannel, TimerConfig};
    use crate::metrics::{ManualTurnClock, TurnClock};

    const INTERVAL: u64 = 1_000;
    const POLL: std::time::Duration = std::time::Duration::from_millis(2);

    async fn next_tick(
        stream: &mut futures::stream::BoxStream<'static, kian_core::seams::ChannelEvent>,
    ) -> kian_core::seams::ChannelEvent {
        tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("tick within the bound")
            .expect("stream still live")
    }

    /// The cadence under a manual clock: tick 1 lands exactly at
    /// `start + interval` (not before), tick 2 at `+ 2*interval` —
    /// and the event shape is the exact form.
    #[tokio::test]
    async fn timer_emits_on_cadence_under_a_manual_clock() {
        let clock = Arc::new(ManualTurnClock::new(10_000));
        let timer = Arc::new(TimerChannel::new(
            "timer",
            TimerConfig::new(INTERVAL),
            clock.clone() as Arc<dyn TurnClock>,
        ));
        let mut stream = timer.subscribe();
        let task = timer.spawn(POLL);
        // Let the run task capture its start anchor before time moves.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        // Before the deadline: nothing fires, however long we wait.
        clock.advance(INTERVAL - 1);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(timer.emitted(), 0);

        clock.advance(1); // exactly the deadline
        let tick = next_tick(&mut stream).await;
        assert_eq!(tick.channel, "timer");
        assert_eq!(tick.topic.as_deref(), Some(DEFAULT_TIMER_TOPIC));
        assert_eq!(tick.event_id, "tick-1");
        assert_eq!(tick.payload["tick"], serde_json::json!(1));
        assert_eq!(tick.ts_ms, 10_000 + INTERVAL);
        assert_eq!(timer.emitted(), 1);

        // The second tick only one full interval later.
        clock.advance(INTERVAL - 1);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(timer.emitted(), 1, "no tick before the next deadline");
        clock.advance(1);
        let tick2 = next_tick(&mut stream).await;
        assert_eq!(tick2.event_id, "tick-2");
        assert_eq!(tick2.payload["tick"], serde_json::json!(2));
        assert_eq!(timer.emitted(), 2);
        task.abort();
    }

    /// `emit_on_start` fires tick 1 immediately — once — and the next
    /// tick waits a full interval.
    #[tokio::test]
    async fn emit_on_start_fires_once_then_waits_the_interval() {
        let clock = Arc::new(ManualTurnClock::new(5_000));
        let mut config = TimerConfig::new(INTERVAL);
        config.emit_on_start = true;
        let timer = Arc::new(TimerChannel::new(
            "timer",
            config,
            clock.clone() as Arc<dyn TurnClock>,
        ));
        let mut stream = timer.subscribe();
        let task = timer.spawn(POLL);
        // The run task anchors its start before any advance.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let first = next_tick(&mut stream).await;
        assert_eq!(first.event_id, "tick-1");
        assert_eq!(first.ts_ms, 5_000, "the startup tick is immediate");
        assert_eq!(timer.emitted(), 1);

        // Idling at start + interval/2 must not produce a second
        // startup tick — exactly one fired.
        clock.advance(INTERVAL / 2);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(timer.emitted(), 1, "the startup tick fires exactly once");

        clock.advance(INTERVAL / 2 + 1);
        let second = next_tick(&mut stream).await;
        assert_eq!(second.event_id, "tick-2");
        // Fired at the first poll at/after the start + interval
        // deadline (never before it).
        assert!(second.ts_ms >= 5_000 + INTERVAL);
        task.abort();
    }

    /// The tick counter is per-process (durable dedupe lives in
    /// admission): a restarted timer re-emits the SAME tick ids from
    /// 1 — the subscription service's `event_id` idempotency is what
    /// makes the restart safe (asserted end-to-end in
    /// tests/watchdog_v2.rs).
    #[tokio::test]
    async fn a_restarted_timer_reemits_the_same_tick_ids() {
        let clock = Arc::new(ManualTurnClock::new(0));
        let timer = Arc::new(TimerChannel::new(
            "timer",
            TimerConfig::new(100),
            clock.clone() as Arc<dyn TurnClock>,
        ));
        let mut stream = timer.subscribe();
        let task = timer.spawn(POLL);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        clock.advance(250);
        let t1 = next_tick(&mut stream).await;
        let t2 = next_tick(&mut stream).await;
        assert_eq!(t1.event_id, "tick-1");
        assert_eq!(t2.event_id, "tick-2");
        task.abort();

        // "Restart": a fresh instance, same cadence, clock continues.
        let restarted = Arc::new(TimerChannel::new(
            "timer",
            TimerConfig::new(100),
            clock.clone() as Arc<dyn TurnClock>,
        ));
        let mut stream2 = restarted.subscribe();
        let task2 = restarted.spawn(POLL);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        clock.advance(100);
        let again = next_tick(&mut stream2).await;
        assert_eq!(again.event_id, "tick-1", "ids restart at 1 per process");
        task2.abort();
    }

    /// Outbound send is an honest error — the timer never delivers.
    #[tokio::test]
    async fn send_is_an_honest_unavailable_error() {
        let timer = TimerChannel::new(
            "timer",
            TimerConfig::new(INTERVAL),
            Arc::new(ManualTurnClock::new(0)) as Arc<dyn TurnClock>,
        );
        let err = timer
            .send(kian_core::seams::ChannelDelivery {
                channel: "timer".into(),
                target: serde_json::json!(null),
                content: serde_json::json!({}),
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("inbound-only"));
    }

    /// Two subscribers each see every tick exactly once.
    #[tokio::test]
    async fn every_subscriber_sees_every_tick() {
        let clock = Arc::new(ManualTurnClock::new(0));
        let timer = Arc::new(TimerChannel::new(
            "timer",
            TimerConfig::new(50),
            Arc::clone(&clock) as Arc<dyn TurnClock>,
        ));
        let mut a = timer.subscribe();
        let mut b = timer.subscribe();
        let task = timer.spawn(POLL);
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        clock.advance(50);
        assert_eq!(next_tick(&mut a).await.event_id, "tick-1");
        assert_eq!(next_tick(&mut b).await.event_id, "tick-1");
        assert_eq!(timer.dropped_full(), 0);
        task.abort();
    }
}
