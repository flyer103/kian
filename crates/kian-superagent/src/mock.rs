//! `MockChannel` — the first-class in-memory [`ChannelAdapter`]
//! (card C23, DESIGN §4.1): production code, not a `#[cfg(test)]` stub.
//!
//! It is the S4 IM stand-in and the framework's reference
//! implementation:
//!
//! - **Inbound** — [`MockChannel::send_event`] (and the convenience
//!   [`MockChannel::inject`]) fans an event out to every live
//!   subscriber through per-subscriber **bounded** queues. Each
//!   `subscribe()` call is an independent consumer; a full queue drops
//!   that subscriber's copy with an audit log line (bounded means
//!   bounded — a slow consumer must never block the injector).
//! - **Outbound** — [`ChannelAdapter::send`] captures the delivery with
//!   its [`SendReceipt`] in a capture buffer, so tests (and S4) assert
//!   exactly what a channel output policy delivered. An honest
//!   `SendReceipt` is returned only after the delivery is captured.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use kian_core::seams::{ChannelAdapter, ChannelDelivery, ChannelError, ChannelEvent, SendReceipt};
use tracing::warn;

/// Default per-subscriber queue bound.
pub const DEFAULT_QUEUE_CAPACITY: usize = 256;

/// One captured outbound delivery: what was sent plus the receipt the
/// channel handed back — the assertion surface for output policies.
#[derive(Debug, Clone, PartialEq)]
pub struct CapturedDelivery {
    pub delivery: ChannelDelivery,
    pub receipt: SendReceipt,
}

/// The in-memory channel. Cheap to construct; clone the `Arc` you share
/// with the adapter registry, not the channel itself.
pub struct MockChannel {
    channel: String,
    capacity: usize,
    subscribers: Mutex<Vec<tokio::sync::mpsc::Sender<ChannelEvent>>>,
    deliveries: Mutex<Vec<CapturedDelivery>>,
    next_message_id: AtomicU64,
    dropped_full: AtomicU64,
}

impl MockChannel {
    /// A mock channel with the default bounded queue capacity.
    pub fn new(channel: impl Into<String>) -> Self {
        Self::with_capacity(channel, DEFAULT_QUEUE_CAPACITY)
    }

    /// A mock channel with an explicit per-subscriber queue bound.
    pub fn with_capacity(channel: impl Into<String>, capacity: usize) -> Self {
        Self {
            channel: channel.into(),
            capacity: capacity.max(1),
            subscribers: Mutex::new(Vec::new()),
            deliveries: Mutex::new(Vec::new()),
            next_message_id: AtomicU64::new(1),
            dropped_full: AtomicU64::new(0),
        }
    }

    /// The channel name this adapter reports on its events.
    pub fn channel_name(&self) -> &str {
        &self.channel
    }

    /// Injects a fully-formed event: fans out to every live subscriber
    /// immediately (bounded `try_send`; a full queue drops that
    /// subscriber's copy with an audit line). Returns the event id.
    pub fn send_event(&self, event: ChannelEvent) -> String {
        let event_id = event.event_id.clone();
        let mut subscribers = self.subscribers.lock().expect("mock channel lock");
        subscribers.retain(|tx| !tx.is_closed());
        for tx in subscribers.iter() {
            match tx.try_send(event.clone()) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    self.dropped_full.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        target: "kian::mock_channel",
                        channel = %self.channel,
                        event_id = %event_id,
                        "mock channel audit: subscriber queue full, event copy dropped"
                    );
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    // Pruned on the next send; nothing to do.
                }
            }
        }
        event_id
    }

    /// Builds an event on this channel (fresh uuid `event_id`, current
    /// timestamp, optional routing topic, verbatim payload) and injects
    /// it via [`MockChannel::send_event`]. Returns the event as sent —
    /// the caller keeps the id for idempotency assertions.
    pub fn inject(&self, topic: Option<&str>, payload: serde_json::Value) -> ChannelEvent {
        let event = ChannelEvent {
            channel: self.channel.clone(),
            event_id: uuid::Uuid::new_v4().to_string(),
            topic: topic.map(str::to_string),
            payload,
            ts_ms: now_ms(),
        };
        self.send_event(event.clone());
        event
    }

    /// The captured outbound deliveries, oldest first.
    pub fn deliveries(&self) -> Vec<CapturedDelivery> {
        self.deliveries.lock().expect("mock channel lock").clone()
    }

    /// How many inbound copies were dropped because a subscriber queue
    /// was full (the bounded-queue observability point).
    pub fn dropped_full(&self) -> u64 {
        self.dropped_full.load(Ordering::Relaxed)
    }

    /// Number of live subscribers (inspection/tests).
    pub fn subscriber_count(&self) -> usize {
        let mut subs = self.subscribers.lock().expect("mock channel lock");
        subs.retain(|tx| !tx.is_closed());
        subs.len()
    }
}

impl ChannelAdapter for MockChannel {
    fn subscribe(&self) -> BoxStream<'static, ChannelEvent> {
        let (tx, rx) = tokio::sync::mpsc::channel(self.capacity);
        self.subscribers.lock().expect("mock channel lock").push(tx);
        // mpsc::Receiver is not itself a `Stream`; unfold over `recv`
        // (no extra dependency, exactly-once per subscriber).
        futures::stream::unfold(rx, |mut rx| async move {
            rx.recv().await.map(|event| (event, rx))
        })
        .boxed()
    }

    fn send(&self, out: ChannelDelivery) -> BoxFuture<'static, Result<SendReceipt, ChannelError>> {
        let receipt = SendReceipt {
            message_id: format!(
                "mock-{}-{}",
                self.channel,
                self.next_message_id.fetch_add(1, Ordering::Relaxed)
            ),
            ts_ms: now_ms(),
        };
        self.deliveries
            .lock()
            .expect("mock channel lock")
            .push(CapturedDelivery {
                delivery: out,
                receipt: receipt.clone(),
            });
        Box::pin(async move { Ok(receipt) })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_QUEUE_CAPACITY, MockChannel};
    use futures::StreamExt;
    use kian_core::seams::{ChannelAdapter, ChannelDelivery};
    use serde_json::json;

    fn event(event_id: &str, topic: Option<&str>) -> kian_core::seams::ChannelEvent {
        kian_core::seams::ChannelEvent {
            channel: "mock-im".into(),
            event_id: event_id.into(),
            topic: topic.map(str::to_string),
            payload: json!({"text": "ping"}),
            ts_ms: 42,
        }
    }

    #[tokio::test]
    async fn send_receive_round_trip_with_receipt() {
        let channel = MockChannel::new("mock-im");
        assert_eq!(channel.channel_name(), "mock-im");

        // Inbound: two independent subscribers each see the event
        // exactly once, byte-identical.
        let mut s1 = channel.subscribe();
        let mut s2 = channel.subscribe();
        assert_eq!(channel.subscriber_count(), 2);
        let injected = channel.inject(Some("ci"), json!({"text": "build ok"}));
        assert_eq!(s1.next().await, Some(injected.clone()));
        assert_eq!(s2.next().await, Some(injected));

        // Outbound: send captures the delivery and returns a receipt.
        let delivery = ChannelDelivery {
            channel: "mock-im".into(),
            target: json!({"chat_id": 7}),
            content: json!({"text": "done"}),
        };
        let receipt = channel.send(delivery.clone()).await.expect("send ok");
        assert!(!receipt.message_id.is_empty());
        assert!(receipt.ts_ms >= 42);
        assert_eq!(channel.deliveries().len(), 1);
        assert_eq!(channel.deliveries()[0].delivery, delivery);
        assert_eq!(channel.deliveries()[0].receipt, receipt);

        // The receipt is honest about the capture buffer: the second
        // send gets a fresh message id.
        let receipt2 = channel.send(delivery.clone()).await.expect("send ok");
        assert_ne!(receipt.message_id, receipt2.message_id);
        assert_eq!(channel.deliveries().len(), 2);
        assert_eq!(channel.dropped_full(), 0);
        assert_eq!(DEFAULT_QUEUE_CAPACITY, 256);
    }

    #[tokio::test]
    async fn bounded_queues_drop_the_slow_subscriber_not_the_injector() {
        let channel = super::MockChannel::with_capacity("mock-im", 1);
        let _slow = channel.subscribe(); // never polled — its queue fills
        let mut fast = channel.subscribe();
        assert_eq!(channel.subscriber_count(), 2);

        channel.send_event(event("e1", None)); // both buffer e1
        assert_eq!(fast.next().await.unwrap().event_id, "e1"); // fast drained
        // The slow subscriber's queue still holds e1: this copy is
        // dropped for it, delivered for the fast one. Injection never
        // blocked on the slow consumer.
        channel.send_event(event("e2", None));
        assert_eq!(fast.next().await.unwrap().event_id, "e2");
        assert_eq!(channel.dropped_full(), 1);
    }

    #[tokio::test]
    async fn closed_subscribers_are_pruned_on_send() {
        let channel = super::MockChannel::new("mock-im");
        {
            let s = channel.subscribe();
            drop(s);
        }
        channel.send_event(event("e1", None));
        // The closed subscriber was pruned; none remain.
        assert_eq!(channel.subscriber_count(), 0);
    }
}
