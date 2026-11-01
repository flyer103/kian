//! Channel-event provenance wrapper (card C23).
//!
//! When a subscription rule matches, the event is admitted as a *normal
//! message payload* whose `content` embeds the wrapper
//! `{"kian":{"type":"channel_event","channel":…,"topic":…,"event_id":…,
//! "raw":…}}`. Because the wrapper rides inside `content` — verbatim
//! passed JSON, never re-formatted afterward (constraint 3) — the frozen
//! C4 message-payload contract is untouched: in the event stream the
//! input appears as the ordinary
//! `{"role":"user","content":{"kian":{…}}}` message the consumption
//! write already produces. The Router's model sees honest provenance;
//! the passthrough registry reads it back during a rebuild (constraint
//! 1: the mapping is derived from events, never stored outside them).

use kian_core::seams::ChannelEvent;
use serde_json::{Value, json};

/// The discriminator inside the wrapper (`kian.type`).
pub const CHANNEL_EVENT_TYPE: &str = "channel_event";

/// The parsed form of a channel-event wrapper found inside a message
/// payload's `content` — what the registry rebuild scans for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelEventMarker {
    pub channel: String,
    pub topic: Option<String>,
    pub event_id: String,
    pub raw: Value,
}

/// Builds the admitted input payload for a matched channel event: the
/// wrapper object that becomes the message `content` (the
/// `{"role":"user","content":…}` shaping happens at the normal
/// consumption write — this payload *is* the content, passed verbatim).
pub fn channel_input_payload(event: &ChannelEvent) -> Value {
    wrap_channel_event(event)
}

/// The wrapper itself: `{"kian":{"type":"channel_event",…}}`.
pub fn wrap_channel_event(event: &ChannelEvent) -> Value {
    json!({
        "kian": {
            "type": CHANNEL_EVENT_TYPE,
            "channel": event.channel,
            "topic": event.topic,
            "event_id": event.event_id,
            "raw": event.payload,
        }
    })
}

/// Parses a channel-event wrapper out of a message payload's `content`
/// (the inverse of [`wrap_channel_event`]). Returns `None` for any
/// content that is not a channel-event wrapper — non-channel inputs are
/// the common case and are never misread.
pub fn parse_channel_marker(content: &Value) -> Option<ChannelEventMarker> {
    let kian = content.get("kian")?;
    if kian.get("type").and_then(Value::as_str) != Some(CHANNEL_EVENT_TYPE) {
        return None;
    }
    Some(ChannelEventMarker {
        channel: kian.get("channel")?.as_str()?.to_string(),
        topic: kian
            .get("topic")
            .and_then(Value::as_str)
            .map(str::to_string),
        event_id: kian.get("event_id")?.as_str()?.to_string(),
        raw: kian.get("raw").cloned().unwrap_or(Value::Null),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        CHANNEL_EVENT_TYPE, ChannelEventMarker, channel_input_payload, parse_channel_marker,
        wrap_channel_event,
    };
    use kian_core::seams::ChannelEvent;
    use serde_json::json;

    fn event() -> ChannelEvent {
        ChannelEvent {
            channel: "mock-im".into(),
            event_id: "evt-42".into(),
            topic: Some("ci".into()),
            payload: json!({"text": "@kian deploy"}),
            ts_ms: 1_762_000_000_000,
        }
    }

    #[test]
    fn wrapper_rides_inside_content_and_round_trips() {
        let payload = channel_input_payload(&event());
        assert_eq!(
            payload,
            json!({
                "kian": {
                    "type": "channel_event",
                    "channel": "mock-im",
                    "topic": "ci",
                    "event_id": "evt-42",
                    "raw": {"text": "@kian deploy"},
                }
            })
        );
        // The inverse read recovers every field verbatim.
        let marker = parse_channel_marker(&payload).expect("marker parses");
        assert_eq!(
            marker,
            ChannelEventMarker {
                channel: "mock-im".into(),
                topic: Some("ci".into()),
                event_id: "evt-42".into(),
                raw: json!({"text": "@kian deploy"}),
            }
        );
    }

    #[test]
    fn wrapper_is_rebuild_stable_across_serialization() {
        // Re-serializing the wrapper must not change it (constraint 3 in
        // spirit: the content is never reformatted).
        let payload = wrap_channel_event(&event());
        let text = serde_json::to_string(&payload).unwrap();
        let back: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(back, payload);
    }

    #[test]
    fn non_channel_content_is_never_misread() {
        assert!(parse_channel_marker(&json!("plain text")).is_none());
        assert!(parse_channel_marker(&json!({"text": "hi"})).is_none());
        assert!(parse_channel_marker(&serde_json::Value::Null).is_none());
        // A kian object of a different type is not a channel event.
        assert!(parse_channel_marker(&json!({"kian": {"type": "other"}})).is_none());
    }

    #[test]
    fn topicless_events_wrap_with_null_topic() {
        let mut ev = event();
        ev.topic = None;
        let payload = wrap_channel_event(&ev);
        assert_eq!(payload["kian"]["topic"], serde_json::Value::Null);
        let marker = parse_channel_marker(&payload).unwrap();
        assert_eq!(marker.topic, None);
        assert_eq!(marker.channel, "mock-im");
        assert_eq!(CHANNEL_EVENT_TYPE, "channel_event");
    }
}
