//! The recall toolpack — the Chain of Recall.
//!
//! Three read-only [`ToolProvider`] tools mounted on the Router and
//! (opt-) worker templates:
//!
//! - `recall_event(event_id)` — surfaces a spilled channel raw: the
//!   Inline/Spill-shaped view plus next-hop ids;
//! - `recall_neighbors(event_id)` — the locality view: adjacent channel
//!   events on the same topic within a seq window, each entry itself
//!   Inline-shaped and capped;
//! - `recall_thread(thread_id)` — the thread's task, its status (from
//!   the worker's `thread/completed`) and the final summary, with the
//!   worker session as the next hop.
//!
//! Every output is itself Inline/Spill-shaped with the shaping
//! vocabulary (`first_chars` excerpts, `neighbor_fields` association
//! ids) so multi-hop recall composes: each hop returns summaries and
//! ids, never an unbounded raw — the verbatim-in-one-output bound is
//! the toolpack's own `output_cap_bytes` ([`RecallToolConfig`]),
//! independent of the admission-time `inline_cap_bytes`.
//!
//! Results land as ordinary `tool/result` events appended at the tail
//! by the unmodified built-in loop (log-before-execute on the call,
//! append-after-completion on the result): persisted before use,
//! byte-stable thereafter, never reordering prior context
//! (constraint 3). Reads go exclusively through
//! [`RecallPort`](crate::recall::RecallPort) — the bundle's read port;
//! this module names no store, no hold, no session machinery (the
//! crate iron rule holds).
//!
//! Untrusted hygiene: a surfaced raw re-wraps through
//! [`raw_view`](crate::provenance::raw_view) keyed off the sibling
//! admitted content's bit, so the untrusted marker survives every hop
//! of a recall chain.
//!
//! Locality note: the startup spill index covers only spills that
//! existed at rebuild time; a raw admitted later is still in the
//! stream (the sole truth), so [`RecallTools::locate`] falls back to a
//! scan over the Router stream and the workers it names — the index is
//! the fast path, never the authority (constraint 1).

use std::sync::Arc;

use futures::future::BoxFuture;
use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_core::seams::{ToolCall, ToolError, ToolProvider, ToolResult, ToolSchema};
use serde_json::{Value, json};

use crate::provenance::{ShapingConfig, channel_event_untrusted, parse_channel_marker, raw_view};
use crate::recall::RecallPort;
use crate::registry::worker_sessions;
use crate::worker::completion_summary;

/// `recall_event(event_id)` — the spilled-raw view.
pub const RECALL_EVENT_TOOL: &str = "recall_event";

/// `recall_neighbors(event_id)` — the locality view.
pub const RECALL_NEIGHBORS_TOOL: &str = "recall_neighbors";

/// `recall_thread(thread_id)` — the thread view.
pub const RECALL_THREAD_TOOL: &str = "recall_thread";

/// The toolpack's names in stable (sorted) order — tool lists and
/// schema serializations are prompt-prefix content (constraint 3's
/// gate: the same config always yields the same bytes).
pub const RECALL_TOOLS: [&str; 3] = [RECALL_EVENT_TOOL, RECALL_NEIGHBORS_TOOL, RECALL_THREAD_TOOL];

/// Default verbatim-in-one-output bound (bytes of the serialized
/// value). Above it a surfaced value becomes the Inline view — no
/// unbounded raw re-injection, by default.
pub const DEFAULT_OUTPUT_CAP_BYTES: usize = 8_192;

/// Default neighbor window: channel events within `window` seq positions
/// on either side of the anchor are considered neighbors.
pub const DEFAULT_NEIGHBOR_WINDOW: u64 = 3;

/// The toolpack's caps. The excerpts and association ids reuse
/// the [`ShapingConfig`]; the verbatim bound is this config's own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecallToolConfig {
    /// Maximum serialized bytes of a single value surfaced verbatim in
    /// one tool output. Above it the output carries the Inline view
    /// (excerpt + continuation handle) so the next hop stays bounded.
    pub output_cap_bytes: usize,
    /// Channel events within this many seq positions on either side of
    /// the anchor (same topic) form the locality view.
    pub neighbor_window: u64,
}

impl Default for RecallToolConfig {
    fn default() -> Self {
        Self {
            output_cap_bytes: DEFAULT_OUTPUT_CAP_BYTES,
            neighbor_window: DEFAULT_NEIGHBOR_WINDOW,
        }
    }
}

/// The recall toolpack as one [`ToolProvider`]: reads through the
/// shared [`RecallPort`], shapes every output with the vocabulary
/// under its own caps, and mutates nothing. Clone shares the port.
pub struct RecallTools {
    port: Arc<dyn RecallPort>,
    /// ADR-040 binding: threads/spills resolve the live resident session
    /// at tool-call time.
    router: Arc<crate::binding::ResidentBinding>,
    shaping: ShapingConfig,
    config: RecallToolConfig,
}

impl RecallTools {
    /// A toolpack over `port` (assembly-implemented), anchored at the
    /// resident Router `router` (threads list off its stream), with the
    /// shaping vocabulary and the toolpack caps.
    #[must_use]
    pub fn new(
        port: Arc<dyn RecallPort>,
        router: Arc<crate::binding::ResidentBinding>,
        shaping: ShapingConfig,
        config: RecallToolConfig,
    ) -> Self {
        Self {
            port,
            router,
            shaping,
            config,
        }
    }

    /// The tool names this provider exposes (the stable sorted order).
    #[must_use]
    pub fn tool_names(&self) -> Vec<String> {
        RECALL_TOOLS.iter().map(|t| (*t).to_string()).collect()
    }

    fn port_error(e: crate::recall::RecallPortError) -> ToolError {
        ToolError::Execution(format!("recall read failed: {e}"))
    }

    /// One session's whole committed stream through the read port
    /// (the window form is the port's only read; `0..u64::MAX` is the
    /// "everything" window the store clamps itself).
    async fn read_all(&self, session: SessionId) -> Result<Vec<Event>, ToolError> {
        self.port
            .read_events(session, 0, u64::MAX)
            .await
            .map_err(Self::port_error)
    }

    /// Where the spilled raw of `event_id` lives: the spill index's
    /// `(session, seq)` when the startup rebuild saw it, else a scan
    /// over the Router stream and the workers it names (spills admitted
    /// after the rebuild are still in the stream — the index is a
    /// cache, never the authority). `None` only when no
    /// `input/spilled` event anywhere carries the id.
    async fn locate(&self, event_id: &str) -> Result<Option<(SessionId, u64)>, ToolError> {
        if let Some(hit) = self
            .port
            .spill_location(event_id)
            .await
            .map_err(Self::port_error)?
        {
            return Ok(Some(hit));
        }
        let router_events = self.read_all(self.router.load()).await?;
        if let Some(hit) = find_spill(&router_events, event_id) {
            return Ok(Some(hit));
        }
        for worker in worker_sessions(&router_events) {
            let events = self.read_all(worker).await?;
            if let Some(hit) = find_spill(&events, event_id) {
                return Ok(Some(hit));
            }
        }
        Ok(None)
    }

    /// The spilled event's own bytes read back through the port.
    async fn spilled_event(&self, location: (SessionId, u64)) -> Result<Option<Event>, ToolError> {
        let events = self
            .port
            .read_events(location.0, location.1, location.1 + 1)
            .await
            .map_err(Self::port_error)?;
        Ok(events
            .into_iter()
            .find(|ev| ev.kind == EventKind::InputSpilled))
    }

    /// The untrusted bit of the admitted content sibling of a spilled
    /// raw: `event_id` is the join key, the sibling sits at most
    /// one seq earlier in the same fenced consumption batch.
    async fn sibling_untrusted(
        &self,
        location: (SessionId, u64),
        event_id: &str,
    ) -> Result<bool, ToolError> {
        let since = location.1.saturating_sub(2);
        let events = self
            .port
            .read_events(location.0, since, location.1)
            .await
            .map_err(Self::port_error)?;
        Ok(events
            .iter()
            .rev()
            .find(|ev| {
                ev.kind == EventKind::InputAdmitted
                    && ev
                        .bytes
                        .pointer("/content/kian/event_id")
                        .and_then(Value::as_str)
                        == Some(event_id)
            })
            .map(|ev| channel_event_untrusted(&ev.bytes["content"]))
            .unwrap_or(false))
    }

    async fn recall_event(&self, call: ToolCall) -> Result<ToolResult, ToolError> {
        let event_id = str_arg(&call.arguments, "event_id", RECALL_EVENT_TOOL)?;
        let Some(location) = self.locate(event_id).await? else {
            return Err(ToolError::Execution(format!(
                "recall_event: unknown event id `{event_id}` — no spilled raw exists for it"
            )));
        };
        let Some(spilled) = self.spilled_event(location).await? else {
            return Err(ToolError::Execution(format!(
                "recall_event: indexed location for `{event_id}` holds no `input/spilled` event \
                 (the index is stale — restart rebuilds it)"
            )));
        };
        let raw = spilled.bytes.get("raw").cloned().unwrap_or(Value::Null);
        let channel = spilled
            .bytes
            .get("channel")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let topic = spilled
            .bytes
            .get("topic")
            .and_then(Value::as_str)
            .map(str::to_string);
        let untrusted = self.sibling_untrusted(location, event_id).await?;
        let view = raw_view(&raw, untrusted);

        let mut next_hops = json!({
            "neighbors": {"tool": RECALL_NEIGHBORS_TOOL, "event_id": event_id},
        });
        if let Some(topic) = &topic {
            next_hops["topic"] = json!(topic);
        }
        let body = if serialized_len(&view) <= self.config.output_cap_bytes {
            json!({
                "event_id": event_id,
                "channel": channel,
                "topic": topic,
                "raw": view,
                "next_hops": next_hops,
            })
        } else {
            json!({
                "event_id": event_id,
                "channel": channel,
                "topic": topic,
                "inline": {
                    "excerpt": excerpt_of(&view, self.shaping.first_chars),
                    "neighbors": association_ids(
                        &raw,
                        topic.as_deref(),
                        event_id,
                        &self.shaping.neighbor_fields,
                    ),
                },
                "spill": true,
                "continuation": {"event_id": event_id},
                "next_hops": next_hops,
            })
        };
        Ok(ToolResult {
            call_id: call.id,
            output: body,
            is_error: false,
            redactions: Vec::new(),
        })
    }

    async fn recall_neighbors(&self, call: ToolCall) -> Result<ToolResult, ToolError> {
        let event_id = str_arg(&call.arguments, "event_id", RECALL_NEIGHBORS_TOOL)?;
        let Some(location) = self.locate(event_id).await? else {
            return Err(ToolError::Execution(format!(
                "recall_neighbors: unknown event id `{event_id}` — no spilled raw exists for it"
            )));
        };
        let Some(spilled) = self.spilled_event(location).await? else {
            return Err(ToolError::Execution(format!(
                "recall_neighbors: indexed location for `{event_id}` holds no `input/spilled` \
                 event (the index is stale — restart rebuilds it)"
            )));
        };
        let topic = spilled
            .bytes
            .get("topic")
            .and_then(Value::as_str)
            .map(str::to_string);
        let window = self.config.neighbor_window;
        let since = location.1.saturating_sub(window);
        let until = location.1.saturating_add(window + 1);
        let events = self
            .port
            .read_events(location.0, since, until)
            .await
            .map_err(Self::port_error)?;

        // Spilled siblings inside the window: over-cap neighbors keep
        // their raws here (the admitted content carries only the
        // Inline view).
        let mut spilled_raws = serde_json::Map::new();
        for ev in &events {
            if ev.kind != EventKind::InputSpilled {
                continue;
            }
            if let Some(id) = ev.bytes.get("event_id").and_then(Value::as_str) {
                spilled_raws.insert(
                    id.to_string(),
                    ev.bytes.get("raw").cloned().unwrap_or(Value::Null),
                );
            }
        }

        // The anchor's own sibling admission is excluded by event id.
        let mut neighbors = Vec::new();
        for ev in &events {
            if ev.kind != EventKind::InputAdmitted {
                continue;
            }
            let Some(content) = ev.bytes.get("content") else {
                continue;
            };
            let Some(marker) = parse_channel_marker(content) else {
                continue;
            };
            if marker.event_id == event_id || marker.topic != topic {
                continue;
            }
            let raw = spilled_raws
                .get(&marker.event_id)
                .cloned()
                .unwrap_or_else(|| marker.raw.clone());
            let view = raw_view(&raw, channel_event_untrusted(content));
            neighbors.push(json!({
                "event_id": marker.event_id,
                "channel": marker.channel,
                "topic": marker.topic,
                "seq": ev.seq,
                "view": shaped_value(
                    &view,
                    json!({"event_id": marker.event_id}),
                    &self.shaping,
                    self.config.output_cap_bytes,
                ),
            }));
        }

        let ids: Vec<&str> = neighbors
            .iter()
            .map(|n| n["event_id"].as_str().unwrap_or_default())
            .collect();
        let body = json!({
            "event_id": event_id,
            "topic": topic,
            "window": window,
            "neighbors": neighbors,
            "next_hops": {"tool": RECALL_EVENT_TOOL, "event_ids": ids},
        });
        Ok(ToolResult {
            call_id: call.id,
            output: body,
            is_error: false,
            redactions: Vec::new(),
        })
    }

    async fn recall_thread(&self, call: ToolCall) -> Result<ToolResult, ToolError> {
        let thread_id = str_arg(&call.arguments, "thread_id", RECALL_THREAD_TOOL)?;
        let threads = self
            .port
            .list_threads(self.router.load())
            .await
            .map_err(Self::port_error)?;
        let Some(worker) = threads
            .iter()
            .find(|(tid, _)| tid.as_str() == thread_id)
            .map(|(_, worker)| *worker)
        else {
            return Err(ToolError::Execution(format!(
                "recall_thread: unknown thread id `{thread_id}` — the Router stream carries no \
                 `thread/started` naming it"
            )));
        };

        // The task is the Router-side thread/started payload's task
        // message content (the delegation wrapper).
        let router_events = self.read_all(self.router.load()).await?;
        let task = router_events
            .iter()
            .find(|ev| {
                ev.kind == EventKind::ThreadStarted
                    && ev.bytes.get("thread_id").and_then(Value::as_str) == Some(thread_id)
            })
            .and_then(|ev| ev.bytes.get("task"))
            .and_then(|task| task.get("content"))
            .cloned()
            .unwrap_or(Value::Null);

        // Status and final summary from the worker's own stream: the
        // thread/completed event when it exists, else the honest
        // outstanding projection (last assistant message, if any).
        let worker_events = self.read_all(worker).await?;
        let completed = worker_events.iter().find(|ev| {
            ev.kind == EventKind::ThreadCompleted
                && ev.bytes.get("thread_id").and_then(Value::as_str) == Some(thread_id)
        });
        let (status, summary) = match completed {
            Some(ev) => (
                ev.bytes
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string(),
                ev.bytes.get("summary").cloned().unwrap_or(Value::Null),
            ),
            None => (
                "outstanding".to_string(),
                completion_summary(&worker_events, "no assistant message yet"),
            ),
        };

        let continuation = json!({"thread_id": thread_id});
        let body = json!({
            "thread_id": thread_id,
            "status": status,
            "task": shaped_value(&task, continuation.clone(), &self.shaping, self.config.output_cap_bytes),
            "summary": shaped_value(&summary, continuation, &self.shaping, self.config.output_cap_bytes),
            "next_hops": {"worker_session": worker.0.to_string()},
        });
        Ok(ToolResult {
            call_id: call.id,
            output: body,
            is_error: false,
            redactions: Vec::new(),
        })
    }
}

impl ToolProvider for RecallTools {
    fn schemas(&self) -> Vec<ToolSchema> {
        vec![
            ToolSchema {
                name: RECALL_EVENT_TOOL.to_string(),
                description: "Recall one spilled channel event: its Inline/Spill-shaped raw \
                    (verbatim up to the output cap) plus next-hop ids (locality, topic)."
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "event_id": {
                            "type": "string",
                            "description": "The channel event id to recall (the continuation handle)"
                        }
                    },
                    "required": ["event_id"],
                }),
            },
            ToolSchema {
                name: RECALL_NEIGHBORS_TOOL.to_string(),
                description: "Recall the locality view of one channel event: adjacent channel \
                    events on the same topic within a window, each Inline-shaped and capped, \
                    with their event ids as next hops."
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "event_id": {
                            "type": "string",
                            "description": "The anchor channel event id"
                        }
                    },
                    "required": ["event_id"],
                }),
            },
            ToolSchema {
                name: RECALL_THREAD_TOOL.to_string(),
                description: "Recall one delegated thread: its task, status (completed | failed \
                    | interrupted | outstanding) and final summary, with the worker session as \
                    the next hop."
                    .into(),
                parameters: json!({
                    "type": "object",
                    "properties": {
                        "thread_id": {
                            "type": "string",
                            "description": "The thread id returned by spawn_worker"
                        }
                    },
                    "required": ["thread_id"],
                }),
            },
        ]
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        let this = self.clone();
        Box::pin(async move {
            match call.name.as_str() {
                s if s == RECALL_EVENT_TOOL => this.recall_event(call).await,
                s if s == RECALL_NEIGHBORS_TOOL => this.recall_neighbors(call).await,
                s if s == RECALL_THREAD_TOOL => this.recall_thread(call).await,
                other => Err(ToolError::InvalidCall(format!(
                    "unknown recall tool `{other}`"
                ))),
            }
        })
    }
}

/// `ToolProvider::invoke` hands out boxed futures that must not borrow
/// the provider; the clone shares the same read port.
impl Clone for RecallTools {
    fn clone(&self) -> Self {
        Self {
            port: Arc::clone(&self.port),
            router: Arc::clone(&self.router),
            shaping: self.shaping.clone(),
            config: self.config.clone(),
        }
    }
}

/// A required string tool argument.
fn str_arg<'a>(args: &'a Value, key: &str, tool: &str) -> Result<&'a str, ToolError> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::InvalidCall(format!("{tool} requires a string `{key}`")))
}

/// The first `input/spilled` event carrying `event_id`, as a
/// `(session, seq)` location (first write wins, matching the spill
/// index's rebuild rule).
fn find_spill(events: &[Event], event_id: &str) -> Option<(SessionId, u64)> {
    events
        .iter()
        .find(|ev| {
            ev.kind == EventKind::InputSpilled
                && ev.bytes.get("event_id").and_then(Value::as_str) == Some(event_id)
        })
        .map(|ev| (ev.session, ev.seq))
}

/// Serialized length of a JSON value; `usize::MAX` if serialization
/// somehow fails (never for a `serde_json::Value`).
fn serialized_len(value: &Value) -> usize {
    serde_json::to_string(value)
        .map(|s| s.len())
        .unwrap_or(usize::MAX)
}

/// A strictly-shorter excerpt of the serialized value (the rule:
/// shaping must never smuggle the complete over-cap payload back into
/// a view, even for a tiny cap or a large `first_chars`).
fn excerpt_of(value: &Value, first_chars: usize) -> String {
    let serialized =
        serde_json::to_string(value).expect("serializing a serde_json::Value cannot fail");
    let len = serialized.chars().count();
    serialized
        .chars()
        .take(first_chars.min(len.saturating_sub(1)))
        .collect()
}

/// The association ids of a payload: canonical `topic`/`event_id` plus
/// any configured neighbor fields that carry scalar values — the same
/// rule the shaper applies (v1: payload-field-driven only).
fn association_ids(
    payload: &Value,
    topic: Option<&str>,
    event_id: &str,
    neighbor_fields: &[String],
) -> Value {
    let mut neighbors = serde_json::Map::new();
    neighbors.insert("topic".into(), json!(topic));
    neighbors.insert("event_id".into(), json!(event_id));
    if let Some(obj) = payload.as_object() {
        for field in neighbor_fields {
            if field == "topic" || field == "event_id" {
                continue;
            }
            if let Some(value) = obj.get(field)
                && (value.is_string() || value.is_number())
            {
                neighbors.insert(field.clone(), value.clone());
            }
        }
    }
    Value::Object(neighbors)
}

/// One surfaced value, Inline/Spill-shaped with the vocabulary
/// under the toolpack's own verbatim bound: verbatim at or below the
/// cap; above it the Inline view (excerpt + continuation handle), so
/// every hop of a recall chain stays bounded and composable.
fn shaped_value(
    value: &Value,
    continuation: Value,
    shaping: &ShapingConfig,
    output_cap_bytes: usize,
) -> Value {
    if serialized_len(value) <= output_cap_bytes {
        return value.clone();
    }
    json!({
        "inline": {"excerpt": excerpt_of(value, shaping.first_chars)},
        "spill": true,
        "continuation": continuation,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        RECALL_EVENT_TOOL, RECALL_NEIGHBORS_TOOL, RECALL_THREAD_TOOL, RECALL_TOOLS,
        RecallToolConfig, RecallTools, association_ids, excerpt_of, serialized_len, shaped_value,
    };
    use kian_core::model::SessionId;
    use kian_core::seams::{ToolCall, ToolProvider};
    use serde_json::json;

    /// The tool names are the sorted stable list — the schemas() order
    /// and the template mount order both derive from it.
    #[test]
    fn tool_names_are_the_sorted_stable_list() {
        assert_eq!(
            RECALL_TOOLS,
            [RECALL_EVENT_TOOL, RECALL_NEIGHBORS_TOOL, RECALL_THREAD_TOOL]
        );
        let mut sorted = RECALL_TOOLS;
        sorted.sort_unstable();
        assert_eq!(sorted, RECALL_TOOLS);
    }

    /// An excerpt is always strictly shorter than the serialized
    /// value — even with a huge first_chars (the anti-smuggle rule).
    #[test]
    fn excerpt_is_strictly_shorter_than_the_serialized_value() {
        let value = json!({"text": " abcdefghijklmnopqrstuvwxyz-0123456789"});
        let serialized = serde_json::to_string(&value).unwrap();
        for first_chars in [0, 1, 5, 1000] {
            let excerpt = excerpt_of(&value, first_chars);
            assert!(
                excerpt.chars().count() < serialized.chars().count(),
                "first_chars={first_chars}: excerpt must be strictly shorter"
            );
        }
    }

    /// The shaped view: verbatim at or below the cap; the Inline view
    /// (excerpt + spill + continuation) above it — never the raw.
    #[test]
    fn shaped_value_is_verbatim_under_cap_and_inline_above_it() {
        let shaping = crate::provenance::ShapingConfig {
            inline_cap_bytes: None,
            first_chars: 8,
            neighbor_fields: vec![],
            vision: false,
        };
        let small = json!({"text": "tiny"});
        assert_eq!(
            shaped_value(&small, json!({"event_id": "e"}), &shaping, 64),
            small
        );

        let big = json!({"text": "0123456789abcdefghijklmnopqrstuvwxyz-VERY_LONG_RAW"});
        let shaped = shaped_value(&big, json!({"event_id": "e"}), &shaping, 16);
        assert_eq!(shaped["spill"], json!(true));
        assert_eq!(shaped["continuation"], json!({"event_id": "e"}));
        let excerpt = shaped["inline"]["excerpt"].as_str().unwrap();
        let serialized = serde_json::to_string(&big).unwrap();
        assert!(serialized.starts_with(excerpt));
        assert!(serialized.len() > excerpt.len());
        assert!(
            !serde_json::to_string(&shaped)
                .unwrap()
                .contains("VERY_LONG_RAW"),
            "the over-cap value must not ride verbatim"
        );
    }

    /// Association ids: canonical topic/event_id plus configured
    /// scalar fields only (nested values and canonical-name collisions
    /// never ride).
    #[test]
    fn association_ids_carry_canonical_and_scalar_neighbor_fields_only() {
        let payload = json!({
            "reply_to": "msg-7",
            "nested": {"not": "an id"},
            "event_id": "payload-controlled-overwrite",
        });
        let ids = association_ids(
            &payload,
            Some("ci"),
            "evt-42",
            &["reply_to".into(), "nested".into(), "event_id".into()],
        );
        assert_eq!(
            ids,
            json!({"topic": "ci", "event_id": "evt-42", "reply_to": "msg-7"})
        );
    }

    /// serialized_len agrees with serde's compact form.
    #[test]
    fn serialized_len_matches_the_compact_form() {
        let value = json!({"b": 1, "a": 2});
        assert_eq!(serialized_len(&value), 13);
    }

    /// The default caps are finite (no unbounded re-injection by
    /// default) and documented constants.
    #[test]
    fn default_caps_are_finite() {
        let config = RecallToolConfig::default();
        assert_eq!(config.output_cap_bytes, super::DEFAULT_OUTPUT_CAP_BYTES);
        assert!(config.output_cap_bytes > 0);
        assert_eq!(config.neighbor_window, super::DEFAULT_NEIGHBOR_WINDOW);
        assert!(config.neighbor_window > 0);
    }

    /// A read port over nothing: every location lookup misses, every
    /// thread list is empty — the honest-error fixture.
    struct EmptyPort;

    impl crate::recall::RecallPort for EmptyPort {
        fn read_events(
            &self,
            _session: SessionId,
            _since_seq: u64,
            _until_seq: u64,
        ) -> futures::future::BoxFuture<
            'static,
            Result<Vec<kian_core::events::Event>, crate::recall::RecallPortError>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn list_threads(
            &self,
            _parent: SessionId,
        ) -> futures::future::BoxFuture<
            'static,
            Result<Vec<(String, SessionId)>, crate::recall::RecallPortError>,
        > {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn spill_location(
            &self,
            _event_id: &str,
        ) -> futures::future::BoxFuture<
            'static,
            Result<Option<(SessionId, u64)>, crate::recall::RecallPortError>,
        > {
            Box::pin(async { Ok(None) })
        }
    }

    fn tools() -> RecallTools {
        RecallTools::new(
            std::sync::Arc::new(EmptyPort),
            crate::binding::ResidentBinding::new(SessionId::new()),
            crate::provenance::ShapingConfig::default(),
            RecallToolConfig::default(),
        )
    }

    /// Unknown ids are honest errors (never silence, never a fabricated
    /// empty hit); a name outside the toolpack is an invalid call; a
    /// missing required argument is an invalid call.
    #[tokio::test]
    async fn unknown_ids_are_honest_errors_and_unknown_names_rejected() {
        use kian_core::seams::ToolError;

        let tools = tools();
        let call = |name: &str, args: serde_json::Value| ToolCall {
            id: "c1".into(),
            name: name.into(),
            arguments: args,
        };

        let err = tools
            .invoke(call("recall_nonsense", json!({})))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidCall(_)), "got {err:?}");

        let err = tools
            .invoke(call(RECALL_EVENT_TOOL, json!({})))
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::InvalidCall(_)), "got {err:?}");

        let err = tools
            .invoke(call(RECALL_EVENT_TOOL, json!({"event_id": "evt-ghost"})))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("unknown event id `evt-ghost`"),
            "got {err}"
        );

        let err = tools
            .invoke(call(
                RECALL_NEIGHBORS_TOOL,
                json!({"event_id": "evt-ghost"}),
            ))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("unknown event id `evt-ghost`"),
            "got {err}"
        );

        let err = tools
            .invoke(call(RECALL_THREAD_TOOL, json!({"thread_id": "t-ghost"})))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("unknown thread id `t-ghost`"),
            "got {err}"
        );
    }
}
