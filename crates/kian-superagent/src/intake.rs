//! Thread/Correlation intake projection (ADR-013 draft).
//!
//! Kian's intake already has channel-admit idempotency
//! (`(session, event_id)`, one row per event, never two) and
//! adapter-chosen topics. The prototype's domain layer (reference 001,
//! Source/Thread/Correlation rows) adds three things this module
//! projects onto that substrate — **without a single new event kind,
//! table, or core seam**:
//!
//! - **Thread key** — one canonical correlation unit per source shape,
//!   derived from config: `superagent.intake.thread_keys:
//!   [{channel, key: [group?, <payload-field>…]}]` producing the
//!   canonical topic form `<channel>:<group>#<key>` ([`ThreadKey`]).
//!   Deterministic and collision-free by construction (every reserved
//!   character is percent-encoded inside its component, so distinct
//!   component tuples always derive distinct topics). Keys are
//!   **config-owned**: a config change is a new key, and old streams
//!   keep their topics — cache-stable, no rewrite. After derivation
//!   the key *is* a topic; the subscription rule matcher is unchanged.
//! - **Source read position** — the resume cursor derived from the
//!   last admitted event's payload cursor fields per source kind
//!   ([`read_position`]): lark chat history exposes `message_id`
//!   (offset semantics — the next poll resumes after it); the GitHub
//!   REST list endpoints expose `since` timestamps (`updated_at`).
//!   Where a source provides **no** derivable cursor, the honest v1
//!   bound is idempotent re-poll under `event_id` dedupe: a restart
//!   re-polls at most once per source and the pipeline's idempotency
//!   absorbs the replay — no duplicate rows, ever. A durable position
//!   table is explicitly rejected (ADR-013: pre-event durable state
//!   needing its own ADR; `intake/positioned` as a kind likewise).
//! - **Correlation is context only** — the derived key surfaces in the
//!   Inline `neighbors` block ([`neighbors_with_thread_key`]) so
//!   the Router and workers reach sibling threads through the existing
//!   recall toolpack. Correlation never routes (001's
//!   rejected-concepts row, kept): subscription routing stays
//!   topic-keyed.
//!
//! Normative companion (recorded in ADR-013 and DESIGN §4.1):
//! **fan-out grants no authority.** A fanned-out message reaches live
//! subscribed tickets as *context* bounded by their Mandates
//! (ADR-012); no admission check runs on it. One trigger fanning out
//! to many tickets is the existing fan-out mechanics restated
//! under the authority rule — nothing here re-derives it.
//!
//! Bundle fence: this module is pure derivation over
//! [`ChannelEvent`]s and admitted stream events. It names no store,
//! no inbox, no wake — assembly (kian-cli) and the adapters call it;
//! `tests/iron_rule.rs` holds the textual line.

use kian_core::events::{Event, EventKind};
use kian_core::seams::ChannelEvent;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::github;
use crate::lark;
use crate::provenance::parse_channel_marker;

/// The neighbors-block key the derived thread key surfaces under (the
/// vocabulary: an association id, context for recall hops — never
/// a routing input).
pub const THREAD_KEY_FIELD: &str = "thread_key";

/// The one provenance field nameable inside a `key` list: the event's
/// `group` (adapter-declared chat/repo provenance), which fills the
/// `<group>` slot of the canonical form.
pub const GROUP_FIELD: &str = "group";

// ---------------------------------------------------------------------
// Thread key derivation
// ---------------------------------------------------------------------

/// One configured thread-key shape: `{channel, key: [group?,
/// <payload-field>…]}`. `key` entries are, in config order:
///
/// - `"group"` — fills the `<group>` slot from the event's group
///   provenance (an event carrying none is honestly underivable);
/// - any other name — a payload field path, dotted segments walk
///   nested objects (`"issue.number"`); the value must be a scalar
///   (string or number) and joins the `<key>` part in config order.
///
/// Serde form (the config spelling, golden):
/// `{"channel": "github", "key": ["group", "issue.number"]}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadKeyRule {
    /// Channel name the rule applies to (must equal the
    /// [`ChannelEvent::channel`] the adapter reports).
    pub channel: String,
    /// The derivation fields, config order (see the type doc).
    pub key: Vec<String>,
}

impl ThreadKeyRule {
    /// Channel agrees and the key list is non-empty (an empty `key`
    /// derives nothing — a misconfiguration is an honest `None`, never
    /// a fallback guess).
    pub fn matches(&self, event: &ChannelEvent) -> bool {
        self.channel == event.channel && !self.key.is_empty()
    }
}

/// A validated set of thread-key rules. First match on channel wins
/// (config order — the ruleset convention); an event whose channel has
/// no entry derives `None` and the adapter's own topic stands.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ThreadKeySet(pub Vec<ThreadKeyRule>);

impl ThreadKeySet {
    pub fn new(rules: Vec<ThreadKeyRule>) -> Self {
        Self(rules)
    }

    /// First rule matching the event's channel, in declaration order.
    pub fn first_match(&self, event: &ChannelEvent) -> Option<&ThreadKeyRule> {
        self.0.iter().find(|rule| rule.matches(event))
    }

    /// Derives the event's canonical thread key, or `None` when no
    /// rule matches / a named field is missing or non-scalar.
    pub fn derive(&self, event: &ChannelEvent) -> Option<ThreadKey> {
        self.first_match(event)
            .and_then(|rule| derive_thread_key(rule, event))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

/// The derived canonical thread key. Its topic form **is** a routing
/// topic after derivation (`<channel>:<group>#<key>`, group slot
/// omitted when no `"group"` field is configured) — collision-free by
/// construction: every reserved character (`%`, `:`, `#`, `.`) inside
/// a component is percent-encoded, so the mapping from component
/// tuples to topics is injective.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ThreadKey {
    /// The channel name verbatim.
    pub channel: String,
    /// The group provenance, when a `"group"` field was configured
    /// (and present — a named-but-absent group is underivable).
    pub group: Option<String>,
    /// The payload key field values, config order, scalar-rendered.
    pub key_parts: Vec<String>,
    topic: String,
}

impl ThreadKey {
    /// The canonical topic form — the string the event routes by once
    /// the key is derived.
    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl std::fmt::Display for ThreadKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.topic())
    }
}

/// Derives one event's thread key under one rule. Pure; `None` is the
/// honest underivable answer (missing field, non-scalar field, missing
/// group, channel mismatch, empty key list) — never a guess.
pub fn derive_thread_key(rule: &ThreadKeyRule, event: &ChannelEvent) -> Option<ThreadKey> {
    if !rule.matches(event) {
        return None;
    }
    let mut group: Option<String> = None;
    let mut key_parts: Vec<String> = Vec::new();
    for field in &rule.key {
        if field == GROUP_FIELD {
            // A named group slot: the event must carry one.
            group = Some(event.group.as_deref()?.to_string());
            continue;
        }
        key_parts.push(scalar_at(&event.payload, field)?);
    }
    if key_parts.is_empty() {
        // `key: ["group"]` alone names a group but no key part: the
        // `<key>` side of the form would be empty — underivable.
        return None;
    }
    let mut topic = encode_component(&event.channel);
    if let Some(group) = &group {
        topic.push(':');
        topic.push_str(&encode_component(group));
    }
    topic.push('#');
    topic.push_str(
        &key_parts
            .iter()
            .map(|part| encode_component(part))
            .collect::<Vec<_>>()
            .join("."),
    );
    Some(ThreadKey {
        channel: event.channel.clone(),
        group,
        key_parts,
        topic,
    })
}

/// The scalar value at a dotted payload path (`"issue.number"`),
/// string-rendered for numbers. `None` when any segment is missing or
/// the leaf is not a scalar (bool/null/array/object never join a key).
fn scalar_at(payload: &Value, path: &str) -> Option<String> {
    let mut cursor = payload;
    for segment in path.split('.') {
        cursor = cursor.get(segment)?;
    }
    match cursor {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Percent-encodes the reserved characters of the canonical form
/// (`%`, `:`, `#`, `.`) inside one component — everything else rides
/// verbatim. This is what makes the form collision-free: a `:` inside
/// a group can never impersonate the channel separator, and so on for
/// every separator in every slot.
fn encode_component(component: &str) -> String {
    let mut out = String::with_capacity(component.len());
    for ch in component.chars() {
        match ch {
            '%' => out.push_str("%25"),
            ':' => out.push_str("%3A"),
            '#' => out.push_str("%23"),
            '.' => out.push_str("%2E"),
            _ => out.push(ch),
        }
    }
    out
}

// ---------------------------------------------------------------------
// Neighbors block (correlation is context only)
// ---------------------------------------------------------------------

/// The Inline `neighbors` block with the derived thread key
/// surfaced as context: canonical `topic`/`event_id`, the configured
/// top-level scalar neighbor fields (the provenance.rs rule — payload
/// values never override the canonical names, non-scalars never ride),
/// and `thread_key` when the set derives one.
///
/// Context only: this block feeds recall hops (the toolpack), not
/// the subscription rule matcher — correlation never routes.
pub fn neighbors_with_thread_key(
    event: &ChannelEvent,
    keys: &ThreadKeySet,
    neighbor_fields: &[String],
) -> Value {
    let mut neighbors = Map::new();
    neighbors.insert("topic".into(), json!(event.topic));
    neighbors.insert("event_id".into(), json!(event.event_id));
    if let Some(obj) = event.payload.as_object() {
        for field in neighbor_fields {
            // Canonical provenance wins over payload-controlled fields.
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
    if let Some(key) = keys.derive(event) {
        neighbors.insert(
            THREAD_KEY_FIELD.into(),
            Value::String(key.topic().to_string()),
        );
    }
    Value::Object(neighbors)
}

// ---------------------------------------------------------------------
// Source read position
// ---------------------------------------------------------------------

/// The cursor vocabulary a source kind exposes: how the resume
/// cursor is derived from the last admitted event's payload. Serde
/// names are the config spelling (`lark-message-id | github-since`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SourceCursor {
    /// Lark chat history: the last admitted `message_id` — offset
    /// semantics, the next poll resumes after it
    /// ([`crate::lark::payload_message_cursor`]).
    LarkMessageId,
    /// GitHub REST list endpoints: the `since` timestamp — the
    /// `updated_at` of the issue/PR the payload belongs to
    /// ([`crate::github::payload_since_cursor`]).
    GitHubSince,
}

/// A derived source read position: the resume cursor, the payload
/// field it came from, and the admitted event that carried it — enough
/// to restart a poll without replaying what is already admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadPosition {
    /// The cursor value (a message id or a timestamp, per source kind).
    pub cursor: String,
    /// The payload field the cursor was derived from.
    pub field: &'static str,
    /// The channel event id of the admitted event the cursor rode on.
    pub event_id: String,
}

/// Derives the source read position from an admitted-event tail: the
/// **last** admitted channel event whose payload exposes the source
/// kind's cursor field wins. Scans newest-first over
/// `input/admitted` (the provenance wrapper's verbatim raw) and
/// `input/spilled` (the over-cap sidecar — the inline view carries no
/// raw, the sidecar always does). `None` when the tail derives no
/// cursor: the caller re-polls bounded by `event_id` dedupe (the
/// honest v1 — ADR-013; there is no position table to consult).
///
/// Pure; the tail is the source's own admitted events (a slice of a
/// session stream read through existing public surfaces).
pub fn read_position(source: SourceCursor, admitted_tail: &[Event]) -> Option<ReadPosition> {
    for event in admitted_tail.iter().rev() {
        // The (event_id, raw) pair this event exposes, if any.
        let carrier: Option<(String, Value)> = match event.kind {
            EventKind::InputAdmitted => {
                // A non-channel admitted input (a person's plain
                // message) carries no marker — skip it, never abort
                // the scan.
                let Some(marker) = event.bytes.get("content").and_then(parse_channel_marker) else {
                    continue;
                };
                if marker.raw.is_null() {
                    // An over-cap inline view: its raw lives in the
                    // sibling spill event, also scanned here.
                    continue;
                }
                Some((marker.event_id, marker.raw))
            }
            EventKind::InputSpilled => {
                let Some(event_id) = event.bytes.get("event_id").and_then(Value::as_str) else {
                    continue;
                };
                let Some(raw) = event.bytes.get("raw").cloned() else {
                    continue;
                };
                if raw.is_null() {
                    continue;
                }
                Some((event_id.to_string(), raw))
            }
            _ => continue,
        };
        let Some((event_id, raw)) = carrier else {
            continue;
        };
        let derived = match source {
            SourceCursor::LarkMessageId => {
                lark::payload_message_cursor(&raw).map(|cursor| (cursor, "message_id"))
            }
            SourceCursor::GitHubSince => {
                github::payload_since_cursor(&raw).map(|cursor| (cursor, "updated_at"))
            }
        };
        if let Some((cursor, field)) = derived {
            return Some(ReadPosition {
                cursor,
                field,
                event_id,
            });
        }
    }
    None
}
