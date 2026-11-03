//! Channel output policies (card C26, DESIGN §4.2 templates), plus the
//! C58 output-side speak-rules (the *whether-to-send* gate).
//!
//! Bandwidth matching: the Router's channel replies are *shaped before
//! the send* — the adapter delivers what it is given (the
//! `ChannelDelivery` contract), so the word limit and the section
//! contract live here, in the bundle, never in a channel adapter and
//! never in the core (the card's placement rule).
//!
//! - [`ImPolicy`] — the DESIGN §4.2 IM row: a ≤`max_words` hard rule.
//!   Word count = whitespace tokens of the plain-text reply. An
//!   over-limit reply is truncated at the limit with an ellipsis and
//!   recorded `truncated: true`; exact-limit and under-limit replies
//!   pass verbatim.
//! - [`StructuredPolicy`] — the §4.2 structured-delivery row: a payload
//!   missing a required section is **rejected honestly** (an error to
//!   the loop), never silently sent.
//! - [`SpeakPolicy`] (C58) — the §4.2 *whether-to-send* gate: which
//!   sends may leave a channel at all, evaluated **before** the
//!   shaping policies (a suppressed send is never shaped, never sent,
//!   and leaves no `channel/delivered` — the C39 deliver-or-discard
//!   discipline). [`SpeakTracker`] is the ack-once memory: a
//!   disposable projection over the Router's own `channel/delivered`
//!   events, rebuilt from the stream, never persisted.
//!
//! Both policies append `channel/delivered` after the send succeeds;
//! the payload builders here produce the C18 §2 golden schema
//! `{channel, target, policy, words?, truncated}`.
//!
//! All functions are pure — no ports, no I/O — so placement and
//! truncation are unit-testable without any server.

use std::collections::BTreeSet;

use kian_core::events::{Event, EventKind};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Policy name stamped on `channel/delivered` for plain-text IM replies.
pub const IM_POLICY_NAME: &str = "im";

/// Policy name stamped on `channel/delivered` for structured deliveries.
pub const STRUCTURED_POLICY_NAME: &str = "structured";

/// The DESIGN §4.2 IM hard rule: ≤100 words.
pub const DEFAULT_MAX_WORDS: u64 = 100;

/// The DESIGN §4.2 structured-delivery sections (problem background /
/// change notes / verification results).
pub const DEFAULT_SECTIONS: [&str; 3] = ["background", "changes", "verification"];

/// The truncation marker appended to an over-limit reply. Attached
/// directly to the last kept word (no space), so the delivered text's
/// whitespace-token count stays exactly at the limit.
pub const ELLIPSIS: &str = "…";

/// The §4.2 IM output policy template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImPolicy {
    /// Hard word limit. Word count = whitespace tokens of the
    /// plain-text reply.
    pub max_words: u64,
}

impl Default for ImPolicy {
    fn default() -> Self {
        Self {
            max_words: DEFAULT_MAX_WORDS,
        }
    }
}

/// A policy-shaped plain-text reply: the text to deliver plus the
/// audit fields for `channel/delivered`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnforcedReply {
    /// The text to deliver (truncated or verbatim).
    pub text: String,
    /// Whitespace-token count of the delivered `text` (excluding the
    /// truncation ellipsis).
    pub words: u64,
    /// True iff the original reply exceeded the limit and was cut.
    pub truncated: bool,
}

/// Whitespace-token count of `text` (the card's word-count definition).
#[must_use]
pub fn count_words(text: &str) -> u64 {
    text.split_whitespace().count() as u64
}

impl ImPolicy {
    /// Shapes `text` for delivery. Over-limit replies keep their first
    /// `max_words` whitespace tokens (joined by single spaces) plus a
    /// trailing ellipsis; exact-limit and under-limit replies pass
    /// verbatim with `truncated: false`.
    #[must_use]
    pub fn enforce(&self, text: &str) -> EnforcedReply {
        let words = count_words(text);
        if words <= self.max_words {
            return EnforcedReply {
                text: text.to_string(),
                words,
                truncated: false,
            };
        }
        let kept: Vec<&str> = text
            .split_whitespace()
            .take(self.max_words as usize)
            .collect();
        EnforcedReply {
            text: format!("{}{ELLIPSIS}", kept.join(" ")),
            words: self.max_words,
            truncated: true,
        }
    }
}

/// The §4.2 structured-delivery output policy template.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredPolicy {
    /// Required top-level payload keys, in policy order.
    pub sections: Vec<String>,
}

impl Default for StructuredPolicy {
    fn default() -> Self {
        Self {
            sections: DEFAULT_SECTIONS.iter().map(|s| (*s).to_string()).collect(),
        }
    }
}

/// Honest policy rejections. A rejected payload is never sent — the
/// error surfaces to the loop, which reports it to the model (and the
/// stream) as a failed tool call (constraint 7).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PolicyError {
    #[error("structured payload missing required section `{0}`")]
    MissingSection(String),
    #[error("structured payload is not a JSON object")]
    NotAnObject,
}

impl StructuredPolicy {
    /// Validates `payload` against the section contract. The payload
    /// must be a JSON object carrying every required section as a
    /// top-level key (the section's value is the channel's business,
    /// not the policy's).
    pub fn validate(&self, payload: &Value) -> Result<(), PolicyError> {
        let Some(obj) = payload.as_object() else {
            return Err(PolicyError::NotAnObject);
        };
        for section in &self.sections {
            if !obj.contains_key(section) {
                return Err(PolicyError::MissingSection(section.clone()));
            }
        }
        Ok(())
    }
}

/// The `channel/delivered` payload for an IM reply (C18 §2 schema;
/// `words` records the delivered count).
#[must_use]
pub fn im_delivered_payload(channel: &str, target: &Value, reply: &EnforcedReply) -> Value {
    json!({
        "channel": channel,
        "target": target,
        "policy": IM_POLICY_NAME,
        "words": reply.words,
        "truncated": reply.truncated,
    })
}

/// The `channel/delivered` payload for a structured delivery (no word
/// count — `words` is omitted per the C18 §2 schema).
#[must_use]
pub fn structured_delivered_payload(channel: &str, target: &Value) -> Value {
    json!({
        "channel": channel,
        "target": target,
        "policy": STRUCTURED_POLICY_NAME,
        "truncated": false,
    })
}

// ---------------------------------------------------------------------------
// Output-side speak-rules (C58, DESIGN §4.2 + §4.7)
// ---------------------------------------------------------------------------

/// The closed `when` vocabulary of a [`SpeakRule`] — the prototype's
/// attention-economics rule (reference 000: "speak only when a person
/// must act or can use a result; confirm once, stay quiet while
/// working") as five machine-checkable conditions. The `code` strings
/// are the stable audit/serde spellings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SpeakCondition {
    /// Every send on the channel passes (today's behavior, per rule).
    Always,
    /// Confirm once: the first send per topic (`per_topic_ack`) or per
    /// channel passes; every further one is suppressed until a result
    /// or a decision rides a different condition.
    AckFirstOnly,
    /// Only sends a person must act on pass — mechanically the
    /// two-way `reply` surface ([`SpeakKind::Decision`]). Structured
    /// reports are suppressed on this channel.
    DecisionNeeded,
    /// Only result deliveries pass — mechanically `deliver_structured`
    /// ([`SpeakKind::Result`]). Progress chatter is suppressed.
    ResultReady,
    /// Nothing leaves the channel. The gate for channels the Router
    /// may listen to but never write.
    Never,
}

impl SpeakCondition {
    /// The complete closed set, in display-table order.
    pub const ALL: [SpeakCondition; 5] = [
        SpeakCondition::Always,
        SpeakCondition::AckFirstOnly,
        SpeakCondition::DecisionNeeded,
        SpeakCondition::ResultReady,
        SpeakCondition::Never,
    ];

    /// The stable audit/serde code (also the config spelling).
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            SpeakCondition::Always => "always",
            SpeakCondition::AckFirstOnly => "ack-first-only",
            SpeakCondition::DecisionNeeded => "decision-needed",
            SpeakCondition::ResultReady => "result-ready",
            SpeakCondition::Never => "never",
        }
    }

    /// Parses a config code. `None` on any unknown spelling — the
    /// caller refuses to start (fail-closed), never silently defaults.
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        SpeakCondition::ALL
            .iter()
            .find(|condition| condition.code() == code)
            .copied()
    }

    /// One-line guidance surfaced in the honest suppression error, so
    /// the Router model can react instead of retrying the same send.
    #[must_use]
    pub fn guidance(&self) -> &'static str {
        match self {
            SpeakCondition::Always => {
                "this channel admits every send; the refusal was not about volume"
            }
            SpeakCondition::AckFirstOnly => {
                "this topic was already acknowledged once — stay quiet while working and \
                 deliver again only when a result is ready or a decision is needed"
            }
            SpeakCondition::DecisionNeeded => {
                "this channel carries decision requests only — send plain-text replies \
                 here and deliver structured results elsewhere"
            }
            SpeakCondition::ResultReady => {
                "this channel carries results only — deliver the structured report here \
                 and keep plain-text conversation elsewhere"
            }
            SpeakCondition::Never => {
                "this channel is muted for the Router — record the finding in the \
                 thread summary and deliver nothing here"
            }
        }
    }
}

impl std::fmt::Display for SpeakCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code())
    }
}

/// What kind of send is at the gate. Derived from the tool, never
/// model-declared (a self-classified "this is definitely a decision"
/// would be gameable prose): `reply` is the two-way surface a person
/// must act on ([`SpeakKind::Decision`]), `deliver_structured` is the
/// report a person can use ([`SpeakKind::Result`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SpeakKind {
    /// A plain-text reply — someone must act (reference 000).
    Decision,
    /// A structured delivery — someone can use a result.
    Result,
}

/// One `superagent.speak_rules` entry: the rule *for a channel*.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpeakRule {
    /// The channel this rule gates (first match wins; a channel with
    /// no rule is ungated — today's behavior).
    pub channel: String,
    /// The condition under which a send may leave.
    pub when: SpeakCondition,
    /// For `ack-first-only`: key the confirm-once memory per topic
    /// (the target's canonical key) rather than per channel.
    pub per_topic_ack: bool,
}

/// The pure input to [`SpeakPolicy::check`]: what is being sent, where,
/// and the ack-once memory. Everything is a value — the same inputs
/// always yield the same verdict.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpeakContext<'a> {
    /// The channel being sent to.
    pub channel: &'a str,
    /// The conversation key ([`speak_key`] of the target) — the
    /// `per_topic_ack` unit.
    pub topic: Option<&'a str>,
    /// The send's kind (tool-derived, see [`SpeakKind`]).
    pub kind: SpeakKind,
    /// The ack-once memory (a disposable projection, never persisted).
    pub tracker: &'a SpeakTracker,
}

/// The gate's decision. A suppression names its condition — the Router
/// turns it into an honest tool error, never a silent swallow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpeakVerdict {
    /// The send may proceed (shaping policies still apply).
    Admit,
    /// The send must not happen: no adapter call, no `channel/delivered`.
    Suppress {
        /// The condition that refused (surfaced in the error + audit).
        condition: SpeakCondition,
    },
}

/// The configured speak-rule table (`superagent.speak_rules`). An empty
/// table — the default, and what an absent config section produces —
/// admits every send: byte-identical to the pre-C58 behavior.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SpeakPolicy {
    rules: Vec<SpeakRule>,
}

impl SpeakPolicy {
    /// A policy over `rules` (config order; [`Self::rule_for`] takes
    /// the first match for a channel).
    #[must_use]
    pub fn from_rules(rules: Vec<SpeakRule>) -> Self {
        Self { rules }
    }

    /// The rule gating `channel`, if any. `None` = ungated.
    #[must_use]
    pub fn rule_for(&self, channel: &str) -> Option<&SpeakRule> {
        self.rules.iter().find(|rule| rule.channel == channel)
    }

    /// The pure gate: does this send may leave? Pure over values —
    /// unit-testable without any port; the caller (the Router tools)
    /// supplies the tracker and turns a suppression into an honest
    /// tool error + audit line + counter.
    #[must_use]
    pub fn check(rule: &SpeakRule, context: &SpeakContext) -> SpeakVerdict {
        match rule.when {
            SpeakCondition::Always => SpeakVerdict::Admit,
            SpeakCondition::Never => SpeakVerdict::Suppress {
                condition: rule.when,
            },
            SpeakCondition::DecisionNeeded => match context.kind {
                SpeakKind::Decision => SpeakVerdict::Admit,
                SpeakKind::Result => SpeakVerdict::Suppress {
                    condition: rule.when,
                },
            },
            SpeakCondition::ResultReady => match context.kind {
                SpeakKind::Result => SpeakVerdict::Admit,
                SpeakKind::Decision => SpeakVerdict::Suppress {
                    condition: rule.when,
                },
            },
            SpeakCondition::AckFirstOnly => {
                let already = if rule.per_topic_ack {
                    context
                        .topic
                        .map(|topic| context.tracker.has_spoken(context.channel, topic))
                        .unwrap_or_else(|| context.tracker.has_spoken_on_channel(context.channel))
                } else {
                    context.tracker.has_spoken_on_channel(context.channel)
                };
                if already {
                    SpeakVerdict::Suppress {
                        condition: rule.when,
                    }
                } else {
                    SpeakVerdict::Admit
                }
            }
        }
    }
}

/// The canonical conversation key of a delivery target: the target's
/// canonical JSON (a deterministic serialization of the same value).
/// The ack-once memory and the stream rebuild derive the same key from
/// the same target bytes, by construction.
#[must_use]
pub fn speak_key(target: &Value) -> String {
    target.to_string()
}

/// The ack-once memory (C58): which `(channel, topic)` pairs the Router
/// has already spoken to. A **disposable projection** over the Router's
/// own `channel/delivered` events — [`SpeakTracker::from_events`]
/// rebuilds it from the stream (at boot, and in tests from any
/// scripted stream), so "first ack only" survives restarts for free
/// (constraint 1); nothing here is ever persisted.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SpeakTracker {
    spoken: BTreeSet<(String, String)>,
}

impl SpeakTracker {
    /// An empty tracker (nothing spoken yet).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuilds the tracker from a committed Router stream: every
    /// `channel/delivered` event contributes its `(channel, target)`
    /// pair. Other kinds are ignored. Pure over events.
    #[must_use]
    pub fn from_events(events: &[Event]) -> Self {
        let mut tracker = Self::default();
        for event in events {
            if event.kind != EventKind::ChannelDelivered {
                continue;
            }
            let Some(channel) = event.bytes.get("channel").and_then(Value::as_str) else {
                continue;
            };
            let key = event
                .bytes
                .get("target")
                .map(speak_key)
                .unwrap_or_else(|| "null".to_string());
            tracker.spoken.insert((channel.to_string(), key));
        }
        tracker
    }

    /// Records one successful send (the runtime half; must agree with
    /// [`SpeakTracker::from_events`] over the same history).
    pub fn record(&mut self, channel: &str, topic_key: &str) {
        self.spoken
            .insert((channel.to_string(), topic_key.to_string()));
    }

    /// True iff the Router has already spoken to `(channel, topic)`.
    #[must_use]
    pub fn has_spoken(&self, channel: &str, topic_key: &str) -> bool {
        self.spoken
            .contains(&(channel.to_string(), topic_key.to_string()))
    }

    /// True iff the Router has already spoken anywhere on `channel`.
    #[must_use]
    pub fn has_spoken_on_channel(&self, channel: &str) -> bool {
        self.spoken.iter().any(|(c, _)| c == channel)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_MAX_WORDS, DEFAULT_SECTIONS, ELLIPSIS, EnforcedReply, IM_POLICY_NAME,
        STRUCTURED_POLICY_NAME, count_words, im_delivered_payload, structured_delivered_payload,
    };
    use crate::policy::{ImPolicy, PolicyError, StructuredPolicy};
    use serde_json::json;

    fn words(n: usize) -> String {
        (1..=n)
            .map(|i| format!("w{i}"))
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    fn word_count_is_whitespace_tokens() {
        assert_eq!(count_words(""), 0);
        assert_eq!(count_words("   "), 0);
        assert_eq!(count_words("one"), 1);
        assert_eq!(count_words("  two   words  "), 2);
        assert_eq!(count_words("a\tb\nc"), 3);
    }

    #[test]
    fn over_limit_reply_truncated_at_the_limit_with_ellipsis() {
        let policy = ImPolicy::default();
        assert_eq!(policy.max_words, DEFAULT_MAX_WORDS);
        let original = words(120);
        let enforced = policy.enforce(&original);
        assert!(enforced.truncated);
        assert_eq!(enforced.words, 100);
        // The delivered text carries exactly 100 words (the ellipsis is
        // punctuation attached to the last kept word, not a token).
        assert_eq!(count_words(&enforced.text), 100);
        assert!(enforced.text.ends_with(ELLIPSIS));
        assert!(!enforced.text.contains(" w101 ") && !enforced.text.contains("w120"));
        // The kept prefix is verbatim.
        assert!(enforced.text.starts_with("w1 w2 w3"));
    }

    #[test]
    fn exact_limit_reply_passes_untruncated() {
        let policy = ImPolicy::default();
        let exact = words(100);
        let enforced = policy.enforce(&exact);
        assert_eq!(
            enforced,
            EnforcedReply {
                text: exact.clone(),
                words: 100,
                truncated: false,
            }
        );
        assert_eq!(enforced.text, exact);
        assert!(!enforced.text.contains(ELLIPSIS));
    }

    #[test]
    fn under_limit_and_empty_replies_pass_untruncated() {
        let policy = ImPolicy::default();
        let short = policy.enforce("done, shipping now");
        assert_eq!(short.words, 3);
        assert!(!short.truncated);
        assert_eq!(short.text, "done, shipping now");

        let empty = policy.enforce("");
        assert_eq!(empty.words, 0);
        assert!(!empty.truncated);

        // A custom (tighter) limit is honored.
        let tight = ImPolicy { max_words: 2 };
        let cut = tight.enforce(&words(5));
        assert!(cut.truncated);
        assert_eq!(cut.words, 2);
        assert_eq!(cut.text, format!("w1 w2{ELLIPSIS}"));
    }

    #[test]
    fn structured_policy_validates_the_section_contract() {
        let policy = StructuredPolicy::default();
        assert_eq!(
            policy.sections,
            DEFAULT_SECTIONS
                .iter()
                .map(|s| (*s).to_string())
                .collect::<Vec<_>>()
        );
        let full = json!({
            "background": "the flaky test",
            "changes": "retry with backoff",
            "verification": "cargo test green",
        });
        assert_eq!(policy.validate(&full), Ok(()));

        // Missing one section: honest rejection naming it.
        let missing = json!({"background": "b", "changes": "c"});
        assert_eq!(
            policy.validate(&missing),
            Err(PolicyError::MissingSection("verification".into()))
        );
        // Not an object at all.
        assert_eq!(
            policy.validate(&json!("text")),
            Err(PolicyError::NotAnObject)
        );
        assert_eq!(
            policy.validate(&serde_json::Value::Null),
            Err(PolicyError::NotAnObject)
        );
        // A custom section set enforces its own contract.
        let custom = StructuredPolicy {
            sections: vec!["summary".into()],
        };
        assert_eq!(custom.validate(&json!({"summary": 1})), Ok(()));
        assert_eq!(
            custom.validate(&json!({"background": 1})),
            Err(PolicyError::MissingSection("summary".into()))
        );
        // Extra sections beyond the contract are the channel's business.
        assert_eq!(
            policy.validate(&json!({"background":1,"changes":1,"verification":1,"extra":1})),
            Ok(())
        );
    }

    #[test]
    fn delivered_payloads_match_the_c18_schema() {
        let reply = EnforcedReply {
            text: format!("{}{ELLIPSIS}", words(100)),
            words: 100,
            truncated: true,
        };
        assert_eq!(
            im_delivered_payload("mock-im", &json!({"chat_id": 7}), &reply),
            json!({
                "channel": "mock-im",
                "target": {"chat_id": 7},
                "policy": IM_POLICY_NAME,
                "words": 100,
                "truncated": true,
            })
        );
        let payload = structured_delivered_payload("github", &json!({"issue": 42}));
        assert_eq!(
            payload,
            json!({
                "channel": "github",
                "target": {"issue": 42},
                "policy": STRUCTURED_POLICY_NAME,
                "truncated": false,
            })
        );
        // `words` is omitted for structured deliveries (schema `words?`).
        assert!(payload.get("words").is_none());
    }
}
