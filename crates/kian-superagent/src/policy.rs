//! Channel output policies (card C26, DESIGN §4.2 templates).
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
//!
//! Both policies append `channel/delivered` after the send succeeds;
//! the payload builders here produce the C18 §2 golden schema
//! `{channel, target, policy, words?, truncated}`.
//!
//! All functions are pure — no ports, no I/O — so placement and
//! truncation are unit-testable without any server.

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
