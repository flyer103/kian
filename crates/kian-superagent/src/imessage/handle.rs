//! Handle normalization (CHANNEL-IMESSAGE §6).
//!
//! The operator pastes a handle from `imsg chats --json`; the bridge
//! reports the raw `chat.db` handle. Both sides normalize before the
//! allowlist comparison so a formatted phone number in the config still
//! matches the E.164 the database stores — but nothing is *guessed*: a
//! handle the normalizer does not recognize is kept verbatim rather
//! than coerced into a phone-shaped key.

/// Normalizes one iMessage handle for allowlist comparison.
///
/// - trim; an empty result is `None` (no identity ⇒ no decision);
/// - a `+`-prefixed number keeps the `+` and drops spaces, dashes,
///   dots and parentheses when the remainder is all digits —
///   otherwise the input is kept verbatim (a malformed phone-shaped
///   handle is not silently repaired);
/// - any other handle containing `@` is ASCII-lowercased (iMessage
///   email addresses are case-insensitive);
/// - everything else (service-suffixed identifiers, short codes) is
///   kept verbatim.
#[must_use]
pub fn normalize_handle(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(rest) = trimmed.strip_prefix('+') {
        let digits: String = rest
            .chars()
            .filter(|c| !matches!(c, ' ' | '-' | '.' | '(' | ')'))
            .collect();
        if !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()) {
            return Some(format!("+{digits}"));
        }
        return Some(trimmed.to_string());
    }
    if trimmed.contains('@') {
        return Some(trimmed.to_ascii_lowercase());
    }
    Some(trimmed.to_string())
}

/// Normalizes a whole config table, failing on keys that normalize to
/// the same value (they would shadow each other) or to nothing.
pub fn normalize_topics(
    topics: &std::collections::BTreeMap<String, String>,
) -> Result<std::collections::BTreeMap<String, String>, String> {
    let mut normalized = std::collections::BTreeMap::new();
    for (raw, topic) in topics {
        let Some(handle) = normalize_handle(raw) else {
            return Err(format!("topics key `{raw}` is empty after normalization"));
        };
        if topic.trim().is_empty() {
            return Err(format!("topics entry `{raw}` has an empty routing topic"));
        }
        if let Some(existing) = normalized.insert(handle.clone(), topic.clone()) {
            return Err(format!(
                "topics keys collide after normalization: `{handle}` maps to both `{existing}` \
                 and `{topic}`"
            ));
        }
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn phones_normalize_to_e164_shape() {
        assert_eq!(
            normalize_handle(" +1 (415) 555-1212 "),
            Some("+14155551212".into())
        );
        assert_eq!(
            normalize_handle("+44 20 7946 0958"),
            Some("+442079460958".into())
        );
        assert_eq!(
            normalize_handle("+1.415.555.1212"),
            Some("+14155551212".into())
        );
    }

    #[test]
    fn emails_lowercase_and_odd_handles_stay_verbatim() {
        assert_eq!(
            normalize_handle("Foo@Example.COM"),
            Some("foo@example.com".into())
        );
        assert_eq!(
            normalize_handle("iMessage;-;+14155551212"),
            Some("iMessage;-;+14155551212".into())
        );
        assert_eq!(normalize_handle("  "), None);
        // A malformed phone-shaped handle is not silently repaired.
        assert_eq!(normalize_handle("+1abc"), Some("+1abc".into()));
    }

    #[test]
    fn colliding_config_keys_are_refused() {
        let topics = BTreeMap::from([
            ("+1 (415) 555-1212".to_string(), "imessage".to_string()),
            ("+14155551212".to_string(), "other".to_string()),
        ]);
        let err = normalize_topics(&topics).unwrap_err();
        assert!(err.contains("collide"), "{err}");
    }

    #[test]
    fn distinct_keys_survive() {
        let topics = BTreeMap::from([
            ("+1 (415) 555-1212".to_string(), "imessage".to_string()),
            ("Me@Example.com".to_string(), "imessage".to_string()),
        ]);
        let normalized = normalize_topics(&topics).expect("distinct");
        assert_eq!(normalized["+14155551212"], "imessage");
        assert_eq!(normalized["me@example.com"], "imessage");
    }
}
