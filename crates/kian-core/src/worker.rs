//! Worker-domain value rules shared by both halves of KiandProto v3:
//! capability names and the declaration sanitizer (ADR-029).
//!
//! These are wire-level value rules, not membership: the server still
//! sanitizes a hostile declaration and the worker validates its own
//! startup config. Keeping the alphabet in the narrow waist means both
//! halves cannot drift on what a capability name is.

/// The maximum number of capabilities one worker may declare. A longer
/// list is truncated by the server (with an audit line), never accepted
/// as an unbounded catalog row.
pub const MAX_CAPABILITIES: usize = 16;

/// One capability name: lowercase `[a-z0-9]`, then `[a-z0-9._-]`,
/// 1..=64 bytes. Surrounding whitespace is not accepted — the worker
/// config rejects it and the wire sanitizer trims before validating.
pub fn valid_capability_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    let Some(first) = bytes.first() else {
        return false;
    };
    name.len() <= 64
        && (first.is_ascii_lowercase() || first.is_ascii_digit())
        && bytes.iter().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        })
}

/// The server-side view of a worker's declared list: trim each name,
/// keep valid unique names in declaration order, cap at
/// [`MAX_CAPABILITIES`]. Invalid or duplicate entries are dropped; the
/// caller audits the count difference (never the raw strings).
pub fn sanitize_capabilities(declared: &[String]) -> Vec<String> {
    let mut accepted: Vec<String> = Vec::with_capacity(declared.len().min(MAX_CAPABILITIES));
    for raw in declared {
        let name = raw.trim();
        if !valid_capability_name(name) {
            continue;
        }
        if accepted.len() >= MAX_CAPABILITIES {
            continue;
        }
        if accepted.iter().any(|existing| existing == name) {
            continue;
        }
        accepted.push(name.to_string());
    }
    accepted
}

#[cfg(test)]
mod tests {
    use super::{MAX_CAPABILITIES, sanitize_capabilities, valid_capability_name};

    #[test]
    fn name_alphabet_is_lowercase_bounded() {
        for good in ["exec", "apple-calendar", "apple.health", "fs_read", "a1"] {
            assert!(valid_capability_name(good), "{good}");
        }
        for bad in [
            "",
            "Exec",
            "apple calendar",
            " apple",
            "apple ",
            "-exec",
            ".exec",
            "a\nb",
            "x@y",
        ] {
            assert!(!valid_capability_name(bad), "{bad:?}");
        }
        assert!(valid_capability_name(&"a".repeat(64)));
        assert!(!valid_capability_name(&"a".repeat(65)));
    }

    #[test]
    fn sanitizer_trims_dedupes_and_caps() {
        let declared = vec![
            " exec ".into(),
            "exec".into(),
            "bad name".into(),
            "fs".into(),
            "B".into(),
        ];
        assert_eq!(sanitize_capabilities(&declared), vec!["exec", "fs"]);
        let many: Vec<String> = (0..(MAX_CAPABILITIES + 4))
            .map(|i| format!("cap-{i}"))
            .collect();
        assert_eq!(sanitize_capabilities(&many).len(), MAX_CAPABILITIES);
    }
}
