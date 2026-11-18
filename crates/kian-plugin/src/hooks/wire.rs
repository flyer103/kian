//! View-building helpers for the interception-port call sites
//! (PLUGIN-SPEC §4's nine rows, P2).
//!
//! The bounds here are the §3.2 byte bounds and §7.6's frame bound,
//! enforced **before** serialization; the digests are the §3.2
//! `*_sha256` fields. They live in `kian-plugin` because K10 allows
//! exactly one new dependency this round (`sha2`, here) — every call
//! site (`kian-loop`, `kian-cli`) digests through these functions
//! instead of growing its own `sha2` edge, and `kian-dsh` (which may
//! not depend on this crate at all, §3.5) ships its own local
//! implementation over the same constants.
//!
//! Redaction is deliberately absent: tier-0 handlers receive the
//! **unredacted** view (§3.2 — same process, same authority), and the
//! secret redactor runs at the sidecar wire (P3), not at these call
//! sites.

use serde_json::Value;

/// §3.2's `content_preview` bound: ≤4096 bytes of the head of the
/// content, cut at a UTF-8 character boundary.
pub const PREVIEW_MAX_BYTES: usize = 4096;

/// §7.6's frame bound: one line, either direction, is at most
/// `max_payload_kib` (256 KiB) — the bound a §3.2.5 `arguments` value
/// must fit to ride parsed.
pub const FRAME_PAYLOAD_MAX_BYTES: usize = 256 * 1024;

/// The §3.2 `*_sha256` digest fields: SHA-256 over the exact bytes,
/// lowercase hex.
pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(data);
    let out = hasher.finalize();
    let mut hex = String::with_capacity(out.len() * 2);
    for byte in out {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// The exact serialized form a digest or byte count describes
/// (§3.2: `content_bytes` is the *serialized* length). Compact JSON —
/// the same bytes `serde_json::to_string` produces everywhere else.
///
/// `serde_json::to_vec` over a `Value` cannot fail (a `Value` is by
/// construction valid JSON: no non-string map keys, no non-finite
/// floats), and an audit digest must never silently describe the empty
/// input — so the invariant is asserted, the same convention
/// `subscriptions.rs` uses, rather than defaulted.
pub fn serialized(v: &Value) -> Vec<u8> {
    serde_json::to_vec(v).expect("serializing a serde_json::Value cannot fail")
}

/// §3.2's preview rule: the head of `content`, at most
/// [`PREVIEW_MAX_BYTES`] bytes, cut at a UTF-8 character boundary;
/// the flag is true when the preview is shorter than the content.
pub fn preview(content: &str) -> (String, bool) {
    if content.len() <= PREVIEW_MAX_BYTES {
        return (content.to_string(), false);
    }
    let mut cut = PREVIEW_MAX_BYTES;
    while cut > 0 && !content.is_char_boundary(cut) {
        cut -= 1;
    }
    (content[..cut].to_string(), true)
}

/// §3.2.5's exhaustive two-form rule for `arguments`: the parsed value
/// when its serialization fits the frame bound, else the longest
/// character-boundary prefix that fits, as a **string** — never a
/// partially serialized object. Returns `(arguments, truncated)`.
pub fn bounded_value(v: &Value) -> (Value, bool) {
    let bytes = serialized(v);
    if bytes.len() <= FRAME_PAYLOAD_MAX_BYTES {
        return (v.clone(), false);
    }
    let text = String::from_utf8_lossy(&bytes);
    let mut cut = FRAME_PAYLOAD_MAX_BYTES;
    // `text` borrows widened bytes; index on its own chars only.
    while cut > 0 && !text.is_char_boundary(cut) {
        cut -= 1;
    }
    (Value::String(text[..cut].to_string()), true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sha256_hex_matches_known_vectors() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn preview_truncates_on_a_character_boundary() {
        let short = "hello";
        assert_eq!(preview(short), (short.to_string(), false));

        // 4096 ASCII bytes + a two-byte char: the cut backs up to the
        // boundary, never splitting the char.
        let long = "é".repeat(3000);
        let (head, truncated) = preview(&long);
        assert!(truncated);
        assert!(head.len() <= PREVIEW_MAX_BYTES);
        assert!(head.chars().all(|c| c == 'é'));
        assert_eq!(head.len() % 2, 0, "cut on a char boundary");

        // Exactly at the bound: not truncated.
        let exact = "a".repeat(PREVIEW_MAX_BYTES);
        assert_eq!(preview(&exact), (exact.clone(), false));
    }

    #[test]
    fn bounded_value_is_parsed_or_prefix_never_both() {
        let small = json!({"cmd": ["ls"]});
        assert_eq!(bounded_value(&small), (small.clone(), false));

        // Over the frame bound: a string prefix + the flag.
        let big_text = "x".repeat(FRAME_PAYLOAD_MAX_BYTES + 10);
        let big = json!(big_text);
        let (view, truncated) = bounded_value(&big);
        assert!(truncated);
        assert!(view.is_string());
        let s = view.as_str().unwrap();
        assert!(s.len() <= FRAME_PAYLOAD_MAX_BYTES);

        // A multi-byte char straddling the cut is not split.
        let straddle = json!("é".repeat(FRAME_PAYLOAD_MAX_BYTES / 2 + 3));
        let (view, truncated) = bounded_value(&straddle);
        assert!(truncated);
        let s = view.as_str().unwrap();
        assert!(s.len() <= FRAME_PAYLOAD_MAX_BYTES);
        // The serialized form opens with a `"` byte, so the char-boundary
        // cut backs up to `1 + 2k`: the head is the quote plus whole é's.
        assert!(s.starts_with('"'));
        assert_eq!((s.len() - 1) % 2, 0);
        assert!(s[1..].chars().all(|c| c == 'é'));
    }
}
