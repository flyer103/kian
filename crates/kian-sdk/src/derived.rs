//! Dependency-free derived-event-id mirror.
//!
//! The SDK is wire-only: its `[dependencies]` allowlist admits no
//! `uuid` crate, so the derived event id rule is mirrored here in pure
//! Rust — RFC 4122 v5 is just SHA-1 over `namespace || name` with the
//! version/variant bits forced in. The golden test in `typed.rs` pins
//! these bytes to the same literals `kian-core` (the `uuid` crate) and
//! an independent generator (Python `uuid.uuid5`) produce, so the
//! mirror can never drift silently.

/// Minimal SHA-1 (FIPS 180-1) over the message, returning the 20-byte
/// digest. Written for the single uuid-v5 use case; not a general
/// hashing API.
struct Sha1 {
    h: [u32; 5],
    len_bytes: u64,
    buf: [u8; 64],
    buf_len: usize,
}

impl Sha1 {
    fn new() -> Self {
        // FIPS 180-1 initial state.
        Self {
            h: [
                0x6745_2301,
                0xEFCD_AB89,
                0x98BA_DCFE,
                0x1032_5476,
                0xC3D2_E1F0,
            ],
            len_bytes: 0,
            buf: [0u8; 64],
            buf_len: 0,
        }
    }

    fn update(&mut self, data: &[u8]) {
        self.len_bytes = self.len_bytes.wrapping_add(data.len() as u64);
        self.absorb(data);
    }

    /// Feeds bytes through the buffer/compress loop without length
    /// tracking (the padding path must not inflate the message length).
    fn absorb(&mut self, mut data: &[u8]) {
        while !data.is_empty() {
            let take = (64 - self.buf_len).min(data.len());
            self.buf[self.buf_len..self.buf_len + take].copy_from_slice(&data[..take]);
            self.buf_len += take;
            data = &data[take..];
            if self.buf_len == 64 {
                let block = self.buf;
                self.compress(&block);
                self.buf_len = 0;
            }
        }
    }

    fn finalize(mut self) -> [u8; 20] {
        let bit_len = self.len_bytes.wrapping_mul(8);
        let rem = (self.len_bytes % 64) as usize;
        // Pad: 0x80, then zeros so the block ends with 8 length bytes
        // (the message + 1 marker byte must land ≡ 56 mod 64).
        let pad = if rem < 56 {
            56 - rem - 1
        } else {
            120 - rem - 1
        };
        let mut tail = [0u8; 128];
        tail[0] = 0x80;
        tail[1 + pad..1 + pad + 8].copy_from_slice(&bit_len.to_be_bytes());
        self.absorb(&tail[..1 + pad + 8]);
        let mut out = [0u8; 20];
        for (i, word) in self.h.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
        }
        out
    }

    fn compress(&mut self, block: &[u8; 64]) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            let b = [
                block[i * 4],
                block[i * 4 + 1],
                block[i * 4 + 2],
                block[i * 4 + 3],
            ];
            w[i] = u32::from_be_bytes(b);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = self.h;
        for (i, &wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let tmp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = tmp;
        }
        self.h[0] = self.h[0].wrapping_add(a);
        self.h[1] = self.h[1].wrapping_add(b);
        self.h[2] = self.h[2].wrapping_add(c);
        self.h[3] = self.h[3].wrapping_add(d);
        self.h[4] = self.h[4].wrapping_add(e);
    }
}

/// RFC 4122 v5: SHA-1 over `namespace || name`, version and variant
/// bits forced, formatted as the lowercase hyphenated uuid string.
fn uuid_v5(namespace: [u8; 16], name: &str) -> String {
    let mut sha = Sha1::new();
    sha.update(&namespace);
    sha.update(name.as_bytes());
    let digest = sha.finalize();
    let mut b = [0u8; 16];
    b.copy_from_slice(&digest[..16]);
    b[6] = (b[6] & 0x0f) | 0x50; // version 5
    b[8] = (b[8] & 0x3f) | 0x80; // RFC 4122 variant
    let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

/// Parses a session uuid string (hyphenated or bare 32-hex; any case)
/// into its 16 bytes.
fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// The derived event id, mirrored from `kian_core::events::event_id`:
/// `"evt_" + uuid5(session, "kian:event:<seq>")`. Public only through
/// [`crate::typed`].
pub(crate) fn event_id(session: &str, seq: u64) -> Option<String> {
    let ns = parse_uuid(session)?;
    Some(format!("evt_{}", uuid_v5(ns, &format!("kian:event:{seq}"))))
}

#[cfg(test)]
mod tests {
    use super::{Sha1, event_id, parse_uuid, uuid_v5};

    // FIPS 180-1 test vectors — the hash underneath the mirror — plus
    // block-boundary lengths (55/56/64/65) that exercise the padding.
    #[test]
    fn sha1_matches_fips_vectors() {
        let digest = |msg: &[u8]| {
            let mut sha = Sha1::new();
            sha.update(msg);
            let out = sha.finalize();
            out.iter().map(|b| format!("{b:02x}")).collect::<String>()
        };
        assert_eq!(digest(b"abc"), "a9993e364706816aba3e25717850c26c9cd0d89d");
        assert_eq!(digest(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        // "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
        let long = b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
        assert_eq!(digest(long), "84983e441c3bd26ebaae4aa1f95129e5e54670f1");
        // Block boundaries (values cross-checked against Python hashlib).
        assert_eq!(
            digest(&[b'a'; 80]),
            "86f33652fcffd7fa1443e246dd34fe5d00e25ffd"
        );
        assert_eq!(
            digest(&[b'x'; 55]),
            "cef734ba81a024479e09eb5a75b6ddae62e6abf1"
        );
        assert_eq!(
            digest(&[b'y'; 56]),
            "8902d391f35bbaf08469aee6ab763e4048f3f6ba"
        );
        assert_eq!(
            digest(&[b'z'; 64]),
            "668cd5ae72f64e8194014f99778081e5ae46df53"
        );
        assert_eq!(
            digest(&[b'z'; 65]),
            "b4d0bc233838cba64eeaa151cae9301542498b30"
        );
    }

    #[test]
    fn uuid_parser_accepts_hyphenated_and_bare_forms() {
        assert_eq!(
            parse_uuid("00000000-0000-0000-0000-000000000001"),
            Some(
                [0u8; 15]
                    .iter()
                    .copied()
                    .chain([1u8])
                    .collect::<Vec<u8>>()
                    .try_into()
                    .unwrap()
            )
        );
        assert_eq!(
            parse_uuid("00000000000000000000000000000001"),
            parse_uuid("00000000-0000-0000-0000-000000000001")
        );
        assert_eq!(parse_uuid("not-a-uuid"), None);
        assert_eq!(parse_uuid("00000000-0000-0000-0000-0000000000"), None);
    }

    /// The v5 core matches the RFC's own worked example: v5(DNS
    /// namespace, "python.org") is the documented
    /// `886313e1-3b8a-5372-9b90-0c9aee199e5d` (the value Python's
    /// `uuid.uuid5` returns for the same inputs).
    #[test]
    fn uuid_v5_matches_rfc_reference_vector() {
        // RFC 4122 NAMESPACE_DNS = 6ba7b810-9dad-11d1-80b4-00c04fd430c8
        let dns: [u8; 16] = [
            0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4,
            0x30, 0xc8,
        ];
        assert_eq!(
            uuid_v5(dns, "python.org"),
            "886313e1-3b8a-5372-9b90-0c9aee199e5d"
        );
    }

    #[test]
    fn event_id_rejects_malformed_sessions() {
        assert!(event_id("zzz", 1).is_none());
    }
}
