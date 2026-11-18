//! Port (d″) — the crate-local `tool/post` port (PLUGIN-SPEC §3.5/§4.4
//! step 5, dsh site).
//!
//! The bridge observes the child's already-executed tool records after
//! the `tool/result` append, retrospectively and observe-only: a dsh
//! child tool call cannot be vetoed before its effect, and this crate
//! does not pretend otherwise. The view's serialization is
//! **byte-identical** to PLUGIN-SPEC §3.2.6 (field names, order,
//! nesting), so the assembly-side adapter that forwards to the real
//! `kian-plugin` bus needs no translation — but `kian-dsh` itself
//! names no `kian-plugin` type and keeps its `kian-core`-only waist
//! (§2, `tests/narrow_waist.rs`).
//!
//! `output_sha256` is computed by a local, std-only SHA-256: the
//! round's one allowed new dependency is `sha2` in `kian-plugin`
//! (K10), and this crate may depend on neither.

use serde::Serialize;

/// One aggregate redaction report entry (§3.2.6) — counts only, never
/// values. The dsh bridge's mapped `tool/result` carries no redaction
/// provenance (the child's own executor owns any scrubbing), so v1
/// always ships an empty list.
#[derive(Debug, Clone, Serialize)]
pub struct RedactionReport {
    pub kind: String,
    pub name: String,
    pub count: u64,
}

/// The `tool/post` view (§3.2.6, byte-identical serialization). No
/// tool output content is shipped in v1.
#[derive(Debug, Clone, Serialize)]
pub struct ToolPostView {
    pub call_id: String,
    pub name: String,
    pub is_error: bool,
    pub output_bytes: u64,
    pub output_sha256: String,
    pub redactions: Vec<RedactionReport>,
}

/// The crate-local port the driver calls at its post-append site
/// (§3.5: `driver.rs`'s `tool/result` branch, after `ctx.append`
/// returns `Ok`). Observe-only by construction — the trait has no
/// verdict. Implemented by assembly over the real hook bus.
pub trait ToolPostPort: Send + Sync {
    fn tool_post(&self, view: ToolPostView) -> futures::future::BoxFuture<'static, ()>;
}

/// SHA-256, lowercase hex — std-only (FIPS 180-4). Pinned to the
/// standard test vectors below; used only for the view's digest field.
pub fn sha256_hex(data: &[u8]) -> String {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
        0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
        0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
        0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
        0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
        0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
        0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
        0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
        0xc67178f2,
    ];

    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    // Padding: 0x80, zeros to 56 mod 64, then the big-endian bit length.
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut message = data.to_vec();
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());

    for block in message.as_chunks::<64>().0 {
        let mut w = [0u32; 64];
        for (i, chunk) in block.as_chunks::<4>().0.iter().enumerate() {
            w[i] = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let (mut a, mut b, mut c, mut d) = (h[0], h[1], h[2], h[3]);
        let (mut e, mut f, mut g, mut hh) = (h[4], h[5], h[6], h[7]);
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ (!e & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    h.iter().map(|word| format!("{word:08x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_matches_the_standard_vectors() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            sha256_hex(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        // One full block plus padding boundary (exactly 55 bytes).
        assert_eq!(
            sha256_hex(&[0x61; 55]),
            "9f4390f8d30c2dd92ec9f095b65e2b9ae9b0a925a5258e241c9f1e910f734318"
        );
        // Two blocks (exactly 64 bytes).
        assert_eq!(
            sha256_hex(&[0x61; 64]),
            "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb"
        );
    }

    #[test]
    fn view_serialization_is_byte_identical_to_spec_3_2_6() {
        let view = ToolPostView {
            call_id: "call_abc".to_string(),
            name: "exec".to_string(),
            is_error: false,
            output_bytes: 812,
            output_sha256: "44b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"
                .to_string(),
            redactions: vec![RedactionReport {
                kind: "env_line".to_string(),
                name: "OPENAI_API_KEY".to_string(),
                count: 1,
            }],
        };
        assert_eq!(
            serde_json::to_string(&view).unwrap(),
            concat!(
                r#"{"call_id":"call_abc","name":"exec","is_error":false,"output_bytes":812,"#,
                r#""output_sha256":"44b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2","#,
                r#""redactions":[{"kind":"env_line","name":"OPENAI_API_KEY","count":1}]}"#
            ),
            "§3.2.6: keys, order and nesting are the closed ABI"
        );

        // Empty redactions serialize as [], never null.
        let bare = ToolPostView {
            redactions: Vec::new(),
            ..view
        };
        assert!(
            serde_json::to_string(&bare)
                .unwrap()
                .ends_with(r#""redactions":[]}"#)
        );
    }

    #[test]
    fn view_digest_describes_the_serialized_output() {
        // The view-building rule the driver site applies: bytes and
        // digest over the compact serialization of the mapped output.
        let output = serde_json::json!({"exit_code": 0});
        let bytes = serde_json::to_vec(&output).unwrap();
        assert_eq!(bytes.len(), 15);
        assert_eq!(
            sha256_hex(&bytes),
            // sha256 of `{"exit_code":0}` — pinned so the site's digest
            // rule is fixed output, not implementation detail.
            "e81bc160c0843ee56f31f12997bb3aff50528d596002d4866676cb8a1b19195a"
        );
    }
}
