//! Voice payloads: one caller-supplied audio blob, decoded, capped and
//! staged for `imsg` (CHANNEL-IMESSAGE §3.2, ADR-066).

use std::io::Write;
use std::path::{Path, PathBuf};

/// The largest audio payload `/v1/send` accepts (decoded bytes).
pub const MAX_VOICE_BYTES: usize = 1024 * 1024;

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// A staged audio file; removed when it goes out of scope, so every exit
/// path (deduplicated, refused, delivered, dropped) cleans up.
#[derive(Debug)]
pub struct Staged {
    path: PathBuf,
    bytes: usize,
}

impl Staged {
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The decoded payload length (reported as `voice_bytes`).
    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for Staged {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Decodes standard padded base64; `None` on malformed or non-canonical
/// input.
#[must_use]
pub fn decode_base64(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let mut pad = 0usize;
        let mut value: u32 = 0;
        for (index, byte) in chunk.iter().enumerate() {
            let digit = if *byte == b'=' {
                if index < 2 {
                    return None;
                }
                pad += 1;
                0
            } else {
                if pad > 0 {
                    return None;
                }
                u32::try_from(ALPHABET.iter().position(|candidate| candidate == byte)?).ok()?
            };
            value = (value << 6) | digit;
        }
        out.push((value >> 16) as u8);
        if pad < 2 {
            out.push((value >> 8) as u8);
        }
        if pad < 1 {
            out.push(value as u8);
        }
    }
    Some(out)
}

/// The file extension Messages keys off when it stages the attachment.
#[must_use]
pub fn extension(media_type: &str) -> &'static str {
    let base = media_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match base.as_str() {
        "audio/mpeg" | "audio/mp3" => "mp3",
        "audio/mp4" | "audio/m4a" | "audio/x-m4a" => "m4a",
        "audio/wav" | "audio/x-wav" | "audio/wave" => "wav",
        "audio/ogg" | "audio/opus" => "ogg",
        "audio/aac" => "aac",
        "audio/caf" | "audio/x-caf" => "caf",
        _ => "bin",
    }
}

/// Writes `bytes` to a private (`0600`) temp file `imsg` can read.
///
/// A write failure removes the partial file before the error propagates.
pub fn stage(bytes: &[u8], media_type: &str) -> std::io::Result<Staged> {
    let path = std::env::temp_dir().join(format!(
        "kian-voice-{}.{}",
        uuid::Uuid::new_v4(),
        extension(media_type)
    ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path)?;
    let staged = Staged {
        path,
        bytes: bytes.len(),
    };
    file.write_all(bytes)?;
    file.flush()?;
    Ok(staged)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_round_trips_padded_input() {
        assert_eq!(decode_base64(""), Some(Vec::new()));
        assert_eq!(decode_base64("YQ=="), Some(b"a".to_vec()));
        assert_eq!(decode_base64("YWI="), Some(b"ab".to_vec()));
        assert_eq!(decode_base64("YWJj"), Some(b"abc".to_vec()));
    }

    #[test]
    fn base64_rejects_malformed_input() {
        assert_eq!(decode_base64("YQ"), None);
        assert_eq!(decode_base64("YQ=a"), None);
        assert_eq!(decode_base64("=Q=="), None);
        assert_eq!(decode_base64("YQ!!"), None);
    }

    #[test]
    fn media_types_map_to_message_extensions() {
        assert_eq!(extension("audio/mpeg"), "mp3");
        assert_eq!(extension("audio/mp4"), "m4a");
        assert_eq!(extension("audio/wav"), "wav");
        assert_eq!(extension("AUDIO/MPEG"), "mp3");
        assert_eq!(extension("audio/wav; codecs=1"), "wav");
        assert_eq!(extension("application/octet-stream"), "bin");
    }

    #[test]
    fn staging_writes_a_private_file_and_drop_removes_it() {
        let staged = stage(b"audio-bytes", "audio/mpeg").unwrap();
        let path = staged.path().to_path_buf();
        assert_eq!(std::fs::read(&path).unwrap(), b"audio-bytes");
        assert_eq!(path.extension().and_then(|ext| ext.to_str()), Some("mp3"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "staged voice is private");
        }
        drop(staged);
        assert!(!path.exists(), "the staged file is removed on drop");
    }
}
