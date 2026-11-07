//! Memory caps, parsed fail-closed.
//!
//! `MemoryConfig` is the caps half of the config block kian-87 owns
//! (`memory.dreamer.{max_bytes_read, max_notes}`): the caller that
//! renders the dream prompt and admits the dream row reads the source
//! range under `max_bytes_read` and hands `max_notes` to the parser /
//! driver. Parsing is fail-closed (MEMORY-MODEL D7): unknown keys,
//! non-positive or non-integer values refuse — an absent block is the
//! caller's "memory off", not a config error.

use serde_json::Value;

/// Default source-read budget: 256 KiB of transcript.
pub const DEFAULT_MAX_BYTES_READ: u64 = 262_144;
/// Default per-answer note cap (matches the injection `max_items`
/// discipline of MEMORY-MODEL §6 I4).
pub const DEFAULT_MAX_NOTES: u64 = 20;

/// Why a memory config block was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MemoryConfigError {
    #[error("memory config must be a JSON object")]
    NotAnObject,
    #[error("unknown memory config key `{0}`")]
    UnknownKey(String),
    #[error("memory config field `{0}` must be a positive integer")]
    Invalid(String),
}

/// The dreamer caps (MEMORY-MODEL D7). Config-first: pure values, no
/// behavior — the caller enforces them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryConfig {
    /// Maximum bytes of source transcript one dream may read.
    pub max_bytes_read: u64,
    /// Maximum notes one dream answer may carry; over-cap is refused.
    pub max_notes: u64,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            max_bytes_read: DEFAULT_MAX_BYTES_READ,
            max_notes: DEFAULT_MAX_NOTES,
        }
    }
}

impl MemoryConfig {
    /// Fail-closed parse of the caps block: an object whose keys are a
    /// subset of `{max_bytes_read, max_notes}`, each a positive
    /// integer. Anything else refuses.
    pub fn from_value(block: &Value) -> Result<Self, MemoryConfigError> {
        let obj = block.as_object().ok_or(MemoryConfigError::NotAnObject)?;
        let mut config = Self::default();
        for (key, value) in obj {
            match key.as_str() {
                "max_bytes_read" => {
                    config.max_bytes_read = positive(value, "max_bytes_read")?;
                }
                "max_notes" => {
                    config.max_notes = positive(value, "max_notes")?;
                }
                unknown => return Err(MemoryConfigError::UnknownKey(unknown.to_string())),
            }
        }
        Ok(config)
    }
}

/// A positive integer (`as_u64` rejects negatives and floats).
fn positive(value: &Value, field: &'static str) -> Result<u64, MemoryConfigError> {
    match value.as_u64() {
        Some(n) if n > 0 => Ok(n),
        _ => Err(MemoryConfigError::Invalid(field.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Defaults are valid and used when the block is empty.
    #[test]
    fn defaults_apply_on_an_empty_block() {
        assert_eq!(
            MemoryConfig::from_value(&json!({})),
            Ok(MemoryConfig::default())
        );
        assert_eq!(
            MemoryConfig::default().max_bytes_read,
            DEFAULT_MAX_BYTES_READ
        );
        assert_eq!(MemoryConfig::default().max_notes, DEFAULT_MAX_NOTES);
    }

    /// Explicit values override; both together parse.
    #[test]
    fn explicit_values_parse() {
        assert_eq!(
            MemoryConfig::from_value(&json!({"max_notes": 5})),
            Ok(MemoryConfig {
                max_notes: 5,
                ..MemoryConfig::default()
            })
        );
        assert_eq!(
            MemoryConfig::from_value(&json!({"max_bytes_read": 1024, "max_notes": 5})),
            Ok(MemoryConfig {
                max_bytes_read: 1024,
                max_notes: 5,
            })
        );
    }

    /// Fail-closed matrix: wrong shape, unknown keys, zero, negative,
    /// fractional, non-number.
    #[test]
    fn parse_is_fail_closed() {
        for (label, block) in [
            ("not an object", json!("nope")),
            ("array", json!([])),
            ("unknown key", json!({"cadence": "hourly"})),
            ("zero max_notes", json!({"max_notes": 0})),
            ("zero max_bytes_read", json!({"max_bytes_read": 0})),
            ("negative", json!({"max_notes": -1})),
            ("fractional", json!({"max_notes": 1.5})),
            ("string", json!({"max_notes": "5"})),
        ] {
            assert!(
                MemoryConfig::from_value(&block).is_err(),
                "{label} must be refused"
            );
        }
    }
}
