//! The WeChat reply-token state file (ADR-026).
//!
//! The iLink reply path echoes the latest inbound `context_token`. The
//! channel keeps that per admitted user in memory; this file is the
//! **best-effort durable bridge** across a `kian serve` restart, so a
//! task that completes after a restart can still reply without waiting
//! for the user to message again. It is deliberately outside the event
//! stream (a transport token must never enter the audit/export surface)
//! and outside the credential file (the login flow owns that schema).
//!
//! File contract: JSON `{version, tokens: {user_id: {token, updated_at_unix_ms}}}`,
//! mode `0600` (parent `0700` on unix), atomic temp+rename; a missing,
//! malformed or old-version file is treated as empty (best-effort), never
//! a startup failure. Entries older than the configured TTL are dropped
//! on load; the token is never logged.

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The only context-state schema version understood.
pub const CONTEXT_STATE_VERSION: u32 = 1;

/// Default reply-token TTL (1 hour): long enough to bridge a restart or a
/// short outage, short enough that a stale capability is not kept around.
pub const DEFAULT_CONTEXT_TTL: std::time::Duration = std::time::Duration::from_secs(3600);

/// Default state-file path derived from the credential file
/// (`<credentials>.context-state.json`). Both live in the same
/// private directory; the caller's explicit config can override it.
#[must_use]
pub fn default_state_path(credentials_file: &Path) -> PathBuf {
    let mut name = credentials_file
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(".context-state.json");
    credentials_file.with_file_name(name)
}

/// One cached reply token plus its observation time (server clock).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedToken {
    /// The iLink `context_token` (a reply capability, not the bot token).
    pub token: String,
    pub updated_at_unix_ms: u64,
}

impl fmt::Debug for CachedToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedToken")
            .field("token", &"<redacted>")
            .field("updated_at_unix_ms", &self.updated_at_unix_ms)
            .finish()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct StateFile {
    version: u32,
    tokens: HashMap<String, CachedToken>,
}

/// Loads the state file, dropping expired entries. A missing file is an
/// empty state; a malformed / wrong-version / unreadable file is also an
/// empty state (the caller logs one warn). The token value is never
/// included in an error or log path.
#[must_use]
pub fn load(path: &Path, ttl: std::time::Duration, now_ms: u64) -> HashMap<String, CachedToken> {
    let Ok(text) = fs::read_to_string(path) else {
        return HashMap::new();
    };
    let Ok(state) = serde_json::from_str::<StateFile>(&text) else {
        return HashMap::new();
    };
    if state.version != CONTEXT_STATE_VERSION {
        return HashMap::new();
    }
    state
        .tokens
        .into_iter()
        .filter(|(user, entry)| {
            !user.trim().is_empty()
                && !entry.token.is_empty()
                && now_ms.saturating_sub(entry.updated_at_unix_ms) <= ttl.as_millis() as u64
        })
        .collect()
}

/// Atomically persists the current token map (0600). Best-effort: the
/// caller logs a failure and keeps the in-memory map.
pub fn save(path: &Path, tokens: &HashMap<String, CachedToken>) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        create_private_dir_all(parent)?;
    }
    let state = StateFile {
        version: CONTEXT_STATE_VERSION,
        tokens: tokens.clone(),
    };
    let body = serde_json::to_vec_pretty(&state)
        .map_err(|e| std::io::Error::other(format!("serialize context state: {e}")))?;
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4().simple()));
    let result = (|| -> std::io::Result<()> {
        let mut file = open_private(&tmp)?;
        file.write_all(&body)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, path)
    })();
    if let Err(e) = result {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
}

fn open_private(path: &Path) -> std::io::Result<fs::File> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn create_private_dir_all(parent: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)
    }
    #[cfg(not(unix))]
    {
        fs::create_dir_all(parent)
    }
}

#[cfg(test)]
mod tests {
    use super::{CONTEXT_STATE_VERSION, CachedToken, default_state_path, load, save};
    use std::collections::HashMap;
    use std::time::Duration;

    fn temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("kian-wechat-state-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn entry(token: &str, ts: u64) -> CachedToken {
        CachedToken {
            token: token.into(),
            updated_at_unix_ms: ts,
        }
    }

    #[test]
    fn save_load_round_trip_and_ttl_filtering() {
        let dir = temp_dir();
        let path = dir.join("state.json");
        let mut tokens = HashMap::new();
        tokens.insert("user-a".to_string(), entry("tok-a", 1_000));
        tokens.insert("user-b".to_string(), entry("tok-b", 1));
        save(&path, &tokens).unwrap();
        let loaded = load(&path, Duration::from_millis(100), 1_050);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded["user-a"].token, "tok-a");
        // Expired entries are dropped, not returned stale.
        assert!(load(&path, Duration::from_millis(100), 10_000).is_empty());
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn missing_or_malformed_state_is_empty_not_fatal() {
        let dir = temp_dir();
        let path = dir.join("state.json");
        assert!(load(&path, Duration::from_secs(60), 1).is_empty());
        std::fs::write(&path, b"not-json").unwrap();
        assert!(load(&path, Duration::from_secs(60), 1).is_empty());
        std::fs::write(&path, "{\"version\":999,\"tokens\":{}}").unwrap();
        assert!(load(&path, Duration::from_secs(60), 1).is_empty());
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn state_file_is_private_and_path_is_derived() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let dir = temp_dir();
            let path = dir.join("state.json");
            save(&path, &HashMap::new()).unwrap();
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "context state is 0600");
            std::fs::remove_dir_all(dir).ok();
        }
        assert_eq!(
            default_state_path(std::path::Path::new("/srv/kian/wechat.json")),
            std::path::PathBuf::from("/srv/kian/wechat.json.context-state.json")
        );
        assert_eq!(CONTEXT_STATE_VERSION, 1);
    }
}
