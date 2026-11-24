//! The bridge configuration file (`~/.config/kian/imessage-bridge.json`,
//! mode 0600) and its fail-closed validation.
//!
//! The token is read from a separate 0600 file (the same secret the
//! server holds in `KIAN_IMESSAGE_BRIDGE_TOKEN`); a group/world-readable
//! token file refuses startup rather than being silently trusted. The
//! listener must be loopback: the ABI has no TLS story because it is
//! never exposed off-host (CHANNEL-IMESSAGE §3.1/§3.3).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

/// Config version this binary understands.
pub const CONFIG_VERSION: u32 = 1;

/// The bridge's config file.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    #[serde(default = "default_version")]
    pub version: u32,
    /// `host:port`; loopback only.
    pub listen: String,
    /// Path to the one-line bearer token file (0600).
    pub token_file: String,
    /// Absolute path to the `imsg` binary (the TCC grant is per path —
    /// never rely on `PATH` here).
    #[serde(default = "default_imsg")]
    pub imsg: String,
    /// The Messages database to read.
    #[serde(default = "default_db")]
    pub db: String,
    /// Disposable bridge state (the `rowid` cursor).
    #[serde(default = "default_state_dir")]
    pub state_dir: String,
    #[serde(default)]
    pub watch: WatchConfig,
}

fn default_version() -> u32 {
    CONFIG_VERSION
}
fn default_imsg() -> String {
    "/opt/homebrew/bin/imsg".into()
}
fn default_db() -> String {
    "~/Library/Messages/chat.db".into()
}
fn default_state_dir() -> String {
    "~/Library/Application Support/kian-imessage-bridge".into()
}

/// The read filter and polling knobs.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchConfig {
    /// Handles whose conversations may leave the Mac. Empty = every
    /// conversation (the health snapshot and `kian imessage check` say
    /// so loudly). This is a **privacy** filter, not an allowlist: the
    /// adapter applies the allowlist (CHANNEL-IMESSAGE §3.3).
    #[serde(default)]
    pub participants: Vec<String>,
    /// Bridge-side poll cadence while serving a long poll.
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// How many rows a `since_rowid: 0` tail replay walks back.
    #[serde(default = "default_catchup_rows")]
    pub catchup_rows: u32,
    /// Maximum rows a single `/v1/updates` response may carry.
    #[serde(default = "default_page_limit")]
    pub page_limit: u32,
    /// Ask `imsg messages.after` for attachment metadata (`attachments:
    /// true`). On by default; `"attachments": false` restores the
    /// phase-1 text-only read without a rebuild.
    #[serde(default = "default_watch_attachments")]
    pub attachments: bool,
    /// Largest file `/v1/attachment` returns; a bigger one answers
    /// `413` so one enormous video cannot be pulled onto the server
    /// just because a row named it.
    #[serde(default = "default_max_attachment_bytes")]
    pub max_attachment_bytes: u64,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            participants: Vec::new(),
            poll_interval_ms: default_poll_interval_ms(),
            catchup_rows: default_catchup_rows(),
            page_limit: default_page_limit(),
            attachments: default_watch_attachments(),
            max_attachment_bytes: default_max_attachment_bytes(),
        }
    }
}

fn default_poll_interval_ms() -> u64 {
    250
}
fn default_catchup_rows() -> u32 {
    200
}
fn default_page_limit() -> u32 {
    200
}
fn default_watch_attachments() -> bool {
    true
}
fn default_max_attachment_bytes() -> u64 {
    16 * 1024 * 1024
}

/// A validated bridge config (paths expanded, token loaded, loopback
/// enforced).
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    pub listen: SocketAddr,
    pub token: String,
    pub imsg: PathBuf,
    pub db: PathBuf,
    pub state_dir: PathBuf,
    pub participants: Vec<String>,
    pub poll_interval: std::time::Duration,
    pub catchup_rows: u32,
    pub page_limit: u32,
    /// `watch.attachments`: request attachment metadata per row.
    pub attachments: bool,
    /// `watch.max_attachment_bytes`: the `/v1/attachment` size cap.
    pub max_attachment_bytes: u64,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read the bridge config {}: {source}", path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("malformed bridge config: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Invalid(String),
}

impl BridgeConfig {
    /// Loads and validates the config file.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let file: FileConfig = serde_json::from_str(&text)?;
        if file.version != CONFIG_VERSION {
            return Err(ConfigError::Invalid(format!(
                "bridge config version {} is not supported (this bridge speaks {CONFIG_VERSION})",
                file.version
            )));
        }
        let listen: SocketAddr = file.listen.trim().parse().map_err(|_| {
            ConfigError::Invalid(format!(
                "listen `{}` is not a host:port address",
                file.listen
            ))
        })?;
        if !listen.ip().is_loopback() {
            return Err(ConfigError::Invalid(format!(
                "listen `{}` is not loopback: the bridge must never be exposed off-host — use \
                 an SSH remote-forward or a mesh VPN (CHANNEL-IMESSAGE §3.3)",
                listen
            )));
        }
        let token_file = expand(&file.token_file)
            .ok_or_else(|| ConfigError::Invalid("token_file must be a non-empty path".into()))?;
        let token = read_token(&token_file)?;
        let imsg = expand(&file.imsg)
            .ok_or_else(|| ConfigError::Invalid("imsg path must be non-empty".into()))?;
        if !imsg.is_file() {
            return Err(ConfigError::Invalid(format!(
                "imsg binary `{}` does not exist — install it with \
                 `brew install steipete/tap/imsg` (macOS 14+)",
                imsg.display()
            )));
        }
        let db = expand(&file.db)
            .ok_or_else(|| ConfigError::Invalid("db path must be non-empty".into()))?;
        let state_dir = expand(&file.state_dir)
            .ok_or_else(|| ConfigError::Invalid("state_dir must be non-empty".into()))?;
        if file.watch.poll_interval_ms == 0 {
            return Err(ConfigError::Invalid(
                "watch.poll_interval_ms must be greater than zero".into(),
            ));
        }
        if file.watch.page_limit == 0 || file.watch.page_limit > 500 {
            return Err(ConfigError::Invalid(
                "watch.page_limit must be between 1 and 500".into(),
            ));
        }
        if file.watch.max_attachment_bytes == 0 {
            return Err(ConfigError::Invalid(
                "watch.max_attachment_bytes must be greater than zero".into(),
            ));
        }
        Ok(Self {
            listen,
            token,
            imsg,
            db,
            state_dir,
            participants: file.watch.participants,
            poll_interval: std::time::Duration::from_millis(file.watch.poll_interval_ms),
            catchup_rows: file.watch.catchup_rows,
            page_limit: file.watch.page_limit,
            attachments: file.watch.attachments,
            max_attachment_bytes: file.watch.max_attachment_bytes,
        })
    }
}

/// Expands a leading `~/` against `$HOME` (a launchd agent has no shell
/// to do it for us).
#[must_use]
pub fn expand(raw: &str) -> Option<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(rest) = trimmed.strip_prefix("~/") {
        let home = std::env::var("HOME").ok()?;
        return Some(PathBuf::from(home).join(rest));
    }
    Some(PathBuf::from(trimmed))
}

/// Reads the bearer token, refusing a group/world-readable file.
fn read_token(path: &Path) -> Result<String, ConfigError> {
    let token = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let token = token.trim().to_string();
    if token.len() < 16 {
        return Err(ConfigError::Invalid(format!(
            "token file `{}` must hold a token of at least 16 characters \
             (`openssl rand -hex 32 > …`)",
            path.display()
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .map_err(|source| ConfigError::Io {
                path: path.to_path_buf(),
                source,
            })?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            return Err(ConfigError::Invalid(format!(
                "token file `{}` is group/world-accessible (mode {:o}); chmod 600 it",
                path.display(),
                mode & 0o777
            )));
        }
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, name: &str, body: &str, mode: u32) -> PathBuf {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        let mut file = std::fs::File::create(&path).expect("create");
        file.write_all(body.as_bytes()).expect("write");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("mode");
        path
    }

    fn config_json(dir: &Path, listen: &str, token_mode: u32) -> PathBuf {
        let token = write(dir, "token", &"a".repeat(64), token_mode);
        let imsg = write(dir, "imsg", "#!/bin/sh\nexit 0\n", 0o755);
        let body = serde_json::json!({
            "version": 1,
            "listen": listen,
            "token_file": token.to_string_lossy(),
            "imsg": imsg.to_string_lossy(),
            "db": "/tmp/does-not-exist-chat.db",
            "state_dir": dir.join("state").to_string_lossy(),
        });
        write(
            dir,
            "config.json",
            &serde_json::to_string(&body).unwrap(),
            0o600,
        )
    }

    #[test]
    fn a_loopback_config_loads() {
        let dir = tempfile_dir();
        let path = config_json(&dir, "127.0.0.1:7420", 0o600);
        let config = BridgeConfig::load(&path).expect("loads");
        assert_eq!(config.listen.to_string(), "127.0.0.1:7420");
        assert_eq!(config.token.len(), 64);
        assert_eq!(config.page_limit, 200);
        assert_eq!(config.poll_interval.as_millis(), 250);
    }

    #[test]
    fn a_non_loopback_listener_refuses() {
        let dir = tempfile_dir();
        let path = config_json(&dir, "0.0.0.0:7420", 0o600);
        let err = BridgeConfig::load(&path).unwrap_err().to_string();
        assert!(err.contains("not loopback"), "{err}");
    }

    #[test]
    fn a_world_readable_token_refuses() {
        let dir = tempfile_dir();
        let path = config_json(&dir, "127.0.0.1:7420", 0o644);
        let err = BridgeConfig::load(&path).unwrap_err().to_string();
        assert!(err.contains("group/world-accessible"), "{err}");
    }

    #[test]
    fn a_short_token_refuses() {
        let dir = tempfile_dir();
        let token = write(&dir, "token", "short", 0o600);
        let imsg = write(&dir, "imsg", "#!/bin/sh\n", 0o755);
        let body = serde_json::json!({
            "version": 1,
            "listen": "127.0.0.1:7420",
            "token_file": token.to_string_lossy(),
            "imsg": imsg.to_string_lossy(),
            "state_dir": dir.join("state").to_string_lossy(),
        });
        let path = write(
            &dir,
            "config.json",
            &serde_json::to_string(&body).unwrap(),
            0o600,
        );
        let err = BridgeConfig::load(&path).unwrap_err().to_string();
        assert!(err.contains("at least 16 characters"), "{err}");
    }

    #[test]
    fn a_missing_imsg_binary_refuses() {
        let dir = tempfile_dir();
        let token = write(&dir, "token", &"a".repeat(64), 0o600);
        let body = serde_json::json!({
            "version": 1,
            "listen": "127.0.0.1:7420",
            "token_file": token.to_string_lossy(),
            "imsg": dir.join("missing").to_string_lossy(),
            "state_dir": dir.join("state").to_string_lossy(),
        });
        let path = write(
            &dir,
            "config.json",
            &serde_json::to_string(&body).unwrap(),
            0o600,
        );
        let err = BridgeConfig::load(&path).unwrap_err().to_string();
        assert!(
            err.contains("brewed") || err.contains("brew install"),
            "{err}"
        );
    }

    #[test]
    fn expand_handles_home_and_absolute_paths() {
        let home = std::env::var("HOME").expect("HOME");
        assert_eq!(
            expand("~/x").expect("home").to_string_lossy(),
            format!("{home}/x")
        );
        assert_eq!(expand("/abs").expect("abs"), PathBuf::from("/abs"));
        assert_eq!(expand("  "), None);
    }

    fn tempfile_dir() -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("kian-imessage-bridge-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }
}
