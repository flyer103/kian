//! The WeChat credential file: schema, atomic 0600 write, mtime probe.
//!
//! Contract: `design/CHANNEL-WECHAT.md` §6 — the bot token is issued by
//! a QR login and lives ONLY in this file (never an env var, never
//! `kian.yaml`, never git, never a log). Errors and `Debug` renders are
//! redacted by construction: no variant or format string here ever
//! carries the token, only the path.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

/// The only credential schema version understood (anything else is a
/// fail-closed `BadVersion`).
pub const CREDENTIAL_VERSION: u32 = 1;

/// The credential file (CHANNEL-WECHAT §6). `Debug` is manual and
/// redacts the token — the struct may be logged by accident, the token
/// may not.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WechatCredentials {
    /// Schema version; must be [`CREDENTIAL_VERSION`].
    pub version: u32,
    /// The iLink API base the token was issued for.
    pub base_url: String,
    /// The bot token (`bot_token` from the QR login). A credential.
    pub token: String,
    /// `ilink_bot_id` from the login.
    pub account_id: String,
    /// `ilink_user_id` from the login.
    pub user_id: String,
    /// When the file was written, unix epoch ms.
    pub saved_at_unix_ms: u64,
}

impl fmt::Debug for WechatCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WechatCredentials")
            .field("version", &self.version)
            .field("base_url", &self.base_url)
            .field("token", &"<redacted>")
            .field("account_id", &self.account_id)
            .field("user_id", &self.user_id)
            .field("saved_at_unix_ms", &self.saved_at_unix_ms)
            .finish()
    }
}

impl WechatCredentials {
    /// Fixture/test constructor (version pinned to 1).
    pub fn new(
        base_url: impl Into<String>,
        token: impl Into<String>,
        account_id: impl Into<String>,
        user_id: impl Into<String>,
        saved_at_unix_ms: u64,
    ) -> Self {
        Self {
            version: CREDENTIAL_VERSION,
            base_url: base_url.into(),
            token: token.into(),
            account_id: account_id.into(),
            user_id: user_id.into(),
            saved_at_unix_ms,
        }
    }
}

/// Credential-file failures. Every message names the path and — where
/// the operator can act — the `kian wechat login` command; none carries
/// the token or the file contents.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CredentialError {
    #[error("wechat credential file `{}` is missing; run `kian wechat login`", path.display())]
    Missing { path: PathBuf },
    #[error("wechat credential file `{}` is unreadable: {detail}", path.display())]
    Unreadable { path: PathBuf, detail: String },
    #[error("wechat credential file `{}` is malformed: {detail}; run `kian wechat login`", path.display())]
    Malformed { path: PathBuf, detail: String },
    #[error(
        "wechat credential file `{}` has unsupported version {found} (expected {CREDENTIAL_VERSION}); run `kian wechat login`",
        path.display()
    )]
    BadVersion { path: PathBuf, found: u32 },
    #[error("cannot write wechat credential file `{}`: {detail}", path.display())]
    WriteFailed { path: PathBuf, detail: String },
}

/// Loads and validates the credential file (fail-closed: missing,
/// unreadable, malformed, wrong version or empty token are all errors
/// naming the path).
pub fn load(path: &Path) -> Result<WechatCredentials, CredentialError> {
    let text = fs::read_to_string(path).map_err(|e| match e.kind() {
        std::io::ErrorKind::NotFound => CredentialError::Missing {
            path: path.to_path_buf(),
        },
        _ => CredentialError::Unreadable {
            path: path.to_path_buf(),
            detail: e.to_string(),
        },
    })?;
    let credentials: WechatCredentials =
        serde_json::from_str(&text).map_err(|e| CredentialError::Malformed {
            path: path.to_path_buf(),
            detail: e.to_string(),
        })?;
    if credentials.version != CREDENTIAL_VERSION {
        return Err(CredentialError::BadVersion {
            path: path.to_path_buf(),
            found: credentials.version,
        });
    }
    if credentials.token.is_empty() {
        return Err(CredentialError::Malformed {
            path: path.to_path_buf(),
            detail: "empty token".to_string(),
        });
    }
    Ok(credentials)
}

/// Writes the credential file atomically with mode 0600: a temp file in
/// the SAME directory (same filesystem ⇒ atomic rename), permission
/// bits asserted on the temp file before the rename, then `rename`
/// replaces the live file in one step — a reader never sees a
/// half-written file, and the mode never opens a window.
pub fn save(path: &Path, credentials: &WechatCredentials) -> Result<(), CredentialError> {
    use std::io::Write;
    // A relative `path` resolves against the process working directory
    // (documented in CHANNEL-WECHAT §6) — `kian serve` and
    // `kian wechat login` must agree on it.
    if let Some(parent) = path.parent() {
        create_private_dir_all(parent).map_err(|e| CredentialError::WriteFailed {
            path: path.to_path_buf(),
            detail: format!("cannot create {}: {e}", parent.display()),
        })?;
    }
    let body =
        serde_json::to_vec_pretty(credentials).map_err(|e| CredentialError::WriteFailed {
            path: path.to_path_buf(),
            detail: format!("serialize: {e}"),
        })?;
    let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4().simple()));
    let write_result = (|| -> std::io::Result<()> {
        let mut file = open_private(&tmp)?;
        file.write_all(&body)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, path)
    })();
    if let Err(e) = write_result {
        let _ = fs::remove_file(&tmp); // best-effort cleanup, never masks the error
        return Err(CredentialError::WriteFailed {
            path: path.to_path_buf(),
            detail: e.to_string(),
        });
    }
    Ok(())
}

/// Creates the credential file's parent chain with mode `0700` on unix
/// (a credential's directory is not group/world-walkable; existing
/// directories keep their bits — only newly created ones get `0700`).
fn create_private_dir_all(parent: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(parent)
    }
}

/// Opens a fresh file for writing with 0600 semantics (unix: the mode
/// is set at creation; elsewhere: default create-new semantics).
fn open_private(path: &Path) -> std::io::Result<std::fs::File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
    }
    #[cfg(not(unix))]
    {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
    }
}

/// The file's mtime, when it exists and the filesystem reports it — the
/// cheap change signal the `-14` pause watches (≤5 s cadence).
pub fn mtime(path: &Path) -> Option<SystemTime> {
    fs::metadata(path).ok().and_then(|m| m.modified().ok())
}

/// Some(mode) when the file is group- or world-readable — reported, not
/// silently trusted, at startup (CHANNEL-WECHAT §6). Non-unix platforms
/// have no mode bits to check: `None`.
pub fn insecure_permissions(path: &Path) -> Option<u32> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(path).ok()?.permissions().mode();
        (mode & 0o077 != 0).then_some(mode)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(tag: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("kian-wechat-cred-{tag}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).expect("temp dir");
        dir.join("wechat-credentials.json")
    }

    #[test]
    fn save_then_load_roundtrips_with_private_mode() {
        let path = temp_path("roundtrip");
        let credentials = WechatCredentials::new(
            "https://ilinkai.weixin.qq.com",
            "fake-bot-token-1",
            "bot-1",
            "user-1",
            1730000000000,
        );
        save(&path, &credentials).expect("save");
        let loaded = load(&path).expect("load");
        assert_eq!(loaded, credentials);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).expect("meta").permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "the credential file is 0600");
        }
        assert!(insecure_permissions(&path).is_none(), "0600 is secure");
        fs::remove_file(&path).ok();
    }

    #[test]
    fn no_temp_files_survive_a_save() {
        let path = temp_path("atomic");
        save(
            &path,
            &WechatCredentials::new("https://x", "t", "a", "u", 1),
        )
        .expect("save");
        let siblings: Vec<_> = fs::read_dir(path.parent().expect("parent"))
            .expect("list")
            .map(|e| e.expect("entry").file_name())
            .collect();
        assert_eq!(
            siblings.len(),
            1,
            "only the credential file remains: {siblings:?}"
        );
        fs::remove_file(&path).ok();
    }

    #[test]
    fn world_readable_files_are_reported() {
        let path = temp_path("perms");
        save(
            &path,
            &WechatCredentials::new("https://x", "t", "a", "u", 1),
        )
        .expect("save");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("chmod");
            let reported = insecure_permissions(&path).expect("insecure mode is reported");
            assert_eq!(
                reported & 0o777,
                0o644,
                "mode bits without the file-type bits"
            );
        }
        fs::remove_file(&path).ok();
    }

    #[test]
    fn missing_malformed_and_wrong_version_fail_closed_without_leaking() {
        let path = temp_path("failures");
        let missing = load(&path).expect_err("missing file errors");
        assert!(matches!(missing, CredentialError::Missing { .. }));
        assert!(missing.to_string().contains("kian wechat login"));

        fs::write(&path, "{ not json").expect("write garbage");
        let malformed = load(&path).expect_err("garbage errors");
        assert!(matches!(malformed, CredentialError::Malformed { .. }));
        assert!(
            !malformed.to_string().contains("not json"),
            "no file contents in errors"
        );

        fs::write(
            &path,
            r#"{"version": 2, "base_url": "https://x", "token": "fake-bot-token-9",
                "account_id": "a", "user_id": "u", "saved_at_unix_ms": 1}"#,
        )
        .expect("write v2");
        let bad_version = load(&path).expect_err("version 2 errors");
        assert!(matches!(
            bad_version,
            CredentialError::BadVersion { found: 2, .. }
        ));
        assert!(!bad_version.to_string().contains("fake-bot-token-9"));

        fs::write(
            &path,
            r#"{"version": 1, "base_url": "https://x", "token": "",
                "account_id": "a", "user_id": "u", "saved_at_unix_ms": 1}"#,
        )
        .expect("write empty token");
        assert!(matches!(
            load(&path),
            Err(CredentialError::Malformed { .. })
        ));
        fs::remove_file(&path).ok();
    }

    #[test]
    fn debug_and_serialization_redact_or_isolate_the_token() {
        let credentials = WechatCredentials::new("https://x", "fake-bot-token-debug", "a", "u", 7);
        let rendered = format!("{credentials:?}");
        assert!(!rendered.contains("fake-bot-token-debug"), "Debug redacts");
        assert!(rendered.contains("<redacted>"));
        // The serialized FILE does carry the token (it is the token's
        // only home); the JSON is never logged by adapter code.
        let json = serde_json::to_string(&credentials).expect("serialize");
        assert!(json.contains("fake-bot-token-debug"));
    }

    #[test]
    fn a_created_parent_directory_is_0700() {
        let root =
            std::env::temp_dir().join(format!("kian-wechat-cred-dir-{}", uuid::Uuid::new_v4()));
        let path = root.join("nested").join("wechat-credentials.json");
        save(
            &path,
            &WechatCredentials::new("https://x", "t", "a", "u", 1),
        )
        .expect("save creates the chain");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for dir in [&root, &root.join("nested")] {
                let mode = fs::metadata(dir).expect("meta").permissions().mode();
                assert_eq!(mode & 0o777, 0o700, "{} is 0700", dir.display());
            }
        }
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn mtime_changes_after_a_resave() {
        let path = temp_path("mtime");
        save(
            &path,
            &WechatCredentials::new("https://x", "t1", "a", "u", 1),
        )
        .expect("save");
        let first = mtime(&path).expect("mtime");
        std::thread::sleep(std::time::Duration::from_millis(20));
        save(
            &path,
            &WechatCredentials::new("https://x", "t2", "a", "u", 2),
        )
        .expect("resave");
        let second = mtime(&path).expect("mtime");
        assert!(second > first, "a re-login must move the mtime");
        assert_eq!(mtime(Path::new("/nonexistent-kian-wechat")), None);
        fs::remove_file(&path).ok();
    }
}
