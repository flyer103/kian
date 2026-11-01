//! kiand configuration (card C22): flags + environment + optional
//! `kiand.yaml`, resolved with the precedence **flags > environment >
//! file > defaults**. Loading is fail-closed: a malformed value, an
//! unknown key or an unparseable `listen` address refuses to start with a
//! clear error — never a silent default.
//!
//! The default config path (`./kiand.yaml`) is *optional*: absence is not
//! an error. An explicitly passed `--config` path that does not exist
//! **is** an error.

use std::net::SocketAddr;
use std::path::Path;

use serde::Deserialize;

/// Default endpoint identity when nothing configures one.
pub const DEFAULT_ENDPOINT_ID: &str = "kiand";
/// Default listen address: loopback, kernel-assigned port. The bound
/// address is printed on stdout for registration.
pub const DEFAULT_LISTEN: &str = "127.0.0.1:0";
/// Concurrency default mirrors the endpoint library.
pub const DEFAULT_MAX_CONCURRENT: usize = kian_endpoint::endpoint::DEFAULT_MAX_CONCURRENT;
/// Heartbeat default mirrors the endpoint library (30s).
pub const DEFAULT_HEARTBEAT_MS: u64 = kian_endpoint::endpoint::DEFAULT_HEARTBEAT.as_millis() as u64;
/// Exec timeout default mirrors `kian-cli`'s tooling constant.
pub const DEFAULT_EXEC_TIMEOUT_MS: u64 = 30_000;

/// Optional config file, looked up at `./kiand.yaml` unless overridden.
pub const DEFAULT_CONFIG_FILE: &str = "kiand.yaml";

/// Environment keys (one per config field).
pub const ENV_ENDPOINT_ID: &str = "KIAND_ENDPOINT_ID";
pub const ENV_LISTEN: &str = "KIAND_LISTEN";
pub const ENV_MAX_CONCURRENT: &str = "KIAND_MAX_CONCURRENT";
pub const ENV_HEARTBEAT_MS: &str = "KIAND_HEARTBEAT_MS";
pub const ENV_EXEC_TIMEOUT_MS: &str = "KIAND_EXEC_TIMEOUT_MS";

/// Every way loading the daemon configuration can refuse to start.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read config file {path}: {source}")]
    Io {
        path: String,
        source: std::io::Error,
    },
    #[error("malformed config file {path}: {source}")]
    Yaml {
        path: String,
        source: serde_yaml::Error,
    },
    #[error("malformed environment: {0}")]
    Env(String),
    #[error("invalid config: {0}")]
    Invalid(String),
}

/// Values given on the command line (all optional; validated after
/// resolution).
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Flags {
    pub config: Option<std::path::PathBuf>,
    pub endpoint_id: Option<String>,
    pub listen: Option<String>,
    pub max_concurrent: Option<usize>,
    pub heartbeat_ms: Option<u64>,
    pub exec_timeout_ms: Option<u64>,
}

/// The `kiand.yaml` document: every key optional, unknown keys rejected.
#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub endpoint_id: Option<String>,
    pub listen: Option<String>,
    pub max_concurrent: Option<usize>,
    pub heartbeat_ms: Option<u64>,
    pub exec_timeout_ms: Option<u64>,
}

/// The resolved daemon configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub endpoint_id: String,
    pub listen: String,
    pub max_concurrent: usize,
    pub heartbeat_ms: u64,
    pub exec_timeout_ms: u64,
}

impl Config {
    /// Parse and validate the listen address (fail-closed).
    pub fn listen_addr(&self) -> Result<SocketAddr, ConfigError> {
        self.listen.parse().map_err(|_| {
            ConfigError::Invalid(format!("listen `{}` is not a valid host:port", self.listen))
        })
    }
}

/// Where environment values come from (injectable so resolution is pure
/// and unit-testable; the real entry point uses [`std::env::var`]).
pub type EnvGetter<'a> = &'a dyn Fn(&str) -> Option<String>;

/// Resolve the configuration from flags, the process environment and the
/// optional config file. Fail-closed at every step.
pub fn resolve(flags: Flags) -> Result<Config, ConfigError> {
    resolve_with(&|key| std::env::var(key).ok(), flags)
}

/// [`resolve`] with an injected environment getter.
pub fn resolve_with(env_get: EnvGetter<'_>, flags: Flags) -> Result<Config, ConfigError> {
    let file = load_file(flags.config.as_deref())?;

    let endpoint_id = flags
        .endpoint_id
        .or_else(|| env(env_get, ENV_ENDPOINT_ID))
        .or_else(|| file.as_ref().and_then(|f| f.endpoint_id.clone()))
        .unwrap_or_else(|| DEFAULT_ENDPOINT_ID.to_string());
    let listen = flags
        .listen
        .or_else(|| env(env_get, ENV_LISTEN))
        .or_else(|| file.as_ref().and_then(|f| f.listen.clone()))
        .unwrap_or_else(|| DEFAULT_LISTEN.to_string());
    let max_concurrent = flags
        .max_concurrent
        .or(env_usize(env_get, ENV_MAX_CONCURRENT)?)
        .or_else(|| file.as_ref().and_then(|f| f.max_concurrent))
        .unwrap_or(DEFAULT_MAX_CONCURRENT);
    let heartbeat_ms = flags
        .heartbeat_ms
        .or(env_u64(env_get, ENV_HEARTBEAT_MS)?)
        .or_else(|| file.as_ref().and_then(|f| f.heartbeat_ms))
        .unwrap_or(DEFAULT_HEARTBEAT_MS);
    let exec_timeout_ms = flags
        .exec_timeout_ms
        .or(env_u64(env_get, ENV_EXEC_TIMEOUT_MS)?)
        .or_else(|| file.as_ref().and_then(|f| f.exec_timeout_ms))
        .unwrap_or(DEFAULT_EXEC_TIMEOUT_MS);

    let config = Config {
        endpoint_id,
        listen,
        max_concurrent,
        heartbeat_ms,
        exec_timeout_ms,
    };
    validate(config)
}

/// Read + parse the config file. The default path is optional; an
/// explicit path must exist and parse.
fn load_file(path: Option<&Path>) -> Result<Option<FileConfig>, ConfigError> {
    let (path, explicit) = match path {
        Some(p) => (p, true),
        None => (Path::new(DEFAULT_CONFIG_FILE), false),
    };
    if !path.exists() {
        if explicit {
            return Err(ConfigError::Io {
                path: path.display().to_string(),
                source: std::io::Error::from(std::io::ErrorKind::NotFound),
            });
        }
        return Ok(None); // the optional default file is simply absent
    }
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.display().to_string(),
        source,
    })?;
    parse_file_text(path, &text).map(Some)
}

/// Parse one config-file body (path is for the error message only).
fn parse_file_text(path: &Path, text: &str) -> Result<FileConfig, ConfigError> {
    serde_yaml::from_str(text).map_err(|source| ConfigError::Yaml {
        path: path.display().to_string(),
        source,
    })
}

/// A string-valued environment entry.
fn env(env: EnvGetter<'_>, key: &str) -> Option<String> {
    env(key).filter(|v| !v.is_empty())
}

fn env_usize(env: EnvGetter<'_>, key: &str) -> Result<Option<usize>, ConfigError> {
    match env(key) {
        None => Ok(None),
        Some(raw) => raw
            .parse::<usize>()
            .map(Some)
            .map_err(|_| ConfigError::Env(format!("{key}=`{raw}` is not a non-negative integer"))),
    }
}

fn env_u64(env: EnvGetter<'_>, key: &str) -> Result<Option<u64>, ConfigError> {
    match env(key) {
        None => Ok(None),
        Some(raw) => raw
            .parse::<u64>()
            .map(Some)
            .map_err(|_| ConfigError::Env(format!("{key}=`{raw}` is not a non-negative integer"))),
    }
}

/// Cross-field validation: refuse anything the daemon cannot serve with.
fn validate(config: Config) -> Result<Config, ConfigError> {
    if config.endpoint_id.trim().is_empty() {
        return Err(ConfigError::Invalid("endpoint_id must not be empty".into()));
    }
    if config.max_concurrent < 1 {
        return Err(ConfigError::Invalid(
            "max_concurrent must be at least 1 (0 would deadlock every command)".into(),
        ));
    }
    if config.heartbeat_ms < 1 {
        return Err(ConfigError::Invalid(
            "heartbeat_ms must be at least 1".into(),
        ));
    }
    if config.exec_timeout_ms < 1 {
        return Err(ConfigError::Invalid(
            "exec_timeout_ms must be at least 1 (the daemon never runs unbounded commands)".into(),
        ));
    }
    // A bad listen address refuses to start before any socket opens.
    config.listen_addr()?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn no_env() -> BTreeMap<&'static str, String> {
        BTreeMap::new()
    }

    fn resolve(map: BTreeMap<&str, String>, flags: Flags) -> Result<Config, ConfigError> {
        let env = move |key: &str| map.get(key).cloned();
        resolve_with(&env, flags)
    }

    #[test]
    fn defaults_when_nothing_is_configured() {
        let config = resolve(no_env(), Flags::default()).unwrap();
        assert_eq!(config.endpoint_id, DEFAULT_ENDPOINT_ID);
        assert_eq!(config.listen, DEFAULT_LISTEN);
        assert_eq!(config.max_concurrent, DEFAULT_MAX_CONCURRENT);
        assert_eq!(config.heartbeat_ms, DEFAULT_HEARTBEAT_MS);
        assert_eq!(config.exec_timeout_ms, DEFAULT_EXEC_TIMEOUT_MS);
        assert!(config.listen_addr().is_ok());
    }

    #[test]
    fn file_values_apply() {
        let dir = tmp_dir();
        let path = dir.join("kiand-values.yaml");
        std::fs::write(
            &path,
            "endpoint_id: box-a\nlisten: 127.0.0.1:9911\nmax_concurrent: 3\nheartbeat_ms: 250\nexec_timeout_ms: 5000\n",
        )
        .unwrap();
        let config = resolve(
            no_env(),
            Flags {
                config: Some(path),
                ..Flags::default()
            },
        )
        .unwrap();
        assert_eq!(config.endpoint_id, "box-a");
        assert_eq!(config.listen_addr().unwrap().port(), 9911);
        assert_eq!(config.max_concurrent, 3);
        assert_eq!(config.heartbeat_ms, 250);
        assert_eq!(config.exec_timeout_ms, 5000);
    }

    #[test]
    fn unknown_file_key_refuses_to_start() {
        let err = parse_file_text(Path::new("kiand.yaml"), "bogus: 1\n").unwrap_err();
        assert!(
            err.to_string().contains("malformed config file"),
            "was: {err}"
        );
    }

    #[test]
    fn malformed_file_value_refuses_to_start() {
        let err = parse_file_text(Path::new("kiand.yaml"), "max_concurrent: many\n").unwrap_err();
        assert!(
            err.to_string().contains("malformed config file"),
            "was: {err}"
        );
    }

    #[test]
    fn env_overrides_file_and_flag_overrides_env() {
        let dir = tmp_dir();
        let path = dir.join("kiand-precedence.yaml");
        std::fs::write(&path, "endpoint_id: from-file\nlisten: 127.0.0.1:0\n").unwrap();

        let mut env = BTreeMap::new();
        env.insert(ENV_ENDPOINT_ID, "from-env".to_string());
        // File beats nothing here, env beats file:
        let config = resolve(
            env.clone(),
            Flags {
                config: Some(path.clone()),
                ..Flags::default()
            },
        )
        .unwrap();
        assert_eq!(config.endpoint_id, "from-env");

        // ... and the flag beats env.
        env.insert(ENV_ENDPOINT_ID, "from-env".to_string());
        let config = resolve(
            env,
            Flags {
                config: Some(path),
                endpoint_id: Some("from-flag".into()),
                ..Flags::default()
            },
        )
        .unwrap();
        assert_eq!(config.endpoint_id, "from-flag");
    }

    #[test]
    fn malformed_env_value_refuses_to_start() {
        let mut env = BTreeMap::new();
        env.insert(ENV_MAX_CONCURRENT, "abc".to_string());
        let err = resolve(env, Flags::default()).unwrap_err();
        assert!(
            err.to_string().contains(ENV_MAX_CONCURRENT),
            "error names the offending key: {err}"
        );
    }

    #[test]
    fn empty_env_values_are_ignored() {
        let mut env = BTreeMap::new();
        env.insert(ENV_ENDPOINT_ID, String::new());
        let config = resolve(env, Flags::default()).unwrap();
        assert_eq!(config.endpoint_id, DEFAULT_ENDPOINT_ID);
    }

    #[test]
    fn zero_max_concurrent_is_rejected() {
        let err = resolve(
            no_env(),
            Flags {
                max_concurrent: Some(0),
                ..Flags::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("max_concurrent"), "was: {err}");
    }

    #[test]
    fn unparseable_listen_is_rejected() {
        let err = resolve(
            no_env(),
            Flags {
                listen: Some("not-an-address".into()),
                ..Flags::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("listen"), "was: {err}");
    }

    #[test]
    fn explicit_missing_config_path_is_an_error() {
        let err = resolve(
            no_env(),
            Flags {
                config: Some(tmp_dir().join("definitely-absent-kiand.yaml")),
                ..Flags::default()
            },
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("cannot read config file"),
            "was: {err}"
        );
    }

    /// A writable scratch dir for file-backed tests.
    fn tmp_dir() -> std::path::PathBuf {
        let fallback = std::env::temp_dir().to_string_lossy().into_owned();
        let dir = std::path::PathBuf::from(option_env!("CARGO_TARGET_TMPDIR").unwrap_or(&fallback));
        let _ = std::fs::create_dir_all(&dir);
        dir
    }
}
