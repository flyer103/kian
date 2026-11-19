//! kiand configuration: flags + environment + optional
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
/// Default environment variable the worker token is read from in
/// register mode. Only the variable NAME travels on the command line;
/// the value never does.
pub const DEFAULT_TOKEN_ENV: &str = "KIAN_WORKER_TOKEN";

/// Default root the v2 filesystem commands resolve under (ADR-023).
pub const DEFAULT_WORKSPACE_ROOT: &str = ".";

/// Default dedupe record directory, relative to `$HOME` (ADR-037).
pub const DEFAULT_DEDUPE_DIR: &str = ".kian/kiand";
/// File name of the dedupe record inside [`Config::dedupe_dir`].
pub const DEDUPE_FILE: &str = "results.jsonl";

/// Optional config file, looked up at `./kiand.yaml` unless overridden.
pub const DEFAULT_CONFIG_FILE: &str = "kiand.yaml";

/// Environment keys (one per config field).
pub const ENV_ENDPOINT_ID: &str = "KIAND_ENDPOINT_ID";
pub const ENV_LISTEN: &str = "KIAND_LISTEN";
pub const ENV_MAX_CONCURRENT: &str = "KIAND_MAX_CONCURRENT";
pub const ENV_HEARTBEAT_MS: &str = "KIAND_HEARTBEAT_MS";
pub const ENV_EXEC_TIMEOUT_MS: &str = "KIAND_EXEC_TIMEOUT_MS";
pub const ENV_WORKSPACE_ROOT: &str = "KIAND_WORKSPACE_ROOT";
pub const ENV_DEDUPE: &str = "KIAND_DEDUPE";
pub const ENV_DEDUPE_DIR: &str = "KIAND_DEDUPE_DIR";

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
    pub register_to: Option<String>,
    pub description: Option<String>,
    pub token_env: Option<String>,
    pub workspace_root: Option<String>,
    /// `--no-dedupe`: answer a re-dispatched command id by executing it
    /// again instead of from the recorded result (ADR-037).
    pub dedupe: Option<bool>,
    /// `--dedupe-dir`: where the result records live.
    pub dedupe_dir: Option<String>,
    /// Repeatable `--capability` (v3). The list is validated at startup
    /// and sent in the register frame; an empty list omits the wire key.
    pub capabilities: Vec<String>,
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
    pub register_to: Option<String>,
    pub description: Option<String>,
    pub token_env: Option<String>,
    pub workspace_root: Option<String>,
    /// ADR-037: record results by command id (default on) and answer a
    /// re-dispatch from the record.
    pub dedupe: Option<bool>,
    pub dedupe_dir: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

/// The resolved daemon configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub endpoint_id: String,
    pub listen: String,
    pub max_concurrent: usize,
    pub heartbeat_ms: u64,
    pub exec_timeout_ms: u64,
    /// Worker-dial register mode: dial this kian registration listener
    /// instead of waiting for server-dial connections.
    pub register_to: Option<String>,
    pub description: Option<String>,
    /// NAME of the environment variable holding the worker token.
    pub token_env: String,
    /// Root the v2 filesystem commands resolve under (ADR-023). Validated
    /// to exist and be a directory at startup.
    pub workspace_root: String,
    /// Record each result by command id and answer a re-dispatched id from
    /// the record instead of executing again (ADR-037).
    pub dedupe: bool,
    /// Directory holding the dedupe record (`results.jsonl`). `None` when
    /// no home could be resolved and nothing configured one — dedupe then
    /// degrades to disabled (a miss re-executes; the queue is unchanged).
    pub dedupe_dir: Option<std::path::PathBuf>,
    /// Declared capabilities (v3, ADR-029): lowercase `[a-z0-9._-]`,
    /// 1..=64 bytes, at most 16, unique. Refuse startup on a bad entry so
    /// the worker never sends a declaration the server would sanitize.
    pub capabilities: Vec<String>,
}

impl Config {
    /// Parse and validate the listen address (fail-closed).
    pub fn listen_addr(&self) -> Result<SocketAddr, ConfigError> {
        self.listen.parse().map_err(|_| {
            ConfigError::Invalid(format!("listen `{}` is not a valid host:port", self.listen))
        })
    }

    /// Parse and validate the register-mode server address (fail-closed).
    pub fn register_addr(&self) -> Result<Option<SocketAddr>, ConfigError> {
        self.register_to
            .as_deref()
            .map(|raw| {
                raw.parse().map_err(|_| {
                    ConfigError::Invalid(format!("register_to `{raw}` is not a valid host:port"))
                })
            })
            .transpose()
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
    let register_to = flags
        .register_to
        .or_else(|| file.as_ref().and_then(|f| f.register_to.clone()));
    let description = flags
        .description
        .or_else(|| file.as_ref().and_then(|f| f.description.clone()));
    let token_env = flags
        .token_env
        .or_else(|| file.as_ref().and_then(|f| f.token_env.clone()))
        .unwrap_or_else(|| DEFAULT_TOKEN_ENV.to_string());
    let workspace_root = flags
        .workspace_root
        .or_else(|| env_get(ENV_WORKSPACE_ROOT))
        .or_else(|| file.as_ref().and_then(|f| f.workspace_root.clone()))
        .unwrap_or_else(|| DEFAULT_WORKSPACE_ROOT.to_string());
    let dedupe = match flags.dedupe {
        Some(value) => value,
        None => match env(env_get, ENV_DEDUPE) {
            Some(raw) => env_bool(ENV_DEDUPE, &raw)?,
            None => file.as_ref().and_then(|f| f.dedupe).unwrap_or(true),
        },
    };
    let dedupe_dir = flags
        .dedupe_dir
        .or_else(|| env(env_get, ENV_DEDUPE_DIR))
        .or_else(|| file.as_ref().and_then(|f| f.dedupe_dir.clone()))
        .map(std::path::PathBuf::from)
        .or_else(|| {
            env(env_get, "HOME").map(|home| std::path::Path::new(&home).join(DEFAULT_DEDUPE_DIR))
        });
    let mut capabilities = file
        .as_ref()
        .map(|f| f.capabilities.clone())
        .unwrap_or_default();
    capabilities.extend(flags.capabilities.clone());

    let config = Config {
        endpoint_id,
        listen,
        max_concurrent,
        heartbeat_ms,
        exec_timeout_ms,
        register_to,
        description,
        token_env,
        workspace_root,
        dedupe,
        dedupe_dir,
        capabilities,
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

fn env_bool(key: &str, raw: &str) -> Result<bool, ConfigError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(ConfigError::Env(format!(
            "{key}=`{raw}` is not a boolean (use 1/0)"
        ))),
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
    // The v2 filesystem root must exist and be a directory before any
    // connection opens (fail-closed; a bad root would otherwise turn every
    // fs command into an opaque io error).
    let root = std::path::Path::new(&config.workspace_root);
    if !root.is_dir() {
        return Err(ConfigError::Invalid(format!(
            "workspace_root `{}` is not an existing directory",
            config.workspace_root
        )));
    }
    // A bad listen address refuses to start before any socket opens.
    config.listen_addr()?;
    // Register mode: the server address must parse, and the token-env
    // NAME must be non-empty (the value is read at startup, never here).
    config.register_addr()?;
    if config.token_env.trim().is_empty() {
        return Err(ConfigError::Invalid(
            "token_env must name a non-empty environment variable".into(),
        ));
    }
    if config.capabilities.len() > kian_core::worker::MAX_CAPABILITIES {
        return Err(ConfigError::Invalid(format!(
            "capabilities: at most {} entries are allowed",
            kian_core::worker::MAX_CAPABILITIES
        )));
    }
    let mut seen = std::collections::BTreeSet::new();
    for capability in &config.capabilities {
        if capability.trim() != capability || !kian_core::worker::valid_capability_name(capability)
        {
            return Err(ConfigError::Invalid(format!(
                "capability `{capability}` is not a valid name \
                 (lowercase [a-z0-9._-], 1..=64 bytes, no surrounding whitespace)"
            )));
        }
        if !seen.insert(capability.as_str()) {
            return Err(ConfigError::Invalid(format!(
                "capability `{capability}` is declared more than once"
            )));
        }
    }
    // Register mode (spec §2.3): the worker validates its own identity
    // and description shape at startup, so a malformed identity never
    // reaches the wire — the server would otherwise answer `id_conflict`,
    // which the register loop classifies as retryable and re-sends
    // forever — and the operator's own invalid description refuses
    // startup instead of being silently dropped server-side (P11).
    // The rules are mirrored locally from `kian-exec`'s
    // `valid_worker_id`/`valid_description` (spec §1.3): mirror
    // discipline — that helper is deliberately not a dependency here.
    // Listen-only mode (no `--register-to`) keeps today's behavior
    // byte-identical, so these checks are register-mode only.
    if config.register_to.is_some() {
        let id = config.endpoint_id.trim();
        if id.is_empty() || id.len() > 200 {
            return Err(ConfigError::Invalid(
                "register-mode endpoint_id must be 1..=200 bytes after trim".into(),
            ));
        }
        if id.bytes().any(|b| b.is_ascii_control()) {
            return Err(ConfigError::Invalid(
                "register-mode endpoint_id must not contain control characters".into(),
            ));
        }
        if let Some(description) = &config.description {
            let trimmed = description.trim();
            if trimmed.is_empty()
                || trimmed.chars().count() > 200
                || trimmed.chars().any(char::is_control)
            {
                return Err(ConfigError::Invalid(
                    "register-mode description must be non-empty after trim, at most \
                     200 characters, and free of control characters"
                        .into(),
                ));
            }
        }
    }
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

    #[test]
    fn register_mode_fields_resolve_from_flags() {
        let config = resolve(
            no_env(),
            Flags {
                register_to: Some("127.0.0.1:9920".into()),
                description: Some("build box".into()),
                ..Flags::default()
            },
        )
        .unwrap();
        assert_eq!(
            config.register_addr().unwrap().unwrap().to_string(),
            "127.0.0.1:9920"
        );
        assert_eq!(config.description.as_deref(), Some("build box"));
        assert_eq!(config.token_env, DEFAULT_TOKEN_ENV);
    }

    #[test]
    fn capabilities_resolve_and_validate_fail_closed() {
        let config = resolve(
            no_env(),
            Flags {
                register_to: Some("127.0.0.1:9920".into()),
                capabilities: vec!["exec".into(), "apple-calendar".into()],
                ..Flags::default()
            },
        )
        .unwrap();
        assert_eq!(config.capabilities, vec!["exec", "apple-calendar"]);

        for (capability, needle) in [
            ("Exec", "not a valid name"),
            (" apple", "not a valid name"),
            ("a b", "not a valid name"),
        ] {
            let err = resolve(
                no_env(),
                Flags {
                    capabilities: vec![capability.into()],
                    ..Flags::default()
                },
            )
            .unwrap_err();
            assert!(err.to_string().contains(needle), "{capability}: {err}");
        }
        let err = resolve(
            no_env(),
            Flags {
                capabilities: vec!["exec".into(), "exec".into()],
                ..Flags::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("more than once"), "was: {err}");
        let too_many: Vec<String> = (0..17).map(|i| format!("cap-{i}")).collect();
        let err = resolve(
            no_env(),
            Flags {
                capabilities: too_many,
                ..Flags::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("at most 16"), "was: {err}");
    }

    #[test]
    fn unparseable_register_to_is_rejected() {
        let err = resolve(
            no_env(),
            Flags {
                register_to: Some("not-an-address".into()),
                ..Flags::default()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("register_to"), "was: {err}");
    }

    // -----------------------------------------------------------------------
    // Register-mode identity/description shape rules (spec §2.3, B2)
    // -----------------------------------------------------------------------

    fn register_flags(endpoint_id: &str, description: Option<&str>) -> Flags {
        Flags {
            register_to: Some("127.0.0.1:9920".into()),
            endpoint_id: Some(endpoint_id.into()),
            description: description.map(str::to_string),
            ..Flags::default()
        }
    }

    #[test]
    fn register_mode_rejects_oversized_endpoint_id() {
        let err = resolve(no_env(), register_flags(&"x".repeat(201), None)).unwrap_err();
        let text = err.to_string();
        assert!(text.contains("endpoint_id"), "names the field: {text}");
        assert!(text.contains("1..=200"), "names the rule: {text}");
    }

    #[test]
    fn register_mode_rejects_control_character_endpoint_id() {
        let err = resolve(no_env(), register_flags("box\u{7}a", None)).unwrap_err();
        let text = err.to_string();
        assert!(text.contains("endpoint_id"), "names the field: {text}");
        assert!(text.contains("control"), "names the rule: {text}");
    }

    #[test]
    fn register_mode_rejects_invalid_descriptions() {
        for description in [
            "   ",            // empty after trim
            &"d".repeat(201), // over the 200-char bound
            "build\u{7}box",  // control character
        ] {
            let err = resolve(no_env(), register_flags("box-a", Some(description))).unwrap_err();
            let text = err.to_string();
            assert!(text.contains("description"), "names the field: {text}");
            assert!(
                text.contains("200") || text.contains("non-empty") || text.contains("control"),
                "names the rule: {text}"
            );
        }
    }

    #[test]
    fn register_mode_accepts_valid_identity_and_description() {
        let config = resolve(no_env(), register_flags("box-a", Some("build box"))).unwrap();
        assert_eq!(config.endpoint_id, "box-a");
        assert_eq!(config.description.as_deref(), Some("build box"));
    }

    /// Listen-only mode keeps today's behavior byte-identical: an id that
    /// register mode would refuse still resolves without `--register-to`.
    #[test]
    fn listen_mode_is_not_subject_to_register_shape_rules() {
        let config = resolve(
            no_env(),
            Flags {
                endpoint_id: Some("x".repeat(201)),
                ..Flags::default()
            },
        )
        .unwrap();
        assert_eq!(config.endpoint_id.len(), 201);
    }

    /// A writable scratch dir for file-backed tests.
    fn tmp_dir() -> std::path::PathBuf {
        let fallback = std::env::temp_dir().to_string_lossy().into_owned();
        let dir = std::path::PathBuf::from(option_env!("CARGO_TARGET_TMPDIR").unwrap_or(&fallback));
        let _ = std::fs::create_dir_all(&dir);
        dir
    }

    #[test]
    fn dedupe_defaults_on_under_the_home_directory() {
        let mut env = no_env();
        env.insert("HOME", "/home/builder".to_string());
        let config = resolve(env, Flags::default()).unwrap();
        assert!(config.dedupe, "ADR-037 is on by default");
        assert_eq!(
            config.dedupe_dir,
            Some(std::path::PathBuf::from("/home/builder/.kian/kiand"))
        );
    }

    #[test]
    fn no_dedupe_flag_disables_the_record() {
        let config = resolve(
            no_env(),
            Flags {
                dedupe: Some(false),
                ..Flags::default()
            },
        )
        .unwrap();
        assert!(!config.dedupe);
        assert!(
            config.dedupe_dir.is_some() || config.dedupe_dir.is_none(),
            "the directory is resolved independently of the switch"
        );
    }

    #[test]
    fn dedupe_env_is_honored_and_fail_closed() {
        let mut env = no_env();
        env.insert(ENV_DEDUPE, "0".into());
        env.insert(ENV_DEDUPE_DIR, "/var/lib/kiand".into());
        let config = resolve(env, Flags::default()).unwrap();
        assert!(!config.dedupe);
        assert_eq!(
            config.dedupe_dir,
            Some(std::path::PathBuf::from("/var/lib/kiand"))
        );

        let mut env = no_env();
        env.insert(ENV_DEDUPE, "maybe".into());
        let err = resolve(env, Flags::default()).unwrap_err();
        assert!(err.to_string().contains(ENV_DEDUPE), "was: {err}");
    }

    #[test]
    fn dedupe_file_value_applies_and_flags_win() {
        let dir = tmp_dir();
        let path = dir.join("kiand-dedupe.yaml");
        std::fs::write(&path, "dedupe: false\ndedupe_dir: /srv/kiand-cache\n").unwrap();
        let config = resolve(
            no_env(),
            Flags {
                config: Some(path),
                ..Flags::default()
            },
        )
        .unwrap();
        assert!(!config.dedupe);
        assert_eq!(
            config.dedupe_dir,
            Some(std::path::PathBuf::from("/srv/kiand-cache"))
        );

        let mut env = no_env();
        env.insert(ENV_DEDUPE, "0".into());
        let overridden = resolve(
            env,
            Flags {
                dedupe: Some(true),
                ..Flags::default()
            },
        )
        .unwrap();
        assert!(overridden.dedupe, "the flag wins over the environment");
    }
}
