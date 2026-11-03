//! kian-77: exec secret hygiene — redaction of exec output and the child
//! environment policy (C65 live finding G4, severity HIGH).
//!
//! The C65 live run proved two facts about the exec surface:
//!
//! 1. **Environment passthrough**: a worker that runs `env`/`printenv`
//!    through the exec tool writes every server secret verbatim into
//!    the durable event stream (`tool/result`, and every later
//!    `step/request` transcript envelope that replays it).
//! 2. **No output scrubbing**: nothing between the child's pipes and the
//!    event store inspects env-shaped or token-shaped output.
//!
//! This module holds the two shared countermeasures, used by every
//! spawn site (kian-loop's `LocalExecBackend`) and result choke point
//! (kian-exec's `RemoteExecBackend::complete`):
//!
//! - [`SecretRedactor`] — redacts (a) the exact values of configured
//!   secret environment variables, (b) well-known token shapes
//!   (`ghp_…`, `sk-…`, AWS key ids, …), and (c) `NAME=value` lines whose
//!   NAME is secret-shaped (`*_TOKEN`, `*_API_KEY`, `*SECRET*`, …), so
//!   an `env` dump persists no secret it did not also configure.
//! - [`ExecEnvPolicy`] — allowlist mode: spawned children inherit
//!   **only** the configured variables plus `DRY_RUN` (the example's
//!   dry-run contract rides the environment); secrets never reach the
//!   child in the first place. `Inherit` is today's behavior and stays
//!   the config default (kian-77 compatibility clause).
//!
//! Redaction is belt-and-suspenders by design: the allowlist bounds what
//! the child can *see*; the redactor bounds what the audit stream *keeps*
//! when the allowlist is off or a secret rides in by another name.

use std::collections::{BTreeMap, BTreeSet};

/// The env var the example's dry-run contract rides on; always forwarded
/// to allowlisted children when set in the parent (kian-77).
pub const DRY_RUN_ENV: &str = "DRY_RUN";

/// Placeholder substituted for a redacted secret value.
pub const REDACTED: &str = "[REDACTED]";

// ---------------------------------------------------------------------------
// SecretRedactor
// ---------------------------------------------------------------------------

/// Redacts secrets from exec stdout/stderr before the result is persisted
/// or emitted as a `tool/result`.
///
/// Three passes, in order:
///
/// 1. **Known values** — the exact current value of every configured
///    secret env var (`[REDACTED:<NAME>]`). This is the precise fix for
///    the G4 leak: the leaked bytes are exactly these values.
/// 2. **Token patterns** — well-known token shapes (`ghp_`, `github_pat_`,
///    `sk-`, `xox?-`, AWS `AKIA…`) with conservative minimum lengths, so
///    unknown secrets of a known shape still never persist.
/// 3. **Env-dump lines** — `NAME=value` lines whose NAME contains a
///    secret-shaped segment (`TOKEN`, `SECRET`, `PASSWORD`, `PASSWD`,
///    `CREDENTIAL`, or a trailing `KEY` segment), redacting the value.
///
/// Constructed from env var *names* (values are read from the process
/// environment at build time — assembly reads config, never the reverse).
#[derive(Debug, Clone, Default)]
pub struct SecretRedactor {
    /// `(name, value)` pairs, sorted by descending value length so longer
    /// values are replaced before shorter ones can match inside them.
    known: Vec<(String, String)>,
    /// Pattern-class scanning on (`true` by default; `false` = known
    /// values only — tests and explicit config opt-out).
    patterns: bool,
}

impl SecretRedactor {
    /// A redactor with no known values and pattern scanning **on** — the
    /// safe default for paths with no configured secret names.
    #[must_use]
    pub fn patterns_only() -> Self {
        Self {
            known: Vec::new(),
            patterns: true,
        }
    }

    /// Builds a redactor over the given environment variable names,
    /// reading each value from the process environment at call time.
    /// Unset, empty, or very short (<8 chars) values are skipped — a
    /// value that short is not a secret and blanket-replacing it would
    /// shred legitimate output.
    #[must_use]
    pub fn from_env_names(names: &[String]) -> Self {
        let mut known: Vec<(String, String)> = names
            .iter()
            .filter_map(|name| {
                let value = std::env::var(name).ok()?;
                if value.len() >= 8 {
                    Some((name.clone(), value))
                } else {
                    None
                }
            })
            .collect();
        known.sort_by_key(|(_, v)| std::cmp::Reverse(v.len()));
        Self {
            known,
            patterns: true,
        }
    }

    /// Adds the names of a second redactor (assembly merges configured
    /// names from several config sections).
    #[must_use]
    pub fn with_env_names(mut self, names: &[String]) -> Self {
        let extra = Self::from_env_names(names);
        self.known.extend(extra.known);
        self.known.sort_by_key(|(_, v)| std::cmp::Reverse(v.len()));
        self
    }

    /// Disables pattern and env-dump scanning (known values still
    /// redact). Config opt-out for byte-exact output.
    #[must_use]
    pub fn without_patterns(mut self) -> Self {
        self.patterns = false;
        self
    }

    /// True when the redactor can never change anything (fast path).
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.known.is_empty() && !self.patterns
    }

    /// Returns `text` with every secret replaced by its placeholder.
    #[must_use]
    pub fn redact(&self, text: &str) -> String {
        if self.is_noop() {
            return text.to_string();
        }
        let mut out = text.to_string();
        // Pass 1: exact configured values, anywhere in the text —
        // longest values first, so a value that is a prefix of another
        // cannot shred the longer replacement (the invariant is
        // enforced here, not only at construction).
        let mut ordered: Vec<&(String, String)> = self.known.iter().collect();
        ordered.sort_by_key(|(_, v)| std::cmp::Reverse(v.len()));
        for (name, value) in ordered {
            if out.contains(value.as_str()) {
                out = out.replace(value.as_str(), &format!("[REDACTED:{name}]"));
            }
        }
        if self.patterns {
            // Pass 2: token shapes.
            out = redact_token_patterns(&out);
            // Pass 3: secret-shaped env-dump lines.
            out = redact_env_lines(&out);
        }
        out
    }
}

/// Token prefix classes and their minimum tail length: (prefix, min-tail).
/// Conservative minimums keep ordinary words out of the blast radius.
const TOKEN_PREFIXES: &[(&str, usize)] = &[
    ("github_pat_", 20),
    ("ghp_", 20),
    ("gho_", 20),
    ("ghu_", 20),
    ("ghs_", 20),
    ("ghr_", 20),
    ("sk-", 20),
    ("xoxb-", 12),
    ("xoxa-", 12),
    ("xoxp-", 12),
    ("xoxr-", 12),
    ("xoxs-", 12),
];

/// True when `c` may appear in a token tail.
fn is_token_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

/// Redacts well-known token shapes (`ghp_…`, `sk-…`, …) → `[REDACTED:token]`.
fn redact_token_patterns(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        let rest: String = chars[i..].iter().collect();
        if let Some((prefix, min_tail)) = TOKEN_PREFIXES
            .iter()
            .find(|(prefix, _)| rest.starts_with(prefix))
        {
            let tail_len = chars[i + prefix.len()..]
                .iter()
                .take_while(|c| is_token_char(**c))
                .count();
            if tail_len >= *min_tail {
                out.push_str("[REDACTED:token]");
                i += prefix.len() + tail_len;
                continue;
            }
        }
        // AWS access key id: AKIA + exactly 16 upper alnum.
        if rest.starts_with("AKIA") {
            let tail: String = chars[i + 4..]
                .iter()
                .take(16)
                .take_while(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
                .collect();
            if tail.len() == 16 {
                out.push_str("[REDACTED:token]");
                i += 4 + 16;
                continue;
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

/// The name segments that mark an environment variable as secret-shaped.
const SECRET_SEGMENTS: &[&str] = &["TOKEN", "SECRET", "PASSWORD", "PASSWD", "CREDENTIAL"];

/// True when an env NAME (uppercase, `_`-separated) is secret-shaped:
/// it contains a secret segment, or ends with a `KEY` segment
/// (`API_KEY`, `DEEPSEEK_API_KEY`) — a *whole* segment, so `KEYBOARD`
/// and `MONKEY_POOL` stay untouched.
fn is_secret_shaped_name(name: &str) -> bool {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
    {
        return false;
    }
    let segments: Vec<&str> = name.split('_').collect();
    segments.iter().any(|s| SECRET_SEGMENTS.contains(s))
        || segments.last() == Some(&"KEY")
        || segments.contains(&"APIKEY")
}

/// Redacts the value half of secret-shaped `NAME=value` lines →
/// `NAME=[REDACTED]`. Catches environment dumps of secrets the deployer
/// never listed (defense in depth behind the known-value pass).
fn redact_env_lines(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut first = true;
    for line in text.split('\n') {
        if !first {
            out.push('\n');
        }
        first = false;
        if let Some((name, value)) = line.split_once('=')
            && !value.is_empty()
            // A pass-1 marker (`[REDACTED:<NAME>]`) is itself the value
            // of a secret-shaped name — redacting it again would erase
            // which variable was scrubbed. Leave marked lines alone.
            && !value.starts_with("[REDACTED")
            && is_secret_shaped_name(name)
        {
            out.push_str(name);
            out.push('=');
            out.push_str(REDACTED);
        } else {
            out.push_str(line);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// ExecEnvPolicy
// ---------------------------------------------------------------------------

/// What environment spawned exec children see (kian-77 fix ①).
///
/// - `Inherit` — today's behavior: the child sees the full server
///   environment (the config default; compatibility clause).
/// - `Allowlist` — the child sees **only** the listed variables plus
///   `DRY_RUN` when the parent has it set. Secrets configured elsewhere
///   in `kian.yaml` are never listed, so they never reach the child.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum ExecEnvPolicy {
    #[default]
    Inherit,
    Allowlist {
        /// Variable names the child may see (names, never values — the
        /// values are read from the parent environment at spawn time).
        allow: BTreeSet<String>,
    },
}

impl ExecEnvPolicy {
    /// Allowlist policy over the given variable names.
    #[must_use]
    pub fn allowlist(names: &[String]) -> Self {
        Self::Allowlist {
            allow: names.iter().cloned().collect(),
        }
    }

    /// Resolves the child environment against an explicit parent
    /// iterator. `None` means inherit (the caller leaves the spawn
    /// command's environment untouched); `Some(map)` is the exact,
    /// closed set of variables the child may see.
    #[must_use]
    pub fn resolve_from<I>(&self, parent: I) -> Option<BTreeMap<String, String>>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        match self {
            Self::Inherit => None,
            Self::Allowlist { allow } => {
                let env: BTreeMap<String, String> = parent
                    .into_iter()
                    // The dry-run contract rides the environment — a
                    // worker must still know it is in dry-run (the
                    // example's "never post unless DRY_RUN=0" mandate).
                    // Forwarded only when the parent carries it; nothing
                    // not listed (and no secret) is ever inherited.
                    .filter(|(name, _)| allow.contains(name) || name == DRY_RUN_ENV)
                    .collect();
                Some(env)
            }
        }
    }

    /// [`Self::resolve_from`] over the live process environment.
    #[must_use]
    pub fn resolve(&self) -> Option<BTreeMap<String, String>> {
        self.resolve_from(std::env::vars())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn redactor_with(name: &str, value: &str) -> SecretRedactor {
        SecretRedactor {
            known: vec![(name.to_string(), value.to_string())],
            patterns: false,
        }
    }

    // -- Pass 1: known values ----------------------------------------------

    #[test]
    fn known_value_is_redacted_anywhere_in_the_text() {
        let r = redactor_with("KIAN_TEST_SECRET", "gh-value-aaaaaaaaaaaaaaaaaaaa");
        let env_dump =
            "PATH=/usr/bin\nKIAN_TEST_SECRET=gh-value-aaaaaaaaaaaaaaaaaaaa\nHOME=/root\n";
        let out = r.redact(env_dump);
        assert!(!out.contains("gh-value-aaaaaaaaaaaaaaaaaaaa"));
        assert!(out.contains("KIAN_TEST_SECRET=[REDACTED:KIAN_TEST_SECRET]"));
        assert!(out.contains("PATH=/usr/bin"));
        assert!(out.contains("HOME=/root"));
    }

    #[test]
    fn known_value_is_redacted_in_loose_echo_output_not_just_env_lines() {
        let r = redactor_with("DEEPSEEK_API_KEY", "sk-loose-value-1234567890abcd");
        let out = r.redact("the key is sk-loose-value-1234567890abcd ok?");
        assert_eq!(out, "the key is [REDACTED:DEEPSEEK_API_KEY] ok?");
    }

    #[test]
    fn longer_known_values_win_over_shorter_ones_sharing_a_prefix() {
        let mut r = redactor_with("SHORTY", "abcdefgh");
        r.known.push(("LONGONE".into(), "abcdefghij1234".into()));
        let out = r.redact("abcdefghij1234 and abcdefgh");
        assert!(out.contains("[REDACTED:LONGONE]"));
        assert!(out.contains("[REDACTED:SHORTY]"));
        assert!(!out.contains("abcdefgh"));
    }

    // -- Pass 2: token patterns --------------------------------------------

    #[test]
    fn github_classic_token_shape_is_redacted_unconfigured() {
        let r = SecretRedactor::patterns_only();
        let out = r.redact("token=ghp_abcdefghijklmnopqrstuvwx");
        assert_eq!(out, "token=[REDACTED:token]");
    }

    #[test]
    fn openai_style_sk_token_is_redacted() {
        let r = SecretRedactor::patterns_only();
        let out = r.redact("key sk-abcdefghij1234567890XYZ over");
        assert_eq!(out, "key [REDACTED:token] over");
    }

    #[test]
    fn short_or_malformed_token_lookalikes_are_left_alone() {
        let r = SecretRedactor::patterns_only();
        let out = r.redact("sk-short and ghp_tiny stay");
        assert_eq!(out, "sk-short and ghp_tiny stay");
    }

    #[test]
    fn aws_access_key_id_shape_is_redacted() {
        let r = SecretRedactor::patterns_only();
        let out = r.redact("aws AKIAIOSFODNN7EXAMPLE here");
        assert_eq!(out, "aws [REDACTED:token] here");
    }

    // -- Pass 3: env-dump lines ---------------------------------------------

    #[test]
    fn secret_shaped_env_lines_are_redacted_unconfigured() {
        let r = SecretRedactor::patterns_only();
        let dump = "PATH=/bin\nMY_SERVICE_TOKEN=zzz-not-a-known-pattern\nDEEPSEEK_API_KEY=whatever123\nKEYBOARD=logitech\nMONKEY_POOL=yes\nHOME=/root\n";
        let out = r.redact(dump);
        assert!(out.contains("MY_SERVICE_TOKEN=[REDACTED]"));
        assert!(out.contains("DEEPSEEK_API_KEY=[REDACTED]"));
        // Whole-segment matching: KEYBOARD and MONKEY_POOL are not keys.
        assert!(out.contains("KEYBOARD=logitech"));
        assert!(out.contains("MONKEY_POOL=yes"));
        // Ordinary variables untouched.
        assert!(out.contains("PATH=/bin"));
        assert!(out.contains("HOME=/root"));
    }

    #[test]
    fn lowercase_non_env_names_are_not_touched() {
        let r = SecretRedactor::patterns_only();
        let out = r.redact("some password=lame in prose");
        assert_eq!(out, "some password=lame in prose");
    }

    // -- Opt-out / noop -----------------------------------------------------

    #[test]
    fn noop_redactor_returns_text_unchanged() {
        let r = SecretRedactor::default();
        assert!(r.is_noop());
        let text = "KIAN_TEST_SECRET=supersecretvalue123";
        assert_eq!(r.redact(text), text);
    }

    #[test]
    fn without_patterns_still_redacts_known_values() {
        let mut r = redactor_with("T", "knownvalue123456");
        r.patterns = false;
        let out = r.redact("x=knownvalue123456 ghp_abcdefghijklmnopqrstuvwx");
        assert!(out.contains("x=[REDACTED:T]"));
        assert!(out.contains("ghp_abcdefghijklmnopqrstuvwx"));
    }

    // -- ExecEnvPolicy -------------------------------------------------------

    #[test]
    fn inherit_policy_resolves_to_none() {
        assert_eq!(ExecEnvPolicy::Inherit.resolve_from(Vec::new()), None);
    }

    #[test]
    fn allowlist_policy_keeps_only_allowed_vars_and_dry_run() {
        let policy = ExecEnvPolicy::allowlist(&[
            "KIAN_ENV_TEST_PATH".to_string(),
            "KIAN_ENV_TEST_HOME".to_string(),
        ]);
        let parent = vec![
            ("KIAN_ENV_TEST_PATH".to_string(), "/bin".to_string()),
            ("KIAN_ENV_TEST_HOME".to_string(), "/root".to_string()),
            ("KIAN_GITHUB_TOKEN".to_string(), "gh-secret".to_string()),
            ("DEEPSEEK_API_KEY".to_string(), "sk-secret".to_string()),
            ("DRY_RUN".to_string(), "1".to_string()),
        ];
        let resolved = policy.resolve_from(parent).expect("allowlist resolves");
        assert_eq!(
            resolved,
            BTreeMap::from([
                ("KIAN_ENV_TEST_PATH".to_string(), "/bin".to_string()),
                ("KIAN_ENV_TEST_HOME".to_string(), "/root".to_string()),
                ("DRY_RUN".to_string(), "1".to_string()),
            ])
        );
    }

    #[test]
    fn allowlist_policy_without_dry_run_set_leaves_it_out() {
        let policy = ExecEnvPolicy::allowlist(&["KIAN_ENV_TEST_PATH".to_string()]);
        let parent = vec![("KIAN_ENV_TEST_PATH".to_string(), "/bin".to_string())];
        let resolved = policy.resolve_from(parent).expect("allowlist resolves");
        assert_eq!(
            resolved,
            BTreeMap::from([("KIAN_ENV_TEST_PATH".to_string(), "/bin".to_string())])
        );
    }

    #[test]
    fn empty_allowlist_is_a_closed_minimal_environment() {
        let policy = ExecEnvPolicy::allowlist(&[]);
        let parent = vec![("ANYTHING".to_string(), "else".to_string())];
        // DRY_RUN is only forwarded when the parent (process) has it;
        // this test does not rely on the live process env.
        let resolved = policy.resolve_from(parent).expect("allowlist resolves");
        assert!(!resolved.contains_key("ANYTHING"));
    }
}
