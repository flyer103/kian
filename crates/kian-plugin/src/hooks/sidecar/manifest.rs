//! The sidecar manifest (PLUGIN-SPEC §6.1, closed TOML) and §6.2's
//! fourteen per-instance validation rules.
//!
//! The manifest grammar is a **closed subset** of TOML: the four sections
//! `[plugin]`, `[process]`, `[grants]`, `[limits]`, their fixed keys, and
//! value forms basic-string / integer / boolean / array-of-basic-strings
//! (plus comments). Anything else is `malformed_manifest` (rule 1,
//! deny-unknown-fields). The parser is hand-rolled because the round's
//! one allowed new dependency is `sha2` (K10) — no `toml` edge exists and
//! none may be added; a closed grammar needs none of a general parser's
//! power, and refusing the general grammar is the point of rule 1.
//!
//! Validation is **per instance, fail-closed, no repair** (§6.2): every
//! refusal names the offender and the `plugin/refuse` code, and the
//! `required` flag a parsed manifest did carry (or `unknown` when the
//! file could not be parsed far enough to read one). One broken instance
//! never unmounts the others; which instances exist at all is the
//! config surface's question (P3b), not this module's.

use std::path::{Path, PathBuf};

use super::super::grants::{Grant, GrantSet};
use super::super::ids::HookId;

/// Context the caller (boot assembly, P3b) supplies for the rules this
/// crate cannot decide alone: which names are already taken (rule 3 —
/// `KNOWN_PLUGINS`, tier-0 plugin names, sibling instance keys) and
/// which environment names are kian secrets (rule 7).
#[derive(Debug, Clone, Default)]
pub struct ValidationCtx<'a> {
    /// The `sidecars.plugins` instance key this manifest is mounted as
    /// (rule 2: `[plugin].name` must equal it).
    pub instance_key: &'a str,
    /// Names a new instance may not take (rule 3).
    pub reserved_names: &'a [String],
    /// Kian secret environment variable names `env_allow` may not carry
    /// (rule 7, §10.4).
    pub secret_env: &'a [String],
}

/// The `plugin/refuse` code set (§6.2's table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefusalCode {
    MalformedManifest,
    NameInvalid,
    NameCollision,
    MissingBinary,
    DigestMismatch,
    UnknownHook,
    UnknownCapability,
    GrantMismatch,
    AbiMismatch,
    ReadyMismatch,
    SpawnFailed,
}

impl RefusalCode {
    pub fn as_str(self) -> &'static str {
        match self {
            RefusalCode::MalformedManifest => "malformed_manifest",
            RefusalCode::NameInvalid => "name_invalid",
            RefusalCode::NameCollision => "name_collision",
            RefusalCode::MissingBinary => "missing_binary",
            RefusalCode::DigestMismatch => "digest_mismatch",
            RefusalCode::UnknownHook => "unknown_hook",
            RefusalCode::UnknownCapability => "unknown_capability",
            RefusalCode::GrantMismatch => "grant_mismatch",
            RefusalCode::AbiMismatch => "abi_mismatch",
            RefusalCode::ReadyMismatch => "ready_mismatch",
            RefusalCode::SpawnFailed => "spawn_failed",
        }
    }
}

/// Whether the refused instance declared itself `required` (§6.2): a
/// manifest that cannot be parsed cannot declare anything, so the audit
/// line says `required=unknown` rather than guessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Required {
    True,
    False,
    Unknown,
}

/// One per-instance refusal: the code, a message naming the offender,
/// and the `required` value the instance did (or could not) declare.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub code: RefusalCode,
    pub message: String,
    pub required: Required,
}

impl Refusal {
    fn new(code: RefusalCode, message: String, required: Required) -> Self {
        Self {
            code,
            message,
            required,
        }
    }
}

/// The accepted `abi` set (§7.3: grows additively only; v1 = `{1}`).
pub const ACCEPTED_ABI: &[u32] = &[1];

// ---------------------------------------------------------------------
// §6.1 sections
// ---------------------------------------------------------------------

/// `[plugin]` (§6.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginSection {
    pub name: String,
    pub version: String,
    pub abi: u32,
    pub order: i32,
    pub required: bool,
}

/// `[process]` (§6.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSection {
    pub argv: Vec<String>,
    pub sha256: String,
    pub cwd: Option<PathBuf>,
    pub env_allow: Vec<String>,
    pub startup_timeout_ms: u64,
}

/// `[grants]` (§6.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantsSection {
    pub hooks: Vec<HookId>,
    pub grants: GrantSet,
}

/// `[limits]` (§6.1) — every value already inside its clamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitsSection {
    pub deadline_ms: u64,
    pub max_payload_kib: u64,
    pub restarts_per_window: u64,
    pub restart_window_ms: u64,
    pub max_settle_vetoes: u64,
    pub max_stderr_line_kib: u64,
    pub stderr_rate_kib_per_min: u64,
}

impl Default for LimitsSection {
    fn default() -> Self {
        Self {
            deadline_ms: 500,
            max_payload_kib: 256,
            restarts_per_window: 3,
            restart_window_ms: 60_000,
            // §6.1: the settle budget's default is 3 (n2) — an
            // explicit `0` in a manifest disables the gate, but the
            // struct default must never carry 0 or any future
            // `..LimitsSection::default()` silently re-creates the M1
            // bug (a disabled gate where §6.1 mandates 3).
            max_settle_vetoes: super::super::bus::DEFAULT_MAX_SETTLE_VETOS,
            max_stderr_line_kib: 8,
            stderr_rate_kib_per_min: 1024,
        }
    }
}

impl LimitsSection {
    /// §7.6's wire bound in bytes (`max_payload_kib`).
    pub fn max_frame_bytes(&self) -> usize {
        (self.max_payload_kib as usize) * 1024
    }
}

/// A parsed, validated manifest (§6.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub plugin: PluginSection,
    pub process: ProcessSection,
    pub grants: GrantsSection,
    pub limits: LimitsSection,
}

impl Manifest {
    /// The manifest's own declared `required` (§6.2).
    pub fn required(&self) -> Required {
        if self.plugin.required {
            Required::True
        } else {
            Required::False
        }
    }
}

// ---------------------------------------------------------------------
// Closed-TOML subset parser (rule 1's deny-unknown-fields substrate)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum RawValue {
    Str(String),
    Int(i64),
    Bool(bool),
    Arr(Vec<String>),
}

impl RawValue {
    fn type_name(&self) -> &'static str {
        match self {
            RawValue::Str(_) => "string",
            RawValue::Int(_) => "integer",
            RawValue::Bool(_) => "boolean",
            RawValue::Arr(_) => "array",
        }
    }
}

/// One raw `section.key = value` triple in file order.
struct RawEntry {
    section: String,
    key: String,
    value: RawValue,
    line: usize,
}

fn parse_raw(text: &str) -> Result<Vec<RawEntry>, String> {
    let mut entries = Vec::new();
    let mut section = String::new();
    for (index, raw_line) in text.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(header) = line.strip_prefix('[') {
            let name = header
                .strip_suffix(']')
                .ok_or_else(|| format!("line {}: unterminated section header", index + 1))?;
            let name = name.trim();
            if name.is_empty() || name.contains('[') || name.contains(']') {
                return Err(format!("line {}: malformed section header", index + 1));
            }
            section = name.to_string();
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("line {}: expected `key = value` or `[section]`", index + 1))?;
        let key = key.trim().to_string();
        if key.is_empty() {
            return Err(format!("line {}: empty key", index + 1));
        }
        let value = parse_value(value.trim()).map_err(|e| format!("line {}: {e}", index + 1))?;
        if entries
            .iter()
            .any(|e: &RawEntry| e.section == section && e.key == key)
        {
            return Err(format!("line {}: duplicate key `{key}`", index + 1));
        }
        entries.push(RawEntry {
            section: section.clone(),
            key,
            value,
            line: index + 1,
        });
    }
    Ok(entries)
}

/// Parses one closed-subset value: basic string, integer, boolean, or an
/// array of basic strings. A trailing comment is allowed outside
/// strings. Everything else — multiline strings, literal strings,
/// dotted keys, floats, heterogeneous arrays — is out of the closed
/// grammar and therefore `malformed_manifest`.
fn parse_value(text: &str) -> Result<RawValue, String> {
    let text = strip_trailing_comment(text)?;
    match text.as_bytes().first() {
        Some(b'"') => parse_basic_string(&text).map(RawValue::Str),
        Some(b'[') => parse_string_array(&text).map(RawValue::Arr),
        Some(b't' | b'f') if text == "true" || text == "false" => {
            Ok(RawValue::Bool(text == "true"))
        }
        Some(c) if c.is_ascii_digit() || *c == b'-' => text
            .parse::<i64>()
            .map(RawValue::Int)
            .map_err(|_| format!("`{text}` is not an integer")),
        _ => Err(format!(
            "`{text}` is not one of string / integer / boolean / array"
        )),
    }
}

fn strip_trailing_comment(text: &str) -> Result<String, String> {
    let mut out = String::new();
    let mut in_string = false;
    let mut escaped = false;
    for ch in text.chars() {
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => {
                in_string = true;
                out.push(ch);
            }
            '#' => return Ok(out.trim_end().to_string()),
            _ => out.push(ch),
        }
    }
    if in_string {
        return Err("unterminated string".to_string());
    }
    Ok(out.trim_end().to_string())
}

fn parse_basic_string(text: &str) -> Result<String, String> {
    let inner = text
        .strip_prefix('"')
        .and_then(|t| t.strip_suffix('"'))
        .ok_or_else(|| format!("`{text}` is not a basic string"))?;
    // Unescape exactly \" and \\ — the closed grammar's two escapes.
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => match chars.next() {
                Some('"') => out.push('"'),
                Some('\\') => out.push('\\'),
                other => {
                    return Err(format!(
                        "unsupported escape `\\{}` (only `\\\"` and `\\\\` exist)",
                        other.unwrap_or(' ')
                    ));
                }
            },
            '"' => return Err("unescaped quote inside string".to_string()),
            c if c.is_control() => return Err("control character in string".to_string()),
            c => out.push(c),
        }
    }
    Ok(out)
}

fn parse_string_array(text: &str) -> Result<Vec<String>, String> {
    let inner = text
        .strip_prefix('[')
        .and_then(|t| t.strip_suffix(']'))
        .ok_or_else(|| format!("`{text}` is not an array"))?;
    let inner = inner.trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    let mut items = Vec::new();
    for part in split_top_level(inner)? {
        let part = part.trim();
        if part.is_empty() {
            return Err("empty array element".to_string());
        }
        items.push(parse_basic_string(part)?);
    }
    Ok(items)
}

fn split_top_level(text: &str) -> Result<Vec<String>, String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut escaped = false;
    for ch in text.chars() {
        if in_string {
            current.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => {
                in_string = true;
                current.push(ch);
            }
            ',' => {
                parts.push(current.clone());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if in_string {
        return Err("unterminated string in array".to_string());
    }
    parts.push(current);
    Ok(parts)
}

// ---------------------------------------------------------------------
// §6.2 validation — the fourteen rules, in table order
// ---------------------------------------------------------------------

/// Parses and validates one instance manifest (§6.2). Every failure is
/// a typed [`Refusal`] naming the offender; there is no repair and no
/// default that invents a missing field's intent (optional fields carry
/// documented defaults; required fields do not).
pub fn parse_manifest(text: &str, ctx: &ValidationCtx<'_>) -> Result<Manifest, Refusal> {
    let malformed =
        |message: String| Refusal::new(RefusalCode::MalformedManifest, message, Required::Unknown);
    let entries = parse_raw(text).map_err(|e| malformed(format!("parse error: {e}")))?;

    // ---- rule 1: deny-unknown-fields (unknown section or key) ----
    let known = |section: &str, key: &str| -> bool {
        match section {
            "plugin" => matches!(key, "name" | "version" | "abi" | "order" | "required"),
            "process" => matches!(
                key,
                "argv" | "sha256" | "cwd" | "env_allow" | "startup_timeout_ms"
            ),
            "grants" => matches!(key, "hooks" | "capabilities"),
            "limits" => matches!(
                key,
                "deadline_ms"
                    | "max_payload_kib"
                    | "restarts_per_window"
                    | "restart_window_ms"
                    | "max_settle_vetoes"
                    | "max_stderr_line_kib"
                    | "stderr_rate_kib_per_min"
            ),
            _ => false,
        }
    };
    for entry in &entries {
        if !known(&entry.section, &entry.key) {
            let where_ = if entry.section.is_empty() {
                String::new()
            } else {
                format!("[{}].", entry.section)
            };
            return Err(malformed(format!(
                "unknown key `{where_}{}` (line {}): the manifest grammar is closed",
                entry.key, entry.line
            )));
        }
    }
    // Entries before any section header are not part of the grammar.
    if entries.iter().any(|e| e.section.is_empty()) {
        return Err(malformed(
            "keys before the first `[section]` header".to_string(),
        ));
    }
    for section in ["plugin", "process", "grants", "limits"] {
        if !entries.iter().any(|e| e.section == section) {
            return Err(malformed(format!("missing section `[{section}]`")));
        }
    }

    // Typed extraction. Wrong types are malformed (rule 4 for
    // `order`/`required`; rule 1's spirit for the rest — a closed
    // schema has no coercion).
    let find = |section: &str, key: &str| -> Option<&RawValue> {
        entries
            .iter()
            .find(|e| e.section == section && e.key == key)
            .map(|e| &e.value)
    };
    let typed_str = |section: &str, key: &str| -> Result<String, Refusal> {
        match find(section, key) {
            Some(RawValue::Str(s)) => Ok(s.clone()),
            Some(other) => Err(malformed(format!(
                "[{section}].{key} must be a string, got {}",
                other.type_name()
            ))),
            None => Err(malformed(format!(
                "missing required key `[{section}].{key}`"
            ))),
        }
    };
    let typed_int = |section: &str, key: &str| -> Result<Option<i64>, Refusal> {
        match find(section, key) {
            Some(RawValue::Int(v)) => Ok(Some(*v)),
            Some(other) => Err(malformed(format!(
                "[{section}].{key} must be an integer, got {}",
                other.type_name()
            ))),
            None => Ok(None),
        }
    };
    let typed_bool = |section: &str, key: &str| -> Result<Option<bool>, Refusal> {
        match find(section, key) {
            Some(RawValue::Bool(v)) => Ok(Some(*v)),
            Some(other) => Err(malformed(format!(
                "[{section}].{key} must be a boolean, got {}",
                other.type_name()
            ))),
            None => Ok(None),
        }
    };
    let typed_arr = |section: &str, key: &str| -> Result<Option<Vec<String>>, Refusal> {
        match find(section, key) {
            Some(RawValue::Arr(v)) => Ok(Some(v.clone())),
            Some(other) => Err(malformed(format!(
                "[{section}].{key} must be an array of strings, got {}",
                other.type_name()
            ))),
            None => Ok(None),
        }
    };

    let name = typed_str("plugin", "name")?;
    let version = typed_str("plugin", "version")?;
    let abi = typed_int("plugin", "abi")?
        .ok_or_else(|| malformed("missing required key `[plugin].abi`".to_string()))?;
    let order_int = typed_int("plugin", "order")?;
    let required = typed_bool("plugin", "required")?;
    // Fail-closed default: an instance that does not say is one whose
    // refusal must be fatal (§6.2's posture).
    let required = required.unwrap_or(true);
    // Rules past 2 get the parsed `required` value; a manifest that
    // could not be parsed far enough to read one reports `unknown`
    // (§6.2's boundary case).
    let refuse = |code: RefusalCode, message: String| {
        let required = if matches!(
            code,
            RefusalCode::MalformedManifest | RefusalCode::NameInvalid
        ) {
            Required::Unknown
        } else if required {
            Required::True
        } else {
            Required::False
        };
        Refusal::new(code, message, required)
    };

    // ---- rule 2: name grammar + instance-key equality ----
    let valid_name = {
        let mut chars = name.chars();
        match chars.next() {
            Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit() => {
                chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
            }
            _ => false,
        }
    };
    if !valid_name || name.len() > 64 {
        return Err(refuse(
            RefusalCode::NameInvalid,
            format!("`[plugin].name = \"{name}\" is not ^[a-z0-9][a-z0-9-]{{0,63}}$"),
        ));
    }
    if name != ctx.instance_key {
        return Err(refuse(
            RefusalCode::NameInvalid,
            format!(
                "`[plugin].name = \"{name}\" does not equal the instance key `{}`",
                ctx.instance_key
            ),
        ));
    }

    // ---- rule 3: name collisions ----
    if ctx
        .reserved_names
        .iter()
        .any(|reserved| reserved.as_str() == name)
    {
        return Err(refuse(
            RefusalCode::NameCollision,
            format!("`[plugin].name = \"{name}\" collides with a reserved name"),
        ));
    }

    // ---- rule 4: semver, order/required types, accepted abi set ----
    if !is_semver(&version) {
        return Err(refuse(
            RefusalCode::MalformedManifest,
            format!("`[plugin].version = \"{version}\" is not semver"),
        ));
    }
    let abi = u32::try_from(abi).map_err(|_| {
        refuse(
            RefusalCode::MalformedManifest,
            format!("`[plugin].abi = {abi}` is out of range"),
        )
    })?;
    if !ACCEPTED_ABI.contains(&abi) {
        return Err(refuse(
            RefusalCode::AbiMismatch,
            format!("`[plugin].abi = {abi}` is outside the accepted set {{1}} (§7.3)"),
        ));
    }
    let order = i32::try_from(order_int.unwrap_or(0)).map_err(|_| {
        refuse(
            RefusalCode::MalformedManifest,
            format!(
                "`[plugin].order = {}` is out of range",
                order_int.unwrap_or(0)
            ),
        )
    })?;

    // ---- rule 5: argv ----
    let argv = typed_arr("process", "argv")?
        .ok_or_else(|| malformed("missing required key `[process].argv`".to_string()))?;
    if argv.is_empty() {
        return Err(refuse(
            RefusalCode::MissingBinary,
            "`[process].argv` is empty".to_string(),
        ));
    }
    let argv0 = Path::new(&argv[0]);
    if !argv0.is_absolute() {
        return Err(refuse(
            RefusalCode::MissingBinary,
            format!(
                "`argv[0] = \"{}` is not absolute — no PATH lookup, no shell",
                argv[0]
            ),
        ));
    }
    match std::fs::metadata(argv0) {
        Ok(meta) if meta.is_file() => {}
        Ok(_) => {
            return Err(refuse(
                RefusalCode::MissingBinary,
                format!("`argv[0] = \"{}\" is not a regular file", argv[0]),
            ));
        }
        Err(e) => {
            return Err(refuse(
                RefusalCode::MissingBinary,
                format!("`argv[0] = \"{}\" does not exist: {e}", argv[0]),
            ));
        }
    }

    // ---- rule 6: sha256 shape (the file digest itself is verified at
    // spawn and every respawn, §10.3 step 6) ----
    let sha256 = typed_str("process", "sha256")?;
    if sha256.len() != 64
        || !sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(refuse(
            RefusalCode::DigestMismatch,
            format!("`[process].sha256 = \"{sha256}\" is not 64 lowercase hex digits"),
        ));
    }

    // ---- rule 7: env_allow grammar, duplicates, kian secrets ----
    let env_allow = typed_arr("process", "env_allow")?.unwrap_or_default();
    let valid_env = |s: &str| {
        let mut chars = s.chars();
        matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    };
    for entry in &env_allow {
        if !valid_env(entry) {
            return Err(refuse(
                RefusalCode::MalformedManifest,
                format!("`env_allow` entry `{entry}` is not ^[A-Za-z_][A-Za-z0-9_]*$"),
            ));
        }
        if env_allow.iter().filter(|e| *e == entry).count() > 1 {
            return Err(refuse(
                RefusalCode::MalformedManifest,
                format!("`env_allow` entry `{entry}` is duplicated"),
            ));
        }
        if ctx.secret_env.iter().any(|s| s == entry) {
            return Err(refuse(
                RefusalCode::MalformedManifest,
                format!("`env_allow` entry `{entry}` names a kian secret variable"),
            ));
        }
    }
    let startup_timeout_ms =
        u64::try_from(typed_int("process", "startup_timeout_ms")?.unwrap_or(2000))
            .map_err(|_| malformed("`[process].startup_timeout_ms` is out of range".to_string()))?;
    if !(100..=30_000).contains(&startup_timeout_ms) {
        return Err(malformed(format!(
            "`[process].startup_timeout_ms = {startup_timeout_ms}` is outside 100..=30000"
        )));
    }

    // ---- rule 8: hooks in the catalog (harness/* registry is empty;
    // plugin/* is bus-owned, never sidecar-subscribable) ----
    let hook_names = typed_arr("grants", "hooks")?
        .ok_or_else(|| malformed("missing required key `[grants].hooks`".to_string()))?;
    let mut hooks = Vec::new();
    for entry in &hook_names {
        match HookId::parse(entry) {
            Some(id) if id.sidecar_subscribable() && !matches!(id, HookId::Harness(_)) => {
                hooks.push(id)
            }
            _ => {
                return Err(refuse(
                    RefusalCode::UnknownHook,
                    format!("`[grants].hooks` entry `{entry}` is not sidecar-subscribable in v1"),
                ));
            }
        }
    }

    // ---- rules 9/10: capabilities closed set + grant consistency ----
    let capability_names = typed_arr("grants", "capabilities")?
        .ok_or_else(|| malformed("missing required key `[grants].capabilities`".to_string()))?;
    let grants = GrantSet::from_names(
        &capability_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
    )
    .map_err(|e| match e {
        super::super::grants::GrantError::UnknownCapability { name } => refuse(
            RefusalCode::UnknownCapability,
            format!("`[grants].capabilities` entry `{name}` is outside the §5.1 closed set"),
        ),
        super::super::grants::GrantError::MissingObserve { veto, observe } => refuse(
            RefusalCode::GrantMismatch,
            format!("grant `{veto}` requires its matching `{observe}` (§5.1)"),
        ),
    })?;
    for hook in &hooks {
        if !grants.observes_class(hook.class()) {
            return Err(refuse(
                RefusalCode::GrantMismatch,
                format!(
                    "subscribed hook `{hook}` has no matching `observe:{}` grant (§6.2 rule 10)",
                    hook.class().as_str()
                ),
            ));
        }
    }
    let veto_needs_subscription = |grant: Grant, hook: &HookId| -> bool {
        match grant {
            Grant::VetoAdmit => matches!(hook, HookId::AdmitPre),
            Grant::VetoTool => matches!(hook, HookId::ToolPre),
            Grant::VetoSettle => matches!(hook, HookId::SettlePre),
            _ => false,
        }
    };
    for grant in grants.iter() {
        let is_veto = matches!(
            grant,
            Grant::VetoAdmit | Grant::VetoTool | Grant::VetoSettle
        );
        if is_veto
            && !hooks
                .iter()
                .any(|hook| veto_needs_subscription(grant, hook))
        {
            return Err(refuse(
                RefusalCode::GrantMismatch,
                format!("grant `{grant}` has no subscribing veto-capable hook (§6.2 rule 10)"),
            ));
        }
    }

    // ---- rules 11: limits clamps ----
    // §6.1: `max_settle_vetoes` defaults to 3 — the struct `Default`
    // carries it (n2), so an absent key parses to 3 here too while an
    // explicit `0` still disables the settle gate; every other key's
    // default is the struct's.
    let mut limits = LimitsSection::default();
    let clamped = |key: &str, value: i64, range: (u64, u64)| -> Result<u64, Refusal> {
        let value =
            u64::try_from(value).map_err(|_| malformed(format!("`[limits].{key}` is negative")))?;
        if !(range.0..=range.1).contains(&value) {
            return Err(malformed(format!(
                "`[limits].{key} = {value}` is outside {}..={}",
                range.0, range.1
            )));
        }
        Ok(value)
    };
    if let Some(v) = typed_int("limits", "deadline_ms")? {
        limits.deadline_ms = clamped("deadline_ms", v, (50, 5_000))?;
    }
    if let Some(v) = typed_int("limits", "max_payload_kib")? {
        limits.max_payload_kib = clamped("max_payload_kib", v, (16, 1_024))?;
    }
    if let Some(v) = typed_int("limits", "restarts_per_window")? {
        limits.restarts_per_window = clamped("restarts_per_window", v, (1, 10))?;
    }
    if let Some(v) = typed_int("limits", "restart_window_ms")? {
        limits.restart_window_ms = clamped("restart_window_ms", v, (1_000, 3_600_000))?;
    }
    if let Some(v) = typed_int("limits", "max_settle_vetoes")? {
        limits.max_settle_vetoes = clamped("max_settle_vetoes", v, (0, 10))?;
    }
    if let Some(v) = typed_int("limits", "max_stderr_line_kib")? {
        limits.max_stderr_line_kib = clamped("max_stderr_line_kib", v, (1, 64))?;
    }
    if let Some(v) = typed_int("limits", "stderr_rate_kib_per_min")? {
        limits.stderr_rate_kib_per_min = clamped("stderr_rate_kib_per_min", v, (64, 65_536))?;
    }

    // ---- rule 14: cwd (boot-time configuration check, not a spawn
    // step — the server never creates an operator-supplied directory) --
    let cwd = match find("process", "cwd") {
        Some(RawValue::Str(p)) => {
            let path = PathBuf::from(p);
            if !path.is_absolute() {
                return Err(malformed(format!(
                    "`[process].cwd = \"{p}\"` is not absolute"
                )));
            }
            match std::fs::metadata(&path) {
                Ok(meta) if meta.is_dir() => {}
                Ok(_) => {
                    return Err(malformed(format!(
                        "`[process].cwd = \"{p}\" is not a directory"
                    )));
                }
                Err(e) => {
                    return Err(malformed(format!(
                        "`[process].cwd = \"{p}\" is not usable: {e}"
                    )));
                }
            }
            // Readable and writable by the server's user: probe with a
            // file the server itself creates and removes.
            let probe = path.join(format!(".kian-cwd-probe-{}", std::process::id()));
            match std::fs::write(&probe, b"") {
                Ok(()) => {
                    let _ = std::fs::remove_file(&probe);
                }
                Err(e) => {
                    return Err(malformed(format!(
                        "`[process].cwd = \"{p}\" is not writable: {e}"
                    )));
                }
            }
            Some(path)
        }
        Some(other) => {
            return Err(malformed(format!(
                "[process].cwd must be a string, got {}",
                other.type_name()
            )));
        }
        None => None,
    };

    Ok(Manifest {
        plugin: PluginSection {
            name,
            version,
            abi,
            order,
            required,
        },
        process: ProcessSection {
            argv,
            sha256,
            cwd,
            env_allow,
            startup_timeout_ms,
        },
        grants: GrantsSection { hooks, grants },
        limits,
    })
}

/// `MAJOR.MINOR.PATCH` with optional pre-release/build (rule 4).
fn is_semver(s: &str) -> bool {
    let (core, rest) = match s.split_once(['-', '+']) {
        Some((core, rest)) => (core, Some(rest)),
        None => (s, None),
    };
    let mut parts = core.split('.');
    let numeric = |p: Option<&str>| match p {
        Some(p) => !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()),
        None => false,
    };
    if !(numeric(parts.next()) && numeric(parts.next()) && numeric(parts.next())) {
        return false;
    }
    if parts.next().is_some() {
        return false;
    }
    match rest {
        Some(rest) => {
            !rest.is_empty()
                && rest
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'-' || b == b'+')
        }
        None => true,
    }
}

// `HookClass::as_str` does not exist on the P1 surface; the grant-
// mismatch message needs the class spelling, so derive it locally.
trait ClassSpell {
    fn as_str(&self) -> &'static str;
}

impl ClassSpell for super::super::ids::HookClass {
    fn as_str(&self) -> &'static str {
        match self {
            super::super::ids::HookClass::Admit => "admit",
            super::super::ids::HookClass::Turn => "turn",
            super::super::ids::HookClass::Tool => "tool",
            super::super::ids::HookClass::Recover => "recover",
            super::super::ids::HookClass::Settle => "settle",
            super::super::ids::HookClass::Harness => "harness",
            super::super::ids::HookClass::Plugin => "plugin",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>() -> ValidationCtx<'a> {
        ValidationCtx {
            instance_key: "approval-gate",
            reserved_names: &[],
            secret_env: &[],
        }
    }

    use crate::hooks::bus::DEFAULT_MAX_SETTLE_VETOS;

    fn valid_toml() -> String {
        [
            "[plugin]",
            "name = \"approval-gate\"",
            "version = \"0.1.0\"",
            "abi = 1",
            "",
            "[process]",
            "argv = [\"/bin/cat\"]",
            "sha256 = \"a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2\"",
            "",
            "[grants]",
            "hooks = [\"tool/pre\", \"tool/post\"]",
            "capabilities = [\"observe:tool\", \"veto:tool\"]",
            "",
            "[limits]",
            "deadline_ms = 500",
        ]
        .iter()
        .map(|l| format!("{l}\n"))
        .collect()
    }

    #[test]
    fn parses_the_spec_example() {
        let manifest = parse_manifest(&valid_toml(), &ctx()).expect("valid manifest");
        assert_eq!(manifest.plugin.name, "approval-gate");
        assert_eq!(manifest.plugin.version, "0.1.0");
        assert_eq!(manifest.plugin.abi, 1);
        assert_eq!(manifest.plugin.order, 0, "order defaults to 0");
        assert!(manifest.plugin.required, "required defaults fail-closed");
        assert_eq!(manifest.process.argv, vec!["/bin/cat".to_string()]);
        assert_eq!(manifest.limits.deadline_ms, 500);
        assert_eq!(manifest.limits.max_payload_kib, 256, "limits default");
        assert_eq!(manifest.grants.hooks.len(), 2);
        assert!(manifest.grants.grants.contains(Grant::VetoTool));
    }

    /// n2: the struct `Default` carries §6.1's settle default, so no
    /// future `..LimitsSection::default()` can silently disable the
    /// settle gate; an absent key parses to 3, an explicit `0` is the
    /// only way to disable.
    #[test]
    fn limits_default_carries_the_settle_budget() {
        assert_eq!(
            LimitsSection::default().max_settle_vetoes,
            DEFAULT_MAX_SETTLE_VETOS,
            "§6.1: Default must carry 3, never 0 (the M1 trap)"
        );
        // The `valid_toml()` fixture omits `max_settle_vetoes` — the
        // absent key parses to the same default.
        let manifest = parse_manifest(&valid_toml(), &ctx()).expect("valid manifest");
        assert_eq!(
            manifest.limits.max_settle_vetoes, DEFAULT_MAX_SETTLE_VETOS,
            "an absent key parses to §6.1's 3"
        );
        // An explicit 0 is honored as "disabled" (clamped range 0..=10).
        let disabled = valid_toml().replace("[limits]\n", "[limits]\nmax_settle_vetoes = 0\n");
        let manifest = parse_manifest(&disabled, &ctx()).expect("valid manifest");
        assert_eq!(manifest.limits.max_settle_vetoes, 0, "explicit 0 disables");
    }

    #[test]
    fn rule_1_unknown_key_is_malformed() {
        for text in [
            valid_toml().replace("[plugin]\n", "[plugin]\nbogus = 1\n"),
            valid_toml().replace("[limits]\n", "[limits]\nretries = 9\n"),
            valid_toml().replace("[plugin]", "[plumbing]"),
        ] {
            let refusal = parse_manifest(&text, &ctx()).expect_err("unknown key refuses");
            assert_eq!(refusal.code, RefusalCode::MalformedManifest);
            assert_eq!(refusal.required, Required::Unknown);
            assert!(
                refusal.message.contains("unknown") || refusal.message.contains("section"),
                "names the offender: {}",
                refusal.message
            );
        }
    }

    #[test]
    fn rule_2_name_grammar_and_instance_key() {
        for name in ["Approval", "-x", "has space", &"a".repeat(65)] {
            let text = valid_toml().replace("\"approval-gate\"", &format!("\"{name}\""));
            let refusal = parse_manifest(&text, &ctx()).expect_err("bad name refuses");
            assert_eq!(refusal.code, RefusalCode::NameInvalid);
        }
        let text = valid_toml(); // name != instance key
        let other = ValidationCtx {
            instance_key: "other-key",
            ..ctx()
        };
        let refusal = parse_manifest(&text, &other).expect_err("key mismatch refuses");
        assert_eq!(refusal.code, RefusalCode::NameInvalid);
    }

    #[test]
    fn rule_3_reserved_name_collides() {
        let reserved = ValidationCtx {
            instance_key: "approval-gate",
            reserved_names: &["approval-gate".to_string()],
            secret_env: &[],
        };
        let refusal = parse_manifest(&valid_toml(), &reserved).expect_err("collision refuses");
        assert_eq!(refusal.code, RefusalCode::NameCollision);
    }

    #[test]
    fn rule_4_semver_and_abi() {
        let bad_version = valid_toml().replace("0.1.0", "0.1");
        assert_eq!(
            parse_manifest(&bad_version, &ctx()).unwrap_err().code,
            RefusalCode::MalformedManifest
        );
        let bad_abi = valid_toml().replace("abi = 1", "abi = 2");
        assert_eq!(
            parse_manifest(&bad_abi, &ctx()).unwrap_err().code,
            RefusalCode::AbiMismatch
        );
        // Valid pre-release semver is accepted.
        let ok = valid_toml().replace("0.1.0", "1.2.3-rc.1");
        assert!(parse_manifest(&ok, &ctx()).is_ok());
    }

    #[test]
    fn rule_5_argv_must_exist_and_be_absolute() {
        let relative = valid_toml().replace("\"/bin/cat\"", "\"cat\"");
        assert_eq!(
            parse_manifest(&relative, &ctx()).unwrap_err().code,
            RefusalCode::MissingBinary
        );
        let missing = valid_toml().replace("/bin/cat", "/nonexistent/definitely-absent");
        let refusal = parse_manifest(&missing, &ctx()).unwrap_err();
        assert_eq!(refusal.code, RefusalCode::MissingBinary);
        assert!(refusal.message.contains("does not exist"));
    }

    #[test]
    fn rule_6_sha256_shape() {
        let bad = valid_toml().replace(
            "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
            "A1B2C3D4E5F6A7B8C9D0E1F2A3B4C5D6E7F8A9B0C1D2E3F4A5B6C7D8E9F0A1B2",
        );
        assert_eq!(
            parse_manifest(&bad, &ctx()).unwrap_err().code,
            RefusalCode::DigestMismatch
        );
    }

    #[test]
    fn rule_7_env_allow_grammar_duplicates_secrets() {
        let secret = ValidationCtx {
            instance_key: "approval-gate",
            reserved_names: &[],
            secret_env: &["KIAN_WORKER_TOKEN".to_string()],
        };
        let bad = valid_toml().replace(
            "[grants]",
            "env_allow = [\"KIAN_WORKER_TOKEN\"]\n\n[grants]",
        );
        assert_eq!(
            parse_manifest(&bad, &secret).unwrap_err().code,
            RefusalCode::MalformedManifest
        );
        let dup = valid_toml().replace("[grants]", "env_allow = [\"URL\", \"URL\"]\n\n[grants]");
        assert!(parse_manifest(&dup, &ctx()).is_err());
        let bad_name = valid_toml().replace("[grants]", "env_allow = [\"1BAD\"]\n\n[grants]");
        assert!(parse_manifest(&bad_name, &ctx()).is_err());
    }

    #[test]
    fn rule_8_unknown_hook() {
        for hook in [
            "tool/preflight",
            "plugin/load",
            "harness/dsh/start",
            "nonsense",
        ] {
            let text = valid_toml().replace(
                "hooks = [\"tool/pre\", \"tool/post\"]",
                &format!("hooks = [\"{hook}\"]"),
            );
            let refusal = parse_manifest(&text, &ctx()).unwrap_err();
            assert_eq!(refusal.code, RefusalCode::UnknownHook, "{hook}");
        }
    }

    #[test]
    fn rules_9_10_capabilities_and_grant_consistency() {
        let unknown = valid_toml().replace(
            "capabilities = [\"observe:tool\", \"veto:tool\"]",
            "capabilities = [\"mutate:context\"]",
        );
        assert_eq!(
            parse_manifest(&unknown, &ctx()).unwrap_err().code,
            RefusalCode::UnknownCapability
        );
        // Subscribed hook without the matching observe grant
        // (an unused observe grant alone is legal — rule 10 constrains
        // subscriptions and veto grants, not extra observes).
        let no_observe = valid_toml().replace(
            "hooks = [\"tool/pre\", \"tool/post\"]",
            "hooks = [\"admit/pre\"]",
        );
        let refusal = parse_manifest(&no_observe, &ctx()).unwrap_err();
        assert_eq!(refusal.code, RefusalCode::GrantMismatch);
        // A veto grant with no veto-capable subscribed hook.
        let orphan_veto = valid_toml().replace(
            "hooks = [\"tool/pre\", \"tool/post\"]",
            "hooks = [\"tool/post\"]",
        );
        assert_eq!(
            parse_manifest(&orphan_veto, &ctx()).unwrap_err().code,
            RefusalCode::GrantMismatch
        );
    }

    #[test]
    fn rule_11_limit_clamps() {
        let over = valid_toml().replace("deadline_ms = 500", "deadline_ms = 9999");
        assert_eq!(
            parse_manifest(&over, &ctx()).unwrap_err().code,
            RefusalCode::MalformedManifest
        );
        let under = valid_toml().replace("deadline_ms = 500", "deadline_ms = 10");
        assert!(parse_manifest(&under, &ctx()).is_err());
        let restarts = valid_toml().replace("deadline_ms = 500", "restarts_per_window = 0");
        assert!(parse_manifest(&restarts, &ctx()).is_err());
    }

    #[test]
    fn rule_14_cwd_checks() {
        let relative = valid_toml().replace("[grants]", "cwd = \"relative/dir\"\n\n[grants]");
        assert_eq!(
            parse_manifest(&relative, &ctx()).unwrap_err().code,
            RefusalCode::MalformedManifest
        );
        let missing = valid_toml().replace(
            "[grants]",
            "cwd = \"/nonexistent/definitely-absent-dir\"\n\n[grants]",
        );
        let refusal = parse_manifest(&missing, &ctx()).unwrap_err();
        assert_eq!(refusal.code, RefusalCode::MalformedManifest);
        assert!(refusal.message.contains("cwd"));
    }

    #[test]
    fn wrong_types_are_malformed() {
        let bad_order = valid_toml().replace("abi = 1", "order = \"zero\"");
        assert_eq!(
            parse_manifest(&bad_order, &ctx()).unwrap_err().code,
            RefusalCode::MalformedManifest
        );
        let bad_required = valid_toml().replace("abi = 1", "required = 1");
        assert!(parse_manifest(&bad_required, &ctx()).is_err());
        let duplicate = valid_toml().replace("[process]", "[process]\nabi = 1\n");
        assert!(parse_manifest(&duplicate, &ctx()).is_err());
    }
}
