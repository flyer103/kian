//! Per-worker resolution (PLUGIN-SPEC §8): the two-scope `sidecars:`
//! grammar, the boot checks that make it closed, and `resolve` as a
//! pure function of (boot config, the session's durable `exec_worker`
//! binding).
//!
//! The types here are the **config surface's** data model, not a YAML
//! reader: `kian-cli` deserializes its `sidecars:` section into
//! [`SidecarConfig`] (every struct `deny_unknown_fields`, so an unknown
//! scope key — `harness:`, `profile:` — refuses boot naming the key,
//! §8.1/§6.2 boundary case 2). Validation splits in two, exactly where
//! the inputs live:
//!
//! - [`validate_shape`] needs names only and runs at config validation
//!   (a read-only verb can catch a typo);
//! - [`validate`] adds the widening check and runs in the serve
//!   assembly, where the manifests are loaded.
//!
//! [`resolve`] follows §8.2's pseudocode verbatim: sorted iteration, the
//! worker layer as a **filter** over the server set (never a second
//! source), `limits:` from exactly one narrowing source, `min_keys`
//! semantics per bounded key. No wall clock, no I/O, no registry cache,
//! no assembly or harness knowledge — the binding is the durable
//! `exec_worker` id and nothing else (§16.18).

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::sidecar::manifest::{LimitsSection, Manifest};

// ---------------------------------------------------------------------
// §8.1 — the closed grammar
// ---------------------------------------------------------------------

/// The `sidecars:` section (§8.1): instance declarations ("what may
/// exist") plus the two scopes ("who gets which instance").
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SidecarConfig {
    /// Instance declarations; each key is the instance name and must
    /// equal its manifest `[plugin].name` (§6.2 rule 2, checked where
    /// the manifest is parsed).
    #[serde(default)]
    pub plugins: BTreeMap<String, InstanceDecl>,
    /// The two scopes. Exactly `server` and `worker`; anything else is
    /// an unknown key and refuses boot (§8.1).
    #[serde(default)]
    pub scopes: ScopesConfig,
}

/// One instance declaration: where the closed-TOML manifest lives.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceDecl {
    pub manifest: PathBuf,
}

/// `sidecars.scopes` — exactly two keys (§8.1).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopesConfig {
    /// Every session's widest set; the widest set any worker may hold.
    /// An empty list is legal and means "no sidecars for anyone".
    #[serde(default)]
    pub server: Vec<String>,
    /// Per execution-worker id — narrows only, never adds.
    #[serde(default)]
    pub worker: BTreeMap<String, WorkerScope>,
}

/// `sidecars.scopes.worker.<id>` (§8.1): `use:` (a subset of
/// `scopes.server`) and/or `limits:` (per-instance narrowing). An
/// **empty** `use: []` is legal and resolves that worker to an empty
/// set; an absent `use:` keeps the whole server set — the two are not
/// the same (§8.1).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerScope {
    #[serde(default, rename = "use")]
    pub use_: Option<Vec<String>>,
    #[serde(default)]
    pub limits: BTreeMap<String, LimitOverrides>,
}

/// A per-instance limit override at the worker scope (§8.1): every key
/// optional, every present key may only **shrink** the manifest bound
/// (§8.2 invariant 2 — a widen refuses boot).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitOverrides {
    pub deadline_ms: Option<u64>,
    pub max_payload_kib: Option<u64>,
    pub restarts_per_window: Option<u64>,
    pub restart_window_ms: Option<u64>,
    pub max_settle_vetoes: Option<u64>,
    pub max_stderr_line_kib: Option<u64>,
    pub stderr_rate_kib_per_min: Option<u64>,
}

/// Which layer declared a name in the effective set (§8.5's `scope`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeLayer {
    Server,
    Worker,
}

impl ScopeLayer {
    pub fn as_str(self) -> &'static str {
        match self {
            ScopeLayer::Server => "server",
            ScopeLayer::Worker => "worker",
        }
    }
}

// ---------------------------------------------------------------------
// Boot validation (§8.1's closed-grammar rules + §8.2 invariant 2)
// ---------------------------------------------------------------------

/// A boot-time config error: the code (stable, machine-readable) and a
/// message naming the offender (§6.2's discipline — a typo must never
/// silently do nothing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveError {
    pub code: ResolveErrorCode,
    pub message: String,
}

/// The closed code set for §8 grammar violations. These are config-boot
/// codes (they refuse `kian serve` as config validation does everywhere
/// in `kian-cli`, §6.2 boundary case 2), not `plugin/refuse` codes —
/// no instance owns a grammar error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveErrorCode {
    /// A set entry (or a `limits:` key) that is not a declared
    /// `sidecars.plugins` instance.
    UndeclaredInstance,
    /// `worker.<id>.use` is not a subset of `scopes.server`.
    SubsetViolation,
    /// A `limits:` key naming an instance that worker cannot receive.
    LimitNotReceivable,
    /// A worker limit that widens the manifest bound (§8.2 invariant 2).
    LimitWidening,
}

impl ResolveErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ResolveErrorCode::UndeclaredInstance => "undeclared_instance",
            ResolveErrorCode::SubsetViolation => "subset_violation",
            ResolveErrorCode::LimitNotReceivable => "limit_not_receivable",
            ResolveErrorCode::LimitWidening => "limit_widening",
        }
    }
}

/// The name-only boot checks (§8.1): every set entry is a declared
/// instance, `use ⊆ scopes.server`, and every `limits:` key names an
/// instance that worker can actually receive. Runs at config
/// validation — no manifest file is read.
pub fn validate_shape(cfg: &SidecarConfig) -> Result<(), Vec<ResolveError>> {
    let mut errors = Vec::new();
    let declared = |name: &str| cfg.plugins.contains_key(name);

    for name in &cfg.scopes.server {
        if !declared(name) {
            errors.push(ResolveError {
                code: ResolveErrorCode::UndeclaredInstance,
                message: format!(
                    "sidecars.scopes.server entry `{name}` is not a declared sidecars.plugins \
                     instance (a typo must refuse boot, never silently do nothing)"
                ),
            });
        }
    }

    for (worker, scope) in &cfg.scopes.worker {
        if let Some(use_) = &scope.use_ {
            for name in use_ {
                if !declared(name) {
                    errors.push(ResolveError {
                        code: ResolveErrorCode::UndeclaredInstance,
                        message: format!(
                            "sidecars.scopes.worker.{worker}.use entry `{name}` is not a \
                             declared sidecars.plugins instance"
                        ),
                    });
                } else if !cfg.scopes.server.contains(name) {
                    errors.push(ResolveError {
                        code: ResolveErrorCode::SubsetViolation,
                        message: format!(
                            "sidecars.scopes.worker.{worker}.use entry `{name}` is not in \
                             scopes.server — `use:` is a filter over the server set, never a \
                             second source (§8.1)"
                        ),
                    });
                }
            }
        }
        for instance in scope.limits.keys() {
            let receivable = match &scope.use_ {
                Some(use_) => use_.iter().any(|n| n == instance),
                None => cfg.scopes.server.iter().any(|n| n == instance),
            };
            if !receivable {
                errors.push(ResolveError {
                    code: ResolveErrorCode::LimitNotReceivable,
                    message: format!(
                        "sidecars.scopes.worker.{worker}.limits key `{instance}` names an \
                         instance that worker cannot receive — a narrowing nobody ever sees \
                         applied is a config error, not a silent no-op (§8.1)"
                    ),
                });
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// The full boot check: [`validate_shape`] plus the widening rule
/// (§8.2 invariant 2 — every present worker limit must shrink the
/// manifest bound; values are static, so a widen is always
/// detectable). Runs in the serve assembly, after the manifests are
/// loaded and validated.
pub fn validate(
    cfg: &SidecarConfig,
    manifests: &BTreeMap<String, Manifest>,
) -> Result<(), Vec<ResolveError>> {
    let mut errors = match validate_shape(cfg) {
        Ok(()) => Vec::new(),
        Err(errors) => errors,
    };
    for (worker, scope) in &cfg.scopes.worker {
        for (instance, limits) in &scope.limits {
            let Some(manifest) = manifests.get(instance) else {
                // validate_shape already reported the undeclared /
                // unreceivable cases; do not double-report.
                continue;
            };
            let bound = manifest.limits;
            let mut widen = |key: &str, configured: Option<u64>, manifest: u64| {
                if let Some(value) = configured
                    && value > manifest
                {
                    errors.push(ResolveError {
                        code: ResolveErrorCode::LimitWidening,
                        message: format!(
                            "sidecars.scopes.worker.{worker}.limits.{instance}.{key} = {value} \
                             widens the manifest bound {manifest} — narrowing only (§8.2 \
                             invariant 2)"
                        ),
                    });
                }
            };
            widen("deadline_ms", limits.deadline_ms, bound.deadline_ms);
            widen(
                "max_payload_kib",
                limits.max_payload_kib,
                bound.max_payload_kib,
            );
            widen(
                "restarts_per_window",
                limits.restarts_per_window,
                bound.restarts_per_window,
            );
            widen(
                "restart_window_ms",
                limits.restart_window_ms,
                bound.restart_window_ms,
            );
            widen(
                "max_settle_vetoes",
                limits.max_settle_vetoes,
                bound.max_settle_vetoes,
            );
            widen(
                "max_stderr_line_kib",
                limits.max_stderr_line_kib,
                bound.max_stderr_line_kib,
            );
            widen(
                "stderr_rate_kib_per_min",
                limits.stderr_rate_kib_per_min,
                bound.stderr_rate_kib_per_min,
            );
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

// ---------------------------------------------------------------------
// §8.2 — resolve (pure)
// ---------------------------------------------------------------------

/// One member of a session's effective set: the instance, its
/// post-narrowing limits, and the layer that declared it (the layer the
/// set digest records, §8.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectivePlugin {
    pub name: String,
    pub manifest: Manifest,
    pub limits: LimitsSection,
    pub layer: ScopeLayer,
}

/// The result of [`resolve`]: the effective set (sorted by name,
/// ascending bytes) and the server names this worker narrowed away
/// (one audit line at the caller, §8.2).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resolution {
    pub effective: Vec<EffectivePlugin>,
    pub dropped: Vec<String>,
}

/// §8.2's `resolve`, verbatim: a pure function of (boot config, the
/// session's durable binding). `binding` is the durable `exec_worker`
/// id; `None` (and the empty string, the wire's honest floor for an
/// unbound session, §7.3) resolves the server scope only — a
/// deterministic answer, not a fallback.
///
/// A name whose manifest is absent from `manifests` (an instance that
/// was refused at boot with `required = false`, §6.2) is simply not in
/// the set: the refusal's audit line is the record, and the digest
/// never counts an instance that is not there.
pub fn resolve(
    cfg: &SidecarConfig,
    manifests: &BTreeMap<String, Manifest>,
    binding: Option<&str>,
) -> Resolution {
    let binding = binding.filter(|b| !b.is_empty());
    let w = binding.and_then(|b| cfg.scopes.worker.get(b));

    // Sorted, deduplicated iteration — never config-file order (§8.2
    // invariant 3). A BTreeSet over the server list gives both.
    let server: std::collections::BTreeSet<&String> = cfg.scopes.server.iter().collect();

    let mut chosen: BTreeMap<&String, ScopeLayer> = BTreeMap::new();
    let mut dropped: Vec<String> = Vec::new();
    for name in &server {
        // The worker layer may never ADD: `use ⊆ scopes.server` is a
        // static boot check, so resolution can never see a name the
        // server scope did not declare.
        if let Some(use_) = w.and_then(|w| w.use_.as_ref())
            && !use_.contains(name)
        {
            dropped.push((*name).clone());
            continue;
        }
        // A name the worker's `use:` restates is declared BY the worker
        // layer; with no `use:`, the worker layer declares nothing.
        chosen.insert(
            name,
            if w.is_some_and(|w| w.use_.is_some()) {
                ScopeLayer::Worker
            } else {
                ScopeLayer::Server
            },
        );
    }

    let empty_narrow: BTreeMap<String, LimitOverrides> = BTreeMap::new();
    let narrow = w.map_or(&empty_narrow, |w| &w.limits);
    let mut effective = Vec::with_capacity(chosen.len());
    for (name, layer) in chosen {
        let Some(manifest) = manifests.get(name) else {
            continue;
        };
        let limits = min_keys(&manifest.limits, narrow.get(name));
        effective.push(EffectivePlugin {
            name: (*name).clone(),
            manifest: manifest.clone(),
            limits,
            layer,
        });
    }
    Resolution { effective, dropped }
}

/// §8.2's `min_keys`: each bounded key takes the smaller of the
/// manifest bound and the worker override; an absent override keeps the
/// manifest value.
fn min_keys(bound: &LimitsSection, narrow: Option<&LimitOverrides>) -> LimitsSection {
    let Some(narrow) = narrow else {
        return *bound;
    };
    let min = |configured: Option<u64>, manifest: u64| {
        configured.map_or(manifest, |value| value.min(manifest))
    };
    LimitsSection {
        deadline_ms: min(narrow.deadline_ms, bound.deadline_ms),
        max_payload_kib: min(narrow.max_payload_kib, bound.max_payload_kib),
        restarts_per_window: min(narrow.restarts_per_window, bound.restarts_per_window),
        restart_window_ms: min(narrow.restart_window_ms, bound.restart_window_ms),
        max_settle_vetoes: min(narrow.max_settle_vetoes, bound.max_settle_vetoes),
        max_stderr_line_kib: min(narrow.max_stderr_line_kib, bound.max_stderr_line_kib),
        stderr_rate_kib_per_min: min(
            narrow.stderr_rate_kib_per_min,
            bound.stderr_rate_kib_per_min,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(name: &str, deadline_ms: u64, max_payload_kib: u64) -> Manifest {
        let text = format!(
            "[plugin]\nname = \"{name}\"\nversion = \"0.1.0\"\nabi = 1\n\
             [process]\nargv = [\"/bin/cat\"]\nsha256 = \"{}\"\n\
             [grants]\nhooks = [\"tool/pre\"]\ncapabilities = [\"observe:tool\"]\n\
             [limits]\ndeadline_ms = {deadline_ms}\nmax_payload_kib = {max_payload_kib}\n",
            "a".repeat(64)
        );
        let ctx = crate::hooks::sidecar::ValidationCtx {
            instance_key: name,
            ..Default::default()
        };
        crate::hooks::sidecar::parse_manifest(&text, &ctx).expect("fixture manifest parses")
    }

    fn fixtures() -> (SidecarConfig, BTreeMap<String, Manifest>) {
        let cfg: SidecarConfig = serde_json::from_value(serde_json::json!({
            "plugins": {
                "approval-gate": {"manifest": "/etc/kian/plugins/approval-gate.toml"},
                "cost-audit": {"manifest": "/etc/kian/plugins/cost-audit.toml"}
            },
            "scopes": {
                "server": ["approval-gate", "cost-audit"],
                "worker": {
                    "node-zulu": {
                        "use": ["approval-gate"],
                        "limits": {"approval-gate": {"deadline_ms": 200}}
                    },
                    "node-yankee": {
                        "limits": {"cost-audit": {"max_payload_kib": 64}}
                    }
                }
            }
        }))
        .unwrap();
        let mut manifests = BTreeMap::new();
        manifests.insert(
            "approval-gate".to_string(),
            manifest("approval-gate", 500, 256),
        );
        manifests.insert("cost-audit".to_string(), manifest("cost-audit", 500, 256));
        (cfg, manifests)
    }

    #[test]
    fn spec_walkthrough_node_zulu() {
        let (cfg, manifests) = fixtures();
        let resolution = resolve(&cfg, &manifests, Some("node-zulu"));
        assert_eq!(resolution.effective.len(), 1, "the gate only");
        assert_eq!(resolution.effective[0].name, "approval-gate");
        assert_eq!(resolution.effective[0].layer, ScopeLayer::Worker);
        assert_eq!(resolution.effective[0].limits.deadline_ms, 200);
        assert_eq!(
            resolution.effective[0].limits.max_payload_kib, 256,
            "every other limit at its manifest value"
        );
        assert_eq!(resolution.dropped, vec!["cost-audit".to_string()]);
    }

    #[test]
    fn spec_walkthrough_node_yankee() {
        let (cfg, manifests) = fixtures();
        let resolution = resolve(&cfg, &manifests, Some("node-yankee"));
        assert_eq!(resolution.effective.len(), 2);
        assert_eq!(resolution.effective[0].name, "approval-gate");
        assert_eq!(resolution.effective[0].layer, ScopeLayer::Server);
        assert_eq!(resolution.effective[0].limits.deadline_ms, 500);
        assert_eq!(resolution.effective[1].name, "cost-audit");
        assert_eq!(resolution.effective[1].limits.max_payload_kib, 64);
        assert!(resolution.dropped.is_empty());
    }

    #[test]
    fn unknown_worker_and_unbound_resolve_the_server_scope() {
        let (cfg, manifests) = fixtures();
        for binding in [Some("node-xray"), None, Some("")] {
            let resolution = resolve(&cfg, &manifests, binding);
            assert_eq!(resolution.effective.len(), 2, "binding {binding:?}");
            assert!(
                resolution
                    .effective
                    .iter()
                    .all(|p| p.layer == ScopeLayer::Server)
            );
        }
    }

    #[test]
    fn empty_use_is_the_empty_set_absent_use_keeps_everything() {
        let cfg: SidecarConfig = serde_json::from_value(serde_json::json!({
            "plugins": {"a": {"manifest": "/etc/a.toml"}},
            "scopes": {
                "server": ["a"],
                "worker": {"w-empty": {"use": []}, "w-absent": {}}
            }
        }))
        .unwrap();
        let mut manifests = BTreeMap::new();
        manifests.insert("a".to_string(), manifest("a", 500, 256));

        let empty = resolve(&cfg, &manifests, Some("w-empty"));
        assert!(empty.effective.is_empty());
        assert_eq!(empty.dropped, vec!["a".to_string()]);

        let full = resolve(&cfg, &manifests, Some("w-absent"));
        assert_eq!(full.effective.len(), 1);
        assert_eq!(full.effective[0].layer, ScopeLayer::Server);
    }

    #[test]
    fn subset_violation_refuses_naming_worker_and_name() {
        let cfg: SidecarConfig = serde_json::from_value(serde_json::json!({
            "plugins": {"a": {"manifest": "/etc/a.toml"}, "b": {"manifest": "/etc/b.toml"}},
            "scopes": {
                "server": ["a"],
                "worker": {"node-bad": {"use": ["b"]}}
            }
        }))
        .unwrap();
        let errors = validate_shape(&cfg).unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.code == ResolveErrorCode::SubsetViolation
                    && e.message.contains("node-bad")
                    && e.message.contains("`b`"))
        );
    }

    #[test]
    fn undeclared_instance_refuses_at_every_site() {
        let cfg: SidecarConfig = serde_json::from_value(serde_json::json!({
            "plugins": {"a": {"manifest": "/etc/a.toml"}, "b": {"manifest": "/etc/b.toml"}},
            "scopes": {
                "server": ["a", "typo-name"],
                "worker": {"w": {"use": ["a", "b", "ghost"], "limits": {"phantom": {}}}}
            }
        }))
        .unwrap();
        let errors = validate_shape(&cfg).unwrap_err();
        let codes: Vec<_> = errors.iter().map(|e| e.code).collect();
        // `typo-name` and `ghost` are not declared instances.
        assert!(codes.contains(&ResolveErrorCode::UndeclaredInstance));
        // `b` is declared but not in scopes.server → the subset rule.
        assert!(codes.contains(&ResolveErrorCode::SubsetViolation));
        // `phantom` is neither declared nor receivable.
        assert!(codes.contains(&ResolveErrorCode::LimitNotReceivable));
        assert!(errors.iter().all(|e| !e.message.is_empty()));
    }

    #[test]
    fn widening_refuses_naming_instance_key_and_values() {
        let (cfg, manifests) = fixtures();
        let mut widened = cfg.clone();
        widened
            .scopes
            .worker
            .get_mut("node-yankee")
            .unwrap()
            .limits
            .insert(
                "cost-audit".to_string(),
                LimitOverrides {
                    max_payload_kib: Some(512),
                    ..Default::default()
                },
            );
        let errors = validate(&widened, &manifests).unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.code == ResolveErrorCode::LimitWidening
                    && e.message.contains("node-yankee")
                    && e.message.contains("cost-audit")
                    && e.message.contains("512")
                    && e.message.contains("256"))
        );
        // The narrowing direction passes.
        assert!(validate(&cfg, &manifests).is_ok());
    }

    #[test]
    fn narrowing_only_takes_the_smaller_value() {
        let (cfg, manifests) = fixtures();
        // A deadline override larger than the manifest keeps the
        // manifest value at resolution (boot refuses it via `validate`;
        // `resolve` itself never widens).
        let mut widened = cfg.clone();
        widened
            .scopes
            .worker
            .get_mut("node-zulu")
            .unwrap()
            .limits
            .insert(
                "approval-gate".to_string(),
                LimitOverrides {
                    deadline_ms: Some(9_999),
                    ..Default::default()
                },
            );
        let resolution = resolve(&widened, &manifests, Some("node-zulu"));
        assert_eq!(resolution.effective[0].limits.deadline_ms, 500);
    }

    #[test]
    fn refused_optional_instance_is_absent_not_fatal() {
        let (cfg, manifests) = fixtures();
        let mut missing = manifests.clone();
        missing.remove("cost-audit");
        let resolution = resolve(&cfg, &missing, None);
        assert_eq!(resolution.effective.len(), 1);
        assert_eq!(resolution.effective[0].name, "approval-gate");
    }

    #[test]
    fn unknown_scope_key_refuses_naming_the_key() {
        let bad = serde_json::from_value::<SidecarConfig>(serde_json::json!({
            "scopes": {"harness": {"react": ["a"]}}
        }))
        .unwrap_err();
        assert!(bad.to_string().contains("harness"));
        let bad = serde_json::from_value::<SidecarConfig>(serde_json::json!({
            "scopes": {"profile": {"router": ["a"]}}
        }))
        .unwrap_err();
        assert!(bad.to_string().contains("profile"));
    }

    #[test]
    fn empty_section_is_legal_and_means_no_sidecars() {
        let cfg: SidecarConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(cfg, SidecarConfig::default());
        assert!(validate_shape(&cfg).is_ok());
        assert!(resolve(&cfg, &BTreeMap::new(), None).effective.is_empty());
    }

    #[test]
    fn resolution_is_independent_of_config_order() {
        let (cfg, manifests) = fixtures();
        // Same grammar, server list reversed and worker keys reordered:
        // JSON objects are maps, so build the reversed server list
        // explicitly.
        let mut reordered = cfg.clone();
        reordered.scopes.server.reverse();
        let a = resolve(&cfg, &manifests, Some("node-yankee"));
        let b = resolve(&reordered, &manifests, Some("node-yankee"));
        assert_eq!(a, b, "sorted iteration, never config-file order");
    }
}
