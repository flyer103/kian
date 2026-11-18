//! The set digest (PLUGIN-SPEC §8.5): the canonical JSON over the
//! effective set's closed behaviour-bearing field list, hashed with
//! `sha2` 0.10.
//!
//! The field list is the **whole input**, and it is closed: name,
//! version, abi, scope, order, required, binary_sha256,
//! process_sha256, hooks, capabilities, and every post-narrowing limit
//! key. Deliberately outside: the manifest **path** (identity at
//! handshake, §7.3 — the contents are the behaviour), the YAML's
//! textual layout, and anything a session cannot observe. The env
//! **values** never enter the digest (they are secrets and they change
//! without a config edit).
//!
//! Canonicalization is `json_compact_keys_sorted`: compact separators,
//! object keys sorted ascending by bytes. The writer below sorts
//! explicitly, so the digest is stable whether or not `serde_json`'s
//! `preserve_order` feature is ever enabled.

use super::resolve::EffectivePlugin;
use super::sidecar::manifest::Manifest;
use super::wire::sha256_hex;

/// A canonical JSON object: keys sorted ascending by bytes, compact
/// serialization. Built from an explicit pair list so the sort is
/// visible at the construction site, not a feature flag away.
fn canonical_object(pairs: Vec<(&str, serde_json::Value)>) -> serde_json::Value {
    let mut sorted = pairs;
    sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let map: serde_json::Map<String, serde_json::Value> = sorted
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    serde_json::Value::Object(map)
}

fn strings(items: impl IntoIterator<Item = impl Into<String>>) -> serde_json::Value {
    serde_json::Value::Array(
        items
            .into_iter()
            .map(|s| serde_json::Value::String(s.into()))
            .collect(),
    )
}

/// `process_sha256` (§8.5): different arguments, environment
/// allowlist, working directory or startup budget is different
/// behaviour. `env_allow` contributes **names only, sorted** — never
/// values.
pub fn process_sha256(manifest: &Manifest) -> String {
    let mut env_allow: Vec<String> = manifest
        .process
        .env_allow
        .iter()
        .map(|n| n.as_str().to_string())
        .collect();
    env_allow.sort();
    let cwd = manifest
        .process
        .cwd
        .as_ref()
        .map(|p| serde_json::Value::String(p.to_string_lossy().into_owned()))
        .unwrap_or(serde_json::Value::Null);
    let canonical = canonical_object(vec![
        (
            "argv",
            strings(manifest.process.argv.iter().map(|a| a.as_str())),
        ),
        ("env_allow", strings(env_allow)),
        ("cwd", cwd),
        (
            "startup_timeout_ms",
            serde_json::Value::from(manifest.process.startup_timeout_ms),
        ),
    ]);
    let text = serde_json::to_string(&canonical).expect("canonical JSON serializes");
    sha256_hex(text.as_bytes())
}

/// The per-plugin canonical segment of §8.5's `plugins` array entry.
fn plugin_entry(p: &EffectivePlugin) -> serde_json::Value {
    let mut hooks: Vec<String> = p
        .manifest
        .grants
        .hooks
        .iter()
        .map(|h| h.as_str().to_string())
        .collect();
    hooks.sort();
    let mut capabilities: Vec<String> = p
        .manifest
        .grants
        .grants
        .iter()
        .map(|g| g.as_str().to_string())
        .collect();
    capabilities.sort();
    canonical_object(vec![
        ("name", serde_json::Value::String(p.name.clone())),
        (
            "version",
            serde_json::Value::String(p.manifest.plugin.version.clone()),
        ),
        ("abi", serde_json::Value::from(p.manifest.plugin.abi)),
        (
            "scope",
            serde_json::Value::String(p.layer.as_str().to_string()),
        ),
        ("order", serde_json::Value::from(p.manifest.plugin.order)),
        (
            "required",
            serde_json::Value::Bool(p.manifest.plugin.required),
        ),
        (
            "binary_sha256",
            serde_json::Value::String(p.manifest.process.sha256.clone()),
        ),
        (
            "process_sha256",
            serde_json::Value::String(process_sha256(&p.manifest)),
        ),
        ("hooks", strings(hooks)),
        ("capabilities", strings(capabilities)),
        (
            "limits",
            canonical_object(vec![
                ("deadline_ms", serde_json::Value::from(p.limits.deadline_ms)),
                (
                    "max_payload_kib",
                    serde_json::Value::from(p.limits.max_payload_kib),
                ),
                (
                    "restarts_per_window",
                    serde_json::Value::from(p.limits.restarts_per_window),
                ),
                (
                    "restart_window_ms",
                    serde_json::Value::from(p.limits.restart_window_ms),
                ),
                (
                    "max_settle_vetoes",
                    serde_json::Value::from(p.limits.max_settle_vetoes),
                ),
                (
                    "max_stderr_line_kib",
                    serde_json::Value::from(p.limits.max_stderr_line_kib),
                ),
                (
                    "stderr_rate_kib_per_min",
                    serde_json::Value::from(p.limits.stderr_rate_kib_per_min),
                ),
            ]),
        ),
    ])
}

/// The canonical preimage of a single-plugin set digest — the test seam
/// for §8.5's stability properties: a test can hash this visible text
/// with its own `Sha256` and compare against [`set_digest`].
pub fn single_plugin_canonical(p: &EffectivePlugin) -> String {
    serde_json::to_string(&canonical_object(vec![
        ("schema", serde_json::Value::from(2)),
        ("plugins", serde_json::Value::Array(vec![plugin_entry(p)])),
    ]))
    .expect("canonical JSON serializes")
}

/// The set digest (§8.5): `"sha256:" + hex(sha256(utf8(canonical)))`
/// over schema 2's closed input. The `effective` slice is sorted by
/// name ascending bytes (the caller — [`super::resolve::resolve`] —
/// guarantees it; an unsorted slice is sorted defensively so the
/// digest can never depend on caller order).
pub fn set_digest(effective: &[EffectivePlugin]) -> String {
    let mut sorted: Vec<&EffectivePlugin> = effective.iter().collect();
    sorted.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
    let canonical = canonical_object(vec![
        ("schema", serde_json::Value::from(2)),
        (
            "plugins",
            serde_json::Value::Array(sorted.iter().map(|p| plugin_entry(p)).collect()),
        ),
    ]);
    let text = serde_json::to_string(&canonical).expect("canonical JSON serializes");
    format!("sha256:{}", sha256_hex(text.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::resolve::{self, EffectivePlugin, SidecarConfig};

    /// An explicit manifest fixture: every knob is a real override, so
    /// no tweak can collide with a defaulted key (the closed-TOML
    /// parser refuses duplicates — rule 1).
    struct Fixture {
        argv: &'static [&'static str],
        sha256: String,
        env_allow: &'static [&'static str],
        cwd: Option<std::path::PathBuf>,
        hooks: &'static [&'static str],
        capabilities: &'static [&'static str],
        limits: &'static str,
    }

    impl Default for Fixture {
        fn default() -> Self {
            Self {
                argv: &["/bin/cat"],
                sha256: "a".repeat(64),
                env_allow: &[],
                cwd: None,
                hooks: &["tool/pre"],
                capabilities: &["observe:tool"],
                limits: "",
            }
        }
    }

    impl Fixture {
        fn manifest(&self, name: &str) -> Manifest {
            let argv = self
                .argv
                .iter()
                .map(|a| format!("\"{a}\""))
                .collect::<Vec<_>>()
                .join(", ");
            let env_allow = self
                .env_allow
                .iter()
                .map(|n| format!("\"{n}\""))
                .collect::<Vec<_>>()
                .join(", ");
            let cwd = self
                .cwd
                .as_ref()
                .map(|p| format!("cwd = \"{}\"\n", p.display()))
                .unwrap_or_default();
            let text = format!(
                "[plugin]\nname = \"{name}\"\nversion = \"0.1.0\"\nabi = 1\n\
                 [process]\nargv = [{argv}]\nsha256 = \"{}\"\nenv_allow = [{env_allow}]\n{cwd}\
                 [grants]\nhooks = [{}]\ncapabilities = [{}]\n[limits]\n{}\n",
                self.sha256,
                self.hooks
                    .iter()
                    .map(|h| format!("\"{h}\""))
                    .collect::<Vec<_>>()
                    .join(", "),
                self.capabilities
                    .iter()
                    .map(|c| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", "),
                if self.limits.is_empty() {
                    // The closed grammar requires the `[limits]` section
                    // to carry at least one key (a fully-defaulted
                    // fixture states the defaults).
                    "deadline_ms = 500"
                } else {
                    self.limits
                },
            );
            let ctx = crate::hooks::sidecar::ValidationCtx {
                instance_key: name,
                ..Default::default()
            };
            crate::hooks::sidecar::parse_manifest(&text, &ctx).expect("fixture manifest parses")
        }

        fn effective(&self) -> Vec<EffectivePlugin> {
            let cfg: SidecarConfig = serde_json::from_value(serde_json::json!({
                "plugins": {"gate": {"manifest": "/etc/kian/plugins/gate.toml"}},
                "scopes": {"server": ["gate"]}
            }))
            .unwrap();
            let mut manifests = std::collections::BTreeMap::new();
            manifests.insert("gate".to_string(), self.manifest("gate"));
            resolve::resolve(&cfg, &manifests, None).effective
        }
    }

    #[test]
    fn digest_matches_an_independently_hashed_canonical_preimage() {
        let effective = Fixture::default().effective();
        assert_eq!(effective.len(), 1);
        // §8.5's rule computed here by hand: sha256 over the visible
        // canonical text, prefixed `sha256:`.
        let expected = format!(
            "sha256:{}",
            crate::hooks::wire::sha256_hex(single_plugin_canonical(&effective[0]).as_bytes())
        );
        assert_eq!(set_digest(&effective), expected);
    }

    #[test]
    fn digest_is_stable_across_manifest_paths_and_layout() {
        // The manifest path never enters the digest (§8.5's outside
        // list); a moved key that resolves to the same effective set
        // changes nothing.
        let a = {
            let cfg: SidecarConfig = serde_json::from_value(serde_json::json!({
                "plugins": {"gate": {"manifest": "/old/path/gate.toml"}},
                "scopes": {"server": ["gate"]}
            }))
            .unwrap();
            let mut manifests = std::collections::BTreeMap::new();
            manifests.insert("gate".to_string(), Fixture::default().manifest("gate"));
            resolve::resolve(&cfg, &manifests, None).effective
        };
        let b = Fixture::default().effective();
        assert_eq!(set_digest(&a), set_digest(&b));
        // Recomputation over the same inputs is byte-stable.
        assert_eq!(set_digest(&b), set_digest(&b));
    }

    #[test]
    fn every_behaviour_bearing_field_changes_the_digest() {
        let baseline = set_digest(&Fixture::default().effective());
        let cases: Vec<(&str, Fixture)> = vec![
            (
                "a narrowed limit",
                Fixture {
                    limits: "deadline_ms = 100",
                    ..Default::default()
                },
            ),
            (
                "a different binary digest",
                Fixture {
                    sha256: "b".repeat(64),
                    ..Default::default()
                },
            ),
            (
                "different argv",
                Fixture {
                    argv: &["/bin/cat", "-u"],
                    ..Default::default()
                },
            ),
            (
                "a different env allowlist",
                Fixture {
                    env_allow: &["APPROVAL_URL"],
                    ..Default::default()
                },
            ),
            (
                "a different cwd",
                Fixture {
                    cwd: Some(std::env::temp_dir()),
                    ..Default::default()
                },
            ),
            (
                "a changed grant set",
                Fixture {
                    hooks: &["tool/pre", "tool/post"],
                    capabilities: &["observe:tool", "veto:tool"],
                    ..Default::default()
                },
            ),
        ];
        for (what, fixture) in cases {
            assert_ne!(set_digest(&fixture.effective()), baseline, "{what}");
        }
    }

    #[test]
    fn env_allow_ordering_does_not_change_process_sha256() {
        // NAMES only, sorted — the same allowlist in a different order
        // is the same behaviour (§8.5).
        let a = Fixture {
            env_allow: &["B_URL", "A_URL"],
            ..Default::default()
        }
        .manifest("gate");
        let b = Fixture {
            env_allow: &["A_URL", "B_URL"],
            ..Default::default()
        }
        .manifest("gate");
        assert_eq!(process_sha256(&a), process_sha256(&b));
        let c = Fixture {
            env_allow: &["A_URL"],
            ..Default::default()
        }
        .manifest("gate");
        assert_ne!(process_sha256(&a), process_sha256(&c));
    }

    #[test]
    fn resolved_scope_layer_changes_the_digest() {
        // Same single member, declared by the worker layer instead of
        // the server layer: a differing scope with an equal set means
        // resolution changed — the one thing the §9.2 guard exists to
        // catch (§8.5's `scope` row).
        let mut manifests = std::collections::BTreeMap::new();
        manifests.insert("gate".to_string(), Fixture::default().manifest("gate"));
        let server_scope: SidecarConfig = serde_json::from_value(serde_json::json!({
            "plugins": {"gate": {"manifest": "/etc/gate.toml"}},
            "scopes": {"server": ["gate"]}
        }))
        .unwrap();
        let worker_scope: SidecarConfig = serde_json::from_value(serde_json::json!({
            "plugins": {"gate": {"manifest": "/etc/gate.toml"}},
            "scopes": {
                "server": ["gate"],
                "worker": {"node-zulu": {"use": ["gate"]}}
            }
        }))
        .unwrap();
        let server_set = resolve::resolve(&server_scope, &manifests, None).effective;
        let worker_set = resolve::resolve(&worker_scope, &manifests, Some("node-zulu")).effective;
        assert_eq!(server_set.len(), 1);
        assert_eq!(worker_set.len(), 1);
        assert_eq!(worker_set[0].layer, resolve::ScopeLayer::Worker);
        let mut same_layer = worker_set.clone();
        same_layer[0].layer = resolve::ScopeLayer::Server;
        assert_eq!(
            set_digest(&server_set),
            set_digest(&same_layer),
            "sanity: identical field values, both server-layer"
        );
        assert_ne!(
            set_digest(&server_set),
            set_digest(&worker_set),
            "the scope layer is a digest input"
        );
    }

    #[test]
    fn adding_or_removing_a_plugin_changes_the_digest() {
        let one = Fixture::default().effective();
        let mut manifests = std::collections::BTreeMap::new();
        manifests.insert("gate".to_string(), Fixture::default().manifest("gate"));
        manifests.insert("audit".to_string(), Fixture::default().manifest("audit"));
        let cfg: SidecarConfig = serde_json::from_value(serde_json::json!({
            "plugins": {
                "gate": {"manifest": "/etc/gate.toml"},
                "audit": {"manifest": "/etc/audit.toml"}
            },
            "scopes": {"server": ["gate", "audit"]}
        }))
        .unwrap();
        let two = resolve::resolve(&cfg, &manifests, None).effective;
        assert_eq!(two.len(), 2);
        assert_ne!(set_digest(&one), set_digest(&two));
    }
}
