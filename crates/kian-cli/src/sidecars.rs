//! The serve assembly's sidecar mount (PLUGIN-SPEC §13.1, P3b).
//!
//! Ordering is normative: the caller invokes [`mount`] **after the
//! inbox rebuild** and never inside a plugin's synchronous `apply` —
//! a sidecar starts once the server's durable state is whole, exactly
//! like the other post-rebuild services. What happens here, in order:
//!
//! 1. every `sidecars.plugins` manifest is read and validated per
//!    instance (§6.2, fail-closed, no repair, **no whole-section
//!    refusal** — one broken instance never unmounts the others; a
//!    refusal is fatal iff that instance declared `required = true`);
//! 2. the full boot validation runs (`use ⊆ scopes.server` was already
//!    checked at config load; the widening check needs the manifests
//!    and runs here — §8.2 invariant 2);
//! 3. each mounted instance's runner spawns (§10.3) and its tier-0
//!    shaped handlers register into the bus's registry (§5.3);
//! 4. the §8/§9 runtime installs on the bus — resolution, the set
//!    digest and the session-consistency guard.
//!
//! With no `sidecars:` section [`mount`] returns `None` and nothing
//! above runs: no bus runtime, no spawns, byte-identical behavior
//! (§13.3).

use std::collections::BTreeMap;
use std::sync::Arc;

use kian_plugin::ctx::Disposer;
use kian_plugin::hooks::HookBus;
use kian_plugin::hooks::sidecar::manifest::{Manifest, Required};
use kian_plugin::hooks::sidecar::{SidecarRunner, ValidationCtx, parse_manifest};

use crate::config::{KNOWN_PLUGINS, ServeConfig};
use crate::server::ServeError;

/// The mounted sidecars, held by the [`Server`](crate::server::Server)
/// for its lifetime: dropping it stops every runner (§7.5's `bye`,
/// bounded grace, then the group kill) and unregisters the handlers
/// (disposer order is registration-reverse, §5.3).
pub struct SidecarMount {
    runners: Vec<Arc<SidecarRunner>>,
    _disposers: Vec<Disposer>,
}

impl Drop for SidecarMount {
    fn drop(&mut self) {
        for runner in &self.runners {
            runner.stop();
        }
    }
}

/// The `required` field of a refusal, as the audit line renders it.
fn required_str(required: Required) -> &'static str {
    match required {
        Required::True => "true",
        Required::False => "false",
        Required::Unknown => "unknown",
    }
}

/// One instance's refusal decided: fatal iff it declared itself
/// required (§6.2). `Required::Unknown` — the manifest could not be
/// read far enough to know — is fatal: fail-closed boot.
fn refuse(key: &str, code: &str, message: &str, required: Required) -> Result<(), ServeError> {
    tracing::warn!(
        target: "kian::hook",
        plugin = key,
        code = code,
        required = required_str(required),
        message = %message,
        "hook-refuse"
    );
    match required {
        Required::False => Ok(()),
        _ => Err(ServeError::Assembly(format!(
            "sidecar plugin `{key}` refused at boot: {message} (code: {code}, \
             required: {}) — the operator asked for a gate and the gate cannot be there",
            required_str(required)
        ))),
    }
}

/// Mounts the configured sidecars onto `bus`. `secret_env` is the
/// assembly's collected secret environment-variable names (rule 7:
/// `env_allow` may name none of them).
pub fn mount(
    config: &ServeConfig,
    bus: &HookBus,
    secret_env: &[String],
) -> Result<Option<SidecarMount>, ServeError> {
    let Some(cfg) = config.sidecars.clone() else {
        return Ok(None);
    };
    let keys: Vec<String> = cfg.plugins.keys().cloned().collect();

    // -- 1. per-instance manifest validation (§6.2) -------------------
    let mut manifests: BTreeMap<String, Manifest> = BTreeMap::new();
    for key in &keys {
        let decl = &cfg.plugins[key];
        let text = std::fs::read_to_string(&decl.manifest).map_err(|e| {
            // An unreadable manifest cannot declare `required = false`
            // either — the boundary case §6.2 states for unparseable
            // files. Fail-closed, naming the offender.
            ServeError::Assembly(format!(
                "sidecars.plugins `{key}`: manifest `{}` cannot be read: {e} \
                 (required: unknown — fail-closed)",
                decl.manifest.display()
            ))
        })?;
        // Rule 3's reserved names: the tier-0 plugin names (the
        // KNOWN_PLUGINS set — §13.1 keeps `plugins:` untouched) and the
        // sibling instance keys (checked pairwise here; each manifest's
        // own `[plugin].name` equality is rule 2 inside the parser).
        let reserved: Vec<String> = KNOWN_PLUGINS
            .iter()
            .map(|s| s.to_string())
            .chain(keys.iter().filter(|k| *k != key).cloned())
            .collect();
        let ctx = ValidationCtx {
            instance_key: key,
            reserved_names: &reserved,
            secret_env,
        };
        match parse_manifest(&text, &ctx) {
            Ok(manifest) => {
                manifests.insert(key.clone(), manifest);
            }
            Err(refusal) => {
                refuse(
                    key,
                    refusal.code.as_str(),
                    &refusal.message,
                    refusal.required,
                )?;
            }
        }
    }

    // -- 2. the full boot validation (§8.2 invariant 2 needs the
    //       manifests; the name-only checks already ran at config
    //       load and pass trivially here) ------------------------------
    if let Err(errors) = kian_plugin::hooks::resolve::validate(&cfg, &manifests) {
        let rendered: Vec<String> = errors
            .iter()
            .map(|e| format!("{} (code: {})", e.message, e.code.as_str()))
            .collect();
        return Err(ServeError::Assembly(format!(
            "sidecars: {}",
            rendered.join("; ")
        )));
    }

    // -- 3. spawn the runners (§10.3) ---------------------------------
    let mut runners: Vec<Arc<SidecarRunner>> = Vec::new();
    for (key, manifest) in &manifests {
        // The handshake identity is the canonical absolute path (§7.3);
        // the file exists (it was just read), so canonicalize cannot
        // fail for a missing file — a failure here is an honest
        // assembly error, never a silent fallback path.
        let canonical = cfg.plugins[key].manifest.canonicalize().map_err(|e| {
            ServeError::Assembly(format!(
                "sidecars.plugins `{key}`: manifest path `{}` cannot be canonicalized: {e}",
                cfg.plugins[key].manifest.display()
            ))
        })?;
        match SidecarRunner::start(manifest.clone(), &canonical) {
            Ok(runner) => runners.push(Arc::new(runner)),
            Err(refusal) => {
                refuse(
                    key,
                    refusal.code.as_str(),
                    &refusal.message,
                    refusal.required,
                )?;
            }
        }
    }

    // -- 4. register + install (§5.3, §8, §9) --------------------------
    let mut mounted: BTreeMap<String, Manifest> = BTreeMap::new();
    let mut disposers: Vec<Disposer> = Vec::new();
    for runner in &runners {
        mounted.insert(runner.name().to_string(), manifests[runner.name()].clone());
        for (handler, f) in runner.hook_fns() {
            disposers.push(bus.registry().register(handler, f));
        }
    }
    bus.install_sidecars(cfg, mounted)
        .map_err(ServeError::Assembly)?;

    tracing::info!(
        target: "kian::hook",
        plugins = runners.len(),
        "sidecars mounted (after the inbox rebuild, §13.1)"
    );
    Ok(Some(SidecarMount {
        runners,
        _disposers: disposers,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_yaml() -> String {
        r#"listen: "127.0.0.1:7331"
storage:
  sqlite: "/tmp/kian-sidecar-mount-test.sqlite"
agent:
  id: default
  model_slots:
    main:
      provider: openai
      model: gpt-test
      fallback:
        - provider: openai
          model: gpt-fallback
  tools: ["fs_read", "fs_write", "exec"]
  budgets:
    tokens: 100000
providers:
  openai:
    base_url: "http://127.0.0.1:9/v1"
    api_key_env: KIAN_API_KEY
drain:
  max_iterations: 8
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: rate_limited
      action: {backoff_retry: {delay_ms: 500}}
    - class: unknown
      action: fail
"#
        .into()
    }

    fn manifest_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("kian-sidecar-{name}.toml"))
    }

    fn write_manifest(name: &str, required: bool, deadline_ms: u64) -> std::path::PathBuf {
        let path = manifest_path(name);
        std::fs::write(
            &path,
            format!(
                "[plugin]\nname = \"{name}\"\nversion = \"0.1.0\"\nabi = 1\nrequired = {required}\n\
                 [process]\nargv = [\"/bin/cat\"]\nsha256 = \"{}\"\n\
                 [grants]\nhooks = [\"tool/pre\"]\ncapabilities = [\"observe:tool\"]\n\
                 [limits]\ndeadline_ms = {deadline_ms}\n",
                "a".repeat(64)
            ),
        )
        .expect("write fixture manifest");
        path
    }

    fn config_with(section: &str) -> ServeConfig {
        ServeConfig::parse(&format!("{}{}", base_yaml(), section)).expect("config parses")
    }

    /// §13.3: no `sidecars:` section → `None`, nothing mounted.
    #[test]
    fn absent_section_mounts_nothing() {
        let config = config_with("");
        let bus = HookBus::new();
        assert!(mount(&config, &bus, &[]).unwrap().is_none());
    }

    /// §6.2: a per-instance refusal is fatal iff that instance declared
    /// `required = true`; an optional refused instance is absent with
    /// one audit line and the runtime still installs (the sha256 will
    /// never match /bin/cat, so the runner refuses at spawn).
    #[test]
    fn optional_refused_instance_is_absent_not_fatal() {
        let path = write_manifest("gate-a", false, 500);
        let config = config_with(&format!(
            "sidecars:\n  plugins:\n    gate-a:\n      manifest: \"{}\"\n  scopes:\n    server: [\"gate-a\"]\n",
            path.display()
        ));
        let bus = HookBus::new();
        let mounted = mount(&config, &bus, &[]).expect("optional refusal is not fatal");
        assert!(mounted.is_some(), "the mount itself happened");
        // The runtime is installed (empty set — the instance is absent)
        // and the §9.2 guard records the empty digest honestly.
        let session: kian_core::model::SessionId =
            serde_json::from_str("\"0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b\"").unwrap();
        bus.session_begin(kian_plugin::hooks::FrameInfo::new(session));
        assert!(
            bus.session_set_digest(&session)
                .expect("the guard recorded")
                .starts_with("sha256:")
        );
    }

    /// §6.2: `required = true` (the default posture) makes the same
    /// refusal fatal, naming the offender and the code.
    #[test]
    fn required_refusal_is_fatal_naming_offender_and_code() {
        let path = write_manifest("gate-b", true, 500);
        let config = config_with(&format!(
            "sidecars:\n  plugins:\n    gate-b:\n      manifest: \"{}\"\n  scopes:\n    server: [\"gate-b\"]\n",
            path.display()
        ));
        let bus = HookBus::new();
        let err = match mount(&config, &bus, &[]) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("required refusal must be fatal"),
        };
        assert!(err.contains("gate-b"), "names the offender: {err}");
        assert!(err.contains("digest_mismatch"), "names the code: {err}");
    }

    /// §6.2's unreadable-manifest boundary case: a manifest that cannot
    /// be read cannot declare `required = false` — fatal, naming it.
    #[test]
    fn unreadable_manifest_is_fatal() {
        let config = config_with(
            "sidecars:\n  plugins:\n    ghost:\n      manifest: \"/nonexistent/ghost.toml\"\n  scopes:\n    server: [\"ghost\"]\n",
        );
        let bus = HookBus::new();
        let err = match mount(&config, &bus, &[]) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("an unreadable manifest must be fatal"),
        };
        assert!(
            err.contains("ghost") && err.contains("cannot be read"),
            "{err}"
        );
    }

    /// §8.2 invariant 2 at the assembly: a worker limit that widens the
    /// manifest bound refuses boot naming the worker, instance, key and
    /// both values (the manifests are loaded here — config validation
    /// cannot see them).
    #[test]
    fn widening_refuses_boot_at_the_assembly() {
        let path = write_manifest("gate-c", false, 200);
        let config = config_with(&format!(
            "sidecars:\n  plugins:\n    gate-c:\n      manifest: \"{}\"\n  scopes:\n    server: [\"gate-c\"]\n    worker:\n      node-zulu:\n        limits:\n          gate-c: {{deadline_ms: 500}}\n",
            path.display()
        ));
        let bus = HookBus::new();
        let err = match mount(&config, &bus, &[]) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("a widening limit must refuse boot"),
        };
        assert!(
            err.contains("node-zulu")
                && err.contains("gate-c")
                && err.contains("deadline_ms")
                && err.contains("500")
                && err.contains("200")
                && err.contains("limit_widening"),
            "{err}"
        );
    }
}
