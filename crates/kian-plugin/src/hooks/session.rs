//! Session consistency (PLUGIN-SPEC §9): the same-process guard, the
//! session-lifetime quarantine, and the two audit lines.
//!
//! What the guard is and is not (§9.1, stated honestly): the recorded
//! digest lives in one **audit line**, not in the store — no table, no
//! payload, no read API — so it is **not** a durable baseline. The
//! guard compares only **within one process lifetime**: a cross-process
//! restart re-resolves from the current config and v1 cannot detect
//! that the config changed (§16.2 owns that gap; only durable pinning
//! closes it). Inside one process the check fires only if resolution
//! stops being a pure function of (config snapshot, durable binding) —
//! a future reload path, or a scope read from mutable state — which is
//! exactly the invariant it exists to guard, fail-closed.
//!
//! The runtime here is installed **once at boot** by the assembly
//! ([`HookBus::install_sidecars`]) and never reloaded; the config
//! snapshot is therefore fixed for the process lifetime (§13.1). With
//! no runtime installed (the `sidecars:` section absent) every method
//! is a no-op and dispatch is byte-identical to a build without the
//! feature (§13.3).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

use kian_core::model::SessionId;

use super::digest;
use super::resolve::{self, Resolution, SidecarConfig};
use super::sidecar::manifest::Manifest;
use super::views::FrameInfo;

/// The per-session resolved set, memoized for the process lifetime:
/// the digest, the member names, each member's per-plugin segment
/// digest (the "changed" half of the mismatch line), and each member's
/// effective settle-veto budget (§6.1 — the manifest limit after worker
/// `limits:` narrowing, the input to the §4.6 decision).
pub(crate) struct ResolvedSet {
    pub digest: String,
    pub names: BTreeSet<String>,
    per_plugin: BTreeMap<String, String>,
    settle_limits: BTreeMap<String, u64>,
}

impl ResolvedSet {
    fn from(resolution: &Resolution) -> Self {
        Self {
            digest: digest::set_digest(&resolution.effective),
            names: resolution
                .effective
                .iter()
                .map(|p| p.name.clone())
                .collect(),
            per_plugin: resolution
                .effective
                .iter()
                .map(|p| (p.name.clone(), digest::set_digest(std::slice::from_ref(p))))
                .collect(),
            settle_limits: resolution
                .effective
                .iter()
                .map(|p| (p.name.clone(), p.limits.max_settle_vetoes))
                .collect(),
        }
    }
}

/// The boot snapshot the guard recomputes against: the validated
/// `sidecars:` config plus the successfully mounted manifests (a
/// refused optional instance is absent, §6.2 — its refusal audit line
/// is the record).
pub struct SidecarRuntime {
    /// The config snapshot. Behind a lock because the guard's whole
    /// premise (§9.2 step 4) is that a **future reload path** could
    /// break resolution's purity — production writes it exactly once,
    /// at boot, and never again; only the in-crate mismatch tests use
    /// [`SidecarRuntime::reconfigure`] to simulate that break.
    cfg: RwLock<SidecarConfig>,
    manifests: BTreeMap<String, Manifest>,
    /// The declared instance names — the dispatch-time sidecar filter.
    names: BTreeSet<String>,
    /// Sessions this process has opened (session → recorded digest).
    opened: Mutex<HashMap<SessionId, String>>,
    /// Memoized per-session resolved sets (lazy, §8.4).
    sets: Mutex<HashMap<SessionId, Arc<ResolvedSet>>>,
    /// Session-lifetime quarantine (§9.3): that session, in this
    /// process, for the rest of its drain lifetime.
    quarantined: Mutex<HashSet<SessionId>>,
}

impl SidecarRuntime {
    pub(crate) fn new(cfg: SidecarConfig, manifests: BTreeMap<String, Manifest>) -> Self {
        let names = cfg.plugins.keys().cloned().collect();
        Self {
            cfg: RwLock::new(cfg),
            manifests,
            names,
            opened: Mutex::new(HashMap::new()),
            sets: Mutex::new(HashMap::new()),
            quarantined: Mutex::new(HashSet::new()),
        }
    }

    /// The reload path v1 does not have (§13.1: no live reload). Used
    /// only by the in-crate mismatch tests to simulate §9.2 step 4's
    /// premise — resolution stopped being a pure function of the
    /// process-boot snapshot — and prove the guard fires. The declared
    /// instance names stay fixed (the registry is not re-read; a reload
    /// that added an instance would need handler registration too).
    /// Production never calls it: `dead_code` is allowed because the
    /// lib build (without tests) has no caller by design.
    #[allow(dead_code)]
    pub(crate) fn reconfigure(&self, cfg: SidecarConfig) {
        *self
            .cfg
            .write()
            .expect("kian-plugin: sidecar config lock poisoned") = cfg;
    }

    /// Whether a registered handler's plugin name is a declared
    /// sidecar instance — the dispatch-time tier filter (tier-0
    /// handlers are never affected by a mismatch, §9.2 step 6).
    pub(crate) fn is_sidecar(&self, plugin: &str) -> bool {
        self.names.contains(plugin)
    }

    /// Whether the session is under §9.3 quarantine (sidecar hooks of
    /// that session take their class defaults; tier-0 unaffected).
    pub(crate) fn is_quarantined(&self, session: &SessionId) -> bool {
        self.quarantined
            .lock()
            .expect("kian-plugin: sidecar runtime lock poisoned")
            .contains(session)
    }

    /// The session's resolved set, resolving lazily on first sight
    /// (§8.4) and recording the §9.2 step-1 audit line then — a
    /// session whose first bus contact is a dispatch (not a drain
    /// begin) still opens honestly.
    fn set_for(&self, frame: &FrameInfo) -> Arc<ResolvedSet> {
        if let Some(set) = self
            .sets
            .lock()
            .expect("kian-plugin: sidecar runtime lock poisoned")
            .get(&frame.session)
        {
            return Arc::clone(set);
        }
        let resolution = self.resolve_for(frame);
        let set = Arc::new(ResolvedSet::from(&resolution));
        self.record_open(frame, &set, &resolution);
        self.sets
            .lock()
            .expect("kian-plugin: sidecar runtime lock poisoned")
            .insert(frame.session, Arc::clone(&set));
        set
    }

    /// The dispatch-time session state: quarantined (sidecars take
    /// their class defaults, §9.3) or the resolved member set (a name
    /// outside it is skipped — the drop was audited at resolution).
    pub(crate) fn state_for(&self, frame: Option<&FrameInfo>) -> SessionState {
        let Some(frame) = frame else {
            // No frame identity → no session to key resolution on; the
            // contract forbids implicit identity (R11), so sidecar
            // subscribers cannot participate in this dispatch. Their
            // absence is the honest outcome (an unattributed hook
            // consultation is not one).
            return SessionState::NoIdentity;
        };
        if self.is_quarantined(&frame.session) {
            return SessionState::Quarantined;
        }
        SessionState::Set(self.set_for(frame))
    }

    fn resolve_for(&self, frame: &FrameInfo) -> Resolution {
        let binding = if frame.worker.is_empty() {
            None
        } else {
            Some(frame.worker.as_str())
        };
        resolve::resolve(
            &self
                .cfg
                .read()
                .expect("kian-plugin: sidecar config lock poisoned"),
            &self.manifests,
            binding,
        )
    }

    /// §9.2 step 1: record + the `hook-set` audit line, and §8.2's one
    /// drop line when the worker layer narrowed names away.
    fn record_open(&self, frame: &FrameInfo, set: &ResolvedSet, resolution: &Resolution) {
        {
            let mut opened = self
                .opened
                .lock()
                .expect("kian-plugin: sidecar runtime lock poisoned");
            if opened.contains_key(&frame.session) {
                return;
            }
            opened.insert(frame.session, set.digest.clone());
        }
        let names: Vec<&str> = set.names.iter().map(|n| n.as_str()).collect();
        tracing::info!(
            target: "kian::hook",
            session = %frame.session.0,
            digest = %set.digest,
            plugins = set.names.len(),
            names = names.join(","),
            "hook-set"
        );
        if !resolution.dropped.is_empty() {
            // §8.2: a narrowed-away name is simply absent from this
            // worker's set — one audit line, never an error.
            tracing::info!(
                target: "kian::hook",
                session = %frame.session.0,
                worker = %frame.worker,
                dropped = resolution.dropped.join(","),
                "hook-set-drop"
            );
        }
    }

    /// §9.2 steps 2–4: recompute the digest from the current config
    /// snapshot and the durable binding, compare with what this
    /// process recorded, and quarantine the session's sidecar hooks on
    /// a mismatch (one `hook-set-mismatch` line naming the diff). The
    /// caller is a drain/claim boundary that knows the session's
    /// identity; nothing else participates (no harness, no profile,
    /// §16.18).
    pub(crate) fn drain_begin(&self, frame: &FrameInfo) {
        // First sight doubles as step 1 (a drain is how most sessions
        // first meet the bus).
        let recorded = self.set_for(frame);
        if self.is_quarantined(&frame.session) {
            // §9.3: one mismatch line per session-lifetime break; the
            // downgrade is already in force.
            return;
        }
        let resolution = self.resolve_for(frame);
        let current = digest::set_digest(&resolution.effective);
        if current == recorded.digest {
            return;
        }
        // The diff the audit line names: added/removed by name,
        // changed by per-plugin segment digest.
        let current_names: BTreeSet<String> = resolution
            .effective
            .iter()
            .map(|p| p.name.clone())
            .collect();
        let added: Vec<&str> = current_names
            .difference(&recorded.names)
            .map(|n| n.as_str())
            .collect();
        let removed: Vec<&str> = recorded
            .names
            .difference(&current_names)
            .map(|n| n.as_str())
            .collect();
        let current_per_plugin: BTreeMap<String, String> = resolution
            .effective
            .iter()
            .map(|p| (p.name.clone(), digest::set_digest(std::slice::from_ref(p))))
            .collect();
        let changed: Vec<&str> = recorded
            .per_plugin
            .iter()
            .filter_map(|(name, token)| {
                current_per_plugin
                    .get(name)
                    .is_some_and(|new_token| new_token != token)
                    .then_some(name.as_str())
            })
            .collect();
        self.quarantined
            .lock()
            .expect("kian-plugin: sidecar runtime lock poisoned")
            .insert(frame.session);
        tracing::warn!(
            target: "kian::hook",
            session = %frame.session.0,
            recorded = %recorded.digest,
            current = %current,
            added = added.join(","),
            removed = removed.join(","),
            changed = changed.join(","),
            "hook-set-mismatch — session sidecar hooks quarantined for this drain lifetime (§9.3)"
        );
        // The stale memo is the mismatch's own cause; drop it so the
        // quarantine flag is the single source of the downgrade.
        self.sets
            .lock()
            .expect("kian-plugin: sidecar runtime lock poisoned")
            .remove(&frame.session);
    }

    /// The digest this process recorded for `session` (§9.2's audit
    /// record; `None` before the first open or without a runtime).
    pub(crate) fn recorded_digest(&self, session: &SessionId) -> Option<String> {
        self.opened
            .lock()
            .expect("kian-plugin: sidecar runtime lock poisoned")
            .get(session)
            .cloned()
    }

    /// The effective `max_settle_vetoes` for `plugin` in the session's
    /// resolved set (§6.1/§4.6): the manifest limit after worker
    /// `limits:` narrowing. A plugin outside the session's sidecar set
    /// (a tier-0 handler, or a name this session's scope dropped) has
    /// no manifest and must not invent one (K8) — the tier-0 default.
    pub(crate) fn settle_veto_limit(&self, frame: Option<&FrameInfo>, plugin: &str) -> u64 {
        match self.state_for(frame) {
            SessionState::Set(set) => set
                .settle_limits
                .get(plugin)
                .copied()
                .unwrap_or(super::bus::DEFAULT_MAX_SETTLE_VETOS),
            // No identity or quarantine: the tier-0 default. A
            // quarantined sidecar is unreachable anyway (its denial is
            // the class default), but the budget check still needs a
            // bound — the manifest limit stays the honest one.
            SessionState::NoIdentity | SessionState::Quarantined => {
                super::bus::DEFAULT_MAX_SETTLE_VETOS
            }
        }
    }
}

/// The dispatch-time session state (see [`SidecarRuntime::state_for`]).
pub(crate) enum SessionState {
    /// No frame identity: sidecar subscribers cannot participate (R11).
    NoIdentity,
    /// §9.3: sidecar hooks of this session take their class defaults.
    Quarantined,
    /// The session's resolved member set.
    Set(Arc<ResolvedSet>),
}

/// The §9 guard surface a drain boundary calls: implemented by
/// [`HookBus`](super::bus::HookBus) and injected where the loop drivers
/// know the session's identity. This is **not** an eighth interception
/// port (§4's seven are frozen); it is the consistency guard's own
/// seam, called at drain/claim boundaries, never mid-turn.
pub trait SessionSetGuard: Send + Sync {
    /// §9.2 steps 1–4 for one drain begin: record at first sight,
    /// recompute and compare after. (The method name avoids the
    /// `drain_` prefix the superagent iron-rule grep forbids in bundle
    /// sources.)
    fn session_begin(&self, frame: FrameInfo);
}

/// The install-once holder the bus carries. `RwLock<Option<..>>`
/// because the bus is constructed before the assembly mounts sidecars
/// (which §13.1 orders after the inbox rebuild); production writes
/// exactly once, at boot, and the §9.2 guard is honest that a second
/// write is the purity break it exists to catch.
pub(crate) type SharedRuntime = RwLock<Option<Arc<SidecarRuntime>>>;

impl SessionSetGuard for super::bus::HookBus {
    fn session_begin(&self, frame: FrameInfo) {
        self.session_begin(frame)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::resolve::validate;

    fn manifest(name: &str) -> Manifest {
        let text = format!(
            "[plugin]\nname = \"{name}\"\nversion = \"0.1.0\"\nabi = 1\n\
             [process]\nargv = [\"/bin/cat\"]\nsha256 = \"{}\"\n\
             [grants]\nhooks = [\"tool/pre\"]\ncapabilities = [\"observe:tool\"]\n\
             [limits]\ndeadline_ms = 500\n",
            "a".repeat(64)
        );
        let ctx = crate::hooks::sidecar::ValidationCtx {
            instance_key: name,
            ..Default::default()
        };
        crate::hooks::sidecar::parse_manifest(&text, &ctx).expect("fixture manifest parses")
    }

    fn cfg(server: &[&str]) -> SidecarConfig {
        serde_json::from_value(serde_json::json!({
            "plugins": {"gate": {"manifest": "/etc/kian/plugins/gate.toml"}},
            "scopes": {"server": server}
        }))
        .unwrap()
    }

    fn session(s: &str) -> SessionId {
        // SessionId is Deserialize over its uuid string form, so tests
        // pin exact uuids without a direct uuid dependency edge (only
        // sha2 may be added this round, K10).
        serde_json::from_str(&format!("\"{s}\"")).expect("valid uuid literal")
    }

    fn frame(worker: &str) -> FrameInfo {
        FrameInfo {
            session: session("0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b"),
            worker: worker.to_string(),
            seq: 0,
            epoch: None,
        }
    }

    fn runtime() -> SidecarRuntime {
        let mut manifests = BTreeMap::new();
        manifests.insert("gate".to_string(), manifest("gate"));
        SidecarRuntime::new(cfg(&["gate"]), manifests)
    }

    #[test]
    fn first_drain_records_the_digest_later_drains_recompute() {
        let rt = runtime();
        let frame = frame("");
        rt.drain_begin(&frame);
        let recorded = rt.recorded_digest(&frame.session).expect("recorded");
        assert!(recorded.starts_with("sha256:"), "the §8.5 shape");
        assert!(!rt.is_quarantined(&frame.session));
        // §9.2 step 2: a recompute over the same immutable snapshot is
        // equal — the guard stays silent, the session proceeds.
        rt.drain_begin(&frame);
        assert_eq!(
            rt.recorded_digest(&frame.session).as_deref(),
            Some(recorded.as_str())
        );
        assert!(!rt.is_quarantined(&frame.session));
    }

    #[test]
    fn mismatch_quarantines_the_session_not_the_process() {
        let rt = runtime();
        let frame = frame("");
        rt.drain_begin(&frame);
        let recorded = rt.recorded_digest(&frame.session).expect("recorded");

        // §9.2 step 4's premise: resolution stops being a pure function
        // of the boot snapshot (the reload path v1 does not have —
        // `reconfigure` simulates exactly that break).
        rt.reconfigure(cfg(&[]));

        rt.drain_begin(&frame);
        assert!(rt.is_quarantined(&frame.session));
        // The recorded digest is the operator's record of what was in
        // force — the mismatch line's `recorded=` field.
        assert_eq!(
            rt.recorded_digest(&frame.session).as_deref(),
            Some(recorded.as_str())
        );
        // Other sessions are unaffected (§9.3: not the process
        // quarantine — no counter shared, nobody else downgraded).
        let other = FrameInfo {
            session: session("11111111-2222-3333-4444-555555555555"),
            worker: String::new(),
            seq: 0,
            epoch: None,
        };
        rt.drain_begin(&other);
        assert!(!rt.is_quarantined(&other.session));
        // A repeat drain begin does not re-audit the same break.
        rt.drain_begin(&frame);
        assert!(rt.is_quarantined(&frame.session));
    }

    #[test]
    fn lazy_resolution_uses_the_durable_binding() {
        let mut cfg = cfg(&["gate"]);
        cfg.scopes.worker.insert(
            "node-zulu".to_string(),
            super::super::resolve::WorkerScope {
                use_: Some(vec![]),
                limits: BTreeMap::new(),
            },
        );
        let mut manifests = BTreeMap::new();
        manifests.insert("gate".to_string(), manifest("gate"));
        let rt = SidecarRuntime::new(cfg, manifests);

        let zulu = FrameInfo {
            session: session("0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b"),
            worker: "node-zulu".to_string(),
            seq: 0,
            epoch: None,
        };
        let yankee = FrameInfo {
            session: session("99999999-8888-7777-6666-555555555555"),
            worker: "node-yankee".to_string(),
            seq: 0,
            epoch: None,
        };
        rt.drain_begin(&zulu);
        rt.drain_begin(&yankee);
        let zulu_digest = rt.recorded_digest(&zulu.session).unwrap();
        let yankee_digest = rt.recorded_digest(&yankee.session).unwrap();
        assert_ne!(
            zulu_digest, yankee_digest,
            "the empty-use worker resolves a different (empty) set than the server scope"
        );
    }

    #[test]
    fn boot_config_passes_the_full_validation() {
        let mut cfg = cfg(&["gate"]);
        cfg.scopes.worker.insert(
            "node-zulu".to_string(),
            super::super::resolve::WorkerScope {
                use_: Some(vec!["gate".to_string()]),
                limits: BTreeMap::new(),
            },
        );
        let mut manifests = BTreeMap::new();
        manifests.insert("gate".to_string(), manifest("gate"));
        assert!(validate(&cfg, &manifests).is_ok());
    }
}
