//! The closed hook vocabulary (PLUGIN-SPEC §3.1, §3.6).
//!
//! `HookId` is an enum over §3.1's closed catalog plus a `Harness`
//! arm whose registry is **empty** in v1 (§3.6 / ADR-028 D27): a
//! `harness/<name>/<point>` id *parses* but is a registry miss, so a
//! manifest subscribing one is refused `unknown_hook` at boot (validation
//! rule 8) and the bus never dispatches it.
//!
//! `HookMode` mirrors the tier-0 dispatch mode that also fixes the legal
//! sidecar reply (§3.3); `HookClass` groups hooks for grants (§5.1) and is
//! the single source of the §7.2 epoch rule through
//! [`HookClass::is_drain_phase`].

use std::borrow::Cow;
use std::fmt;

/// Which payload of the §3.2 catalog a hook dispatches.
///
/// Kept as a column of the [`CATALOG`] table so "mode / class / payload
/// agree" is checkable in one place (PLUGIN-SPEC §18: a hook id without
/// all six registrations is not part of the contract).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    AdmitPre,
    AdmitPost,
    TurnRequest,
    TurnResponse,
    ToolPre,
    ToolPost,
    RecoverDecided,
    SettlePre,
    /// `plugin/*` lifecycle payloads (§3.2.9) — owned by the sidecar
    /// machinery (P3); the tier-0 bus never dispatches them in P1.
    PluginLifecycle,
    /// `harness/<name>/<point>` payloads — declared by the owning harness
    /// together with its registry entry (§3.6); the v1 registry is empty.
    Harness,
}

/// The tier-0 dispatch mode of a hook, which also fixes the legal reply
/// (§3.3). Tier-0 handlers are ordinary async functions; the mode is what
/// the bus enforces when it classifies a reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookMode {
    /// Observe only; legal reply `ack` (§3.3 `emit`).
    Emit,
    /// `admit/pre` only; legal replies `flow:"next"` / `flow:"stop"` (§3.3).
    Flow,
    /// `tool/pre`, `settle/pre`; legal replies `bail:null` / `bail:{deny}`
    /// (§3.3).
    Bail,
}

/// The grant class a hook belongs to (§5.1). A `veto:*` grant is keyed by
/// this class, and the §7.2 epoch rule hangs off
/// [`HookClass::is_drain_phase`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookClass {
    Admit,
    Turn,
    Tool,
    Recover,
    Settle,
    /// The `harness/<name>/*` namespace (§3.6); empty registry in v1.
    Harness,
    /// Bus lifecycle (`plugin/load|refuse|quarantine`, §3.2.9); tier-0
    /// only, never sidecar-subscribable.
    Plugin,
}

impl HookClass {
    /// The §7.2 epoch rule's single source: frames for drain-phase hooks
    /// (`turn/*`, `tool/*`, `recover/decided`, `settle/pre`) carry the
    /// fencing epoch; `admit/*` and `plugin/*` run before a drain exists
    /// (or are bus lifecycle) and their frames omit the key entirely.
    ///
    /// Harness points are declared by their owning harness together with
    /// the registry entry; the v1 registry is empty, so no dispatch can
    /// ever consult this arm.
    pub fn is_drain_phase(self) -> bool {
        match self {
            HookClass::Turn | HookClass::Tool | HookClass::Recover | HookClass::Settle => true,
            HookClass::Admit | HookClass::Plugin | HookClass::Harness => false,
        }
    }
}

/// One declared harness point: `harness/<harness>/<point>`, both segments
/// `^[a-z0-9][a-z0-9_-]*$` (§3.6). No wildcard subscriptions exist.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HarnessPoint {
    pub harness: String,
    pub point: String,
}

impl HarnessPoint {
    /// Parses `harness/<h>/<p>` with both segments validated against
    /// `^[a-z0-9][a-z0-9_-]*$`. A parse success is *not* a registry hit —
    /// the v1 registry is empty (§3.6).
    pub fn parse(id: &str) -> Option<Self> {
        let rest = id.strip_prefix("harness/")?;
        let (harness, point) = rest.split_once('/')?;
        // Exactly two segments: a second '/' in the point is not a point.
        if point.contains('/') {
            return None;
        }
        if !valid_segment(harness) || !valid_segment(point) {
            return None;
        }
        Some(Self {
            harness: harness.to_string(),
            point: point.to_string(),
        })
    }

    /// The wire spelling `harness/<harness>/<point>`.
    pub fn as_str(&self) -> String {
        format!("harness/{}/{}", self.harness, self.point)
    }
}

fn valid_segment(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

/// The closed hook id catalog (§3.1).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HookId {
    AdmitPre,
    AdmitPost,
    TurnRequest,
    TurnResponse,
    ToolPre,
    ToolPost,
    RecoverDecided,
    SettlePre,
    PluginLoad,
    PluginRefuse,
    PluginQuarantine,
    /// `harness/<name>/<point>` (§3.6); parseable, registry-empty in v1.
    Harness(HarnessPoint),
}

impl HookId {
    /// Canonical wire spelling (§3.1 / §3.6). Borrowed for the closed
    /// catalog, owned for the harness namespace.
    pub fn as_str(&self) -> Cow<'_, str> {
        match self {
            HookId::AdmitPre => Cow::Borrowed("admit/pre"),
            HookId::AdmitPost => Cow::Borrowed("admit/post"),
            HookId::TurnRequest => Cow::Borrowed("turn/request"),
            HookId::TurnResponse => Cow::Borrowed("turn/response"),
            HookId::ToolPre => Cow::Borrowed("tool/pre"),
            HookId::ToolPost => Cow::Borrowed("tool/post"),
            HookId::RecoverDecided => Cow::Borrowed("recover/decided"),
            HookId::SettlePre => Cow::Borrowed("settle/pre"),
            HookId::PluginLoad => Cow::Borrowed("plugin/load"),
            HookId::PluginRefuse => Cow::Borrowed("plugin/refuse"),
            HookId::PluginQuarantine => Cow::Borrowed("plugin/quarantine"),
            HookId::Harness(point) => Cow::Owned(point.as_str()),
        }
    }

    /// Parses a hook id. `harness/…` ids parse (segments validated) but are
    /// registry misses — see [`HARNESS_REGISTRY`].
    pub fn parse(id: &str) -> Option<Self> {
        match id {
            "admit/pre" => Some(HookId::AdmitPre),
            "admit/post" => Some(HookId::AdmitPost),
            "turn/request" => Some(HookId::TurnRequest),
            "turn/response" => Some(HookId::TurnResponse),
            "tool/pre" => Some(HookId::ToolPre),
            "tool/post" => Some(HookId::ToolPost),
            "recover/decided" => Some(HookId::RecoverDecided),
            "settle/pre" => Some(HookId::SettlePre),
            "plugin/load" => Some(HookId::PluginLoad),
            "plugin/refuse" => Some(HookId::PluginRefuse),
            "plugin/quarantine" => Some(HookId::PluginQuarantine),
            _ => HarnessPoint::parse(id).map(HookId::Harness),
        }
    }

    /// The catalog row's mode (§3.1 "Mode" column; §3.3 fixes it per id).
    pub fn mode(&self) -> HookMode {
        match self {
            HookId::AdmitPre => HookMode::Flow,
            HookId::ToolPre | HookId::SettlePre => HookMode::Bail,
            HookId::Harness(point) => harness_declared(point).unwrap_or(HookMode::Emit),
            _ => HookMode::Emit,
        }
    }

    /// The catalog row's grant class (§5.1).
    pub fn class(&self) -> HookClass {
        match self {
            HookId::AdmitPre | HookId::AdmitPost => HookClass::Admit,
            HookId::TurnRequest | HookId::TurnResponse => HookClass::Turn,
            HookId::ToolPre | HookId::ToolPost => HookClass::Tool,
            HookId::RecoverDecided => HookClass::Recover,
            HookId::SettlePre => HookClass::Settle,
            HookId::PluginLoad | HookId::PluginRefuse | HookId::PluginQuarantine => {
                HookClass::Plugin
            }
            HookId::Harness(_) => HookClass::Harness,
        }
    }

    /// The catalog row's payload (§3.2).
    pub fn payload_kind(&self) -> PayloadKind {
        match self {
            HookId::AdmitPre => PayloadKind::AdmitPre,
            HookId::AdmitPost => PayloadKind::AdmitPost,
            HookId::TurnRequest => PayloadKind::TurnRequest,
            HookId::TurnResponse => PayloadKind::TurnResponse,
            HookId::ToolPre => PayloadKind::ToolPre,
            HookId::ToolPost => PayloadKind::ToolPost,
            HookId::RecoverDecided => PayloadKind::RecoverDecided,
            HookId::SettlePre => PayloadKind::SettlePre,
            HookId::PluginLoad | HookId::PluginRefuse | HookId::PluginQuarantine => {
                PayloadKind::PluginLifecycle
            }
            HookId::Harness(_) => PayloadKind::Harness,
        }
    }

    /// Whether a sidecar may subscribe this hook in v1 (§3.1 "Sidecar"
    /// column; `plugin/*` is bus-owned, tier-0 only).
    pub fn sidecar_subscribable(&self) -> bool {
        !matches!(
            self,
            HookId::PluginLoad | HookId::PluginRefuse | HookId::PluginQuarantine
        )
    }
}

impl fmt::Display for HookId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_str())
    }
}

/// The closed catalog as one table (§3.1):
/// `(hook id, mode, class, payload, sidecar-subscribable)`. The test module
/// asserts this table against the accessor methods so the two cannot drift.
type CatalogEntry = (&'static str, HookMode, HookClass, PayloadKind, bool);

static CATALOG: &[CatalogEntry] = &[
    (
        "admit/pre",
        HookMode::Flow,
        HookClass::Admit,
        PayloadKind::AdmitPre,
        true,
    ),
    (
        "admit/post",
        HookMode::Emit,
        HookClass::Admit,
        PayloadKind::AdmitPost,
        true,
    ),
    (
        "turn/request",
        HookMode::Emit,
        HookClass::Turn,
        PayloadKind::TurnRequest,
        true,
    ),
    (
        "turn/response",
        HookMode::Emit,
        HookClass::Turn,
        PayloadKind::TurnResponse,
        true,
    ),
    (
        "tool/pre",
        HookMode::Bail,
        HookClass::Tool,
        PayloadKind::ToolPre,
        true,
    ),
    (
        "tool/post",
        HookMode::Emit,
        HookClass::Tool,
        PayloadKind::ToolPost,
        true,
    ),
    (
        "recover/decided",
        HookMode::Emit,
        HookClass::Recover,
        PayloadKind::RecoverDecided,
        true,
    ),
    (
        "settle/pre",
        HookMode::Bail,
        HookClass::Settle,
        PayloadKind::SettlePre,
        true,
    ),
    (
        "plugin/load",
        HookMode::Emit,
        HookClass::Plugin,
        PayloadKind::PluginLifecycle,
        false,
    ),
    (
        "plugin/refuse",
        HookMode::Emit,
        HookClass::Plugin,
        PayloadKind::PluginLifecycle,
        false,
    ),
    (
        "plugin/quarantine",
        HookMode::Emit,
        HookClass::Plugin,
        PayloadKind::PluginLifecycle,
        false,
    ),
];

/// The catalog table itself (for tests and boot-time validation).
pub fn catalog() -> &'static [CatalogEntry] {
    CATALOG
}

/// The harness point registry (§3.6): an array of
/// `(harness, point, mode)` triples, contract only when the owning harness
/// ships the entry — which must then also appear in §3.1, §3.4.1 and §3.2
/// in the same change. **The v1 registry is empty** (ADR-028 D27).
pub static HARNESS_REGISTRY: &[(&str, &str, HookMode)] = &[];

/// Whether a harness id is declared in [`HARNESS_REGISTRY`] — the check
/// validation rule 8 performs for `harness/…` subscriptions.
pub fn harness_declared(point: &HarnessPoint) -> Option<HookMode> {
    HARNESS_REGISTRY
        .iter()
        .find(|(h, p, _)| *h == point.harness && *p == point.point)
        .map(|(_, _, mode)| *mode)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_is_closed_and_each_row_has_mode_and_class() {
        // §3.1's eleven rows, with mode/class/payload exactly as the
        // contract states them (§3.3 fixes the mode per hook id).
        let expected: &[(&str, HookMode, HookClass, PayloadKind, bool)] = &[
            (
                "admit/pre",
                HookMode::Flow,
                HookClass::Admit,
                PayloadKind::AdmitPre,
                true,
            ),
            (
                "admit/post",
                HookMode::Emit,
                HookClass::Admit,
                PayloadKind::AdmitPost,
                true,
            ),
            (
                "turn/request",
                HookMode::Emit,
                HookClass::Turn,
                PayloadKind::TurnRequest,
                true,
            ),
            (
                "turn/response",
                HookMode::Emit,
                HookClass::Turn,
                PayloadKind::TurnResponse,
                true,
            ),
            (
                "tool/pre",
                HookMode::Bail,
                HookClass::Tool,
                PayloadKind::ToolPre,
                true,
            ),
            (
                "tool/post",
                HookMode::Emit,
                HookClass::Tool,
                PayloadKind::ToolPost,
                true,
            ),
            (
                "recover/decided",
                HookMode::Emit,
                HookClass::Recover,
                PayloadKind::RecoverDecided,
                true,
            ),
            (
                "settle/pre",
                HookMode::Bail,
                HookClass::Settle,
                PayloadKind::SettlePre,
                true,
            ),
            (
                "plugin/load",
                HookMode::Emit,
                HookClass::Plugin,
                PayloadKind::PluginLifecycle,
                false,
            ),
            (
                "plugin/refuse",
                HookMode::Emit,
                HookClass::Plugin,
                PayloadKind::PluginLifecycle,
                false,
            ),
            (
                "plugin/quarantine",
                HookMode::Emit,
                HookClass::Plugin,
                PayloadKind::PluginLifecycle,
                false,
            ),
        ];
        assert_eq!(CATALOG.len(), expected.len());
        assert_eq!(CATALOG, expected, "the catalog table is §3.1, verbatim");

        for (id, mode, class, payload, sidecar) in CATALOG {
            let hook = HookId::parse(id).expect("every catalog id parses");
            // Round-trip: as_str/parse agree for every closed row.
            assert_eq!(hook.as_str(), Cow::Borrowed(*id));
            assert_eq!(hook.mode(), *mode, "mode of {id}");
            assert_eq!(hook.class(), *class, "class of {id}");
            assert_eq!(hook.payload_kind(), *payload, "payload of {id}");
            assert_eq!(
                hook.sidecar_subscribable(),
                *sidecar,
                "sidecar availability of {id}"
            );
        }

        // The catalog is closed: a made-up id does not parse.
        assert!(HookId::parse("tool/pre-flight").is_none());
        assert!(HookId::parse("admit").is_none());
        assert!(HookId::parse("").is_none());
    }

    #[test]
    fn admission_hooks_are_not_drain_phase() {
        // §7.2: epoch rides only drain-phase frames. Both directions.
        for id in [
            "admit/pre",
            "admit/post",
            "plugin/load",
            "plugin/refuse",
            "plugin/quarantine",
        ] {
            let hook = HookId::parse(id).unwrap();
            assert!(!hook.class().is_drain_phase(), "{id} is not drain-phase");
        }
        for id in [
            "turn/request",
            "turn/response",
            "tool/pre",
            "tool/post",
            "recover/decided",
            "settle/pre",
        ] {
            let hook = HookId::parse(id).unwrap();
            assert!(hook.class().is_drain_phase(), "{id} is drain-phase");
        }
    }

    #[test]
    fn harness_namespace_is_empty() {
        // §3.6/D27: `harness/x/y` is a parse success and a registry miss.
        let id = HookId::parse("harness/dsh/start").expect("harness id parses");
        match &id {
            HookId::Harness(point) => {
                assert_eq!(point.harness, "dsh");
                assert_eq!(point.point, "start");
            }
            other => panic!("expected Harness arm, got {other:?}"),
        }
        assert_eq!(
            id.as_str(),
            Cow::<str>::Owned("harness/dsh/start".to_string())
        );
        let point = match &id {
            HookId::Harness(p) => p.clone(),
            _ => unreachable!(),
        };
        assert!(
            harness_declared(&point).is_none(),
            "the v1 harness registry is empty — every harness id is a miss"
        );

        // Segment grammar (§3.6): both segments ^[a-z0-9][a-z0-9_-]*$.
        for bad in [
            "harness//start",
            "harness/dsh/",
            "harness/Dsh/start",
            "harness/dsh/start/extra",
            "harness/ds h/start",
        ] {
            assert!(HookId::parse(bad).is_none(), "{bad} must not parse");
        }
        for good in ["harness/dsh/start", "harness/kian_loop/pre-render_2"] {
            assert!(HookId::parse(good).is_some(), "{good} must parse");
        }
    }
}
