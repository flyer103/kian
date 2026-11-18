//! Capability grants (PLUGIN-SPEC §5).
//!
//! The v1 closed set is eight grants; `mutate:*` names are **reserved**
//! and refused (`unknown_capability`-shaped) so a future transform grammar
//! cannot silently inherit today's configurations (§5.1, D2 as amended).
//! A `veto:*` grant implies the matching `observe:*` and is refused
//! without it — a handler cannot veto what it does not receive.
//!
//! [`GrantSet::from_names`] is the single constructor enforcing both
//! rules, so the sidecar manifest validation (§6.2 rules 9/10) and the
//! per-worker resolution (§8) both call it instead of re-deriving the
//! checks.

use std::collections::BTreeSet;

use thiserror::Error;

use super::ids::{HookClass, HookId, HookMode};

/// One capability of the closed v1 grant set (§5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Grant {
    ObserveAdmit,
    ObserveTurn,
    ObserveTool,
    ObserveRecover,
    ObserveSettle,
    VetoAdmit,
    VetoTool,
    VetoSettle,
}

impl Grant {
    pub fn as_str(self) -> &'static str {
        match self {
            Grant::ObserveAdmit => "observe:admit",
            Grant::ObserveTurn => "observe:turn",
            Grant::ObserveTool => "observe:tool",
            Grant::ObserveRecover => "observe:recover",
            Grant::ObserveSettle => "observe:settle",
            Grant::VetoAdmit => "veto:admit",
            Grant::VetoTool => "veto:tool",
            Grant::VetoSettle => "veto:settle",
        }
    }
}

impl std::fmt::Display for Grant {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Grant {
    /// Parses a grant name; `None` for anything outside the closed set —
    /// including the reserved `mutate:*` names (§5.1).
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "observe:admit" => Some(Grant::ObserveAdmit),
            "observe:turn" => Some(Grant::ObserveTurn),
            "observe:tool" => Some(Grant::ObserveTool),
            "observe:recover" => Some(Grant::ObserveRecover),
            "observe:settle" => Some(Grant::ObserveSettle),
            "veto:admit" => Some(Grant::VetoAdmit),
            "veto:tool" => Some(Grant::VetoTool),
            "veto:settle" => Some(Grant::VetoSettle),
            _ => None,
        }
    }

    /// The `observe:*` grant a `veto:*` grant implies (§5.1); observe
    /// grants imply themselves for the `covers` check.
    pub fn implied_observe(self) -> Option<Grant> {
        match self {
            Grant::VetoAdmit => Some(Grant::ObserveAdmit),
            Grant::VetoTool => Some(Grant::ObserveTool),
            Grant::VetoSettle => Some(Grant::ObserveSettle),
            grant @ (Grant::ObserveAdmit
            | Grant::ObserveTurn
            | Grant::ObserveTool
            | Grant::ObserveRecover
            | Grant::ObserveSettle) => Some(grant),
        }
    }
}

/// Named constructor failures (§5.1's two refusal shapes).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GrantError {
    /// A name outside the closed set — including the reserved `mutate:*`
    /// names, refused `unknown_capability`-shaped so a manifest cannot
    /// name a capability the contract removed (§5.1).
    #[error("unknown capability `{name}` (the v1 set is closed; `mutate:*` names are reserved)")]
    UnknownCapability { name: String },
    /// A `veto:*` grant without its matching `observe:*` (§5.1: a handler
    /// cannot veto what it does not receive).
    #[error("grant `{veto}` requires its matching `{observe}`")]
    MissingObserve { veto: Grant, observe: Grant },
}

/// A validated set of grants (§5.1). Construction through
/// [`GrantSet::from_names`] is the shared validator.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrantSet(BTreeSet<Grant>);

impl GrantSet {
    /// Builds a grant set from wire names, enforcing both §5.1 rules:
    /// every name must be in the closed set, and every `veto:*` must be
    /// accompanied by its matching `observe:*`.
    pub fn from_names(names: &[&str]) -> Result<Self, GrantError> {
        let mut grants = BTreeSet::new();
        for name in names {
            let grant = Grant::parse(name).ok_or_else(|| GrantError::UnknownCapability {
                name: (*name).to_string(),
            })?;
            if let Some(observe) = veto_requires(grant)
                && !names_contains(names, observe)
            {
                return Err(GrantError::MissingObserve {
                    veto: grant,
                    observe,
                });
            }
            grants.insert(grant);
        }
        Ok(Self(grants))
    }

    /// A set from already-validated grants (no re-validation; used where
    /// the grants were constructed as typed values).
    pub fn from_grants(grants: impl IntoIterator<Item = Grant>) -> Self {
        Self(grants.into_iter().collect())
    }

    pub fn contains(&self, grant: Grant) -> bool {
        self.0.contains(&grant)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = Grant> + '_ {
        self.0.iter().copied()
    }

    /// Whether the set's `observe:*` coverage includes `class` (§5.2's
    /// boot check: every subscribed hook must be covered).
    pub fn observes_class(&self, class: HookClass) -> bool {
        match observe_grant_for_class(class) {
            Some(grant) => self.contains(grant),
            None => false,
        }
    }
}

impl IntoIterator for &GrantSet {
    type Item = Grant;
    type IntoIter = std::collections::btree_set::IntoIter<Grant>;

    fn into_iter(self) -> Self::IntoIter {
        self.0
            .iter()
            .copied()
            .collect::<BTreeSet<Grant>>()
            .into_iter()
    }
}

fn veto_requires(grant: Grant) -> Option<Grant> {
    grant.implied_observe().filter(|_| {
        matches!(
            grant,
            Grant::VetoAdmit | Grant::VetoTool | Grant::VetoSettle
        )
    })
}

fn names_contains(names: &[&str], grant: Grant) -> bool {
    names.iter().any(|n| *n == grant.as_str())
}

/// The `observe:*` grant covering a hook class (§5.1's "Requires
/// subscription to" column). `None` for classes no observe grant covers
/// (`Harness`, `Plugin` — neither is subscribable through grants in v1).
pub fn observe_grant_for_class(class: HookClass) -> Option<Grant> {
    match class {
        HookClass::Admit => Some(Grant::ObserveAdmit),
        HookClass::Turn => Some(Grant::ObserveTurn),
        HookClass::Tool => Some(Grant::ObserveTool),
        HookClass::Recover => Some(Grant::ObserveRecover),
        HookClass::Settle => Some(Grant::ObserveSettle),
        HookClass::Harness | HookClass::Plugin => None,
    }
}

/// The `veto:*` grant that makes a hook's verdict reachable (§5.1, §11.1:
/// a class default belongs to a `veto:*` grant, not to a hook id). `None`
/// for classes with no veto-capable hook in v1.
pub fn veto_grant_for_class(class: HookClass) -> Option<Grant> {
    match class {
        HookClass::Admit => Some(Grant::VetoAdmit),
        HookClass::Tool => Some(Grant::VetoTool),
        HookClass::Settle => Some(Grant::VetoSettle),
        HookClass::Turn | HookClass::Recover | HookClass::Harness | HookClass::Plugin => None,
    }
}

/// The reply → required-grant map (§3.3's "Capability required by the
/// reply" column): which grant a reply variant must hold to be an opinion
/// at all. `None` replies (ack / next / abstain) require none.
///
/// Takes the reply as a shape discriminator so both the tier-0 bus and
/// the C3 sidecar frame decoder classify through one function.
pub fn reply_required_grant(hook: &HookId, reply_shape: ReplyShape) -> Option<Grant> {
    match reply_shape {
        ReplyShape::Ack | ReplyShape::FlowNext | ReplyShape::BailNone => None,
        ReplyShape::FlowStop => {
            // Only `admit/pre` is a flow hook; a stop there is `veto:admit`.
            match hook {
                HookId::AdmitPre => Some(Grant::VetoAdmit),
                _ => None,
            }
        }
        ReplyShape::BailDeny => match hook {
            HookId::ToolPre => Some(Grant::VetoTool),
            HookId::SettlePre => Some(Grant::VetoSettle),
            _ => None,
        },
    }
}

/// The shape of a reply, independent of its payload — the discriminator
/// [`reply_required_grant`] and the mode check (§3.3) both classify on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyShape {
    Ack,
    FlowNext,
    FlowStop,
    BailNone,
    BailDeny,
}

impl ReplyShape {
    /// Whether this shape is a legal reply at `mode` (§3.3's mode table;
    /// wrong-mode replies are speech defects, §5.2).
    pub fn legal_at(self, mode: HookMode) -> bool {
        matches!(
            (self, mode),
            (ReplyShape::Ack, HookMode::Emit)
                | (ReplyShape::FlowNext | ReplyShape::FlowStop, HookMode::Flow)
                | (ReplyShape::BailNone | ReplyShape::BailDeny, HookMode::Bail)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_mutate_names_are_refused() {
        // §5.1: `mutate:*` names are reserved — refused as unknown
        // capability, so a future transform grammar cannot silently
        // inherit today's configurations.
        for name in [
            "mutate:context",
            "mutate:admit",
            "mutate:tool",
            "mutate:whatever",
        ] {
            let err = GrantSet::from_names(&[name]).expect_err("{name} must be refused");
            assert_eq!(
                err,
                GrantError::UnknownCapability {
                    name: name.to_string()
                },
                "mutate names are unknown-capability shaped"
            );
        }
        // And so is any other non-grant.
        assert!(matches!(
            GrantSet::from_names(&["sudo"]),
            Err(GrantError::UnknownCapability { .. })
        ));
        assert!(GrantSet::from_names(&[]).unwrap().is_empty());
    }

    #[test]
    fn veto_implies_observe() {
        // §5.1: a `veto:*` grant is refused without its matching
        // `observe:*` — both missing directions.
        assert!(matches!(
            GrantSet::from_names(&["veto:admit"]),
            Err(GrantError::MissingObserve { .. })
        ));
        assert!(matches!(
            GrantSet::from_names(&["veto:tool", "observe:admit"]),
            Err(GrantError::MissingObserve { .. })
        ));
        // A veto does not satisfy another veto's requirement either.
        assert!(matches!(
            GrantSet::from_names(&["veto:settle", "veto:admit", "observe:admit"]),
            Err(GrantError::MissingObserve { .. })
        ));
        // With the matching observe, each veto is accepted.
        for ok in [
            &["veto:admit", "observe:admit"][..],
            &["veto:tool", "observe:tool"][..],
            &["veto:settle", "observe:settle"][..],
        ] {
            GrantSet::from_names(ok).expect("veto with matching observe is valid");
        }
        // Observe alone is fine and covers its class.
        let set = GrantSet::from_names(&["observe:tool"]).unwrap();
        assert!(set.observes_class(HookClass::Tool));
        assert!(!set.observes_class(HookClass::Admit));
    }

    #[test]
    fn reply_requires_exactly_one_grant() {
        // §3.3's table: exactly the veto rows require a grant; ack / next
        // / abstain never do.
        let admit_pre = HookId::parse("admit/pre").unwrap();
        let tool_pre = HookId::parse("tool/pre").unwrap();
        let settle_pre = HookId::parse("settle/pre").unwrap();
        let turn_request = HookId::parse("turn/request").unwrap();

        assert_eq!(
            reply_required_grant(&admit_pre, ReplyShape::FlowStop),
            Some(Grant::VetoAdmit)
        );
        assert_eq!(
            reply_required_grant(&tool_pre, ReplyShape::BailDeny),
            Some(Grant::VetoTool)
        );
        assert_eq!(
            reply_required_grant(&settle_pre, ReplyShape::BailDeny),
            Some(Grant::VetoSettle)
        );
        for (hook, shape) in [
            (&admit_pre, ReplyShape::Ack),
            (&admit_pre, ReplyShape::FlowNext),
            (&admit_pre, ReplyShape::BailNone),
            (&tool_pre, ReplyShape::BailNone),
            (&settle_pre, ReplyShape::BailNone),
            (&turn_request, ReplyShape::Ack),
        ] {
            assert_eq!(
                reply_required_grant(hook, shape),
                None,
                "no-opinion reply requires no grant"
            );
        }
        // A stop/deny at a hook that cannot carry it maps to no grant —
        // the mode check classifies it as a wrong-mode defect instead.
        assert_eq!(
            reply_required_grant(&turn_request, ReplyShape::FlowStop),
            None
        );
        assert_eq!(
            reply_required_grant(&turn_request, ReplyShape::BailDeny),
            None
        );

        // Mode legality (§3.3): only the mode's own reply shapes are legal.
        assert!(ReplyShape::Ack.legal_at(HookMode::Emit));
        assert!(!ReplyShape::Ack.legal_at(HookMode::Bail));
        assert!(ReplyShape::FlowStop.legal_at(HookMode::Flow));
        assert!(!ReplyShape::FlowStop.legal_at(HookMode::Emit));
        assert!(ReplyShape::BailDeny.legal_at(HookMode::Bail));
        assert!(!ReplyShape::BailDeny.legal_at(HookMode::Flow));
        assert!(ReplyShape::FlowNext.legal_at(HookMode::Flow));
        assert!(!ReplyShape::BailNone.legal_at(HookMode::Emit));
    }
}
