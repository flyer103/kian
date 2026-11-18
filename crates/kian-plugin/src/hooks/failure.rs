//! Failure classification and class defaults (PLUGIN-SPEC §11.1, §5.2).
//!
//! Two failure classes, disjoint by construction:
//!
//! - **Unreachability** — the handler cannot answer (deadline, crash,
//!   quarantine, busy, handshake incomplete, EOF without a reply, or a
//!   self-declared `err:deadline`/`err:crashed`). Resolves to the
//!   **grant-keyed class default**.
//! - **Speech defect** — the handler answered wrongly (out-of-grant,
//!   wrong-mode, unparseable, or an `err` whose code is not a
//!   self-declared unreachability). The reply is **discarded as an
//!   opinion**: the outcome is exactly the as-if-absent one, never the
//!   class default.
//!
//! [`resolve_outcome`] is the *only* place a reply becomes a verdict
//! (K6): the bus and both tiers test through it, so the escalation bug of
//! §5.2 (a draft resolving a defect to the fail-closed default) cannot
//! reappear in a second code path.

use super::grants::{Grant, GrantSet, ReplyShape, veto_grant_for_class};
use super::ids::HookId;
use super::registry::{HookReply, ReplyErrCode};
use super::views::Origin;

/// Why a handler could not answer (§11.1's unreachability members).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    Deadline,
    Crash,
    Quarantine,
    Busy,
    HandshakeIncomplete,
    EofWithoutReply,
}

impl Cause {
    pub fn as_str(self) -> &'static str {
        match self {
            Cause::Deadline => "deadline",
            Cause::Crash => "crash",
            Cause::Quarantine => "quarantine",
            Cause::Busy => "busy",
            Cause::HandshakeIncomplete => "handshake_incomplete",
            Cause::EofWithoutReply => "eof_without_reply",
        }
    }
}

/// Which speech defect a discarded reply committed. The first five are
/// the ABI's non-unreachability `err` codes (§7.2); `Unparseable` arises
/// only at the wire (the tier-0 carrier is typed, so it cannot occur
/// in-process); `OriginObserveOnly` classifies the one in-process defect
/// the wire never sees — a stop answered at an observe-only admission
/// origin (§3.4.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpeechCode {
    Unauthorized,
    Malformed,
    UnsupportedHook,
    ProtocolViolation,
    Unparseable,
    OriginObserveOnly,
}

impl SpeechCode {
    pub fn as_str(self) -> &'static str {
        match self {
            SpeechCode::Unauthorized => "unauthorized",
            SpeechCode::Malformed => "malformed",
            SpeechCode::UnsupportedHook => "unsupported_hook",
            SpeechCode::ProtocolViolation => "protocol_violation",
            SpeechCode::Unparseable => "unparseable",
            SpeechCode::OriginObserveOnly => "origin_observe_only",
        }
    }
}

/// The two failure classes (§11.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureClass {
    Unreachable(Cause),
    Speech(SpeechCode),
}

/// The class default an unreachability resolves to — **a property of a
/// `veto:*` grant, not of a hook id** (§11.1):
///
/// - `Deny` — `veto:tool` holder at `tool/pre`: fail closed.
/// - `RefuseAdmission` — `veto:admit` holder at a channel-borne
///   `admit/pre`: fail closed for every target.
/// - `Abstain` — `veto:settle` holder at `settle/pre`: fail open; a
///   wedged quality gate must not hold a session hostage.
/// - `Lapse` — an observe-only subscriber anywhere: the observation is
///   lost and nothing else happens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClassDefault {
    Deny { cause: Cause },
    RefuseAdmission { cause: Cause },
    Abstain,
    Lapse,
}

/// What one subscriber's reply resolves to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// An observation (ack / next / abstain), or a defect whose reply was
    /// discarded: this subscriber contributes no verdict, exactly as if
    /// absent (§5.2).
    AsIfAbsent,
    /// A valid veto reply — the subscriber holds the grant and answered
    /// in the hook's mode.
    Denied { reason: String },
    /// Unreachability resolved to the grant-keyed default (§11.1).
    Default(ClassDefault),
    /// A speech defect: discarded, but recorded for the `hook-discard`
    /// audit line (§10.6) — audited as a defection, never as a lapse.
    Discarded {
        code: SpeechCode,
        grant: Option<Grant>,
    },
}

/// The one place a reply becomes a verdict (K6).
///
/// `hook` fixes the mode (wrong-mode check) and the class (grant-keyed
/// default); `origin` carries §11.1's per-origin split for the admission
/// port — the only veto-capable one is `channel`.
pub fn resolve_outcome(
    reply: &HookReply,
    grants: &GrantSet,
    hook: &HookId,
    origin: Origin,
) -> Outcome {
    // Self-declared unreachability first (§11.1): an honest plugin that
    // knows it cannot decide must not be mistaken for one speaking out of
    // turn.
    if let HookReply::Err { code, .. } = reply {
        return match code {
            ReplyErrCode::Deadline => {
                Outcome::Default(grant_keyed_default(grants, hook, origin, Cause::Deadline))
            }
            ReplyErrCode::Crashed => {
                Outcome::Default(grant_keyed_default(grants, hook, origin, Cause::Crash))
            }
            ReplyErrCode::Malformed => Outcome::Discarded {
                code: SpeechCode::Malformed,
                grant: None,
            },
            ReplyErrCode::Unauthorized => Outcome::Discarded {
                code: SpeechCode::Unauthorized,
                grant: None,
            },
            ReplyErrCode::UnsupportedHook => Outcome::Discarded {
                code: SpeechCode::UnsupportedHook,
                grant: None,
            },
            ReplyErrCode::ProtocolViolation => Outcome::Discarded {
                code: SpeechCode::ProtocolViolation,
                grant: None,
            },
        };
    }

    let shape = reply_shape(reply);
    let mode = hook.mode();
    if !shape.legal_at(mode) {
        // Wrong-mode reply: `err:malformed`-shaped speech defect (§5.2).
        return Outcome::Discarded {
            code: SpeechCode::Malformed,
            grant: None,
        };
    }
    match shape {
        ReplyShape::Ack | ReplyShape::FlowNext | ReplyShape::BailNone => Outcome::AsIfAbsent,
        ReplyShape::FlowStop => {
            // Only `admit/pre` is a flow hook (the mode check above has
            // already refused a stop anywhere else).
            match hook {
                HookId::AdmitPre if origin != Origin::Channel => {
                    // §3.4.3: the API-borne port is observe-only in v1 —
                    // there is no verdict to fail closed and no wire to
                    // express a refusal; the stop is discarded.
                    Outcome::Discarded {
                        code: SpeechCode::OriginObserveOnly,
                        grant: Some(Grant::VetoAdmit),
                    }
                }
                HookId::AdmitPre if !grants.contains(Grant::VetoAdmit) => Outcome::Discarded {
                    code: SpeechCode::Unauthorized,
                    grant: Some(Grant::VetoAdmit),
                },
                HookId::AdmitPre => Outcome::Denied {
                    reason: match reply {
                        HookReply::FlowStop { reason } => reason.clone(),
                        _ => unreachable!("shape checked above"),
                    },
                },
                _ => Outcome::Discarded {
                    code: SpeechCode::Malformed,
                    grant: None,
                },
            }
        }
        ReplyShape::BailDeny => {
            let required = veto_grant_for_class(hook.class());
            match required {
                Some(grant) if grants.contains(grant) => Outcome::Denied {
                    reason: match reply {
                        HookReply::BailDeny { reason } => reason.clone(),
                        _ => unreachable!("shape checked above"),
                    },
                },
                Some(grant) => Outcome::Discarded {
                    code: SpeechCode::Unauthorized,
                    grant: Some(grant),
                },
                // A deny at a hook with no veto-capable class: malformed.
                None => Outcome::Discarded {
                    code: SpeechCode::Malformed,
                    grant: None,
                },
            }
        }
    }
}

fn reply_shape(reply: &HookReply) -> ReplyShape {
    match reply {
        HookReply::Ack { .. } => ReplyShape::Ack,
        HookReply::FlowNext => ReplyShape::FlowNext,
        HookReply::FlowStop { .. } => ReplyShape::FlowStop,
        HookReply::BailNone => ReplyShape::BailNone,
        HookReply::BailDeny { .. } => ReplyShape::BailDeny,
        HookReply::Err { .. } => unreachable!("err handled before shaping"),
    }
}

/// The grant-keyed unreachability default (§11.1's table).
/// `pub(crate)`: the bus's session-quarantine path (§9.3) synthesizes
/// the default directly — a quarantined sidecar is unreachable, and
/// the cause must read `quarantine`, not `crashed`.
pub(crate) fn grant_keyed_default(
    grants: &GrantSet,
    hook: &HookId,
    origin: Origin,
    cause: Cause,
) -> ClassDefault {
    match hook {
        HookId::ToolPre if grants.contains(Grant::VetoTool) => ClassDefault::Deny { cause },
        HookId::AdmitPre if origin == Origin::Channel && grants.contains(Grant::VetoAdmit) => {
            ClassDefault::RefuseAdmission { cause }
        }
        HookId::SettlePre if grants.contains(Grant::VetoSettle) => ClassDefault::Abstain,
        // An observe-only subscriber — at any hook, including a
        // veto-capable one, and at an observe-only port — lapses (§11.1
        // row 4): an observer has no decision to lose.
        _ => ClassDefault::Lapse,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grants(names: &[&str]) -> GrantSet {
        GrantSet::from_names(names).unwrap()
    }

    #[test]
    fn class_default_is_keyed_by_grant() {
        let tool_pre = HookId::parse("tool/pre").unwrap();
        let admit_pre = HookId::parse("admit/pre").unwrap();
        let settle_pre = HookId::parse("settle/pre").unwrap();

        let unreachable = HookReply::Err {
            code: ReplyErrCode::Crashed,
            message: "gone".to_string(),
        };

        // Row 1: `veto:tool` at `tool/pre` fails CLOSED.
        assert_eq!(
            resolve_outcome(
                &unreachable,
                &grants(&["observe:tool", "veto:tool"]),
                &tool_pre,
                Origin::Api
            ),
            Outcome::Default(ClassDefault::Deny {
                cause: Cause::Crash
            })
        );
        // Row 2: `veto:admit` at a channel-borne `admit/pre` fails CLOSED.
        assert_eq!(
            resolve_outcome(
                &unreachable,
                &grants(&["observe:admit", "veto:admit"]),
                &admit_pre,
                Origin::Channel
            ),
            Outcome::Default(ClassDefault::RefuseAdmission {
                cause: Cause::Crash
            })
        );
        // The same holder at an API-borne origin lapses: the port is
        // observe-only there, so there is no verdict to fail closed.
        assert_eq!(
            resolve_outcome(
                &unreachable,
                &grants(&["observe:admit", "veto:admit"]),
                &admit_pre,
                Origin::Api
            ),
            Outcome::Default(ClassDefault::Lapse)
        );
        // Row 3: `veto:settle` at `settle/pre` fails OPEN.
        assert_eq!(
            resolve_outcome(
                &unreachable,
                &grants(&["observe:settle", "veto:settle"]),
                &settle_pre,
                Origin::Api
            ),
            Outcome::Default(ClassDefault::Abstain)
        );
        // Row 4: observe-only lapses — even at a veto-capable hook.
        assert_eq!(
            resolve_outcome(
                &unreachable,
                &grants(&["observe:tool"]),
                &tool_pre,
                Origin::Api
            ),
            Outcome::Default(ClassDefault::Lapse)
        );
        // Deadline is the same default with a different cause.
        let overdue = HookReply::Err {
            code: ReplyErrCode::Deadline,
            message: "slow".to_string(),
        };
        assert_eq!(
            resolve_outcome(
                &overdue,
                &grants(&["observe:tool", "veto:tool"]),
                &tool_pre,
                Origin::Api
            ),
            Outcome::Default(ClassDefault::Deny {
                cause: Cause::Deadline
            })
        );
    }

    #[test]
    fn unreachable_veto_denies_at_any_order_position() {
        // §11.1 consequence 1: "not reachable" cannot be read as
        // "abstained". `resolve_outcome` is position-free — the ordering
        // guarantee is exercised end-to-end in the bus tests, where the
        // same reply placed *after* an abstaining subscriber still denies.
        let tool_pre = HookId::parse("tool/pre").unwrap();
        let abstain = HookReply::BailNone;
        let unreachable = HookReply::Err {
            code: ReplyErrCode::Deadline,
            message: "over deadline".to_string(),
        };
        let veto_holder = grants(&["observe:tool", "veto:tool"]);

        assert_eq!(
            resolve_outcome(&abstain, &veto_holder, &tool_pre, Origin::Api),
            Outcome::AsIfAbsent
        );
        assert_eq!(
            resolve_outcome(&unreachable, &veto_holder, &tool_pre, Origin::Api),
            Outcome::Default(ClassDefault::Deny {
                cause: Cause::Deadline
            })
        );
    }

    #[test]
    fn speech_defects_are_discarded_never_defaulted() {
        // §5.2: an out-of-grant or wrong-mode reply is not the class
        // default — the outcome is exactly as-if-absent.
        let tool_pre = HookId::parse("tool/pre").unwrap();
        let observe_only = grants(&["observe:tool"]);

        // Out-of-grant deny from an observe-only subscriber.
        assert_eq!(
            resolve_outcome(
                &HookReply::BailDeny {
                    reason: "no".to_string()
                },
                &observe_only,
                &tool_pre,
                Origin::Api
            ),
            Outcome::Discarded {
                code: SpeechCode::Unauthorized,
                grant: Some(Grant::VetoTool)
            }
        );
        // Wrong-mode: an `ack` at a bail hook.
        assert_eq!(
            resolve_outcome(
                &HookReply::Ack { note: None },
                &observe_only,
                &tool_pre,
                Origin::Api
            ),
            Outcome::Discarded {
                code: SpeechCode::Malformed,
                grant: None
            }
        );
        // A flow stop at a bail hook is wrong-mode too.
        assert_eq!(
            resolve_outcome(
                &HookReply::FlowStop {
                    reason: "no".to_string()
                },
                &observe_only,
                &tool_pre,
                Origin::Api
            ),
            Outcome::Discarded {
                code: SpeechCode::Malformed,
                grant: None
            }
        );
        // A stop at an API-borne admission is discarded (observe-only
        // origin), even with the grant held.
        let admit_pre = HookId::parse("admit/pre").unwrap();
        assert_eq!(
            resolve_outcome(
                &HookReply::FlowStop {
                    reason: "no".to_string()
                },
                &grants(&["observe:admit", "veto:admit"]),
                &admit_pre,
                Origin::Api
            ),
            Outcome::Discarded {
                code: SpeechCode::OriginObserveOnly,
                grant: Some(Grant::VetoAdmit)
            }
        );
        // A valid stop at a channel-borne admission with the grant is a
        // verdict, not a defect.
        assert_eq!(
            resolve_outcome(
                &HookReply::FlowStop {
                    reason: "off-hours".to_string()
                },
                &grants(&["observe:admit", "veto:admit"]),
                &admit_pre,
                Origin::Channel
            ),
            Outcome::Denied {
                reason: "off-hours".to_string()
            }
        );
        // Valid deny and abstain at bail hooks with the grant held.
        assert_eq!(
            resolve_outcome(
                &HookReply::BailDeny {
                    reason: "blocked".to_string()
                },
                &grants(&["observe:tool", "veto:tool"]),
                &tool_pre,
                Origin::Api
            ),
            Outcome::Denied {
                reason: "blocked".to_string()
            }
        );
        assert_eq!(
            resolve_outcome(
                &HookReply::BailNone,
                &grants(&["observe:settle", "veto:settle"]),
                &HookId::parse("settle/pre").unwrap(),
                Origin::Api
            ),
            Outcome::AsIfAbsent
        );
    }
}
