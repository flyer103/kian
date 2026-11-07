//! Concept model (DESIGN §2.1 "Concept model and Rust type skeletons").
//!
//! Pure types only — no I/O, no runtime state (acceptance). The event
//! stream is the sole truth: these types are the vocabulary the projections
//! and the store are built from, and none of them hide authoritative state
//! outside the event log.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The one first-class durable entity (DESIGN §2.1). A session's state is
/// never a column or a struct field — it is only ever the projection of the
/// session's event stream (constraint 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct SessionId(pub Uuid);

impl SessionId {
    /// Generate a fresh random session id (v4 UUID).
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

/// Fencing token issued by a claim (ADR-005). Monotonically increasing per
/// session; every drain write carries it and the store rejects stale-epoch
/// writes. Ord derives the monotone comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Epoch(pub u64);

impl Epoch {
    pub const GENESIS: Epoch = Epoch(0);

    /// The next epoch in the monotone sequence (claim-time issuance).
    pub fn next(self) -> Epoch {
        Epoch(self.0 + 1)
    }
}

/// Lease = liveness only (ADR-005): TTL + heartbeat *detect* recoverable
/// sessions; expiry never grants permission to accept stale writes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseRecord {
    pub session: SessionId,
    pub epoch: Epoch,
    /// Identity of the drain holder (process/node id).
    pub holder: String,
    /// Lease time-to-live, milliseconds.
    pub ttl_ms: u64,
    /// Last heartbeat, unix epoch milliseconds.
    pub last_heartbeat: u64,
}

/// A drain has no durable identity (DESIGN §2.1, invariant 4): after a
/// crash no in-process continuation is assumed — recovery is a fresh drain
/// replaying events and continuing to consume the inbox.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Drain {
    pub session: SessionId,
    pub epoch: Epoch,
    pub claim: LeaseRecord,
}

/// An admitted input. Inputs are durable rows *before* any wake fires
/// (admit-before-wake, invariant 1); the idempotency key makes admit
/// idempotent and `eligible_at` gates consumption.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputRow {
    pub id: Uuid,
    pub session: SessionId,
    /// Opaque input payload.
    pub payload: serde_json::Value,
    /// Admitted at, unix epoch milliseconds.
    pub admitted_at: u64,
    /// Becomes eligible for a drain to take at, unix epoch milliseconds.
    pub eligible_at: u64,
    /// Deduplication key for idempotent admit.
    pub idempotency_key: Option<String>,
}

/// Declarative model-slot name (ADR-006): e.g. `main`, `fast`, `vision`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct SlotName(pub String);

/// One concrete (provider, model) target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotTarget {
    pub provider: String,
    pub model: String,
}

/// Slot → provider/model/fallback mapping (ADR-006). Fixed for a session's
/// lifetime; switching requires an explicit `slot/migration` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotSpec {
    pub provider: String,
    pub model: String,
    /// Fallback chain, tried in order; each switch emits a durable
    /// `slot/fallback` event.
    pub fallback: Vec<SlotTarget>,
}

/// Budgets consumed with refund semantics (A5): unused remainders are
/// returned on task completion; exhaustion escalates recovery — never loops.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Budgets {
    pub tokens: Option<u64>,
    pub wall_clock_ms: Option<u64>,
}

/// Per-call tool permission (ADR-015): a **closed** vocabulary —
/// `allow` or `deny`, nothing else parses (fail-closed: an unknown value
/// refuses boot). `ask` is deliberately absent this round; the closed
/// shape lets a later confirmation plane add it additively.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Permission {
    Allow,
    Deny,
}

/// Agent = versioned config (DESIGN §2.1). No runtime state lives here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDef {
    pub id: String,
    /// Monotone config version; a `Thread` may pin one.
    pub version: u64,
    pub model_slots: BTreeMap<SlotName, SlotSpec>,
    /// Names of tools the agent may call (resolved via `ToolProvider`).
    pub tools: Vec<String>,
    pub budgets: Budgets,
}

/// Environment spec, unversioned config (DESIGN §2.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvSpec {
    /// Sandbox selection (built-in: `"none"` = local).
    pub sandbox: String,
    pub network_allowlist: Vec<String>,
    pub deps: Vec<String>,
}

/// Thread: an independent event stream pinning a parent session and an
/// agent version — the core's only cross-session delegation primitive
/// (super-agent bundles compose it; ADR-003).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Thread {
    pub id: Uuid,
    pub parent_session: SessionId,
    /// Pinned `AgentDef` version.
    pub pinned_version: u64,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;
    use uuid::Uuid;

    use super::{
        AgentDef, Budgets, Drain, EnvSpec, Epoch, InputRow, LeaseRecord, Permission, SessionId,
        SlotName, SlotSpec, SlotTarget, Thread,
    };

    /// `Permission` is a closed vocabulary — `allow`/`deny` parse
    /// and serialize lowercase, and any other value (notably `ask`,
    /// deferred to a later round) refuses to parse (fail-closed).
    #[test]
    fn permission_is_a_closed_vocabulary_fail_closed() {
        assert_eq!(
            serde_json::to_value(Permission::Allow).unwrap(),
            json!("allow")
        );
        assert_eq!(
            serde_json::to_value(Permission::Deny).unwrap(),
            json!("deny")
        );
        let allow: Permission = serde_json::from_value(json!("allow")).unwrap();
        let deny: Permission = serde_json::from_value(json!("deny")).unwrap();
        assert_eq!(allow, Permission::Allow);
        assert_eq!(deny, Permission::Deny);
        for unknown in ["ask", "Allow", "ALLOW", "maybe", ""] {
            assert!(
                serde_json::from_value::<Permission>(json!(unknown)).is_err(),
                "`{unknown}` must refuse to parse (closed vocabulary, fail-closed)"
            );
        }
    }

    #[test]
    fn session_id_and_epoch_are_serde_transparent() {
        let sid = SessionId(Uuid::nil());
        assert_eq!(
            serde_json::to_value(sid).unwrap(),
            json!("00000000-0000-0000-0000-000000000000")
        );
        let back: SessionId =
            serde_json::from_value(json!("00000000-0000-0000-0000-000000000000")).unwrap();
        assert_eq!(back, sid);
        assert_eq!(serde_json::to_value(Epoch(9)).unwrap(), json!(9));
    }

    #[test]
    fn input_row_round_trips_and_idempotency_key_is_optional() {
        let row = InputRow {
            id: Uuid::nil(),
            session: SessionId(Uuid::nil()),
            payload: json!({"text": "hello"}),
            admitted_at: 100,
            eligible_at: 150,
            idempotency_key: Some("key-1".into()),
        };
        let back: InputRow = serde_json::from_value(serde_json::to_value(&row).unwrap()).unwrap();
        assert_eq!(back, row);

        let row_no_key = InputRow {
            idempotency_key: None,
            ..row
        };
        let back: InputRow =
            serde_json::from_value(serde_json::to_value(&row_no_key).unwrap()).unwrap();
        assert_eq!(back, row_no_key);
    }

    #[test]
    fn agent_def_round_trips_with_slots_and_budgets() {
        let def = AgentDef {
            id: "researcher".into(),
            version: 3,
            model_slots: BTreeMap::from([(
                SlotName("main".into()),
                SlotSpec {
                    provider: "openai-compat".into(),
                    model: "kimi-k2".into(),
                    fallback: vec![SlotTarget {
                        provider: "openai-compat".into(),
                        model: "deepseek-v3".into(),
                    }],
                },
            )]),
            tools: vec!["fs.read".into(), "fs.write".into()],
            budgets: Budgets {
                tokens: Some(1_000_000),
                wall_clock_ms: Some(3_600_000),
            },
        };
        let back: AgentDef = serde_json::from_value(serde_json::to_value(&def).unwrap()).unwrap();
        assert_eq!(back, def);
    }

    #[test]
    fn env_spec_thread_drain_lease_round_trip() {
        let env = EnvSpec {
            sandbox: "none".into(),
            network_allowlist: vec!["api.example.com".into()],
            deps: vec!["ripgrep".into()],
        };
        let thread = Thread {
            id: Uuid::nil(),
            parent_session: SessionId(Uuid::nil()),
            pinned_version: 2,
        };
        let lease = LeaseRecord {
            session: SessionId(Uuid::nil()),
            epoch: Epoch(5),
            holder: "kiand-1".into(),
            ttl_ms: 30_000,
            last_heartbeat: 1_761_900_000_000,
        };
        let drain = Drain {
            session: lease.session,
            epoch: Epoch(5),
            claim: lease.clone(),
        };
        for (name, ser) in [
            ("env", serde_json::to_value(&env).unwrap()),
            ("thread", serde_json::to_value(&thread).unwrap()),
            ("lease", serde_json::to_value(&lease).unwrap()),
            ("drain", serde_json::to_value(&drain).unwrap()),
        ] {
            match name {
                "env" => assert_eq!(serde_json::from_value::<EnvSpec>(ser).unwrap(), env),
                "thread" => assert_eq!(serde_json::from_value::<Thread>(ser).unwrap(), thread),
                "lease" => assert_eq!(serde_json::from_value::<LeaseRecord>(ser).unwrap(), lease),
                _ => assert_eq!(serde_json::from_value::<Drain>(ser).unwrap(), drain),
            }
        }
    }
}
