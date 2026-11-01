//! Slot resolution (card C8, ADR-006).
//!
//! `AgentDef` slots bind to provider/model/fallback once per session
//! (`slot/binding` event at claim); runtime model swap is cache-hostile and
//! forbidden; the explicit migration event is the only escape hatch;
//! fallback switches each emit a `slot/fallback` durable event.

use std::collections::BTreeMap;

use kian_core::events::{Event, EventKind};
use kian_core::model::{AgentDef, SessionId, SlotName, SlotSpec, SlotTarget};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SlotError {
    #[error("unknown slot: {0}")]
    UnknownSlot(String),
    /// Runtime slot swaps are forbidden by ADR-006: the binding is fixed
    /// for a session's lifetime (cache-byte stability); only an explicit
    /// `slot/migration` event may change it.
    #[error(
        "slot swap forbidden (ADR-006): slot `{slot}` is bound for the session lifetime; \
         use migrate() and persist an explicit slot/migration event"
    )]
    SwapForbidden { slot: String },
    #[error("slot event replay error: {0}")]
    Replay(String),
}

// ---------------------------------------------------------------------------
// Durable-event payload schemas (owned by this module)
// ---------------------------------------------------------------------------

/// `slot/binding` payload: the full spec the slot is bound to at claim.
pub fn binding_payload(slot: &SlotName, spec: &SlotSpec) -> Value {
    json!({ "slot": slot, "spec": spec })
}

/// Inverse of [`binding_payload`]; `None` when the payload is malformed.
pub fn spec_from_binding_payload(payload: &Value) -> Option<SlotSpec> {
    serde_json::from_value(payload.get("spec")?.clone()).ok()
}

/// `slot/fallback` payload: one chain switch within a turn.
pub fn fallback_payload(slot: &SlotName, from: &SlotTarget, to: &SlotTarget) -> Value {
    json!({ "slot": slot, "from": from, "to": to })
}

/// `slot/migration` payload: the explicit escape hatch (ADR-006). Carries
/// full specs so replay can rebuild the binding without external state.
pub fn migration_payload(slot: &SlotName, from: &SlotSpec, to: &SlotSpec, reason: &str) -> Value {
    json!({ "slot": slot, "from_spec": from, "to_spec": to, "reason": reason })
}

fn spec_at(payload: &Value, key: &str) -> Option<SlotSpec> {
    serde_json::from_value(payload.get(key)?.clone()).ok()
}

// ---------------------------------------------------------------------------
// Resolver / per-session bindings
// ---------------------------------------------------------------------------

/// Resolves `AgentDef` slots (ADR-006, DESIGN §7). Pure data: the binding
/// decision is made once per session at claim time.
pub struct SlotResolver {
    specs: BTreeMap<SlotName, SlotSpec>,
}

impl SlotResolver {
    pub fn from_agent_def(def: &AgentDef) -> Self {
        Self {
            specs: def.model_slots.clone(),
        }
    }

    pub fn spec(&self, slot: &SlotName) -> Result<&SlotSpec, SlotError> {
        self.specs
            .get(slot)
            .ok_or_else(|| SlotError::UnknownSlot(slot.0.clone()))
    }

    /// Bind every slot of the agent for one session. The returned
    /// [`SessionSlots::binding_payloads`] are the `slot/binding` event
    /// payloads the caller persists at claim (ADR-006).
    pub fn bind_session(&self, session: SessionId) -> SessionSlots {
        SessionSlots {
            session,
            bindings: self
                .specs
                .iter()
                .map(|(name, spec)| (name.clone(), SlotBinding::from_spec(spec)))
                .collect(),
        }
    }
}

/// A slot's effective binding for one session. Rebuildable at any time
/// from the session's `slot/binding` + `slot/migration` events (see
/// [`SessionSlots::replay`]); nothing here is authoritative state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotBinding {
    /// Current primary target.
    pub target: SlotTarget,
    /// Fallback chain in effect (tried in order, one `slot/fallback`
    /// event per switch).
    pub fallback: Vec<SlotTarget>,
    /// True iff the binding was changed by an explicit `slot/migration`.
    pub migrated: bool,
}

impl SlotBinding {
    fn from_spec(spec: &SlotSpec) -> Self {
        Self {
            target: SlotTarget {
                provider: spec.provider.clone(),
                model: spec.model.clone(),
            },
            fallback: spec.fallback.clone(),
            migrated: false,
        }
    }
}

/// The record of one explicit migration — the caller persists
/// `SlotMigration::payload` as a `slot/migration` event.
#[derive(Debug, Clone, PartialEq)]
pub struct SlotMigration {
    pub slot: SlotName,
    pub from: SlotSpec,
    pub to: SlotSpec,
    pub reason: String,
    pub payload: Value,
}

/// Per-session slot bindings. Fixed for the session's lifetime (ADR-006).
#[derive(Debug)]
pub struct SessionSlots {
    session: SessionId,
    bindings: BTreeMap<SlotName, SlotBinding>,
}

impl SessionSlots {
    pub fn session(&self) -> SessionId {
        self.session
    }

    pub fn binding(&self, slot: &SlotName) -> Result<&SlotBinding, SlotError> {
        self.bindings
            .get(slot)
            .ok_or_else(|| SlotError::UnknownSlot(slot.0.clone()))
    }

    pub fn target(&self, slot: &SlotName) -> Result<SlotTarget, SlotError> {
        Ok(self.binding(slot)?.target.clone())
    }

    /// The `slot/binding` event payloads for every bound slot, in slot-name
    /// order. Persist these at claim.
    pub fn binding_payloads(&self) -> Vec<Value> {
        self.bindings
            .iter()
            .map(|(name, b)| {
                let spec = SlotSpec {
                    provider: b.target.provider.clone(),
                    model: b.target.model.clone(),
                    fallback: b.fallback.clone(),
                };
                binding_payload(name, &spec)
            })
            .collect()
    }

    /// The forbidden operation, made explicit and testable: a runtime slot
    /// swap is always rejected (ADR-006). Cache-hostile model switching
    /// must go through [`SessionSlots::migrate`].
    pub fn swap(&mut self, slot: &SlotName, _spec: SlotSpec) -> Result<(), SlotError> {
        self.binding(slot)?;
        Err(SlotError::SwapForbidden {
            slot: slot.0.clone(),
        })
    }

    /// The only escape hatch: an explicit migration. Returns the record
    /// whose `payload` the caller must persist as a `slot/migration`
    /// event; only then is the new binding durable.
    pub fn migrate(
        &mut self,
        slot: &SlotName,
        spec: SlotSpec,
        reason: &str,
    ) -> Result<SlotMigration, SlotError> {
        let binding = self
            .bindings
            .get_mut(slot)
            .ok_or_else(|| SlotError::UnknownSlot(slot.0.clone()))?;
        let from = SlotSpec {
            provider: binding.target.provider.clone(),
            model: binding.target.model.clone(),
            fallback: binding.fallback.clone(),
        };
        let payload = migration_payload(slot, &from, &spec, reason);
        *binding = SlotBinding::from_spec(&spec);
        binding.migrated = true;
        Ok(SlotMigration {
            slot: slot.clone(),
            from,
            to: spec,
            reason: reason.to_string(),
            payload,
        })
    }

    /// Rebuild bindings purely from a session's event stream (constraint
    /// 1: the event log is the sole truth; the live object is a
    /// projection). `slot/fallback` events are per-turn traversal records
    /// and do not affect the binding.
    pub fn replay(session: SessionId, events: &[Event]) -> Result<Self, SlotError> {
        let mut bindings: BTreeMap<SlotName, SlotBinding> = BTreeMap::new();
        for ev in events {
            match ev.kind {
                EventKind::SlotBinding => {
                    let name = SlotName(
                        ev.bytes
                            .get("slot")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                SlotError::Replay(format!("slot/binding seq {} lacks slot", ev.seq))
                            })?
                            .to_string(),
                    );
                    let spec = spec_at(&ev.bytes, "spec").ok_or_else(|| {
                        SlotError::Replay(format!(
                            "malformed slot/binding payload at seq {}",
                            ev.seq
                        ))
                    })?;
                    if bindings.contains_key(&name) {
                        return Err(SlotError::Replay(format!(
                            "second slot/binding for `{}` at seq {} without an intervening slot/migration",
                            name.0, ev.seq
                        )));
                    }
                    bindings.insert(name, SlotBinding::from_spec(&spec));
                }
                EventKind::SlotMigration => {
                    let name = SlotName(
                        ev.bytes
                            .get("slot")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                SlotError::Replay(format!(
                                    "slot/migration seq {} lacks slot",
                                    ev.seq
                                ))
                            })?
                            .to_string(),
                    );
                    let spec = spec_at(&ev.bytes, "to_spec").ok_or_else(|| {
                        SlotError::Replay(format!(
                            "malformed slot/migration payload at seq {}",
                            ev.seq
                        ))
                    })?;
                    let binding = bindings.get_mut(&name).ok_or_else(|| {
                        SlotError::Replay(format!(
                            "slot/migration for `{}` at seq {} without a slot/binding",
                            name.0, ev.seq
                        ))
                    })?;
                    *binding = SlotBinding::from_spec(&spec);
                    binding.migrated = true;
                }
                _ => {}
            }
        }
        Ok(Self { session, bindings })
    }
}

// ---------------------------------------------------------------------------
// Fallback traversal
// ---------------------------------------------------------------------------

/// One fallback switch: durable `slot/fallback` payload + the targets.
#[derive(Debug, Clone, PartialEq)]
pub struct FallbackSwitch {
    pub from: SlotTarget,
    pub to: SlotTarget,
    /// Persist this as a `slot/fallback` event *before* issuing the
    /// request to `to` (one event per switch, ADR-006).
    pub payload: Value,
}

/// Per-turn fallback cursor over `[primary, fallback…]`. A fresh cursor
/// starts at the primary every turn — the *binding* stays fixed; only a
/// failing turn walks the chain.
pub struct FallbackCursor {
    slot: SlotName,
    chain: Vec<SlotTarget>,
    index: usize,
}

impl FallbackCursor {
    pub fn new(slot: SlotName, binding: &SlotBinding) -> Self {
        let mut chain = Vec::with_capacity(binding.fallback.len() + 1);
        chain.push(binding.target.clone());
        chain.extend(binding.fallback.iter().cloned());
        Self {
            slot,
            chain,
            index: 0,
        }
    }

    pub fn current(&self) -> &SlotTarget {
        &self.chain[self.index]
    }

    /// How many fallback switches remain after the current target.
    pub fn remaining_switches(&self) -> usize {
        self.chain.len() - 1 - self.index
    }

    /// Advance one link; `None` when the chain is exhausted.
    pub fn advance(&mut self) -> Option<FallbackSwitch> {
        let next = self.chain.get(self.index + 1)?.clone();
        let from = self.chain[self.index].clone();
        self.index += 1;
        Some(FallbackSwitch {
            payload: fallback_payload(&self.slot, &from, &next),
            from,
            to: next,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use kian_core::events::{Event, EventKind};
    use kian_core::model::{AgentDef, Budgets, Epoch, SessionId, SlotName, SlotSpec, SlotTarget};
    use serde_json::json;

    use super::{
        FallbackCursor, SessionSlots, SlotError, SlotResolver, binding_payload, fallback_payload,
        migration_payload, spec_from_binding_payload,
    };

    fn target(provider: &str, model: &str) -> SlotTarget {
        SlotTarget {
            provider: provider.into(),
            model: model.into(),
        }
    }

    fn spec(provider: &str, model: &str, fallback: &[SlotTarget]) -> SlotSpec {
        SlotSpec {
            provider: provider.into(),
            model: model.into(),
            fallback: fallback.to_vec(),
        }
    }

    fn agent_def() -> AgentDef {
        AgentDef {
            id: "researcher".into(),
            version: 1,
            model_slots: BTreeMap::from([(
                SlotName("main".into()),
                spec(
                    "primary",
                    "model-a",
                    &[target("backup", "model-b"), target("backup2", "model-c")],
                ),
            )]),
            tools: vec![],
            budgets: Budgets {
                tokens: None,
                wall_clock_ms: None,
            },
        }
    }

    fn event(seq: u64, session: SessionId, kind: EventKind, bytes: serde_json::Value) -> Event {
        Event {
            seq,
            session,
            epoch: Epoch(1),
            ts: 1_761_900_000_000 + seq,
            kind,
            bytes,
            version: 0,
        }
    }

    #[test]
    fn binding_is_fixed_swap_forbidden_migration_succeeds() {
        let resolver = SlotResolver::from_agent_def(&agent_def());
        let session = SessionId::new();
        let mut slots = resolver.bind_session(session);

        let main = SlotName("main".into());
        assert_eq!(slots.target(&main).unwrap(), target("primary", "model-a"));

        // Runtime swap is explicitly forbidden (ADR-006) — cache-hostile.
        let err = slots
            .swap(&main, spec("other", "model-x", &[]))
            .unwrap_err();
        assert_eq!(
            err,
            SlotError::SwapForbidden {
                slot: "main".into()
            },
            "swap must fail with an explicit error"
        );
        // The failed swap changed nothing.
        assert_eq!(slots.target(&main).unwrap(), target("primary", "model-a"));

        // The migration event is the only escape hatch.
        let migration = slots
            .migrate(
                &main,
                spec("other", "model-x", &[target("backup", "model-b")]),
                "cost",
            )
            .unwrap();
        assert_eq!(
            migration.from,
            spec(
                "primary",
                "model-a",
                &[target("backup", "model-b"), target("backup2", "model-c")]
            )
        );
        assert_eq!(migration.to.model, "model-x");
        assert_eq!(migration.payload["to_spec"]["model"], json!("model-x"));
        assert_eq!(migration.payload["reason"], json!("cost"));
        assert_eq!(slots.target(&main).unwrap(), target("other", "model-x"));

        // Unknown slot is a named error, not a panic.
        assert_eq!(
            slots.target(&SlotName("nope".into())).unwrap_err(),
            SlotError::UnknownSlot("nope".into())
        );
        assert_eq!(
            resolver.spec(&SlotName("nope".into())).unwrap_err(),
            SlotError::UnknownSlot("nope".into())
        );
    }

    #[test]
    fn binding_payload_round_trips() {
        let s = spec("primary", "model-a", &[target("backup", "model-b")]);
        let payload = binding_payload(&SlotName("main".into()), &s);
        assert_eq!(payload["slot"], json!("main"));
        assert_eq!(spec_from_binding_payload(&payload).unwrap(), s);
        assert!(spec_from_binding_payload(&json!({"slot": "main"})).is_none());
    }

    /// Bindings are projections of `slot/binding` + `slot/migration`
    /// events: a replayed stream rebuilds the identical slot state
    /// (constraint 1 — no authoritative state outside the event log).
    #[test]
    fn replay_rebuilds_bindings_including_migration() {
        let resolver = SlotResolver::from_agent_def(&agent_def());
        let session = SessionId::new();
        let mut slots = resolver.bind_session(session);
        let main = SlotName("main".into());

        let mut events: Vec<Event> = slots
            .binding_payloads()
            .into_iter()
            .enumerate()
            .map(|(i, bytes)| event(i as u64 + 1, session, EventKind::SlotBinding, bytes))
            .collect();

        let replayed = SessionSlots::replay(session, &events).unwrap();
        assert_eq!(
            replayed.target(&main).unwrap(),
            slots.target(&main).unwrap()
        );

        // Migrate live, persist the event, replay again — states agree.
        let migration = slots
            .migrate(&main, spec("other", "model-x", &[]), "cost")
            .unwrap();
        events.push(event(
            events.len() as u64 + 1,
            session,
            EventKind::SlotMigration,
            migration.payload,
        ));
        let replayed = SessionSlots::replay(session, &events).unwrap();
        assert_eq!(replayed.target(&main).unwrap(), target("other", "model-x"));
        // The migration is visible on the binding.
        assert!(replayed.binding(&main).unwrap().migrated);

        // A second binding for an already-bound slot without an
        // intervening migration is a replay error.
        let mut dupe = events.clone();
        dupe.push(event(
            dupe.len() as u64 + 1,
            session,
            EventKind::SlotBinding,
            binding_payload(&main, &spec("x", "y", &[])),
        ));
        assert!(matches!(
            SessionSlots::replay(session, &dupe).unwrap_err(),
            SlotError::Replay(_)
        ));
    }

    #[test]
    fn fallback_cursor_advances_once_per_switch_with_event_payload() {
        let resolver = SlotResolver::from_agent_def(&agent_def());
        let slots = resolver.bind_session(SessionId::new());
        let main = SlotName("main".into());
        let mut cursor = FallbackCursor::new(main.clone(), slots.binding(&main).unwrap());

        assert_eq!(cursor.current(), &target("primary", "model-a"));
        assert_eq!(cursor.remaining_switches(), 2);

        let sw1 = cursor.advance().unwrap();
        assert_eq!(
            (sw1.from.clone(), sw1.to.clone()),
            (target("primary", "model-a"), target("backup", "model-b"))
        );
        assert_eq!(
            sw1.payload,
            fallback_payload(
                &main,
                &target("primary", "model-a"),
                &target("backup", "model-b")
            )
        );
        assert_eq!(cursor.current(), &target("backup", "model-b"));

        let sw2 = cursor.advance().unwrap();
        assert_eq!(sw2.to, target("backup2", "model-c"));
        assert!(cursor.advance().is_none(), "chain exhausted");
        assert_eq!(cursor.remaining_switches(), 0);
    }

    #[test]
    fn migration_payload_round_trips() {
        let from = spec("a", "m-a", &[]);
        let to = spec("b", "m-b", &[target("c", "m-c")]);
        let payload = migration_payload(&SlotName("main".into()), &from, &to, "cost");
        assert_eq!(payload["slot"], json!("main"));
        assert_eq!(payload["reason"], json!("cost"));
        assert_eq!(payload["from_spec"]["model"], json!("m-a"));
        assert_eq!(payload["to_spec"]["fallback"][0]["model"], json!("m-c"));
    }
}
