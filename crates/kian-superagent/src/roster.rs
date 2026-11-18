//! The roster model (MULTIAGENT-MODEL §3.1, §7.1): the durable,
//! auditable answer to "which agent" — config, not prompt
//! improvisation.
//!
//! Pure by construction (no I/O, no store, no new dependency — pin
//! P2-3): the model, the fail-closed validation over an injected
//! agent-surface lookup (the kian-cli side passes
//! `ServeConfig::named_agent_def`, pin P2-1), the closed JSON-Schema
//! subset for `output_schema` (pin P2-3), and the `list_agents`
//! result rendering. The roster reaches the model only through a
//! `list_agents` tool **result** — never a prompt block, a tool
//! description or an enum (M9, the I6 lesson).
//!
//! Feature-off ([anchors.md rule 4]): an absent/empty roster is
//! `None` at the Router surface — no `list_agents` tool, no
//! validation, today's exact bytes (pin P2-6).

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::multiagent::DEFAULT_MAX_CHILDREN;

// ---------------------------------------------------------------------------
// The entry model (§3.1:357-362)
// ---------------------------------------------------------------------------

/// The mandate's strength (ADR-012): `explicit` only ever comes from
/// an adapter-declared signal; everything a config file can spell is
/// `inferred`. The closed vocabulary keeps a typo a boot refusal
/// (serde), never a silent default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MandateStrength {
    Explicit,
    Inferred,
}

/// The entry's mandate template (§3.1:359): the **advisory** scope
/// text the entry carries (§5.1 — recorded and audited, never a
/// permission) plus its strength. The enforced boundary is the tool
/// surface (`tools_allow`), not this text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MandateTemplate {
    /// The advisory scope text (ADR-012 `scope`). Never blank — an
    /// empty authority string is never issued.
    pub scope: String,
    pub strength: MandateStrength,
}

/// The entry's limits — `MultiagentLimits`-shaped but carrying only
/// the per-entry bound the contract defines (§3.1:360-361):
/// `max_children: 0` makes the entry a **leaf** (a legal, explicit
/// policy, not a silently-disabled spawn surface).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RosterLimits {
    pub max_children: u32,
}

/// One roster entry (§3.1:357-362), field-for-field:
/// `{id, description, agent, mandate_template, tools_allow, limits,
/// output_schema?}`.
///
/// - `id` — the name the model uses; unique, non-empty.
/// - `description` — the one line used for selection; ≤200 chars, no
///   control characters (the `WorkerEntry` precedent).
/// - `agent` — an `AgentDef` reference and the memory scope; resolved
///   by the caller through `ServeConfig::named_agent_def` (pin P2-1),
///   never the default `agent:`.
/// - `tools_allow` — intersected with the parent's surface, **never a
///   widening**: an entry listing a tool the AgentDef lacks refuses
///   boot (M3, §5.1:551-552).
/// - `output_schema` — the optional default expectation for this
///   entry's children (§3.2.1); must sit inside the closed subset
///   below (pin P2-3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RosterEntry {
    pub id: String,
    pub description: String,
    pub agent: String,
    pub mandate_template: MandateTemplate,
    pub tools_allow: Vec<String>,
    pub limits: RosterLimits,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
    /// (R17-C2, pin P2-1) the resolved `AgentDef` identity this entry's
    /// `agent` names — stamped by the config conversion through
    /// `ServeConfig::named_agent_def` (never the default `agent:`),
    /// so the spawn-side resolution reads a recorded fact instead of
    /// re-consulting live config (§2.1.1 constraint 13). Serde-skipped:
    /// it is a conversion product, not a config field — the wire form
    /// stays exactly §3.1's field set. An entry without it (a
    /// programmatically built roster) is a spawn-time fail-closed
    /// refusal, never a silent fallback onto the worker template.
    #[serde(skip)]
    pub resolved_agent: Option<ResolvedAgent>,
}

/// The recorded resolution of one roster entry's `agent` reference
/// (pin P2-1): the `AgentDef` id + version that ride a spawn's wrapper
/// as `agent_def` / `agent_def_version` once the entry is selected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedAgent {
    /// The resolved `AgentDef.id` (an `agents:` entry id).
    pub def_id: String,
    /// The resolved `AgentDef.version` at conversion time.
    pub version: u64,
}

impl RosterEntry {
    /// The `list_agents` result row (M9): the selection facts only —
    /// `id`, `description`, `agent`, `tools_allow`, `max_children`.
    /// No mandate text, no schema, no live state: the row answers
    /// "who can I ask", not "what happened".
    #[must_use]
    pub fn list_agents_row(&self) -> Value {
        serde_json::json!({
            "id": self.id,
            "description": self.description,
            "agent": self.agent,
            "tools_allow": self.tools_allow,
            "max_children": self.limits.max_children,
        })
    }
}

/// The configured roster (config order preserved — the order is
/// deterministic and `list_agents` renders it verbatim).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Roster {
    pub entries: Vec<RosterEntry>,
}

impl Roster {
    /// An empty roster — the feature-off shape (pin P2-6).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Resolves `id` to its entry — the P2-6 spawn-side resolution
    /// seam (an unknown id is the caller's honest refusal, never a
    /// fallback).
    pub fn resolve(&self, id: &str) -> Result<&RosterEntry, RosterError> {
        self.entries
            .iter()
            .find(|entry| entry.id == id)
            .ok_or_else(|| RosterError::UnknownEntry {
                id: id.to_string(),
                known: self.known_ids(),
            })
    }

    /// The known entry ids, config order — the refusal text half.
    #[must_use]
    pub fn known_ids(&self) -> Vec<String> {
        self.entries.iter().map(|entry| entry.id.clone()).collect()
    }

    /// The `list_agents` result (§3.1, M9): `{"agents":[…]}` in
    /// config order, each row the selection facts only.
    #[must_use]
    pub fn list_agents_result(&self) -> Value {
        serde_json::json!({
            "agents": self.entries.iter().map(RosterEntry::list_agents_row)
                .collect::<Vec<_>>(),
        })
    }

    /// Fail-closed validation over an injected agent surface (pin
    /// P2-1: the kian-cli side passes `ServeConfig::named_agent_def`):
    ///
    /// - `id` unique and non-empty;
    /// - `description` one line, ≤200 chars, no control characters;
    /// - `agent` resolves through `lookup`, else the refusal names the
    ///   known `agents:` ids (`known_agent_ids`, config order);
    /// - `tools_allow` ⊆ the AgentDef's `tools` — a **widening
    ///   refuses boot** (M3, §5.1:551-552);
    /// - `output_schema` inside the closed subset (pin P2-3);
    /// - `limits.max_children == 0` is **legal** — a leaf
    ///   (§3.1:360-361).
    ///
    /// Pure: no I/O, no clock, no store.
    pub fn validate(
        &self,
        lookup: &dyn Fn(&str) -> Option<AgentSurface>,
        known_agent_ids: &[String],
    ) -> Result<(), RosterError> {
        let mut seen: Vec<&str> = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            if entry.id.is_empty() {
                return Err(RosterError::EmptyId);
            }
            if seen.contains(&entry.id.as_str()) {
                return Err(RosterError::DuplicateId {
                    id: entry.id.clone(),
                });
            }
            seen.push(entry.id.as_str());
            validate_description(&entry.id, &entry.description)?;
            if entry.mandate_template.scope.trim().is_empty() {
                return Err(RosterError::BlankMandateScope {
                    id: entry.id.clone(),
                });
            }
            let Some(surface) = lookup(&entry.agent) else {
                return Err(RosterError::UnknownAgent {
                    id: entry.id.clone(),
                    agent: entry.agent.clone(),
                    known: known_agent_ids.to_vec(),
                });
            };
            for tool in &entry.tools_allow {
                if !surface.tools.iter().any(|allowed| allowed == tool) {
                    return Err(RosterError::ToolWidening {
                        id: entry.id.clone(),
                        agent: entry.agent.clone(),
                        tool: tool.clone(),
                    });
                }
            }
            if let Some(schema) = &entry.output_schema {
                validate_output_schema(schema).map_err(|error| RosterError::Schema {
                    id: entry.id.clone(),
                    error,
                })?;
            }
            // max_children == 0 is legal — the leaf idiom (§3.1).
        }
        Ok(())
    }
}

/// The resolved agent surface [`Roster::validate`] needs: the
/// `AgentDef` id plus its `tools` (the fence `tools_allow` is checked
/// against). Owned so the injected lookup can come from anywhere
/// (config, tests) without borrowing the config tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSurface {
    pub id: String,
    pub tools: Vec<String>,
}

/// Validates one entry `description` (the `WorkerEntry` precedent,
/// `router.rs:127-140`): required, one line, ≤200 chars, no control
/// characters — it renders into a `list_agents` result.
fn validate_description(id: &str, description: &str) -> Result<(), RosterError> {
    if description.is_empty() {
        return Err(RosterError::Description {
            id: id.to_string(),
            reason: "empty — the one line used for selection is required".to_string(),
        });
    }
    if description.chars().count() > 200 {
        return Err(RosterError::Description {
            id: id.to_string(),
            reason: format!(
                "{} chars — over the 200-char bound (the `WorkerEntry` precedent)",
                description.chars().count()
            ),
        });
    }
    if description.chars().any(|c| c.is_control()) {
        return Err(RosterError::Description {
            id: id.to_string(),
            reason: "control characters — the description is one line".to_string(),
        });
    }
    Ok(())
}

/// Fail-closed roster validation errors. Every message names the entry
/// (and, where relevant, the offending key/value) — the config layer
/// prefixes `superagent.roster[i]` for the boot refusal text.
#[derive(Debug, Clone, PartialEq)]
pub enum RosterError {
    EmptyId,
    DuplicateId {
        id: String,
    },
    Description {
        id: String,
        reason: String,
    },
    BlankMandateScope {
        id: String,
    },
    UnknownAgent {
        id: String,
        agent: String,
        known: Vec<String>,
    },
    ToolWidening {
        id: String,
        agent: String,
        tool: String,
    },
    Schema {
        id: String,
        error: SchemaError,
    },
    UnknownEntry {
        id: String,
        known: Vec<String>,
    },
}

impl fmt::Display for RosterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyId => write!(
                f,
                "an entry has an empty id — roster ids are the name the model uses"
            ),
            Self::DuplicateId { id } => {
                write!(f, "duplicate roster id `{id}` — ids are unique")
            }
            Self::Description { id, reason } => {
                write!(f, "roster entry `{id}` description is invalid: {reason}")
            }
            Self::BlankMandateScope { id } => write!(
                f,
                "roster entry `{id}` mandate_template.scope is blank — an empty \
                 authority string is never issued"
            ),
            Self::UnknownAgent { id, agent, known } => write!(
                f,
                "roster entry `{id}` names agent `{agent}`, which is no configured \
                 `agents:` entry (known: {})",
                if known.is_empty() {
                    "none".to_string()
                } else {
                    known.join(", ")
                }
            ),
            Self::ToolWidening { id, agent, tool } => write!(
                f,
                "roster entry `{id}` tools_allow lists `{tool}`, which agent \
                 `{agent}` does not have — tools_allow is a narrowing, never a \
                 widening (the fence cannot be configured open)"
            ),
            Self::Schema { id, error } => {
                write!(f, "roster entry `{id}` output_schema: {error}")
            }
            Self::UnknownEntry { id, known } => {
                write!(f, "unknown roster id `{id}` (known: {})", known.join(", "))
            }
        }
    }
}

impl std::error::Error for RosterError {}

// ---------------------------------------------------------------------------
// The output_schema subset (pin P2-3) — closed, in-crate, no dependency
// ---------------------------------------------------------------------------

/// The closed keyword subset `output_schema` supports (pin P2-3).
/// Anything outside it is a refusal naming the keyword — draft-07
/// keywords outside the subset are common and silent, which is exactly
/// why the refusal must name them.
///
/// (The card body says `[&str; 15]`; pin P2-3's closed list counts
/// exactly 13 — the pin is the decision record and widening it by two
/// unnamed keywords is not this card's to take. Deviation recorded.)
pub const SUPPORTED_SCHEMA_KEYWORDS: [&str; 13] = [
    "type",
    "properties",
    "required",
    "additionalProperties",
    "items",
    "enum",
    "const",
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
    "minItems",
    "maxItems",
];

/// The `type` vocabulary (pin P2-3).
pub const SUPPORTED_TYPE_NAMES: [&str; 7] = [
    "object", "array", "string", "number", "integer", "boolean", "null",
];

/// The `output_schema` size bound (pin P2-3): 8192 bytes of canonical
/// JSON.
pub const OUTPUT_SCHEMA_MAX_BYTES: usize = 8 * 1024;

/// `output_schema` validation failures. Every message names the
/// offending keyword (`UnknownKeyword`) or the malformed shape.
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaError {
    UnknownKeyword { keyword: String, at: String },
    Malformed { at: String, detail: String },
    Oversized { bytes: usize },
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownKeyword { keyword, at } => write!(
                f,
                "keyword `{keyword}` at {at} is outside the supported subset \
                 (supported: {})",
                SUPPORTED_SCHEMA_KEYWORDS.join(", ")
            ),
            Self::Malformed { at, detail } => write!(f, "malformed schema at {at}: {detail}"),
            Self::Oversized { bytes } => write!(
                f,
                "canonical JSON is {bytes} bytes — over the {OUTPUT_SCHEMA_MAX_BYTES}-byte bound"
            ),
        }
    }
}

impl std::error::Error for SchemaError {}

/// The full `output_schema` gate (pin P2-3): the canonical-JSON size
/// bound plus the closed-subset check. A malformed schema is a
/// refusal before any side effect (§5.3:589).
pub fn validate_output_schema(schema: &Value) -> Result<(), SchemaError> {
    let canonical = serde_json::to_string(schema).map_err(|e| SchemaError::Malformed {
        at: "<root>".to_string(),
        detail: format!("cannot serialize canonically: {e}"),
    })?;
    if canonical.len() > OUTPUT_SCHEMA_MAX_BYTES {
        return Err(SchemaError::Oversized {
            bytes: canonical.len(),
        });
    }
    validate_schema(schema)
}

/// Validates that `schema` sits inside the closed subset (pin P2-3),
/// recursively (`properties` values, `items`). Pure.
pub fn validate_schema(schema: &Value) -> Result<(), SchemaError> {
    validate_schema_at(schema, "<root>")
}

fn validate_schema_at(schema: &Value, at: &str) -> Result<(), SchemaError> {
    let Some(object) = schema.as_object() else {
        return Err(SchemaError::Malformed {
            at: at.to_string(),
            detail: "a schema is a JSON object".to_string(),
        });
    };
    for (keyword, value) in object {
        if !SUPPORTED_SCHEMA_KEYWORDS.contains(&keyword.as_str()) {
            return Err(SchemaError::UnknownKeyword {
                keyword: keyword.clone(),
                at: at.to_string(),
            });
        }
        let malformed = |detail: &str| SchemaError::Malformed {
            at: format!("{at}.{keyword}"),
            detail: detail.to_string(),
        };
        match keyword.as_str() {
            "type" => {
                let Some(name) = value.as_str() else {
                    return Err(malformed("type is one of the seven type names"));
                };
                if !SUPPORTED_TYPE_NAMES.contains(&name) {
                    return Err(malformed(&format!(
                        "unknown type `{name}` (supported: {})",
                        SUPPORTED_TYPE_NAMES.join(", ")
                    )));
                }
            }
            "properties" => {
                let Some(properties) = value.as_object() else {
                    return Err(malformed("properties is an object"));
                };
                for (name, subschema) in properties {
                    validate_schema_at(subschema, &format!("{at}.{keyword}.{name}"))?;
                }
            }
            "required" => {
                let Some(list) = value.as_array() else {
                    return Err(malformed("required is an array of property names"));
                };
                if list.iter().any(|name| !name.is_string()) {
                    return Err(malformed("required entries are strings"));
                }
            }
            "additionalProperties" => {
                if !value.is_boolean() {
                    return Err(malformed("additionalProperties is a bool"));
                }
            }
            "items" => {
                validate_schema_at(value, &format!("{at}.{keyword}"))?;
            }
            "enum" => {
                let Some(list) = value.as_array() else {
                    return Err(malformed("enum is an array"));
                };
                if list.is_empty() {
                    return Err(malformed(
                        "enum is non-empty (an empty enum matches nothing)",
                    ));
                }
            }
            "const" => {}
            "minLength" | "maxLength" | "minItems" | "maxItems" => {
                if value.as_u64().is_none() {
                    return Err(malformed("the bound is a non-negative integer"));
                }
            }
            "minimum" | "maximum" => {
                if value.as_f64().is_none() {
                    return Err(malformed("the bound is a number"));
                }
            }
            _ => unreachable!("the membership check above is exhaustive"),
        }
    }
    Ok(())
}

/// One keyword check on an instance (the schema-gate evidence unit,
/// C4's `checks` rows): which keyword, whether it passed, and an
/// honest detail line when it did not.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CheckOutcome {
    pub keyword: String,
    pub passed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl CheckOutcome {
    fn pass(keyword: &str) -> Self {
        Self {
            keyword: keyword.to_string(),
            passed: true,
            detail: None,
        }
    }

    fn fail(keyword: &str, detail: String) -> Self {
        Self {
            keyword: keyword.to_string(),
            passed: false,
            detail: Some(detail),
        }
    }
}

/// Validates `value` against `schema` (already subset-validated):
/// one [`CheckOutcome`] per keyword the schema carries, recursing
/// into `properties` members and `items` elements. Pure — the caller
/// (the child's schema gate, C4) filters `!passed` for the verdict.
pub fn validate_instance(schema: &Value, value: &Value) -> Vec<CheckOutcome> {
    let mut outcomes = Vec::new();
    validate_instance_into(schema, value, &mut outcomes);
    outcomes
}

/// The JSON type name of `value` in the subset's vocabulary
/// (`integer` = a number with zero fractional part, draft-07).
fn json_type_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() || n.as_f64().is_some_and(|f| f.fract() == 0.0) {
                "integer"
            } else {
                "number"
            }
        }
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn validate_instance_into(schema: &Value, value: &Value, outcomes: &mut Vec<CheckOutcome>) {
    let Some(object) = schema.as_object() else {
        // An already-validated schema is an object; anything else is a
        // malformed-schema failure, never a guess.
        outcomes.push(CheckOutcome::fail(
            "<schema>",
            "the schema is not a JSON object".to_string(),
        ));
        return;
    };
    for (keyword, bound) in object {
        match keyword.as_str() {
            "type" => {
                let expected = bound.as_str().unwrap_or_default();
                let actual = json_type_of(value);
                // draft-07: `integer` accepts `number`-typed integral
                // values (already folded into `json_type_of`), and
                // `number` accepts integers.
                let ok = actual == expected
                    || (expected == "number" && matches!(actual, "integer" | "number"));
                outcomes.push(if ok {
                    CheckOutcome::pass("type")
                } else {
                    CheckOutcome::fail(
                        "type",
                        format!("expected {expected}, the value is {actual}"),
                    )
                });
            }
            "properties" => {
                if let (Some(properties), Some(instance)) = (bound.as_object(), value.as_object()) {
                    for (name, subschema) in properties {
                        if let Some(member) = instance.get(name) {
                            validate_instance_into(subschema, member, outcomes);
                        }
                    }
                }
                // A non-object instance is the `type` keyword's failure;
                // `properties` alone says nothing about it.
            }
            "required" => {
                let missing: Vec<&str> = bound
                    .as_array()
                    .map(|required| {
                        required
                            .iter()
                            .filter_map(|name| name.as_str())
                            .filter(|name| value.get(name).is_none())
                            .collect()
                    })
                    .unwrap_or_default();
                outcomes.push(if missing.is_empty() {
                    CheckOutcome::pass("required")
                } else {
                    CheckOutcome::fail("required", format!("missing: {}", missing.join(", ")))
                });
            }
            "additionalProperties" => {
                if bound.as_bool() == Some(false)
                    && let (Some(properties), Some(instance)) = (
                        object.get("properties").and_then(Value::as_object),
                        value.as_object(),
                    )
                {
                    let extra: Vec<&str> = instance
                        .keys()
                        .filter(|key| !properties.contains_key(key.as_str()))
                        .map(String::as_str)
                        .collect();
                    outcomes.push(if extra.is_empty() {
                        CheckOutcome::pass("additionalProperties")
                    } else {
                        CheckOutcome::fail(
                            "additionalProperties",
                            format!("unexpected: {}", extra.join(", ")),
                        )
                    });
                }
            }
            "items" => {
                if let Some(elements) = value.as_array() {
                    for element in elements {
                        validate_instance_into(bound, element, outcomes);
                    }
                }
            }
            "enum" => {
                let ok = bound.as_array().is_some_and(|candidates| {
                    candidates.iter().any(|candidate| candidate == value)
                });
                outcomes.push(if ok {
                    CheckOutcome::pass("enum")
                } else {
                    CheckOutcome::fail("enum", "the value matches no enum member".to_string())
                });
            }
            "const" => {
                outcomes.push(if bound == value {
                    CheckOutcome::pass("const")
                } else {
                    CheckOutcome::fail("const", "the value differs from the const".to_string())
                });
            }
            "minLength" => {
                if let Some(text) = value.as_str() {
                    let min = bound.as_u64().unwrap_or(0) as usize;
                    outcomes.push(if text.chars().count() >= min {
                        CheckOutcome::pass("minLength")
                    } else {
                        CheckOutcome::fail(
                            "minLength",
                            format!("{} chars, under {min}", text.chars().count()),
                        )
                    });
                }
            }
            "maxLength" => {
                if let Some(text) = value.as_str() {
                    let max = bound.as_u64().unwrap_or(0) as usize;
                    outcomes.push(if text.chars().count() <= max {
                        CheckOutcome::pass("maxLength")
                    } else {
                        CheckOutcome::fail(
                            "maxLength",
                            format!("{} chars, over {max}", text.chars().count()),
                        )
                    });
                }
            }
            "minimum" => {
                if let Some(number) = value.as_f64() {
                    let min = bound.as_f64().unwrap_or(f64::NAN);
                    outcomes.push(if number >= min {
                        CheckOutcome::pass("minimum")
                    } else {
                        CheckOutcome::fail("minimum", format!("{number} under {min}"))
                    });
                }
            }
            "maximum" => {
                if let Some(number) = value.as_f64() {
                    let max = bound.as_f64().unwrap_or(f64::NAN);
                    outcomes.push(if number <= max {
                        CheckOutcome::pass("maximum")
                    } else {
                        CheckOutcome::fail("maximum", format!("{number} over {max}"))
                    });
                }
            }
            "minItems" => {
                if let Some(items) = value.as_array() {
                    let min = bound.as_u64().unwrap_or(0) as usize;
                    outcomes.push(if items.len() >= min {
                        CheckOutcome::pass("minItems")
                    } else {
                        CheckOutcome::fail(
                            "minItems",
                            format!("{} items, under {min}", items.len()),
                        )
                    });
                }
            }
            "maxItems" => {
                if let Some(items) = value.as_array() {
                    let max = bound.as_u64().unwrap_or(0) as usize;
                    outcomes.push(if items.len() <= max {
                        CheckOutcome::pass("maxItems")
                    } else {
                        CheckOutcome::fail("maxItems", format!("{} items, over {max}", items.len()))
                    });
                }
            }
            _ => {}
        }
    }
}

/// The default per-entry `limits.max_children` when the wire form
/// omits it — the §5.3 global default, mirroring
/// `MultiagentConfigDef`'s optional-with-defaults shape.
#[must_use]
pub fn default_roster_max_children() -> u32 {
    DEFAULT_MAX_CHILDREN
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, agent: &str, tools: &[&str]) -> RosterEntry {
        RosterEntry {
            id: id.to_string(),
            description: format!("{id} does one thing well"),
            agent: agent.to_string(),
            mandate_template: MandateTemplate {
                scope: "advisory scope text".to_string(),
                strength: MandateStrength::Inferred,
            },
            tools_allow: tools.iter().map(|t| t.to_string()).collect(),
            limits: RosterLimits { max_children: 0 },
            output_schema: None,
            resolved_agent: None,
        }
    }

    fn surface(tools: &[&str]) -> AgentSurface {
        AgentSurface {
            id: "surf".to_string(),
            tools: tools.iter().map(|t| t.to_string()).collect(),
        }
    }

    #[test]
    fn list_agents_result_renders_config_order_and_selection_facts() {
        let roster = Roster {
            entries: vec![
                entry("researcher", "research", &["fs_read"]),
                entry("coder", "code", &["exec"]),
            ],
        };
        let result = roster.list_agents_result();
        let agents = result["agents"].as_array().expect("agents array");
        assert_eq!(agents.len(), 2);
        assert_eq!(agents[0]["id"], serde_json::json!("researcher"));
        assert_eq!(agents[1]["id"], serde_json::json!("coder"));
        for row in agents {
            let mut keys: Vec<&str> = row
                .as_object()
                .expect("row object")
                .keys()
                .map(String::as_str)
                .collect();
            keys.sort_unstable();
            assert_eq!(
                keys,
                vec!["agent", "description", "id", "max_children", "tools_allow"]
            );
        }
    }

    #[test]
    fn resolve_and_known_ids_round_trip() {
        let roster = Roster {
            entries: vec![
                entry("researcher", "research", &[]),
                entry("coder", "code", &[]),
            ],
        };
        assert_eq!(roster.known_ids(), vec!["researcher", "coder"]);
        assert!(roster.resolve("coder").is_ok());
        let err = roster.resolve("ghost").unwrap_err().to_string();
        assert!(err.contains("ghost") && err.contains("researcher") && err.contains("coder"));
    }

    #[test]
    fn an_integer_typed_number_and_a_plain_number_cross_accept() {
        use super::CheckOutcome;
        // draft-07: `number` accepts integral values.
        let schema = serde_json::json!({"type": "number"});
        assert!(
            validate_instance(&schema, &serde_json::json!(3))
                .iter()
                .all(|o: &CheckOutcome| o.passed)
        );
        let schema = serde_json::json!({"type": "integer"});
        assert!(
            validate_instance(&schema, &serde_json::json!(2.0))
                .iter()
                .all(|o: &CheckOutcome| o.passed)
        );
        assert!(
            validate_instance(&schema, &serde_json::json!(2.5))
                .iter()
                .any(|o| !o.passed)
        );
    }

    #[test]
    fn the_lookup_absence_is_the_only_unknown_agent_source() {
        let roster = Roster {
            entries: vec![entry("r", "research", &["fs_read"])],
        };
        let known = vec!["research".to_string(), "code".to_string()];
        let lookup = |id: &str| (id == "research").then(|| surface(&["fs_read"]));
        roster.validate(&lookup, &known).expect("valid");
        let bad = Roster {
            entries: vec![entry("r", "nope", &[])],
        };
        let err = bad.validate(&lookup, &known).unwrap_err().to_string();
        assert!(err.contains("nope") && err.contains("research") && err.contains("code"));
    }
}
