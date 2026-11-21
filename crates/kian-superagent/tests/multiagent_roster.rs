//! R17-C1 — the roster core's acceptance tests (MULTIAGENT-MODEL
//! §3.1, pins P2-1/P2-3/P2-6, M3, M9).
//!
//! The pure-model tests (round trip, validation fence, schema
//! subset) run against [`kian_superagent::roster`] directly; the
//! `list_agents` surface tests drive a real [`RouterTools`] through
//! the [`ToolProvider`] seam, exactly as the loop does. The
//! boot-refusal conversions over a real `ServeConfig` live in
//! kian-cli's config tests (`config.rs`, `roster_config`).

use std::sync::Arc;

use kian_core::model::SessionId;
use kian_core::seams::{ToolCall, ToolProvider};
use kian_orchestrator::WakeOutcome;
use kian_superagent::inbox::{AdmitReceipt, InboxWriteError, InboxWriter};
use kian_superagent::roster::{
    AgentSurface, MandateStrength, MandateTemplate, Roster, RosterEntry, RosterLimits,
    SUPPORTED_SCHEMA_KEYWORDS, validate_instance, validate_output_schema, validate_schema,
};
use kian_superagent::templates::{SPAWN_WORKER_TOOL, router_template, worker_template};
use kian_superagent::{
    ChannelRegistry, DELIVER_STRUCTURED_TOOL, LIST_AGENTS_TOOL, RosterError, RouterHold,
    RouterProfile, RouterTools, WorkerSpawner,
};
use serde_json::json;

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

/// A no-op inbox: `list_agents` (and schema mounting) must not touch
/// the inbox or the store, and the `WorkerSpawner` handle only needs
/// the seam to exist.
struct NullInbox;

impl InboxWriter for NullInbox {
    fn admit(
        &self,
        _session: SessionId,
        _payload: serde_json::Value,
        _idempotency_key: Option<String>,
    ) -> futures::future::BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        Box::pin(async {
            Ok(AdmitReceipt {
                deduplicated: false,
            })
        })
    }
    fn wake(&self, _session: SessionId) -> WakeOutcome {
        WakeOutcome::Folded
    }
}

fn sid(seed: u8) -> SessionId {
    SessionId(uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        &[b'r', b'o', b's', b't', b'e', b'r', seed],
    ))
}

/// A Router toolset with an optional roster installed (membership
/// off, multiagent off — the feature-off baseline everywhere else in
/// the surface is today's bytes).
fn router_tools(roster: Option<Roster>) -> Arc<RouterTools> {
    let router = sid(1);
    let inbox: Arc<dyn InboxWriter> = Arc::new(NullInbox);
    let spawner = Arc::new(WorkerSpawner::new(
        kian_superagent::ResidentBinding::new(router),
        &worker_template("mock", "mock-worker", 7),
        inbox,
    ));
    let profile = RouterProfile::new(router, router_template("mock", "mock-router"));
    let tools = RouterTools::new(
        spawner,
        ChannelRegistry::new(),
        &profile,
        RouterHold::shared(),
        kian_superagent::ResidentBinding::new(profile.session),
    );
    let tools = match roster {
        Some(roster) => tools.with_roster(roster),
        None => tools,
    };
    Arc::new(tools)
}

/// One valid entry over a fake agent surface (`lookup` below).
fn entry(id: &str, agent: &str, tools: &[&str]) -> RosterEntry {
    RosterEntry {
        id: id.to_string(),
        description: format!("The {id} does one thing well"),
        agent: agent.to_string(),
        mandate_template: MandateTemplate {
            scope: "advisory scope text".to_string(),
            strength: MandateStrength::Inferred,
        },
        tools_allow: tools.iter().map(|t| (*t).to_string()).collect(),
        limits: RosterLimits { max_children: 2 },
        output_schema: None,
        harness_bridged: false,
        resolved_agent: None,
    }
}

/// The agent-surface lookup every pure validation test uses: two
/// configured agents, `research` (fs_read, recall_event) and `code`
/// (exec, fs_read).
fn lookup() -> impl Fn(&str) -> Option<AgentSurface> {
    |id: &str| {
        match id {
            "research" => Some(&["fs_read", "recall_event"][..]),
            "code" => Some(&["exec", "fs_read"][..]),
            _ => None,
        }
        .map(|tools| AgentSurface {
            id: id.to_string(),
            tools: tools.iter().map(|t| (*t).to_string()).collect(),
        })
    }
}

fn known_agents() -> Vec<String> {
    vec!["research".to_string(), "code".to_string()]
}

/// A padding string over the 8192-byte canonical bound.
fn pad_over_bound() -> String {
    "x".repeat(9000)
}

// ---------------------------------------------------------------------------
// the entry model (§3.1:357-362)
// ---------------------------------------------------------------------------

#[test]
fn roster_entry_round_trips_with_the_contract_field_set() {
    let original = RosterEntry {
        id: "researcher".to_string(),
        description: "Finds and cites sources".to_string(),
        agent: "research".to_string(),
        mandate_template: MandateTemplate {
            scope: "read-only research".to_string(),
            strength: MandateStrength::Explicit,
        },
        tools_allow: vec!["fs_read".to_string()],
        limits: RosterLimits { max_children: 0 },
        output_schema: Some(json!({
            "type": "object",
            "required": ["summary"],
            "properties": {"summary": {"type": "string"}}
        })),
        harness_bridged: false,
        resolved_agent: None,
    };
    let wire = serde_json::to_value(&original).expect("serializes");
    // The field set is the contract's, field-for-field (§3.1:357):
    // seven keys, `output_schema` the only optional one.
    let mut keys: Vec<&str> = wire
        .as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "agent",
            "description",
            "id",
            "limits",
            "mandate_template",
            "output_schema",
            "tools_allow"
        ]
    );
    let mut template_keys: Vec<&str> = wire["mandate_template"]
        .as_object()
        .expect("template")
        .keys()
        .map(String::as_str)
        .collect();
    template_keys.sort_unstable();
    assert_eq!(template_keys, vec!["scope", "strength"]);
    let mut limits_keys: Vec<&str> = wire["limits"]
        .as_object()
        .expect("limits")
        .keys()
        .map(String::as_str)
        .collect();
    limits_keys.sort_unstable();
    assert_eq!(limits_keys, vec!["max_children"]);
    let back: RosterEntry = serde_json::from_value(wire).expect("deserializes");
    assert_eq!(back, original);
    // An absent output_schema round-trips too (skip_serializing_if).
    let bare = RosterEntry {
        output_schema: None,
        ..entry("r", "research", &["fs_read"])
    };
    let wire = serde_json::to_value(&bare).expect("serializes");
    assert!(wire.get("output_schema").is_none());
    assert_eq!(serde_json::from_value::<RosterEntry>(wire).unwrap(), bare);
}

#[test]
fn duplicate_entry_ids_refuse_boot() {
    let roster = Roster {
        entries: vec![
            entry("researcher", "research", &["fs_read"]),
            entry("researcher", "code", &["exec"]),
        ],
    };
    let err = roster
        .validate(&lookup(), &known_agents())
        .expect_err("duplicate ids refuse");
    assert!(
        matches!(err, RosterError::DuplicateId { ref id } if id == "researcher"),
        "{err}"
    );
    assert!(err.to_string().contains("researcher"));
}

#[test]
fn tools_allow_that_widens_the_agent_surface_refuses_boot() {
    // M3, §5.1:551-552: the fence cannot be configured open — an
    // entry listing a tool the AgentDef lacks is a boot refusal.
    let roster = Roster {
        entries: vec![entry("researcher", "research", &["fs_read", "exec"])],
    };
    let err = roster
        .validate(&lookup(), &known_agents())
        .expect_err("widening refuses");
    assert!(
        matches!(
            &err,
            RosterError::ToolWidening { id, agent, tool } if id == "researcher"
                && agent == "research" && tool == "exec"
        ),
        "{err}"
    );
    assert!(err.to_string().contains("exec"), "{err}");
    // The narrowing direction is fine: a subset of the surface, and
    // the empty subset (a tool-less leaf) is a legal narrowing too.
    Roster {
        entries: vec![entry("researcher", "research", &[])],
    }
    .validate(&lookup(), &known_agents())
    .expect("empty tools_allow is a narrowing, not a widening");
}

#[test]
fn limits_max_children_zero_marks_a_leaf_and_is_legal() {
    // §3.1:360-361: `limits.max_children: 0` names a leaf — an
    // explicit policy, never a silently-disabled surface.
    let leaf = RosterEntry {
        limits: RosterLimits { max_children: 0 },
        ..entry("researcher", "research", &["fs_read"])
    };
    let roster = Roster {
        entries: vec![leaf],
    };
    roster
        .validate(&lookup(), &known_agents())
        .expect("a leaf is legal");
    let resolved = roster.resolve("researcher").expect("resolves");
    assert_eq!(resolved.limits.max_children, 0);
}

#[test]
fn an_output_schema_outside_the_subset_refuses_boot_naming_the_keyword() {
    // Pin P2-3: any keyword outside the closed subset is a refusal
    // naming the keyword — never a silent ignore.
    let with_bad_schema = |schema: serde_json::Value| Roster {
        entries: vec![RosterEntry {
            output_schema: Some(schema),
            ..entry("researcher", "research", &["fs_read"])
        }],
    };
    let cases: [(&str, serde_json::Value); 4] = [
        (
            "oneOf",
            json!({"oneOf": [{"type": "string"}, {"type": "number"}]}),
        ),
        ("allOf", json!({"type": "object", "allOf": []})),
        ("$ref", json!({"$ref": "#/definitions/x"})),
        ("pattern", json!({"type": "string", "pattern": "^a"})),
    ];
    for (keyword, schema) in cases {
        let err = with_bad_schema(schema)
            .validate(&lookup(), &known_agents())
            .expect_err("outside the subset refuses");
        let text = err.to_string();
        assert!(text.contains(keyword), "names {keyword}: {text}");
        assert!(text.contains("output_schema"), "names the key: {text}");
    }
    // The RosterError path names the entry and carries the SchemaError.
    let err = with_bad_schema(json!({"oneOf": []}))
        .validate(&lookup(), &known_agents())
        .unwrap_err();
    assert!(
        matches!(err, RosterError::Schema { ref id, .. } if id == "researcher"),
        "{err}"
    );
}

#[test]
fn an_entry_naming_no_configured_agent_refuses_boot_naming_the_known_ids() {
    // Pin P2-1 (pure half; the ServeConfig half is kian-cli's config
    // test): discovery is the injected `named_agent_def` seam — an
    // unknown `agent` refuses, naming the known `agents:` ids.
    let roster = Roster {
        entries: vec![entry("ghost-holder", "nope", &[])],
    };
    let err = roster
        .validate(&lookup(), &known_agents())
        .expect_err("unknown agent refuses");
    let text = err.to_string();
    assert!(text.contains("nope"), "names the unknown id: {text}");
    assert!(
        text.contains("research") && text.contains("code"),
        "names the known ids: {text}"
    );
    assert!(
        matches!(err, RosterError::UnknownAgent { ref id, .. } if id == "ghost-holder"),
        "{err}"
    );
}

#[test]
fn the_schema_subset_validator_accepts_and_rejects_the_documented_shapes() {
    // One case per supported keyword (pin P2-3), plus the documented
    // trio: `{"type":"object","required":["a"],"properties":{…}}`
    // accepts `{"a":1}`, rejects `{}` and `5`.
    assert_eq!(SUPPORTED_SCHEMA_KEYWORDS.len(), 13);

    // type
    let schema = json!({"type": "string"});
    validate_schema(&schema).expect("type accepts");
    assert!(
        validate_instance(&schema, &json!("x"))
            .iter()
            .all(|o| o.passed)
    );
    assert!(
        validate_instance(&schema, &json!(1))
            .iter()
            .any(|o| !o.passed)
    );

    // properties (recursive)
    let schema = json!({"type": "object", "properties": {"a": {"type": "integer"}}});
    validate_schema(&schema).expect("properties accepts");
    assert!(
        validate_instance(&schema, &json!({"a": 1}))
            .iter()
            .all(|o| o.passed)
    );
    assert!(
        validate_instance(&schema, &json!({"a": "x"}))
            .iter()
            .any(|o| !o.passed)
    );

    // the documented shape
    let documented = json!({
        "type": "object",
        "required": ["a"],
        "properties": {"a": {"type": "number"}}
    });
    validate_schema(&documented).expect("documented schema is inside the subset");
    assert!(
        validate_instance(&documented, &json!({"a": 1}))
            .iter()
            .all(|o| o.passed),
        "accepts {{\"a\":1}}"
    );
    let missing = validate_instance(&documented, &json!({}));
    assert!(
        missing.iter().any(|o| !o.passed && o.keyword == "required"),
        "rejects {{}} on required: {missing:?}"
    );
    let wrong_type = validate_instance(&documented, &json!(5));
    assert!(
        wrong_type.iter().any(|o| !o.passed && o.keyword == "type"),
        "rejects 5 on type: {wrong_type:?}"
    );

    // required (already covered above), additionalProperties
    let schema = json!({
        "type": "object",
        "properties": {"a": {}},
        "additionalProperties": false
    });
    validate_schema(&schema).expect("additionalProperties accepts");
    assert!(
        validate_instance(&schema, &json!({"a": 1}))
            .iter()
            .all(|o| o.passed)
    );
    let extra = validate_instance(&schema, &json!({"a": 1, "b": 2}));
    assert!(
        extra
            .iter()
            .any(|o| !o.passed && o.keyword == "additionalProperties"),
        "{extra:?}"
    );

    // items
    let schema = json!({"type": "array", "items": {"type": "string"}});
    validate_schema(&schema).expect("items accepts");
    assert!(
        validate_instance(&schema, &json!(["a", "b"]))
            .iter()
            .all(|o| o.passed)
    );
    assert!(
        validate_instance(&schema, &json!(["a", 1]))
            .iter()
            .any(|o| !o.passed)
    );

    // enum
    let schema = json!({"enum": ["red", "green"]});
    validate_schema(&schema).expect("enum accepts");
    assert!(
        validate_instance(&schema, &json!("green"))
            .iter()
            .all(|o| o.passed)
    );
    assert!(
        validate_instance(&schema, &json!("blue"))
            .iter()
            .any(|o| !o.passed)
    );

    // const
    let schema = json!({"const": 42});
    validate_schema(&schema).expect("const accepts");
    assert!(
        validate_instance(&schema, &json!(42))
            .iter()
            .all(|o| o.passed)
    );
    assert!(
        validate_instance(&schema, &json!(41))
            .iter()
            .any(|o| !o.passed)
    );

    // minLength / maxLength
    let schema = json!({"type": "string", "minLength": 2, "maxLength": 3});
    validate_schema(&schema).expect("length bounds accept");
    assert!(
        validate_instance(&schema, &json!("ab"))
            .iter()
            .all(|o| o.passed)
    );
    let short = validate_instance(&schema, &json!("a"));
    assert!(
        short.iter().any(|o| !o.passed && o.keyword == "minLength"),
        "{short:?}"
    );
    let long = validate_instance(&schema, &json!("abcd"));
    assert!(
        long.iter().any(|o| !o.passed && o.keyword == "maxLength"),
        "{long:?}"
    );

    // minimum / maximum
    let schema = json!({"type": "number", "minimum": 1, "maximum": 10});
    validate_schema(&schema).expect("numeric bounds accept");
    assert!(
        validate_instance(&schema, &json!(5))
            .iter()
            .all(|o| o.passed)
    );
    let under = validate_instance(&schema, &json!(0));
    assert!(
        under.iter().any(|o| !o.passed && o.keyword == "minimum"),
        "{under:?}"
    );
    let over = validate_instance(&schema, &json!(11));
    assert!(
        over.iter().any(|o| !o.passed && o.keyword == "maximum"),
        "{over:?}"
    );

    // minItems / maxItems
    let schema = json!({"type": "array", "minItems": 1, "maxItems": 2});
    validate_schema(&schema).expect("item-count bounds accept");
    assert!(
        validate_instance(&schema, &json!([1]))
            .iter()
            .all(|o| o.passed)
    );
    let empty = validate_instance(&schema, &json!([]));
    assert!(
        empty.iter().any(|o| !o.passed && o.keyword == "minItems"),
        "{empty:?}"
    );
    let big = validate_instance(&schema, &json!([1, 2, 3]));
    assert!(
        big.iter().any(|o| !o.passed && o.keyword == "maxItems"),
        "{big:?}"
    );

    // Malformed schemas refuse: non-object, unknown keyword, bad type
    // name, wrong keyword shape — and the size bound holds.
    assert!(validate_schema(&json!("string")).is_err());
    assert!(validate_schema(&json!({"oneOf": []})).is_err());
    assert!(validate_schema(&json!({"type": "strng"})).is_err());
    assert!(validate_schema(&json!({"required": "a"})).is_err());
    let oversized = json!({"type": "string", "const": pad_over_bound()});
    let bytes = serde_json::to_string(&oversized).unwrap().len();
    assert!(bytes > 8192, "fixture must exceed the bound ({bytes})");
    assert!(validate_output_schema(&oversized).is_err());
    // The same schema inside the bound passes.
    let in_bounds = json!({"type": "string", "const": "x".repeat(100)});
    assert!(validate_output_schema(&in_bounds).is_ok());
}

// ---------------------------------------------------------------------------
// the list_agents surface (pin P2-6, M9)
// ---------------------------------------------------------------------------

#[test]
fn an_empty_roster_mounts_no_list_agents_and_keeps_the_tool_set_byte_identical() {
    // Pin P2-6 / §3.1:366-367: roster absent (and empty) ⇒ the P1
    // golden tool set exactly — no `list_agents` schema, no ordering
    // change, the legacy `spawn_worker` parameters untouched.
    for roster in [None, Some(Roster::default())] {
        let tools = router_tools(roster);
        let schemas = tools.schemas();
        let names: Vec<&str> = schemas.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(
            names,
            vec![SPAWN_WORKER_TOOL, "reply", DELIVER_STRUCTURED_TOOL],
            "the P1 golden set, in order"
        );
        assert!(!names.contains(&LIST_AGENTS_TOOL));
        // The legacy spawn_worker parameters: membership off, roster
        // off — `required: ["task"]`, the three legacy properties.
        let spawn = &schemas[0];
        assert_eq!(
            spawn.parameters,
            json!({
                "type": "object",
                "properties": {
                    "task": {"type": "string", "description": "The actual work, as user content \
                     (a channel-event dispatch may pass the triggering wrapper serialized as JSON text)"},
                    "correlation": {"description": "Optional ticket correlation — a topic token \
                     (bare string, e.g. the ticket thread key, or {\"topic\": …}); a token binds \
                     the spawned worker as that topic's live Executor (follow-up ownership) and \
                     orders outbox deliveries per (correlation, target conversation)"},
                    "speculative": {"type": "boolean", "description": "Spawn on a weak signal, \
                     budgeted by the Router's speculation policy (refused honestly at the cap; \
                     the worker's first message carries the speculative marker)"}
                },
                "required": ["task"],
            })
        );
        // reply / deliver_structured keep their P1 bytes.
        assert_eq!(schemas[1].name, "reply");
        assert_eq!(
            schemas[1].parameters,
            json!({
                "type": "object",
                "properties": {
                    "channel": {"type": "string"},
                    "text": {"type": "string"},
                    "target": {"description": "Channel-defined delivery target"},
                    "correlation": {"type": "string", "description": "Optional ticket correlation \
                     (thread id or topic) — the outbox orders and reports deliveries per \
                     (correlation, target conversation)"}
                },
                "required": ["channel", "text"],
            })
        );
        assert_eq!(schemas[2].name, DELIVER_STRUCTURED_TOOL);
        assert_eq!(
            schemas[2].parameters["required"],
            json!(["channel", "target", "payload"])
        );
    }
    // And the invocation is an honest unknown-tool error, not a guess.
    let tools = router_tools(None);
    let err = futures::executor::block_on(tools.invoke(ToolCall {
        id: "c1".to_string(),
        name: LIST_AGENTS_TOOL.to_string(),
        arguments: json!({}),
    }))
    .expect_err("unknown tool when off");
    assert!(
        err.to_string()
            .contains("unknown router tool `list_agents`"),
        "{err}"
    );
}

#[test]
fn list_agents_schema_is_a_constant_with_no_ids_counts_or_enums() {
    // M9 (the I6 lesson): the schema is one constant — no roster id,
    // no count, no enum. Membership of the roster reaches the model
    // only through the tool result.
    let tools = router_tools(Some(Roster {
        entries: vec![
            entry("researcher", "research", &["fs_read"]),
            entry("coder", "code", &["exec"]),
        ],
    }));
    let schema = tools
        .schemas()
        .into_iter()
        .find(|s| s.name == LIST_AGENTS_TOOL)
        .expect("list_agents present when the roster is non-empty");
    assert_eq!(
        schema.parameters,
        json!({"type": "object", "properties": {}, "required": []})
    );
    let whole = format!(
        "{}{}{}",
        schema.name,
        kian_superagent::LIST_AGENTS_DESCRIPTION,
        serde_json::to_string(&schema.parameters).unwrap()
    );
    assert!(!whole.contains("researcher"), "no roster id: {whole}");
    assert!(!whole.contains("coder"), "no roster id: {whole}");
    assert!(!whole.contains("enum"), "no enum: {whole}");
    // The schema is byte-identical across roster sizes (a
    // count-dependent schema would churn prefix bytes on a roster
    // edit).
    let three = router_tools(Some(Roster {
        entries: vec![
            entry("researcher", "research", &["fs_read"]),
            entry("coder", "code", &["exec"]),
            entry("writer", "research", &[]),
        ],
    }));
    let other = three
        .schemas()
        .into_iter()
        .find(|s| s.name == LIST_AGENTS_TOOL)
        .expect("present");
    assert_eq!(
        serde_json::to_string(&schema.parameters).unwrap(),
        serde_json::to_string(&other.parameters).unwrap()
    );
    assert_eq!(schema.description, other.description);
}

#[tokio::test]
async fn list_agents_result_is_config_ordered_and_carries_the_selection_facts() {
    let roster = Roster {
        entries: vec![
            entry("researcher", "research", &["fs_read", "recall_event"]),
            RosterEntry {
                limits: RosterLimits { max_children: 0 },
                output_schema: Some(json!({"type": "string"})),
                ..entry("coder", "code", &["exec"])
            },
        ],
    };
    let tools = router_tools(Some(roster));
    let result = tools
        .invoke(ToolCall {
            id: "call-1".to_string(),
            name: LIST_AGENTS_TOOL.to_string(),
            arguments: json!({"ignored": true}),
        })
        .await
        .expect("read-only render");
    assert!(!result.is_error);
    assert_eq!(result.call_id, "call-1");
    let agents = result.output["agents"].as_array().expect("agents array");
    // Config order, not sorted order (coder after researcher).
    assert_eq!(agents.len(), 2);
    assert_eq!(agents[0]["id"], json!("researcher"));
    assert_eq!(agents[1]["id"], json!("coder"));
    // The selection facts only — the mandate text, the schema and
    // live state never enter the row.
    for row in agents {
        let mut keys: Vec<&str> = row
            .as_object()
            .expect("row")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["agent", "description", "id", "max_children", "tools_allow"]
        );
    }
    assert_eq!(agents[1]["max_children"], json!(0), "the leaf fact");
    assert_eq!(agents[1]["tools_allow"], json!(["exec"]));
}
