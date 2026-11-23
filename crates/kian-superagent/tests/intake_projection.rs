//! Thread/Correlation intake projection acceptance (the
//! ADR-013 draft's contract in code).
//!
//! - thread-key derivation goldens per configured source shape (the
//!   canonical `<channel>:<group>#<key>` topic form, stable and
//!   collision-free by construction);
//! - keys are config-owned: the same underlying event through two
//!   configs derives two keys, and the old config's key stays
//!   byte-stable (old streams keep their topics — cache-stable, no
//!   rewrite);
//! - read-position derivation from a scripted admitted-event tail
//!   (lark message-id offsets; GitHub REST `since` timestamps);
//! - a no-cursor source documents the re-poll bound: the replayed poll
//!   produces `deduplicated` admissions and zero duplicate rows
//!   (idempotent re-poll under `event_id` dedupe — no position table);
//! - the derived key surfaces in the Inline `neighbors` block as
//!   context (correlation never routes);
//! - `intake.rs` names no store type (bundle-only, `kian-core` seams
//!   only).
//!
//! Fixture payloads only — no network.

use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_core::seams::ChannelEvent;
use kian_orchestrator::WakeOutcome;
use kian_superagent::inbox::{AdmitReceipt, InboxWriteError, InboxWriter};
use kian_superagent::rules::{Route, RuleMatcher, SubscriptionRule, SubscriptionRuleSet};
use kian_superagent::{
    RoutingOutcome, SourceCursor, SubscriptionService, THREAD_KEY_FIELD, ThreadKeyRule,
    ThreadKeySet, channel_input_payload, derive_thread_key, neighbors_with_thread_key,
    read_position,
};
use serde_json::{Value, json};
use uuid::Uuid;

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------

fn sid(n: u128) -> SessionId {
    SessionId(Uuid::from_u128(n))
}

fn event(seq: u64, kind: EventKind, bytes: Value) -> Event {
    Event {
        seq,
        session: sid(1),
        epoch: Epoch(2),
        ts: 1_762_000_000_000 + seq,
        kind,
        bytes,
        version: SCHEMA_VERSION,
    }
}

/// A lark-shaped channel event (chat-history poll mapping): the
/// verbatim message item as payload, the chat id as group.
fn lark_event(event_id: &str, chat: &str, message_id: &str) -> ChannelEvent {
    ChannelEvent {
        channel: "lark".into(),
        event_id: event_id.into(),
        topic: Some(chat.into()),
        sender: Some(json!({"id": "ou_9", "sender_type": "user"})),
        group: Some(chat.into()),
        payload: json!({
            "message_id": message_id,
            "chat_id": chat,
            "msg_type": "text",
            "create_time": "1730000100",
            "content": json!({"text": "please review"}).to_string(),
            "nested": {"not": "an id"},
        }),
        ts_ms: 1_762_000_000_500,
    }
}

/// A github `issue_comment` webhook-shaped channel event (mapping):
/// the verbatim body as payload, `owner/repo` as group.
fn github_issue_event(event_id: &str, number: u64, updated_at: &str) -> ChannelEvent {
    ChannelEvent {
        channel: "github".into(),
        event_id: event_id.into(),
        topic: Some("ci".into()),
        sender: Some(json!({"login": "reporter", "type": "User"})),
        group: Some("flyer103/kian-runtime".into()),
        payload: json!({
            "action": "created",
            "issue": {
                "number": number,
                "title": "build broken on main",
                "updated_at": updated_at,
            },
            "comment": {"id": 99, "body": "please take a look"},
            "repository": {"id": 1, "full_name": "flyer103/kian-runtime", "name": "kian"},
        }),
        ts_ms: 1_762_000_000_500,
    }
}

/// One admitted channel input as a stream event (the wrapper rides
/// inside `content`, the -frozen payload contract).
fn admitted(ev: &ChannelEvent, seq: u64) -> Event {
    event(
        seq,
        EventKind::InputAdmitted,
        json!({"role": "user", "content": channel_input_payload(ev)}),
    )
}

/// The spill sidecar of an over-cap admitted input: the verbatim
/// raw plus the join key.
fn spilled(ev: &ChannelEvent, seq: u64) -> Event {
    event(
        seq,
        EventKind::InputSpilled,
        json!({
            "event_id": ev.event_id,
            "channel": ev.channel,
            "topic": ev.topic,
            "raw": ev.payload,
        }),
    )
}

// ---------------------------------------------------------------------
// Thread-key derivation
// ---------------------------------------------------------------------

/// The config serde form round-trips exactly: one entry is
/// `{"channel": …, "key": [group?, <payload-field>…]}`.
#[test]
fn thread_key_rule_serde_golden_form() {
    let rule: ThreadKeyRule = serde_json::from_value(json!({
        "channel": "github",
        "key": ["group", "issue.number"]
    }))
    .unwrap();
    assert_eq!(rule.channel, "github");
    assert_eq!(
        rule.key,
        vec!["group".to_string(), "issue.number".to_string()]
    );
    // Round trip back to the same golden JSON.
    assert_eq!(
        serde_json::to_value(&rule).unwrap(),
        json!({"channel": "github", "key": ["group", "issue.number"]})
    );

    // The config key `superagent.intake.thread_keys` is a list of these.
    let entries: Vec<ThreadKeyRule> = serde_json::from_value(json!([
        {"channel": "lark", "key": ["group", "message_id"]},
        {"channel": "github", "key": ["group", "issue.number"]}
    ]))
    .unwrap();
    let set = ThreadKeySet::new(entries);
    assert_eq!(set.len(), 2);
    assert!(!set.is_empty());
}

/// Golden derivations per configured source shape: the canonical topic
/// form `<channel>:<group>#<key>` — group optional, key fields joined
/// in config order.
#[test]
fn thread_key_derivation_golden_per_configured_source_shape() {
    struct Case {
        name: &'static str,
        rule: ThreadKeyRule,
        event: ChannelEvent,
        expected: &'static str,
    }
    let cases = [
        Case {
            name: "lark chat + message id",
            rule: serde_json::from_value(json!({
                "channel": "lark", "key": ["group", "message_id"]
            }))
            .unwrap(),
            event: lark_event("om_8812", "oc_7f3a", "om_8812"),
            expected: "lark:oc_7f3a#om_8812",
        },
        Case {
            name: "github repo + issue number",
            rule: serde_json::from_value(json!({
                "channel": "github", "key": ["group", "issue.number"]
            }))
            .unwrap(),
            event: github_issue_event("d-1", 7, "2026-11-03T10:00:00Z"),
            expected: "github:flyer103/kian-runtime#7",
        },
        Case {
            name: "groupless config omits the group slot",
            rule: serde_json::from_value(json!({
                "channel": "github", "key": ["issue.number"]
            }))
            .unwrap(),
            event: github_issue_event("d-1", 7, "2026-11-03T10:00:00Z"),
            expected: "github#7",
        },
        Case {
            name: "multi-field key joins in config order",
            rule: serde_json::from_value(json!({
                "channel": "github", "key": ["group", "issue.number", "action"]
            }))
            .unwrap(),
            event: github_issue_event("d-1", 7, "2026-11-03T10:00:00Z"),
            expected: "github:flyer103/kian-runtime#7.created",
        },
    ];
    for case in cases {
        let key = derive_thread_key(&case.rule, &case.event)
            .unwrap_or_else(|| panic!("case `{}` must derive", case.name));
        assert_eq!(key.topic(), case.expected, "case `{}`", case.name);
    }
}

/// Underivable is an honest `None`, never a guess: a missing field, a
/// non-scalar field, an empty key list, a channel mismatch, and a
/// named `group` on a groupless event all refuse.
#[test]
fn underivable_keys_are_honest_nones() {
    let rule = ThreadKeyRule {
        channel: "github".into(),
        key: vec!["issue.number".into()],
    };
    // Missing field.
    let mut ev = github_issue_event("d-1", 7, "t");
    ev.payload["issue"]
        .as_object_mut()
        .unwrap()
        .remove("number");
    assert!(derive_thread_key(&rule, &ev).is_none());
    // Non-scalar field.
    let mut ev = github_issue_event("d-1", 7, "t");
    ev.payload["issue"]["number"] = json!({"nested": true});
    assert!(derive_thread_key(&rule, &ev).is_none());
    // Empty key list.
    let empty = ThreadKeyRule {
        channel: "github".into(),
        key: vec![],
    };
    assert!(derive_thread_key(&empty, &github_issue_event("d-1", 7, "t")).is_none());
    // Channel mismatch: the rule is for another channel.
    let lark_rule = ThreadKeyRule {
        channel: "lark".into(),
        key: vec!["message_id".into()],
    };
    assert!(derive_thread_key(&lark_rule, &github_issue_event("d-1", 7, "t")).is_none());
    // `group` named but the event carries none.
    let grouped = ThreadKeyRule {
        channel: "github".into(),
        key: vec!["group".into(), "issue.number".into()],
    };
    let mut ev = github_issue_event("d-1", 7, "t");
    ev.group = None;
    assert!(derive_thread_key(&grouped, &ev).is_none());
}

/// Keys are config-owned (ADR-013): the same underlying event through
/// two configs derives two keys, and the original config's key stays
/// byte-stable across the change — a config change is a new key, old
/// streams keep their topics (cache-stable, no rewrite).
#[test]
fn keys_are_config_owned_two_configs_two_keys() {
    let ev = github_issue_event("d-1", 7, "2026-11-03T10:00:00Z");
    let before = ThreadKeySet::new(vec![
        serde_json::from_value(json!({
            "channel": "github", "key": ["group", "issue.number"]
        }))
        .unwrap(),
    ]);
    let after = ThreadKeySet::new(vec![
        serde_json::from_value(json!({
            "channel": "github", "key": ["group", "action"]
        }))
        .unwrap(),
    ]);

    let key_before = before.derive(&ev).expect("before config derives");
    let key_after = after.derive(&ev).expect("after config derives");
    assert_eq!(key_before.topic(), "github:flyer103/kian-runtime#7");
    assert_eq!(key_after.topic(), "github:flyer103/kian-runtime#created");
    assert_ne!(key_before.topic(), key_after.topic());

    // The old config still derives the old key byte-identically: no
    // rewrite of already-admitted streams ever happens.
    assert_eq!(
        before.derive(&ev).expect("re-derive").topic(),
        "github:flyer103/kian-runtime#7"
    );
}

/// The canonical form is collision-free by construction: every
/// reserved character (`%`, `:`, `#`, `.`) inside a component is
/// percent-encoded, so distinct component tuples always derive
/// distinct topics — including the pairs a naive join would collide.
#[test]
fn canonical_form_is_collision_free_by_construction() {
    let dotted = ThreadKeyRule {
        channel: "im".into(),
        key: vec!["text".into()],
    };
    let split = ThreadKeyRule {
        channel: "im".into(),
        key: vec!["a".into(), "b".into()],
    };
    let ev = |payload: Value| ChannelEvent {
        channel: "im".into(),
        event_id: "e-1".into(),
        topic: Some("ci".into()),
        sender: None,
        group: None,
        payload,
        ts_ms: 1,
    };
    // `a.b` as one field vs `a` + `b` as two fields: a naive "."-join
    // would collide; the encoding keeps them distinct.
    let one = derive_thread_key(&dotted, &ev(json!({"text": "a.b"}))).unwrap();
    let two = derive_thread_key(&split, &ev(json!({"a": "a", "b": "b"}))).unwrap();
    assert_eq!(one.topic(), "im#a%2Eb");
    assert_eq!(two.topic(), "im#a.b");
    assert_ne!(one.topic(), two.topic());

    // A `:` inside the group never impersonates the channel separator,
    // and `#` inside a value never impersonates the key separator.
    let grouped = ThreadKeyRule {
        channel: "im".into(),
        key: vec!["group".into(), "text".into()],
    };
    let mut tricky = ev(json!({"text": "x#y"}));
    tricky.group = Some("g:1".into());
    let key = derive_thread_key(&grouped, &tricky).unwrap();
    assert_eq!(key.topic(), "im:g%3A1#x%23y");
    // And decoding hazards: a literal `%` encodes too, so `%2E` in a
    // value can never be confused with an encoded `.`.
    let mut pct = ev(json!({"text": "100%2E"}));
    pct.group = None;
    let groupless = ThreadKeyRule {
        channel: "im".into(),
        key: vec!["text".into()],
    };
    let key = derive_thread_key(&groupless, &pct).unwrap();
    assert_eq!(key.topic(), "im#100%252E");
    assert_ne!(key.topic(), "im#100%2E");
}

/// First-match on channel in config order (the ruleset convention).
#[test]
fn thread_key_set_first_match_wins_in_declaration_order() {
    let ev = github_issue_event("d-1", 7, "t");
    let set = ThreadKeySet::new(vec![
        ThreadKeyRule {
            channel: "lark".into(),
            key: vec!["message_id".into()],
        },
        ThreadKeyRule {
            channel: "github".into(),
            key: vec!["issue.number".into()],
        },
    ]);
    assert_eq!(
        set.derive(&ev).unwrap().topic(),
        "github#7",
        "the lark entry does not match; the github entry does"
    );
    // No entry for the channel → None (the adapter topic stands; the
    // matcher is unchanged).
    let mut ev = lark_event("om_1", "oc_1", "om_1");
    ev.channel = "webhook-feed".into();
    assert!(set.derive(&ev).is_none());
}

// ---------------------------------------------------------------------
// Neighbors block (correlation is context only)
// ---------------------------------------------------------------------

/// The derived key surfaces in the Inline `neighbors` block: the
/// canonical `topic`/`event_id`, the configured scalar neighbor
/// fields (payload-controlled values never override the canonical
/// ones), and `thread_key` — context for recall hops, never a routing
/// input (the subscription rule matcher is unchanged ).
#[test]
fn neighbors_block_carries_the_thread_key() {
    let set = ThreadKeySet::new(vec![
        serde_json::from_value(json!({
            "channel": "lark", "key": ["group", "message_id"]
        }))
        .unwrap(),
    ]);
    let ev = lark_event("om_8812", "oc_7f3a", "om_8812");
    let neighbors =
        neighbors_with_thread_key(&ev, &set, &["message_id".to_string(), "nested".to_string()]);
    assert_eq!(
        neighbors,
        json!({
            "topic": "oc_7f3a",
            "event_id": "om_8812",
            "message_id": "om_8812",
            THREAD_KEY_FIELD: "lark:oc_7f3a#om_8812",
        }),
        "scalar neighbor fields ride; non-scalars (nested objects) are skipped"
    );

    // No rule for the channel → the neighbors block stands without a
    // thread key (absent config = today's behavior, additive only).
    let mut foreign = lark_event("om_9", "oc_9", "om_9");
    foreign.channel = "webhook-feed".into();
    let neighbors = neighbors_with_thread_key(&foreign, &set, &[]);
    assert!(neighbors.get(THREAD_KEY_FIELD).is_none());
    assert_eq!(neighbors["event_id"], json!("om_9"));
}

// ---------------------------------------------------------------------
// Read position
// ---------------------------------------------------------------------

/// Read-position derivation from a scripted admitted-event tail: the
/// LAST admitted event's payload cursor field per source kind wins
/// (lark message-id offsets; GitHub REST `since` timestamps); the
/// spill sidecar of an over-cap input carries the same raw; a tail
/// with no derivable cursor derives `None` — the honest re-poll bound.
#[test]
fn read_position_from_a_scripted_admitted_tail() {
    // --- lark: last admitted message id wins ---
    let tail = vec![
        admitted(&lark_event("om_1", "oc_c", "om_1"), 1),
        admitted(&lark_event("om_2", "oc_c", "om_2"), 2),
        event(
            3,
            EventKind::InputAdmitted,
            json!({"role": "user", "content": "a person's plain message"}),
        ),
        admitted(&lark_event("om_8812", "oc_c", "om_8812"), 4),
    ];
    let pos = read_position(SourceCursor::LarkMessageId, &tail).expect("lark cursor derives");
    assert_eq!(pos.cursor, "om_8812");
    assert_eq!(pos.event_id, "om_8812");
    assert_eq!(pos.field, "message_id");

    // --- github: the REST `since` timestamp (issue or PR shape) ---
    let gh_tail = vec![
        admitted(&github_issue_event("d-1", 7, "2026-11-03T09:00:00Z"), 1),
        admitted(&github_issue_event("d-2", 9, "2026-11-03T10:30:00Z"), 2),
    ];
    let pos = read_position(SourceCursor::GitHubSince, &gh_tail).expect("github cursor derives");
    assert_eq!(pos.cursor, "2026-11-03T10:30:00Z");
    assert_eq!(pos.event_id, "d-2");
    assert_eq!(pos.field, "updated_at");

    // A PR-shaped payload derives from `pull_request.updated_at`.
    let mut pr = github_issue_event("d-3", 11, "unused");
    pr.payload
        .as_object_mut()
        .unwrap()
        .remove("issue")
        .expect("issue present");
    pr.payload["pull_request"] = json!({"number": 11, "updated_at": "2026-11-03T11:00:00Z"});
    let pos =
        read_position(SourceCursor::GitHubSince, &[admitted(&pr, 1)]).expect("pr cursor derives");
    assert_eq!(pos.cursor, "2026-11-03T11:00:00Z");

    // --- the spill sidecar carries the raw when the inline view
    //     cannot (over-cap admission) ---
    let spill_tail = vec![spilled(&lark_event("om_77", "oc_c", "om_77"), 1)];
    let pos = read_position(SourceCursor::LarkMessageId, &spill_tail)
        .expect("the sidecar raw carries the cursor");
    assert_eq!(pos.cursor, "om_77");
    assert_eq!(pos.event_id, "om_77");

    // --- no derivable cursor: None (the caller re-polls bounded by
    //     `event_id` dedupe — no position table, ADR-013) ---
    let timer = ChannelEvent {
        channel: "timer".into(),
        event_id: "tick-42".into(),
        topic: Some("watchdog".into()),
        sender: None,
        group: None,
        payload: json!({"tick": 42}),
        ts_ms: 1,
    };
    assert!(read_position(SourceCursor::LarkMessageId, &[admitted(&timer, 1)]).is_none());
    assert!(read_position(SourceCursor::GitHubSince, &[admitted(&timer, 1)]).is_none());
    assert!(read_position(SourceCursor::LarkMessageId, &[]).is_none());
}

// ---------------------------------------------------------------------
// The no-cursor re-poll bound (idempotent re-poll under event_id dedupe)
// ---------------------------------------------------------------------

/// An inbox port with the pipeline's `(session, event_id)`
/// idempotency, so the re-poll bound is observable: exactly one row.
struct DedupInbox {
    rows: Mutex<Vec<(SessionId, String)>>,
    wakes: Mutex<Vec<SessionId>>,
}

impl InboxWriter for DedupInbox {
    fn admit(
        &self,
        session: SessionId,
        _payload: Value,
        idempotency_key: Option<String>,
    ) -> BoxFuture<'static, Result<AdmitReceipt, InboxWriteError>> {
        let key = idempotency_key.unwrap_or_default();
        let mut rows = self.rows.lock().expect("rows lock");
        let deduplicated = rows.contains(&(session, key.clone()));
        if !deduplicated {
            rows.push((session, key));
        }
        Box::pin(async move { Ok(AdmitReceipt { deduplicated }) })
    }

    fn wake(&self, session: SessionId) -> WakeOutcome {
        self.wakes.lock().expect("wakes lock").push(session);
        WakeOutcome::Folded
    }
}

/// A no-cursor source (a timer-shaped payload exposes no cursor
/// field) has no durable position (ADR-013 rejects the table): the
/// honest v1 bound is idempotent re-poll — the replayed poll produces
/// a `deduplicated` admission and zero duplicate rows.
#[tokio::test]
async fn no_cursor_source_repoll_produces_deduplicated_admissions_zero_duplicate_rows() {
    let router = sid(0x10);
    let inbox = Arc::new(DedupInbox {
        rows: Mutex::new(Vec::new()),
        wakes: Mutex::new(Vec::new()),
    });
    let service = SubscriptionService::new(
        kian_superagent::ResidentBinding::new(router),
        SubscriptionRuleSet::new(vec![SubscriptionRule {
            channel: "timer".into(),
            matcher: RuleMatcher::Always,
            route: Route::Router,
        }]),
        inbox.clone(),
    );

    let ev = ChannelEvent {
        channel: "timer".into(),
        event_id: "tick-42".into(),
        topic: Some("watchdog".into()),
        sender: None,
        group: None,
        payload: json!({"tick": 42}),
        ts_ms: 1,
    };

    // First poll (fresh start): an honest admission.
    let first = service.handle_event(ev.clone()).await.unwrap();
    assert_eq!(
        first,
        RoutingOutcome::Routed {
            session: router,
            deduplicated: false,
        }
    );

    // Restart + replayed poll of the same event id: deduplicated, and
    // the row count never grows — the re-poll bound is the pipeline's
    // idempotency, not a position table.
    for _ in 0..3 {
        let replay = service.handle_event(ev.clone()).await.unwrap();
        assert_eq!(
            replay,
            RoutingOutcome::Routed {
                session: router,
                deduplicated: true,
            },
            "every replayed poll deduplicates"
        );
    }
    let rows = inbox.rows.lock().expect("rows lock");
    assert_eq!(rows.len(), 1, "zero duplicate rows across the re-polls");
    assert_eq!(rows[0], (router, "tick-42".to_string()));
}

// ---------------------------------------------------------------------
// Bundle fence
// ---------------------------------------------------------------------

/// `intake.rs` is bundle-only: it names no store, no orchestrator
/// execution surface — the derivation reads `ChannelEvent`s and
/// admitted stream events through `kian-core` seams only (the
/// acceptance, in the iron-rule textual pattern).
#[test]
fn intake_module_names_no_store_type() {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("intake.rs");
    let src = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    for token in [
        "kian_store",
        "EventStore",
        "kian_orchestrator",
        "Engine",
        "DrainCtx",
        "InboxWriter",
    ] {
        assert!(
            !src.contains(token),
            "src/intake.rs must not name `{token}` — the intake projection is bundle-only"
        );
    }
}
