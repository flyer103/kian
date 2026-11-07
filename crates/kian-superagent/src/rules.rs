//! Declarative subscription rules (DESIGN §4.1,; Footprint
//! Ladder level 1 — pure config, no code).
//!
//! A rule names a channel, a match condition on inbound
//! [`ChannelEvent`]s, and a route: the resident Router session, or a
//! passthrough correlated by topic (CI-style events of a worker's own
//! topic flow straight into that worker's inbox without Router
//! judgment — the DESIGN §4.1 load-reduction device). The passthrough
//! `topic → set of worker sessions` mapping (many-to-many
//! fan-out) lives in [`crate::registry::PassthroughRegistry`], a
//! disposable projection.

use kian_core::model::SessionId;
use kian_core::seams::ChannelEvent;
use serde::{Deserialize, Serialize};

/// Match condition on an inbound channel event. Serde form (externally
/// tagged, the `match: always|mentions(str)|topic(str)`):
/// `"always"`, `{"mentions": "@kian"}`, `{"topic": "ci"}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleMatcher {
    #[serde(rename = "always")]
    Always,
    #[serde(rename = "mentions")]
    Mentions(String),
    #[serde(rename = "topic")]
    Topic(String),
}

impl RuleMatcher {
    /// Pure predicate over one inbound event.
    ///
    /// - [`RuleMatcher::Always`] — every event on the rule's channel.
    /// - [`RuleMatcher::Mentions`] — the event's payload carries a
    ///   top-level `"text"` string containing the mention token
    ///   (the IM @-mention convention).
    /// - [`RuleMatcher::Topic`] — the event's routing topic matches
    ///   exactly.
    pub fn matches(&self, event: &ChannelEvent) -> bool {
        match self {
            Self::Always => true,
            Self::Mentions(token) => event
                .payload
                .get("text")
                .and_then(|t| t.as_str())
                .is_some_and(|text| text.contains(token.as_str())),
            Self::Topic(topic) => event.topic.as_deref() == Some(topic.as_str()),
        }
    }
}

/// How a matched event's target session is chosen. Serde form:
/// `"router"`, `{"passthrough": {"correlate_by": "topic"}}`, or —
/// , additive — `{"session": "<uuid>"}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Route {
    #[serde(rename = "router")]
    Router,
    #[serde(rename = "passthrough")]
    Passthrough { correlate_by: CorrelateBy },
    /// A config-addressed fixed session: the timer's ticks (and
    /// any other event an operator wants pinned to one resident
    /// session) route here — generic vocabulary, nothing
    /// watchdog-specific. The target is admitted like any other
    /// routed event (idempotent by `event_id`).
    #[serde(rename = "session")]
    Session(SessionId),
}

impl Route {
    /// True for the passthrough route (the DESIGN §4.1 "passthrough
    /// flag" of a rule, expressed as the route variant itself).
    pub fn is_passthrough(&self) -> bool {
        matches!(self, Self::Passthrough { .. })
    }
}

/// The correlation key a passthrough route resolves its target by. The
/// v1 vocabulary is exactly one entry — `topic` — so the correlation is
/// always against the event's routing topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CorrelateBy {
    #[serde(rename = "topic")]
    Topic,
}

/// One declarative subscription rule: channel + match + route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionRule {
    /// Channel name the rule applies to (must equal the
    /// [`ChannelEvent::channel`] the adapter reports).
    pub channel: String,
    #[serde(rename = "match")]
    pub matcher: RuleMatcher,
    pub route: Route,
}

impl SubscriptionRule {
    /// Channel + matcher both agree (the full rule predicate).
    pub fn matches(&self, event: &ChannelEvent) -> bool {
        self.channel == event.channel && self.matcher.matches(event)
    }

    /// Convenience: this rule routes by passthrough.
    pub fn is_passthrough(&self) -> bool {
        self.route.is_passthrough()
    }
}

/// A validated rule set. First match wins (rule order is config
/// order); an event matching no rule is dropped with an audit log line
/// and no inbox row.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionRuleSet(pub Vec<SubscriptionRule>);

impl SubscriptionRuleSet {
    pub fn new(rules: Vec<SubscriptionRule>) -> Self {
        Self(rules)
    }

    /// First rule matching the event, in declaration order.
    pub fn first_match(&self, event: &ChannelEvent) -> Option<&SubscriptionRule> {
        self.0.iter().find(|rule| rule.matches(event))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

// ---------------------------------------------------------------------------
// Admission classes (— the article §3.1 Fast Path): an admission
// taxonomy over provenance, evaluated BEFORE the rule set and before
// any model call. Additive config layer; absent classes = today's
// behavior, unchanged.
// ---------------------------------------------------------------------------

/// The admission class one provenance key resolves to. The
/// serde form is the config spelling: `candidate` | `explicit-only` |
/// `ignore`.
///
/// - [`AdmissionClass::Candidate`] — flows to the rule set as today.
/// - [`AdmissionClass::ExplicitOnly`] — admitted only when the event
///   carries an explicit-request signal (see
///   [`event_has_explicit_signal`]); otherwise dropped with an audit
///   line, no row, no wake.
/// - [`AdmissionClass::Ignore`] — dropped with an audit line and a
///   counter, no row, no wake (the `DroppedNoRule` shape: an honest
///   no-op, never a silent swallow).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AdmissionClass {
    Candidate,
    ExplicitOnly,
    Ignore,
}

/// One class-table entry, keyed by `(channel, topic?, sender?)`. A
/// `None` key component matches any value; a `Some` component matches
/// exactly (sender by JSON equality — the adapter-declared provenance
/// value).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdmissionClassRule {
    /// Channel name (must equal the [`ChannelEvent::channel`] the
    /// adapter reports).
    pub channel: String,
    /// Optional topic key (`None` = any topic).
    #[serde(default)]
    pub topic: Option<String>,
    /// Optional sender key (`None` = any sender; adapter-defined JSON
    /// provenance compared by equality).
    #[serde(default)]
    pub sender: Option<serde_json::Value>,
    pub class: AdmissionClass,
}

impl AdmissionClassRule {
    /// Does this entry's key match the event's provenance?
    fn key_matches(&self, event: &ChannelEvent) -> bool {
        self.channel == event.channel
            && self
                .topic
                .as_deref()
                .is_none_or(|t| event.topic.as_deref() == Some(t))
            && self
                .sender
                .as_ref()
                .is_none_or(|s| event.sender.as_ref() == Some(s))
    }

    /// Specificity: how many optional key components are pinned
    /// (0–2). Higher wins; ties break by declaration order.
    fn specificity(&self) -> usize {
        self.topic.is_some() as usize + self.sender.is_some() as usize
    }
}

/// The validated class table. Resolution is **most-specific-wins**:
/// among all entries whose key matches the event, the one with the most
/// pinned key components (topic, sender) decides; a tie resolves to the
/// earliest declared entry — deterministic by construction. No matching
/// entry (or an empty table) = [`AdmissionClass::Candidate`]: exactly
/// today's behavior, the rule set decides.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AdmissionClassTable(pub Vec<AdmissionClassRule>);

impl AdmissionClassTable {
    pub fn new(rules: Vec<AdmissionClassRule>) -> Self {
        Self(rules)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Most-specific-wins resolution for one event's provenance. Among
    /// matching entries the highest specificity decides; a tie
    /// resolves to the **earliest** declared entry (strict `>` keeps
    /// the first of equal maxima — `max_by_key` would keep the last).
    pub fn resolve(&self, event: &ChannelEvent) -> AdmissionClass {
        let mut best: Option<&AdmissionClassRule> = None;
        for rule in &self.0 {
            if !rule.key_matches(event) {
                continue;
            }
            if best.is_none_or(|b| rule.specificity() > b.specificity()) {
                best = Some(rule);
            }
        }
        best.map_or(AdmissionClass::Candidate, |best| best.class)
    }
}

/// The explicit-request signal an [`AdmissionClass::ExplicitOnly`]
/// event must carry to be admitted: an @-mention in the payload's
/// `text` (the IM convention the `mentions` matcher detects — a `@`
/// followed by at least one non-whitespace character), or an
/// adapter-declared top-level `"explicit": true` payload field.
///
/// Pure predicate, no config: the class table decides *who* must be
/// explicit; this decides *what counts* as explicit.
pub fn event_has_explicit_signal(event: &ChannelEvent) -> bool {
    if event.payload.get("explicit") == Some(&serde_json::Value::Bool(true)) {
        return true;
    }
    event
        .payload
        .get("text")
        .and_then(|t| t.as_str())
        .is_some_and(text_has_at_mention)
}

/// `@` followed by at least one non-whitespace character.
fn text_has_at_mention(text: &str) -> bool {
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '@' && chars.next().is_some_and(|next| !next.is_whitespace()) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{CorrelateBy, Route, RuleMatcher, SubscriptionRule, SubscriptionRuleSet};
    use kian_core::seams::ChannelEvent;
    use serde_json::json;

    fn event(channel: &str, topic: Option<&str>, text: &str) -> ChannelEvent {
        ChannelEvent {
            channel: channel.into(),
            event_id: "evt-1".into(),
            topic: topic.map(str::to_string),
            sender: None,
            group: None,
            payload: json!({"text": text}),
            ts_ms: 1,
        }
    }

    #[test]
    fn matchers_evaluate_their_named_condition() {
        let e = event("mock-im", Some("ci"), "@kian deploy please");
        assert!(RuleMatcher::Always.matches(&e));
        assert!(RuleMatcher::Mentions("@kian".into()).matches(&e));
        assert!(!RuleMatcher::Mentions("@other".into()).matches(&e));
        // No "text" string → a mention cannot match.
        let bare = ChannelEvent {
            payload: json!({"x": 1}),
            ..e.clone()
        };
        assert!(!RuleMatcher::Mentions("@kian".into()).matches(&bare));
        assert!(RuleMatcher::Topic("ci".into()).matches(&e));
        assert!(!RuleMatcher::Topic("other".into()).matches(&e));
        let topicless = event("mock-im", None, "hi");
        assert!(!RuleMatcher::Topic("ci".into()).matches(&topicless));
    }

    #[test]
    fn rules_require_channel_and_matcher_to_agree() {
        let rule = SubscriptionRule {
            channel: "mock-im".into(),
            matcher: RuleMatcher::Always,
            route: Route::Router,
        };
        assert!(rule.matches(&event("mock-im", None, "hi")));
        assert!(!rule.matches(&event("lark", None, "hi")));
        assert!(!rule.route.is_passthrough());
        assert!(!rule.is_passthrough());
        let passthrough = SubscriptionRule {
            route: Route::Passthrough {
                correlate_by: CorrelateBy::Topic,
            },
            ..rule.clone()
        };
        assert!(passthrough.is_passthrough());
    }

    #[test]
    fn rule_serde_golden_forms() {
        // The design's config shape: match: always|mentions(str)|topic(str),
        // route: router | passthrough(correlate_by: topic).
        let always_router: SubscriptionRule = serde_json::from_value(
            json!({"channel": "mock-im", "match": "always", "route": "router"}),
        )
        .unwrap();
        assert_eq!(always_router.matcher, RuleMatcher::Always);
        assert_eq!(always_router.route, Route::Router);

        let mentions: SubscriptionRule = serde_json::from_value(
            json!({"channel": "lark", "match": {"mentions": "@kian"}, "route": "router"}),
        )
        .unwrap();
        assert_eq!(mentions.matcher, RuleMatcher::Mentions("@kian".into()));

        let passthrough: SubscriptionRule = serde_json::from_value(json!({
            "channel": "ci",
            "match": {"topic": "worker-1"},
            "route": {"passthrough": {"correlate_by": "topic"}}
        }))
        .unwrap();
        assert_eq!(passthrough.matcher, RuleMatcher::Topic("worker-1".into()));
        assert_eq!(
            passthrough.route,
            Route::Passthrough {
                correlate_by: CorrelateBy::Topic
            }
        );

        // Round trip back to the same golden JSON.
        assert_eq!(
            serde_json::to_value(&passthrough).unwrap(),
            json!({
                "channel": "ci",
                "match": {"topic": "worker-1"},
                "route": {"passthrough": {"correlate_by": "topic"}}
            })
        );
        assert_eq!(
            serde_json::to_value(&always_router).unwrap(),
            json!({"channel": "mock-im", "match": "always", "route": "router"})
        );
    }

    #[test]
    fn first_match_wins_in_declaration_order() {
        let set = SubscriptionRuleSet::new(vec![
            SubscriptionRule {
                channel: "mock-im".into(),
                matcher: RuleMatcher::Topic("ci".into()),
                route: Route::Passthrough {
                    correlate_by: CorrelateBy::Topic,
                },
            },
            SubscriptionRule {
                channel: "mock-im".into(),
                matcher: RuleMatcher::Always,
                route: Route::Router,
            },
        ]);
        let e = event("mock-im", Some("ci"), "build done");
        assert!(set.first_match(&e).unwrap().is_passthrough());
        let other = event("mock-im", Some("docs"), "hello");
        assert!(!set.first_match(&other).unwrap().is_passthrough());
        let unknown = event("github", None, "pr");
        assert!(set.first_match(&unknown).is_none());
        assert_eq!(set.len(), 2);
        assert!(!set.is_empty());
    }

    // -----------------------------------------------------------------
    // admission classes
    // -----------------------------------------------------------------

    use super::{
        AdmissionClass, AdmissionClassRule, AdmissionClassTable, event_has_explicit_signal,
    };

    fn provenance_event(
        channel: &str,
        topic: Option<&str>,
        sender: Option<serde_json::Value>,
    ) -> ChannelEvent {
        ChannelEvent {
            channel: channel.into(),
            event_id: "evt-class".into(),
            topic: topic.map(str::to_string),
            sender,
            group: None,
            payload: json!({}),
            ts_ms: 1,
        }
    }

    /// The class table resolves most-specific-wins deterministically
    /// (table-driven): more pinned key components win; a tie resolves
    /// to the earliest declared entry; no match (and an empty table)
    /// is Candidate — today's behavior.
    #[test]
    fn class_table_resolves_most_specific_wins() {
        let table = AdmissionClassTable::new(vec![
            // specificity 0: the channel default
            AdmissionClassRule {
                channel: "lark".into(),
                topic: None,
                sender: None,
                class: AdmissionClass::ExplicitOnly,
            },
            // specificity 1: one noisy topic is ignored outright
            AdmissionClassRule {
                channel: "lark".into(),
                topic: Some("ci-noise".into()),
                sender: None,
                class: AdmissionClass::Ignore,
            },
            // specificity 2: one human sender in that topic is a candidate
            AdmissionClassRule {
                channel: "lark".into(),
                topic: Some("ci-noise".into()),
                sender: Some(json!({"id": "u-1"})),
                class: AdmissionClass::Candidate,
            },
            // specificity 1 (sender only, any topic)
            AdmissionClassRule {
                channel: "lark".into(),
                topic: None,
                sender: Some(json!({"id": "u-bot"})),
                class: AdmissionClass::Ignore,
            },
        ]);

        struct Case {
            name: &'static str,
            topic: Option<&'static str>,
            sender: Option<serde_json::Value>,
            expected: AdmissionClass,
        }
        let cases = [
            // topic+sender exact: specificity 2 beats both 1s and 0
            Case {
                name: "topic+sender pin wins",
                topic: Some("ci-noise"),
                sender: Some(json!({"id": "u-1"})),
                expected: AdmissionClass::Candidate,
            },
            // topic pin only: the 1s match; topic entry (declared
            // first among 1s... sender-bot also matches at 1) — both
            // specificity 1, earliest declared wins → Ignore (ci-noise)
            Case {
                name: "specificity tie → earliest declared",
                topic: Some("ci-noise"),
                sender: Some(json!({"id": "u-other"})),
                expected: AdmissionClass::Ignore,
            },
            // no topic: only the 0 and the sender-bot (1) match → bot
            // rule wins for u-bot
            Case {
                name: "sender-only pin beats channel default",
                topic: None,
                sender: Some(json!({"id": "u-bot"})),
                expected: AdmissionClass::Ignore,
            },
            // channel default
            Case {
                name: "channel default",
                topic: None,
                sender: Some(json!({"id": "u-2"})),
                expected: AdmissionClass::ExplicitOnly,
            },
        ];
        for case in cases {
            let event = provenance_event("lark", case.topic, case.sender.clone());
            assert_eq!(
                table.resolve(&event),
                case.expected,
                "case `{}` resolved wrong",
                case.name
            );
        }

        // Unkeyed channel → Candidate (no entry matches).
        assert_eq!(
            table.resolve(&provenance_event("github", None, None)),
            AdmissionClass::Candidate
        );
        // Empty table → Candidate (absent classes = today's behavior).
        assert_eq!(
            AdmissionClassTable::default().resolve(&provenance_event(
                "lark",
                Some("ci-noise"),
                None
            )),
            AdmissionClass::Candidate
        );
    }

    /// The explicit-request predicate: @-mention in `text`, or the
    /// adapter-declared `explicit: true` payload field. Both signals,
    /// and their absence.
    #[test]
    fn explicit_signal_is_mention_or_adapter_declared() {
        fn ev(payload: serde_json::Value) -> ChannelEvent {
            ChannelEvent {
                channel: "lark".into(),
                event_id: "evt-x".into(),
                topic: None,
                sender: None,
                group: None,
                payload,
                ts_ms: 1,
            }
        }
        assert!(event_has_explicit_signal(&ev(
            json!({"text": "hey @kian deploy"})
        )));
        assert!(event_has_explicit_signal(&ev(
            json!({"text": "@team status?"})
        )));
        assert!(event_has_explicit_signal(&ev(json!({"explicit": true}))));
        // Absences: plain text, a trailing bare @, explicit not-true,
        // no text at all.
        assert!(!event_has_explicit_signal(&ev(
            json!({"text": "just chatting"})
        )));
        assert!(!event_has_explicit_signal(&ev(
            json!({"text": "email me@"})
        )));
        assert!(!event_has_explicit_signal(&ev(json!({"explicit": false}))));
        assert!(!event_has_explicit_signal(&ev(json!({"build": "passed"}))));
    }

    /// The class vocabulary round-trips through the config serde form
    /// (`candidate | explicit-only | ignore`, kebab-case).
    #[test]
    fn admission_class_serde_golden_forms() {
        assert_eq!(
            serde_json::to_value(AdmissionClass::Candidate).unwrap(),
            json!("candidate")
        );
        assert_eq!(
            serde_json::to_value(AdmissionClass::ExplicitOnly).unwrap(),
            json!("explicit-only")
        );
        assert_eq!(
            serde_json::to_value(AdmissionClass::Ignore).unwrap(),
            json!("ignore")
        );
        let rule: AdmissionClassRule = serde_json::from_value(json!({
            "channel": "lark",
            "topic": "ci-noise",
            "sender": {"id": "u-1"},
            "class": "explicit-only",
        }))
        .unwrap();
        assert_eq!(rule.class, AdmissionClass::ExplicitOnly);
        assert_eq!(rule.specificity(), 2);
        // Optional keys default to None (channel-only entries).
        let bare: AdmissionClassRule =
            serde_json::from_value(json!({"channel": "lark", "class": "ignore"})).unwrap();
        assert_eq!(bare.topic, None);
        assert_eq!(bare.sender, None);
    }

    // -----------------------------------------------------------------
    // Route::Session — additive serde golden
    // -----------------------------------------------------------------

    /// The `Route::Session` variant is strictly additive: the two
    /// pre-existing route forms parse and serialize byte-identically,
    /// and the new `{"session": "<uuid>"}` form round-trips. Old
    /// route JSON (a config file from before) parses unchanged.
    #[test]
    fn route_session_serde_is_additive_and_old_forms_are_unchanged() {
        use kian_core::model::SessionId;

        let uuid = "0b6e1e02-7da4-4a76-b4b3-9a1f1fd64d01";
        let sid = SessionId(uuid::Uuid::parse_str(uuid).unwrap());

        // The legacy golden forms, byte-for-byte.
        assert_eq!(
            serde_json::to_value(Route::Router).unwrap(),
            json!("router")
        );
        assert_eq!(
            serde_json::to_value(Route::Passthrough {
                correlate_by: CorrelateBy::Topic
            })
            .unwrap(),
            json!({"passthrough": {"correlate_by": "topic"}})
        );
        // Old JSON parses unchanged (additions-only proof).
        let old_router: Route = serde_json::from_value(json!("router")).unwrap();
        assert_eq!(old_router, Route::Router);
        let old_passthrough: Route =
            serde_json::from_value(json!({"passthrough": {"correlate_by": "topic"}})).unwrap();
        assert_eq!(
            old_passthrough,
            Route::Passthrough {
                correlate_by: CorrelateBy::Topic
            }
        );

        // The new form parses and round-trips.
        let session: Route = serde_json::from_value(json!({"session": uuid})).unwrap();
        assert_eq!(session, Route::Session(sid));
        assert_eq!(
            serde_json::to_value(&session).unwrap(),
            json!({"session": uuid})
        );
        assert!(!session.is_passthrough());

        // A full rule with the session route round-trips too.
        let rule: SubscriptionRule = serde_json::from_value(json!({
            "channel": "timer",
            "match": {"topic": "watchdog"},
            "route": {"session": uuid}
        }))
        .unwrap();
        assert_eq!(rule.route, Route::Session(sid));
        assert_eq!(
            serde_json::to_value(&rule).unwrap(),
            json!({
                "channel": "timer",
                "match": {"topic": "watchdog"},
                "route": {"session": uuid}
            })
        );

        // A malformed session id fails closed, never a default.
        assert!(serde_json::from_value::<Route>(json!({"session": "not-a-uuid"})).is_err());
    }
}
