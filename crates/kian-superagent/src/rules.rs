//! Declarative subscription rules (DESIGN §4.1, card C23; Footprint
//! Ladder level 1 — pure config, no code).
//!
//! A rule names a channel, a match condition on inbound
//! [`ChannelEvent`]s, and a route: the resident Router session, or a
//! passthrough correlated by topic (CI-style events of a worker's own
//! topic flow straight into that worker's inbox without Router
//! judgment — the DESIGN §4.1 load-reduction device). The passthrough
//! `topic → worker session` mapping lives in
//! [`crate::registry::PassthroughRegistry`], a disposable projection.

use kian_core::seams::ChannelEvent;
use serde::{Deserialize, Serialize};

/// Match condition on an inbound channel event. Serde form (externally
/// tagged, the card's `match: always|mentions(str)|topic(str)`):
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
/// `"router"` or `{"passthrough": {"correlate_by": "topic"}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Route {
    #[serde(rename = "router")]
    Router,
    #[serde(rename = "passthrough")]
    Passthrough { correlate_by: CorrelateBy },
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
        // The card's config shape: match: always|mentions(str)|topic(str),
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
}
