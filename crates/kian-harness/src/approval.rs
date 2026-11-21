//! The declarative approval policy (HARNESS-BRIDGE §4.4 / §6.3).
//!
//! A harness that gates a tool asks kian; kian answers from config. Phase 1 supports `allow` and
//! `deny` only: `ask` refuses boot because a human answer cannot be delivered to a harness
//! process that died with the drain (see HARNESS-BRIDGE §10). Matching is first-match-wins over
//! an ordered rule list; no match denies (fail-closed).

use serde_json::Value;

/// One decision a rule (or the default) can return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny,
    /// Defer to a human (durable ask, ADR-050): the call is recorded as pending and the drain
    /// ends `awaiting_input`. Only an opt-in harness config may produce this decision.
    Ask,
}

/// One ordered rule: a tool name, optionally narrowed by an argument glob over the canonical
/// JSON of the tool input (so a command match is written `Bash(*git status*)`, not
/// `Bash(git status*)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    pub tool: String,
    pub args_glob: Option<String>,
    pub decision: Decision,
}

impl Rule {
    /// Parses the config form: `Tool` or `Tool(args-glob)`.
    ///
    /// An unparsable pattern is an error (boot refuses), never a silently ignored rule.
    pub fn parse(text: &str, decision: Decision) -> Result<Self, String> {
        let text = text.trim();
        if text.is_empty() {
            return Err("approval rule is empty".to_owned());
        }
        let (tool, args_glob) = match (text.find('('), text.ends_with(')')) {
            (None, _) => (text.to_owned(), None),
            (Some(open), true) if open > 0 => {
                let tool = &text[..open];
                let args = &text[open + 1..text.len() - 1];
                if tool.trim().is_empty() || args.trim().is_empty() {
                    return Err(format!("approval rule `{text}` has an empty part"));
                }
                (tool.trim().to_owned(), Some(args.trim().to_owned()))
            }
            _ => {
                return Err(format!(
                    "approval rule `{text}` is not `Tool` or `Tool(args)`"
                ));
            }
        };
        if tool.is_empty()
            || !tool
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "_-.:".contains(c))
        {
            return Err(format!("approval rule `{text}` has an invalid tool name"));
        }
        Ok(Self {
            tool,
            args_glob,
            decision,
        })
    }

    fn matches(&self, tool: &str, input: &Value) -> bool {
        if tool != self.tool {
            return false;
        }
        match &self.args_glob {
            None => true,
            Some(glob) => {
                let text = serde_json::to_string(input).unwrap_or_default();
                glob_matches(glob, &text)
            }
        }
    }
}

/// The policy: a default plus ordered rules; first match wins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalPolicy {
    pub default: Decision,
    pub rules: Vec<Rule>,
}

impl ApprovalPolicy {
    /// Deny-by-default with no rules (the fail-closed baseline).
    #[must_use]
    pub fn deny_all() -> Self {
        Self {
            default: Decision::Deny,
            rules: Vec::new(),
        }
    }

    /// The decision for one gated tool: `(decision, matched label)`.
    ///
    /// `matched` is the rule text when a rule decided, `"default"` otherwise; it is the value
    /// recorded as `tool/call.permission.matched`.
    #[must_use]
    pub fn decide(&self, tool: &str, input: &Value) -> (Decision, String) {
        for rule in &self.rules {
            if rule.matches(tool, input) {
                let label = match &rule.args_glob {
                    Some(args) => format!("{}({args})", rule.tool),
                    None => rule.tool.clone(),
                };
                return (rule.decision, label);
            }
        }
        (self.default, "default".to_owned())
    }
}

/// A tiny glob: `*` matches any run of characters, everything else is literal.
fn glob_matches(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut mark) = (None, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(star_pos) = star {
            pi = star_pos + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rule(text: &str, decision: Decision) -> Rule {
        Rule::parse(text, decision).expect("parses")
    }

    #[test]
    fn rules_parse_and_match_first_wins() {
        let policy = ApprovalPolicy {
            default: Decision::Deny,
            rules: vec![
                rule("Bash(*git status*)", Decision::Allow),
                rule("Bash", Decision::Deny),
                rule("Read", Decision::Allow),
            ],
        };
        assert_eq!(
            policy.decide("Bash", &json!({"command": "git status --short"})),
            (Decision::Allow, "Bash(*git status*)".to_owned())
        );
        assert_eq!(
            policy.decide("Bash", &json!({"command": "rm -rf /"})),
            (Decision::Deny, "Bash".to_owned())
        );
        assert_eq!(
            policy.decide("Read", &json!({"path": "x"})),
            (Decision::Allow, "Read".to_owned())
        );
        assert_eq!(
            policy.decide("Write", &json!({})),
            (Decision::Deny, "default".to_owned())
        );
    }

    #[test]
    fn unparsable_rules_are_errors_not_silent_noops() {
        for text in ["", "  ", "Bash(", "Bash( )", "(x)", "Bash(x) tail", "B!ash"] {
            assert!(
                Rule::parse(text, Decision::Allow).is_err(),
                "must refuse: {text:?}"
            );
        }
    }

    #[test]
    fn globbing_is_literal_outside_star() {
        assert!(glob_matches("git status*", "git status --short"));
        assert!(glob_matches("*", "anything"));
        assert!(!glob_matches("git diff*", "git status"));
        assert!(glob_matches("a*b*c", "axxbyyc"));
    }
}
