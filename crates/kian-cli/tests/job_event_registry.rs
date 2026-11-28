//! JOB-MODEL §18.4: "a registry test: every event kind the conductor
//! writes is one of EVENT-MODEL's 24" — checked here, not asserted in
//! prose. The catalog stays normative in `design/EVENT-MODEL.md`; this
//! test reads it, then reads the production (pre-`#[cfg(test)]`) part of
//! every file that carries job semantics and proves every kind it names
//! is catalogued.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

/// The files that carry the job surface: the conductor and its
/// projections, the alert plan, and the CLI wiring (admission, cancel,
/// run-verdict). A new production `EventKind::` in any of them must be
/// catalogued and deliberate.
const JOB_RUNTIME_FILES: &[&str] = &[
    "crates/kian-superagent/src/job/conduct.rs",
    "crates/kian-superagent/src/job/status.rs",
    "crates/kian-superagent/src/job/alert.rs",
    "crates/kian-cli/src/job_runtime.rs",
    "crates/kian-cli/src/job_cli.rs",
    "crates/kian-cli/src/job_config.rs",
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../")
}

/// The §3 kind table: `| `kind` | ...` rows.
fn catalog_kinds(model: &str) -> BTreeSet<String> {
    let start = model
        .find("## 3. Kind catalog")
        .expect("EVENT-MODEL §3 kind catalog heading");
    let end = model[start..]
        .find("\n## 4.")
        .map(|i| start + i)
        .unwrap_or(model.len());
    let mut kinds = BTreeSet::new();
    for line in model[start..end].lines() {
        let Some(rest) = line.trim_start().strip_prefix("| `") else {
            continue;
        };
        let Some(close) = rest.find('`') else {
            continue;
        };
        kinds.insert(rest[..close].to_string());
    }
    kinds
}

/// `EventKind` variant name → its serde `kind/name` tag, read from the
/// enum body (the same pairing `docs_map` uses for the count).
fn variant_kinds(events_rs: &str) -> BTreeMap<String, String> {
    let start = events_rs
        .find("pub enum EventKind {")
        .expect("EventKind enum");
    let body = &events_rs[start..];
    let body = body.find("\n}\n").map(|i| &body[..i]).unwrap_or(body);
    let mut pairs = BTreeMap::new();
    let mut pending: Option<String> = None;
    for line in body.lines() {
        let trimmed = line.trim_start();
        if let Some(pos) = trimmed.find("#[serde(rename = \"") {
            let rest = &trimmed[pos + "#[serde(rename = \"".len()..];
            if let Some(close) = rest.find('\"') {
                pending = Some(rest[..close].to_string());
            }
            continue;
        }
        let Some(kind) = pending.clone() else {
            continue;
        };
        let variant: String = trimmed
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if variant.chars().next().is_some_and(|c| c.is_uppercase()) {
            pairs.insert(variant, kind);
            pending = None;
        }
    }
    pairs
}

fn named_kinds(production: &str) -> BTreeSet<String> {
    let mut named = BTreeSet::new();
    let needle = "EventKind::";
    for (idx, _) in production.match_indices(needle) {
        let rest = &production[idx + needle.len()..];
        let variant: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        named.insert(variant);
    }
    named
}

fn production_part(text: &str) -> &str {
    match text.find("#[cfg(test)]") {
        Some(idx) => &text[..idx],
        None => text,
    }
}

#[test]
fn the_job_runtime_only_names_catalogued_kinds() {
    let root = repo_root();
    let model = std::fs::read_to_string(root.join("design/EVENT-MODEL.md"))
        .expect("design/EVENT-MODEL.md is the normative event contract");
    let events_rs = std::fs::read_to_string(root.join("crates/kian-core/src/events.rs"))
        .expect("crates/kian-core/src/events.rs");

    let catalog = catalog_kinds(&model);
    assert_eq!(catalog.len(), 24, "EVENT-MODEL §3 must list 24 kinds");
    let by_variant = variant_kinds(&events_rs);

    let mut named: BTreeSet<String> = BTreeSet::new();
    for rel in JOB_RUNTIME_FILES {
        let text = std::fs::read_to_string(root.join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"));
        named.extend(named_kinds(production_part(&text)));
    }

    // The deliberate set. A new entry here is a design change: the
    // conductor may only write the existing carrier facts (a child
    // dispatch, a delivery, a verdict, an end) plus the admission rows
    // the alert path builds — never a new kind (JOB-MODEL §14). Read-only
    // entries: the child's final message is the artifact a step harvests
    // (JOB-MODEL §12.1); `DrainClaimed` tells a claimed-but-unsettled run
    // from a released one in the §15.1 cancellation-pending projection.
    let expected: BTreeSet<String> = [
        "AssistantMessage",
        "ChannelDelivered",
        "ControlInterrupt",
        "DrainClaimed",
        "EvidenceVerdict",
        "InputAdmitted",
        "SessionEnded",
        "ThreadStarted",
        "ToolCall",
        "ToolResult",
    ]
    .iter()
    .map(|kind| (*kind).to_string())
    .collect();
    let unexpected: Vec<&String> = named.difference(&expected).collect();
    let missing: Vec<&String> = expected.difference(&named).collect();
    assert!(
        unexpected.is_empty() && missing.is_empty(),
        "the job surface's kind set changed: unexpected={unexpected:?} missing={missing:?}"
    );

    for variant in &named {
        let Some(kind) = by_variant.get(variant) else {
            panic!("the job surface names EventKind::{variant}, which the enum does not declare");
        };
        assert!(
            catalog.contains(kind),
            "the job surface names EventKind::{variant} ({kind}), which the catalog does not list"
        );
    }
}

#[test]
fn the_catalog_parse_sees_every_kind_the_enum_declares() {
    let root = repo_root();
    let model = std::fs::read_to_string(root.join("design/EVENT-MODEL.md")).expect("model");
    let events_rs =
        std::fs::read_to_string(root.join("crates/kian-core/src/events.rs")).expect("events");
    let catalog = catalog_kinds(&model);
    let by_variant = variant_kinds(&events_rs);
    let coded: BTreeSet<&String> = by_variant.values().collect();
    let documented: BTreeSet<&String> = catalog.iter().collect();
    assert_eq!(
        coded, documented,
        "variant/kind pairing must agree with the catalog; update design/EVENT-MODEL.md in the same change"
    );
}
