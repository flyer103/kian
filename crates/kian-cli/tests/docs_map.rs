//! Docs-map consistency check (closeout): every relative
//! link in the authored docs (README, AGENTS, SPEC, DESIGN, ADRs, the
//! materials digest) resolves to a file that exists. Frozen reference
//! snapshots in `a vendored archive/` are archived source material, not
//! authored text — their outbound links are not our contract.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Authored markdown surfaces of the docs map (AGENTS.md §Docs map).
/// `design/EVENT-MODEL.md` is the normative event contract, so its
/// outbound links are part of the enforced set too.
const AUTHORED: [&str; 11] = [
    "README.md",
    "AGENTS.md",
    "docs/SPEC.md",
    "design/DESIGN.md",
    "design/EVENT-MODEL.md",
    "design/API-CONTRACT.md",
    "design/MEMORY-MODEL.md",
    "design/READ-MODEL.md",
    "design/CHANNEL-WECHAT.md",
    "design/WORKER-MODEL.md",
    "the design notes",
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../")
        .canonicalize()
        .expect("repo root")
}

/// Collects relative link targets from one markdown file (inline
/// `[text](target)` links — the only form the authored docs use).
fn relative_targets(file: &Path) -> Vec<String> {
    let text = std::fs::read_to_string(file).unwrap_or_default();
    let mut out = Vec::new();
    let mut rest = text.as_str();
    // `](` immediately adjacent is what makes an inline link; a `]`
    // and a later `(` are just prose.
    while let Some(idx) = rest.find("](") {
        let after = &rest[idx + 2..];
        let Some(close) = after.find(')') else {
            break;
        };
        let target = after[..close].split('#').next().unwrap_or("").trim();
        if !target.is_empty()
            && !target.starts_with("http://")
            && !target.starts_with("https://")
            && !target.starts_with("mailto:")
        {
            out.push(target.to_string());
        }
        rest = &after[close..];
    }
    out
}

#[test]
fn every_authored_doc_link_resolves() {
    let root = repo_root();
    let mut failures: Vec<String> = Vec::new();
    let mut checked = 0usize;

    let mut files: Vec<PathBuf> = AUTHORED.iter().map(|f| root.join(f)).collect();
    // ADRs are part of the decisions surface.
    let adr_dir = root.join("design/decisions");
    for entry in std::fs::read_dir(&adr_dir).expect("decisions dir") {
        let p = entry.expect("dir entry").path();
        if p.extension().is_some_and(|e| e == "md") {
            files.push(p);
        }
    }

    for file in &files {
        for target in relative_targets(file) {
            checked += 1;
            let resolved = file.parent().unwrap_or(&root).join(&target);
            if !resolved.exists() {
                failures.push(format!(
                    "{} -> {} (resolved {})",
                    file.strip_prefix(&root).unwrap_or(file).display(),
                    target,
                    resolved.display()
                ));
            }
        }
    }

    assert!(
        failures.is_empty(),
        "docs-map consistency: {} dangling links:\n{}",
        failures.len(),
        failures.join("\n")
    );
    // The docs map must have something to check — a silently empty
    // pass would be vacuous.
    assert!(checked >= 10, "only {checked} links checked — suspicious");
}

/// The ADR series is append-only and contiguous (no gaps that would
/// silently break cross-references like "see ADR-006").
#[test]
fn adr_series_is_contiguous_from_001_and_unique() {
    let root = repo_root();
    let mut seen: BTreeSet<u32> = BTreeSet::new();
    for entry in std::fs::read_dir(root.join("design/decisions")).expect("decisions dir") {
        let name = entry.expect("dir entry").file_name();
        let name = name.to_string_lossy();
        if let Some(n) = name
            .strip_prefix("ADR-")
            .and_then(|r| r.split('-').next())
            .and_then(|n| n.parse::<u32>().ok())
        {
            // Append-only numbering: a second file claiming a taken number is
            // a merge-order accident (two branches mint the same ADR in
            // parallel), and it silently makes every prose reference
            // ambiguous — so the series is also unique.
            assert!(
                seen.insert(n),
                "ADR-{n:03} is claimed by more than one file; the next ADR takes the next free number"
            );
        }
    }
    assert!(!seen.is_empty(), "no ADRs found");
    let max = *seen.iter().next_back().unwrap();
    for n in 1..=max {
        assert!(seen.contains(&n), "ADR-{n:03} missing from the series");
    }
}

/// Every workspace crate is mapped in the docs: the crate map in
/// DESIGN §7 and the README must each name all of `crates/*` — the
/// workspace includes `kian-exec`, `kian-superagent` and `kiand`, and a
/// future crate that forgets the docs map fails here rather than
/// silently disappearing from the documentation.
#[test]
fn every_workspace_crate_is_mapped_in_the_docs() {
    let root = repo_root();
    let design = std::fs::read_to_string(root.join("design/DESIGN.md")).expect("DESIGN.md");
    let readme = std::fs::read_to_string(root.join("README.md")).expect("README.md");

    let mut crates: Vec<String> = std::fs::read_dir(root.join("crates"))
        .expect("crates dir")
        .map(|entry| {
            entry
                .expect("dir entry")
                .file_name()
                .to_string_lossy()
                .to_string()
        })
        .filter(|name| root.join("crates").join(name).is_dir())
        .collect();
    crates.sort();

    assert!(
        crates.len() >= 11,
        "expected the workspace crates, found {crates:?}"
    );
    let mut failures = Vec::new();
    for name in &crates {
        if !design.contains(name.as_str()) {
            failures.push(format!(
                "{name}: missing from design/DESIGN.md (§7 crate map)"
            ));
        }
        if !readme.contains(name.as_str()) {
            failures.push(format!("{name}: missing from README.md"));
        }
    }
    assert!(
        failures.is_empty(),
        "docs map out of sync with the workspace: {}",
        failures.join("; ")
    );
}

/// The SPEC §5 capability table must name every landed capability, and
/// the deferral table must carry the moved items. The SPEC is the
/// contract — a capability that ships without a row here has silently
/// left the contract, and a deferred item that stays in a capability
/// table misstates scope.
#[test]
fn spec_capability_table_names_each_landed_capability() {
    let root = repo_root();
    let spec = std::fs::read_to_string(root.join("docs/SPEC.md")).expect("SPEC.md");

    // The §5 recall/channels capability section: from its heading to the
    // next heading of any level (the hosted set and Deferred are separate
    // sections).
    let start = spec
        .find("### Recall, admission economics, channels")
        .expect("SPEC §5 recall/channels heading");
    let end = spec[start..]
        .find("\n#")
        .map(|i| start + i)
        .unwrap_or(spec.len());
    let section = &spec[start..end];

    // One keyword per capability recorded in this section — the landed
    // set plus the round-10 WeChat row (whose status reads `in progress`
    // until the closeout flips it); the audit-export record carries its
    // own keyword. Each must appear in the section text.
    let landed = [
        "Inline/Spill",
        "Recall toolpack",
        "Two-level",
        "`start`/`stop`",
        "fan-out",
        "Speculative",
        "Watchdog Session v2",
        "Batching metrics",
        "Ablation",
        "Untrusted-input marking",
        "lark channel adapter",
        "WeChat iLink channel",
        "Audit export/replay tooling",
    ];
    let missing: Vec<&str> = landed
        .iter()
        .copied()
        .filter(|c| !section.contains(c))
        .collect();
    assert!(
        missing.is_empty(),
        "SPEC §5 capability table is missing landed capabilities: {missing:?}"
    );
    assert!(
        section.contains("**Demonstrated**"),
        "audit export/replay must be recorded as demonstrated"
    );

    // The hosted-set table carries the moved items with their
    // justifications.
    let hosted_start = spec
        .find("### Hosted foundation, SDK freeze, ecosystem surfaces")
        .expect("SPEC hosted-set heading");
    let hosted = &spec[hosted_start..];
    for item in ["Postgres", "dsh", "kian-sdk", "GitHub adapter", "A2A"] {
        assert!(
            hosted.contains(item),
            "SPEC hosted-set table is missing `{item}`"
        );
    }
}

/// The event catalog in `design/EVENT-MODEL.md` is the normative source
/// of truth; `EventKind` is its executable form. This test fails on any
/// drift: a kind in the enum but not the catalog, a kind in the catalog
/// but not the enum, or a changed kind count. Adding/removing an event
/// kind therefore requires updating the catalog in the same change.
#[test]
fn event_model_catalog_matches_core_kinds() {
    let root = repo_root();
    let model = std::fs::read_to_string(root.join("design/EVENT-MODEL.md"))
        .expect("design/EVENT-MODEL.md is the normative event contract");
    let events_rs = std::fs::read_to_string(root.join("crates/kian-core/src/events.rs"))
        .expect("crates/kian-core/src/events.rs");

    // Catalog rows live in the §3 kind table: `| \`kind\` | ...`.
    let start = model
        .find("## 3. Kind catalog")
        .expect("EVENT-MODEL §3 kind catalog heading");
    let end = model[start..]
        .find("\n## 4.")
        .map(|i| start + i)
        .unwrap_or(model.len());
    let catalog = &model[start..end];
    let mut documented: BTreeSet<String> = BTreeSet::new();
    for line in catalog.lines() {
        let Some(rest) = line.trim_start().strip_prefix("| `") else {
            continue;
        };
        let Some(close) = rest.find('`') else {
            continue;
        };
        let kind = &rest[..close];
        documented.insert(kind.to_string());
    }

    // Executable form: the `#[serde(rename = "kind/name")]` tags on
    // `EventKind` (the golden tests pin them to `as_str`).
    let mut coded: BTreeSet<String> = BTreeSet::new();
    for line in events_rs.lines() {
        let Some(pos) = line.find("#[serde(rename = \"") else {
            continue;
        };
        let rest = &line[pos + "#[serde(rename = \"".len()..];
        let Some(close) = rest.find('"') else {
            continue;
        };
        let kind = &rest[..close];
        coded.insert(kind.to_string());
    }

    assert_eq!(
        coded.len(),
        22,
        "expected 22 EventKind variants, got {coded:?}"
    );
    assert_eq!(
        documented.len(),
        coded.len(),
        "EVENT-MODEL catalog has {} kinds but EventKind has {}; \
         update design/EVENT-MODEL.md in the same change: missing={:?} extra={:?}",
        documented.len(),
        coded.len(),
        coded.difference(&documented).collect::<Vec<_>>(),
        documented.difference(&coded).collect::<Vec<_>>()
    );
    assert_eq!(
        documented,
        coded,
        "EVENT-MODEL catalog and EventKind drift: missing={:?} extra={:?}",
        coded.difference(&documented).collect::<Vec<_>>(),
        documented.difference(&coded).collect::<Vec<_>>()
    );

    // The change protocol must stay discoverable from the contract.
    assert!(
        model.contains("## 6. Change protocol"),
        "EVENT-MODEL must keep the change-protocol section"
    );
}

/// The control/ops method catalog in `design/API-CONTRACT.md` is the
/// normative wire contract; the JSON-RPC dispatch arms in `kian-cli`
/// are its executable form. This test fails on drift in either
/// direction: adding, removing or renaming a method therefore requires
/// updating the contract in the same change.
#[test]
fn api_contract_method_catalog_matches_dispatch() {
    let root = repo_root();
    let contract = std::fs::read_to_string(root.join("design/API-CONTRACT.md"))
        .expect("design/API-CONTRACT.md is the normative API contract");

    let mut documented = catalog_methods(&contract, "### 3.1 Method catalog");
    documented.extend(catalog_methods(&contract, "### 4.1 Method catalog"));

    let mut coded: BTreeSet<String> = BTreeSet::new();
    for file in ["crates/kian-cli/src/api.rs", "crates/kian-cli/src/ops.rs"] {
        let src =
            std::fs::read_to_string(root.join(file)).unwrap_or_else(|e| panic!("read {file}: {e}"));
        coded.extend(dispatch_methods(&src));
    }

    assert_eq!(
        coded.len(),
        10,
        "expected 10 JSON-RPC methods (6 control + 4 ops), got {coded:?}"
    );
    assert_eq!(
        documented,
        coded,
        "API-CONTRACT catalog and dispatch drift: missing={:?} extra={:?}",
        coded.difference(&documented).collect::<Vec<_>>(),
        documented.difference(&coded).collect::<Vec<_>>()
    );

    // The change protocol must stay discoverable from the contract.
    assert!(
        contract.contains("## 10. Change protocol"),
        "API-CONTRACT must keep the change-protocol section"
    );
}

/// Method rows of one `### ... Method catalog` table: the first cell is
/// backticked (`| \`method\` | ...`) and the section ends at the next
/// `###` heading.
fn catalog_methods(contract: &str, heading: &str) -> BTreeSet<String> {
    let start = contract
        .find(heading)
        .unwrap_or_else(|| panic!("missing `{heading}` in design/API-CONTRACT.md"));
    let end = contract[start..]
        .find("\n### ")
        .map(|i| start + i)
        .unwrap_or(contract.len());
    let mut methods = BTreeSet::new();
    for line in contract[start..end].lines() {
        let Some(rest) = line.trim_start().strip_prefix("| `") else {
            continue;
        };
        let Some(close) = rest.find('`') else {
            continue;
        };
        methods.insert(rest[..close].to_string());
    }
    methods
}

/// The `"method" =>` arms of the handler module's `match method {`.
fn dispatch_methods(src: &str) -> BTreeSet<String> {
    let start = src
        .find("match method {")
        .expect("dispatch must match on `method`");
    let mut methods = BTreeSet::new();
    for line in src[start..].lines().skip(1) {
        let line = line.trim();
        if line.starts_with("other =>") {
            break;
        }
        let Some(rest) = line.strip_prefix('"') else {
            continue;
        };
        let Some(close) = rest.find('"') else {
            continue;
        };
        if rest[close + 1..].trim_start().starts_with("=>") {
            methods.insert(rest[..close].to_string());
        }
    }
    methods
}
