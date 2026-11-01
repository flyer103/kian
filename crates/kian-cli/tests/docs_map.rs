//! Docs-map consistency check (card C17 closeout): every relative
//! link in the authored docs (README, AGENTS, SPEC, DESIGN, ADRs, the
//! materials digest) resolves to a file that exists. Frozen reference
//! snapshots in `a vendored archive/` are archived source material, not
//! authored text — their outbound links are not our contract.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Authored markdown surfaces of the docs map (AGENTS.md §Docs map).
const AUTHORED: [&str; 5] = [
    "README.md",
    "AGENTS.md",
    "docs/SPEC.md",
    "design/DESIGN.md",
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
fn adr_series_is_contiguous_from_001() {
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
            seen.insert(n);
        }
    }
    assert!(!seen.is_empty(), "no ADRs found");
    let max = *seen.iter().next_back().unwrap();
    for n in 1..=max {
        assert!(seen.contains(&n), "ADR-{n:03} missing from the series");
    }
}

/// Every workspace crate is mapped in the docs (Phase-2 closeout): the
/// crate map in DESIGN §7 and the README must each name all of
/// `crates/*` — Phase 2 added `kian-exec`, `kian-superagent` and
/// `kiand`, and a future crate that forgets the docs map fails here
/// rather than silently disappearing from the documentation.
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
        "expected the 11-member Phase-2 workspace, found {crates:?}"
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

/// Phase-3 closeout (C45): the SPEC §5 Phase-3 table must name every
/// capability the round landed, and the Phase-4 deferral table must
/// carry the moved items. The SPEC is the contract — a capability
/// that ships without a row here has silently left the contract, and
/// a deferred item that stays in the Phase-3 table misstates scope.
#[test]
fn spec_phase3_table_names_each_landed_capability() {
    let root = repo_root();
    let spec = std::fs::read_to_string(root.join("docs/SPEC.md")).expect("SPEC.md");

    // The §5 Phase-3 section: from its heading to the next heading of
    // any level (Phase 4 / Deferred are separate sections).
    let start = spec.find("### Phase 3").expect("SPEC §5 Phase-3 heading");
    let end = spec[start..]
        .find("\n#")
        .map(|i| start + i)
        .unwrap_or(spec.len());
    let section = &spec[start..end];

    // One keyword per landed round-3 card (C31–C44) plus the S7
    // record; each must appear in the Phase-3 section's text.
    let landed = [
        "Inline/Spill",
        "Recall toolpack",
        "Two-level",
        "`start()`/`stop()`",
        "fan-out",
        "Speculative",
        "Watchdog Session v2",
        "Batching metrics",
        "Ablation",
        "Untrusted-input marking",
        "lark channel adapter",
        "(S7)",
    ];
    let missing: Vec<&str> = landed
        .iter()
        .copied()
        .filter(|c| !section.contains(c))
        .collect();
    assert!(
        missing.is_empty(),
        "SPEC §5 Phase-3 table is missing landed capabilities: {missing:?}"
    );
    assert!(
        section.contains("**demonstrated**"),
        "S7 must be recorded as demonstrated in the Phase-3 table"
    );

    // The Phase-4 deferral table carries the moved items with their
    // justifications (round-3 plan §3).
    let phase4_start = spec.find("### Phase 4").expect("SPEC Phase-4 heading");
    let phase4 = &spec[phase4_start..];
    for item in ["Postgres", "dsh", "kian-sdk", "GitHub adapter", "A2A"] {
        assert!(
            phase4.contains(item),
            "SPEC Phase-4 deferral table is missing `{item}`"
        );
    }
}
