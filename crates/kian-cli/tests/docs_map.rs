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
