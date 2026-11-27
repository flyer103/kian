//! The self-evolution surface check (SELF-EVOLUTION-MODEL 5.3, A13).
//!
//! A patch is a unified diff and the check answers one question: does every
//! path the diff would change sit inside the surface the plan declared? The
//! functions here are pure and synchronous: the conductor evaluates them over
//! committed artifact bytes, never over a live checkout.
//!
//! Normalisation is part of the surface. A leading "a/", "b/" or "./" is
//! stripped (git writes one of them on each side of a file header); an
//! absolute path, a path with a ".." segment, or an empty path cannot be
//! inside the surface, so it is refused under the "outside allow" rule with
//! the path quoted as written.

use std::collections::BTreeMap;

/// The most paths one refusal detail names (5.3 bounds the list to 20).
const MAX_NAMED_PATHS: usize = 20;

const OUTSIDE: &str = "outside allow";
const DENIED: &str = "denied";
const TEST_ONLY: &str = "test-only";

/// The verdict one job-owned check carries: the boolean plus the detail a
/// reviewer reads in the step verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurfaceVerdict {
    /// Whether every changed path is inside the surface.
    pub passed: bool,
    /// The reviewer-facing detail: the refused paths and their rule, or the
    /// count of accepted paths.
    pub detail: String,
}

impl SurfaceVerdict {
    fn refused(detail: String) -> Self {
        Self {
            passed: false,
            detail,
        }
    }
}

/// Evaluate patch_paths_allowed over a unified diff (5.3).
///
/// The four conditions are checked in the order the contract states them:
/// the diff parses, every path is repository-relative and inside the surface,
/// and at least one changed path is not a test path (SE8).
#[must_use]
pub fn check_surface(diff: &str, allow: &[String], deny: &[String]) -> SurfaceVerdict {
    let paths = match parse_diff_paths(diff) {
        Ok(paths) => paths,
        Err(reason) => return SurfaceVerdict::refused(format!("unparsable: {reason}")),
    };
    let mut refusals: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();
    let mut normalised: Vec<String> = Vec::new();
    for raw in &paths {
        let Ok(path) = normalise_path(raw) else {
            push_unique(refusals.entry(OUTSIDE).or_default(), raw.clone());
            continue;
        };
        if matches_any(deny, &path) {
            push_unique(refusals.entry(DENIED).or_default(), path);
        } else if matches_any(allow, &path) {
            push_unique(&mut normalised, path);
        } else {
            push_unique(refusals.entry(OUTSIDE).or_default(), path);
        }
    }
    if !refusals.is_empty() {
        return SurfaceVerdict::refused(name_refusals(&refusals));
    }
    if normalised.iter().all(|path| is_test_path(path)) {
        return SurfaceVerdict::refused(format!(
            "{TEST_ONLY}: {}",
            join_names(&normalised, normalised.len())
        ));
    }
    SurfaceVerdict {
        passed: true,
        detail: format!("{} path(s) inside the surface", normalised.len()),
    }
}

/// Every path a unified diff would change, in diff order and without
/// duplicates. Unparsable bytes are an error, never an empty result: a
/// surface that cannot be read has not passed.
///
/// Both sides of a rename or copy are paths; /dev/null is the absent side of a
/// creation or deletion and names nothing. Hunk bodies are skipped by their
/// header counts so an added line that happens to read like a file header is
/// not mistaken for one.
pub fn parse_diff_paths(diff: &str) -> Result<Vec<String>, String> {
    let lines: Vec<&str> = diff.lines().collect();
    let mut paths: Vec<String> = Vec::new();
    let mut index = 0;
    let mut have_file = false;
    while index < lines.len() {
        let line = lines[index];
        if let Some((old, new)) = header_pair(&lines, index) {
            for side in [old, new] {
                if side != "/dev/null" {
                    push_unique(&mut paths, side);
                }
            }
            have_file = true;
            index += 2;
            continue;
        }
        if let Some(path) = line.strip_prefix("--- ") {
            let _ = path;
            return Err("a --- line is not followed by a +++ line".to_string());
        }
        let mut moved = false;
        for prefix in ["rename from ", "copy from ", "rename to ", "copy to "] {
            if let Some(path) = line.strip_prefix(prefix) {
                push_unique(&mut paths, header_path(path));
                have_file = true;
                moved = true;
                break;
            }
        }
        if moved {
            index += 1;
            continue;
        }
        if line.starts_with("@@") {
            if !have_file {
                return Err("a hunk header appears before any file header".to_string());
            }
            let (old_left, new_left) = hunk_counts(line)
                .ok_or_else(|| "a hunk header does not carry its line counts".to_string())?;
            index += 1;
            index = skip_hunk(&lines, index, old_left, new_left);
            continue;
        }
        index += 1;
    }
    if paths.is_empty() {
        return Err("the diff names no path".to_string());
    }
    Ok(paths)
}

/// Strip the diff prefix and the optional tab-separated timestamp git appends
/// in some formats.
fn header_path(raw: &str) -> String {
    raw.split('\t').next().unwrap_or(raw).trim().to_string()
}

/// The two sides of a file header, when this line and the next form one.
fn header_pair(lines: &[&str], index: usize) -> Option<(String, String)> {
    let old = lines[index].strip_prefix("--- ")?;
    let new = lines.get(index + 1)?.strip_prefix("+++ ")?;
    Some((header_path(old), header_path(new)))
}

/// Skip one hunk body by its counts, tolerating a body that ends early at a
/// new file header.
fn skip_hunk(lines: &[&str], mut index: usize, mut old_left: usize, mut new_left: usize) -> usize {
    while index < lines.len() && (old_left > 0 || new_left > 0) {
        let line = lines[index];
        if line.starts_with("diff --git ") {
            break;
        }
        match line.chars().next() {
            Some(' ') | None => {
                old_left = old_left.saturating_sub(1);
                new_left = new_left.saturating_sub(1);
            }
            Some('-') => old_left = old_left.saturating_sub(1),
            Some('+') => new_left = new_left.saturating_sub(1),
            Some('\\') => {}
            _ => break,
        }
        index += 1;
    }
    index
}

/// The old and new line counts of a hunk header.
fn hunk_counts(line: &str) -> Option<(usize, usize)> {
    let rest = line.strip_prefix("@@")?;
    let end = rest.find("@@")?;
    let header = &rest[..end];
    let mut counts = (None, None);
    for token in header.split_whitespace() {
        if let Some(range) = token.strip_prefix('-') {
            counts.0 = Some(count_of(range)?);
        } else if let Some(range) = token.strip_prefix('+') {
            counts.1 = Some(count_of(range)?);
        }
    }
    Some((counts.0?, counts.1?))
}

fn count_of(range: &str) -> Option<usize> {
    match range.split_once(',') {
        Some((_, count)) => count.parse().ok(),
        None => Some(1),
    }
}

fn push_unique(paths: &mut Vec<String>, path: String) {
    if !paths.contains(&path) {
        paths.push(path);
    }
}

/// Normalise one diff path to a repository-relative path (5.3 condition 1).
pub fn normalise_path(raw: &str) -> Result<String, &'static str> {
    let mut path = raw.trim();
    if let Some(rest) = path.strip_prefix("./") {
        path = rest;
    } else if let Some(rest) = path.strip_prefix("a/") {
        path = rest;
    } else if let Some(rest) = path.strip_prefix("b/") {
        path = rest;
    }
    if path.is_empty() {
        return Err("empty path");
    }
    if path.starts_with('/') {
        return Err("absolute path");
    }
    let segments: Vec<&str> = path.split('/').collect();
    if segments
        .iter()
        .any(|segment| segment.is_empty() || *segment == ".." || *segment == ".")
    {
        return Err("path escapes the repository root");
    }
    Ok(path.to_string())
}

/// Whether a path is a test path (SE8): under a tests/ directory, or a file
/// whose name ends in _test.rs, .test.ts, .test.js or .snap.
#[must_use]
pub fn is_test_path(path: &str) -> bool {
    let segments: Vec<&str> = path.split('/').collect();
    if segments.contains(&"tests") {
        return true;
    }
    let name = segments.last().copied().unwrap_or(path);
    ["_test.rs", ".test.ts", ".test.js", ".snap"]
        .iter()
        .any(|suffix| name.ends_with(suffix))
}

fn matches_any(patterns: &[String], path: &str) -> bool {
    patterns.iter().any(|pattern| glob_match(pattern, path))
}

/// Match a path against a glob whose ** is matched against the whole path
/// (5.3 condition 2): ** spans segments, * and ? stay inside one segment.
#[must_use]
pub fn glob_match(pattern: &str, path: &str) -> bool {
    let pattern: Vec<&str> = pattern.split('/').collect();
    let path: Vec<&str> = path.split('/').collect();
    match_segments(&pattern, &path)
}

fn match_segments(pattern: &[&str], path: &[&str]) -> bool {
    match (pattern.split_first(), path.split_first()) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some((&"**", rest)), _) => {
            (0..=path.len()).any(|skip| match_segments(rest, &path[skip..]))
        }
        (Some((segment, rest)), Some((head, tail))) => {
            segment_match(segment, head) && match_segments(rest, tail)
        }
        (Some(_), None) => false,
    }
}

fn segment_match(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    let mut table = vec![vec![false; text.len() + 1]; pattern.len() + 1];
    table[0][0] = true;
    for row in 1..=pattern.len() {
        if pattern[row - 1] == '*' {
            table[row][0] = table[row - 1][0];
        }
    }
    for row in 1..=pattern.len() {
        for column in 1..=text.len() {
            table[row][column] = match pattern[row - 1] {
                '*' => table[row - 1][column] || table[row][column - 1],
                '?' => table[row - 1][column - 1],
                literal => table[row - 1][column - 1] && literal == text[column - 1],
            };
        }
    }
    table[pattern.len()][text.len()]
}

fn join_names(paths: &[String], limit: usize) -> String {
    let mut names: Vec<String> = paths.iter().take(limit).cloned().collect();
    if paths.len() > limit {
        names.push(format!("(+{} more)", paths.len() - limit));
    }
    names.join(", ")
}

fn name_refusals(refusals: &BTreeMap<&'static str, Vec<String>>) -> String {
    let total: usize = refusals.values().map(Vec::len).sum();
    let mut budget = MAX_NAMED_PATHS;
    let mut parts: Vec<String> = Vec::new();
    for (rule, paths) in refusals {
        if budget == 0 {
            break;
        }
        let take = paths.len().min(budget);
        parts.push(format!("{rule}: {}", join_names(paths, take)));
        budget -= take;
    }
    if total > MAX_NAMED_PATHS {
        parts.push(format!("(+{} more)", total - MAX_NAMED_PATHS));
    }
    parts.join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow(entries: &[&str]) -> Vec<String> {
        entries.iter().map(|entry| (*entry).to_string()).collect()
    }

    fn diff_for(paths: &[&str]) -> String {
        let mut diff = String::new();
        for path in paths {
            diff.push_str(&format!("diff --git a/{path} b/{path}\n"));
            diff.push_str(&format!("--- a/{path}\n"));
            diff.push_str(&format!("+++ b/{path}\n"));
            diff.push_str("@@ -1,1 +1,1 @@\n");
            diff.push_str("-old\n");
            diff.push_str("+new\n");
        }
        diff
    }

    fn surface() -> (Vec<String>, Vec<String>) {
        (
            allow(&["crates/**", "docs/**"]),
            allow(&["design/decisions/**"]),
        )
    }

    #[test]
    fn a_diff_inside_the_surface_passes() {
        let (allow, deny) = surface();
        let verdict = check_surface(
            &diff_for(&["crates/kian-cli/src/job_cli.rs"]),
            &allow,
            &deny,
        );
        assert!(verdict.passed, "{}", verdict.detail);
        assert_eq!(verdict.detail, "1 path(s) inside the surface");
    }

    #[test]
    fn a_denied_path_is_refused_by_deny() {
        let (allow, deny) = (allow(&["**"]), allow(&["design/decisions/**"]));
        let verdict = check_surface(&diff_for(&["design/decisions/ADR-069-x.md"]), &allow, &deny);
        assert!(!verdict.passed);
        assert_eq!(verdict.detail, "denied: design/decisions/ADR-069-x.md");
    }

    #[test]
    fn deny_wins_over_allow() {
        let (allow, deny) = (allow(&["**"]), allow(&["AGENTS.md"]));
        let verdict = check_surface(&diff_for(&["AGENTS.md"]), &allow, &deny);
        assert!(!verdict.passed);
        assert!(
            verdict.detail.starts_with("denied: AGENTS.md"),
            "{}",
            verdict.detail
        );
    }

    #[test]
    fn a_path_outside_allow_is_refused() {
        let (allow, deny) = surface();
        let verdict = check_surface(&diff_for(&["kian-core/src/lib.rs"]), &allow, &deny);
        assert!(!verdict.passed);
        assert_eq!(verdict.detail, "outside allow: kian-core/src/lib.rs");
    }

    #[test]
    fn absolute_and_parent_paths_are_not_repository_relative() {
        let (allow, deny) = (allow(&["**"]), allow(&[]));
        for path in ["/etc/passwd", "../outside.txt"] {
            let diff = format!("--- a/{path}\n+++ b/{path}\n@@ -1 +1 @@\n-old\n+new\n");
            let verdict = check_surface(&diff, &allow, &deny);
            assert!(!verdict.passed, "{} must be refused", path);
            assert!(
                verdict.detail.contains("outside allow"),
                "{}",
                verdict.detail
            );
            assert!(verdict.detail.contains(path), "{}", verdict.detail);
        }
    }

    #[test]
    fn an_unparsable_diff_is_refused() {
        let (allow, deny) = surface();
        for diff in ["", "this is not a diff\n", "--- a/x\n(no plus line)\n"] {
            let verdict = check_surface(diff, &allow, &deny);
            assert!(!verdict.passed, "{}", diff);
            assert!(
                verdict.detail.starts_with("unparsable: "),
                "{}",
                verdict.detail
            );
        }
    }

    #[test]
    fn a_test_only_diff_is_refused() {
        let (allow, deny) = (allow(&["crates/**"]), allow(&["design/decisions/**"]));
        let verdict = check_surface(
            &diff_for(&[
                "crates/kian-cli/tests/job_run_e2e.rs",
                "crates/kian-cli/src/thing_test.rs",
            ]),
            &allow,
            &deny,
        );
        assert!(!verdict.passed);
        assert!(
            verdict.detail.starts_with("test-only: "),
            "{}",
            verdict.detail
        );
    }

    #[test]
    fn a_rename_is_checked_on_both_sides() {
        let (allow, deny) = (allow(&["crates/**"]), allow(&["**/*.key"]));
        let diff = "diff --git a/crates/x.rs b/design/x.rs\nrename from crates/x.rs\nrename to design/x.rs\n";
        let verdict = check_surface(diff, &allow, &deny);
        assert!(!verdict.passed);
        assert!(verdict.detail.contains("design/x.rs"), "{}", verdict.detail);
    }

    #[test]
    fn creation_and_deletion_name_the_present_side() {
        let (allow, deny) = (allow(&["crates/**"]), allow(&[]));
        let created = "--- /dev/null\n+++ b/crates/new.rs\n@@ -0,0 +1 @@\n+x\n";
        assert!(
            check_surface(created, &allow, &deny).passed,
            "a new file inside the surface"
        );
        let deleted = "--- a/crates/old.rs\n+++ /dev/null\n@@ -1 +0,0 @@\n-x\n";
        assert!(
            check_surface(deleted, &allow, &deny).passed,
            "a deleted file inside the surface"
        );
    }

    #[test]
    fn the_refusal_detail_is_bounded() {
        let (allow, deny) = (allow(&["crates/**"]), allow(&[]));
        let paths: Vec<String> = (0..25).map(|index| format!("outside/{index}.rs")).collect();
        let refs: Vec<&str> = paths.iter().map(String::as_str).collect();
        let verdict = check_surface(&diff_for(&refs), &allow, &deny);
        assert!(!verdict.passed);
        assert!(verdict.detail.contains("(+5 more)"), "{}", verdict.detail);
        let named = verdict.detail.matches(".rs").count();
        assert_eq!(named, 20, "{}", verdict.detail);
    }

    #[test]
    fn globs_stay_inside_their_segments() {
        assert!(glob_match("crates/**", "crates/a/b/c.rs"));
        assert!(glob_match(
            "crates/*/src/lib.rs",
            "crates/kian-cli/src/lib.rs"
        ));
        assert!(!glob_match("crates/*", "crates/a/b"));
        assert!(glob_match("**/*.pem", "a/b/key.pem"));
        assert!(!glob_match("**/*.pem", "a/b/key.pemx"));
        assert!(glob_match("src/?.rs", "src/a.rs"));
        assert!(!glob_match("src/?.rs", "src/ab.rs"));
        assert!(glob_match("**", "anything/at/all"));
    }

    #[test]
    fn a_hunk_body_that_looks_like_a_header_is_not_a_path() {
        let (allow, deny) = (allow(&["crates/**"]), allow(&[]));
        let diff = concat!(
            "--- a/crates/x.rs\n",
            "+++ b/crates/x.rs\n",
            "@@ -1,2 +1,2 @@\n",
            "--- a/not-a-header.rs\n",
            "+++ b/not-a-header.rs\n",
            " same\n",
        );
        let verdict = check_surface(diff, &allow, &deny);
        assert!(verdict.passed, "{}", verdict.detail);
    }
}
