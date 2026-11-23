//! The wire-only dependency contract: `cargo tree -p
//! kian-sdk` must show nothing beyond reqwest/serde/serde_json/futures/
//! tokio/thiserror and never a kian crate — the SDK is embeddable
//! anywhere. Enforced here at the manifest source so a stray
//! dependency fails CI, not review.

/// The `[dependencies]` table is exactly the wire-only allowlist.
#[test]
fn sdk_dependencies_are_wire_only() {
    let manifest = include_str!("../Cargo.toml");
    let section = manifest
        .split("[dependencies]")
        .nth(1)
        .expect("[dependencies] table")
        .split('[')
        .next()
        .expect("deps section end");

    const ALLOWED: [&str; 5] = ["reqwest", "serde", "serde_json", "futures", "thiserror"];
    let mut seen = Vec::new();
    for line in section.lines() {
        let dep = line
            .split('=')
            .next()
            .unwrap_or("")
            .trim()
            .split('.')
            .next()
            .unwrap_or("")
            .trim();
        if dep.is_empty() {
            continue;
        }
        assert!(
            ALLOWED.contains(&dep),
            "`{dep}` is outside the wire-only allowlist {ALLOWED:?}"
        );
        seen.push(dep);
    }
    assert_eq!(seen.len(), ALLOWED.len(), "every allowlist entry expected");
}

/// No kian crate appears anywhere in the manifest dependencies (the
/// SDK must never gain a server-side dependency).
#[test]
fn no_kian_crate_dependency_anywhere() {
    let manifest = include_str!("../Cargo.toml");
    for line in manifest.lines() {
        let dep = line.split('=').next().unwrap_or("").trim();
        assert!(
            !dep.starts_with("kian-"),
            "the SDK must stay wire-only: `{dep}` would couple it to the server"
        );
    }
}

/// The SDK carries its own version, decoupled from the workspace bump
/// cadence (versioning rule).
#[test]
fn sdk_carries_its_own_version() {
    let manifest = include_str!("../Cargo.toml");
    // The rule is "carries its own literal version", not a specific number:
    // the SDK bumps on its own cadence, so pinning the value here would make
    // every SDK release edit this test for no reason.
    assert!(
        manifest
            .lines()
            .any(|l| l.trim_start().starts_with("version = \"")),
        "kian-sdk must pin its own version"
    );
    let inherits_workspace = manifest.lines().any(|l| {
        let l = l.trim();
        l.starts_with("version.workspace")
    });
    assert!(
        !inherits_workspace,
        "kian-sdk must not inherit the workspace version"
    );
}
