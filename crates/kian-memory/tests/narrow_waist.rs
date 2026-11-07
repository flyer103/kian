//! The narrow-waist gate: `kian-memory` is a bundle layer whose only
//! kian dependencies are the narrow waist plus the loop it wraps —
//! `kian-core` (memory vocabulary + events), `kian-orchestrator`
//! (`DrainCtx`) and `kian-loop` (the wrapped `ReActLoop`). No
//! provider/channel/super-agent crate may enter the production
//! dependency set. Enforced statically over the manifest — the same
//! property `cargo tree -p kian-memory -e normal` reports (dev-only
//! store/registry fixtures for the tests are exempt, the kian-loop
//! precedent).

use std::collections::BTreeSet;

#[test]
fn kian_memory_depends_only_on_core_orchestrator_and_loop() {
    let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
    let text = std::fs::read_to_string(manifest).expect("read kian-memory Cargo.toml");

    // The [dependencies] section (before any dev-dependencies header).
    let deps_section = text
        .split("[dev-dependencies]")
        .next()
        .expect("dev-dependencies section present");
    let deps = deps_section
        .split("[dependencies]")
        .nth(1)
        .expect("dependencies section present");

    let kian_deps: BTreeSet<String> = deps
        .lines()
        .map(str::trim)
        .filter_map(|l| l.split(['=', '.', ' ']).next())
        .map(str::trim)
        .filter(|name| name.starts_with("kian-"))
        .map(str::to_owned)
        .collect();
    assert_eq!(
        kian_deps,
        BTreeSet::from([
            "kian-core".to_owned(),
            "kian-orchestrator".to_owned(),
            "kian-loop".to_owned(),
        ]),
        "the memory bundle stays on the waist: core + orchestrator + loop only"
    );
}
