//! The narrow-waist gate: `kian-harness` depends on no kian crate beyond `kian-core` (the seam
//! types plus the shared event vocabulary the adapters select from).

use std::collections::BTreeSet;

#[test]
fn kian_harness_adds_no_kian_core_crate_dependency_beyond_the_seam_types() {
    let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
    let text = std::fs::read_to_string(manifest).expect("read kian-harness Cargo.toml");

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
        BTreeSet::from(["kian-core".to_owned()]),
        "the bridge skeleton stays on the narrow waist: kian-core only"
    );
}
