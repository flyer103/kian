//! The narrow-waist gate: `kian-dsh` depends on no kian crate
//! beyond `kian-core` (the seam types + the shared event vocabulary
//! the mapping selects from). Enforced statically over the
//! manifest — the same property `cargo tree -p kian-dsh` reports.

use std::collections::BTreeSet;

#[test]
fn kian_dsh_adds_no_kian_core_crate_dependency_beyond_the_seam_types() {
    let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
    let text = std::fs::read_to_string(manifest).expect("read kian-dsh Cargo.toml");

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
        BTreeSet::from(["kian-core".to_owned()]),
        "the dsh bridge stays on the narrow waist: kian-core only"
    );
}
