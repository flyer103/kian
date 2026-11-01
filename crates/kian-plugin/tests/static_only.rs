//! Static-only check (card C7, ADR-004): the kian-plugin dependency tree
//! must not pull any dynamic-loading crate (`libloading` / dylib / wasm).
//! Phase-1 plugins are compile-time registered via `kian_plugins![...]`;
//! dynamic loading is Phase 3.
//!
//! Uses `cargo tree` (a metadata-only command — it does not take the cargo
//! build lock, so nesting it inside `cargo test` is safe). A raw Cargo.lock
//! scan would over-report: target-gated deps such as getrandom's
//! `wasm-bindgen` appear in the shared lockfile but are never compiled into
//! kian-plugin.

use std::collections::BTreeSet;
use std::process::Command;

#[test]
fn dependency_tree_contains_no_dynamic_loading_crate() {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = Command::new(env!("CARGO"))
        .arg("tree")
        .arg("-p")
        .arg("kian-plugin")
        .arg("-e")
        .arg("normal")
        .arg("--prefix")
        .arg("none")
        .current_dir(&workspace_root)
        .output()
        .expect("cargo tree must be runnable");

    assert!(
        output.status.success(),
        "cargo tree failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Lines look like `name v1.2.3` (feature lists `feature "x"` may append).
    let names: BTreeSet<String> = stdout
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .map(|spec| spec.to_string())
        .collect();
    assert!(
        names.iter().any(|n| n.starts_with("kian-plugin")),
        "cargo tree output must contain kian-plugin itself:\n{stdout}"
    );

    for name in &names {
        let dynamic = name == "libloading" || name.contains("dylib") || name.starts_with("wasm");
        assert!(
            !dynamic,
            "package `{name}` is a dynamic-loading crate in kian-plugin's \
             normal dependency tree; kian-plugin is static-only per ADR-004"
        );
    }
}
