//! Iron-rule test (card C13): no control-plane handler references any
//! execution API. Enforced textually over the handler module — `api.rs`
//! must be HTTP-only, with the `ControlPlane` trait as its sole server
//! surface (inbox writes + projection/event reads).

use std::path::PathBuf;

fn api_src() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/api.rs");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

/// Symbols that would let a handler touch execution: drain machinery,
/// loop drivers, engine, store writes, claims, event appends, wakes.
const FORBIDDEN: [&str; 16] = [
    "DrainCtx",
    "LoopDriver",
    "Engine",
    "EventStore",
    "kian_orchestrator",
    "kian_store",
    "kian_loop",
    "kian_llm",
    ".drive(",
    "drain_",
    ".claim(",
    "mark_consumed",
    ".append(",
    "startup_rebuild",
    ".wake(",
    "interrupt_flag",
];

#[test]
fn control_plane_handlers_reference_no_execution_api() {
    let src = api_src();
    let mut failures = Vec::new();
    for token in FORBIDDEN {
        if src.contains(token) {
            failures.push(token);
        }
    }
    assert!(
        failures.is_empty(),
        "api.rs (the HTTP handler module) references execution symbols {failers:?}; \
         handlers may only write the inbox and read projections via ControlPlane",
        failers = failures
    );
}

#[test]
fn control_plane_trait_exposes_only_inbox_and_reads() {
    let src = api_src();
    // The trait must exist and be the only `pub trait` in the module.
    assert!(
        src.contains("pub trait ControlPlane"),
        "ControlPlane trait missing"
    );
    // Every method of the trait — inbox writes and reads only.
    for method in [
        "fn create_session",
        "fn submit",
        "fn steer",
        "fn queue",
        "fn interrupt",
        "fn projection",
        "fn session_events",
    ] {
        assert!(src.contains(method), "ControlPlane is missing `{method}`");
    }
}

#[test]
fn handlers_dispatch_only_the_phase1_method_set() {
    let src = api_src();
    for method in [
        "\"create_session\"",
        "\"submit\"",
        "\"steer\"",
        "\"interrupt\"",
        "\"queue\"",
        "\"projection\"",
    ] {
        assert!(src.contains(method), "rpc dispatch missing {method}");
    }
    // No Phase-2+ endpoints smuggled in.
    for banned in ["confirm", "auth", "tenant", "channel", "router"] {
        assert!(
            !src.contains(&format!("\"{banned}\"")),
            "api.rs dispatches a non-Phase-1 method `{banned}`"
        );
    }
}

// ---------------------------------------------------------------------------
// C14: the operational plane obeys the same structural discipline —
// ops handlers speak only the OpsPlane trait, never execution APIs.
// ---------------------------------------------------------------------------

fn ops_src() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/ops.rs");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
}

#[test]
fn ops_handlers_reference_no_execution_api() {
    let src = ops_src();
    let mut failures = Vec::new();
    // The engine/store may be named only in doc comments is NOT allowed
    // either — keep the same token table as the control plane.
    for token in FORBIDDEN {
        if src.contains(token) {
            failures.push(token);
        }
    }
    assert!(
        failures.is_empty(),
        "ops.rs (the operational handler module) references execution symbols {failures:?}; \
         ops handlers may only speak the OpsPlane trait"
    );
}

#[test]
fn ops_plane_exposes_only_the_operator_actions() {
    let src = ops_src();
    assert!(src.contains("pub trait OpsPlane"), "OpsPlane trait missing");
    for method in ["fn wake_now", "fn recovery_budget_reset"] {
        assert!(src.contains(method), "OpsPlane is missing `{method}`");
    }
    // The C14 ops dispatch set is closed: exactly the two methods.
    for banned in [
        "\"confirm\"",
        "\"submit\"",
        "\"steer\"",
        "\"interrupt\"",
        "\"append\"",
        "\"claim\"",
    ] {
        assert!(
            !src.contains(banned),
            "ops.rs dispatches a non-ops method {banned}"
        );
    }
}
