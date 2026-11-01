//! Crate-level iron-rule test (card C23, the C13 pattern): the bundle
//! never references an execution API. `kian-superagent` reaches the
//! server exclusively through its ports — `InboxWriter` (write),
//! `StreamPort` (per-hold, own-session) and `RecallPort` (C34,
//! cross-session read-only) — the only permitted `kian-orchestrator`
//! surface is the pure `WakeOutcome` type used in the port's signature
//! (the "types only via a narrow port" rule). Enforced textually over
//! every module under `src/`.

use std::fs;
use std::path::PathBuf;

fn src_sources() -> Vec<(PathBuf, String)> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut out = Vec::new();
    let mut stack = vec![dir];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display())) {
            let entry = entry.expect("dir entry");
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                let text = fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
                out.push((path, text));
            }
        }
    }
    assert!(!out.is_empty(), "no sources found under src/");
    out
}

/// Symbols that would let the bundle touch execution: the orchestrator
/// machinery, the store, the loop *as a client*, claims, wakes-as-execution.
/// Same shape as the C13 table in kian-cli, adapted to this crate.
///
/// C25 amendment: `LoopDriver` and `.drive(` are banned everywhere
/// **except `src/worker.rs` and `src/router.rs`** — those modules
/// legitimately *implement* the core `LoopDriver` seam (a kian-core
/// type, not an orchestrator execution API) as the mandated
/// `WorkerLoopDriver` and `RouterDriver` decorators. They are still
/// banned there from naming any engine/store/hold symbol, and every
/// other module remains fully banned from the loop surface.
///
/// C26 amendment: `src/router.rs` joins the decorator modules for the
/// same reason (`RouterDriver` implements the seam).
const FORBIDDEN: [&str; 11] = [
    "Engine",
    "DrainCtx",
    "EventStore",
    "kian_store",
    "drain_",
    ".claim(",
    "claim_",
    "mark_consumed",
    ".append(",
    "startup_rebuild",
    "take_eligible_inputs",
];

/// Loop-seam tokens only the decorator module may use (see the C25
/// amendment above). `lib.rs` is exempt *for re-exports only*: the
/// crate root's job is `pub use` (the `WorkerLoopDriver` type name
/// contains the seam trait's name); it implements and calls nothing.
const DECORATOR_SEAM_TOKENS: [&str; 2] = ["LoopDriver", ".drive("];

/// Modules allowed to name the loop seam: the decorators themselves and
/// the re-exporting crate root.
const DECORATOR_MODULES: [&str; 3] = ["worker.rs", "router.rs", "lib.rs"];

#[test]
fn bundle_references_no_execution_api() {
    let mut failures = Vec::new();
    for (path, src) in src_sources() {
        let is_decorator = path.file_name().is_some_and(|n| {
            DECORATOR_MODULES
                .iter()
                .any(|m| n.as_encoded_bytes() == m.as_bytes())
        });
        for token in FORBIDDEN {
            if src.contains(token) {
                failures.push(format!("{} contains `{token}`", path.display()));
            }
        }
        if !is_decorator {
            for token in DECORATOR_SEAM_TOKENS {
                if src.contains(token) {
                    failures.push(format!(
                        "{} contains loop-seam token `{token}` (only {DECORATOR_MODULES:?} may name the seam)",
                        path.display()
                    ));
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "kian-superagent src/ references execution symbols:\n{failures:#?}\n\
         the bundle reaches the server only through the InboxWriter, StreamPort and RecallPort ports"
    );
}

#[test]
fn the_only_orchestrator_surface_is_the_wake_outcome_type() {
    for (path, src) in src_sources() {
        for (idx, line) in src.lines().enumerate() {
            if line.contains("kian_orchestrator") && !line.contains("WakeOutcome") {
                panic!(
                    "{}:{} references kian_orchestrator beyond the WakeOutcome type:\n{line}",
                    path.display(),
                    idx + 1
                );
            }
        }
    }
}

#[test]
fn channel_shaper_is_only_called_before_inbox_admission() {
    let sources = src_sources();
    for (path, src) in &sources {
        let file = path.file_name().and_then(|name| name.to_str()).unwrap();
        if !matches!(file, "provenance.rs" | "subscriptions.rs" | "lib.rs") {
            assert!(
                !src.contains("shape_channel_input"),
                "C33 shaper leaked into {}: shaping is admission-time only",
                path.display()
            );
        }
    }

    let (path, subscriptions) = sources
        .iter()
        .find(|(path, _)| path.ends_with("subscriptions.rs"))
        .expect("subscription service source");
    // C43 renamed the shaper entry point to carry the untrusted bit;
    // the admission-time-only invariant this test guards is unchanged.
    let shape = subscriptions
        .find("shape_channel_input_marked(&event, &self.shaping, untrusted)")
        .expect("the one admission-time shaper call");
    let admit = subscriptions[shape..]
        .find(".admit(")
        .map(|offset| shape + offset)
        .expect("inbox admission follows shaping");
    assert!(shape < admit);
    assert_eq!(
        subscriptions
            .matches("shape_channel_input_marked(&event, &self.shaping, untrusted)")
            .count(),
        1,
        "exactly one shaping call may feed inbox admission in {}",
        path.display()
    );
}

#[test]
fn inbox_writer_port_exposes_exactly_admit_and_wake() {
    let (path, src) = src_sources()
        .into_iter()
        .find(|(p, _)| p.ends_with("inbox.rs"))
        .expect("src/inbox.rs exists");
    assert!(src.contains("pub trait InboxWriter"), "port trait missing");
    for method in ["fn admit", "fn wake"] {
        assert!(src.contains(method), "port missing `{method}`");
    }
    // No third capability smuggled onto the port.
    for banned in [
        "fn append",
        "fn create",
        "fn steer",
        "fn interrupt",
        "fn projection",
    ] {
        assert!(
            !src.contains(banned),
            "InboxWriter grew beyond admit+wake: `{banned}` found in {}",
            path.display()
        );
    }
}

/// C34: the read port is exactly a read port — cross-session reads and
/// the spill-index lookup, nothing that writes, wakes or steers.
#[test]
fn recall_port_exposes_exactly_the_read_surface() {
    let (path, src) = src_sources()
        .into_iter()
        .find(|(p, _)| p.ends_with("recall.rs"))
        .expect("src/recall.rs exists");
    assert!(src.contains("pub trait RecallPort"), "port trait missing");
    for method in ["fn read_events", "fn list_threads", "fn spill_location"] {
        assert!(src.contains(method), "port missing `{method}`");
    }
    // No write capability smuggled onto the read port.
    for banned in [
        "fn admit",
        "fn wake",
        "fn emit",
        "fn append",
        "fn steer",
        "fn create",
        "fn interrupt",
    ] {
        assert!(
            !src.contains(banned),
            "RecallPort grew beyond reads: `{banned}` found in {}",
            path.display()
        );
    }
}
