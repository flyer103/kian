//! A8: the code's capability declaration must match the normative matrix (HARNESS-BRIDGE §3).

use kian_codex::session::CAPABILITIES;
use kian_harness::SteerMode;

#[test]
fn codex_capabilities_match_the_matrix() {
    let caps = CAPABILITIES;
    assert_eq!(caps.name, "codex");
    assert!(!caps.request_bytes, "projection-only");
    assert_eq!(caps.steer, SteerMode::InTurn);
    assert!(caps.interrupt);
    assert!(caps.approval_gate);
    assert!(!caps.resume, "phase 1 has no cross-process resume");
}
