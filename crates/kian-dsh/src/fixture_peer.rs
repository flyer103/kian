//! The scripted stdio peer as a library function: the same
//! fixture-script interpreter the `dsh-fixture-peer` bins run, so the
//! driver's CI tests (kian-dsh) and the conformance suite
//! (kian-superagent, unchanged) execute one implementation.
//!
//! Script operations (one JSON object per line):
//! - `{"op":"send","frame":<frame>}` — write the frame to stdout (one
//!   JSON line), validated with the contract's own parser first;
//! - `{"op":"expect","frame":<frame>}` — read one line from stdin and
//!   require JSON equality (any mismatch is a hard failure with a
//!   diagnostic on stderr and exit code 1 — the peer never best-efforts
//!   past an unexpected control frame);
//! - `{"op":"exit","code":N}` — stop immediately with exit code N
//!   (the honest child-crash shape — mid-script, no cleanup);
//! - `{"op":"eof"}` — stop and exit 0.

use std::error::Error;
use std::io::{BufRead, Write};

use crate::mapping::parse_peer_frame;

/// Runs one fixture script over already-locked stdio handles (the bins
/// are thin wrappers around this).
pub fn run_script(script: &std::ffi::OsStr) -> Result<(), Box<dyn Error>> {
    let path = std::path::Path::new(script);
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read script {}: {e}", path.display()))?;

    let stdin = std::io::stdin();
    let input = stdin.lock();
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    run_ops(&text, &mut out, input)
}

/// The interpreter core, over abstracted io (unit-testable).
pub fn run_ops(
    text: &str,
    out: &mut dyn Write,
    mut input: std::io::StdinLock<'_>,
) -> Result<(), Box<dyn Error>> {
    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let op: serde_json::Value = serde_json::from_str(trimmed)
            .map_err(|e| format!("script line {} is not JSON: {e}", idx + 1))?;
        let op_name = op
            .get("op")
            .and_then(|v| v.as_str())
            .ok_or(format!("script line {} has no 'op'", idx + 1))?;
        match op_name {
            "send" => {
                let frame = op.get("frame").cloned().ok_or(format!(
                    "script line {}: 'send' requires a 'frame'",
                    idx + 1
                ))?;
                // The peer only speaks contract frames: validate with
                // the contract parser before emitting anything.
                let wire = serde_json::to_string(&frame)?;
                parse_peer_frame(&wire).map_err(|e| {
                    format!(
                        "script line {}: frame is not a legal contract frame: {e}",
                        idx + 1
                    )
                })?;
                writeln!(out, "{wire}")?;
                out.flush()?;
            }
            "expect" => {
                let expected = op.get("frame").cloned().ok_or(format!(
                    "script line {}: 'expect' requires a 'frame'",
                    idx + 1
                ))?;
                let mut received = String::new();
                let n = input.read_line(&mut received)?;
                if n == 0 {
                    return Err(format!(
                        "script line {}: peer expected a control frame but stdin closed",
                        idx + 1
                    )
                    .into());
                }
                let got: serde_json::Value =
                    serde_json::from_str(received.trim()).map_err(|e| {
                        format!("script line {}: control line is not JSON: {e}", idx + 1)
                    })?;
                if got != expected {
                    return Err(format!(
                        "script line {}: control frame mismatch\n  expected: {expected}\n  received: {got}",
                        idx + 1
                    )
                    .into());
                }
            }
            "exit" => {
                let code = op.get("code").and_then(|v| v.as_i64()).unwrap_or(1);
                std::process::exit(code.clamp(1, 125) as i32);
            }
            "eof" => break,
            other => {
                return Err(format!("script line {}: unknown op `{other}`", idx + 1).into());
            }
        }
    }
    out.flush()?;
    Ok(())
}
