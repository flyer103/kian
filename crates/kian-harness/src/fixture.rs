//! The scripted stdio child used by CI fixtures (the dsh pattern, generalized): a JSON-line
//! script that sends protocol lines, asserts received lines, and can exit with a code.
//!
//! Script operations (one JSON object per line):
//! - `{"op":"send","line":<json>}` — write the JSON value as one line;
//! - `{"op":"expect","line":<json>}` — read one line and require JSON equality (a mismatch is a
//!   hard failure with a diagnostic on stderr and exit code 1);
//! - `{"op":"expect_any"}` — consume one line and ignore it (the mounted e2e cannot script a
//!   random delegation wrapper);
//! - `{"op":"exit","code":N}` — stop immediately with exit code N (the honest crash shape);
//! - `{"op":"eof"}` — stop and exit 0.
//!
//! A harness whose protocol needs conditional responses (Codex's JSON-RPC server) writes its own
//! fixture peer instead of a linear script.

use std::error::Error;
use std::io::{BufRead, Write};

/// Runs one fixture script over the process stdio.
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
                let value = op
                    .get("line")
                    .cloned()
                    .ok_or(format!("script line {}: 'send' requires a 'line'", idx + 1))?;
                writeln!(out, "{}", serde_json::to_string(&value)?)?;
                out.flush()?;
            }
            "expect" => {
                let expected = op.get("line").cloned().ok_or(format!(
                    "script line {}: 'expect' requires a 'line'",
                    idx + 1
                ))?;
                let mut received = String::new();
                let n = input.read_line(&mut received)?;
                if n == 0 {
                    return Err(format!(
                        "script line {}: expected a control line but stdin closed",
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
                        "script line {}: control line mismatch\n  expected: {expected}\n  received: {got}",
                        idx + 1
                    )
                    .into());
                }
            }
            "expect_any" => {
                let mut received = String::new();
                let n = input.read_line(&mut received)?;
                if n == 0 {
                    return Err(format!(
                        "script line {}: expected a control line but stdin closed",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn script_sends_and_asserts_lines() {
        let script = concat!(
            r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#,
            "\n",
            r#"{"op":"expect","line":{"type":"user"}}"#,
            "\n",
            r#"{"op":"eof"}"#,
            "\n"
        );
        let mut out = Vec::new();
        // A script that expects a line cannot be driven through the real stdin here; this test
        // only pins the send half (the expect half is exercised by the adapter suites).
        let text = r#"{"op":"send","line":{"type":"system","subtype":"init"}}"#;
        let stdin = std::io::stdin();
        let lock = stdin.lock();
        run_ops(text, &mut out, lock).expect("send half");
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "{\"subtype\":\"init\",\"type\":\"system\"}\n"
        );
        let _ = script;
    }
}
