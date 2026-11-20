//! `kian-codex-fixture-peer`: the scripted `codex app-server` child for CI (no `codex` binary,
//! no credentials). Speaks the pinned JSON-RPC subset: initialize/initialized, thread/start
//! (emitting `thread/started`), turn/start (emitting `turn/started`, items, `turn/completed`) and
//! the `item/commandExecution/requestApproval` flow.
//!
//! The last argument selects the scenario: `simple` (default), `approval`, `crash`.

use std::io::{BufRead, Write};
use std::process::ExitCode;

use serde_json::{Value, json};

fn emit(out: &mut impl Write, value: Value) -> Result<(), String> {
    writeln!(out, "{value}").map_err(|e| e.to_string())?;
    out.flush().map_err(|e| e.to_string())
}

fn read_value(lines: &mut impl Iterator<Item = String>) -> Result<Value, String> {
    let line = lines.next().ok_or("stdin closed")?;
    serde_json::from_str(&line).map_err(|e| format!("input is not JSON: {e}"))
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = args
        .iter()
        .rev()
        .find(|a| matches!(a.as_str(), "simple" | "approval" | "crash"))
        .cloned()
        .unwrap_or_else(|| "simple".to_owned());

    let stdin = std::io::stdin();
    let mut lines = stdin.lock().lines().map(|l| l.expect("stdin line"));
    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    let result = run(&mode, &mut lines, &mut out);
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("kian-codex-fixture-peer: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run(
    mode: &str,
    lines: &mut impl Iterator<Item = String>,
    out: &mut impl Write,
) -> Result<(), String> {
    // initialize → response, then `initialized` notification.
    loop {
        let value = read_value(lines)?;
        match value.get("method").and_then(Value::as_str) {
            Some("initialize") => emit(
                out,
                json!({"id": value["id"], "result": {"userAgent": "kian-codex-fixture"}}),
            )?,
            Some("initialized") => break,
            Some(other) => return Err(format!("unexpected handshake method `{other}`")),
            None => return Err("expected initialize".to_owned()),
        }
    }
    // thread/start.
    let value = read_value(lines)?;
    if value.get("method").and_then(Value::as_str) != Some("thread/start") {
        return Err("expected thread/start".to_owned());
    }
    emit(
        out,
        json!({"id": value["id"], "result": {"thread": {"id": "thr_1"}}}),
    )?;
    emit(
        out,
        json!({"method": "thread/started", "params": {"thread": {"id": "thr_1"}}}),
    )?;
    // turn/start.
    let value = read_value(lines)?;
    if value.get("method").and_then(Value::as_str) != Some("turn/start") {
        return Err("expected turn/start".to_owned());
    }
    emit(
        out,
        json!({"id": value["id"], "result": {"turn": {"id": "turn_1", "status": "inProgress", "items": []}}}),
    )?;
    emit(
        out,
        json!({"method": "turn/started", "params": {"turn": {"id": "turn_1"}}}),
    )?;

    match mode {
        "crash" => {
            emit(
                out,
                json!({"method": "item/started", "params": {"item": {
                    "id": "i1", "type": "agentMessage", "text": ""
                }}}),
            )?;
            std::process::exit(9);
        }
        "approval" => {
            emit(
                out,
                json!({"method": "item/started", "params": {"item": {
                    "id": "i2", "type": "commandExecution",
                    "command": "rm -rf /", "cwd": "/", "status": "inProgress"
                }}}),
            )?;
            emit(
                out,
                json!({"id": 100, "method": "item/commandExecution/requestApproval",
                       "params": {"itemId": "i2", "threadId": "thr_1", "turnId": "turn_1",
                                  "command": "rm -rf /", "cwd": "/"}}),
            )?;
            let answer = read_value(lines)?;
            if answer.get("id") != Some(&json!(100)) {
                return Err(format!("expected the approval answer, got {answer}"));
            }
            let decision = answer
                .pointer("/result/decision")
                .and_then(Value::as_str)
                .unwrap_or("");
            if decision != "decline" {
                return Err(format!("expected a decline, got `{decision}`"));
            }
            emit(
                out,
                json!({"method": "item/completed", "params": {"item": {
                    "id": "i2", "type": "commandExecution", "command": "rm -rf /",
                    "cwd": "/", "status": "declined", "exitCode": null
                }}}),
            )?;
            emit(
                out,
                json!({"method": "item/completed", "params": {"item": {
                    "id": "i3", "type": "agentMessage", "text": "denied, moving on"
                }}}),
            )?;
        }
        _ => {
            emit(
                out,
                json!({"method": "item/completed", "params": {"item": {
                    "id": "i1", "type": "agentMessage", "text": "pong"
                }}}),
            )?;
        }
    }

    emit(
        out,
        json!({"method": "turn/completed", "params": {"turn": {"id": "turn_1", "status": "completed"}}}),
    )?;
    // Stay alive until the driver closes the pipe (like the real app-server).
    for _ in lines {}
    Ok(())
}
