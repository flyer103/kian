//! `kian_sidecar_child` — the reference sidecar child (test fixture
//! and protocol example; PLUGIN-SPEC §7).
//!
//! Not a shipped product surface: it exists so the runner tests in
//! `crates/kian-plugin/tests/` exercise the **real** stdio ABI against
//! a real child process — hardened spawn, env_allow, digest
//! verification and the handshake all apply to it exactly as they
//! would to an operator's plugin. The role is selected with
//! `KIAN_SIDECAR_ROLE`; the scripted reply with
//! `KIAN_SIDECAR_REPLY` (a JSON reply line without the trailing
//! newline).
//!
//! Roles:
//! - `ok`          — handshake correctly, then answer every `hook`
//!   with `KIAN_SIDECAR_REPLY` (default `ack`)
//! - `never-ready` — read hello, never answer (rule 13)
//! - `bad-ready`   — answer ready restating a different version
//! - `abi-ready`   — answer ready restating a different abi
//! - `slow`        — handshake correctly, never answer hooks (deadline)
//! - `crash`       — handshake correctly, exit on the first hook
//! - `oversize`    — handshake correctly, answer with a huge line
//! - `wrong-id`    — handshake correctly, answer with a stray id
//! - `garbage`     — handshake correctly, answer with non-JSON
//! - `stderr-spam` — handshake correctly, then flood stderr with
//!   oversized lines and never reply

use std::io::{BufRead, BufReader, Write};

fn main() {
    let role = std::env::var("KIAN_SIDECAR_ROLE").unwrap_or_else(|_| "ok".to_string());
    let stdin = std::io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let mut out = std::io::stdout();
    let mut hello = String::new();
    if reader.read_line(&mut hello).unwrap_or(0) == 0 {
        return;
    }

    match role.as_str() {
        "never-ready" => loop {
            std::thread::sleep(std::time::Duration::from_secs(60));
        },
        "bad-ready" => {
            let ready = ready_from_hello(&hello, |r| r.version = "9.9.9".to_string());
            let _ = writeln!(out, "{ready}");
            let _ = out.flush();
            park();
        }
        "abi-ready" => {
            let ready = ready_from_hello(&hello, |r| r.abi = 2);
            let _ = writeln!(out, "{ready}");
            let _ = out.flush();
            park();
        }
        _ => {}
    }

    // Correct handshake: restate the hello's identity exactly.
    let ready = ready_from_hello(&hello, |_| {});
    let _ = writeln!(out, "{ready}");
    let _ = out.flush();

    if role == "stderr-spam" {
        // Continuous oversized stderr with no replies: the stderr-only
        // window must be able to trip (§6.5).
        loop {
            let big = "e".repeat(9 * 1024);
            let _ = writeln!(std::io::stderr(), "{big}");
            let _ = std::io::stderr().flush();
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    let reply_script = std::env::var("KIAN_SIDECAR_REPLY")
        .unwrap_or_else(|_| r#"{"id":"PLACEHOLDER","ack":true}"#.to_string());
    loop {
        let mut hook = String::new();
        if reader.read_line(&mut hook).unwrap_or(0) == 0 {
            return; // stdin closed: clean exit
        }
        if hook.trim() == "{}" {
            // bye (§7.5): half-close done by the parent; exit cleanly.
            return;
        }
        let id = frame_id(&hook);
        match role.as_str() {
            "slow" => {
                let _ = id;
                park();
            }
            "crash" => std::process::exit(0),
            "oversize" => {
                let line = "x".repeat(40 * 1024); // > any legal 16 KiB-bound test
                let _ = writeln!(out, "{line}");
                let _ = out.flush();
                park();
            }
            "wrong-id" => {
                let reply = reply_script.replace("PLACEHOLDER", "bogus-id");
                let _ = writeln!(out, "{reply}");
                let _ = out.flush();
                park();
            }
            "garbage" => {
                let _ = writeln!(out, "this is not json");
                let _ = out.flush();
                park();
            }
            "env-audit" => {
                // Deny iff a canary the test process carries but the
                // manifest does NOT allow leaked through env_clear
                // (§10.3 step 2: deny-by-default environment).
                let leaked = std::env::var("KIAN_ENV_CANARY").is_ok();
                let reply = if leaked {
                    r#"{"id":"PLACEHOLDER","bail":{"decision":"deny","reason":"canary leaked"}}"#
                } else {
                    r#"{"id":"PLACEHOLDER","bail":null}"#
                };
                let reply = reply.replace("PLACEHOLDER", &id);
                let _ = writeln!(out, "{reply}");
                let _ = out.flush();
            }
            "stderr-flood" => {
                // One oversized stderr line (dropped, counted once per
                // burst — §10.5), then the scripted reply.
                let big = "e".repeat(9 * 1024); // > the 8 KiB default line bound
                let _ = writeln!(std::io::stderr(), "{big}");
                let reply = reply_script.replace("PLACEHOLDER", &id);
                let _ = writeln!(out, "{reply}");
                let _ = out.flush();
            }
            _ => {
                // `ok`: the scripted reply with this dispatch's id.
                let reply = reply_script.replace("PLACEHOLDER", &id);
                let _ = writeln!(out, "{reply}");
                let _ = out.flush();
            }
        }
    }
}

fn park() -> ! {
    loop {
        std::thread::sleep(std::time::Duration::from_secs(60));
    }
}

/// Pulls `id` out of a hook line (the child side needs no full parser:
/// the runner's goldens already pin the encoder).
fn frame_id(hook_line: &str) -> String {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(hook_line)
        && let Some(id) = value.get("id").and_then(|v| v.as_str())
    {
        return id.to_string();
    }
    String::new()
}

/// Builds the ready line by restating the hello's identity exactly
/// (§7.3 step 2) — the same JSON shape `kian_plugin::hooks::sidecar`
/// decodes, so the fixture and the runner cannot drift.
fn ready_from_hello(hello_line: &str, edit: impl FnOnce(&mut Ready)) -> String {
    let hello: serde_json::Value = serde_json::from_str(hello_line.trim()).unwrap_or_default();
    let mut ready = Ready {
        manifest: hello
            .get("manifest")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        name: hello
            .pointer("/plugin/name")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        version: hello
            .pointer("/plugin/version")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
        abi: hello.get("abi").and_then(|v| v.as_u64()).unwrap_or(1) as u32,
        hooks: hello
            .get("hooks")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        capabilities: hello
            .get("capabilities")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
    };
    edit(&mut ready);
    let mut out = String::with_capacity(160);
    out.push_str("{\"v\":1,\"manifest\":");
    out.push_str(&serde_json::to_string(&ready.manifest).unwrap_or_default());
    out.push_str(",\"plugin\":{\"name\":");
    out.push_str(&serde_json::to_string(&ready.name).unwrap_or_default());
    out.push_str(",\"version\":");
    out.push_str(&serde_json::to_string(&ready.version).unwrap_or_default());
    out.push_str("},\"abi\":");
    out.push_str(&ready.abi.to_string());
    out.push_str(",\"hooks\":");
    out.push_str(&serde_json::to_string(&ready.hooks).unwrap_or_default());
    out.push_str(",\"capabilities\":");
    out.push_str(&serde_json::to_string(&ready.capabilities).unwrap_or_default());
    out.push('}');
    out
}

struct Ready {
    manifest: String,
    name: String,
    version: String,
    abi: u32,
    hooks: Vec<String>,
    capabilities: Vec<String>,
}
