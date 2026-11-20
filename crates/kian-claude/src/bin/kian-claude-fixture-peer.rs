//! `kian-claude-fixture-peer`: the scripted Claude Code stream-json child for CI (no `claude`
//! binary, no credentials). See [`kian_harness::fixture`].
//!
//! The adapter spawns the real CLI argv (`--print --input-format stream-json ...`); the fixture
//! ignores those flags and takes the **last** argument as its script, exactly like the dsh
//! fixture peer accepts and ignores `--profile sdk`.

use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<std::ffi::OsString> = env::args_os().skip(1).collect();
    let Some(script) = args.last() else {
        eprintln!("usage: kian-claude-fixture-peer [claude flags…] <script.jsonl>");
        return ExitCode::FAILURE;
    };
    match kian_harness::fixture::run_script(script) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("kian-claude-fixture-peer: {err}");
            ExitCode::FAILURE
        }
    }
}
