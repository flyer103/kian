//! `dsh-fixture-peer` — the C53 conformance fixture (ADR-010): a
//! minimal stdio peer speaking the dsh bridge contract from fixture
//! scripts, so CI proves both directions of the mapping, the
//! steer/interrupt translation and the pre-flight ordering rule
//! **without any Node/dsh dependency**.
//!
//! C54: the interpreter lives in `kian_dsh::fixture_peer` (one
//! implementation shared with the bridge driver's tests); this bin is
//! the thin wrapper this package's conformance suite spawns.

use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    let (Some(script), None) = (args.next(), args.next()) else {
        eprintln!("usage: dsh-fixture-peer <script.jsonl>");
        return ExitCode::FAILURE;
    };
    match kian_superagent::dsh::fixture_peer::run_script(&script) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("dsh-fixture-peer: {err}");
            ExitCode::FAILURE
        }
    }
}
