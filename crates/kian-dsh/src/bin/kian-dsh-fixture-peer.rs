//! `dsh-fixture-peer` (kian-dsh's copy, C54): the C53 scripted stdio
//! peer — see [`kian_dsh::fixture_peer`]. Drives the bridge driver's
//! own CI tests with zero Node/dsh present.

use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    let (Some(script), None) = (args.next(), args.next()) else {
        eprintln!("usage: kian-dsh-fixture-peer <script.jsonl>");
        return ExitCode::FAILURE;
    };
    match kian_dsh::fixture_peer::run_script(&script) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("dsh-fixture-peer: {err}");
            ExitCode::FAILURE
        }
    }
}
