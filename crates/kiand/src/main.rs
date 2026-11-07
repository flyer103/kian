//! `kiand` binary entry point: the endpoint daemon —
//! `kian-endpoint` + a thin main.
//!
//! - `kiand [flags]` — serve the endpoint over TCP + JSON lines. Config
//!   comes from flags > environment > optional `kiand.yaml` > defaults;
//!   loading is fail-closed (a malformed value refuses to start with a
//!   clear error and a non-zero exit code).
//! - `kiand --version` — print the version.
//!
//! With the default `--listen 127.0.0.1:0` the daemon prints its bound
//! address on stdout (`kiand: endpoint <id> listening on <addr>` — the
//! only stdout line, the registration surface for whoever launched it);
//! all logs go to stderr. SIGTERM/SIGINT stop the accept loop, let
//! in-flight commands finish, and then close the transport by exiting.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use kian_endpoint::{Endpoint, HeartbeatTap};
use kiand::config::{Config, Flags};
use kiand::executor::DaemonExecutor;

const USAGE: &str = "usage:
  kiand [--config <path>] [--endpoint-id <id>] [--listen <host:port>]
        [--max-concurrent <n>] [--heartbeat-ms <n>] [--exec-timeout-ms <n>]
                                         run the endpoint daemon (default listen: 127.0.0.1:0;
                                         the bound address is printed on stdout)
  kiand --version                        print the version

configuration precedence: flags > environment (KIAND_ENDPOINT_ID, KIAND_LISTEN,
KIAND_MAX_CONCURRENT, KIAND_HEARTBEAT_MS, KIAND_EXEC_TIMEOUT_MS) > optional
./kiand.yaml > defaults; malformed configuration refuses to start
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--version" || a == "-V") {
        println!("kiand {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    let flags = match parse_flags(&args) {
        Ok(flags) => flags,
        Err(e) => {
            eprintln!("kiand: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let config = match kiand::config::resolve(flags) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("kiand: refusing to start — {e}");
            return ExitCode::from(2);
        }
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("kiand: cannot start async runtime: {e}");
            return ExitCode::from(1);
        }
    };
    runtime.block_on(run(config))
}

fn parse_flags(args: &[String]) -> Result<Flags, String> {
    fn next_value<'a>(
        iter: &mut impl Iterator<Item = &'a String>,
        name: &str,
    ) -> Result<String, String> {
        iter.next()
            .cloned()
            .ok_or_else(|| format!("{name} requires a value"))
    }
    fn parse_num<T: std::str::FromStr>(raw: &str, name: &str) -> Result<T, String> {
        raw.parse()
            .map_err(|_| format!("{name} expects a non-negative integer, got `{raw}`"))
    }

    let mut flags = Flags::default();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => flags.config = Some(next_value(&mut iter, "--config")?.into()),
            "--endpoint-id" => flags.endpoint_id = Some(next_value(&mut iter, "--endpoint-id")?),
            "--listen" => flags.listen = Some(next_value(&mut iter, "--listen")?),
            "--max-concurrent" => {
                let raw = next_value(&mut iter, "--max-concurrent")?;
                flags.max_concurrent = Some(parse_num(&raw, "--max-concurrent")?);
            }
            "--heartbeat-ms" => {
                let raw = next_value(&mut iter, "--heartbeat-ms")?;
                flags.heartbeat_ms = Some(parse_num(&raw, "--heartbeat-ms")?);
            }
            "--exec-timeout-ms" => {
                let raw = next_value(&mut iter, "--exec-timeout-ms")?;
                flags.exec_timeout_ms = Some(parse_num(&raw, "--exec-timeout-ms")?);
            }
            other => return Err(format!("unknown option `{other}`")),
        }
    }
    Ok(flags)
}

async fn run(config: Config) -> ExitCode {
    // Logs go to stderr: stdout carries only the registration line.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let addr = match config.listen_addr() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("kiand: refusing to start — {e}");
            return ExitCode::from(2);
        }
    };
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("kiand: cannot bind {addr}: {e}");
            return ExitCode::from(1);
        }
    };
    let bound = listener
        .local_addr()
        .expect("a bound listener has a local address");
    println!(
        "kiand: endpoint {} listening on {bound}",
        config.endpoint_id
    );
    use std::io::Write as _;
    let _ = std::io::stdout().flush();

    let (executor, mut inflight) = DaemonExecutor::new(config.exec_timeout_ms);
    let tap: HeartbeatTap = Arc::new(|beat| {
        tracing::info!(
            endpoint = %beat.endpoint_id,
            inflight = beat.inflight,
            "heartbeat"
        );
    });
    let endpoint = Arc::new(
        Endpoint::new(config.endpoint_id.clone(), executor)
            .with_max_concurrent(config.max_concurrent)
            .with_heartbeat(Duration::from_millis(config.heartbeat_ms))
            .with_heartbeat_tap(tap),
    );
    let serve = tokio::spawn(endpoint.clone().serve(listener));
    tracing::info!(
        %bound,
        endpoint = %config.endpoint_id,
        max_concurrent = config.max_concurrent,
        heartbeat_ms = config.heartbeat_ms,
        exec_timeout_ms = config.exec_timeout_ms,
        "kiand serving"
    );

    // SIGTERM/SIGINT: stop accepting, finish in-flight commands, close
    // the transport by exiting.
    let mut sigterm = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        Ok(sigterm) => sigterm,
        Err(e) => {
            eprintln!("kiand: cannot install SIGTERM handler: {e}");
            return ExitCode::from(1);
        }
    };
    tokio::select! {
        _ = sigterm.recv() => tracing::info!("SIGTERM received; draining in-flight commands"),
        _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT received; draining in-flight commands"),
    }
    // Stop accepting; live connections run their in-flight commands to
    // completion (the drain below waits for exactly that).
    serve.abort();

    // Every command without its own timeout is bounded by
    // exec_timeout_ms (the DaemonExecutor default); a command may carry
    // an explicit longer timeout, so the drain bound leaves room for one
    // full extra flight beyond the default.
    let drain_bound = Duration::from_millis(2 * config.exec_timeout_ms + 60_000);
    let deadline = tokio::time::Instant::now() + drain_bound;
    // Drained = zero in-flight **and** zero holds through a quiet window:
    // a command already accepted by the endpoint but not yet counted here
    // (scheduler window) gets its execution counted during the grace
    // period, and the wait resumes instead of exiting from under it.
    let drain = async {
        loop {
            // A dropped sender means no executor is left — nothing can
            // ever start again; that counts as drained.
            if inflight.wait_for(|&n| n == 0).await.is_err() {
                return;
            }
            let quiet = tokio::time::sleep(Duration::from_millis(150));
            tokio::select! {
                _ = quiet => return, // zero held through the grace window
                changed = inflight.changed() => match changed {
                    // A late command landed (or re-confirmed zero): if it
                    // is in flight the loop waits for it again; if zero,
                    // the quiet window simply restarts.
                    Ok(()) => continue,
                    Err(_) => return, // no sender left — nothing can start
                },
            }
        }
    };
    if tokio::time::timeout_at(deadline, drain).await.is_ok() {
        tracing::info!("drained; closing transport");
        ExitCode::SUCCESS
    } else {
        tracing::error!(
            "in-flight commands did not finish within the drain bound; exiting without them"
        );
        ExitCode::FAILURE
    }
}
