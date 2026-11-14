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

use kian_endpoint::proto::RegisterToken;
use kian_endpoint::{Endpoint, HeartbeatTap, RegisterBackoff, RegisterInfo, register_loop};
use kiand::config::{Config, Flags};
use kiand::executor::DaemonExecutor;

const USAGE: &str = "usage:
  kiand [--config <path>] [--endpoint-id <id>] [--listen <host:port>]
        [--workspace-root <dir>]
        [--max-concurrent <n>] [--heartbeat-ms <n>] [--exec-timeout-ms <n>]
                                         run the endpoint daemon (default listen: 127.0.0.1:0;
                                         the bound address is printed on stdout)
  kiand --register-to <kian-host:port> [--endpoint-id <id>] [--description <text>]
        [--heartbeat-ms <n>] [--token-env <NAME>]
                                         worker-dial register mode: dial kian, register with the
                                         token from the environment variable NAME
                                         (default KIAN_WORKER_TOKEN; only the NAME is on argv)
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
            "--workspace-root" => {
                flags.workspace_root = Some(next_value(&mut iter, "--workspace-root")?)
            }
            "--register-to" => flags.register_to = Some(next_value(&mut iter, "--register-to")?),
            "--description" => flags.description = Some(next_value(&mut iter, "--description")?),
            "--token-env" => flags.token_env = Some(next_value(&mut iter, "--token-env")?),
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

    if config.register_to.is_some() {
        return run_worker(config).await;
    }
    run_server(config).await
}

/// Read the worker token from the named environment variable. Only the
/// variable NAME is configurable; an absent/empty value refuses startup
/// with an actionable message (never echoing any token material).
fn worker_token(
    env_name: &str,
    get: impl Fn(&str) -> Option<String>,
) -> Result<RegisterToken, String> {
    match get(env_name).filter(|v| !v.is_empty()) {
        Some(value) => Ok(RegisterToken::new(value)),
        None => Err(format!(
            "register mode requires the worker token in environment variable \
             `{env_name}` (set it, or point --token-env at another variable); \
             only the variable name belongs on the command line"
        )),
    }
}

/// Worker-dial register mode: dial kian, register, serve the steady
/// state, reconnect with bounded backoff. SIGTERM/SIGINT drains in-flight
/// commands and sends `bye` before exiting; a terminal refusal logs one
/// actionable line and exits non-zero.
async fn run_worker(config: Config) -> ExitCode {
    let addr = match config.register_addr() {
        Ok(Some(addr)) => addr,
        Ok(None) => unreachable!("run_worker is only entered in register mode"),
        Err(e) => {
            eprintln!("kiand: refusing to start — {e}");
            return ExitCode::from(2);
        }
    };
    let token = match worker_token(&config.token_env, |key| std::env::var(key).ok()) {
        Ok(token) => token,
        Err(message) => {
            eprintln!("kiand: refusing to start — {message}");
            return ExitCode::from(2);
        }
    };

    let (executor, _inflight) = DaemonExecutor::new(config.exec_timeout_ms);
    let endpoint = Arc::new(
        Endpoint::new(config.endpoint_id.clone(), executor)
            .with_max_concurrent(config.max_concurrent)
            .with_workspace_root(config.workspace_root.clone())
            .with_heartbeat(Duration::from_millis(config.heartbeat_ms))
            .with_workspace_root(config.workspace_root.clone()),
    );
    let info = RegisterInfo {
        endpoint_id: config.endpoint_id.clone(),
        description: config.description.clone(),
        token,
    };
    tracing::info!(
        endpoint = %config.endpoint_id,
        %addr,
        max_concurrent = config.max_concurrent,
        "kiand worker-dial register mode"
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut sigterm = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        Ok(sigterm) => sigterm,
        Err(e) => {
            eprintln!("kiand: cannot install SIGTERM handler: {e}");
            return ExitCode::from(1);
        }
    };
    let mut supervise = Box::pin(register_loop(
        endpoint,
        info,
        addr,
        RegisterBackoff::default(),
        shutdown_rx,
    ));
    let result = tokio::select! {
        result = &mut supervise => result,
        _ = sigterm.recv() => {
            tracing::info!("SIGTERM received; draining in-flight commands, then bye");
            let _ = shutdown_tx.send(true);
            supervise.await
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("SIGINT received; draining in-flight commands, then bye");
            let _ = shutdown_tx.send(true);
            supervise.await
        }
    };
    match result {
        Ok(()) => {
            tracing::info!("register loop ended; exiting");
            ExitCode::SUCCESS
        }
        Err(e) => {
            // The error is typed and token-free by construction; the hint
            // names the fix, never the secret.
            let hint = match &e {
                kian_endpoint::EndpointError::RegisterRejected { reason, .. } => match reason {
                    kian_endpoint::proto::RefusalReason::BadToken => format!(
                        "check that `{}` holds the token kian expects",
                        config.token_env
                    ),
                    kian_endpoint::proto::RefusalReason::VersionMismatch => {
                        "upgrade kiand or kian so both speak the same KiandProto version".into()
                    }
                    kian_endpoint::proto::RefusalReason::DuplicateStatic => format!(
                        "worker id `{}` is statically configured on kian; use a dynamic id",
                        config.endpoint_id
                    ),
                    _ => "see the kian registration log for details".into(),
                },
                _ => "see the kian registration log for details".into(),
            };
            eprintln!("kiand: worker registration failed: {e} — {hint}");
            ExitCode::from(1)
        }
    }
}

async fn run_server(config: Config) -> ExitCode {
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
            .with_workspace_root(config.workspace_root.clone())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_token_env_refuses_startup_with_actionable_message() {
        let err = worker_token("KIAN_WORKER_TOKEN", |_| None).unwrap_err();
        assert!(
            err.contains("KIAN_WORKER_TOKEN"),
            "names the variable: {err}"
        );
        assert!(err.contains("--token-env"), "names the fix: {err}");
    }

    #[test]
    fn empty_token_env_value_is_treated_as_absent() {
        let err = worker_token("KIAN_WORKER_TOKEN", |_| Some(String::new())).unwrap_err();
        assert!(err.contains("KIAN_WORKER_TOKEN"));
    }

    #[test]
    fn custom_token_env_name_is_honored_and_value_never_echoed() {
        let token = worker_token("MY_SECRET_VAR", |key| {
            (key == "MY_SECRET_VAR").then(|| "fake-test-token-0123456789".to_string())
        })
        .unwrap();
        // The newtype redacts: neither Debug nor Display leaks the value.
        assert!(!format!("{token:?}").contains("fake-test-token"));
        assert!(!format!("{token}").contains("fake-test-token"));
        // Errors naming the variable never carry the value either.
        let err = worker_token("MY_SECRET_VAR", |_| None).unwrap_err();
        assert!(err.contains("MY_SECRET_VAR"));
    }

    #[test]
    fn register_mode_flags_parse() {
        let args = vec![
            "--register-to".to_string(),
            "127.0.0.1:9920".to_string(),
            "--endpoint-id".to_string(),
            "box-a".to_string(),
            "--description".to_string(),
            "build box".to_string(),
            "--token-env".to_string(),
            "MY_SECRET_VAR".to_string(),
        ];
        let flags = parse_flags(&args).unwrap();
        assert_eq!(flags.register_to.as_deref(), Some("127.0.0.1:9920"));
        assert_eq!(flags.endpoint_id.as_deref(), Some("box-a"));
        assert_eq!(flags.description.as_deref(), Some("build box"));
        assert_eq!(flags.token_env.as_deref(), Some("MY_SECRET_VAR"));
    }
}
