//! `kian` binary entry point (cards C13–C14, C17).
//!
//! - `kian` / `kian --version` — print the version.
//! - `kian serve [--config <path>]` — run the single-machine server
//!   (axum JSON-RPC + SSE control plane). Default config path:
//!   `./kian.yaml`. Loading is fail-closed: a malformed config refuses
//!   to start with a clear error and a non-zero exit code.
//! - `kian cache-stats [--config <path>] [--session <uuid>] [--json]`
//!   — S3 cache-byte stability report (C17): prefix hit-rate and
//!   invalidation-cause counters derived from the event stream.
//! - `kian session inspect <uuid>` — projection + counters (C14;
//!   embedded read-only).
//! - `kian session replay <uuid> [--url <base>]` — re-derive everything
//!   from events, verify A2 byte-identity, optionally diff against the
//!   live server projection.
//! - `kian session export <uuid> [--since S] [--until U] [--out F]` —
//!   event interval → JSONL audit package.
//! - `kian wake-now <uuid>` / `kian lease inspect` /
//!   `kian recovery budget reset <uuid>` — operator actions (C14).
//!
//! Read-only commands open the sqlite file in-process (read-only);
//! mutating commands talk to a running `serve` via `POST /ops/rpc`.
//! `--json` switches every command to its documented JSON schema.

use std::process::ExitCode;

const USAGE: &str = "usage:
  kian serve [--config <path>]          run the kian server (default config: ./kian.yaml)
  kian cache-stats [--config <path>] [--session <uuid>] [--json]
                                         S3 cache-byte stability report (C17)
  kian session inspect <uuid> [--db <sqlite>] [--config <path>] [--json]
                                         projection + counters (C14)
  kian session replay <uuid> [--db <sqlite>] [--url <base>] [--config <path>] [--json]
                                         re-derive everything from events (C14)
  kian session export <uuid> [--since S] [--until U] [--out <file>] [--db <sqlite>] [--config <path>]
                                         event interval → JSONL audit package (C14)
  kian wake-now <uuid> [--url <base>] [--config <path>] [--json]
                                         operator wake via the running server (C14)
  kian lease inspect [--db <sqlite>] [--config <path>] [--json]
                                         epoch/TTL/holder of every lease (C14)
  kian recovery budget reset <uuid> [--reason <text>] [--url <base>] [--config <path>] [--json]
                                         explicit recovery/budget-reset event (C14)
  kian --version                        print the version
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        None | Some("--version") | Some("-V") => {
            println!("kian {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Some("serve") => serve(&args[2..]),
        Some("cache-stats") => cache_stats(&args[2..]),
        Some("session") => session(&args[2..]),
        Some("wake-now") => wake_now(&args[2..]),
        Some("lease") => lease(&args[2..]),
        Some("recovery") => recovery(&args[2..]),
        Some("help") | Some("--help") | Some("-h") => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        Some(other) => {
            eprintln!("kian: unknown command `{other}`\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

fn serve(args: &[String]) -> ExitCode {
    let mut config_path = std::path::PathBuf::from("kian.yaml");
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => match iter.next() {
                Some(p) => config_path = p.into(),
                None => {
                    eprintln!("kian: --config requires a path\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            other => {
                eprintln!("kian: unknown serve option `{other}`\n{USAGE}");
                return ExitCode::from(2);
            }
        }
    }

    // Fail-closed config load: refuse to start with a clear error.
    let config = match kian_cli::config::ServeConfig::load(&config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("kian: refusing to start — {e}");
            return ExitCode::from(2);
        }
    };
    let addr = match config.listen() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("kian: refusing to start — {e}");
            return ExitCode::from(2);
        }
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("kian: cannot start async runtime: {e}");
            return ExitCode::from(1);
        }
    };

    runtime.block_on(async move {
        let server = match kian_cli::server::Server::from_config(config).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("kian: refusing to start — {e}");
                std::process::exit(2);
            }
        };
        if let Err(e) = std::sync::Arc::new(server).serve(addr).await {
            eprintln!("kian: {e}");
            std::process::exit(1);
        }
    });
    ExitCode::SUCCESS
}

/// `kian cache-stats [--config <path>] [--session <uuid>] [--json]` —
/// the C17 S3 report from the configured store.
fn cache_stats(args: &[String]) -> ExitCode {
    let mut config_path = std::path::PathBuf::from("kian.yaml");
    let mut session: Option<kian_core::model::SessionId> = None;
    let mut json = false;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => match iter.next() {
                Some(p) => config_path = p.into(),
                None => {
                    eprintln!("kian: --config requires a path\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--session" => match iter.next() {
                Some(s) => match s.parse::<uuid::Uuid>() {
                    Ok(u) => session = Some(kian_core::model::SessionId(u)),
                    Err(_) => {
                        eprintln!("kian: --session requires a uuid\n{USAGE}");
                        return ExitCode::from(2);
                    }
                },
                None => {
                    eprintln!("kian: --session requires a uuid\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--json" => json = true,
            other => {
                eprintln!("kian: unknown cache-stats option `{other}`\n{USAGE}");
                return ExitCode::from(2);
            }
        }
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("kian: cannot start async runtime: {e}");
            return ExitCode::from(1);
        }
    };
    match runtime.block_on(kian_cli::cache_metrics::run_cache_stats(
        &config_path,
        session,
        json,
    )) {
        Ok(out) => {
            print!("{out}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("kian: cache-stats failed — {e}");
            ExitCode::from(2)
        }
    }
}

// ---------------------------------------------------------------------------
// C14 session commands
// ---------------------------------------------------------------------------

/// Shared flag set for the C14 commands (each uses the subset it needs).
struct SessionArgs {
    config: std::path::PathBuf,
    db: Option<std::path::PathBuf>,
    url: Option<String>,
    session: Option<kian_core::model::SessionId>,
    since: Option<u64>,
    until: Option<u64>,
    out: Option<std::path::PathBuf>,
    reason: Option<String>,
    json: bool,
}

impl Default for SessionArgs {
    fn default() -> Self {
        Self {
            config: std::path::PathBuf::from("kian.yaml"),
            db: None,
            url: None,
            session: None,
            since: None,
            until: None,
            out: None,
            reason: None,
            json: false,
        }
    }
}

/// Parses the shared C14 flag set; `usage_for` names the subcommand in
/// errors. Positional <uuid> sets `session`.
fn parse_session_args(args: &[String], usage_for: &str) -> Result<SessionArgs, String> {
    let mut out = SessionArgs::default();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => match iter.next() {
                Some(p) => out.config = p.into(),
                None => return Err(format!("--config requires a path ({usage_for})")),
            },
            "--db" => match iter.next() {
                Some(p) => out.db = Some(p.into()),
                None => return Err(format!("--db requires a path ({usage_for})")),
            },
            "--url" => match iter.next() {
                Some(u) => out.url = Some(u.to_string()),
                None => return Err(format!("--url requires a base URL ({usage_for})")),
            },
            "--since" | "--until" => {
                let which = arg.as_str();
                let Some(raw) = iter.next() else {
                    return Err(format!("{which} requires a seq number ({usage_for})"));
                };
                let n: u64 = raw
                    .parse()
                    .map_err(|_| format!("{which} requires a seq number, got `{raw}`"))?;
                if which == "--since" {
                    out.since = Some(n);
                } else {
                    out.until = Some(n);
                }
            }
            "--out" => match iter.next() {
                Some(p) => out.out = Some(p.into()),
                None => return Err(format!("--out requires a path ({usage_for})")),
            },
            "--reason" => match iter.next() {
                Some(r) => out.reason = Some(r.to_string()),
                None => return Err(format!("--reason requires a text ({usage_for})")),
            },
            "--json" => out.json = true,
            other if !other.starts_with("--") => {
                let sid = kian_cli::commands::parse_session(other)
                    .map_err(|e| format!("{e} ({usage_for})"))?;
                out.session = Some(sid);
            }
            other => return Err(format!("unknown option `{other}` ({usage_for})")),
        }
    }
    Ok(out)
}

/// Runs `f` on a fresh multi-thread runtime and prints its output.
fn run_command(
    f: impl std::future::Future<Output = Result<String, String>>,
    what: &str,
) -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("kian: cannot start async runtime: {e}");
            return ExitCode::from(1);
        }
    };
    match runtime.block_on(f) {
        Ok(out) => {
            print!("{out}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("kian: {what} failed — {e}");
            ExitCode::from(2)
        }
    }
}

/// `kian session inspect|replay|export <uuid> [flags]`
fn session(args: &[String]) -> ExitCode {
    let Some(sub) = args.first().map(String::as_str) else {
        eprintln!("kian: `session` requires a subcommand (inspect, replay, export)\n{USAGE}");
        return ExitCode::from(2);
    };
    let usage_for = format!("kian session {sub}");
    let parsed = match parse_session_args(&args[1..], &usage_for) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("kian: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let Some(session) = parsed.session else {
        eprintln!("kian: {usage_for} requires a session uuid\n{USAGE}");
        return ExitCode::from(2);
    };
    match sub {
        "inspect" => run_command(
            kian_cli::commands::run_inspect(
                &parsed.config,
                parsed.db.as_deref(),
                session,
                parsed.json,
            ),
            "session inspect",
        ),
        "replay" => run_command(
            kian_cli::commands::run_replay(
                &parsed.config,
                parsed.db.as_deref(),
                parsed.url.as_deref(),
                session,
                parsed.json,
            ),
            "session replay",
        ),
        "export" => {
            // Export writes the JSONL body itself (stdout or --out).
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("kian: cannot start async runtime: {e}");
                    return ExitCode::from(1);
                }
            };
            match runtime.block_on(kian_cli::commands::run_export(
                &parsed.config,
                parsed.db.as_deref(),
                session,
                parsed.since,
                parsed.until,
                parsed.out.as_deref(),
            )) {
                Ok(out) => {
                    print!("{out}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("kian: session export failed — {e}");
                    ExitCode::from(2)
                }
            }
        }
        other => {
            eprintln!("kian: unknown session subcommand `{other}`\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

/// `kian wake-now <uuid> [--url <base>] [--json]`
fn wake_now(args: &[String]) -> ExitCode {
    let parsed = match parse_session_args(args, "kian wake-now") {
        Ok(p) => p,
        Err(e) => {
            eprintln!("kian: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let Some(session) = parsed.session else {
        eprintln!("kian: wake-now requires a session uuid\n{USAGE}");
        return ExitCode::from(2);
    };
    run_command(
        kian_cli::commands::run_wake_now(
            &parsed.config,
            parsed.url.as_deref(),
            session,
            parsed.json,
        ),
        "wake-now",
    )
}

/// `kian lease inspect [--db <sqlite>] [--json]`
fn lease(args: &[String]) -> ExitCode {
    let Some(sub) = args.first().map(String::as_str) else {
        eprintln!("kian: `lease` requires a subcommand (inspect)\n{USAGE}");
        return ExitCode::from(2);
    };
    if sub != "inspect" {
        eprintln!("kian: unknown lease subcommand `{sub}`\n{USAGE}");
        return ExitCode::from(2);
    }
    let parsed = match parse_session_args(&args[1..], "kian lease inspect") {
        Ok(p) => p,
        Err(e) => {
            eprintln!("kian: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    run_command(
        kian_cli::commands::run_lease_inspect(&parsed.config, parsed.db.as_deref(), parsed.json),
        "lease inspect",
    )
}

/// `kian recovery budget reset <uuid> [--reason <text>] [--url <base>]`
fn recovery(args: &[String]) -> ExitCode {
    let ok_shape = args.first().map(String::as_str) == Some("budget")
        && args.get(1).map(String::as_str) == Some("reset");
    if !ok_shape {
        eprintln!("kian: `recovery` supports only `budget reset <uuid>`\n{USAGE}");
        return ExitCode::from(2);
    }
    let parsed = match parse_session_args(&args[2..], "kian recovery budget reset") {
        Ok(p) => p,
        Err(e) => {
            eprintln!("kian: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let Some(session) = parsed.session else {
        eprintln!("kian: recovery budget reset requires a session uuid\n{USAGE}");
        return ExitCode::from(2);
    };
    run_command(
        kian_cli::commands::run_budget_reset(
            &parsed.config,
            parsed.url.as_deref(),
            session,
            parsed.reason.as_deref(),
            parsed.json,
        ),
        "recovery budget reset",
    )
}
