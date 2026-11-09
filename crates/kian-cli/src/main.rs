//! `kian` binary entry point.
//!
//! - `kian` / `kian --version` — print the version.
//! - `kian serve [--config <path>]` — run the single-machine server
//!   (axum JSON-RPC + SSE control plane). Default config path:
//!   `./kian.yaml`. Loading is fail-closed: a malformed config refuses
//!   to start with a clear error and a non-zero exit code.
//! - `kian cache-stats [--config <path>] [--session <uuid>] [--json]`
//!   — cache-byte stability report: prefix hit-rate and
//!   invalidation-cause counters derived from the event stream.
//! - `kian usage-stats [--config <path>] [--session <uuid>] [--json]`
//!   — provider token-usage report: per-store token totals derived
//!   from `turn/end` usage payloads (verbatim, no pricing).
//! - `kian session inspect <uuid>` — projection + counters (
//!   embedded read-only).
//! - `kian session replay <uuid> [--url <base>]` — re-derive everything
//!   from events, verify A2 byte-identity, optionally diff against the
//!   live server projection.
//! - `kian session export <uuid> [--since S] [--until U] [--out F]
//!   [--with-event-ids]` — event interval → JSONL audit package
//!   (`--with-event-ids` adds the derived event id per line). An
//!   archived session exports from its verified package, byte-identical
//!   to the pre-archive export.
//! - `kian archive run|verify` — opt-in export-only retention
//!   (READ-MODEL §7): package → verify → commit, and package
//!   re-verification. `kian session import <package>` restores one.
//! - `kian wake-now <uuid>` / `kian lease inspect` /
//!   `kian recovery budget reset <uuid>` — operator actions.
//!
//! Read-only commands open the sqlite file in-process (read-only);
//! mutating commands talk to a running `serve` via `POST /ops/rpc`.
//! `--json` switches every command to its documented JSON schema.

use std::process::ExitCode;

const USAGE: &str = "usage:
  kian serve [--config <path>]          run the kian server (default config: ./kian.yaml)
  kian cache-stats [--config <path>] [--session <uuid>] [--json]
                                         cache-byte stability report
  kian usage-stats [--config <path>] [--session <uuid>] [--json]
                                         provider token-usage report
  kian session inspect <uuid> [--db <sqlite>] [--config <path>] [--json]
                                         projection + counters
  kian session replay <uuid> [--db <sqlite>] [--url <base>] [--config <path>] [--json]
                                         re-derive everything from events
  kian session export <uuid> [--since S] [--until U] [--out <file>] [--with-event-ids] [--db <sqlite>] [--config <path>]
                                         event interval → JSONL audit package (--with-event-ids adds the derived event id)
  kian session import <package-dir> [--db <sqlite>] [--config <path>] [--json]
                                         verify + restore an archive package into a store (replay/forensics)
  kian archive run [--db <sqlite>] [--config <path>] [--json]
                                         one opt-in retention pass (write → verify → commit per terminal session)
  kian archive verify <uuid> [--db <sqlite>] [--config <path>] [--json]
                                         verify a session's archive package (hashes, replay, counts)
  kian wake-now <uuid> [--url <base>] [--config <path>] [--json]
                                         operator wake via the running server
  kian lease inspect [--db <sqlite>] [--config <path>] [--json]
                                         epoch/TTL/holder of every lease
  kian tickets [--db <sqlite>] [--config <path>] [--json]
                                         Ticket stage board: every worker session's
                                         'what is it waiting for' stage
  kian workers [--db <sqlite>] [--config <path>] [--json]
                                         worker membership board: id, online|offline,
                                         last heartbeat, bound live session/thread
                                         (read-only; description and protocol version
                                         are runtime-only facts, absent by design)
  kian recovery budget reset <uuid> [--reason <text>] [--url <base>] [--config <path>] [--json]
                                         explicit recovery/budget-reset event
  kian wechat login [--config <path>] [--channel <name>] [--json]
                                         WeChat QR login → the channel's 0600 credential file
                                         (prints account/user ids, never the token)
  kian mock-channel-send <channel> [--text <text> | --payload <json>] [--topic <topic>]
                                  [--url <base>] [--config <path>] [--json]
                                         demo channel injection via ops
  kian memory create [--config <path>] [--json]
                                         operator-owned memory session (via createSession)
  kian memory dream --session <uuid> --source <uuid> --from <seq> --to <seq>
                                 [--url <base>] [--config <path>] [--json]
                                         one dream request via the memory_dream ops method
  kian memory list --session <uuid> [--db <sqlite>] [--config <path>] [--json]
                                         the projected notes of a memory session
  kian memory search <query> --session <uuid> [--db <sqlite>] [--config <path>] [--json]
                                         read-only lexical search over the folded notes
  kian memory map --session <uuid> [--db <sqlite>] [--config <path>] [--json]
                                         read-only knowledge map of the verified notes
  kian memory show --session <uuid> <event-id|subject> [--db <sqlite>] [--config <path>] [--json]
                                         one note by event id or subject
  kian memory verify --session <uuid> [--db <sqlite>] [--config <path>] [--json]
                                         re-check derived_from ranges against the sources
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
        Some("usage-stats") => usage_stats(&args[2..]),
        Some("session") => session(&args[2..]),
        Some("archive") => archive(&args[2..]),
        Some("wake-now") => wake_now(&args[2..]),
        Some("mock-channel-send") => mock_channel_send(&args[2..]),
        Some("memory") => memory(&args[2..]),
        Some("lease") => lease(&args[2..]),
        Some("tickets") => tickets(&args[2..]),
        Some("workers") => workers(&args[2..]),
        Some("recovery") => recovery(&args[2..]),
        Some("wechat") => wechat(&args[2..]),
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
    // `serve` is a long-lived resident — without a subscriber
    // its tracing lines (including the resident re-arm succession note)
    // go nowhere and operators have no boot evidence. Default to warn;
    // RUST_LOG overrides (the demo runs with RUST_LOG=info to
    // capture the re-arm line in serve.log).
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
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
/// the report from the configured store.
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

/// `kian usage-stats [--config <path>] [--session <uuid>] [--json]` —
/// the provider token-usage report from the configured store
/// (mirrors `cache-stats`).
fn usage_stats(args: &[String]) -> ExitCode {
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
                eprintln!("kian: unknown usage-stats option `{other}`\n{USAGE}");
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
    match runtime.block_on(kian_cli::usage_metrics::run_usage_stats(
        &config_path,
        session,
        json,
    )) {
        Ok(out) => {
            print!("{out}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("kian: usage-stats failed — {e}");
            ExitCode::from(2)
        }
    }
}

// ---------------------------------------------------------------------------
// session commands
// ---------------------------------------------------------------------------

/// Shared flag set for the commands (each uses the subset it needs).
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
    /// `session export --with-event-ids` — adds the derived event
    /// id to each exported line (export path only; other subcommands
    /// ignore the flag).
    with_event_ids: bool,
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
            with_event_ids: false,
        }
    }
}

/// Parses the shared flag set; `usage_for` names the subcommand in
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
            "--with-event-ids" => out.with_event_ids = true,
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

/// `kian session inspect|replay|export|import ...`
fn session(args: &[String]) -> ExitCode {
    let Some(sub) = args.first().map(String::as_str) else {
        eprintln!(
            "kian: `session` requires a subcommand (inspect, replay, export, import)\n{USAGE}"
        );
        return ExitCode::from(2);
    };
    // `session import <package-dir>` takes a directory, not a uuid —
    // parse it with its own flag loop.
    if sub == "import" {
        let mut config = std::path::PathBuf::from("kian.yaml");
        let mut db: Option<std::path::PathBuf> = None;
        let mut package: Option<std::path::PathBuf> = None;
        let mut json = false;
        let mut iter = args[1..].iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--config" => match iter.next() {
                    Some(p) => config = p.into(),
                    None => {
                        eprintln!("kian: --config requires a path\n{USAGE}");
                        return ExitCode::from(2);
                    }
                },
                "--db" => match iter.next() {
                    Some(p) => db = Some(p.into()),
                    None => {
                        eprintln!("kian: --db requires a path\n{USAGE}");
                        return ExitCode::from(2);
                    }
                },
                "--json" => json = true,
                other if !other.starts_with("--") => package = Some(other.into()),
                other => {
                    eprintln!("kian: unknown option `{other}` (session import)\n{USAGE}");
                    return ExitCode::from(2);
                }
            }
        }
        let Some(package) = package else {
            eprintln!("kian: session import requires a package directory\n{USAGE}");
            return ExitCode::from(2);
        };
        return run_command(
            kian_cli::archive::run_import(&package, db.as_deref(), &config, json),
            "session import",
        );
    }
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
                parsed.with_event_ids,
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

/// `kian archive run|verify [flags]` — the retention commands
/// (READ-MODEL §7, P2b). `run` is one opt-in retention pass; `verify`
/// re-checks a session's package.
fn archive(args: &[String]) -> ExitCode {
    let Some(sub) = args.first().map(String::as_str) else {
        eprintln!("kian: `archive` requires a subcommand (run, verify)\n{USAGE}");
        return ExitCode::from(2);
    };
    let usage_for = format!("kian archive {sub}");
    let parsed = match parse_session_args(&args[1..], &usage_for) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("kian: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match sub {
        "run" => run_command(
            kian_cli::archive::run_archive_run(&parsed.config, parsed.db.as_deref(), parsed.json),
            "archive run",
        ),
        "verify" => {
            let Some(session) = parsed.session else {
                eprintln!("kian: {usage_for} requires a session uuid\n{USAGE}");
                return ExitCode::from(2);
            };
            run_command(
                kian_cli::archive::run_archive_verify(
                    &parsed.config,
                    parsed.db.as_deref(),
                    session,
                    parsed.json,
                ),
                "archive verify",
            )
        }
        other => {
            eprintln!("kian: unknown archive subcommand `{other}`\n{USAGE}");
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

/// `kian mock-channel-send <channel> [--text S | --payload JSON]
/// [--topic T] [--url <base>] [--json]` (demo injection).
fn mock_channel_send(args: &[String]) -> ExitCode {
    let mut channel: Option<String> = None;
    let mut text: Option<String> = None;
    let mut payload_json: Option<String> = None;
    let mut topic: Option<String> = None;
    let mut url: Option<String> = None;
    let mut config = std::path::PathBuf::from("kian.yaml");
    let mut json = false;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--url" => match iter.next() {
                Some(u) => url = Some(u.clone()),
                None => {
                    eprintln!("kian: --url requires a base URL\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--config" => match iter.next() {
                Some(p) => config = p.into(),
                None => {
                    eprintln!("kian: --config requires a path\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--topic" => match iter.next() {
                Some(t) => topic = Some(t.clone()),
                None => {
                    eprintln!("kian: --topic requires a topic\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--text" => match iter.next() {
                Some(t) => text = Some(t.clone()),
                None => {
                    eprintln!("kian: --text requires a string\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--payload" => match iter.next() {
                Some(p) => payload_json = Some(p.clone()),
                None => {
                    eprintln!("kian: --payload requires JSON\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--json" => json = true,
            other if other.starts_with("--") => {
                eprintln!("kian: unknown flag `{other}`\n{USAGE}");
                return ExitCode::from(2);
            }
            other if channel.is_none() => channel = Some(other.to_owned()),
            other => {
                eprintln!("kian: unexpected argument `{other}`\n{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    let Some(channel) = channel else {
        eprintln!("kian: mock-channel-send requires a channel name\n{USAGE}");
        return ExitCode::from(2);
    };
    let payload = match (payload_json, text) {
        (Some(raw), _) => match serde_json::from_str(&raw) {
            Ok(value) => value,
            Err(e) => {
                eprintln!("kian: --payload is not valid JSON: {e}");
                return ExitCode::from(2);
            }
        },
        (None, Some(t)) => serde_json::json!({ "text": t }),
        (None, None) => {
            eprintln!("kian: mock-channel-send requires --text or --payload\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    run_command(
        kian_cli::commands::run_mock_channel_send(
            &config,
            url.as_deref(),
            &channel,
            topic.as_deref(),
            payload,
            json,
        ),
        "mock-channel-send",
    )
}

/// `kian memory create|dream|list|show|verify …` — the memory verbs
/// (round-8 plan, kian-87).
fn memory(args: &[String]) -> ExitCode {
    let Some(sub) = args.first().map(String::as_str) else {
        eprintln!(
            "kian: `memory` requires a subcommand (create, dream, list, search, map, show, verify)\n{USAGE}"
        );
        return ExitCode::from(2);
    };
    let usage_for = format!("kian memory {sub}");
    let rest = &args[1..];

    // `create` has no session flag.
    if sub == "create" {
        let mut config = std::path::PathBuf::from("kian.yaml");
        let mut json = false;
        let mut iter = rest.iter();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--config" => match iter.next() {
                    Some(p) => config = p.into(),
                    None => {
                        eprintln!("kian: --config requires a path\n{USAGE}");
                        return ExitCode::from(2);
                    }
                },
                "--json" => json = true,
                other => {
                    eprintln!("kian: unknown memory create option `{other}`\n{USAGE}");
                    return ExitCode::from(2);
                }
            }
        }
        return run_command(
            kian_cli::commands::run_memory_create(&config, json),
            "memory create",
        );
    }

    let parsed = match parse_memory_args(rest, &usage_for) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("kian: {e}\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    match sub {
        "dream" => {
            let (Some(session), Some(source), Some(from), Some(to)) = (
                parsed.session,
                parsed.source,
                parsed.from_seq,
                parsed.to_seq,
            ) else {
                eprintln!(
                    "kian: memory dream requires --session, --source, --from and --to\n{USAGE}"
                );
                return ExitCode::from(2);
            };
            run_command(
                kian_cli::commands::run_memory_dream(
                    &parsed.config,
                    parsed.url.as_deref(),
                    session,
                    source,
                    from,
                    to,
                    parsed.json,
                ),
                "memory dream",
            )
        }
        "list" => {
            let Some(session) = parsed.session else {
                eprintln!("kian: memory list requires a session uuid\n{USAGE}");
                return ExitCode::from(2);
            };
            run_command(
                kian_cli::commands::run_memory_list(
                    &parsed.config,
                    parsed.db.as_deref(),
                    session,
                    parsed.json,
                ),
                "memory list",
            )
        }
        "search" => {
            let (Some(session), Some(query)) = (parsed.session, parsed.selector.clone()) else {
                eprintln!("kian: memory search requires a session uuid and a <query>\n{USAGE}");
                return ExitCode::from(2);
            };
            run_command(
                kian_cli::commands::run_memory_search(
                    &parsed.config,
                    parsed.db.as_deref(),
                    session,
                    &query,
                    parsed.json,
                ),
                "memory search",
            )
        }
        "map" => {
            let Some(session) = parsed.session else {
                eprintln!("kian: memory map requires a session uuid\n{USAGE}");
                return ExitCode::from(2);
            };
            run_command(
                kian_cli::commands::run_memory_map(
                    &parsed.config,
                    parsed.db.as_deref(),
                    session,
                    parsed.json,
                ),
                "memory map",
            )
        }
        "show" => {
            let (Some(session), Some(selector)) = (parsed.session, parsed.selector.clone()) else {
                eprintln!(
                    "kian: memory show requires a session uuid and an <event-id|subject>\n{USAGE}"
                );
                return ExitCode::from(2);
            };
            run_command(
                kian_cli::commands::run_memory_show(
                    &parsed.config,
                    parsed.db.as_deref(),
                    session,
                    &selector,
                    parsed.json,
                ),
                "memory show",
            )
        }
        "verify" => {
            let Some(session) = parsed.session else {
                eprintln!("kian: memory verify requires a session uuid\n{USAGE}");
                return ExitCode::from(2);
            };
            run_command(
                kian_cli::commands::run_memory_verify(
                    &parsed.config,
                    parsed.db.as_deref(),
                    session,
                    parsed.json,
                ),
                "memory verify",
            )
        }
        other => {
            eprintln!("kian: unknown memory subcommand `{other}`\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

/// The shared flag set of the memory subcommands (`create` uses a
/// subset by hand).
struct MemoryArgs {
    config: std::path::PathBuf,
    db: Option<std::path::PathBuf>,
    url: Option<String>,
    session: Option<kian_core::model::SessionId>,
    source: Option<kian_core::model::SessionId>,
    from_seq: Option<u64>,
    to_seq: Option<u64>,
    selector: Option<String>,
    json: bool,
}

impl Default for MemoryArgs {
    fn default() -> Self {
        Self {
            config: std::path::PathBuf::from("kian.yaml"),
            db: None,
            url: None,
            session: None,
            source: None,
            from_seq: None,
            to_seq: None,
            selector: None,
            json: false,
        }
    }
}

fn parse_memory_args(args: &[String], usage_for: &str) -> Result<MemoryArgs, String> {
    let mut out = MemoryArgs::default();
    let mut iter = args.iter();
    let uuid =
        |raw: &str, flag: &str, usage_for: &str| -> Result<kian_core::model::SessionId, String> {
            kian_cli::commands::parse_session(raw).map_err(|e| format!("{e} ({usage_for}; {flag})"))
        };
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
            "--session" => match iter.next() {
                Some(s) => out.session = Some(uuid(s, "--session", usage_for)?),
                None => return Err(format!("--session requires a uuid ({usage_for})")),
            },
            "--source" => match iter.next() {
                Some(s) => out.source = Some(uuid(s, "--source", usage_for)?),
                None => return Err(format!("--source requires a uuid ({usage_for})")),
            },
            "--from" => match iter.next() {
                Some(raw) => {
                    out.from_seq = Some(
                        raw.parse()
                            .map_err(|_| format!("--from requires a seq number, got `{raw}`"))?,
                    );
                }
                None => return Err(format!("--from requires a seq number ({usage_for})")),
            },
            "--to" => match iter.next() {
                Some(raw) => {
                    out.to_seq = Some(
                        raw.parse()
                            .map_err(|_| format!("--to requires a seq number, got `{raw}`"))?,
                    );
                }
                None => return Err(format!("--to requires a seq number ({usage_for})")),
            },
            "--json" => out.json = true,
            // `show`'s positional <event-id|subject> and `search`'s
            // positional <query> (event ids are uuid-shaped; subjects
            // and queries are free text).
            other
                if !other.starts_with("--")
                    && out.selector.is_none()
                    && sub_takes_positional(usage_for) =>
            {
                out.selector = Some(other.to_string());
            }
            other => return Err(format!("unknown option `{other}` ({usage_for})")),
        }
    }
    Ok(out)
}

/// Whether the flag set is being parsed for a subcommand with a
/// positional argument (`show`: <event-id|subject>, `search`: <query>).
fn sub_takes_positional(usage_for: &str) -> bool {
    usage_for.ends_with("show") || usage_for.ends_with("search")
}

/// `kian wechat login [--config <path>] [--channel <name>] [--json]` —
/// the WeChat iLink QR login (writes the channel's 0600 credential
/// file; prints account/user ids, never the token; non-zero exit on
/// any failure).
fn wechat(args: &[String]) -> ExitCode {
    let Some(sub) = args.first().map(String::as_str) else {
        eprintln!("kian: `wechat` requires a subcommand (login)\n{USAGE}");
        return ExitCode::from(2);
    };
    if sub != "login" {
        eprintln!("kian: unknown wechat subcommand `{sub}`\n{USAGE}");
        return ExitCode::from(2);
    }
    let mut login_args = kian_cli::wechat_login::WechatLoginArgs::default();
    let mut iter = args[1..].iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => match iter.next() {
                Some(p) => login_args.config = p.into(),
                None => {
                    eprintln!("kian: --config requires a path\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--channel" => match iter.next() {
                Some(c) => login_args.channel = Some(c.clone()),
                None => {
                    eprintln!("kian: --channel requires a channel name\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--json" => login_args.json = true,
            other => {
                eprintln!("kian: unknown wechat login option `{other}`\n{USAGE}");
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
    match runtime.block_on(kian_cli::wechat_login::run_login(&login_args)) {
        Ok(summary) => {
            print!("{summary}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            if login_args.json {
                // Machine-readable failure (still no token — the error
                // strings never carry one).
                println!("{}", serde_json::json!({"status": "error", "error": e}));
            } else {
                eprintln!("kian: wechat login failed — {e}");
            }
            ExitCode::from(2)
        }
    }
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

/// `kian tickets [--db <sqlite>] [--config <path>] [--json]` — the
/// Ticket stage board (read-only embedded).
fn tickets(args: &[String]) -> ExitCode {
    let mut config = std::path::PathBuf::from("kian.yaml");
    let mut db: Option<std::path::PathBuf> = None;
    let mut json = false;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => match iter.next() {
                Some(p) => config = p.into(),
                None => {
                    eprintln!("kian: --config requires a path\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--db" => match iter.next() {
                Some(p) => db = Some(p.into()),
                None => {
                    eprintln!("kian: --db requires a path\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--json" => json = true,
            other => {
                eprintln!("kian: unknown tickets option `{other}`\n{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    run_command(
        kian_cli::commands::run_tickets(&config, db.as_deref(), json),
        "tickets",
    )
}

/// `kian workers [--db <sqlite>] [--config <path>] [--json]` — the
/// worker membership board (read-only embedded; never refuses).
fn workers(args: &[String]) -> ExitCode {
    let mut config = std::path::PathBuf::from("kian.yaml");
    let mut db: Option<std::path::PathBuf> = None;
    let mut json = false;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => match iter.next() {
                Some(p) => config = p.into(),
                None => {
                    eprintln!("kian: --config requires a path\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--db" => match iter.next() {
                Some(p) => db = Some(p.into()),
                None => {
                    eprintln!("kian: --db requires a path\n{USAGE}");
                    return ExitCode::from(2);
                }
            },
            "--json" => json = true,
            other => {
                eprintln!("kian: unknown workers option `{other}`\n{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    run_command(
        kian_cli::commands::run_workers(&config, db.as_deref(), json),
        "workers",
    )
}

/// `kian recovery budget reset <uuid> [--reason <text>] [--url <base>] [--json]`
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
