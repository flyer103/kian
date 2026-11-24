//! `kian-imessage-bridge` — see the crate docs (CHANNEL-IMESSAGE §3).
//!
//! Startup is fail-closed: the config, the token file and the `imsg rpc`
//! child are proven before the listener binds (a broken bridge refuses to
//! start with the operator action named). Full Disk Access being revoked
//! later is **not** a startup failure: `/v1/health` reports
//! `database.ready: false` and sends keep working, exactly as the
//! contract's degradation table says.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use kian_imessage_bridge::app::App;
use kian_imessage_bridge::config::{BridgeConfig, expand};
use kian_imessage_bridge::imsg::{Backend, ImsgRpc};
use kian_imessage_bridge::state::CursorStore;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

const DEFAULT_CONFIG: &str = "~/.config/kian/imessage-bridge.json";

/// How many `messages.after` pages the first-boot tail bootstrap may
/// walk (a safety bound; 500 rows per page, so 10k pages ≈ 5M rows).
const MAX_BOOTSTRAP_PAGES: u32 = 10_000;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut config: Option<PathBuf> = None;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => match iter.next() {
                Some(path) => config = expand(path),
                None => {
                    eprintln!("kian-imessage-bridge: --config requires a path");
                    return ExitCode::from(2);
                }
            },
            "--version" | "-V" => {
                println!("kian-imessage-bridge {}", env!("CARGO_PKG_VERSION"));
                return ExitCode::SUCCESS;
            }
            "--help" | "-h" => {
                print!("{}", USAGE);
                return ExitCode::SUCCESS;
            }
            other => {
                eprintln!("kian-imessage-bridge: unknown argument `{other}`\n{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    let config_path = config
        .or_else(|| expand(DEFAULT_CONFIG))
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG));

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("kian-imessage-bridge: cannot start the async runtime: {e}");
            return ExitCode::from(1);
        }
    };
    match runtime.block_on(run(&config_path)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("kian-imessage-bridge: {e}");
            ExitCode::from(1)
        }
    }
}

const USAGE: &str = "usage:
  kian-imessage-bridge [--config <path>]

The macOS-side iMessage bridge (CHANNEL-IMESSAGE §3): loopback HTTP over
`imsg rpc`, for `kian serve`'s `type: imessage` channel. Default config:
~/.config/kian/imessage-bridge.json. Run it as a LaunchAgent in the
logged-in Messages user's session (Full Disk Access + Automation →
Messages); never expose the listener off-host.
";

async fn run(config_path: &std::path::Path) -> Result<(), String> {
    let config = BridgeConfig::load(config_path).map_err(|e| e.to_string())?;
    let backend = ImsgRpc::new(config.imsg.clone(), config.db.clone());
    // Fail-closed: prove the `imsg rpc` child before binding. A missing
    // or incompatible imsg is a startup error naming the install
    // command (CHANNEL-IMESSAGE §10); a missing Full Disk Access is not
    // (the bridge still sends and reports `database.ready: false`).
    let status = backend.status().await.map_err(|e| {
        format!(
            "imsg rpc preflight failed: {e}\n\
                 install/upgrade imsg on this Mac (`brew install steipete/tap/imsg`) and re-run"
        )
    })?;
    if !status.db_ready {
        tracing::warn!(
            target: "kian::imessage_bridge",
            "Messages database is unreadable: grant Full Disk Access to this binary under \
             System Settings → Privacy & Security → Full Disk Access. Sends will still work; \
             receives will not."
        );
    }
    if config.participants.is_empty() {
        tracing::warn!(
            target: "kian::imessage_bridge",
            "watch.participants is empty: every conversation on this Mac is read. Set it to the \
             handles kian's topics allowlists — the filter is a privacy boundary, not a policy."
        );
    }

    let cursor = CursorStore::load(&config.state_dir, &config.db);
    if cursor.fresh() {
        let tail = bootstrap_tail(&backend, &cursor).await?;
        tracing::info!(
            target: "kian::imessage_bridge",
            tail_rowid = tail,
            "first boot (or replaced database): bootstrapped the read cursor at the current tail"
        );
    }

    let app = Arc::new(App::new(
        backend,
        config.token.clone(),
        cursor,
        config.participants.clone(),
        config.poll_interval,
        config.catchup_rows,
        config.page_limit,
        config.attachments,
        config.max_attachment_bytes,
    ));
    let listener = TcpListener::bind(config.listen)
        .await
        .map_err(|e| format!("cannot bind {}: {e}", config.listen))?;
    // ASCII only: never print the token.
    eprintln!(
        "kian-imessage-bridge {} listening on http://{} (imsg {}, database ready: {}, participants: {})",
        env!("CARGO_PKG_VERSION"),
        config.listen,
        status.version,
        status.db_ready,
        if config.participants.is_empty() {
            "ALL".to_string()
        } else {
            format!("{} handle(s)", config.participants.len())
        }
    );
    axum::serve(listener, app.router())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| format!("serve failed: {e}"))
}

/// Walks `messages.after` from rowid 1 to the current tail so the first
/// boot starts at "now" instead of replaying history (CHANNEL-IMESSAGE
/// §9: a first-ever run has an honest gap of "before this bridge
/// existed").
async fn bootstrap_tail(backend: &ImsgRpc, cursor: &CursorStore) -> Result<u64, String> {
    let mut start = 1u64;
    for _ in 0..MAX_BOOTSTRAP_PAGES {
        let page = backend
            .after(start, 500, false)
            .await
            .map_err(|e| format!("cannot read the Messages history: {e}"))?;
        cursor.observe(page.next_rowid);
        start = page.next_rowid;
        if !page.has_more {
            break;
        }
    }
    cursor.persist_now();
    Ok(start)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!(target: "kian::imessage_bridge", "shutting down");
    // Give an in-flight request a moment to settle.
    tokio::time::sleep(Duration::from_millis(50)).await;
}
