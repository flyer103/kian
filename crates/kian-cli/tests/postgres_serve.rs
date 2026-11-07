//! postgres assembly tests: fail-closed startup rules (always run —
//! they need no postgres) and the env-gated serve-assembly smoke
//! (`KIAN_PG_URL` absent → SKIP with a log line, never fail).

use kian_cli::config::ServeConfig;
use kian_cli::server::Server;

/// A minimal-but-valid config body (no `storage:`/`drain:` — the tests
/// append their own).
fn config_body() -> String {
    r#"listen: "127.0.0.1:0"
agent:
  id: default
  model_slots:
    main:
      provider: openai
      model: gpt-test
  tools: []
providers:
  openai:
    base_url: "http://127.0.0.1:9/v1"
    api_key_env: KIAN_C51_TEST_KEY
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#
    .into()
}

/// A full postgres-mode config: explicit holder, env-only URL.
fn postgres_yaml(url_env: &str, holder: &str) -> String {
    let mut yaml = config_body();
    yaml.push_str(&format!(
        "drain:\n  holder: {holder}\n  max_iterations: 4\nstorage:\n  event_store: postgres\n  postgres:\n    url_env: {url_env}\n"
    ));
    yaml
}

/// An env var name nothing sets (the missing-URL fixture).
const NEVER_SET_ENV: &str = "KIAN_C51_PG_URL_NEVER_SET";

/// True when the pg-gated tests should run.
fn gated() -> bool {
    match std::env::var("KIAN_PG_URL") {
        Ok(url) if !url.is_empty() => true,
        _ => {
            eprintln!(
                "SKIP: KIAN_PG_URL is not set — postgres serve-assembly suite disabled (env gate)"
            );
            false
        }
    }
}

/// `postgres://…/db?opts` → `postgres://…/<other-db>?opts`.
fn with_db(base: &str, db: &str) -> String {
    let (path_part, query) = match base.split_once('?') {
        Some((p, q)) => (p, format!("?{q}")),
        None => (base, String::new()),
    };
    let scheme_end = path_part.find("://").map(|i| i + 3).unwrap_or(0);
    let rest = &path_part[scheme_end..];
    let root = match rest.find('/') {
        Some(i) => path_part[..scheme_end + i].to_string(),
        None => path_part.to_string(),
    };
    format!("{root}/{db}{query}")
}

// ---------------------------------------------------------------------------
// Fail-closed rules (no postgres needed)
// ---------------------------------------------------------------------------

/// `event_store: postgres` with a missing URL env refuses to start —
/// before any connection is attempted.
#[tokio::test]
async fn postgres_mode_with_missing_url_env_refuses_to_start() {
    unsafe { std::env::set_var("KIAN_C51_TEST_KEY", "test-key") };
    let config =
        ServeConfig::parse(&postgres_yaml(NEVER_SET_ENV, "replica-test")).expect("valid shape");
    assert!(config.storage.is_postgres());
    let err = match Server::from_config(config).await {
        Ok(_) => panic!("postgres mode with a missing URL env must refuse to start"),
        Err(e) => e.to_string(),
    };
    assert!(
        err.contains(NEVER_SET_ENV),
        "error must name the missing env var: {err}"
    );
    assert!(
        err.contains("not set"),
        "error must say the URL env is missing: {err}"
    );
}

/// The default drain.holder is refused under postgres (condition
/// (c)): two replicas sharing a holder would defeat same-holder claim
/// admission.
#[test]
fn postgres_mode_refuses_default_holder() {
    let mut yaml = postgres_yaml("KIAN_PG_URL", "kian-serve");
    // Holder kian-serve IS the default — parse must refuse.
    let err = ServeConfig::parse(&yaml).expect_err("default holder must refuse");
    let msg = err.to_string();
    assert!(
        msg.contains("drain.holder") && msg.contains("unique"),
        "error must demand a unique holder: {msg}"
    );
    yaml = yaml.replace("holder: kian-serve", "holder: replica-a");
    ServeConfig::parse(&yaml).expect("explicit holder is accepted");
}

/// An empty holder is refused under postgres too.
#[test]
fn postgres_mode_refuses_empty_holder() {
    let yaml = postgres_yaml("KIAN_PG_URL", "\"\"");
    let err = ServeConfig::parse(&yaml).expect_err("empty holder must refuse");
    assert!(err.to_string().contains("drain.holder"), "{err}");
}

/// Storage the active backend would ignore is refused (one server, one
/// database): a sqlite path under postgres mode, and a postgres section
/// under sqlite mode, both refuse.
#[test]
fn postgres_and_sqlite_storage_never_coexist() {
    let mut yaml = postgres_yaml("KIAN_PG_URL", "replica-a");
    yaml.push_str("  sqlite: \"/tmp/ignored.sqlite\"\n");
    let err = ServeConfig::parse(&yaml).expect_err("ignored sqlite path must refuse");
    assert!(
        err.to_string().contains("storage.sqlite"),
        "{}",
        err.to_string()
    );

    let mut yaml = config_body();
    yaml.push_str(
        "storage:\n  sqlite: \"/tmp/kian.sqlite\"\n  postgres:\n    url_env: KIAN_PG_URL\n",
    );
    let err = ServeConfig::parse(&yaml).expect_err("ignored pg section must refuse");
    assert!(
        err.to_string().contains("storage.postgres"),
        "{}",
        err.to_string()
    );
}

// ---------------------------------------------------------------------------
// Env-gated serve-assembly smoke
// ---------------------------------------------------------------------------

/// `kian serve` on postgres opens the matched store pair and runs the
/// inbox rebuild before the listener could accept traffic — proven by
/// the recorded startup order.
#[tokio::test]
async fn pg_serve_assembles_and_rebuilds_before_traffic() {
    if !gated() {
        return;
    }
    let base = std::env::var("KIAN_PG_URL").unwrap();
    let db = format!("kian_cli_c51_{}", uuid::Uuid::new_v4().simple());
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&base)
        .await
        .expect("connect KIAN_PG_URL");
    sqlx::query(&format!("CREATE DATABASE {db}"))
        .execute(&admin)
        .await
        .expect("create throwaway database");
    admin.close().await;
    unsafe {
        std::env::set_var("KIAN_C51_PG_URL", with_db(&base, &db));
        std::env::set_var("KIAN_C51_TEST_KEY", "test-key");
    }

    let config = ServeConfig::parse(&postgres_yaml("KIAN_C51_PG_URL", "replica-c51"))
        .expect("valid postgres config");
    let server = Server::from_config(config)
        .await
        .expect("postgres assembly must boot");
    let order = server.startup_order();
    assert!(
        order.contains(&"startup_rebuild_done"),
        "inbox rebuild must complete before the listener: {order:?}"
    );

    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&base)
        .await
        .unwrap();
    let _ = sqlx::query(&format!("DROP DATABASE {db}"))
        .execute(&admin)
        .await;
    admin.close().await;
}
