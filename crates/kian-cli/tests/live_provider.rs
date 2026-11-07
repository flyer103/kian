//! Live-provider e2e — env-gated, ignored by default.
//!
//! built-in asserts byte-stability as the controllable proxy; the live
//! run is a *manual* pass against a real openai-compat endpoint, not
//! CI. Run it once with:
//!
//! ```text
//! KIAN_API_KEY=sk-... \
//! KIAN_BASE_URL=https://api.openai.com/v1 \ # optional
//! KIAN_MODEL=gpt-4o-mini \ # optional
//! cargo test -p kian-cli --test live_provider -- --ignored --nocapture
//! ```
//!
//! The run is recorded in README.md ("Live-provider run log"). The
//! test asserts the same honest-completion and zero-unexpected-
//! invalidation invariants as the mock e2e; provider-side cache-hit
//! reporting is out of scope (plan risk #5).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

const KEY_ENV: &str = "KIAN_API_KEY";
const BASE_URL_ENV: &str = "KIAN_BASE_URL";
const MODEL_ENV: &str = "KIAN_MODEL";

fn force_no_proxy() {
    unsafe {
        std::env::set_var("NO_PROXY", "127.0.0.1,localhost");
        std::env::set_var("no_proxy", "127.0.0.1,localhost");
    }
}

async fn rpc(client: &reqwest::Client, base: &str, method: &str, params: Value) -> Value {
    let resp = client
        .post(format!("{base}/rpc"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .expect("rpc call");
    let body: Value = resp.json().await.expect("rpc json");
    assert!(body.get("error").is_none(), "rpc {method} failed: {body}");
    body["result"].clone()
}

/// One real session against a live openai-compat endpoint: completes
/// honestly and reports zero unexpected prefix invalidations.
#[tokio::test]
#[ignore = "env-gated: set KIAN_API_KEY (and optionally KIAN_BASE_URL / KIAN_MODEL) and run with --ignored"]
async fn live_provider_session_completes_with_stable_prefix() {
    let Ok(key) = std::env::var(KEY_ENV) else {
        eprintln!("skipping: {KEY_ENV} not set (this test is a manual, recorded run)");
        return;
    };
    force_no_proxy();
    let base_url =
        std::env::var(BASE_URL_ENV).unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
    let model = std::env::var(MODEL_ENV).unwrap_or_else(|_| "gpt-4o-mini".to_string());

    unsafe { std::env::set_var("KIAN_LIVE_KEY", &key) };

    let dir = std::env::temp_dir().join(format!("kian-c17-live-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let sqlite = dir.join("live.db");
    let sqlite_path = sqlite.display().to_string();
    let yaml = format!(
        r#"listen: "127.0.0.1:0"
storage:
  sqlite: "{sqlite_path}"
agent:
  id: live-agent
  model_slots:
    main:
      provider: live
      model: "{model}"
  tools: []
providers:
  live:
    base_url: "{base_url}"
    api_key_env: KIAN_LIVE_KEY
drain:
  max_iterations: 8
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#
    );
    let config_path: PathBuf = dir.join("kian.yaml");
    std::fs::write(&config_path, yaml).expect("write kian.yaml");

    let config = kian_cli::config::ServeConfig::load(&config_path).expect("config loads");
    let server = Arc::new(
        kian_cli::server::Server::from_config(config)
            .await
            .expect("server"),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, server.router()).await.expect("serve");
    });

    let client = reqwest::Client::new();
    let session = rpc(&client, &base, "create_session", serde_json::json!({})).await["session"]
        .as_str()
        .expect("session uuid")
        .to_string();
    rpc(
        &client,
        &base,
        "submit",
        serde_json::json!({"session": session, "content": "Reply with the single word: ok"}),
    )
    .await;

    // Live providers can be slow; allow two minutes.
    let mut state = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while tokio::time::Instant::now() < deadline {
        let p = rpc(
            &client,
            &base,
            "projection",
            serde_json::json!({"session": session}),
        )
        .await;
        state = p["state"].as_str().unwrap_or_default().to_string();
        if state == "completed" || state == "failed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    println!("live run: session {session} -> {state}");
    assert_eq!(state, "completed", "session must complete honestly");

    // proxy against the real request bytes: whatever consecutive
    // requests happened, none may break the prefix without a compact.
    let out = kian_cli::cache_metrics::run_cache_stats(&config_path, None, true)
        .await
        .expect("cache stats");
    let report: Value = serde_json::from_str(&out).expect("json");
    let causes = report["invalidation_causes"].as_object().expect("causes");
    assert!(
        !causes.contains_key("unexpected_prefix_change"),
        "live run broke the byte prefix without a compact: {report}"
    );
    println!("live run cache report: {report}");

    std::fs::remove_dir_all(&dir).ok();
}
