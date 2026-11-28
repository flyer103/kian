//! (R12-07 / kian-133) Independent QA acceptance probes for the
//! `kian workers` operator verb — L1.
//!
//! QA-authored fixtures only, re-derived from brief §7 (`kian workers`
//! text/`--json` agreement, read-only) and the locked spec §7/D-L2 —
//! with emphasis on what the per-id liveness bound actually derives
//! from: a real config file carrying BOTH a static pin's
//! `offline_after_ms` and the `workers:` section's bound. The same
//! physical staleness must read `online` for the pin and `offline` for
//! the dynamic id (per-id bounds), in both output forms, sorted by id,
//! with the verb writing nothing.

use std::path::PathBuf;

use uuid::Uuid;

fn temp_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("kian-qa-workers-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A real, fully-valid config: a static pin with a 10-minute liveness
/// bound and a `workers:` listener with a 5-second bound. Same real
/// staleness (60 s) must therefore derive different states per id.
fn qa_config(dir: &std::path::Path, sqlite: &std::path::Path) -> PathBuf {
    let yaml = format!(
        r#"listen: "127.0.0.1:7340"
storage:
  sqlite: "{sqlite}"
agent:
  id: default
  model_slots:
    main:
      provider: router-mock
      model: mock-model
  tools: ["fs_read"]
agents:
  router:
    id: router
    version: 3
    model_slots:
      router:
        provider: router-mock
        model: mock-router
    tools: ["spawn_worker", "reply", "deliver_structured"]
  worker:
    id: worker
    version: 7
    model_slots:
      main:
        provider: worker-mock
        model: mock-worker
    tools: ["fs_read", "fs_write", "exec"]
superagent:
  router:
    session_id: "00000000-0000-0000-0000-000000000111"
    agent: router
    slot: router
  worker:
    agent: worker
  subscriptions:
    - channel: mock-im
      match: always
      route: router
  channels:
    mock-im:
      type: mock-im
endpoints:
  - id: slow-pin
    addr: "127.0.0.1:7401"
    description: "the QA static pin"
    queue_timeout_ms: 60000
    offline_after_ms: 600000
workers:
  listen: "127.0.0.1:7410"
  token_env: KIAN_QA_WORKER_TOKEN_UNUSED
  offline_after_ms: 5000
providers:
  router-mock:
    base_url: "http://127.0.0.1:1"
    api_key_env: KIAN_E2E_API_KEY
  worker-mock:
    base_url: "http://127.0.0.1:1"
    api_key_env: KIAN_E2E_API_KEY
plugins: ["openai-compat", "local-tools", "superagent", "remote-exec"]
drain:
  max_iterations: 8
  lease_ttl_ms: 30000
  heartbeat_interval_ms: 10000
tools:
  root: "{dir}"
  exec_timeout_ms: 60000
recovery:
  budget:
    tokens: 64
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
"#,
        sqlite = sqlite.display(),
        dir = dir.display(),
    );
    let path = dir.join("kian.yaml");
    std::fs::write(&path, yaml).expect("write qa config");
    path
}

async fn seeded(dir: &std::path::Path) -> PathBuf {
    let db = dir.join("kian.sqlite");
    let store = kian_exec::ExecStore::open(&db).await.unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    // Identical physical staleness (60 s): inside the pin's 600 s bound,
    // outside the dynamic 5 s bound.
    store
        .upsert_endpoint_with_declaration(
            "slow-pin",
            "127.0.0.1:7401",
            now - 60_000,
            "online",
            None,
            3,
            &["exec".to_string()],
        )
        .await
        .unwrap();
    store
        .upsert_endpoint_with_declaration(
            "zz-dyn",
            "127.0.0.1:55555",
            now - 60_000,
            "online",
            None,
            3,
            &["apple-calendar".to_string(), "apple-health".to_string()],
        )
        .await
        .unwrap();
    store
        .upsert_endpoint("aa-dyn", "127.0.0.1:55556", now, "online")
        .await
        .unwrap();
    store.close().await;
    // The config carries a superagent section, so the verb also opens
    // the event store for the bound-session projection — initialize it.
    let events = kian_store::EventStore::open(&db).await.unwrap();
    events.close().await;
    db
}

fn text_facts(text: &str) -> Vec<(String, String)> {
    text.lines()
        .skip(1)
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut parts = line.split_whitespace();
            (
                parts.next().expect("id").to_string(),
                parts.next().expect("state").to_string(),
            )
        })
        .collect()
}

/// Per-id liveness bounds from a real config; text/JSON agreement;
/// id-ascending order; description/version absent by construction.
#[tokio::test]
async fn qa_workers_per_id_bounds_and_form_agreement() {
    let dir = temp_dir();
    let db = seeded(&dir).await;
    let config = qa_config(&dir, &db);

    let json_text = kian_cli::commands::run_workers(&config, Some(&db), true)
        .await
        .expect("json never refuses");
    let plain_text = kian_cli::commands::run_workers(&config, Some(&db), false)
        .await
        .expect("text never refuses");

    let report: serde_json::Value = serde_json::from_str(&json_text).unwrap();
    let rows = report["workers"].as_array().expect("workers array");
    let json_facts: Vec<(String, String)> = rows
        .iter()
        .map(|row| {
            (
                row["id"].as_str().unwrap().to_string(),
                row["state"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    // The durable last-declared capability cache reaches both output
    // forms; an id without a declaration omits the field entirely.
    let caps: Vec<(String, Vec<String>)> = rows
        .iter()
        .filter_map(|row| {
            row.get("capabilities").map(|caps| {
                (
                    row["id"].as_str().unwrap().to_string(),
                    caps.as_array()
                        .unwrap()
                        .iter()
                        .map(|c| c.as_str().unwrap().to_string())
                        .collect::<Vec<_>>(),
                )
            })
        })
        .collect();
    assert_eq!(
        caps,
        vec![
            ("slow-pin".to_string(), vec!["exec".to_string()]),
            (
                "zz-dyn".to_string(),
                vec!["apple-calendar".to_string(), "apple-health".to_string()]
            ),
        ]
    );
    assert!(!plain_text.is_empty());
    assert!(
        plain_text.contains("apple-calendar"),
        "text board carries capabilities: {plain_text}"
    );

    // Sorted by id ascending. The board is a registry projection, so a
    // cached heartbeat can never make a worker online (WORKER-MODEL W1):
    // every row is offline whatever the per-id liveness bound records.
    assert_eq!(
        json_facts,
        vec![
            ("aa-dyn".into(), "offline".into()),
            ("slow-pin".into(), "offline".into()),
            ("zz-dyn".into(), "offline".into()),
        ],
        "a registry row is offline until the live directory says otherwise"
    );
    for row in rows {
        assert!(row.get("description").is_none(), "D-L2: absent");
        // A declaration (needed to seed capabilities) also caches the
        // protocol version; the undeclared aa-dyn row keeps M4's absent
        // shape.
        assert_eq!(
            row.get("protocol_version").is_some(),
            row.get("capabilities").is_some(),
            "declaration fields travel together"
        );
    }
    // The text board carries exactly the same facts in the same order.
    assert_eq!(text_facts(&plain_text), json_facts);

    // Read-only: the registry rows are identical after both forms.
    let before = {
        let exec = kian_exec::ExecStore::open_read_only(&db).await.unwrap();
        let rows = exec.endpoints().await.unwrap();
        exec.close().await;
        rows
    };
    let _ = kian_cli::commands::run_workers(&config, Some(&db), true).await;
    let _ = kian_cli::commands::run_workers(&config, Some(&db), false).await;
    let after = {
        let exec = kian_exec::ExecStore::open_read_only(&db).await.unwrap();
        let rows = exec.endpoints().await.unwrap();
        exec.close().await;
        rows
    };
    assert_eq!(before, after, "the verb writes nothing");
    let _ = std::fs::remove_dir_all(&dir);
}
