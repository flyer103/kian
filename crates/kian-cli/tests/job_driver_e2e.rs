use kian_cli::config::ServeConfig;
use kian_cli::server::Server;

fn config(root: &std::path::Path) -> ServeConfig {
    let sqlite = root.join("events.sqlite");
    let yaml = format!(
        r#"
listen: "127.0.0.1:0"
storage:
  sqlite: "{}"
agent:
  id: default
  model_slots:
    main: {{provider: mock, model: mock-model}}
  tools: []
providers:
  mock:
    base_url: "http://127.0.0.1:1"
    api_key_env: KIAN_JOB_DRIVER_KEY
recovery:
  budget:
    tokens: 16
    wall_clock_ms: 300000
    retry_cost_tokens: 1
  rules:
    - class: unknown
      action: fail
plugins: ["openai-compat", "local-tools", "superagent"]
agents:
  worker:
    id: worker
    version: 1
    model_slots:
      main: {{provider: mock, model: mock-model}}
    tools: []
superagent:
  worker:
    agent: worker
  jobs:
    jobs:
      - id: digest
        plan:
          - id: gather
            prompt: gather
            verify:
              - check: final_message_present
"#,
        sqlite.display()
    );
    ServeConfig::parse(&yaml).expect("job-only config parses")
}

#[tokio::test]
async fn a_job_only_server_assembles_without_a_router_resident() {
    unsafe { std::env::set_var("KIAN_JOB_DRIVER_KEY", "test-key") };
    let root = std::env::temp_dir().join(format!("kian-job-driver-e2e-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).expect("temp dir");
    let server = Server::from_config(config(&root))
        .await
        .expect("job-only server assembles");
    assert!(
        !server.startup_order().contains(&"router_bootstrapped"),
        "a Router-free deployment never bootstraps a Router"
    );
    assert!(server.superagent_runtime().is_none());
    let _ = std::fs::remove_dir_all(root);
}
