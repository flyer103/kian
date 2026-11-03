//! kian-77: remote-exec redaction acceptance — a command whose output
//! carries a configured secret must persist **no** secret verbatim, not
//! in the caller's result, not in the durable queue row (the ADR-007
//! replay source), matching the local exec path's guarantees.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kian_core::seams::ExecCommand;
use kian_core::secrets::SecretRedactor;
use kian_endpoint::Endpoint;
use kian_exec::proto::CommandId;
use kian_exec::store::ExecStore;
use kian_exec::{
    BackoffConfig, ConnectionConfig, ConnectionManager, RemoteExecBackend, RemoteExecConfig,
};
use tokio::net::TcpListener;
use uuid::Uuid;

fn temp_db() -> PathBuf {
    std::env::temp_dir().join(format!("kian-exec-redact-{}.sqlite", Uuid::new_v4()))
}

fn fast_backoff() -> BackoffConfig {
    BackoffConfig {
        initial: Duration::from_millis(20),
        max: Duration::from_millis(200),
        multiplier: 2.0,
    }
}

/// The G4 leak, replayed through the real remote path: the endpoint's
/// child inherits the server environment (the endpoint daemon and the
/// test share one process here), prints the secret, and both the
/// caller's result AND the durable queue row must come back redacted.
#[tokio::test]
async fn remote_exec_output_and_durable_row_carry_no_secret() {
    const NAME: &str = "KIAN_EXEC_TEST_SECRET";
    const VALUE: &str = "kian-exec-redact-secret-value-1";
    // SAFETY: unique test-only name, constant value.
    unsafe {
        std::env::set_var(NAME, VALUE);
    }
    let redactor = SecretRedactor::from_env_names(&[NAME.to_string()]);

    let db = temp_db();
    let store = ExecStore::open(&db).await.unwrap();
    let endpoint = Arc::new(Endpoint::new(
        "dev-box",
        Arc::new(kian_endpoint::LocalExecutor),
    ));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(endpoint.serve(listener));
    let manager = Arc::new(ConnectionManager::new(
        ConnectionConfig::new(addr).with_backoff(fast_backoff()),
    ));
    manager.start();
    let backend = RemoteExecBackend::with_row_leases_redacting(
        store.clone(),
        vec![("dev-box".into(), manager.clone())],
        "dev-box",
        RemoteExecConfig {
            queue_timeout: Duration::from_secs(8),
            poll_interval: Duration::from_millis(40),
        },
        "redact-test",
        Duration::from_secs(8),
        redactor,
    )
    .unwrap();

    let command_id = CommandId::new_v5(
        &uuid::Uuid::NAMESPACE_URL,
        format!("kian:redact-test:{VALUE}").as_bytes(),
    );
    let cmd = ExecCommand {
        argv: vec!["bash".into(), "-c".into(), format!("echo {NAME}=${NAME}")],
        env: Default::default(),
        cwd: None,
        timeout_ms: Some(5_000),
    };

    // First execution: live dispatch; the caller's result is redacted.
    let result = backend
        .exec_tracked(command_id, "dev-box", cmd.clone(), None)
        .await
        .unwrap();
    assert_eq!(result.exit_code, Some(0));
    assert!(
        !result.stdout.contains(VALUE),
        "caller result must be redacted: {:?}",
        result.stdout
    );
    assert!(result.stdout.contains(&format!("{NAME}=[REDACTED:{NAME}]")));

    // Second execution, same command id: answered from the durable row
    // (ADR-007 dedupe) — the row itself must be redacted too.
    let replay = backend
        .exec_tracked(command_id, "dev-box", cmd, None)
        .await
        .unwrap();
    assert!(
        !replay.stdout.contains(VALUE),
        "the durable queue row must be redacted: {:?}",
        replay.stdout
    );
    assert!(replay.stdout.contains(&format!("{NAME}=[REDACTED:{NAME}]")));

    backend.shutdown().await;
    drop(store);
    let _ = std::fs::remove_file(&db);
}
