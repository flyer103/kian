//! `RemoteExecTool` acceptance tests (card C20): the model-visible remote
//! exec tool — same schema shape as the local built-in, tool-call-id
//! correlation, honest errors, command-id dedupe.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use kian_core::seams::{ExecBackend, ExecCommand, ExecError, ExecResult, ToolCall, ToolProvider};
use kian_endpoint::Endpoint;
use kian_exec::proto::CommandId;
use kian_exec::store::ExecStore;
use kian_exec::{
    BackoffConfig, ConnectionConfig, ConnectionManager, RemoteExecBackend, RemoteExecConfig,
    RemoteExecTool,
};
use tokio::net::TcpListener;
use uuid::Uuid;

fn no_proxy() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        // SAFETY: written once, before any socket is opened.
        unsafe { std::env::set_var("NO_PROXY", "127.0.0.1,localhost") };
    });
}

fn temp_db() -> PathBuf {
    std::env::temp_dir().join(format!("kian-exec-tool-{}.sqlite", Uuid::new_v4()))
}

fn fast_backoff() -> BackoffConfig {
    BackoffConfig {
        initial: Duration::from_millis(20),
        max: Duration::from_millis(200),
        multiplier: 2.0,
    }
}

fn call(id: &str, arguments: serde_json::Value) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: "exec".into(),
        arguments,
    }
}

#[derive(Clone)]
struct CountingLocal {
    count: Arc<AtomicUsize>,
}

impl ExecBackend for CountingLocal {
    fn exec(&self, _cmd: ExecCommand) -> BoxFuture<'static, Result<ExecResult, ExecError>> {
        let count = self.count.clone();
        count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            Ok(ExecResult {
                exit_code: Some(0),
                stdout: "tool-ran".into(),
                stderr: String::new(),
                timed_out: false,
                redactions: Vec::new(),
            })
        })
    }
}

async fn setup() -> (
    Arc<ConnectionManager>,
    Arc<RemoteExecBackend>,
    Arc<RemoteExecTool>,
    ExecStore,
    PathBuf,
) {
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
    let backend = Arc::new(
        RemoteExecBackend::new(
            store.clone(),
            vec![("dev-box".into(), manager.clone())],
            "dev-box",
            RemoteExecConfig {
                queue_timeout: Duration::from_secs(8),
                poll_interval: Duration::from_millis(40),
            },
        )
        .unwrap(),
    );
    let tool = Arc::new(RemoteExecTool::new(backend.clone(), "dev-box"));
    (manager, backend, tool, store, db)
}

/// The tool exposes exactly the built-in `exec` schema (name + parameter
/// shape), so the model-visible interface is uniform local vs remote.
#[tokio::test]
async fn remote_tool_schema_matches_builtin_exec_shape() {
    let (_manager, _backend, tool, _store, _db) = setup().await;
    let schemas = tool.schemas();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, "exec");
    assert_eq!(
        schemas[0].parameters,
        serde_json::json!({
            "type": "object",
            "properties": {
                "argv": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "command and arguments"
                },
                "cwd": {"type": "string"},
                "timeout_ms": {"type": "integer"}
            },
            "required": ["argv"]
        })
    );
}

/// Happy path over real TCP: invoke resolves with the tool-call id echoed
/// back and the real endpoint output, and the queue row is durably done
/// with the tool-call id as its audit reference.
#[tokio::test]
async fn remote_tool_invoke_round_trips_over_tcp() {
    no_proxy();
    let (manager, backend, tool, store, db) = setup().await;
    let result = tool
        .invoke(call(
            "call-42",
            serde_json::json!({"argv": ["echo", "from-tool"]}),
        ))
        .await
        .unwrap();
    assert_eq!(result.call_id, "call-42", "loop correlation id is echoed");
    assert!(!result.is_error);
    assert_eq!(
        result.output["stdout"].as_str().unwrap().trim(),
        "from-tool"
    );
    assert_eq!(result.output["exit_code"], 0);

    let command_id = kian_exec::tool::command_id_for_tool_call("call-42");
    let row = store.get(command_id).await.unwrap().expect("row durable");
    assert_eq!(row.status, kian_exec::QueueStatus::Done);
    assert_eq!(row.tool_call_ref.as_deref(), Some("call-42"));

    // Invalid calls are rejected before any row exists.
    let bad = tool
        .invoke(call("call-bad", serde_json::json!({"argv": []})))
        .await
        .unwrap_err();
    assert!(bad.to_string().contains("argv"));
    assert!(
        store
            .get(kian_exec::tool::command_id_for_tool_call("call-bad"))
            .await
            .unwrap()
            .is_none()
    );

    backend.shutdown().await;
    manager.shutdown().await;
    store.close().await;
    let _ = std::fs::remove_file(&db);
}

/// Execution failure on the endpoint flows back as an honest `is_error`
/// tool result — never a fake success (constraint 7).
#[tokio::test]
async fn remote_tool_error_is_honest() {
    no_proxy();
    let (manager, backend, tool, store, db) = setup().await;
    let result = tool
        .invoke(call(
            "call-fail",
            serde_json::json!({"argv": ["kian-definitely-missing-binary"]}),
        ))
        .await
        .unwrap();
    assert_eq!(result.call_id, "call-fail");
    assert!(result.is_error, "spawn failure must be an error result");
    assert!(result.output["error"].as_str().unwrap().contains("spawn"));
    let command_id: CommandId = kian_exec::tool::command_id_for_tool_call("call-fail");
    let row = store.get(command_id).await.unwrap().expect("row durable");
    assert_eq!(row.status, kian_exec::QueueStatus::Done);
    assert!(
        row.outcome.is_some(),
        "failure recorded honestly, not dropped"
    );

    backend.shutdown().await;
    manager.shutdown().await;
    store.close().await;
    let _ = std::fs::remove_file(&db);
}

/// Re-invoking the same tool-call id (session recovery / re-execution)
/// hits the same durable row: one dispatch on the endpoint, both calls
/// answered.
#[tokio::test]
async fn remote_tool_duplicate_call_id_is_deduped_at_the_row() {
    no_proxy();
    let db = temp_db();
    let store = ExecStore::open(&db).await.unwrap();
    let counting = CountingLocal {
        count: Arc::new(AtomicUsize::new(0)),
    };
    let executed = counting.count.clone();
    let endpoint = Arc::new(Endpoint::new("dev-box", Arc::new(counting)));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(endpoint.serve(listener));
    let manager = Arc::new(ConnectionManager::new(
        ConnectionConfig::new(addr).with_backoff(fast_backoff()),
    ));
    manager.start();
    let backend = Arc::new(
        RemoteExecBackend::new(
            store.clone(),
            vec![("dev-box".into(), manager.clone())],
            "dev-box",
            RemoteExecConfig {
                queue_timeout: Duration::from_secs(8),
                poll_interval: Duration::from_millis(40),
            },
        )
        .unwrap(),
    );
    let tool = Arc::new(RemoteExecTool::new(backend.clone(), "dev-box"));

    let (first, second) = tokio::join!(
        tool.invoke(call("call-dup", serde_json::json!({"argv": ["work"]}))),
        tool.invoke(call("call-dup", serde_json::json!({"argv": ["work"]}))),
    );
    assert_eq!(first.unwrap().output["stdout"], "tool-ran");
    assert_eq!(second.unwrap().output["stdout"], "tool-ran");
    assert_eq!(executed.load(Ordering::SeqCst), 1, "one dispatch ever");

    // And a *later* replay (post-completion) answers from the row.
    let third = tool
        .invoke(call("call-dup", serde_json::json!({"argv": ["work"]})))
        .await
        .unwrap();
    assert_eq!(third.output["stdout"], "tool-ran");
    assert_eq!(executed.load(Ordering::SeqCst), 1, "still one dispatch");

    backend.shutdown().await;
    manager.shutdown().await;
    store.close().await;
    let _ = std::fs::remove_file(&db);
}
