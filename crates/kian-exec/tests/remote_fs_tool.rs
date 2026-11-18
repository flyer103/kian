//! `RemoteFsTool` acceptance tests (KiandProto v2, ADR-023): the three
//! worker filesystem tools over a real TCP endpoint, a real durable queue
//! and a real sqlite file. Same harness discipline as `remote_tool.rs`.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use kian_core::seams::{ToolCall, ToolProvider};
use kian_endpoint::Endpoint;
use kian_exec::store::ExecStore;
use kian_exec::{
    BackoffConfig, ConnectionConfig, ConnectionManager, RemoteExecBackend, RemoteExecConfig,
    RemoteFsTool,
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

fn temp_paths() -> (PathBuf, PathBuf) {
    let base = std::env::temp_dir().join(format!("kian-exec-fs-{}", Uuid::new_v4()));
    let root = base.join("workspace");
    std::fs::create_dir_all(&root).unwrap();
    (base.join("queue.sqlite"), root)
}

fn fast_backoff() -> BackoffConfig {
    BackoffConfig {
        initial: Duration::from_millis(20),
        max: Duration::from_millis(200),
        multiplier: 2.0,
    }
}

fn call(id: &str, name: &str, arguments: serde_json::Value) -> ToolCall {
    ToolCall {
        id: id.into(),
        name: name.into(),
        arguments,
    }
}

struct Harness {
    manager: Arc<ConnectionManager>,
    backend: Arc<RemoteExecBackend>,
    tool: Arc<RemoteFsTool>,
    store: ExecStore,
    db: PathBuf,
    root: PathBuf,
}

impl Harness {
    async fn shutdown(self) {
        self.backend.shutdown().await;
        self.manager.shutdown().await;
        self.store.close().await;
        let _ = std::fs::remove_file(&self.db);
        let _ = std::fs::remove_dir_all(self.root.parent().unwrap());
    }
}

async fn setup() -> Harness {
    let (db, root) = temp_paths();
    let store = ExecStore::open(&db).await.unwrap();
    let endpoint = Arc::new(
        Endpoint::new("dev-box", Arc::new(kian_endpoint::LocalExecutor))
            .with_workspace_root(root.clone()),
    );
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
                max_redrives: 1,
            },
        )
        .unwrap(),
    );
    let tool = Arc::new(RemoteFsTool::new(backend.clone(), "dev-box"));
    Harness {
        manager,
        backend,
        tool,
        store,
        db,
        root,
    }
}

/// The three names and their parameter shapes are the local built-ins'
/// (the cross-crate parity test lives in `kian-cli`).
#[tokio::test]
async fn remote_fs_tool_exposes_three_schemas() {
    let h = setup().await;
    let names: Vec<String> = h.tool.schemas().into_iter().map(|s| s.name).collect();
    assert_eq!(names, vec!["fs_list", "fs_read", "fs_write"]);
    h.shutdown().await;
}

/// write → read → list over real TCP, with the side effect only on the
/// endpoint's workspace root.
#[tokio::test]
async fn remote_fs_write_read_list_round_trip_over_tcp() {
    no_proxy();
    let h = setup().await;
    let write = h
        .tool
        .invoke(call(
            "call-w",
            "fs_write",
            serde_json::json!({"path": "notes/a.txt", "content": "hello worker"}),
        ))
        .await
        .unwrap();
    assert!(!write.is_error, "{:?}", write.output);
    assert_eq!(write.output, serde_json::json!({"written": true}));
    assert_eq!(
        std::fs::read_to_string(h.root.join("notes/a.txt")).unwrap(),
        "hello worker"
    );

    let read = h
        .tool
        .invoke(call(
            "call-r",
            "fs_read",
            serde_json::json!({"path": "notes/a.txt"}),
        ))
        .await
        .unwrap();
    assert!(!read.is_error);
    assert_eq!(read.output, serde_json::json!({"content": "hello worker"}));

    let list = h
        .tool
        .invoke(call(
            "call-l",
            "fs_list",
            serde_json::json!({"path": "notes"}),
        ))
        .await
        .unwrap();
    assert!(!list.is_error);
    assert_eq!(list.output, serde_json::json!({"entries": ["a.txt"]}));

    h.shutdown().await;
}

/// Upward paths are refused before any row exists; runtime failures are
/// honest `is_error` results (never fake successes).
#[tokio::test]
async fn remote_fs_tool_refuses_upward_and_reports_runtime_errors_honestly() {
    no_proxy();
    let h = setup().await;
    let bad = h
        .tool
        .invoke(call(
            "call-bad",
            "fs_read",
            serde_json::json!({"path": "../escape"}),
        ))
        .await
        .unwrap_err();
    assert!(bad.to_string().contains(".."));
    assert!(
        h.store
            .get(kian_exec::tool::command_id_for_tool_call("call-bad"))
            .await
            .unwrap()
            .is_none()
    );

    let missing = h
        .tool
        .invoke(call(
            "call-missing",
            "fs_read",
            serde_json::json!({"path": "nope.txt"}),
        ))
        .await
        .unwrap();
    assert!(missing.is_error);
    assert!(
        missing.output["error"]
            .as_str()
            .unwrap()
            .contains("nope.txt")
    );
    h.shutdown().await;
}
