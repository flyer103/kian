//! The `imsg rpc` child and the bridge's backend seam.
//!
//! `imsg` (MIT, <https://github.com/openclaw/imsg>) is the only component
//! that touches macOS: it reads `chat.db` under Full Disk Access and
//! sends through Messages.app under Automation. The bridge supervises one
//! long-lived `imsg rpc` child (JSON-RPC 2.0 over stdio,
//! `protocol_version: 1`) and uses exactly four methods —
//! `status`, `messages.after`, `send` and (for the fake-`imsg` tests) the
//! framing itself. `watch.subscribe` is deliberately not used: polling
//! `messages.after` keeps the bridge a plain request/response client
//! (CHANNEL-IMESSAGE §2.5).
//!
//! A dead child is dropped (and reaped) and respawned on the next call;
//! a hung child is killed on a request timeout. Errors are mapped from
//! the JSON-RPC codes into the ABI's delivery dispositions — never
//! parsed from message text, apart from the documented AppleScript
//! `-1743`/`-1712` markers that `imsg` surfaces as message text.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::Mutex;

/// How long one RPC call may take before the child is killed and the
/// call reports [`RpcError::Timeout`].
pub const RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// A readiness snapshot (the subset the ABI's `/v1/health` needs).
#[derive(Debug, Clone, PartialEq)]
pub struct Status {
    pub version: String,
    pub protocol_version: u32,
    pub db_ready: bool,
    pub db_path: String,
    pub supported_methods: Vec<String>,
    /// The injected IMCore helper is running (private-API features
    /// available). Phase 1 never needs it; it is reported for diagnosis.
    pub private_api: bool,
}

/// One `messages.after` page (raw `imsg` message objects; the app
/// reduces/filters them).
#[derive(Debug, Clone, PartialEq)]
pub struct AfterPage {
    pub messages: Vec<Value>,
    pub next_rowid: u64,
    pub has_more: bool,
}

/// A typed RPC failure, pre-aligned with the ABI's error kinds.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RpcError {
    #[error("imsg rpc unavailable: {0}")]
    Unavailable(String),
    #[error("imsg rpc request timed out")]
    Timeout,
    /// A JSON-RPC error response; `disposition` is the delivery
    /// disposition the ABI maps (`not_started` / `may_have_completed` /
    /// `still_in_flight`), when the child supplies one.
    #[error("imsg rpc error {code}: {message}")]
    Rejected {
        code: i64,
        message: String,
        disposition: Option<String>,
        retry_safe: Option<bool>,
    },
    #[error("imsg rpc response malformed: {0}")]
    Malformed(String),
}

impl RpcError {
    /// The ABI error kind (`not_started`, `may_have_completed`,
    /// `still_in_flight`, `database_unreadable`, `bridge_unavailable`,
    /// `bridge_busy`, `bad_request`, `unauthorized`) and whether a retry
    /// is safe by construction.
    #[must_use]
    pub fn kind(&self) -> (String, bool) {
        match self {
            RpcError::Timeout => ("may_have_completed".into(), false),
            RpcError::Unavailable(_) => ("bridge_unavailable".into(), true),
            RpcError::Malformed(_) => ("may_have_completed".into(), false),
            RpcError::Rejected {
                code,
                message,
                disposition,
                retry_safe,
            } => {
                if let Some(disposition) = disposition {
                    return (disposition.clone(), retry_safe.unwrap_or(false));
                }
                match code {
                    -32001 => ("may_have_completed".into(), false),
                    -32002 => ("database_unreadable".into(), true),
                    -32003 => ("bridge_unavailable".into(), true),
                    -32004 => ("still_in_flight".into(), false),
                    -32000 => ("bridge_busy".into(), true),
                    -32602 => ("bad_request".into(), true),
                    -32603 => {
                        // AppleScript permission failures happen before
                        // dispatch; an Apple event timeout may be
                        // post-dispatch (uncertain by definition).
                        if message.contains("-1743")
                            || message.to_ascii_lowercase().contains("not authorized")
                        {
                            // A permission refusal happens before dispatch.
                            ("not_started".into(), true)
                        } else {
                            // Including the `-1712` Apple event timeout:
                            // possibly post-dispatch, so uncertain.
                            ("may_have_completed".into(), false)
                        }
                    }
                    _ => ("may_have_completed".into(), false),
                }
            }
        }
    }
}

/// The bridge's macOS seam. `ImsgRpc` is the production implementation;
/// the app tests substitute a fake.
pub trait Backend: Send + Sync + 'static {
    fn status(&self) -> impl std::future::Future<Output = Result<Status, RpcError>> + Send;
    fn after(
        &self,
        since_rowid: u64,
        limit: u32,
    ) -> impl std::future::Future<Output = Result<AfterPage, RpcError>> + Send;
    fn send(
        &self,
        to: Option<String>,
        chat_guid: Option<String>,
        text: String,
        service: String,
    ) -> impl std::future::Future<Output = Result<Value, RpcError>> + Send;
}

/// The production backend: one supervised `imsg rpc` child, calls
/// serialized (the bridge is a single-consumer ABI).
pub struct ImsgRpc {
    imsg: PathBuf,
    db: PathBuf,
    child: Mutex<Option<RpcChild>>,
    next_id: AtomicU64,
    timeout: Duration,
}

struct RpcChild {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl ImsgRpc {
    #[must_use]
    pub fn new(imsg: impl Into<PathBuf>, db: impl Into<PathBuf>) -> Self {
        Self {
            imsg: imsg.into(),
            db: db.into(),
            child: Mutex::new(None),
            next_id: AtomicU64::new(1),
            timeout: RPC_TIMEOUT,
        }
    }

    /// Overrides the RPC deadline (tests only).
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The configured `imsg` path (diagnostics; never a token).
    #[must_use]
    pub fn imsg_path(&self) -> &Path {
        &self.imsg
    }

    async fn spawn(&self) -> Result<RpcChild, RpcError> {
        let mut command = tokio::process::Command::new(&self.imsg);
        command
            .arg("rpc")
            .arg("--db")
            .arg(&self.db)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|e| {
            RpcError::Unavailable(format!(
                "cannot spawn `{} rpc`: {e} (install imsg with `brew install steipete/tap/imsg`)",
                self.imsg.display()
            ))
        })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| RpcError::Unavailable("imsg rpc stdin unavailable".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| RpcError::Unavailable("imsg rpc stdout unavailable".into()))?;
        Ok(RpcChild {
            child,
            stdin,
            stdout: BufReader::new(stdout),
        })
    }

    /// One JSON-RPC call: write the request, read lines until the
    /// matching `id` arrives (notification lines are ignored — this
    /// bridge never subscribes).
    async fn request(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let mut guard = self.child.lock().await;
        if guard.is_none() {
            *guard = Some(self.spawn().await?);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let line = format!(
            "{}\n",
            json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
        );
        let Some(child) = guard.as_mut() else {
            return Err(RpcError::Unavailable("imsg rpc child missing".into()));
        };
        let written = async {
            child.stdin.write_all(line.as_bytes()).await?;
            child.stdin.flush().await
        }
        .await;
        if let Err(e) = written {
            *guard = None;
            return Err(RpcError::Unavailable(format!(
                "cannot write to imsg rpc: {e}"
            )));
        }
        loop {
            let mut buffer = String::new();
            let read =
                tokio::time::timeout(self.timeout, child.stdout.read_line(&mut buffer)).await;
            match read {
                Err(_) => {
                    // A hung child would poison the mutex forever: kill
                    // and drop it, keep the honest uncertain outcome.
                    if let Some(mut dead) = guard.take() {
                        let _ = dead.child.start_kill();
                    }
                    return Err(RpcError::Timeout);
                }
                Ok(Err(e)) => {
                    *guard = None;
                    return Err(RpcError::Unavailable(format!(
                        "cannot read from imsg rpc: {e}"
                    )));
                }
                Ok(Ok(0)) => {
                    *guard = None;
                    return Err(RpcError::Unavailable("imsg rpc closed its stdout".into()));
                }
                Ok(Ok(_)) => {}
            }
            let trimmed = buffer.trim();
            if trimmed.is_empty() {
                continue;
            }
            let value: Value = match serde_json::from_str(trimmed) {
                Ok(value) => value,
                Err(e) => return Err(RpcError::Malformed(format!("non-JSON line: {e}"))),
            };
            if value.get("id").and_then(Value::as_u64) != Some(id) {
                continue; // a notification or an out-of-band line
            }
            if let Some(error) = value.get("error") {
                return Err(parse_rpc_error(error));
            }
            return Ok(value.get("result").cloned().unwrap_or(Value::Null));
        }
    }
}

fn parse_rpc_error(error: &Value) -> RpcError {
    let code = error.get("code").and_then(Value::as_i64).unwrap_or(0);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let data = error.get("data");
    let disposition = data
        .and_then(|data| data.get("disposition"))
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            data.and_then(Value::as_str)
                .and_then(|text| text.strip_prefix("disposition: "))
                .map(str::to_string)
        });
    let retry_safe = data
        .and_then(|data| data.get("retry_safe"))
        .and_then(Value::as_bool);
    let detail = data
        .and_then(|data| data.get("detail"))
        .and_then(Value::as_str)
        .map(str::to_string);
    RpcError::Rejected {
        code,
        message: detail.unwrap_or(message),
        disposition,
        retry_safe,
    }
}

impl Backend for ImsgRpc {
    async fn status(&self) -> Result<Status, RpcError> {
        let result = self.request("status", json!({})).await?;
        let version = result
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let protocol_version = result
            .get("protocol_version")
            .and_then(Value::as_u64)
            .unwrap_or(0) as u32;
        let database = result.get("database");
        let db_ready = database
            .and_then(|db| db.get("ready"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let db_path = database
            .and_then(|db| db.get("path"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let supported_methods = result
            .get("supported_methods")
            .and_then(Value::as_array)
            .map(|methods| {
                methods
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        if protocol_version != 1 {
            return Err(RpcError::Malformed(format!(
                "imsg rpc protocol_version {protocol_version} (this bridge speaks 1)"
            )));
        }
        let private_api = result
            .get("bridge")
            .and_then(|bridge| bridge.get("ready"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        Ok(Status {
            version,
            protocol_version,
            db_ready,
            db_path,
            supported_methods,
            private_api,
        })
    }

    async fn after(&self, since_rowid: u64, limit: u32) -> Result<AfterPage, RpcError> {
        let result = self
            .request(
                "messages.after",
                json!({
                    "since_rowid": since_rowid,
                    "limit": limit,
                    "attachments": false,
                    "include_reactions": false,
                }),
            )
            .await?;
        let messages = result
            .get("messages")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let next_rowid = result
            .get("next_rowid")
            .and_then(Value::as_u64)
            .unwrap_or(since_rowid);
        let has_more = result
            .get("has_more")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        Ok(AfterPage {
            messages,
            next_rowid,
            has_more,
        })
    }

    async fn send(
        &self,
        to: Option<String>,
        chat_guid: Option<String>,
        text: String,
        service: String,
    ) -> Result<Value, RpcError> {
        let mut params = serde_json::Map::new();
        params.insert("text".into(), Value::String(text));
        params.insert("service".into(), Value::String(service));
        // A chat target wins (thread/service continuity); `to` stays the
        // allowlist key the adapter validated.
        if let Some(chat_guid) = chat_guid.filter(|guid| !guid.trim().is_empty()) {
            params.insert("chat_guid".into(), Value::String(chat_guid));
        } else if let Some(to) = to.filter(|handle| !handle.trim().is_empty()) {
            params.insert("to".into(), Value::String(to));
        } else {
            return Err(RpcError::Rejected {
                code: -32602,
                message: "send requires `to` or `chat_guid`".into(),
                disposition: Some("bad_request".into()),
                retry_safe: Some(true),
            });
        }
        self.request("send", Value::Object(params)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rpc_error_codes_map_to_abi_kinds() {
        let rejected = |code: i64, message: &str, data: Value| RpcError::Rejected {
            code,
            message: message.into(),
            disposition: data
                .get("disposition")
                .and_then(Value::as_str)
                .map(str::to_string),
            retry_safe: data.get("retry_safe").and_then(Value::as_bool),
        };
        assert_eq!(
            rejected(-32001, "", json!({})).kind(),
            ("may_have_completed".into(), false)
        );
        assert_eq!(
            rejected(-32004, "", json!({})).kind(),
            ("still_in_flight".into(), false)
        );
        assert_eq!(
            rejected(-32002, "", json!({})).kind(),
            ("database_unreadable".into(), true)
        );
        assert_eq!(
            rejected(-32000, "", json!({})).kind(),
            ("bridge_busy".into(), true)
        );
        assert_eq!(
            rejected(-32602, "", json!({})).kind(),
            ("bad_request".into(), true)
        );
        assert_eq!(
            rejected(
                -32603,
                "Not authorized to send Apple events to Messages. (-1743)",
                json!({})
            )
            .kind(),
            ("not_started".into(), true)
        );
        assert_eq!(
            rejected(-32603, "Apple event timed out. (-1712)", json!({})).kind(),
            ("may_have_completed".into(), false)
        );
        // The child's own disposition wins over the code default.
        assert_eq!(
            rejected(
                -32603,
                "",
                json!({"disposition": "not_started", "retry_safe": true})
            )
            .kind(),
            ("not_started".into(), true)
        );
        assert_eq!(
            RpcError::Timeout.kind(),
            ("may_have_completed".into(), false)
        );
        assert_eq!(
            RpcError::Unavailable("x".into()).kind(),
            ("bridge_unavailable".into(), true)
        );
    }

    #[test]
    fn parse_rpc_error_reads_the_data_object() {
        let error = json!({
            "code": -32001,
            "message": "Send may have completed",
            "data": {"retry_safe": false, "disposition": "may_have_completed", "detail": "ambiguous receipt"}
        });
        let parsed = parse_rpc_error(&error);
        match parsed {
            RpcError::Rejected {
                code,
                message,
                disposition,
                retry_safe,
            } => {
                assert_eq!(code, -32001);
                assert_eq!(message, "ambiguous receipt");
                assert_eq!(disposition.as_deref(), Some("may_have_completed"));
                assert_eq!(retry_safe, Some(false));
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }
}
