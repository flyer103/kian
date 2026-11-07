//! `RemoteExecTool` — remote exec as a model-visible tool (card C20).
//!
//! Implements [`ToolProvider`] with the **same `exec` tool schema as the
//! local built-in** (kian-loop C11), so a worker's model sees one uniform
//! exec interface whether it runs locally or on a `kiand` endpoint. The
//! difference is entirely in routing: `invoke` derives the durable command
//! id deterministically from the calling tool-call id and goes through
//! [`crate::RemoteExecBackend::exec_tracked`], so session recovery +
//! re-execution dedupes at the queue-row level (ADR-007).

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use uuid::Uuid;

use kian_core::seams::{ExecCommand, ToolCall, ToolError, ToolProvider, ToolResult, ToolSchema};

use crate::RemoteExecBackend;

/// Derives the durable command id from a tool-call id: deterministic, so a
/// replayed tool call (session recovery, re-execution) maps onto the same
/// queue row — one row, one dispatch (ADR-007 command-id correlation).
pub fn command_id_for_tool_call(call_id: &str) -> Uuid {
    Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!("kian:exec:{call_id}").as_bytes(),
    )
}

/// The remote `exec` tool, routed through a configured endpoint. The tool
/// schema is byte-identical in shape to the local built-in `exec` tool.
#[derive(Clone)]
pub struct RemoteExecTool {
    backend: Arc<RemoteExecBackend>,
    endpoint_id: String,
}

impl RemoteExecTool {
    pub fn new(backend: Arc<RemoteExecBackend>, endpoint_id: impl Into<String>) -> Self {
        Self {
            backend,
            endpoint_id: endpoint_id.into(),
        }
    }

    /// The endpoint this tool routes through.
    pub fn endpoint_id(&self) -> &str {
        &self.endpoint_id
    }
}

impl ToolProvider for RemoteExecTool {
    fn schemas(&self) -> Vec<ToolSchema> {
        vec![ToolSchema {
            name: "exec".into(),
            description: "Run a command on the remote execution endpoint (argv vector, no shell)."
                .into(),
            parameters: serde_json::json!({
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
            }),
        }]
    }

    fn invoke(&self, call: ToolCall) -> BoxFuture<'static, Result<ToolResult, ToolError>> {
        let parsed = parse_exec(&call.arguments);
        let call_id = call.id;
        let backend = self.backend.clone();
        let endpoint_id = self.endpoint_id.clone();
        Box::pin(async move {
            let cmd = parsed?;
            // The derived id dedupes at the row level; the *original*
            // tool-call id stays the loop's correlation key (and is kept
            // on the row as `tool_call_ref` for audit).
            let command_id = command_id_for_tool_call(&call_id);
            let tool_call_ref = call_id.clone();
            // Result shape mirrors the local built-in exec tool exactly:
            // execution failures are honest `is_error` outputs, never fake
            // successes (constraint 7). C69: the redaction provenance
            // report from the settlement pass rides the ToolResult; the
            // `output` keeps the four legacy fields only.
            let (value, is_error, redactions) = match backend
                .exec_tracked(command_id, &endpoint_id, cmd, Some(tool_call_ref))
                .await
            {
                Ok(r) => (
                    serde_json::json!({
                        "exit_code": r.exit_code,
                        "stdout": r.stdout,
                        "stderr": r.stderr,
                        "timed_out": r.timed_out,
                    }),
                    r.exit_code != Some(0),
                    r.redactions,
                ),
                Err(e) => (
                    serde_json::json!({"error": e.to_string()}),
                    true,
                    Vec::new(),
                ),
            };
            Ok(ToolResult {
                call_id,
                output: value,
                is_error,
                redactions,
            })
        })
    }
}

/// Same argument contract as the local built-in `exec` tool (kian-loop
/// C11): argv vector, optional cwd/timeout_ms, env empty. Shared by
/// the sync `RemoteExecTool` and the async `start` tool (C37) — one
/// uniform argument surface.
pub(crate) fn parse_exec(args: &serde_json::Value) -> Result<ExecCommand, ToolError> {
    let argv = args
        .get("argv")
        .and_then(serde_json::Value::as_array)
        .filter(|a| !a.is_empty())
        .and_then(|a| {
            a.iter()
                .map(|v| v.as_str().map(str::to_owned))
                .collect::<Option<Vec<_>>>()
        })
        .ok_or_else(|| ToolError::InvalidCall("'argv' must be a non-empty string array".into()))?;
    Ok(ExecCommand {
        argv,
        env: BTreeMap::new(),
        cwd: args
            .get("cwd")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        timeout_ms: args.get("timeout_ms").and_then(serde_json::Value::as_u64),
    })
}

#[cfg(test)]
mod tests {
    use super::command_id_for_tool_call;

    /// Command ids are a pure function of the tool-call id — the dedupe
    /// contract session recovery relies on.
    #[test]
    fn command_id_is_deterministic_and_distinct_per_call() {
        let a = command_id_for_tool_call("call-1");
        assert_eq!(a, command_id_for_tool_call("call-1"));
        assert_ne!(a, command_id_for_tool_call("call-2"));
        assert_ne!(a, uuid::Uuid::nil());
    }
}
