//! The Codex `app-server` mapping (HARNESS-BRIDGE §5.2).
//!
//! Pure line-in / event-out logic plus the JSON-RPC frame builders. Protocol facts are pinned to
//! the vendored app-server README (thread/start, turn/start, turn/steer, turn/interrupt,
//! `item/*` notifications, `item/commandExecution/requestApproval` /
//! `item/fileChange/requestApproval`, decision `accept`/`decline`). Unverified against a live
//! binary until the env-gated smoke runs; an unknown method is drift, never a guess.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use kian_core::events::EventKind;
use kian_harness::event::{
    ApprovalAnswer, ApprovalConfirmation, ApprovalRequest, ContractError, Mapped, SessionEvent,
};
use serde_json::{Value, json};

/// The adapter's spawn recipe.
#[derive(Debug, Clone)]
pub struct CodexConfig {
    /// The `codex` binary (or a fixture peer).
    pub command: String,
    /// Extra argv appended after `app-server`.
    pub args: Vec<String>,
    /// The model override passed to `thread/start` (the harness owns the call).
    pub model: Option<String>,
    /// The working directory for the thread.
    pub cwd: Option<String>,
    /// The approval policy pinned for the thread (`on-request` keeps the gate live).
    pub approval_policy: String,
    /// Extra environment (credential names resolved by assembly; never logged).
    pub env: Vec<(String, String)>,
    /// The handshake deadline.
    pub startup_timeout: Duration,
    /// The loopback capture proxy's base URL (ADR-046); the child gets `-c
    /// model_providers.kian-capture.base_url=<base>/<session>/v1`. `None` = no capture.
    pub capture_base: Option<String>,
}

impl Default for CodexConfig {
    fn default() -> Self {
        Self {
            command: "codex".to_owned(),
            args: Vec::new(),
            model: None,
            cwd: None,
            approval_policy: "on-request".to_owned(),
            env: Vec::new(),
            startup_timeout: Duration::from_secs(15),
            capture_base: None,
        }
    }
}

impl CodexConfig {
    /// The child argv: `codex app-server`, then extras, then the capture provider overrides.
    #[must_use]
    pub fn argv(&self, session: &kian_core::model::SessionId) -> Vec<String> {
        let mut argv = vec!["app-server".to_owned()];
        argv.extend(self.args.iter().cloned());
        if let Some(base) = &self.capture_base {
            let provider = "kian-capture".to_owned();
            argv.push("-c".to_owned());
            argv.push(format!("model_provider={provider}"));
            argv.push("-c".to_owned());
            argv.push(format!("model_providers.{provider}.name={provider}"));
            argv.push("-c".to_owned());
            argv.push(format!(
                "model_providers.{provider}.base_url={base}/{}/v1",
                session.0
            ));
            argv.push("-c".to_owned());
            argv.push(format!("model_providers.{provider}.wire_api=responses"));
            argv.push("-c".to_owned());
            argv.push(format!("model_providers.{provider}.env_key=OPENAI_API_KEY"));
        }
        argv
    }

    /// The `thread/start` params.
    #[must_use]
    pub fn thread_params(&self) -> Value {
        let mut params = json!({"approvalPolicy": self.approval_policy});
        if let Some(model) = &self.model {
            params["model"] = json!(model);
        }
        if let Some(cwd) = &self.cwd {
            params["cwd"] = json!(cwd);
        }
        params
    }
}

/// One in-flight tool item the harness may gate before it runs.
#[derive(Debug, Clone)]
struct PendingItem {
    item_id: String,
    name: String,
    arguments: Value,
}

/// The stateful mapping: per-item pending calls, the approval awaiting an answer, and the turn id.
#[derive(Debug, Default)]
pub struct CodexMapper {
    held: VecDeque<SessionEvent>,
    pending: HashMap<String, PendingItem>,
    /// The approval request id → item id awaiting the kian answer.
    awaiting: Option<(u64, String)>,
    interrupt_sent: bool,
    thread_id: Option<String>,
    turn_id: Option<String>,
    handshakes: usize,
    /// A durable confirmation per harness item id (ADR-050): its result maps onto the pending call.
    confirmed: HashMap<String, ApprovalConfirmation>,
}

impl CodexMapper {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The thread id learned from `thread/start` (spawn handshake).
    #[must_use]
    pub fn thread_id(&self) -> Option<&str> {
        self.thread_id.as_deref()
    }

    /// Records the `thread/start` response.
    pub fn set_thread_id(&mut self, id: String) {
        self.thread_id = Some(id);
    }

    /// The active turn id, when known.
    #[must_use]
    pub fn turn_id(&self) -> Option<&str> {
        self.turn_id.as_deref()
    }

    /// Takes the next queued event.
    pub fn pop(&mut self) -> Option<SessionEvent> {
        self.held.pop_front()
    }

    /// Decodes one line (response, notification or server request).
    pub fn ingest(&mut self, line: &str) -> Result<(), ContractError> {
        let value: Value = serde_json::from_str(line)
            .map_err(|e| ContractError::Drift(format!("codex line is not JSON: {e}")))?;
        let obj = value
            .as_object()
            .ok_or_else(|| ContractError::Drift("codex line is not a JSON object".into()))?;
        if let Some(method) = obj.get("method").and_then(Value::as_str) {
            if let Some(id) = obj.get("id").and_then(Value::as_u64) {
                return self.ingest_server_request(id, method, obj.get("params"));
            }
            return self.ingest_notification(method, obj.get("params"));
        }
        if obj.contains_key("id") {
            return self.ingest_response(obj);
        }
        Err(ContractError::Drift(
            "codex line is neither a request, notification nor response".into(),
        ))
    }

    fn ingest_response(
        &mut self,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<(), ContractError> {
        if let Some(error) = obj.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown json-rpc error");
            return Err(ContractError::Drift(format!(
                "codex error response: {message}"
            )));
        }
        let result = obj.get("result").cloned().unwrap_or(Value::Null);
        if let Some(turn) = result
            .get("turn")
            .and_then(|t| t.get("id"))
            .and_then(Value::as_str)
        {
            self.turn_id = Some(turn.to_owned());
        }
        self.handshakes += 1;
        self.held.push_back(SessionEvent::Mapped(Mapped::Absorbed {
            kind: "codex/response",
        }));
        Ok(())
    }

    fn ingest_notification(
        &mut self,
        method: &str,
        params: Option<&Value>,
    ) -> Result<(), ContractError> {
        let params = params.cloned().unwrap_or(Value::Null);
        match method {
            "turn/started" => {
                if let Some(id) = params.pointer("/turn/id").and_then(Value::as_str) {
                    self.turn_id = Some(id.to_owned());
                }
                self.absorbed("codex/turn-start");
                Ok(())
            }
            "turn/completed" => {
                self.flush_pending(None);
                let status = params
                    .pointer("/turn/status")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let outcome = if self.interrupt_sent || status == "interrupted" {
                    "cancelled"
                } else if status == "completed" {
                    "final_answer"
                } else if status == "failed" {
                    "error"
                } else {
                    return Err(ContractError::Drift(format!(
                        "codex turn/completed with unknown status `{status}`"
                    )));
                };
                self.turn_id = None;
                self.held.push_back(SessionEvent::Mapped(Mapped::Event {
                    kind: EventKind::TurnEnd,
                    payload: json!({"outcome": outcome}),
                }));
                Ok(())
            }
            "item/started" => {
                self.ingest_item(params.get("item"), false);
                self.absorbed("codex/item-started");
                Ok(())
            }
            "item/completed" => {
                self.ingest_item(params.get("item"), true);
                Ok(())
            }
            // Streaming deltas, plans, diffs and usage are audit-only: the completed item is the
            // authoritative state.
            "item/agentMessage/delta"
            | "item/commandExecution/outputDelta"
            | "item/fileChange/outputDelta"
            | "item/plan/delta"
            | "item/reasoning/summaryTextDelta"
            | "item/reasoning/summaryPartAdded"
            | "item/reasoning/textDelta"
            | "turn/diff/updated"
            | "turn/plan/updated"
            | "thread/tokenUsage/updated"
            | "thread/started"
            | "error" => {
                self.absorbed("codex/advisory");
                Ok(())
            }
            other if is_advisory_notification(other) => {
                // Advisory namespaces carry no model-visible content (MCP lifecycle, account,
                // skills, config, token usage, diffs, plans, deltas). Absorbed with a counter; a
                // genuinely unknown method is still drift.
                tracing::debug!(target: "kian::codex", method = other, "absorbed advisory notification");
                self.absorbed("codex/advisory");
                Ok(())
            }
            other => Err(ContractError::Drift(format!(
                "unknown codex notification `{other}`"
            ))),
        }
    }

    fn ingest_item(&mut self, item: Option<&Value>, completed: bool) {
        let Some(item) = item else { return };
        let Some(item_type) = item.get("type").and_then(Value::as_str) else {
            return;
        };
        let item_id = item
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        match item_type {
            "agentMessage" if completed => {
                let text = item.get("text").and_then(Value::as_str).unwrap_or("");
                self.held.push_back(SessionEvent::Mapped(Mapped::Event {
                    kind: EventKind::AssistantMessage,
                    payload: json!({"role": "assistant", "content": {"text": text}}),
                }));
            }
            "agentMessage" | "userMessage" | "reasoning" | "plan" | "enteredReviewMode"
            | "exitedReviewMode" | "contextCompaction" => {}
            "commandExecution" | "fileChange" | "mcpToolCall" | "webSearch" => {
                if !completed {
                    let (name, arguments) = tool_shape(item_type, item);
                    self.pending.insert(
                        item_id.clone(),
                        PendingItem {
                            item_id,
                            name,
                            arguments,
                        },
                    );
                } else {
                    self.finish_item(&item_id, item);
                }
            }
            other => {
                // Unknown item types are audit-only; the turn end still settles honestly.
                tracing::debug!(target: "kian::codex", item = other, "absorbed item");
            }
        }
    }

    fn finish_item(&mut self, item_id: &str, item: &Value) {
        self.flush_item(item_id, None);
        let status = item.get("status").and_then(Value::as_str).unwrap_or("");
        let is_error = matches!(status, "failed" | "declined");
        let output = item
            .get("aggregatedOutput")
            .or_else(|| item.get("result"))
            .cloned()
            .unwrap_or_else(|| {
                json!({
                    "status": status,
                    "exit_code": item.get("exitCode").cloned().unwrap_or(Value::Null),
                })
            });
        let mut content = json!({"call_id": item_id, "output": output, "is_error": is_error});
        let mut payload = json!({"role": "tool", "content": content.clone()});
        if let Some(stamp) = self.confirmed.remove(item_id) {
            content["call_id"] = json!(stamp.call_id);
            payload["content"] = content;
            payload["confirmation"] = json!({
                "call_id": stamp.call_id,
                "decision": if stamp.allow { "approve" } else { "reject" },
                "requested_by": stamp.requested_by,
                "answered_at_ms": stamp.answered_at_ms,
                "expired": stamp.expired,
            });
        }
        self.held.push_back(SessionEvent::Mapped(Mapped::Event {
            kind: EventKind::ToolResult,
            payload,
        }));
    }

    fn ingest_server_request(
        &mut self,
        id: u64,
        method: &str,
        params: Option<&Value>,
    ) -> Result<(), ContractError> {
        let params = params.cloned().unwrap_or(Value::Null);
        let (tool, input) = match method {
            "item/commandExecution/requestApproval" => (
                "commandExecution",
                json!({
                    "command": params.get("command").cloned().unwrap_or(Value::Null),
                    "cwd": params.get("cwd").cloned().unwrap_or(Value::Null),
                }),
            ),
            "item/fileChange/requestApproval" => (
                "fileChange",
                json!({"reason": params.get("reason").cloned().unwrap_or(Value::Null)}),
            ),
            other => {
                return Err(ContractError::Drift(format!(
                    "unknown codex server request `{other}`"
                )));
            }
        };
        let item_id = params
            .get("itemId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        self.awaiting = Some((id, item_id.clone()));
        self.held.push_back(SessionEvent::Approval(ApprovalRequest {
            id: id.to_string(),
            call_id: Some(item_id.clone()),
            tool: tool.to_owned(),
            input,
        }));
        Ok(())
    }

    /// Records the kian decision and builds the JSON-RPC response line. The held call gains its
    /// `permission` provenance; the harness's own result follows.
    pub fn answer(&mut self, answer: &ApprovalAnswer) -> Result<String, ContractError> {
        let (request_id, item_id) = self.awaiting.take().ok_or_else(|| {
            ContractError::Drift("approval answer with no pending request".into())
        })?;
        if let Some(stamp) = &answer.confirmation {
            if let Some(item) = self.pending.remove(&item_id) {
                self.confirmed.insert(item.item_id, stamp.clone());
            }
        } else {
            self.flush_item(&item_id, Some(answer));
        }
        let decision = if answer.allow { "accept" } else { "decline" };
        Ok(json!({"id": request_id, "result": {"decision": decision}}).to_string())
    }

    /// Marks an interrupt was sent so the next `turn/completed` maps to `cancelled`.
    pub fn mark_interrupt(&mut self) {
        self.interrupt_sent = true;
    }

    fn flush_item(&mut self, item_id: &str, permission: Option<&ApprovalAnswer>) {
        let Some(item) = self.pending.remove(item_id) else {
            return;
        };
        let mut content = json!({
            "id": item.item_id,
            "name": item.name,
            "arguments": item.arguments,
        });
        if let Some(answer) = permission {
            let mut provenance = json!({
                "evaluated": if answer.allow { "allow" } else { "deny" },
                "policy": answer.policy,
                "matched": answer.matched,
            });
            if let Some(reason) = &answer.reason {
                provenance["reason"] = json!(reason);
            }
            content["permission"] = provenance;
        }
        self.held.push_back(SessionEvent::Mapped(Mapped::Event {
            kind: EventKind::ToolCall,
            payload: json!({"role": "assistant", "content": content}),
        }));
    }

    fn flush_pending(&mut self, permission: Option<&ApprovalAnswer>) {
        let ids: Vec<String> = self.pending.keys().cloned().collect();
        for id in ids {
            self.flush_item(&id, permission);
        }
    }

    fn absorbed(&mut self, kind: &'static str) {
        self.held
            .push_back(SessionEvent::Mapped(Mapped::Absorbed { kind }));
    }
}

/// Advisory notification namespaces: lifecycle/telemetry that never carry the turn's content.
/// A method outside these prefixes is still a contract error.
fn is_advisory_notification(method: &str) -> bool {
    const PREFIXES: [&str; 12] = [
        "mcpServer/",
        "account/",
        "skills/",
        "app/",
        "config/",
        "experimentalFeature/",
        "thread/",
        "turn/diff/",
        "turn/plan/",
        "item/",
        "remoteControl/",
        "window/",
    ];
    PREFIXES.iter().any(|p| method.starts_with(p))
}

fn tool_shape(item_type: &str, item: &Value) -> (String, Value) {
    match item_type {
        "commandExecution" => (
            "commandExecution".to_owned(),
            json!({
                "command": item.get("command").cloned().unwrap_or(Value::Null),
                "cwd": item.get("cwd").cloned().unwrap_or(Value::Null),
            }),
        ),
        "fileChange" => (
            "fileChange".to_owned(),
            json!({"changes": item.get("changes").cloned().unwrap_or(Value::Null)}),
        ),
        "mcpToolCall" => (
            format!(
                "mcp:{}:{}",
                item.get("server").and_then(Value::as_str).unwrap_or("?"),
                item.get("tool").and_then(Value::as_str).unwrap_or("?")
            ),
            item.get("arguments").cloned().unwrap_or(Value::Null),
        ),
        "webSearch" => (
            "webSearch".to_owned(),
            json!({"query": item.get("query").cloned().unwrap_or(Value::Null)}),
        ),
        other => (other.to_owned(), Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ingest(mapper: &mut CodexMapper, value: Value) {
        mapper.ingest(&value.to_string()).expect("ingest");
    }

    #[test]
    fn an_agent_message_and_turn_end_map_onto_frozen_kinds() {
        let mut mapper = CodexMapper::new();
        ingest(
            &mut mapper,
            json!({"method": "turn/started", "params": {"turn": {"id": "turn_1"}}}),
        );
        ingest(
            &mut mapper,
            json!({"method": "item/completed", "params": {"item": {
                "id": "i1", "type": "agentMessage", "text": "pong"
            }}}),
        );
        assert_eq!(
            mapper.pop(),
            Some(SessionEvent::Mapped(Mapped::Absorbed {
                kind: "codex/turn-start"
            }))
        );
        assert_eq!(
            mapper.pop(),
            Some(SessionEvent::Mapped(Mapped::Event {
                kind: EventKind::AssistantMessage,
                payload: json!({"role": "assistant", "content": {"text": "pong"}}),
            }))
        );
        ingest(
            &mut mapper,
            json!({"method": "turn/completed", "params": {"turn": {"id": "turn_1", "status": "completed"}}}),
        );
        assert_eq!(
            mapper.pop(),
            Some(SessionEvent::Mapped(Mapped::Event {
                kind: EventKind::TurnEnd,
                payload: json!({"outcome": "final_answer"}),
            }))
        );
    }

    #[test]
    fn a_command_approval_holds_the_call_and_records_the_decision() {
        let mut mapper = CodexMapper::new();
        ingest(
            &mut mapper,
            json!({"method": "item/started", "params": {"item": {
                "id": "i2", "type": "commandExecution", "command": "rm -rf /", "cwd": "/"
            }}}),
        );
        ingest(
            &mut mapper,
            json!({"id": 100, "method": "item/commandExecution/requestApproval",
                   "params": {"itemId": "i2", "threadId": "thr_1", "turnId": "turn_1",
                              "command": "rm -rf /", "cwd": "/"}}),
        );
        assert_eq!(
            mapper.pop(),
            Some(SessionEvent::Mapped(Mapped::Absorbed {
                kind: "codex/item-started"
            }))
        );
        assert_eq!(
            mapper.pop(),
            Some(SessionEvent::Approval(ApprovalRequest {
                id: "100".into(),
                call_id: Some("i2".into()),
                tool: "commandExecution".into(),
                input: json!({"command": "rm -rf /", "cwd": "/"}),
            }))
        );
        let wire = mapper
            .answer(&ApprovalAnswer {
                id: "100".into(),
                allow: false,
                policy: "configured",
                matched: "default".into(),
                reason: None,
                confirmation: None,
            })
            .expect("answer");
        let parsed: Value = serde_json::from_str(&wire).unwrap();
        assert_eq!(parsed["result"]["decision"], "decline");

        let call = mapper.pop().expect("held call");
        match call {
            SessionEvent::Mapped(Mapped::Event { kind, payload }) => {
                assert_eq!(kind, EventKind::ToolCall);
                assert_eq!(
                    payload.pointer("/content/permission/evaluated"),
                    Some(&json!("deny"))
                );
            }
            other => panic!("expected tool/call, got {other:?}"),
        }
    }

    #[test]
    fn advisory_namespaces_are_absorbed_but_unknown_methods_are_still_drift() {
        let mut mapper = CodexMapper::new();
        mapper
            .ingest(r#"{"method":"mcpServer/startupStatus/updated","params":{}}"#)
            .expect("advisory absorbed");
        assert_eq!(
            mapper.pop(),
            Some(SessionEvent::Mapped(Mapped::Absorbed {
                kind: "codex/advisory"
            }))
        );

        let err = mapper
            .ingest(r#"{"method":"mystery/thing","params":{}}"#)
            .unwrap_err();
        assert!(matches!(err, ContractError::Drift(_)), "{err:?}");
    }

    #[test]
    fn unknown_methods_are_drift() {
        let mut mapper = CodexMapper::new();
        let err = mapper
            .ingest(r#"{"method":"mystery/thing","params":{}}"#)
            .unwrap_err();
        assert!(matches!(err, ContractError::Drift(_)));
    }
}
