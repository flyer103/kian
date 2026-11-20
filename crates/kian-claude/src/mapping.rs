//! The Claude Code stream-json mapping (HARNESS-BRIDGE §5.1).
//!
//! Pure line-in / event-out logic plus the kian→Claude frame builders. The adapter owns no I/O
//! here, so every mapping rule is unit-testable without a child.

use kian_core::events::EventKind;
use kian_harness::event::{
    ApprovalAnswer, ApprovalConfirmation, ApprovalRequest, ContractError, Mapped, SessionEvent,
};
use serde_json::{Value, json};
use std::collections::VecDeque;

/// The adapter's spawn recipe.
#[derive(Debug, Clone)]
pub struct ClaudeConfig {
    /// The `claude` binary (or a fixture peer).
    pub command: String,
    /// Extra argv appended after the adapter's fixed flags.
    pub args: Vec<String>,
    /// Passed as `--model` when present (the harness owns the call; this is its selection).
    pub model: Option<String>,
    /// Extra environment (credential *names* resolved by assembly; never logged).
    pub env: Vec<(String, String)>,
    /// The loopback capture proxy's base URL (ADR-046); the child gets
    /// `ANTHROPIC_BASE_URL=<base>/<session>`. `None` = no capture.
    pub capture_base: Option<String>,
}

impl Default for ClaudeConfig {
    fn default() -> Self {
        Self {
            command: "claude".to_owned(),
            args: Vec::new(),
            model: None,
            env: Vec::new(),
            capture_base: None,
        }
    }
}

impl ClaudeConfig {
    /// The child argv: the fixed stream-json shape first, then `--model`/`--resume`, then extras.
    #[must_use]
    pub fn argv(&self, resume: Option<&str>) -> Vec<String> {
        let mut argv: Vec<String> = vec![
            "--print".to_owned(),
            "--input-format".to_owned(),
            "stream-json".to_owned(),
            "--output-format".to_owned(),
            "stream-json".to_owned(),
            "--verbose".to_owned(),
            "--permission-mode".to_owned(),
            "default".to_owned(),
        ];
        if let Some(model) = &self.model {
            argv.push("--model".to_owned());
            argv.push(model.clone());
        }
        if let Some(session) = resume {
            argv.push("--resume".to_owned());
            argv.push(session.to_owned());
        }
        argv.extend(self.args.iter().cloned());
        argv
    }
}

/// One Claude tool call waiting for its kian approval decision.
#[derive(Debug, Clone)]
struct PendingCall {
    id: String,
    name: String,
    input: Value,
}

/// The stateful mapping: pending tool calls (held until the approval outcome is known), queued
/// output events, and the interrupt marker.
#[derive(Debug, Default)]
pub struct ClaudeMapper {
    held: VecDeque<SessionEvent>,
    pending: Vec<PendingCall>,
    /// A durable confirmation per harness call id: the result is mapped onto the pending call.
    confirmed: std::collections::HashMap<String, ApprovalConfirmation>,
    /// The tool of the approval request currently awaiting an answer.
    awaiting: Option<String>,
    interrupt_sent: bool,
    init_seen: bool,
    /// The CLI's session id from `system/init` (the resume handle, ADR-047).
    session_id: Option<String>,
}

impl ClaudeMapper {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the `system/init` handshake has been observed.
    #[must_use]
    pub fn init_seen(&self) -> bool {
        self.init_seen
    }

    /// The harness's continuation handle, learned from `system/init` (ADR-047).
    #[must_use]
    pub fn session_id(&self) -> Option<String> {
        self.session_id.clone()
    }

    /// Takes the next queued event.
    pub fn pop(&mut self) -> Option<SessionEvent> {
        self.held.pop_front()
    }

    /// Decodes one stdout line, queueing the resulting event(s).
    pub fn ingest(&mut self, line: &str) -> Result<(), ContractError> {
        let value: Value = serde_json::from_str(line)
            .map_err(|e| ContractError::Drift(format!("claude line is not JSON: {e}")))?;
        let obj = value
            .as_object()
            .ok_or_else(|| ContractError::Drift("claude line is not a JSON object".into()))?;
        let kind = obj
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| ContractError::Drift("claude line has no 'type'".into()))?;

        match kind {
            "system" => {
                let subtype = obj.get("subtype").and_then(Value::as_str).unwrap_or("");
                if subtype == "init" {
                    self.init_seen = true;
                    self.session_id = obj
                        .get("session_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                }
                self.held.push_back(SessionEvent::Mapped(Mapped::Absorbed {
                    kind: "claude/system",
                }));
            }
            "assistant" => self.ingest_assistant(obj)?,
            "user" => self.ingest_user(obj),
            "result" => self.ingest_result(obj),
            "control_request" => self.ingest_control_request(obj)?,
            "control_response" | "stream_event" | "rate_limit_event" | "tool_progress" => {
                self.held.push_back(SessionEvent::Mapped(Mapped::Absorbed {
                    kind: "claude/advisory",
                }));
            }
            other => {
                return Err(ContractError::Drift(format!(
                    "unknown claude message type `{other}`"
                )));
            }
        }
        Ok(())
    }

    fn ingest_assistant(
        &mut self,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<(), ContractError> {
        // The CLI reports some provider/auth failures as a *synthetic* assistant message
        // (`"model":"<synthetic>"`, `"is_api_error_message":true`, `"error":...`). That text is
        // not model output; the honest record is the `result`'s error turn end. Absorb it.
        if obj.get("is_api_error_message").and_then(Value::as_bool) == Some(true)
            || obj.get("error").is_some()
        {
            self.held.push_back(SessionEvent::Mapped(Mapped::Absorbed {
                kind: "claude/api_error",
            }));
            return Ok(());
        }
        let content = obj
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ContractError::Drift("claude assistant message has no content array".into())
            })?;
        let mut text = String::new();
        let mut tools = 0usize;
        for block in content {
            match block.get("type").and_then(Value::as_str) {
                Some("text") => {
                    if let Some(part) = block.get("text").and_then(Value::as_str) {
                        text.push_str(part);
                    }
                }
                Some("tool_use") => {
                    let id = block
                        .get("id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| ContractError::Drift("tool_use without id".into()))?;
                    let name = block
                        .get("name")
                        .and_then(Value::as_str)
                        .ok_or_else(|| ContractError::Drift("tool_use without name".into()))?;
                    let input = block.get("input").cloned().unwrap_or(Value::Null);
                    self.pending.push(PendingCall {
                        id: id.to_owned(),
                        name: name.to_owned(),
                        input,
                    });
                    tools += 1;
                }
                // thinking / redacted_thinking / images are not kian context.
                Some(_) => {}
                None => {
                    return Err(ContractError::Drift(
                        "claude assistant content block has no type".into(),
                    ));
                }
            }
        }
        if !text.is_empty() {
            self.held.push_back(SessionEvent::Mapped(Mapped::Event {
                kind: EventKind::AssistantMessage,
                payload: json!({"role": "assistant", "content": {"text": text}}),
            }));
        }
        if text.is_empty() && tools == 0 {
            return Err(ContractError::Drift(
                "claude assistant message carried no text and no tool_use".into(),
            ));
        }
        Ok(())
    }

    fn ingest_user(&mut self, obj: &serde_json::Map<String, Value>) {
        let blocks = obj
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for block in &blocks {
            if block.get("type").and_then(Value::as_str) != Some("tool_result") {
                continue;
            }
            self.flush_pending(None);
            let call_id = block
                .get("tool_use_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let is_error = block
                .get("is_error")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let output = block.get("content").cloned().unwrap_or(Value::Null);
            let mut content = json!({
                "call_id": call_id,
                "output": output,
                "is_error": is_error,
            });
            let mut payload = json!({"role": "tool", "content": content.clone()});
            if let Some(stamp) = self.confirmed.remove(call_id) {
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
    }

    fn ingest_result(&mut self, obj: &serde_json::Map<String, Value>) {
        self.flush_pending(None);
        let is_error = obj
            .get("is_error")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let subtype = obj.get("subtype").and_then(Value::as_str).unwrap_or("");
        let outcome =
            if self.interrupt_sent || subtype.contains("interrupt") || subtype == "cancelled" {
                "cancelled"
            } else if is_error || subtype.starts_with("error") {
                "error"
            } else {
                "final_answer"
            };
        self.held.push_back(SessionEvent::Mapped(Mapped::Event {
            kind: EventKind::TurnEnd,
            payload: json!({"outcome": outcome}),
        }));
        self.interrupt_sent = false;
    }

    fn ingest_control_request(
        &mut self,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<(), ContractError> {
        let request = obj
            .get("request")
            .and_then(Value::as_object)
            .ok_or_else(|| ContractError::Drift("control_request without request".into()))?;
        let subtype = request.get("subtype").and_then(Value::as_str).unwrap_or("");
        match subtype {
            "can_use_tool" => {
                let id = obj
                    .get("request_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        ContractError::Drift("can_use_tool without request_id".into())
                    })?;
                let tool = request
                    .get("tool_name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ContractError::Drift("can_use_tool without tool_name".into()))?;
                let input = request.get("input").cloned().unwrap_or(Value::Null);
                self.awaiting = Some(tool.to_owned());
                // The harness's call id for the held `tool_use` block (matched by name; the
                // approval request itself carries no tool_use id).
                let call_id = self
                    .pending
                    .iter()
                    .find(|c| c.name == tool)
                    .map(|c| c.id.clone());
                self.held.push_back(SessionEvent::Approval(ApprovalRequest {
                    id: id.to_owned(),
                    call_id,
                    tool: tool.to_owned(),
                    input,
                }));
                Ok(())
            }
            _ => {
                self.held.push_back(SessionEvent::Mapped(Mapped::Absorbed {
                    kind: "claude/control_request",
                }));
                Ok(())
            }
        }
    }

    /// Records the kian decision: builds the harness answer line and queues the held call with
    /// its `permission` provenance. The harness's own error result follows as a normal
    /// `tool/result` when it denies.
    pub fn answer(&mut self, answer: &ApprovalAnswer) -> Result<String, ContractError> {
        let awaiting = self.awaiting.take();
        if let Some(tool) = awaiting
            && let Some(pos) = self.pending.iter().position(|c| c.name == tool)
        {
            let call = self.pending.remove(pos);
            if let Some(stamp) = &answer.confirmation {
                // The pending `tool/call` already exists: map the harness result onto it.
                self.confirmed.insert(call.id, stamp.clone());
                return Ok(approval_line(answer));
            }
            let mut content = json!({
                "id": call.id,
                "name": call.name,
                "arguments": call.input,
            });
            let mut permission = json!({
                "evaluated": if answer.allow { "allow" } else { "deny" },
                "policy": answer.policy,
                "matched": answer.matched,
            });
            if let Some(reason) = &answer.reason {
                permission["reason"] = json!(reason);
            }
            content["permission"] = permission;
            self.held.push_back(SessionEvent::Mapped(Mapped::Event {
                kind: EventKind::ToolCall,
                payload: json!({"role": "assistant", "content": content}),
            }));
        }
        Ok(approval_line(answer))
    }

    /// Marks that an interrupt frame was sent, so the next `result` maps to `cancelled`.
    pub fn mark_interrupt(&mut self) {
        self.interrupt_sent = true;
    }

    fn flush_pending(&mut self, permission: Option<&ApprovalAnswer>) {
        for call in self.pending.drain(..) {
            let mut content = json!({
                "id": call.id,
                "name": call.name,
                "arguments": call.input,
            });
            if let Some(answer) = permission {
                let mut permission = json!({
                    "evaluated": if answer.allow { "allow" } else { "deny" },
                    "policy": answer.policy,
                    "matched": answer.matched,
                });
                if let Some(reason) = &answer.reason {
                    permission["reason"] = json!(reason);
                }
                content["permission"] = permission;
            }
            self.held.push_back(SessionEvent::Mapped(Mapped::Event {
                kind: EventKind::ToolCall,
                payload: json!({"role": "assistant", "content": content}),
            }));
        }
    }
}

/// One user message line for the harness's stream-json input.
#[must_use]
pub fn user_line(content: &Value) -> String {
    let text = match content {
        Value::String(text) => text.clone(),
        Value::Object(obj) => obj
            .get("text")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| content.to_string()),
        other => other.to_string(),
    };
    serde_json::to_string(&json!({
        "type": "user",
        "message": {"role": "user", "content": [{"type": "text", "text": text}]}
    }))
    .expect("user frame serializes")
}

/// One interrupt control request.
#[must_use]
pub fn interrupt_line(request_id: &str) -> String {
    serde_json::to_string(&json!({
        "type": "control_request",
        "request_id": request_id,
        "request": {"subtype": "interrupt"}
    }))
    .expect("interrupt frame serializes")
}

/// One approval answer.
#[must_use]
pub fn approval_line(answer: &ApprovalAnswer) -> String {
    serde_json::to_string(&json!({
        "type": "control_response",
        "response": {
            "subtype": "success",
            "request_id": answer.id,
            "response": {
                "behavior": if answer.allow { "allow" } else { "deny" },
                "message": if answer.allow { "" } else { "denied by kian approval policy" }
            }
        }
    }))
    .expect("approval frame serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(value: Value) -> String {
        serde_json::to_string(&value).unwrap()
    }

    #[test]
    fn argv_resumes_only_when_a_handle_is_given() {
        let fresh = ClaudeConfig::default().argv(None);
        assert!(!fresh.contains(&"--resume".to_owned()));
        let resumed = ClaudeConfig::default().argv(Some("sess-1"));
        let pos = resumed
            .iter()
            .position(|arg| arg == "--resume")
            .expect("--resume");
        assert_eq!(resumed[pos + 1], "sess-1");
    }

    #[test]
    fn init_text_and_result_map_onto_existing_kinds() {
        let mut mapper = ClaudeMapper::new();
        mapper
            .ingest(&line(json!({"type": "system", "subtype": "init"})))
            .unwrap();
        assert!(mapper.init_seen());
        assert_eq!(
            mapper.pop(),
            Some(SessionEvent::Mapped(Mapped::Absorbed {
                kind: "claude/system"
            }))
        );

        mapper
            .ingest(&line(json!({
                "type": "assistant",
                "message": {"role": "assistant", "content": [
                    {"type": "text", "text": "hello"},
                    {"type": "tool_use", "id": "t1", "name": "Read", "input": {"path": "x"}}
                ]}
            })))
            .unwrap();
        assert_eq!(
            mapper.pop(),
            Some(SessionEvent::Mapped(Mapped::Event {
                kind: EventKind::AssistantMessage,
                payload: json!({"role": "assistant", "content": {"text": "hello"}}),
            }))
        );

        mapper
            .ingest(&line(json!({
                "type": "user",
                "message": {"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": "t1", "content": "ok"}
                ]}
            })))
            .unwrap();
        // The pending call is flushed before its result (log-before-execute ordering).
        assert_eq!(
            mapper.pop(),
            Some(SessionEvent::Mapped(Mapped::Event {
                kind: EventKind::ToolCall,
                payload: json!({
                    "role": "assistant",
                    "content": {"id": "t1", "name": "Read", "arguments": {"path": "x"}}
                }),
            }))
        );
        assert_eq!(
            mapper.pop(),
            Some(SessionEvent::Mapped(Mapped::Event {
                kind: EventKind::ToolResult,
                payload: json!({
                    "role": "tool",
                    "content": {"call_id": "t1", "output": "ok", "is_error": false}
                }),
            }))
        );

        mapper
            .ingest(&line(
                json!({"type": "result", "subtype": "success", "is_error": false}),
            ))
            .unwrap();
        assert_eq!(
            mapper.pop(),
            Some(SessionEvent::Mapped(Mapped::Event {
                kind: EventKind::TurnEnd,
                payload: json!({"outcome": "final_answer"}),
            }))
        );
    }

    #[test]
    fn approval_decides_the_held_call_and_records_provenance() {
        let mut mapper = ClaudeMapper::new();
        mapper
            .ingest(&line(json!({
                "type": "assistant",
                "message": {"role": "assistant", "content": [
                    {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "rm -rf /"}}
                ]}
            })))
            .unwrap();
        mapper
            .ingest(&line(json!({
                "type": "control_request",
                "request_id": "r1",
                "request": {"subtype": "can_use_tool", "tool_name": "Bash", "input": {"command": "rm -rf /"}}
            })))
            .unwrap();
        assert_eq!(
            mapper.pop(),
            Some(SessionEvent::Approval(ApprovalRequest {
                id: "r1".into(),
                call_id: Some("t1".into()),
                tool: "Bash".into(),
                input: json!({"command": "rm -rf /"}),
            }))
        );

        let answer = ApprovalAnswer {
            id: "r1".into(),
            allow: false,
            policy: "configured",
            matched: "default".into(),
            reason: None,
            confirmation: None,
        };
        let wire = mapper.answer(&answer).expect("answer line");
        let parsed: Value = serde_json::from_str(&wire).unwrap();
        assert_eq!(parsed["type"], "control_response");
        assert_eq!(parsed["response"]["response"]["behavior"], "deny");

        assert_eq!(
            mapper.pop(),
            Some(SessionEvent::Mapped(Mapped::Event {
                kind: EventKind::ToolCall,
                payload: json!({
                    "role": "assistant",
                    "content": {
                        "id": "t1",
                        "name": "Bash",
                        "arguments": {"command": "rm -rf /"},
                        "permission": {"evaluated": "deny", "policy": "configured", "matched": "default"}
                    }
                }),
            }))
        );
    }

    #[test]
    fn interrupt_maps_the_result_to_cancelled_never_final() {
        let mut mapper = ClaudeMapper::new();
        mapper.mark_interrupt();
        mapper
            .ingest(&line(
                json!({"type": "result", "subtype": "success", "is_error": false}),
            ))
            .unwrap();
        assert_eq!(
            mapper.pop(),
            Some(SessionEvent::Mapped(Mapped::Event {
                kind: EventKind::TurnEnd,
                payload: json!({"outcome": "cancelled"}),
            }))
        );
    }

    #[test]
    fn unknown_messages_are_drift_not_silence() {
        let mut mapper = ClaudeMapper::new();
        let err = mapper.ingest("{\"type\":\"mystery\"}").unwrap_err();
        assert!(matches!(err, ContractError::Drift(_)));
        assert_eq!(mapper.pop(), None);
    }
}
