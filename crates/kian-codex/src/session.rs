//! The Codex adapter: spawn `codex app-server`, perform the initialize/thread handshake, and
//! expose the session as a [`HarnessSession`].

use std::time::Duration;

use futures::future::BoxFuture;
use kian_core::model::SessionId;
use kian_harness::SessionFactory;
use kian_harness::event::{
    ApprovalAnswer, Capabilities, ContractError, Control, HarnessSession, SessionEvent, SteerMode,
};
use kian_harness::peer::{LinePeer, PeerLine};
use serde_json::{Value, json};

use crate::codec::{CodexConfig, CodexMapper};

/// The Codex capability declaration (HARNESS-BRIDGE §3, drift-tested).
pub const CAPABILITIES: Capabilities = Capabilities {
    name: "codex",
    request_bytes: false,
    steer: SteerMode::InTurn,
    interrupt: true,
    approval_gate: true,
    resume: false,
};

/// The session factory.
#[derive(Debug, Clone)]
pub struct CodexAdapter {
    pub config: CodexConfig,
}

impl CodexAdapter {
    #[must_use]
    pub fn new(config: CodexConfig) -> Self {
        Self { config }
    }
}

impl SessionFactory for CodexAdapter {
    fn capabilities(&self) -> Capabilities {
        CAPABILITIES
    }

    fn spawn(
        &self,
        session: SessionId,
    ) -> BoxFuture<'static, Result<Box<dyn HarnessSession>, String>> {
        let config = self.config.clone();
        Box::pin(async move {
            let argv = config.argv(&session);
            let peer = LinePeer::spawn(&config.command, &argv, &config.env).await?;
            let mut session = CodexSession {
                peer,
                mapper: CodexMapper::new(),
                thread_id: None,
                next_id: 1,
                startup_timeout: config.startup_timeout,
            };
            session
                .request(
                    "initialize",
                    json!({"clientInfo": {"name": "kian", "title": "Kian", "version": "0.1.0"}}),
                )
                .await?;
            session.read_until_response(1).await?;
            session.notify("initialized", json!({})).await?;
            session
                .request("thread/start", config.thread_params())
                .await?;
            let response = session.read_until_response(2).await?;
            let thread = response
                .pointer("/result/thread/id")
                .and_then(Value::as_str)
                .ok_or_else(|| "codex handshake: thread/start returned no thread id".to_owned())?;
            session.thread_id = Some(thread.to_owned());
            session.mapper.set_thread_id(thread.to_owned());
            Ok(Box::new(session) as Box<dyn HarnessSession>)
        })
    }
}

/// One live Codex app-server child.
pub struct CodexSession {
    peer: LinePeer,
    mapper: CodexMapper,
    thread_id: Option<String>,
    next_id: u64,
    startup_timeout: Duration,
}

impl CodexSession {
    async fn request(&mut self, method: &str, params: Value) -> Result<u64, String> {
        let id = self.next_id;
        self.next_id += 1;
        let line = json!({"id": id, "method": method, "params": params}).to_string();
        self.peer
            .send(line)
            .await
            .map_err(|e| format!("codex {method}: {e}"))?;
        Ok(id)
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<(), String> {
        let line = json!({"method": method, "params": params}).to_string();
        self.peer
            .send(line)
            .await
            .map_err(|e| format!("codex {method}: {e}"))
    }

    async fn read_until_response(&mut self, id: u64) -> Result<Value, String> {
        loop {
            let line = self
                .peer
                .next_line_within(self.startup_timeout)
                .await
                .map_err(|e| format!("codex handshake: {e}"))?;
            let value: Value = serde_json::from_str(&line)
                .map_err(|e| format!("codex handshake: not JSON: {e}"))?;
            if let Some(error) = value.get("error") {
                let message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown json-rpc error");
                return Err(format!("codex handshake: {message}"));
            }
            if value.get("id").and_then(Value::as_u64) == Some(id) && value.get("method").is_none()
            {
                return Ok(value);
            }
            // Handshake-adjacent notifications/requests are drained by the mapper (or ignored),
            // never silently mistaken for the response.
        }
    }

    fn text_of(content: &Value) -> String {
        match content {
            Value::String(text) => text.clone(),
            Value::Object(obj) => obj
                .get("text")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_else(|| content.to_string()),
            other => other.to_string(),
        }
    }
}

impl HarnessSession for CodexSession {
    fn capabilities(&self) -> Capabilities {
        CAPABILITIES
    }

    fn next(&mut self) -> BoxFuture<'_, Result<SessionEvent, ContractError>> {
        Box::pin(async move {
            loop {
                if let Some(event) = self.mapper.pop() {
                    return Ok(event);
                }
                match self.peer.next().await {
                    PeerLine::Line(line) => self.mapper.ingest(&line)?,
                    PeerLine::Exit(code) => return Ok(SessionEvent::Exit { code }),
                    PeerLine::Io(e) => return Err(ContractError::Io(e)),
                }
            }
        })
    }

    fn send(&mut self, control: Control) -> BoxFuture<'_, Result<(), ContractError>> {
        Box::pin(async move {
            let thread = self
                .thread_id
                .clone()
                .ok_or_else(|| ContractError::Drift("codex: no thread id".into()))?;
            let line = match control {
                Control::Input { content } => {
                    let text = Self::text_of(&content);
                    json!({
                        "id": self.next_id,
                        "method": "turn/start",
                        "params": {"threadId": thread, "input": [{"type": "text", "text": text}]}
                    })
                    .to_string()
                }
                Control::Steer { content } => {
                    let text = Self::text_of(&content);
                    match self.mapper.turn_id() {
                        Some(turn) => json!({
                            "id": self.next_id,
                            "method": "turn/steer",
                            "params": {"threadId": thread, "expectedTurnId": turn,
                                       "input": [{"type": "text", "text": text}]}
                        })
                        .to_string(),
                        None => json!({
                            "id": self.next_id,
                            "method": "turn/start",
                            "params": {"threadId": thread, "input": [{"type": "text", "text": text}]}
                        })
                        .to_string(),
                    }
                }
                Control::Interrupt => {
                    self.mapper.mark_interrupt();
                    match self.mapper.turn_id() {
                        Some(turn) => json!({
                            "id": self.next_id,
                            "method": "turn/interrupt",
                            "params": {"threadId": thread, "turnId": turn}
                        })
                        .to_string(),
                        None => return Ok(()),
                    }
                }
            };
            self.next_id += 1;
            self.peer.send(line).await
        })
    }

    fn answer(&mut self, answer: ApprovalAnswer) -> BoxFuture<'_, Result<(), ContractError>> {
        Box::pin(async move {
            let line = self.mapper.answer(&answer)?;
            self.peer.send(line).await
        })
    }
}
