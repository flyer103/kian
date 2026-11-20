//! The Claude Code adapter: spawn the child, perform the init handshake, and expose it as a
//! [`HarnessSession`].

use std::time::Duration;

use futures::future::BoxFuture;
use kian_core::model::SessionId;
use kian_harness::SessionFactory;
use kian_harness::event::{
    Capabilities, ContractError, Control, HarnessSession, SessionEvent, SteerMode,
};
use kian_harness::peer::{LinePeer, PeerLine};

use crate::mapping::{ClaudeConfig, ClaudeMapper, interrupt_line, user_line};

/// The Claude Code capability declaration (HARNESS-BRIDGE §3, drift-tested).
pub const CAPABILITIES: Capabilities = Capabilities {
    name: "claude-code",
    request_bytes: false,
    steer: SteerMode::NextTurn,
    interrupt: true,
    approval_gate: true,
    resume: false,
};

/// The session factory: one spawn recipe per episode.
#[derive(Debug, Clone)]
pub struct ClaudeAdapter {
    pub config: ClaudeConfig,
}

impl ClaudeAdapter {
    #[must_use]
    pub fn new(config: ClaudeConfig) -> Self {
        Self { config }
    }
}

impl SessionFactory for ClaudeAdapter {
    fn capabilities(&self) -> Capabilities {
        CAPABILITIES
    }

    fn spawn(
        &self,
        _session: SessionId,
    ) -> BoxFuture<'static, Result<Box<dyn HarnessSession>, String>> {
        let config = self.config.clone();
        Box::pin(async move {
            let argv = config.argv();
            let peer = LinePeer::spawn(&config.command, &argv, &config.env).await?;
            let mapper = ClaudeMapper::new();
            // The CLI has no explicit ready frame, and it may emit nothing until it receives the
            // first input (session hooks are not guaranteed). Readiness is therefore the spawn
            // itself; `system/init` is absorbed by the mapper when it arrives. A child that dies
            // early surfaces as `Exit` on the first read (honest `SoftFailed`).
            Ok(Box::new(ClaudeSession {
                peer,
                mapper,
                interrupt_seq: 0,
            }) as Box<dyn HarnessSession>)
        })
    }
}

/// One live Claude Code child.
pub struct ClaudeSession {
    peer: LinePeer,
    mapper: ClaudeMapper,
    interrupt_seq: u64,
}

impl HarnessSession for ClaudeSession {
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
            let line = match control {
                Control::Input { content } | Control::Steer { content } => user_line(&content),
                Control::Interrupt => {
                    self.mapper.mark_interrupt();
                    self.interrupt_seq += 1;
                    interrupt_line(&format!("kian-interrupt-{}", self.interrupt_seq))
                }
            };
            self.peer.send(line).await
        })
    }

    fn answer(
        &mut self,
        answer: kian_harness::event::ApprovalAnswer,
    ) -> BoxFuture<'_, Result<(), ContractError>> {
        Box::pin(async move {
            let line = self.mapper.answer(&answer)?;
            self.peer.send(line).await
        })
    }
}

impl ClaudeSession {
    /// The handshake deadline the adapter uses (exposed for assembly diagnostics).
    #[must_use]
    pub fn default_startup_timeout() -> Duration {
        Duration::from_secs(10)
    }
}
