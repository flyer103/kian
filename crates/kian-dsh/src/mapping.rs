//! The S6 dsh mapping contract (ADR-010, DESIGN §16) — harness session-log
//! semantics translated onto the existing Kian durable-event subset.
//!
//! This module is *the contract*, not the bridge: C54's driver embeds it, and
//! any dsh-side plugin (the `dsh-superagent-prototype` lineage, ecosystem-
//! owned, outside this repo) must satisfy it to be kian-compatible. It is
//! bundle-level by design (card C53): the `kian-core` kind set and
//! [`kian_core::context::derive_model_context`] are untouched — the mapping
//! only *selects* existing kinds and shapes their payloads exactly as the
//! built-in loop already does.
//!
//! # The mapping table (ADR-010 / DESIGN §16)
//!
//! | dsh durable kind | kian kind | payload rule |
//! |---|---|---|
//! | `user/message` | `input/admitted` | `{"role":"user","content":<verbatim>}` (+`synthetic` when the peer marks the injected message) |
//! | `system/message` | `input/admitted` | `{"role":"system","content":<verbatim>}` — closest call #1: legal under the frozen message contract (any non-empty role) and honest — it *is* an admitted input |
//! | `assistant/message` | `assistant/message` | `{"role":"assistant","content":<assembled content verbatim>}` |
//! | `assistant/attempt` | `step/request` | the attempt's assembled request bytes ride `body` verbatim; the envelope's `slot`/`provider`/`model`/`iteration` come from the bridge session's binding. An attempt with no following `assistant/message` **is** the honest failure trace — closest call #2, no `attempt/*` kind exists |
//! | `tool/call` | `tool/call` | `{"role":"assistant","content":{"id","name","arguments"}}` |
//! | `tool/result` | `tool/result` | `{"role":"tool","content":{"call_id","output","is_error"}}` |
//! | `step/start` | `turn/start` | `{"iteration":<n>}` — a dsh *step* is one model request plus its tools, exactly a Kian provider turn |
//! | `step/end` | `turn/end` | the loop's payload shapes (`tool_calls` / `final_answer` / `error` / `cancelled`) |
//! | `turn/start`, `turn/end` | — (absorbed) | the dsh episode envelope's kian counterpart is the drain episode, whose events the orchestrator already owns; the mapper reports [`Mapped::Absorbed`] explicitly — never a silent drop |
//! | anything else | error | [`ContractError::UnknownKind`] — an honest contract error; adding a row is a contract amendment |
//!
//! # Generations
//!
//! dsh session files are versioned generations that decode to *current*
//! logical events before anything else happens (the dsh session-format
//! contract). The bridge accepts only logical events: the peer's `ready`
//! frame declares its logical format, and any other value is
//! [`ContractError::ForeignLogicalFormat`] — Kian never stores foreign
//! generations; mapped events are re-encoded as v0 Kian envelopes.
//!
//! # Persist-what-you-send across the process boundary
//!
//! The peer must deliver the step's assembled request bytes
//! (`assistant/attempt`) **before** the provider call settles — observed at
//! the boundary as: no `assistant/message` or `tool/*` frame is accepted for
//! a step whose request bytes have not arrived first
//! ([`ContractError::ResponseBeforeRequestBytes`]). If the dsh SDK surface
//! cannot emit them pre-flight, constraint 2 cannot hold and the bridge
//! stops at a documented negative result — never a best-effort post-hoc
//! logger.
//!
//! # Control translation (kian → dsh)
//!
//! [`ControlFrame`] mirrors the orchestrator's input surfaces: promoted
//! inputs ride `input`, steers ride `steer` (the peer injects the message
//! into its next admitted request), and an interrupt rides `interrupt` (the
//! peer stops its turn; a cancelled step settles as an attempt with no
//! `assistant/message` — `interrupted`, never `completed`).

use kian_core::events::EventKind;
use serde_json::{Map, Value, json};
use thiserror::Error;

/// The bridge wire-contract version (ADR-010). Additive-only, like every
/// frozen surface in this repo: a breaking change is a contract amendment.
pub const BRIDGE_CONTRACT_VERSION: u64 = 0;

/// The dsh *logical* event format this contract supports. The peer's
/// `ready` frame must declare exactly this; generations are decoded on the
/// dsh side before events reach the bridge.
pub const SUPPORTED_LOGICAL_FORMAT: u64 = 0;

/// Honest contract errors — the bridge stops on any of these; none is ever
/// a silent drop (constraint 7).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ContractError {
    /// A frame line is not valid JSON or not a frame object.
    #[error("dsh bridge contract: malformed frame line: {reason}")]
    MalformedFrame { reason: &'static str },
    /// The frame's `v` is not the contract version.
    #[error("dsh bridge contract: frame version {got} != v{supported}")]
    UnsupportedVersion { got: u64, supported: u64 },
    /// A frame `type` outside the contract.
    #[error(
        "dsh bridge contract: unknown frame type `{frame_type}` (v{supported}); adding one is a contract amendment"
    )]
    UnknownFrame { frame_type: String, supported: u64 },
    /// The peer's declared logical format is not the supported one — dsh
    /// session generations must be decoded to current logical events
    /// *before* mapping; Kian never stores foreign generations.
    #[error(
        "dsh bridge contract: peer logical format {got} != supported {supported}; decode generations to current logical events on the dsh side"
    )]
    ForeignLogicalFormat { got: u64, supported: u64 },
    /// The first frame was not `ready`.
    #[error("dsh bridge contract: the first frame must be ready")]
    ReadyNotFirst,
    /// A duplicate `ready` frame.
    #[error("dsh bridge contract: duplicate ready frame")]
    DuplicateReady,
    /// A durable event kind outside the mapping table. The honest outcome
    /// is an error the caller can act on — never a silent drop, never a
    /// guess.
    #[error(
        "dsh bridge contract: unknown durable event kind `{kind}`; adding a mapping row is a contract amendment"
    )]
    UnknownKind { kind: String },
    /// A known kind whose payload does not match the contract.
    #[error("dsh bridge contract: {kind} payload malformed: {reason}")]
    MalformedEvent { kind: String, reason: &'static str },
    /// **The pre-flight rule**: a response frame was offered for a step
    /// whose assembled request bytes have not been received and persisted.
    /// Persist-what-you-send cannot hold; the bridge stops here.
    #[error(
        "dsh bridge contract: pre-flight violation — {kind} offered before the step's request bytes (assistant/attempt)"
    )]
    ResponseBeforeRequestBytes { kind: &'static str },
    /// An event that belongs inside a step arrived with no step open.
    #[error("dsh bridge contract: {kind} outside an open step")]
    StepNotOpen { kind: &'static str },
    /// A `step/start` while a step is already open.
    #[error("dsh bridge contract: nested step/start (a step must end before the next opens)")]
    StepAlreadyOpen,
}

/// One logical dsh session event as it crosses the bridge: the durable
/// `kind`, its `payload`, and the dsh-side timestamp (ms).
///
/// This is the *logical* shape (the contract's own vocabulary over the dsh
/// session-log families), not a dsh generation: the peer decodes its own
/// versioned session files first (ADR-010).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DshEvent {
    pub kind: String,
    pub payload: Value,
    pub ts: u64,
}

/// One dsh→kian wire frame (one JSON object per line over the peer's
/// stdout).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerFrame {
    /// The peer's first frame: declares the logical format of every event
    /// that follows.
    Ready { logical_format: u64 },
    /// One logical dsh session event.
    SessionEvent(DshEvent),
}

/// Parses one dsh→kian wire line into a [`PeerFrame`]. Version and frame
/// shape are validated here; mapping-level rules (ready-first, ordering)
/// live in [`DshMapper`] and [`PeerSession`].
pub fn parse_peer_frame(line: &str) -> Result<PeerFrame, ContractError> {
    let value: Value = serde_json::from_str(line).map_err(|_| ContractError::MalformedFrame {
        reason: "not valid JSON",
    })?;
    let obj = value.as_object().ok_or(ContractError::MalformedFrame {
        reason: "a frame must be a JSON object",
    })?;

    let version = obj
        .get("v")
        .and_then(Value::as_u64)
        .ok_or(ContractError::MalformedFrame {
            reason: "'v' must be a u64",
        })?;
    if version != BRIDGE_CONTRACT_VERSION {
        return Err(ContractError::UnsupportedVersion {
            got: version,
            supported: BRIDGE_CONTRACT_VERSION,
        });
    }

    let frame_type =
        obj.get("type")
            .and_then(Value::as_str)
            .ok_or(ContractError::MalformedFrame {
                reason: "'type' must be a string",
            })?;
    match frame_type {
        "ready" => {
            let logical_format = obj.get("logical_format").and_then(Value::as_u64).ok_or(
                ContractError::MalformedFrame {
                    reason: "ready requires a u64 'logical_format'",
                },
            )?;
            Ok(PeerFrame::Ready { logical_format })
        }
        "session-event" => {
            let event = obj
                .get("event")
                .ok_or(ContractError::MalformedFrame {
                    reason: "session-event requires an 'event' object",
                })?
                .as_object()
                .ok_or(ContractError::MalformedFrame {
                    reason: "'event' must be a JSON object",
                })?;
            let kind = event
                .get("kind")
                .and_then(Value::as_str)
                .filter(|k| !k.is_empty())
                .ok_or(ContractError::MalformedFrame {
                    reason: "'event.kind' must be a non-empty string",
                })?;
            let payload = event
                .get("payload")
                .cloned()
                .ok_or(ContractError::MalformedFrame {
                    reason: "'event.payload' is required (use {} when empty)",
                })?;
            let ts =
                event
                    .get("ts")
                    .and_then(Value::as_u64)
                    .ok_or(ContractError::MalformedFrame {
                        reason: "'event.ts' must be a u64 (unix epoch ms)",
                    })?;
            Ok(PeerFrame::SessionEvent(DshEvent {
                kind: kind.to_owned(),
                payload,
                ts,
            }))
        }
        other => Err(ContractError::UnknownFrame {
            frame_type: other.to_owned(),
            supported: BRIDGE_CONTRACT_VERSION,
        }),
    }
}

/// One kian→dsh control frame (one JSON object per line over the peer's
/// stdin): the translation of the orchestrator's input surfaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlFrame {
    /// A promoted input row — the plain message content, exactly what the
    /// inbox row carried.
    Input { content: Value },
    /// A steer: the synthetic (strippable) content; the peer injects it
    /// into its next admitted request and marks the resulting
    /// `user/message` with `synthetic: true`.
    Steer { content: Value },
    /// An interrupt: the peer stops its current turn; a cancelled step
    /// settles as an attempt with no `assistant/message`.
    Interrupt,
}

/// Serializes one [`ControlFrame`] to its wire line.
pub fn control_line(frame: &ControlFrame) -> String {
    let value = match frame {
        ControlFrame::Input { content } => json!({
            "v": BRIDGE_CONTRACT_VERSION,
            "type": "input",
            "content": content,
        }),
        ControlFrame::Steer { content } => json!({
            "v": BRIDGE_CONTRACT_VERSION,
            "type": "steer",
            "content": content,
        }),
        ControlFrame::Interrupt => json!({
            "v": BRIDGE_CONTRACT_VERSION,
            "type": "interrupt",
        }),
    };
    serde_json::to_string(&value).expect("control frame serializes infallibly")
}

/// The bridge session's slot facts, filled from the kian-side binding: the
/// `step/request` envelope names the slot target that governs the session
/// while `body` carries the peer's assembled request verbatim (ADR-010).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapCtx {
    pub slot: String,
    pub provider: String,
    pub model: String,
}

/// The result of mapping one dsh event: either a kian event (kind + the
/// frozen payload shape the built-in loop already emits), or an explicit
/// **absorbed** marker for the dsh episode envelope — reported, auditable,
/// never a silent drop.
#[derive(Debug, Clone, PartialEq)]
pub enum Mapped {
    Event {
        kind: EventKind,
        payload: Value,
    },
    /// `turn/start`/`turn/end`: the kian counterpart is the drain episode,
    /// whose events the orchestrator already owns. The bridge writes
    /// nothing for these; the marker exists so callers can audit the
    /// absorption.
    Absorbed {
        kind: &'static str,
    },
}

/// The stateful mapping: tracks step boundaries (dsh steps map onto kian
/// provider turns), the per-step request-bytes witness (the pre-flight
/// rule), and the iteration counter (derived from emitted `turn/start`s,
/// exactly as the built-in loop derives it from the stream).
#[derive(Debug, Clone)]
pub struct DshMapper {
    ctx: MapCtx,
    /// How many `turn/start`s have been emitted (the next step's index).
    steps_emitted: u64,
    /// The currently open step's iteration index, if any.
    open_iteration: Option<u64>,
    /// Whether the open step's `assistant/attempt` (request bytes) has
    /// been mapped — required before any response frame is accepted.
    request_bytes_seen: bool,
}

impl DshMapper {
    pub fn new(ctx: MapCtx) -> Self {
        Self {
            ctx,
            steps_emitted: 0,
            open_iteration: None,
            request_bytes_seen: false,
        }
    }

    /// The slot facts this mapper stamps into `step/request` envelopes.
    pub fn ctx(&self) -> &MapCtx {
        &self.ctx
    }

    /// Maps one logical dsh event. Errors are the honest contract errors
    /// of ADR-010; on error the bridge stops — nothing is buffered, so no
    /// partial write can follow.
    pub fn map(&mut self, ev: &DshEvent) -> Result<Mapped, ContractError> {
        match ev.kind.as_str() {
            "user/message" => self.map_input(ev, "user"),
            "system/message" => self.map_input(ev, "system"),
            "assistant/attempt" => self.map_attempt(ev),
            "assistant/message" => self.map_message(ev),
            "tool/call" => self.map_tool_call(ev),
            "tool/result" => self.map_tool_result(ev),
            "step/start" => self.map_step_start(ev),
            "step/end" => self.map_step_end(ev),
            // The dsh episode envelope: absorbed into the drain episode's
            // own events (which the orchestrator owns), explicitly.
            "turn/start" => Ok(Mapped::Absorbed { kind: "turn/start" }),
            "turn/end" => Ok(Mapped::Absorbed { kind: "turn/end" }),
            other => Err(ContractError::UnknownKind {
                kind: other.to_owned(),
            }),
        }
    }

    fn require_obj<'a>(&self, ev: &'a DshEvent) -> Result<&'a Map<String, Value>, ContractError> {
        ev.payload.as_object().ok_or(ContractError::MalformedEvent {
            kind: ev.kind.clone(),
            reason: "payload must be a JSON object",
        })
    }

    fn field<'m>(
        &self,
        obj: &'m Map<String, Value>,
        ev: &DshEvent,
        name: &'static str,
    ) -> Result<&'m Value, ContractError> {
        obj.get(name).ok_or(ContractError::MalformedEvent {
            kind: ev.kind.clone(),
            reason: name,
        })
    }

    fn str_field<'m>(
        &self,
        obj: &'m Map<String, Value>,
        ev: &DshEvent,
        name: &'static str,
    ) -> Result<&'m str, ContractError> {
        self.field(obj, ev, name)?
            .as_str()
            .ok_or(ContractError::MalformedEvent {
                kind: ev.kind.clone(),
                reason: name,
            })
    }

    /// `user/message` / `system/message` → `input/admitted` with the
    /// frozen message payload (`role` any non-empty string — the C4
    /// contract the closest call #1 relies on). A `synthetic: true`
    /// marker on the dsh event (an injected steer message) rides through
    /// so replay can strip it (A6).
    fn map_input(&self, ev: &DshEvent, role: &str) -> Result<Mapped, ContractError> {
        let obj = self.require_obj(ev)?;
        let content = self.field(obj, ev, "content")?.clone();
        let mut payload = json!({ "role": role, "content": content });
        match obj.get("synthetic") {
            None => {}
            Some(v) => {
                let flag = v.as_bool().ok_or(ContractError::MalformedEvent {
                    kind: ev.kind.clone(),
                    reason: "'synthetic' must be a boolean when present",
                })?;
                if flag {
                    payload["synthetic"] = json!(true);
                }
            }
        }
        Ok(Mapped::Event {
            kind: EventKind::InputAdmitted,
            payload,
        })
    }

    /// `assistant/attempt` → `step/request`: the assembled request bytes
    /// ride `body` verbatim (A2 across the process boundary). Requires an
    /// open step; the attempt's diagnostic `status` string is accepted but
    /// not mapped — the honest failure trace is structural (no following
    /// `assistant/message`), not a status field.
    fn map_attempt(&mut self, ev: &DshEvent) -> Result<Mapped, ContractError> {
        let iteration = self.open_iteration.ok_or(ContractError::StepNotOpen {
            kind: "assistant/attempt",
        })?;
        let obj = self.require_obj(ev)?;
        // Validate the shape, keep the bytes.
        let _status = self.str_field(obj, ev, "status")?;
        let request = self.field(obj, ev, "request")?.clone();
        self.request_bytes_seen = true;
        Ok(Mapped::Event {
            kind: EventKind::StepRequest,
            payload: json!({
                "slot": self.ctx.slot,
                "provider": self.ctx.provider,
                "model": self.ctx.model,
                "iteration": iteration,
                "body": request,
            }),
        })
    }

    /// `assistant/message` → `assistant/message`. **The pre-flight rule**:
    /// no response is accepted for a step whose request bytes were not
    /// received first — persist-what-you-send across the boundary.
    fn map_message(&mut self, ev: &DshEvent) -> Result<Mapped, ContractError> {
        self.require_open_step_with_request_bytes("assistant/message")?;
        let obj = self.require_obj(ev)?;
        let content = self.field(obj, ev, "content")?.clone();
        Ok(Mapped::Event {
            kind: EventKind::AssistantMessage,
            payload: json!({ "role": "assistant", "content": content }),
        })
    }

    /// `tool/call` → `tool/call` (the loop's log-before-execute shape).
    fn map_tool_call(&mut self, ev: &DshEvent) -> Result<Mapped, ContractError> {
        self.require_open_step_with_request_bytes("tool/call")?;
        let obj = self.require_obj(ev)?;
        let id = self.str_field(obj, ev, "id")?;
        let name = self.str_field(obj, ev, "name")?;
        let arguments = self.field(obj, ev, "arguments")?.clone();
        Ok(Mapped::Event {
            kind: EventKind::ToolCall,
            payload: json!({
                "role": "assistant",
                "content": { "id": id, "name": name, "arguments": arguments }
            }),
        })
    }

    /// `tool/result` → `tool/result`.
    fn map_tool_result(&mut self, ev: &DshEvent) -> Result<Mapped, ContractError> {
        self.require_open_step_with_request_bytes("tool/result")?;
        let obj = self.require_obj(ev)?;
        let call_id = self.str_field(obj, ev, "call_id")?;
        let output = self.field(obj, ev, "output")?.clone();
        let is_error =
            self.field(obj, ev, "is_error")?
                .as_bool()
                .ok_or(ContractError::MalformedEvent {
                    kind: ev.kind.clone(),
                    reason: "'is_error' must be a boolean",
                })?;
        Ok(Mapped::Event {
            kind: EventKind::ToolResult,
            payload: json!({
                "role": "tool",
                "content": { "call_id": call_id, "output": output, "is_error": is_error }
            }),
        })
    }

    /// `step/start` → `turn/start`: a dsh step (one model request plus its
    /// tools) is exactly a kian provider turn; `iteration` is the count of
    /// prior steps, derived here exactly as the loop derives it from the
    /// stream.
    fn map_step_start(&mut self, ev: &DshEvent) -> Result<Mapped, ContractError> {
        if self.open_iteration.is_some() {
            return Err(ContractError::StepAlreadyOpen);
        }
        self.require_obj(ev)?;
        let iteration = self.steps_emitted;
        self.steps_emitted += 1;
        self.open_iteration = Some(iteration);
        self.request_bytes_seen = false;
        Ok(Mapped::Event {
            kind: EventKind::TurnStart,
            payload: json!({ "iteration": iteration }),
        })
    }

    /// `step/end` → `turn/end` in the loop's payload shapes. `cancelled`
    /// is the interrupt-settled outcome — `interrupted`, never `completed`
    /// (constraint 7); the session-level end reason stays the
    /// orchestrator's end write.
    fn map_step_end(&mut self, ev: &DshEvent) -> Result<Mapped, ContractError> {
        let iteration = self
            .open_iteration
            .ok_or(ContractError::StepNotOpen { kind: "step/end" })?;
        let obj = self.require_obj(ev)?;
        let outcome = self.str_field(obj, ev, "outcome")?;
        let payload = match outcome {
            "tool_calls" => {
                let tool_calls = self.field(obj, ev, "tool_calls")?.as_u64().ok_or(
                    ContractError::MalformedEvent {
                        kind: ev.kind.clone(),
                        reason: "'tool_calls' must be a u64",
                    },
                )?;
                json!({ "iteration": iteration, "tool_calls": tool_calls })
            }
            "final" => json!({ "iteration": iteration, "outcome": "final_answer" }),
            "error" => json!({ "iteration": iteration, "outcome": "error" }),
            "cancelled" => json!({ "iteration": iteration, "outcome": "cancelled" }),
            _ => {
                return Err(ContractError::MalformedEvent {
                    kind: ev.kind.clone(),
                    reason: "unknown 'outcome' (use tool_calls|final|error|cancelled)",
                });
            }
        };
        self.open_iteration = None;
        self.request_bytes_seen = false;
        Ok(Mapped::Event {
            kind: EventKind::TurnEnd,
            payload,
        })
    }

    fn require_open_step_with_request_bytes(
        &self,
        kind: &'static str,
    ) -> Result<(), ContractError> {
        if self.open_iteration.is_none() {
            return Err(ContractError::StepNotOpen { kind });
        }
        if !self.request_bytes_seen {
            return Err(ContractError::ResponseBeforeRequestBytes { kind });
        }
        Ok(())
    }
}

/// The bridge-side half of a peer session: enforces the frame protocol
/// (ready-first, logical-format declaration) around [`DshMapper`]. This is
/// the piece C54's driver embeds; it performs no I/O — lines in, mapping
/// results out, honest errors instead of best-effort behavior.
#[derive(Debug, Clone)]
pub struct PeerSession {
    mapper: DshMapper,
    ready_seen: bool,
}

impl PeerSession {
    pub fn new(ctx: MapCtx) -> Self {
        Self {
            mapper: DshMapper::new(ctx),
            ready_seen: false,
        }
    }

    /// Accepts one wire line from the peer. `Ok(None)` covers the `ready`
    /// handshake and explicitly-absorbed episode events; every mapped kian
    /// event comes back as `Ok(Some(Mapped::Event { .. }))`, ready to ride
    /// the ordinary fenced append path (the drain stays the sole writer).
    pub fn accept_line(&mut self, line: &str) -> Result<Option<Mapped>, ContractError> {
        match parse_peer_frame(line)? {
            PeerFrame::Ready { logical_format } => {
                if self.ready_seen {
                    return Err(ContractError::DuplicateReady);
                }
                if logical_format != SUPPORTED_LOGICAL_FORMAT {
                    return Err(ContractError::ForeignLogicalFormat {
                        got: logical_format,
                        supported: SUPPORTED_LOGICAL_FORMAT,
                    });
                }
                self.ready_seen = true;
                Ok(None)
            }
            PeerFrame::SessionEvent(ev) => {
                if !self.ready_seen {
                    return Err(ContractError::ReadyNotFirst);
                }
                match self.mapper.map(&ev)? {
                    Mapped::Absorbed { .. } => Ok(None),
                    mapped => Ok(Some(mapped)),
                }
            }
        }
    }
}

//
// C54 driver accessors (additive; the mapping semantics are unchanged)
//
impl PeerSession {
    /// Whether a dsh step is currently open (a `step/start` without
    /// its `step/end`) — the driver's safe-boundary marker.
    #[must_use]
    pub fn step_open(&self) -> bool {
        self.mapper.open_iteration.is_some()
    }

    /// How many `turn/start`s this mapper has emitted (the next step's
    /// index) — the driver's step-budget counter piece.
    #[must_use]
    pub fn steps_emitted(&self) -> u64 {
        self.mapper.steps_emitted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dsh(kind: &str, payload: Value) -> DshEvent {
        DshEvent {
            kind: kind.to_owned(),
            payload,
            ts: 1_761_900_000_000,
        }
    }

    fn ctx() -> MapCtx {
        MapCtx {
            slot: "main".into(),
            provider: "openai-compat".into(),
            model: "test-model".into(),
        }
    }

    #[test]
    fn control_frames_serialize_to_their_exact_wire_lines() {
        assert_eq!(
            control_line(&ControlFrame::Input {
                content: json!({"text": "deploy"})
            }),
            r#"{"content":{"text":"deploy"},"type":"input","v":0}"#
        );
        assert_eq!(
            control_line(&ControlFrame::Steer {
                content: json!("plan B")
            }),
            r#"{"content":"plan B","type":"steer","v":0}"#
        );
        assert_eq!(
            control_line(&ControlFrame::Interrupt),
            r#"{"type":"interrupt","v":0}"#
        );
    }

    #[test]
    fn frame_parsing_rejects_version_and_shape_drift_honestly() {
        assert_eq!(
            parse_peer_frame(r#"{"v":1,"type":"ready","logical_format":0}"#),
            Err(ContractError::UnsupportedVersion {
                got: 1,
                supported: 0
            })
        );
        assert_eq!(
            parse_peer_frame(r#"{"v":0,"type":"mystery"}"#),
            Err(ContractError::UnknownFrame {
                frame_type: "mystery".into(),
                supported: 0
            })
        );
        assert_eq!(
            parse_peer_frame("not json"),
            Err(ContractError::MalformedFrame {
                reason: "not valid JSON"
            })
        );
        // A well-formed frame round-trips.
        let frame = parse_peer_frame(
            r#"{"v":0,"type":"session-event","event":{"kind":"user/message","payload":{"content":"hi"},"ts":7}}"#,
        )
        .expect("valid frame");
        assert_eq!(
            frame,
            PeerFrame::SessionEvent(DshEvent {
                kind: "user/message".into(),
                payload: json!({"content": "hi"}),
                ts: 7,
            })
        );
    }

    #[test]
    fn session_enforces_ready_first_and_the_logical_format_gate() {
        let mut session = PeerSession::new(ctx());
        // session-event before ready → honest error.
        assert_eq!(
            session.accept_line(
                r#"{"v":0,"type":"session-event","event":{"kind":"user/message","payload":{},"ts":1}}"#
            ),
            Err(ContractError::ReadyNotFirst)
        );
        // Foreign logical format → refused before any mapping happens.
        assert_eq!(
            session.accept_line(r#"{"v":0,"type":"ready","logical_format":2}"#),
            Err(ContractError::ForeignLogicalFormat {
                got: 2,
                supported: 0
            })
        );
        // Supported handshake maps nothing and admits events.
        assert_eq!(
            session.accept_line(r#"{"v":0,"type":"ready","logical_format":0}"#),
            Ok(None)
        );
        assert_eq!(
            session.accept_line(r#"{"v":0,"type":"ready","logical_format":0}"#),
            Err(ContractError::DuplicateReady)
        );
    }

    #[test]
    fn episode_envelope_events_are_absorbed_explicitly_not_silently() {
        let mut mapper = DshMapper::new(ctx());
        assert_eq!(
            mapper.map(&dsh("turn/start", json!({}))),
            Ok(Mapped::Absorbed { kind: "turn/start" })
        );
        assert_eq!(
            mapper.map(&dsh("turn/end", json!({}))),
            Ok(Mapped::Absorbed { kind: "turn/end" })
        );
    }

    #[test]
    fn unknown_dsh_kinds_are_an_honest_error_never_a_drop() {
        let mut mapper = DshMapper::new(ctx());
        // request/header and request/context are real dsh durable kinds
        // that are outside contract v0 — the error is the extension point.
        for kind in ["request/header", "request/context", "bogus/thing"] {
            assert_eq!(
                mapper.map(&dsh(kind, json!({}))),
                Err(ContractError::UnknownKind {
                    kind: kind.to_owned()
                })
            );
        }
    }
}
