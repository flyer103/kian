//! The answer surface: mapping an inbound IM message onto a pending
//! `ask` (CONFIRMATION-MODEL §14d, phase 3a).
//!
//! An `ask` parks a session in `awaiting_input` and waits for a
//! decision that, until this module, only a CLI/HTTP client could give.
//! The answer surface closes that loop for one channel class: a message
//! whose **entire** normalized text is a closed command (`approve
//! <code>` / `reject <code> [reason]`) is intercepted **at the adapter
//! intake**, before the rule set and before any model call, and written
//! as a `control/confirm` row through the assembly's
//! [`AnswerPort`] — the assembly's fenced confirmation writer,
//! CONFIRMATION-MODEL §5 A2's one write path).
//!
//! Three properties hold by construction:
//!
//! - **Authority is the sender, never the code.** Channel allowlisting
//!   (`superagent.channels.<name>.topics` for iMessage) decides *who*
//!   may speak at all, before an event exists; the code is a
//!   correlation token that names *which* pending call, not a secret.
//! - **The model never parses an answer.** A command-shaped message
//!   never becomes an inbox row, so no prompt can reinterpret it; a
//!   message that merely *starts* like a command (`approve` alone) is
//!   ordinary input, and the Router is instructed (§14d X5) to never
//!   claim a confirmation happened.
//! - **The code is a pure function of the committed stream**
//!   ([`code_for`]): session id + `tool/call` seq + call id, hashed.
//!   No storage, no expiry bookkeeping, and a replay derives the same
//!   code — a retried message is idempotent at the confirmation plane
//!   by the existing `(session, call_id)` key.
//!
//! Nothing here can *fabricate* a decision: [`AnswerPort`] is the same
//! write the CLI issues, subject to the same pending-set check, the same
//! TTL rewrite, and the same `not_pending` answer for a racing decision.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::future::BoxFuture;
use kian_core::confirmation::{PendingCall, is_expired, pending_calls};
use kian_core::control::ConfirmDecision;
use kian_core::model::SessionId;
use kian_core::seams::{ChannelEvent, ToolCall, ToolResult, ToolSchema};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::recall::{RecallPort, RecallPortError};

/// The channel types the answer surface may be mounted on (§14d X7):
/// the text-first adapters whose inbound payload carries one plain-text
/// message. Phase 3a's closed set — a channel type not listed here
/// refuses the configuration rather than silently answering nothing.
pub const ANSWER_CHANNEL_TYPES: [&str; 2] = ["imessage", "mock-im"];

/// The command code length: 30 bits of the derivation below. Short
/// enough to retype on a phone keyboard, long enough that a *random*
/// six-character token cannot collide with a live pending set.
pub const ANSWER_CODE_LEN: usize = 6;

/// The base32-lower alphabet the code is spelled in (`[0-9a-z]`, no
/// vowel-free confusion with the verb tokens).
const CODE_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

/// The domain-separation prefix of the code derivation (§14d X6).
/// Changing it re-keys every outstanding code — a deliberate contract
/// edit, never a refactor.
const CODE_DOMAIN: &str = "kian-confirm-v1";

/// The reject reason is audit-only (§14d X2): it rides the
/// `control/confirm` payload and never reaches a model request. Capped
/// so a pasted log cannot bloat the stream.
pub const ANSWER_REASON_CAP: usize = 280;

/// The approve verbs (§14d X2, closed set, matched after
/// normalization).
const APPROVE_VERBS: [&str; 5] = ["approve", "ok", "yes", "同意", "批准"];

/// The reject verbs (same closed set discipline).
const REJECT_VERBS: [&str; 4] = ["reject", "no", "拒绝", "驳回"];

/// One parsed answer command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnswerCommand {
    /// What the sender decided.
    pub decision: ConfirmDecision,
    /// The correlation token naming the pending call.
    pub code: String,
    /// Optional audit note (only the reject path renders it).
    pub reason: Option<String>,
}

/// The closed parse outcome of one inbound message (§14d X5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseOutcome {
    /// A complete command: a closed verb plus a code-shaped second
    /// token. Intercepted — never admitted.
    Command(AnswerCommand),
    /// Ordinary input, which includes a bare verb and a verb followed
    /// by a token that is not code-shaped: the person is talking, and
    /// dropping their words would lose real work (§14d X5). The Router
    /// never treats such a message as a decision -- it may only check.
    None,
}

/// Whether `text` is one complete answer command.
#[must_use]
pub fn parse_answer(text: &str) -> ParseOutcome {
    let normalized = normalize(text);
    let mut tokens = normalized.split(' ').filter(|token| !token.is_empty());
    let Some(verb) = tokens.next() else {
        return ParseOutcome::None;
    };
    let Some(decision) = verb_decision(verb) else {
        return ParseOutcome::None;
    };
    let Some(code) = tokens.next() else {
        // A bare verb is ordinary input: people type "ok" constantly,
        // and §14d X5 makes the absence of a code mean "not a command".
        return ParseOutcome::None;
    };
    if !is_code_shaped(code) {
        // A verb followed by a word is prose ("approve it, but change
        // the path first"): ordinary input, never a decision, and the
        // Router is told to treat it as a request to check (§14d X5).
        return ParseOutcome::None;
    }
    let rest: Vec<&str> = tokens.collect();
    let reason = if rest.is_empty() {
        None
    } else {
        Some(cap_reason(&rest.join(" ")))
    };
    ParseOutcome::Command(AnswerCommand {
        decision,
        code: code.to_string(),
        reason,
    })
}

/// The text payload of a channel event, when it carries one (both IM
/// adapters spell it `payload.text`).
#[must_use]
pub fn message_text(event: &ChannelEvent) -> Option<&str> {
    event.payload.get("text").and_then(Value::as_str)
}

/// The correlation code of one pending call (§14d X6): a pure function
/// of the committed stream — `base32_lower(sha256("kian-confirm-v1|"
/// + session + "|" + call_seq + "|" + call_id))[..6]`.
#[must_use]
pub fn code_for(session: SessionId, call: &PendingCall) -> String {
    let mut hasher = Sha256::new();
    hasher.update(CODE_DOMAIN.as_bytes());
    hasher.update(b"|");
    hasher.update(session.0.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(call.call_seq.to_string().as_bytes());
    hasher.update(b"|");
    hasher.update(call.call_id.as_bytes());
    let digest = hasher.finalize();
    let mut spelled = base32_lower(&digest);
    spelled.truncate(ANSWER_CODE_LEN);
    spelled
}

/// What one channel may do on the answer surface.
pub struct AnswerSurface {
    channels: BTreeSet<String>,
    read: Arc<dyn RecallPort>,
    answer: Arc<dyn AnswerPort>,
    ttl_ms: u64,
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl AnswerSurface {
    /// Builds a surface over the given channel names, the read port
    /// (the same one the recall toolpack uses) and the confirmation
    /// write port. `ttl_ms` mirrors `confirmation.ttl_ms`: the surface
    /// never applies it itself — the engine rewrites a lapsed answer
    /// into a durable reject (§5 A6) — it only reports expiry in the
    /// pending listing.
    #[must_use]
    pub fn new(
        channels: BTreeSet<String>,
        read: Arc<dyn RecallPort>,
        answer: Arc<dyn AnswerPort>,
        ttl_ms: u64,
    ) -> Self {
        Self {
            channels,
            read,
            answer,
            ttl_ms,
            now_ms: Arc::new(system_now_ms),
        }
    }

    /// Replaces the clock (tests only: deterministic expiry overlays).
    #[must_use]
    pub fn with_clock(mut self, now_ms: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        self.now_ms = now_ms;
        self
    }

    /// Whether `channel` opted into the answer surface.
    #[must_use]
    pub fn covers(&self, channel: &str) -> bool {
        self.channels.contains(channel)
    }

    /// The configured channel set (inspection/tests).
    #[must_use]
    pub fn channels(&self) -> &BTreeSet<String> {
        &self.channels
    }

    /// The pending call ids of `worker` whose code is `code`, in call
    /// order. Zero is the common case; two or more can only be a
    /// derivation collision, which the caller treats as ambiguous and
    /// drops.
    pub async fn matches(
        &self,
        worker: SessionId,
        code: &str,
    ) -> Result<Vec<String>, RecallPortError> {
        let events = self.read.read_events(worker, 0, u64::MAX).await?;
        Ok(pending_calls(&events)
            .iter()
            .filter(|call| code_for(worker, call) == code)
            .map(|call| call.call_id.clone())
            .collect())
    }

    /// Submits the decision through the confirmation plane.
    pub async fn submit(
        &self,
        session: SessionId,
        call_id: String,
        decision: ConfirmDecision,
        reason: Option<String>,
        requested_by: String,
    ) -> Result<AnswerOutcome, AnswerError> {
        self.answer
            .answer(session, call_id, decision, reason, requested_by)
            .await
    }

    /// The configured TTL (`confirmation.ttl_ms`), for the pending
    /// listing's `expired` overlay.
    #[must_use]
    pub fn ttl_ms(&self) -> u64 {
        self.ttl_ms
    }

    /// The surface clock, milliseconds since the epoch.
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        (self.now_ms)()
    }
}

/// What one [`AnswerPort::answer`] call did (the assembly maps
/// the assembly's confirmation outcome onto this closed set).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnswerOutcome {
    /// The answer row is durable; the decision takes effect on the
    /// stream.
    Accepted,
    /// No unanswered ask with that `call_id` (already answered,
    /// unknown, not an ask, or the session is terminal).
    NotPending,
    /// The call lapsed under the TTL: a reject-with-`expired` row was
    /// admitted; the sender's decision was **not** applied.
    Expired,
    /// No such session.
    UnknownSession,
}

impl AnswerOutcome {
    /// The stable machine spelling used in audit lines.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::NotPending => "not_pending",
            Self::Expired => "expired",
            Self::UnknownSession => "unknown_session",
        }
    }
}

/// A failed confirmation write (transport/engine trouble — a *refused*
/// answer is an [`AnswerOutcome`], not an error).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AnswerError {
    #[error("confirmation write failed: {0}")]
    Write(String),
}

/// The bundle's confirmation-write port. Assembly implements it over
/// the orchestrator's fenced confirmation write; this crate never names an
/// execution API (iron rule).
pub trait AnswerPort: Send + Sync {
    /// Answers one pending call. `requested_by` records the provenance
    /// of the answer (`{channel}:{sender}:{event_id}`), never the
    /// message text.
    fn answer(
        &self,
        session: SessionId,
        call_id: String,
        decision: ConfirmDecision,
        reason: Option<String>,
        requested_by: String,
    ) -> BoxFuture<'static, Result<AnswerOutcome, AnswerError>>;
}

/// The Router's read-only view of the pending decisions a channel can
/// answer (§14d X7): without it the operator cannot learn the code.
pub struct PendingDecisionTools {
    read: Arc<dyn RecallPort>,
    /// The live Router slot, read at call time: the resident Router may
    /// rotate (ADR-040), and a listing bound to a stale session would
    /// silently report no pending decisions.
    router: Arc<crate::binding::ResidentBinding>,
    ttl_ms: u64,
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
}

/// The one tool this surface mounts.
pub const PENDING_DECISIONS_TOOL: &str = "pending_decisions";

/// How many of the Router's most recent threads the listing reads.
/// A pending ask parks its worker, so a decision that has been waiting
/// longest may sit deeper in the list than this bound — the tool says
/// so honestly (`truncated`) rather than silently hiding it.
pub const PENDING_SCAN_THREADS: usize = 16;

/// The arguments preview cap of one listed decision.
const ARGUMENTS_PREVIEW_CAP: usize = 200;

impl PendingDecisionTools {
    /// Builds the tool over the read port and the Router session whose
    /// threads own the workers.
    #[must_use]
    pub fn new(
        read: Arc<dyn RecallPort>,
        router: Arc<crate::binding::ResidentBinding>,
        ttl_ms: u64,
    ) -> Self {
        Self {
            read,
            router,
            ttl_ms,
            now_ms: Arc::new(system_now_ms),
        }
    }

    /// Replaces the clock (tests only).
    #[must_use]
    pub fn with_clock(mut self, now_ms: Arc<dyn Fn() -> u64 + Send + Sync>) -> Self {
        self.now_ms = now_ms;
        self
    }

    async fn list(&self, call_id: &str) -> Result<ToolResult, kian_core::seams::ToolError> {
        let threads = match self.read.list_threads(self.router.load()).await {
            Ok(threads) => threads,
            Err(error) => {
                return Ok(ToolResult {
                    call_id: call_id.to_string(),
                    output: json!({"error": error.to_string()}),
                    is_error: true,
                    redactions: Vec::new(),
                });
            }
        };
        let truncated = threads.len() > PENDING_SCAN_THREADS;
        let scan: Vec<(String, SessionId)> = threads
            .iter()
            .rev()
            .take(PENDING_SCAN_THREADS)
            .cloned()
            .collect();
        let mut pending: Vec<Value> = Vec::new();
        for (thread_id, worker) in scan {
            let events = match self.read.read_events(worker, 0, u64::MAX).await {
                Ok(events) => events,
                // One unreadable thread must not hide the others: the
                // entry records the gap honestly.
                Err(error) => {
                    pending.push(json!({
                        "thread_id": thread_id,
                        "worker_session": worker.0.to_string(),
                        "error": error.to_string(),
                    }));
                    continue;
                }
            };
            let now = (self.now_ms)();
            for call in pending_calls(&events) {
                pending.push(json!({
                    "thread_id": thread_id,
                    "worker_session": worker.0.to_string(),
                    "call_id": call.call_id,
                    "call_seq": call.call_seq,
                    "code": code_for(worker, &call),
                    "tool": call.tool,
                    "requested_at_ms": call.requested_at_ms,
                    "expired": is_expired(&call, self.ttl_ms, now),
                    "arguments_preview": preview(&call.arguments, ARGUMENTS_PREVIEW_CAP),
                }));
            }
        }
        Ok(ToolResult {
            call_id: call_id.to_string(),
            output: json!({
                "pending": pending,
                "scanned_threads": PENDING_SCAN_THREADS,
                "truncated": truncated,
                "answer_hint": "Reply on the channel with `approve <code>` or \
                    `reject <code> [reason]` — exactly one of those forms, nothing else.",
            }),
            is_error: false,
            redactions: Vec::new(),
        })
    }
}

impl kian_core::seams::ToolProvider for PendingDecisionTools {
    fn schemas(&self) -> Vec<ToolSchema> {
        vec![ToolSchema {
            name: PENDING_DECISIONS_TOOL.to_string(),
            description: "List the tool calls of your live workers that are waiting for a \
                human decision, each with the short code the human replies with. Use it when \
                the human asks what is waiting or asks for a code."
                .into(),
            parameters: json!({"type": "object", "properties": {}}),
        }]
    }

    fn invoke(
        &self,
        call: ToolCall,
    ) -> BoxFuture<'static, Result<ToolResult, kian_core::seams::ToolError>> {
        let this = self.clone();
        Box::pin(async move {
            if call.name != PENDING_DECISIONS_TOOL {
                return Err(kian_core::seams::ToolError::InvalidCall(format!(
                    "unknown tool `{}`",
                    call.name
                )));
            }
            this.list(&call.id).await
        })
    }
}

impl Clone for PendingDecisionTools {
    fn clone(&self) -> Self {
        Self {
            read: Arc::clone(&self.read),
            router: Arc::clone(&self.router),
            ttl_ms: self.ttl_ms,
            now_ms: Arc::clone(&self.now_ms),
        }
    }
}

/// Folds the compatibility forms IM input methods actually produce
/// (§14d X2): full-width ASCII (`U+FF01..=U+FF5E`) and the
/// ideographic space become their ASCII spelling, the text is
/// lower-cased, and whitespace runs collapse to one `' '`. In-tree
/// approximation of NFKC restricted to those blocks — no table-driven
/// normalization dependency rides the bundle for one grammar.
#[must_use]
pub fn normalize(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pending_space = false;
    for ch in text.chars() {
        let folded = match ch {
            '\u{3000}' => ' ',
            '\u{FF01}'..='\u{FF5E}' => char::from_u32(u32::from(ch) - 0xFEE0).unwrap_or(ch),
            _ => ch,
        };
        for lower in folded.to_lowercase() {
            if lower.is_whitespace() {
                pending_space = !out.is_empty();
                continue;
            }
            if pending_space {
                out.push(' ');
                pending_space = false;
            }
            out.push(lower);
        }
    }
    out
}

fn verb_decision(verb: &str) -> Option<ConfirmDecision> {
    if APPROVE_VERBS.contains(&verb) {
        return Some(ConfirmDecision::Approve);
    }
    if REJECT_VERBS.contains(&verb) {
        return Some(ConfirmDecision::Reject);
    }
    None
}

fn is_code_shaped(token: &str) -> bool {
    // Exactly the derivation's alphabet: a token carrying `0`, `1`,
    // `8`, `9` or punctuation cannot be a code, and is prose instead —
    // treated as ordinary input, never as a command that names nothing.
    token.len() == ANSWER_CODE_LEN && token.bytes().all(|b| CODE_ALPHABET.contains(&b))
}

fn cap_reason(reason: &str) -> String {
    let mut capped: String = reason.chars().take(ANSWER_REASON_CAP).collect();
    if reason.chars().count() > ANSWER_REASON_CAP {
        capped.push('…');
    }
    capped
}

fn preview(value: &Value, cap: usize) -> String {
    let rendered = value.to_string();
    if rendered.chars().count() <= cap {
        return rendered;
    }
    let mut out: String = rendered.chars().take(cap).collect();
    out.push('…');
    out
}

fn base32_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 8 / 5 + 1);
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    for byte in bytes {
        acc = (acc << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            let index = ((acc >> bits) & 0x1f) as usize;
            out.push(char::from(CODE_ALPHABET[index]));
        }
    }
    if bits > 0 {
        let index = ((acc << (5 - bits)) & 0x1f) as usize;
        out.push(char::from(CODE_ALPHABET[index]));
    }
    out
}

fn system_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use kian_core::control::ConfirmDecision;
    use kian_core::model::SessionId;
    use kian_core::seams::ChannelEvent;
    use serde_json::json;
    use uuid::Uuid;

    use super::{
        ANSWER_CODE_LEN, ANSWER_REASON_CAP, AnswerCommand, ParseOutcome, code_for, message_text,
        normalize, parse_answer,
    };

    fn call(seq: u64, id: &str) -> kian_core::confirmation::PendingCall {
        kian_core::confirmation::PendingCall {
            call_id: id.to_string(),
            tool: "exec".into(),
            arguments: json!({"argv": ["ls"]}),
            call_seq: seq,
            requested_at_ms: 0,
        }
    }

    fn command(decision: ConfirmDecision, code: &str, reason: Option<&str>) -> ParseOutcome {
        ParseOutcome::Command(AnswerCommand {
            decision,
            code: code.to_string(),
            reason: reason.map(ToString::to_string),
        })
    }

    #[test]
    fn normalize_folds_full_width_and_collapses_whitespace() {
        assert_eq!(
            normalize("  ＡＰＰＲＯＶＥ\u{3000} abc234  "),
            "approve abc234"
        );
        assert_eq!(
            normalize("Reject\tABC123\nnot ready"),
            "reject abc123 not ready"
        );
        assert_eq!(normalize(""), "");
        assert_eq!(normalize("   "), "");
    }

    #[test]
    fn parse_accepts_the_closed_commands() {
        assert_eq!(
            parse_answer("approve abc234"),
            command(ConfirmDecision::Approve, "abc234", None)
        );
        assert_eq!(
            parse_answer("OK   234567"),
            command(ConfirmDecision::Approve, "234567", None)
        );
        assert_eq!(
            parse_answer("同意 abc234"),
            command(ConfirmDecision::Approve, "abc234", None)
        );
        assert_eq!(
            parse_answer("reject abc234 数据库没备份"),
            command(ConfirmDecision::Reject, "abc234", Some("数据库没备份"))
        );
        assert_eq!(
            parse_answer("驳回 234567"),
            command(ConfirmDecision::Reject, "234567", None)
        );
    }

    #[test]
    fn parse_keeps_ordinary_input_out_of_the_command_grammar() {
        // A bare verb (no second token) is ordinary input, never a
        // command: people type "ok" constantly.
        assert_eq!(parse_answer("ok"), ParseOutcome::None);
        assert_eq!(parse_answer("approve"), ParseOutcome::None);
        assert_eq!(
            parse_answer("please approve abc234 for me"),
            ParseOutcome::None
        );
        assert_eq!(parse_answer("昨天那个部署 ok 吗"), ParseOutcome::None);
        assert_eq!(parse_answer(""), ParseOutcome::None);
    }

    #[test]
    fn a_verb_followed_by_prose_stays_ordinary_input() {
        // §14d X5: a verb plus a word is a person talking, not an
        // answer — dropping it would lose real work. Only a token from
        // the derivation's alphabet can be a command.
        assert_eq!(parse_answer("approve abcdefg"), ParseOutcome::None);
        assert_eq!(parse_answer("reject 12"), ParseOutcome::None);
        assert_eq!(parse_answer("approve abc-12"), ParseOutcome::None);
        // `1` is outside the base32-lower alphabet.
        assert_eq!(parse_answer("approve abc123"), ParseOutcome::None);
        assert_eq!(
            parse_answer("approve 但先把目标路径改一下"),
            ParseOutcome::None
        );
    }

    #[test]
    fn reason_is_capped() {
        let long = "x".repeat(ANSWER_REASON_CAP + 10);
        match parse_answer(&format!("reject abc234 {long}")) {
            ParseOutcome::Command(command) => {
                let reason = command.reason.expect("reason");
                assert_eq!(reason.chars().count(), ANSWER_REASON_CAP + 1);
                assert!(reason.ends_with('…'));
            }
            other => panic!("expected a command, got {other:?}"),
        }
    }

    #[test]
    fn code_is_stable_shaped_and_stream_derived() {
        let session = SessionId(Uuid::from_bytes([7; 16]));
        let first = code_for(session, &call(3, "call-1"));
        assert_eq!(first.len(), ANSWER_CODE_LEN);
        assert!(
            first
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        );
        // Pure: the same committed stream derives the same code.
        assert_eq!(first, code_for(session, &call(3, "call-1")));
        // Each input moves the code.
        assert_ne!(first, code_for(session, &call(4, "call-1")));
        assert_ne!(first, code_for(session, &call(3, "call-2")));
        assert_ne!(
            first,
            code_for(SessionId(Uuid::from_bytes([8; 16])), &call(3, "call-1"))
        );
        // Golden value: the derivation is a contract, not an
        // implementation detail — a change here re-keys every
        // outstanding code (§14d X6).
        assert_eq!(first, "qoauds");
    }

    #[test]
    fn message_text_reads_the_im_payload() {
        let event = ChannelEvent {
            channel: "mock-im".into(),
            event_id: "e1".into(),
            topic: Some("phone".into()),
            sender: None,
            group: None,
            payload: json!({"text": "approve abc234"}),
            ts_ms: 1,
        };
        assert_eq!(message_text(&event), Some("approve abc234"));
        let empty = ChannelEvent {
            payload: json!({"message": "x"}),
            ..event
        };
        assert_eq!(message_text(&empty), None);
    }
}
