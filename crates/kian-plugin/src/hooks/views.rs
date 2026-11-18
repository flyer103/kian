//! The §3.2 payload catalog (closed) — plain serde structs, one per hook
//! id, with field names, order and nesting exactly per the §3.2 field
//! tables. Serialization is byte-checked against the spec examples (K2):
//! a field-order drift is a silent ABI break for the sidecar tier.
//!
//! Each view carries an in-process `frame: Option<FrameInfo>` that is
//! `#[serde(skip)]`d: it is the dispatch identity (§1 "Dispatch", §7.3
//! frame metadata — `session`, `worker`, `seq`, and `epoch` for
//! drain-phase hooks only) the bus needs for scope filtering and audit,
//! and that the sidecar frame adds on the wire, so it must never ride the
//! payload itself.

use serde::Serialize;

use kian_core::model::SessionId;

/// In-process dispatch identity (§1 "Dispatch", §7.3). On the wire these
/// are frame keys, not payload keys, which is why the view field is
/// `#[serde(skip)]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameInfo {
    /// The **target** session of this dispatch (§7.2): the one drain's
    /// session for a drain-phase hook, the one target session for an
    /// `admit/pre` frame.
    pub session: SessionId,
    /// That target's durable `exec_worker` id; the empty string when the
    /// session has no binding (§7.3).
    pub worker: String,
    /// That target's last observed seq at consultation; `0` when the
    /// session has no event yet (§7.3).
    pub seq: u64,
    /// The fencing epoch of the drain that consulted — present for
    /// drain-phase hooks only (§7.2); `None` for `admit/*`.
    pub epoch: Option<u64>,
}

impl FrameInfo {
    /// A frame for `session` with no worker binding and no events yet
    /// (§7.3's floor values).
    pub fn new(session: SessionId) -> Self {
        Self {
            session,
            worker: String::new(),
            seq: 0,
            epoch: None,
        }
    }
}

/// The durable execution-worker binding key — **one spelling on every
/// surface** (the delegation wrapper, the `thread/started` payload, the
/// `spawn_worker` argument, the spawn result echo; §7.3/§8.2). The
/// binding grammar and the routing-layer refusal rules live in
/// `kian-superagent` (ADR-020 D1/D2); this crate owns only what frame
/// identity needs — the key, and the stream-derived reader below.
/// `kian-superagent` re-exports this constant, so the two cannot drift.
pub const EXEC_WORKER_KEY: &str = "exec_worker";

fn binding_of_value(value: Option<&serde_json::Value>) -> Option<&str> {
    match value {
        Some(serde_json::Value::String(id)) if !id.is_empty() => Some(id.as_str()),
        _ => None,
    }
}

/// The session's durable `exec_worker` binding, derived from its own
/// stream alone (§7.3's `worker` field, §8.2's resolve input): the
/// `thread/started` payload naming this session as the worker and the
/// **first** `input/admitted` delegation wrapper must agree on the id.
/// `Some(id)` when the two readable ids agree, or when exactly one
/// side names one; `None` when neither names one or the two readable
/// ids disagree. A present-but-unreadable value (non-string, empty)
/// reads as unnamed — so a malformed payload key plus a valid wrapper
/// resolves to the wrapper's id (the §7.3 floor picks rather than
/// refuses), where the stricter routing-layer reading
/// (`kian_superagent::worker_binding_of`) returns `Malformed` and
/// refuses. The two agree on every Bound and Unbound stream; the
/// relationship — including the malformed-input divergence — is
/// pinned cross-crate in `kian-superagent`'s tests (n1).
pub fn durable_worker_binding(
    events: &[kian_core::events::Event],
    session: &SessionId,
) -> Option<String> {
    let started = events.iter().find(|ev| {
        ev.kind == kian_core::events::EventKind::ThreadStarted
            && ev.bytes.get("worker").and_then(serde_json::Value::as_str)
                == Some(session.0.to_string().as_str())
    });
    let payload = started.and_then(|ev| binding_of_value(ev.bytes.get(EXEC_WORKER_KEY)));
    let wrapper = events
        .iter()
        .find(|ev| ev.kind == kian_core::events::EventKind::InputAdmitted)
        .and_then(|ev| ev.bytes.get("content"))
        .and_then(|content| binding_of_value(content.get(EXEC_WORKER_KEY)));
    match (payload, wrapper) {
        (Some(a), Some(b)) if a == b => Some(a.to_string()),
        (Some(a), None) | (None, Some(a)) => Some(a.to_string()),
        _ => None,
    }
}

/// Which ingress produced an admission (§3.2.1 `origin`); decides veto
/// availability (§3.4.3 — only the channel-borne port is veto-capable in
/// v1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    Channel,
    Api,
}

impl Origin {
    pub fn as_str(self) -> &'static str {
        match self {
            Origin::Channel => "channel",
            Origin::Api => "api",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "channel" => Some(Origin::Channel),
            "api" => Some(Origin::Api),
            _ => None,
        }
    }
}

impl Serialize for Origin {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// One fan-out target of an admission evaluation (§3.2.1 `targets`).
#[derive(Debug, Clone, Serialize)]
pub struct AdmitTarget {
    pub session: SessionId,
    /// Whether this target is the fan-out's Router session.
    pub router: bool,
}

/// The shaped input an admission would carry (§3.2.1 `input`).
#[derive(Debug, Clone, Serialize)]
pub struct AdmitInput {
    pub role: String,
    /// True for steer-injected content (strippable, EVENT-MODEL A6).
    pub synthetic: bool,
    /// Redacted, possibly truncated head of the content, ≤4096 bytes.
    pub content_preview: String,
    /// Exact serialized content length.
    pub content_bytes: u64,
    /// Digest of the **unredacted** serialized content (correlation only).
    pub content_sha256: String,
    /// True when the preview is shorter than the content.
    pub content_truncated: bool,
    /// True when the row will carry only an Inline view (over-cap spill).
    pub inline_view: bool,
}

/// Channel provenance for `origin:"channel"`; `null` for `"api"`
/// (§3.2.1 `channel`).
#[derive(Debug, Clone, Serialize)]
pub struct ChannelProvenance {
    pub name: String,
    pub event_id: String,
    pub topic: Option<String>,
}

/// The shaping that will be applied (§3.2.1 `admission`).
#[derive(Debug, Clone, Serialize)]
pub struct AdmissionInfo {
    pub policy: String,
    pub spilled: bool,
}

/// `admit/pre` payload (§3.2.1). One view per inbound event — the complete
/// fan-out set rides in `targets`; the evaluation is one, the frames are
/// per target (§4.1).
#[derive(Debug, Clone, Serialize)]
pub struct AdmitDraft {
    pub origin: Origin,
    /// The complete fan-out set this one evaluation covers, sorted by
    /// session id (§3.2.1).
    pub targets: Vec<AdmitTarget>,
    pub input: AdmitInput,
    /// Channel provenance for `origin:"channel"`, `null` for `"api"`.
    pub channel: Option<ChannelProvenance>,
    pub admission: AdmissionInfo,
    /// Per-target frame identity, aligned with `targets` (skip: the wire
    /// frame carries it, §7.2). When shorter than `targets`, the bus
    /// derives floor frames (`worker` empty, `seq` 0).
    #[serde(skip)]
    pub frames: Vec<FrameInfo>,
}

/// `admit/post` payload (§3.2.2); `event_id`/`seq` name the durable row
/// that now exists.
#[derive(Debug, Clone, Serialize)]
pub struct AdmitReceiptView {
    pub session: SessionId,
    pub event_id: String,
    pub seq: u64,
    pub origin: Origin,
    pub deduplicated: bool,
    pub input: AdmitPostInput,
    #[serde(skip)]
    pub frame: Option<FrameInfo>,
}

/// The input summary of an `admit/post` receipt (§3.2.2).
#[derive(Debug, Clone, Serialize)]
pub struct AdmitPostInput {
    pub role: String,
    pub content_bytes: u64,
    pub spilled: bool,
}

/// `turn/request` payload (§3.2.3). Fires once per provider turn, on the
/// persisted primary body — never per fallback attempt (§4.2).
#[derive(Debug, Clone, Serialize)]
pub struct RequestMeta {
    pub slot: String,
    pub provider: String,
    pub model: String,
    pub iteration: u64,
    pub request_bytes: u64,
    pub request_sha256: String,
    pub message_count: u64,
    /// Sorted tool names in the body's `tools` array.
    pub tool_names: Vec<String>,
    pub tool_schema_sha256: String,
    /// The only value ever dispatched in v1: `"pre_send"` (§3.2.3).
    pub timing: String,
    pub step_request_seq: u64,
    #[serde(skip)]
    pub frame: Option<FrameInfo>,
}

/// The `turn/end`-vocabulary error audit object of a `turn/response`
/// payload (§3.2.4 `error`).
#[derive(Debug, Clone, Serialize)]
pub struct TurnErrorAudit {
    pub class: String,
    pub action: String,
}

/// `turn/response` payload (§3.2.4); `usage` is the provider's verbatim
/// object (audit-only).
#[derive(Debug, Clone, Serialize)]
pub struct ResponseView {
    pub iteration: u64,
    /// `turn/end` vocabulary: `final_answer | tool_calls | soft_failure |
    /// error`.
    pub outcome: String,
    pub tool_calls: u64,
    pub text_bytes: u64,
    pub reasoning_bytes: u64,
    pub usage: serde_json::Value,
    pub error: Option<TurnErrorAudit>,
    /// The only value in v1: `"settled"` (§3.2.4).
    pub timing: String,
    #[serde(skip)]
    pub frame: Option<FrameInfo>,
}

/// The declarative gate's decision for a tool call, which has already run
/// (§4.4 ordering). Field shape matches the `tool/call.permission`
/// provenance object (ADR-015; `reason` rides only a deny).
#[derive(Debug, Clone, Serialize)]
pub struct PermissionView {
    pub evaluated: String,
    pub policy: String,
    pub matched: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// `tool/pre` payload (§3.2.5). `arguments` is the parsed value
/// (redacted) when its serialization fits the frame bound, else a string
/// prefix with `arguments_truncated: true` — the two forms are exhaustive
/// and unambiguous.
#[derive(Debug, Clone, Serialize)]
pub struct ToolCallView {
    pub call_id: String,
    pub name: String,
    pub iteration: u64,
    pub arguments: serde_json::Value,
    pub arguments_bytes: u64,
    pub arguments_sha256: String,
    pub arguments_truncated: bool,
    pub permission: PermissionView,
    /// The only value in v1: `"react"` (§3.5).
    pub surface: String,
    #[serde(skip)]
    pub frame: Option<FrameInfo>,
}

/// One aggregate redaction report entry (§3.2.6) — counts only, never
/// values.
#[derive(Debug, Clone, Serialize)]
pub struct RedactionReport {
    pub kind: String,
    pub name: String,
    pub count: u64,
}

/// `tool/post` payload (§3.2.6). No tool output content is shipped in v1
/// (§16.7).
#[derive(Debug, Clone, Serialize)]
pub struct ToolPostView {
    pub call_id: String,
    pub name: String,
    pub is_error: bool,
    pub output_bytes: u64,
    pub output_sha256: String,
    pub redactions: Vec<RedactionReport>,
    #[serde(skip)]
    pub frame: Option<FrameInfo>,
}

/// `recover/decided` payload (§3.2.7). A hook may not veto or alter
/// recovery (constraint 8): this point exists so a plugin can observe
/// which class fired.
#[derive(Debug, Clone, Serialize)]
pub struct RecoveryView {
    pub error_class: String,
    /// A disposition name from the declarative table, or `"none"`.
    pub action: String,
    pub iteration: u64,
    #[serde(skip)]
    pub frame: Option<FrameInfo>,
}

/// `settle/pre` payload (§3.2.8). `reason` is `"completed"` — the only
/// value ever dispatched (§3.4.2).
#[derive(Debug, Clone, Serialize)]
pub struct SettleView {
    pub reason: String,
    pub turns: u64,
    pub tool_calls: u64,
    /// `"none" | "passed" | "failed"` — the evidence gate's state, if it
    /// ran.
    pub evidence: String,
    /// **This plugin's** completion-veto budget in this session (§3.2.8).
    pub vetoes_used: u64,
    pub vetoes_remaining: u64,
    pub episodes: u64,
    #[serde(skip)]
    pub frame: Option<FrameInfo>,
}

/// The verdict of an `admit/pre` evaluation (§3.3: `Allow` is the default
/// when no handler denies). One evaluation → one verdict for the whole
/// fan-out (§4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmitVerdict {
    /// No valid `stop` anywhere in the grid: the write loop proceeds.
    Next,
    /// A `stop` (or a fail-closed default of a `veto:admit` holder at
    /// channel origin) refused the event for **every** target.
    Stop { plugin: String, reason: String },
}

/// The verdict of a `tool/pre` dispatch (§3.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolVerdict {
    /// No valid denial: the declarative permission gate's decision stands.
    Allow,
    /// A valid `deny` from a `veto:tool` holder, or the fail-closed class
    /// default (§11.1) — for the default the reason is the honest-error
    /// tail `hook unavailable (<cause>)`.
    Deny { plugin: String, reason: String },
}

/// The verdict of a `settle/pre` consultation (§4.6 steps 3–5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SettleVerdict {
    /// No handler denied: settlement proceeds.
    Allow,
    /// A `veto:settle` holder denied with budget remaining and the marker
    /// row was admitted: the caller propagates the outcome unchanged and
    /// the engine re-arms (§4.6 step 3).
    Deny { plugin: String, reason: String },
    /// Budget exhausted (§4.6 step 4): the veto is recorded but settlement
    /// proceeds — one audit line records the override.
    Override {
        plugin: String,
        reason: String,
        vetoes_used: u64,
    },
    /// The marker append failed (§4.6, the three-step failure path): the
    /// gate does **not** settle — the caller propagates the append error
    /// through the existing append-failure route, and the budget was
    /// consumed anyway via the in-process counter.
    AdmitFailed {
        plugin: String,
        reason: String,
        error: String,
        vetoes_used: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sid(s: &str) -> SessionId {
        // SessionId is Deserialize over its uuid string form, so tests pin
        // exact uuids without a direct uuid dependency edge (only sha2 may
        // be added this round, K10).
        serde_json::from_str(&format!("\"{s}\"")).expect("valid uuid literal")
    }

    fn frame(session: &str) -> Option<FrameInfo> {
        Some(FrameInfo {
            session: sid(session),
            worker: "node-zulu".to_string(),
            seq: 41,
            epoch: Some(7),
        })
    }

    const SESSION_A: &str = "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b";
    const SESSION_B: &str = "1f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b";

    fn compact<T: Serialize>(v: &T) -> String {
        serde_json::to_string(v).expect("view serializes")
    }

    #[test]
    fn payload_goldens_match_spec_3_2() {
        // §3.2.1 `admit/pre` — compact form of the spec example, keys in
        // spec order (the spec block is pretty-printed; K2 pins keys,
        // order and nesting, so the golden is the compact rendering).
        let draft = AdmitDraft {
            origin: Origin::Channel,
            targets: vec![
                AdmitTarget {
                    session: sid(SESSION_A),
                    router: false,
                },
                AdmitTarget {
                    session: sid(SESSION_B),
                    router: true,
                },
            ],
            input: AdmitInput {
                role: "user".to_string(),
                synthetic: false,
                content_preview: "please run the cleanup".to_string(),
                content_bytes: 91234,
                content_sha256: "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"
                    .to_string(),
                content_truncated: true,
                inline_view: false,
            },
            channel: Some(ChannelProvenance {
                name: "lark".to_string(),
                event_id: "evt_42".to_string(),
                topic: None,
            }),
            admission: AdmissionInfo {
                policy: "inline_cap_bytes=8192".to_string(),
                spilled: true,
            },
            frames: vec![frame(SESSION_A).unwrap(), frame(SESSION_B).unwrap()],
        };
        assert_eq!(
            compact(&draft),
            concat!(
                r#"{"origin":"channel","targets":[{"session":"0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b","router":false},"#,
                r#"{"session":"1f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b","router":true}],"#,
                r#""input":{"role":"user","synthetic":false,"content_preview":"please run the cleanup","content_bytes":91234,"#,
                r#""content_sha256":"a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2","#,
                r#""content_truncated":true,"inline_view":false},"#,
                r#""channel":{"name":"lark","event_id":"evt_42","topic":null},"#,
                r#""admission":{"policy":"inline_cap_bytes=8192","spilled":true}}"#
            ),
            "the in-process frame identity must not ride the wire (K2)"
        );

        // §3.2.2 `admit/post`.
        let receipt = AdmitReceiptView {
            session: sid(SESSION_A),
            event_id: format!("{SESSION_A}:41"),
            seq: 41,
            origin: Origin::Channel,
            deduplicated: false,
            input: AdmitPostInput {
                role: "user".to_string(),
                content_bytes: 91234,
                spilled: true,
            },
            frame: frame(SESSION_A),
        };
        assert_eq!(
            compact(&receipt),
            concat!(
                r#"{"session":"0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b","event_id":"0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b:41","#,
                r#""seq":41,"origin":"channel","deduplicated":false,"#,
                r#""input":{"role":"user","content_bytes":91234,"spilled":true}}"#
            )
        );

        // §3.2.3 `turn/request`.
        let request = RequestMeta {
            slot: "main".to_string(),
            provider: "openai-compat".to_string(),
            model: "gpt-5".to_string(),
            iteration: 2,
            request_bytes: 48211,
            request_sha256: "11b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"
                .to_string(),
            message_count: 11,
            tool_names: vec!["fs_read".to_string(), "fs_write".to_string()],
            tool_schema_sha256: "22b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"
                .to_string(),
            timing: "pre_send".to_string(),
            step_request_seq: 57,
            frame: frame(SESSION_A),
        };
        assert_eq!(
            compact(&request),
            concat!(
                r#"{"slot":"main","provider":"openai-compat","model":"gpt-5","iteration":2,"#,
                r#""request_bytes":48211,"request_sha256":"11b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2","#,
                r#""message_count":11,"tool_names":["fs_read","fs_write"],"#,
                r#""tool_schema_sha256":"22b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2","#,
                r#""timing":"pre_send","step_request_seq":57}"#
            )
        );

        // §3.2.4 `turn/response`.
        let response = ResponseView {
            iteration: 2,
            outcome: "final_answer".to_string(),
            tool_calls: 0,
            text_bytes: 1422,
            reasoning_bytes: 0,
            usage: serde_json::json!({"input_tokens": 120, "output_tokens": 30}),
            error: None,
            timing: "settled".to_string(),
            frame: frame(SESSION_A),
        };
        assert_eq!(
            compact(&response),
            concat!(
                r#"{"iteration":2,"outcome":"final_answer","tool_calls":0,"text_bytes":1422,"#,
                r#""reasoning_bytes":0,"usage":{"input_tokens":120,"output_tokens":30},"error":null,"#,
                r#""timing":"settled"}"#
            )
        );

        // §3.2.5 `tool/pre`.
        let call = ToolCallView {
            call_id: "call_abc".to_string(),
            name: "exec".to_string(),
            iteration: 2,
            arguments: serde_json::json!({"cmd": ["rm", "-rf", "/tmp/x"]}),
            arguments_bytes: 38,
            arguments_sha256: "33b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"
                .to_string(),
            arguments_truncated: false,
            permission: PermissionView {
                evaluated: "allow".to_string(),
                policy: "configured".to_string(),
                matched: "rule[3]".to_string(),
                reason: None,
            },
            surface: "react".to_string(),
            frame: frame(SESSION_A),
        };
        assert_eq!(
            compact(&call),
            concat!(
                r#"{"call_id":"call_abc","name":"exec","iteration":2,"arguments":{"cmd":["rm","-rf","/tmp/x"]},"#,
                r#""arguments_bytes":38,"arguments_sha256":"33b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2","#,
                r#""arguments_truncated":false,"#,
                r#""permission":{"evaluated":"allow","policy":"configured","matched":"rule[3]"},"#,
                r#""surface":"react"}"#
            )
        );

        // §3.2.6 `tool/post`.
        let post = ToolPostView {
            call_id: "call_abc".to_string(),
            name: "exec".to_string(),
            is_error: false,
            output_bytes: 812,
            output_sha256: "44b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2"
                .to_string(),
            redactions: vec![RedactionReport {
                kind: "env_line".to_string(),
                name: "OPENAI_API_KEY".to_string(),
                count: 1,
            }],
            frame: frame(SESSION_A),
        };
        assert_eq!(
            compact(&post),
            concat!(
                r#"{"call_id":"call_abc","name":"exec","is_error":false,"output_bytes":812,"#,
                r#""output_sha256":"44b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2","#,
                r#""redactions":[{"kind":"env_line","name":"OPENAI_API_KEY","count":1}]}"#
            )
        );

        // §3.2.7 `recover/decided`.
        let recovery = RecoveryView {
            error_class: "rate_limited".to_string(),
            action: "backoff_retry".to_string(),
            iteration: 3,
            frame: frame(SESSION_A),
        };
        assert_eq!(
            compact(&recovery),
            r#"{"error_class":"rate_limited","action":"backoff_retry","iteration":3}"#
        );

        // §3.2.8 `settle/pre`.
        let settle = SettleView {
            reason: "completed".to_string(),
            turns: 4,
            tool_calls: 7,
            evidence: "passed".to_string(),
            vetoes_used: 0,
            vetoes_remaining: 3,
            episodes: 2,
            frame: frame(SESSION_A),
        };
        assert_eq!(
            compact(&settle),
            concat!(
                r#"{"reason":"completed","turns":4,"tool_calls":7,"evidence":"passed","#,
                r#""vetoes_used":0,"vetoes_remaining":3,"episodes":2}"#
            )
        );
    }
}
