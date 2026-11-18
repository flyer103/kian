//! The stdio ABI frames (PLUGIN-SPEC §7.1–§7.2, v1).
//!
//! Newline-delimited compact JSON objects with **closed shapes**: field
//! names, order and presence are exactly the §7.2 table, checked by the
//! golden tests byte-for-byte. The epoch rule lives here as the single
//! source: a `hook` frame for a drain-phase hook **must** carry `epoch`,
//! an admission frame **must omit the key entirely** — no `null`, no
//! sentinel, no `0` (§7.2).
//!
//! Reply decoding is deny-unknown-fields and bound-checks every audit
//! string (`note` ≤ 256, `reason`/`message` ≤ 512, §7.2): a violation
//! is a malformed speech defect, never a panic and never a silent
//! truncation that would smuggle unbounded bytes upward.

use serde_json::Value;

use super::super::ids::HookId;
use super::super::registry::ReplyErrCode;
use super::super::views::FrameInfo;

/// §7.2's `note` bound.
pub const NOTE_MAX_BYTES: usize = 256;
/// §7.2's `reason` / `message` bound.
pub const REASON_MAX_BYTES: usize = 512;

// ---------------------------------------------------------------------
// S→P frames
// ---------------------------------------------------------------------

/// The `hello` frame (§7.2, §7.3 step 1): the server's identity
/// statement — manifest canonical path, plugin name/version, abi — plus
/// the declared sets and the two bounds the child must respect.
#[derive(Debug, Clone, PartialEq)]
pub struct Hello {
    pub manifest: String,
    pub name: String,
    pub version: String,
    pub abi: u32,
    pub hooks: Vec<String>,
    pub capabilities: Vec<String>,
    pub deadline_default_ms: u64,
    pub max_frame_bytes: usize,
}

impl Hello {
    /// Compact one-line JSON in §7.2's field order.
    pub fn encode(&self) -> String {
        let mut out = String::with_capacity(256);
        out.push_str("{\"v\":1,\"manifest\":");
        push_json_string(&mut out, &self.manifest);
        out.push_str(",\"plugin\":{\"name\":");
        push_json_string(&mut out, &self.name);
        out.push_str(",\"version\":");
        push_json_string(&mut out, &self.version);
        out.push_str("},\"abi\":");
        out.push_str(&self.abi.to_string());
        out.push_str(",\"hooks\":");
        push_json_string_array(&mut out, &self.hooks);
        out.push_str(",\"capabilities\":");
        push_json_string_array(&mut out, &self.capabilities);
        out.push_str(",\"deadline_default_ms\":");
        out.push_str(&self.deadline_default_ms.to_string());
        out.push_str(",\"max_frame_bytes\":");
        out.push_str(&self.max_frame_bytes.to_string());
        out.push('}');
        out
    }
}

/// The `bye` frame (§7.2): the empty object, nothing else.
pub fn encode_bye() -> String {
    "{}".to_string()
}

/// A server→plugin `err` frame (§7.2 allows the direction; the runner
/// uses it only in tests and diagnostics today — refusal and quarantine
/// travel as process death plus audit lines, not as err frames).
pub fn encode_err(id: Option<&str>, code: ReplyErrCode, message: &str) -> String {
    let mut out = String::with_capacity(96);
    out.push('{');
    if let Some(id) = id {
        out.push_str("\"id\":");
        push_json_string(&mut out, id);
        out.push(',');
    }
    out.push_str("\"code\":");
    push_json_string(&mut out, code.as_str());
    out.push_str(",\"message\":");
    push_json_string(&mut out, message);
    out.push('}');
    out
}

/// Why a `hook` frame could not be built: the epoch rule's local guard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameError {
    pub message: String,
}

/// The `hook` frame (§7.2): full target identity (`session`, `worker`,
/// `seq`), a fresh dispatch id, the per-dispatch deadline, the mode and
/// the §3.2 payload. `epoch` rides the frame — never the payload — and
/// only for drain-phase hooks.
#[derive(Debug, Clone, PartialEq)]
pub struct HookFrame {
    pub id: String,
    pub hook: HookId,
    pub session: String,
    pub worker: String,
    pub seq: u64,
    pub epoch: Option<u64>,
    pub deadline_ms: u64,
    pub mode: &'static str,
    pub payload: Value,
}

impl HookFrame {
    /// Builds a frame from the in-process dispatch identity. The epoch
    /// rule is enforced here: drain-phase hooks require a frame with an
    /// epoch, admission hooks must not carry one (§7.2) — a caller that
    /// gets this wrong is refused, not silently repaired.
    pub fn new(
        hook: HookId,
        frame: &FrameInfo,
        id: String,
        deadline_ms: u64,
        payload: Value,
    ) -> Result<Self, FrameError> {
        let drain_phase = hook.class().is_drain_phase();
        let epoch = frame.epoch;
        match (drain_phase, epoch) {
            (true, Some(_)) => {}
            (true, None) => {
                return Err(FrameError {
                    message: format!(
                        "hook `{hook}` is drain-phase: its frame requires an epoch (§7.2)"
                    ),
                });
            }
            (false, Some(_)) => {
                return Err(FrameError {
                    message: format!(
                        "hook `{hook}` runs before a drain exists: its frame must omit `epoch` (§7.2)"
                    ),
                });
            }
            (false, None) => {}
        }
        Ok(Self {
            id,
            hook: hook.clone(),
            session: frame.session.0.to_string(),
            worker: frame.worker.clone(),
            seq: frame.seq,
            epoch,
            deadline_ms,
            mode: mode_str(hook.mode()),
            payload,
        })
    }

    /// Compact one-line JSON in §7.2's field order, `epoch` present
    /// only when `Some`.
    pub fn encode(&self) -> String {
        let mut out = String::with_capacity(256);
        out.push_str("{\"id\":");
        push_json_string(&mut out, &self.id);
        out.push_str(",\"hook\":");
        push_json_string(&mut out, &self.hook.to_string());
        out.push_str(",\"session\":");
        push_json_string(&mut out, &self.session);
        out.push_str(",\"worker\":");
        push_json_string(&mut out, &self.worker);
        out.push_str(",\"seq\":");
        out.push_str(&self.seq.to_string());
        if let Some(epoch) = self.epoch {
            out.push_str(",\"epoch\":");
            out.push_str(&epoch.to_string());
        }
        out.push_str(",\"deadline_ms\":");
        out.push_str(&self.deadline_ms.to_string());
        out.push_str(",\"mode\":");
        push_json_string(&mut out, self.mode);
        out.push_str(",\"payload\":");
        out.push_str(&serde_json::to_string(&self.payload).unwrap_or_default());
        out.push('}');
        out
    }
}

fn mode_str(mode: super::super::ids::HookMode) -> &'static str {
    match mode {
        super::super::ids::HookMode::Emit => "emit",
        super::super::ids::HookMode::Flow => "flow",
        super::super::ids::HookMode::Bail => "bail",
    }
}

// ---------------------------------------------------------------------
// P→S frames
// ---------------------------------------------------------------------

/// The `ready` frame (§7.2, §7.3 step 2): the child's identity
/// restatement, compared by the runner for exact identity equality and
/// set-equal `hooks`/`capabilities`.
#[derive(Debug, Clone, PartialEq)]
pub struct Ready {
    pub manifest: String,
    pub name: String,
    pub version: String,
    pub abi: u32,
    pub hooks: Vec<String>,
    pub capabilities: Vec<String>,
}

/// A plugin→server reply frame: `ack`, `flow`, `bail` or `err`, with
/// the closed shapes of §7.2.
#[derive(Debug, Clone, PartialEq)]
pub enum ReplyFrame {
    Ack { note: Option<String> },
    FlowNext,
    FlowStop { reason: String },
    BailNone,
    BailDeny { reason: String },
    Err { code: ReplyErrCode, message: String },
}

impl ReplyFrame {
    /// Canonical compact JSON with the frame `id` prepended, in §7.2's
    /// field order. The reference child and the golden tests share this
    /// encoder, so the wire shape has exactly one spelling.
    pub fn encode(&self, id: &str) -> String {
        let mut out = String::with_capacity(128);
        out.push_str("{\"id\":");
        push_json_string(&mut out, id);
        match self {
            ReplyFrame::Ack { note } => {
                out.push_str(",\"ack\":true");
                if let Some(note) = note {
                    out.push_str(",\"note\":");
                    push_json_string(&mut out, note);
                }
            }
            ReplyFrame::FlowNext => out.push_str(",\"flow\":\"next\""),
            ReplyFrame::FlowStop { reason } => {
                out.push_str(",\"flow\":\"stop\",\"reason\":");
                push_json_string(&mut out, reason);
            }
            ReplyFrame::BailNone => out.push_str(",\"bail\":null"),
            ReplyFrame::BailDeny { reason } => {
                out.push_str(",\"bail\":{\"decision\":\"deny\",\"reason\":");
                push_json_string(&mut out, reason);
                out.push('}');
            }
            ReplyFrame::Err { code, message } => {
                out.push_str(",\"code\":");
                push_json_string(&mut out, code.as_str());
                out.push_str(",\"message\":");
                push_json_string(&mut out, message);
            }
        }
        out.push('}');
        out
    }
}

/// Why a P→S line is not a legal frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// Not JSON, not an object, or a shape violation — the speech
    /// defect `err:malformed` classifies from (§7.2, §11.1).
    Malformed(String),
}

impl DecodeError {
    fn malformed(message: impl Into<String>) -> Self {
        DecodeError::Malformed(message.into())
    }
}

/// Decodes a `ready` line (§7.3 step 2). Structural violations are
/// malformed — the runner maps identity mismatches to the typed
/// refusals (`ready_mismatch` / `abi_mismatch`) itself, after decoding.
pub fn decode_ready(line: &str) -> Result<Ready, DecodeError> {
    let v: Value = serde_json::from_str(line).map_err(|e| DecodeError::malformed(e.to_string()))?;
    let obj = v
        .as_object()
        .ok_or_else(|| DecodeError::malformed("ready is not an object"))?;
    for key in obj.keys() {
        if !matches!(
            key.as_str(),
            "v" | "manifest" | "plugin" | "abi" | "hooks" | "capabilities"
        ) {
            return Err(DecodeError::malformed(format!(
                "ready carries unknown key `{key}` (closed shape)"
            )));
        }
    }
    if obj.get("v").and_then(Value::as_u64) != Some(1) {
        return Err(DecodeError::malformed("ready v must be 1"));
    }
    let string_field = |name: &str| -> Result<String, DecodeError> {
        obj.get(name)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| DecodeError::malformed(format!("ready.{name} must be a string")))
    };
    let manifest = string_field("manifest")?;
    let plugin = obj
        .get("plugin")
        .and_then(Value::as_object)
        .ok_or_else(|| DecodeError::malformed("ready.plugin must be an object"))?;
    if plugin.len() != 2 || !plugin.contains_key("name") || !plugin.contains_key("version") {
        return Err(DecodeError::malformed(
            "ready.plugin must be exactly {name, version}",
        ));
    }
    let name = plugin
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| DecodeError::malformed("ready.plugin.name must be a string"))?
        .to_string();
    let version = plugin
        .get("version")
        .and_then(Value::as_str)
        .ok_or_else(|| DecodeError::malformed("ready.plugin.version must be a string"))?
        .to_string();
    let abi = obj
        .get("abi")
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(|| DecodeError::malformed("ready.abi must be a small integer"))?;
    let str_array = |name: &str| -> Result<Vec<String>, DecodeError> {
        let arr = obj
            .get(name)
            .and_then(Value::as_array)
            .ok_or_else(|| DecodeError::malformed(format!("ready.{name} must be an array")))?;
        arr.iter()
            .map(|item| {
                item.as_str().map(str::to_string).ok_or_else(|| {
                    DecodeError::malformed(format!("ready.{name} entries are strings"))
                })
            })
            .collect()
    };
    Ok(Ready {
        manifest,
        name,
        version,
        abi,
        hooks: str_array("hooks")?,
        capabilities: str_array("capabilities")?,
    })
}

/// Decodes a reply line (`ack` / `flow` / `bail` / `err`) for dispatch
/// `expected_id`. A reply carrying any other id is **not** an error
/// here — the runner treats it as `protocol_violation` (§7.4) and the
/// caller needs to see the stray id to say so; this function returns
/// the frame's own id alongside (`None` only on an id-less `err`, the
/// one shape §7.2 allows to omit it — attribution falls back to the
/// single in-flight dispatch).
pub fn decode_reply(line: &str) -> Result<(Option<String>, ReplyFrame), DecodeError> {
    let v: Value = serde_json::from_str(line).map_err(|e| DecodeError::malformed(e.to_string()))?;
    let obj = v
        .as_object()
        .ok_or_else(|| DecodeError::malformed("reply is not an object"))?
        .clone();
    let is_err = obj.contains_key("code");
    let id = match obj.get("id") {
        Some(Value::String(s)) => Some(s.clone()),
        None if is_err => None,
        None => return Err(DecodeError::malformed("reply.id must be a string")),
        Some(_) => return Err(DecodeError::malformed("reply.id must be a string")),
    };
    let shape_keys: Vec<&String> = obj
        .keys()
        .filter(|k| {
            matches!(
                k.as_str(),
                "ack" | "flow" | "bail" | "code" | "message" | "note" | "reason"
            )
        })
        .collect();
    // An err is the flat `{"id"?,"code","message"}` shape (§7.2); the
    // other three are one-key shapes with their own optional fields.
    let is_err = obj.contains_key("code");
    if is_err {
        if obj.contains_key("ack") || obj.contains_key("flow") || obj.contains_key("bail") {
            return Err(DecodeError::malformed("reply mixes err with a reply shape"));
        }
    } else {
        let strict: Vec<&String> = obj
            .keys()
            .filter(|k| matches!(k.as_str(), "ack" | "flow" | "bail"))
            .collect();
        if strict.len() != 1 {
            return Err(DecodeError::malformed(
                "reply carries exactly one of ack / flow / bail / err",
            ));
        }
    }
    for key in obj.keys() {
        if !matches!(
            key.as_str(),
            "id" | "ack" | "note" | "flow" | "reason" | "bail" | "code" | "message"
        ) {
            return Err(DecodeError::malformed(format!(
                "reply carries unknown key `{key}` (closed shape)"
            )));
        }
    }
    let bounded = |s: &str, max: usize, what: &str| -> Result<String, DecodeError> {
        if s.len() > max {
            return Err(DecodeError::malformed(format!(
                "{what} exceeds the {max}-byte audit-string bound"
            )));
        }
        // §7.2's defense-in-depth redaction: the server redacts audit
        // strings again before they are logged or persisted — a plugin
        // cannot be trusted to redact. Control characters have no
        // legitimate place in an audit string; the byte bound is
        // enforced above.
        Ok(if s.bytes().any(|b| b < 0x20 || b == 0x7f) {
            s.chars()
                .map(|c| if c.is_control() { '·' } else { c })
                .collect()
        } else {
            s.to_string()
        })
    };
    let frame = if is_err {
        if obj.len() != usize::from(id.is_some()) + 2 {
            return Err(DecodeError::malformed(
                "err must be exactly {id?, code, message}",
            ));
        }
        let code = obj
            .get("code")
            .and_then(Value::as_str)
            .and_then(parse_err_code)
            .ok_or_else(|| {
                DecodeError::malformed("err.code is outside the closed set — itself malformed")
            })?;
        let message = obj
            .get("message")
            .and_then(Value::as_str)
            .ok_or_else(|| DecodeError::malformed("err.message must be a string"))?;
        ReplyFrame::Err {
            code,
            message: bounded(message, REASON_MAX_BYTES, "err.message")?,
        }
    } else {
        match shape_keys[0].as_str() {
            "ack" => {
                if obj.get("ack") != Some(&Value::Bool(true)) {
                    return Err(DecodeError::malformed("ack must be literal true"));
                }
                let note = match obj.get("note") {
                    None => None,
                    Some(Value::String(s)) => Some(bounded(s, NOTE_MAX_BYTES, "ack.note")?),
                    Some(_) => return Err(DecodeError::malformed("ack.note must be a string")),
                };
                ReplyFrame::Ack { note }
            }
            "flow" => match obj.get("flow").and_then(Value::as_str) {
                Some("next") => ReplyFrame::FlowNext,
                Some("stop") => {
                    let reason = obj
                        .get("reason")
                        .and_then(Value::as_str)
                        .ok_or_else(|| DecodeError::malformed("flow stop requires reason"))?;
                    ReplyFrame::FlowStop {
                        reason: bounded(reason, REASON_MAX_BYTES, "flow.reason")?,
                    }
                }
                _ => return Err(DecodeError::malformed("flow must be \"next\" or \"stop\"")),
            },
            "bail" => match obj.get("bail") {
                Some(Value::Null) => ReplyFrame::BailNone,
                Some(Value::Object(deny)) => {
                    if deny.len() != 2
                        || deny.get("decision").and_then(Value::as_str) != Some("deny")
                        || !deny.contains_key("reason")
                    {
                        return Err(DecodeError::malformed(
                            "bail object must be exactly {decision:\"deny\", reason}",
                        ));
                    }
                    let reason = deny
                        .get("reason")
                        .and_then(Value::as_str)
                        .ok_or_else(|| DecodeError::malformed("bail.reason must be a string"))?;
                    ReplyFrame::BailDeny {
                        reason: bounded(reason, REASON_MAX_BYTES, "bail.reason")?,
                    }
                }
                _ => return Err(DecodeError::malformed("bail must be null or a deny object")),
            },
            _ => unreachable!("shape_keys filtered above"),
        }
    };
    Ok((id, frame))
}

fn parse_err_code(code: &str) -> Option<ReplyErrCode> {
    match code {
        "deadline" => Some(ReplyErrCode::Deadline),
        "crashed" => Some(ReplyErrCode::Crashed),
        "malformed" => Some(ReplyErrCode::Malformed),
        "unauthorized" => Some(ReplyErrCode::Unauthorized),
        "unsupported_hook" => Some(ReplyErrCode::UnsupportedHook),
        "protocol_violation" => Some(ReplyErrCode::ProtocolViolation),
        _ => None,
    }
}

// ---------------------------------------------------------------------
// JSON string helpers — serde_json's Value escaping with a stable,
// minimal spelling so goldens pin one byte sequence.
// ---------------------------------------------------------------------

fn push_json_string(out: &mut String, s: &str) {
    out.push_str(&serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string()));
}

fn push_json_string_array(out: &mut String, items: &[String]) {
    out.push('[');
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        push_json_string(out, item);
    }
    out.push(']');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn err_code_outside_the_set_is_malformed() {
        let line = r#"{"id":"sc-1","code":"mystery","message":"x"}"#;
        assert!(decode_reply(line).is_err());
    }

    #[test]
    fn audit_string_bounds_are_enforced() {
        let long = "x".repeat(REASON_MAX_BYTES + 1);
        let deny = format!(r#"{{"id":"sc-1","bail":{{"decision":"deny","reason":"{long}"}}}}"#);
        assert!(decode_reply(&deny).is_err());
        let note = format!(
            r#"{{"id":"sc-1","ack":true,"note":"{}"}}"#,
            "y".repeat(NOTE_MAX_BYTES + 1)
        );
        assert!(decode_reply(&note).is_err());
    }

    #[test]
    fn audit_strings_are_redacted_before_rising() {
        // §7.2 defense in depth: control characters never ride a
        // reply's audit strings upward.
        let (_, frame) = decode_reply(
            "{\"id\":\"x\",\"bail\":{\"decision\":\"deny\",\"reason\":\"line1\\nline2\\u0007\"}}",
        )
        .unwrap();
        match frame {
            ReplyFrame::BailDeny { reason } => {
                assert!(reason.contains('·'), "{reason:?}");
                assert!(!reason.contains('\n'));
                assert!(!reason.contains('\u{7}'));
            }
            other => panic!("expected deny, got {other:?}"),
        }
    }

    #[test]
    fn reply_shapes_are_closed() {
        // Two shape keys, an unknown key, a non-literal ack: all malformed.
        for line in [
            r#"{"id":"x","ack":true,"flow":"next"}"#,
            r#"{"id":"x","ack":true,"extra":1}"#,
            r#"{"id":"x","ack":false}"#,
            r#"{"id":"x","bail":{"decision":"allow","reason":"y"}}"#,
            "not json",
            r#"{"ack":true}"#,
        ] {
            assert!(decode_reply(line).is_err(), "{line}");
        }
        // The four legal shapes decode.
        assert_eq!(
            decode_reply(r#"{"id":"x","ack":true}"#).unwrap().1,
            ReplyFrame::Ack { note: None }
        );
        assert_eq!(
            decode_reply(r#"{"id":"x","flow":"stop","reason":"off-hours"}"#)
                .unwrap()
                .1,
            ReplyFrame::FlowStop {
                reason: "off-hours".to_string()
            }
        );
        assert_eq!(
            decode_reply(r#"{"id":"x","bail":null}"#).unwrap().1,
            ReplyFrame::BailNone
        );
        assert_eq!(
            decode_reply(r#"{"id":"x","code":"deadline","message":"slow"}"#)
                .unwrap()
                .1,
            ReplyFrame::Err {
                code: ReplyErrCode::Deadline,
                message: "slow".to_string()
            }
        );
        // An id-less err is the one legal id omission (§7.2).
        assert_eq!(
            decode_reply(r#"{"code":"crashed","message":"gone"}"#)
                .unwrap()
                .0,
            None
        );
    }

    #[test]
    fn ready_is_closed() {
        assert!(decode_ready("{\"v\":1}").is_err());
        assert!(decode_ready(r#"{"v":2,"manifest":"/m","plugin":{"name":"a","version":"0.1.0"},"abi":1,"hooks":[],"capabilities":[]}"#).is_err());
        let ok = decode_ready(
            r#"{"v":1,"manifest":"/m","plugin":{"name":"a","version":"0.1.0"},"abi":1,"hooks":["tool/pre"],"capabilities":["observe:tool"]}"#,
        )
        .unwrap();
        assert_eq!(ok.name, "a");
        assert_eq!(ok.hooks, vec!["tool/pre".to_string()]);
    }
}
