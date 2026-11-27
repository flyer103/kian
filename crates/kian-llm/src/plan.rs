//! Request planning: one value that the sender, the persistence sink, the
//! cache checker and the budget gate all consume (ADR-054).
//!
//! A plan is built by a pure function of the slot profile, the model, the
//! exact message list and the tool schemas. It never reads process state, so
//! the same committed conversation yields the same bytes on replay.

use kian_core::model::{ModelDialect, ModelProfile, ReasoningEffort};
use kian_core::seams::{ProviderError, ToolSchema};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::provider::{ChatImage, ChatMessage, build_chat_body, serialize_request_body};

/// The body dialect version carried next to every attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BodyVersion {
    /// The frozen pre-profile body: `{messages, model, stream}` plus tools.
    #[serde(rename = "v1")]
    V1,
    /// The profile-aware body: V1 plus declared thinking/effort/max_tokens.
    #[serde(rename = "v2")]
    V2,
}

/// A deliberately coarse prompt-size band. It is not a tokenizer and never
/// claims to be exact; the budget gate uses the upper bound as its worst
/// case, which is the only consumer that needs a conservative number.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TokenEstimate {
    /// Lower bound of the prompt size in tokens.
    pub low: u64,
    /// Upper bound of the prompt size in tokens.
    pub high: u64,
    /// The declared output cap, when the profile declares one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u64>,
    /// The estimator family that produced the band.
    pub source: &'static str,
    /// The estimator version, so a later reader can tell bands apart.
    pub estimator_version: u32,
}

/// The estimator identity recorded in every estimate.
pub const ESTIMATOR_SOURCE: &str = "chars_v1";
/// The estimator version recorded in every estimate.
pub const ESTIMATOR_VERSION: u32 = 1;

/// Which form a persisted request body takes (ADR-073).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BodyForm {
    /// Image parts are `artifact:<sha256-hex>` references rather than inline
    /// base64, so the log stops copying the same pixels every turn.
    ArtifactRefs,
}

/// One fully built attempt: the exact bytes to send plus the metadata that
/// makes the attempt auditable without re-deriving it.
#[derive(Debug, Clone)]
pub struct RequestPlan {
    /// The deterministic request body.
    pub body: Value,
    /// Which body dialect produced it.
    pub body_version: BodyVersion,
    /// The dialect the target profile declares.
    pub dialect: ModelDialect,
    /// SHA-256 of the canonical profile, absent when no profile is declared.
    pub profile_digest: Option<String>,
    /// SHA-256 of the exact request bytes.
    pub body_sha256: String,
    /// The prompt-size band for this attempt.
    pub estimate: TokenEstimate,
    /// The rendering persisted next to the wire body (ADR-073): identical to
    /// `body` except that every image part names the artifact store instead of
    /// inlining base64. `None` when the body carries no image part, so the wire
    /// body is persisted as-is.
    pub audit_body: Option<Value>,
    /// The form `audit_body` takes; always present when `audit_body` is.
    pub audit_form: Option<BodyForm>,
    /// The length of the wire body in bytes, recorded as `body_bytes`.
    pub body_bytes: u64,
}

impl RequestPlan {
    /// The body to persist and the form to record beside it (ADR-073).
    ///
    /// The wire body and its digest describe what flies; the audit body
    /// describes the same request without copying the pixels.
    pub fn audit(&self) -> (&Value, Option<BodyForm>) {
        match (&self.audit_body, self.audit_form) {
            (Some(body), Some(form)) => (body, Some(form)),
            _ => (&self.body, None),
        }
    }
}

/// Builds one request plan.
///
/// With no profile the plan is byte-identical to the pre-profile contract:
/// the frozen V1 body and its hash are untouched. A declared profile yields
/// the V2 body and filters reasoning replay according to the profile.
pub fn build_request_plan(
    profile: Option<&ModelProfile>,
    model: &str,
    messages: &[ChatMessage],
    tools: &[ToolSchema],
) -> Result<RequestPlan, ProviderError> {
    let Some(profile) = profile else {
        let body = build_chat_body(model, messages, tools);
        return finish(
            body,
            None,
            BodyVersion::V1,
            ModelDialect::OpenAiChat,
            None,
            None,
        );
    };

    let keep_reasoning = profile.reasoning && profile.thinking != Some(false);
    let filtered;
    let messages = if keep_reasoning {
        messages
    } else if messages.iter().any(|m| m.reasoning_content.is_some()) {
        filtered = messages
            .iter()
            .cloned()
            .map(|mut m| {
                m.reasoning_content = None;
                m
            })
            .collect::<Vec<_>>();
        filtered.as_slice()
    } else {
        messages
    };

    let mut body = build_chat_body(model, messages, tools);
    if let Some(thinking) = profile.thinking {
        body["thinking"] = json!({
            "type": if thinking { "enabled" } else { "disabled" },
        });
    }
    if let Some(effort) = profile.effort {
        body["reasoning_effort"] = json!(match effort {
            ReasoningEffort::Low => "low",
            ReasoningEffort::High => "high",
            ReasoningEffort::Max => "max",
        });
    }
    if let Some(max_output) = profile.max_output {
        body["max_tokens"] = json!(max_output);
    }
    // ADR-063: an image-bearing message becomes OpenAI content parts, and
    // only under a profile that declares the capability. Every other body
    // keeps the exact bytes `build_chat_body` produced. ADR-073: the same plan
    // also renders the audit body, which names each image by its artifact id
    // instead of copying the pixels into the log.
    let audit_body = if profile.vision && messages.iter().any(|m| !m.images.is_empty()) {
        body["messages"] = Value::Array(vision_messages(messages, ImageRender::Inline)?);
        let mut audit = body.clone();
        audit["messages"] = Value::Array(vision_messages(messages, ImageRender::Reference)?);
        Some(audit)
    } else {
        None
    };

    let digest = sha256_hex(
        &serde_json::to_vec(profile)
            .map_err(|e| ProviderError::Other(format!("profile serialization failed: {e}")))?,
    );
    finish(
        body,
        audit_body,
        BodyVersion::V2,
        profile.dialect,
        Some(digest),
        profile.max_output,
    )
}

/// How an image part is rendered: the wire form or the audit form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImageRender {
    /// The wire form: `data:<media_type>;base64,<bytes>` (ADR-063).
    Inline,
    /// The audit form: `artifact:<sha256-hex>` plus the type and size
    /// (ADR-073), so the persisted body stays small and still names exactly
    /// the bytes that flew.
    Reference,
}

/// Renders the image-bearing messages as OpenAI content parts (ADR-063).
///
/// Only a message that actually carries an image changes shape: its string
/// content becomes a `text` part followed by one `image_url` part per image.
/// Every other message keeps the exact object `build_chat_body` produced, so
/// the audit rendering differs from the wire body only inside an image part.
fn vision_messages(
    messages: &[ChatMessage],
    render: ImageRender,
) -> Result<Vec<Value>, ProviderError> {
    let mut out = Vec::with_capacity(messages.len());
    for message in messages {
        let mut value = serde_json::to_value(message)
            .map_err(|e| ProviderError::Other(format!("message serialization failed: {e}")))?;
        if message.images.is_empty() {
            out.push(value);
            continue;
        }
        let mut parts = Vec::with_capacity(message.images.len() + 1);
        if let Some(text) = message.content.as_deref().filter(|t| !t.is_empty()) {
            parts.push(json!({"type": "text", "text": text}));
        }
        for image in &message.images {
            parts.push(image_part(image, render));
        }
        let Some(object) = value.as_object_mut() else {
            return Err(ProviderError::Other(
                "message serialization produced a non-object".to_string(),
            ));
        };
        object.insert("content".to_string(), Value::Array(parts));
        out.push(value);
    }
    Ok(out)
}

/// One `image_url` part in the requested rendering.
fn image_part(image: &ChatImage, render: ImageRender) -> Value {
    match render {
        ImageRender::Inline => json!({
            "type": "image_url",
            "image_url": {
                "url": format!(
                    "data:{};base64,{}",
                    image.media_type,
                    base64_encode(&image.bytes)
                ),
            },
        }),
        ImageRender::Reference => json!({
            "type": "image_url",
            "image_url": {
                "url": format!("artifact:{}", sha256_hex(&image.bytes)),
                "media_type": image.media_type,
                "size": image.bytes.len() as u64,
            },
        }),
    }
}

/// Standard padded base64 (RFC 4648) of an image payload (ADR-063).
///
/// The workspace carries no base64 dependency; this encoder is the inverse
/// of the decoder the channel media edge uses and is covered by vectors.
fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = u32::from(chunk[0]);
        let second = chunk.get(1).copied().map(u32::from);
        let third = chunk.get(2).copied().map(u32::from);
        out.push(char::from(ALPHABET[(first >> 2) as usize]));
        out.push(char::from(
            ALPHABET[(((first & 0b11) << 4) | (second.unwrap_or(0) >> 4)) as usize],
        ));
        match (second, third) {
            (Some(one), None) => {
                out.push(char::from(ALPHABET[((one & 0b1111) << 2) as usize]));
                out.push('=');
            }
            (Some(one), Some(two)) => {
                out.push(char::from(
                    ALPHABET[(((one & 0b1111) << 2) | (two >> 6)) as usize],
                ));
                out.push(char::from(ALPHABET[(two & 0b111111) as usize]));
            }
            (None, _) => {
                out.push('=');
                out.push('=');
            }
        }
    }
    out
}

fn finish(
    body: Value,
    audit_body: Option<Value>,
    body_version: BodyVersion,
    dialect: ModelDialect,
    profile_digest: Option<String>,
    max_output_tokens: Option<u64>,
) -> Result<RequestPlan, ProviderError> {
    let bytes = serialize_request_body(&body)?;
    let body_sha256 = sha256_hex(&bytes);
    let high = (bytes.len() as u64) / 3 + 256;
    let low = (bytes.len() as u64) / 5;
    Ok(RequestPlan {
        body,
        audit_form: audit_body.as_ref().map(|_| BodyForm::ArtifactRefs),
        audit_body,
        body_bytes: bytes.len() as u64,
        body_version,
        dialect,
        profile_digest,
        body_sha256,
        estimate: TokenEstimate {
            low,
            high,
            max_output_tokens,
            source: ESTIMATOR_SOURCE,
            estimator_version: ESTIMATOR_VERSION,
        },
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Why a persisted audit body could not be rebuilt into its wire form
/// (ADR-073).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditError {
    /// An image part is neither an inline `data:` URL nor a reference.
    Malformed { part: String },
    /// The store no longer holds the blob the reference names.
    MissingBlob { id: String },
    /// The store holds the blob but could not read it.
    Unreadable { id: String, detail: String },
    /// The blob's bytes do not hash to the id that names them.
    CorruptBlob { id: String, actual: String },
    /// The recorded size disagrees with the blob the store returned.
    SizeMismatch {
        id: String,
        recorded: u64,
        actual: u64,
    },
    /// Rebuilding from the store did not reproduce `body_sha256`.
    DigestMismatch { expected: String, actual: String },
}

impl std::fmt::Display for AuditError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuditError::Malformed { part } => write!(f, "malformed image part: {part}"),
            AuditError::MissingBlob { id } => write!(
                f,
                "artifact {id} is no longer in the blob store: the pixels are gone, the stream keeps the identity"
            ),
            AuditError::Unreadable { id, detail } => {
                write!(f, "artifact {id} could not be read: {detail}")
            }
            AuditError::CorruptBlob { id, actual } => write!(
                f,
                "artifact {id} hashes to {actual}; refusing to rebuild a body from bytes that are not the recorded image"
            ),
            AuditError::SizeMismatch {
                id,
                recorded,
                actual,
            } => write!(
                f,
                "artifact {id} records {recorded} bytes but the blob holds {actual}"
            ),
            AuditError::DigestMismatch { expected, actual } => write!(
                f,
                "the rebuilt body hashes to {actual}, not the recorded {expected}"
            ),
        }
    }
}

impl std::error::Error for AuditError {}

/// A rebuilt wire body plus how many references it expanded (ADR-073).
#[derive(Debug, Clone)]
pub struct Expansion {
    /// The wire body: every reference replaced by its exact `data:` URL.
    pub body: Value,
    /// How many image parts were rebuilt from the store.
    pub references: usize,
}

/// Rebuilds the wire body from a persisted audit body (ADR-073).
///
/// `blob` returns the stored bytes for an artifact id — `Ok(None)` when the
/// blob was reclaimed, `Err` when the store could not be read. Every rebuilt
/// part is verified: the bytes must hash to the id that named them and match
/// the recorded size; then the whole result must reproduce `body_sha256`, so a
/// reconstruction is never silently partial or silently different. Inline
/// `data:` parts pass through unchanged, which keeps the call safe on a body
/// that mixes forms.
pub fn expand_audit_body(
    body: &Value,
    body_sha256: Option<&str>,
    mut blob: impl FnMut(&str) -> Result<Option<Vec<u8>>, String>,
) -> Result<Expansion, AuditError> {
    let mut expanded = body.clone();
    let mut references = 0usize;
    if let Some(messages) = expanded.get_mut("messages").and_then(Value::as_array_mut) {
        for message in messages.iter_mut() {
            let Some(parts) = message.get_mut("content").and_then(Value::as_array_mut) else {
                continue;
            };
            for part in parts.iter_mut() {
                let Some(image) = part.get_mut("image_url") else {
                    continue;
                };
                let Some(url) = image.get("url").and_then(Value::as_str).map(str::to_string) else {
                    return Err(AuditError::Malformed {
                        part: "an image part carries no url".into(),
                    });
                };
                let Some(id) = url.strip_prefix("artifact:").map(str::to_string) else {
                    if url.starts_with("data:") {
                        continue;
                    }
                    return Err(AuditError::Malformed { part: url });
                };
                let Some(media_type) = image
                    .get("media_type")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                else {
                    return Err(AuditError::Malformed {
                        part: format!("artifact:{id} carries no media_type"),
                    });
                };
                let recorded = image.get("size").and_then(Value::as_u64);
                let bytes = match blob(&id) {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => return Err(AuditError::MissingBlob { id }),
                    Err(detail) => return Err(AuditError::Unreadable { id, detail }),
                };
                let actual = sha256_hex(&bytes);
                if actual != id {
                    return Err(AuditError::CorruptBlob { id, actual });
                }
                if let Some(recorded) = recorded
                    && recorded != bytes.len() as u64
                {
                    return Err(AuditError::SizeMismatch {
                        id,
                        recorded,
                        actual: bytes.len() as u64,
                    });
                }
                *image = json!({
                    "url": format!("data:{media_type};base64,{}", base64_encode(&bytes)),
                });
                references += 1;
            }
        }
    }
    if references > 0
        && let Some(expected) = body_sha256
    {
        let bytes = serialize_request_body(&expanded).map_err(|e| AuditError::Malformed {
            part: e.to_string(),
        })?;
        let actual = sha256_hex(&bytes);
        if actual != expected {
            return Err(AuditError::DigestMismatch {
                expected: expected.to_string(),
                actual,
            });
        }
    }
    Ok(Expansion {
        body: expanded,
        references,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::{ChatImage, FROZEN_CHAT_BODY_SHA256};
    use kian_core::seams::ToolSchema;

    fn user() -> Vec<ChatMessage> {
        vec![ChatMessage::plain("user", "hello")]
    }

    fn profile() -> ModelProfile {
        ModelProfile {
            dialect: ModelDialect::OpenAiChat,
            context_window: Some(1_000_000),
            max_output: Some(384_000),
            reasoning: true,
            thinking: Some(true),
            effort: Some(ReasoningEffort::High),
            strict: false,
            cache: true,
            vision: true,
        }
    }

    #[test]
    fn no_profile_keeps_the_frozen_v1_body_and_hash() {
        let plan = build_request_plan(None, "deepseek-flash", &user(), &[]).unwrap();
        assert_eq!(plan.body_version, BodyVersion::V1);
        assert_eq!(plan.profile_digest, None);
        assert_eq!(plan.body, build_chat_body("deepseek-flash", &user(), &[]));
        let bytes = serialize_request_body(&plan.body).unwrap();
        assert_eq!(plan.body_sha256, sha256_hex(&bytes));
        assert_eq!(plan.estimate.max_output_tokens, None);
        assert_eq!(FROZEN_CHAT_BODY_SHA256.len(), 64);
    }

    #[test]
    fn declared_profile_builds_a_deterministic_v2_body() {
        let p = profile();
        let a = build_request_plan(Some(&p), "deepseek-flash", &user(), &[]).unwrap();
        let b = build_request_plan(Some(&p), "deepseek-flash", &user(), &[]).unwrap();
        assert_eq!(a.body_version, BodyVersion::V2);
        assert_eq!(a.body, b.body);
        assert_eq!(a.body_sha256, b.body_sha256);
        assert_eq!(a.profile_digest, b.profile_digest);
        assert_eq!(a.body["thinking"], json!({"type": "enabled"}));
        assert_eq!(a.body["reasoning_effort"], json!("high"));
        assert_eq!(a.body["max_tokens"], json!(384_000));
        assert_eq!(a.estimate.max_output_tokens, Some(384_000));
        assert!(a.estimate.low < a.estimate.high);
    }

    #[test]
    fn absent_wire_knobs_leave_the_v2_body_field_set_unchanged() {
        let p = ModelProfile {
            thinking: None,
            effort: None,
            max_output: None,
            ..profile()
        };
        let plan = build_request_plan(Some(&p), "deepseek-flash", &user(), &[]).unwrap();
        assert_eq!(plan.body_version, BodyVersion::V2);
        assert_eq!(plan.body.get("thinking"), None);
        assert_eq!(plan.body.get("reasoning_effort"), None);
        assert_eq!(plan.body.get("max_tokens"), None);
    }

    #[test]
    fn reasoning_is_stripped_when_the_profile_disables_thinking() {
        let mut message = ChatMessage::plain("assistant", "answer");
        message.reasoning_content = Some("chain of thought".into());
        let p = ModelProfile {
            thinking: Some(false),
            effort: None,
            ..profile()
        };
        let plan = build_request_plan(Some(&p), "deepseek-flash", &[message.clone()], &[]).unwrap();
        assert_eq!(plan.body["messages"][0].get("reasoning_content"), None);
        let kept = build_request_plan(Some(&profile()), "deepseek-flash", &[message], &[]).unwrap();
        assert_eq!(
            kept.body["messages"][0]["reasoning_content"],
            json!("chain of thought")
        );
    }

    #[test]
    fn reasoning_is_stripped_when_the_profile_denies_reasoning() {
        let mut message = ChatMessage::plain("assistant", "answer");
        message.reasoning_content = Some("chain of thought".into());
        let p = ModelProfile {
            reasoning: false,
            thinking: None,
            effort: None,
            ..profile()
        };
        let plan = build_request_plan(Some(&p), "deepseek-flash", &[message], &[]).unwrap();
        assert_eq!(plan.body["messages"][0].get("reasoning_content"), None);
    }

    #[test]
    fn tools_keep_the_frozen_entry_shape() {
        let tool = ToolSchema {
            name: "fs_read".into(),
            description: "read".into(),
            parameters: json!({"type": "object"}),
        };
        let plan = build_request_plan(Some(&profile()), "m", &user(), &[tool]).unwrap();
        assert_eq!(plan.body["tools"][0]["function"]["name"], json!("fs_read"));
        assert_eq!(plan.body["tools"][0].get("strict"), None);
    }

    fn image_message() -> ChatMessage {
        let mut message = ChatMessage::plain("user", "what is on this invoice?");
        message.images = vec![ChatImage {
            media_type: "image/png".into(),
            bytes: b"hi".to_vec(),
        }];
        message
    }

    #[test]
    fn base64_encoder_matches_the_reference_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        assert_eq!(base64_encode(b"hi"), "aGk=");
    }

    #[test]
    fn a_vision_profile_renders_an_image_bearing_message_as_content_parts() {
        let plan = build_request_plan(
            Some(&profile()),
            "deepseek-vision",
            &[ChatMessage::plain("system", "be terse"), image_message()],
            &[],
        )
        .unwrap();
        assert_eq!(plan.body_version, BodyVersion::V2);
        assert_eq!(plan.body["messages"][0]["content"], json!("be terse"));
        assert_eq!(
            plan.body["messages"][1]["content"],
            json!([
                {"type": "text", "text": "what is on this invoice?"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,aGk="}},
            ])
        );
        assert_eq!(plan.body["messages"][1].get("images"), None);
    }

    #[test]
    fn a_vision_profile_without_images_keeps_the_v2_body_unchanged() {
        let p = profile();
        let plain = ChatMessage::plain("user", "what is on this invoice?");
        let without_images = build_request_plan(Some(&p), "m", &[plain], &[]).unwrap();
        let mut with_image = image_message();
        with_image.images.clear();
        let stripped = build_request_plan(Some(&p), "m", &[with_image], &[]).unwrap();
        assert_eq!(without_images.body["messages"], stripped.body["messages"]);
    }

    #[test]
    fn a_profile_without_vision_never_renders_content_parts() {
        let p = ModelProfile {
            vision: false,
            ..profile()
        };
        let plan = build_request_plan(Some(&p), "m", &[image_message()], &[]).unwrap();
        assert_eq!(
            plan.body["messages"][0]["content"],
            json!("what is on this invoice?")
        );
        assert_eq!(plan.body["messages"][0].get("images"), None);
    }

    #[test]
    fn the_v1_body_never_carries_image_parts() {
        let messages = [image_message()];
        let plan = build_request_plan(None, "m", &messages, &[]).unwrap();
        assert_eq!(plan.body_version, BodyVersion::V1);
        assert_eq!(plan.body, build_chat_body("m", &messages, &[]));
        assert_eq!(
            plan.body["messages"][0]["content"],
            json!("what is on this invoice?")
        );
    }

    fn vision_plan() -> RequestPlan {
        build_request_plan(Some(&profile()), "deepseek-vision", &[image_message()], &[]).unwrap()
    }

    #[test]
    fn an_image_bearing_body_is_stored_as_references_and_keeps_its_wire_digest() {
        let plan = vision_plan();
        assert_eq!(plan.audit_form, Some(BodyForm::ArtifactRefs));
        let id = sha256_hex(b"hi");
        let audit = plan.audit_body.as_ref().expect("audit body");
        assert_eq!(
            audit["messages"][0]["content"],
            json!([
                {"type": "text", "text": "what is on this invoice?"},
                {"type": "image_url", "image_url": {
                    "url": format!("artifact:{id}"),
                    "media_type": "image/png",
                    "size": 2,
                }},
            ])
        );
        let wire = serialize_request_body(&plan.body).unwrap();
        assert_eq!(plan.body_bytes, wire.len() as u64);
        assert_eq!(plan.body_sha256, sha256_hex(&wire));
        assert_eq!(plan.audit(), (audit, Some(BodyForm::ArtifactRefs)));
        let stored = serialize_request_body(audit).unwrap();
        assert!(
            !String::from_utf8_lossy(&stored).contains("data:"),
            "the audit form must not carry a base64 payload: {}",
            String::from_utf8_lossy(&stored)
        );
    }

    #[test]
    fn a_text_only_body_has_no_audit_form() {
        let plan = build_request_plan(Some(&profile()), "m", &user(), &[]).unwrap();
        assert_eq!(plan.audit_form, None);
        assert!(plan.audit_body.is_none());
        assert_eq!(plan.audit(), (&plan.body, None));
    }

    #[test]
    fn an_audit_body_expands_back_to_the_wire_bytes() {
        let plan = vision_plan();
        let audit = plan.audit_body.clone().expect("audit body");
        let expansion = expand_audit_body(&audit, Some(&plan.body_sha256), |id| {
            assert_eq!(id, sha256_hex(b"hi"));
            Ok(Some(b"hi".to_vec()))
        })
        .unwrap();
        assert_eq!(expansion.references, 1);
        assert_eq!(expansion.body, plan.body);
    }

    #[test]
    fn an_expansion_names_a_reclaimed_blob_by_id() {
        let plan = vision_plan();
        let audit = plan.audit_body.clone().expect("audit body");
        let err = expand_audit_body(&audit, Some(&plan.body_sha256), |_| Ok(None)).unwrap_err();
        assert_eq!(
            err,
            AuditError::MissingBlob {
                id: sha256_hex(b"hi")
            }
        );
    }

    #[test]
    fn an_expansion_refuses_bytes_that_are_not_the_recorded_image() {
        let plan = vision_plan();
        let audit = plan.audit_body.clone().expect("audit body");
        let err = expand_audit_body(&audit, Some(&plan.body_sha256), |_| {
            Ok(Some(b"other".to_vec()))
        })
        .unwrap_err();
        assert_eq!(
            err,
            AuditError::CorruptBlob {
                id: sha256_hex(b"hi"),
                actual: sha256_hex(b"other"),
            }
        );
    }

    #[test]
    fn an_expansion_verifies_the_rebuild_against_the_recorded_digest() {
        let plan = vision_plan();
        let audit = plan.audit_body.clone().expect("audit body");
        let err = expand_audit_body(&audit, Some("00"), |_| Ok(Some(b"hi".to_vec()))).unwrap_err();
        match err {
            AuditError::DigestMismatch { expected, .. } => assert_eq!(expected, "00"),
            other => panic!("expected a digest mismatch, got {other:?}"),
        }
    }

    #[test]
    fn an_expansion_reports_a_part_that_names_no_blob() {
        let audit = json!({"messages": [{"content": [
            {"type": "image_url", "image_url": {"url": "https://example.com/x.png"}},
        ]}]});
        let err = expand_audit_body(&audit, None, |_| Ok(None)).unwrap_err();
        assert_eq!(
            err,
            AuditError::Malformed {
                part: "https://example.com/x.png".into()
            }
        );
    }
}
