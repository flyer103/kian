//! Request planning: one value that the sender, the persistence sink, the
//! cache checker and the budget gate all consume (ADR-053).
//!
//! A plan is built by a pure function of the slot profile, the model, the
//! exact message list and the tool schemas. It never reads process state, so
//! the same committed conversation yields the same bytes on replay.

use kian_core::model::{ModelDialect, ModelProfile, ReasoningEffort};
use kian_core::seams::{ProviderError, ToolSchema};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::provider::{ChatMessage, build_chat_body, serialize_request_body};

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
        return finish(body, BodyVersion::V1, ModelDialect::OpenAiChat, None, None);
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

    let digest = sha256_hex(
        &serde_json::to_vec(profile)
            .map_err(|e| ProviderError::Other(format!("profile serialization failed: {e}")))?,
    );
    finish(
        body,
        BodyVersion::V2,
        profile.dialect,
        Some(digest),
        profile.max_output,
    )
}

fn finish(
    body: Value,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::FROZEN_CHAT_BODY_SHA256;
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
}
