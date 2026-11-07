//! The fail-closed dream-answer parser.
//!
//! The model's answer is **untrusted input**: nothing it says becomes a
//! note unless it positively parses. [`parse_notes`] never fabricates a
//! note (a parse failure is an honest zero, counted by the caller) and
//! never trusts the model's provenance — `derived_from` is filled from
//! the request header (the only ground truth about what was read), and
//! a `verified` claim survives only with
//! `verified_by.method == "transcript"` and an in-range seq ref,
//! exactly what the dream prompt demands (round-8 plan §2.4, L1/L2).

use kian_core::memory::{DerivedRange, NoteStatus, VerifiedBy, VerifyMethod};
use serde::Serialize;
use serde_json::Value;

use crate::prompt::DreamRequest;

/// Why an answer did not parse. Every variant is an honest zero: the
/// caller appends nothing and counts.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    #[error("no JSON array found in the answer")]
    NoArray,
    #[error("answer is not valid JSON: {0}")]
    Malformed(String),
    #[error("note [{index}] is not a JSON object")]
    NoteNotObject { index: usize },
    #[error("note [{index}]: `{field}` must be a non-empty string")]
    BadField { index: usize, field: &'static str },
    #[error("answer carries {count} notes, over the cap of {max}")]
    TooManyNotes { count: usize, max: u64 },
}

/// One parsed note draft — the pre-event form of a `memory/note`
/// payload. `derived_from` is filled from the request header by the
/// parser, never read from the model's answer.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NoteDraft {
    pub subject: String,
    pub text: String,
    pub status: NoteStatus,
    pub derived_from: Vec<DerivedRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_by: Option<VerifiedBy>,
}

impl NoteDraft {
    /// The `memory/note` event payload (EVENT-MODEL §3 field names; the
    /// kian-core vocabulary serializes to exactly the tabled contract).
    #[must_use]
    pub fn to_payload(&self) -> Value {
        serde_json::to_value(self).expect("note drafts always serialize")
    }
}

/// Parses the model's dream answer into note drafts, fail-closed.
///
/// - The answer must contain a JSON array of note objects
///   (`subject`/`text` non-empty strings, `status` closed vocabulary,
///   optional `verified_by`); an empty array is a valid honest zero.
/// - `derived_from` is ALWAYS the single range from the request header
///   — the model never supplies provenance.
/// - `status:"verified"` demotes to `candidate` unless
///   `verified_by.method == "transcript"` and `verified_by.ref` parses
///   as a seq inside `[start_seq, end_seq)`. A `verified_by` riding a
///   `candidate`/`stale` note is stripped (N3).
/// - More than `request.max_notes` notes is a refusal
///   ([`ParseError::TooManyNotes`]), never a silent truncation (the
///   D3 discipline: over budget is refused honestly).
pub fn parse_notes(answer: &str, request: &DreamRequest) -> Result<Vec<NoteDraft>, ParseError> {
    let slice = answer_array_text(answer).ok_or(ParseError::NoArray)?;
    let parsed: Value =
        serde_json::from_str(slice).map_err(|e| ParseError::Malformed(e.to_string()))?;
    let arr = parsed.as_array().ok_or(ParseError::NoArray)?;
    if arr.len() as u64 > request.max_notes {
        return Err(ParseError::TooManyNotes {
            count: arr.len(),
            max: request.max_notes,
        });
    }

    // Provenance is ALWAYS the header's range — the model never
    // supplies it (D4: a note without real provenance is not written;
    // here provenance is real by construction).
    let derived_from = vec![DerivedRange {
        session: request.source_session,
        start_seq: request.start_seq,
        end_seq: request.end_seq,
    }];

    let mut drafts = Vec::with_capacity(arr.len());
    for (index, entry) in arr.iter().enumerate() {
        let obj = entry
            .as_object()
            .ok_or(ParseError::NoteNotObject { index })?;
        let subject = non_empty_str(obj.get("subject"), index, "subject")?;
        let text = non_empty_str(obj.get("text"), index, "text")?;

        // Fail-closed status: unknown/absent → candidate (the fold's
        // rule); verified only survives a positively valid transcript
        // check; provenance never rides candidate/stale (N3).
        let (status, verified_by) = match obj.get("status").and_then(Value::as_str) {
            Some("verified") => match transcript_verified(obj.get("verified_by"), request) {
                Some(vb) => (NoteStatus::Verified, Some(vb)),
                None => (NoteStatus::Candidate, None),
            },
            Some("stale") => (NoteStatus::Stale, None),
            _ => (NoteStatus::Candidate, None),
        };

        drafts.push(NoteDraft {
            subject,
            text,
            status,
            derived_from: derived_from.clone(),
            verified_by,
        });
    }
    Ok(drafts)
}

/// Reads a non-empty string field, fail-closed on absent/empty/wrong
/// type.
fn non_empty_str(
    value: Option<&Value>,
    index: usize,
    field: &'static str,
) -> Result<String, ParseError> {
    let text = value
        .and_then(Value::as_str)
        .ok_or(ParseError::BadField { index, field })?;
    if text.is_empty() {
        return Err(ParseError::BadField { index, field });
    }
    Ok(text.to_string())
}

/// The verified claim's positive check: `method == "transcript"` and a
/// `ref` that parses as a seq inside the request's half-open range.
fn transcript_verified(value: Option<&Value>, request: &DreamRequest) -> Option<VerifiedBy> {
    let obj = value?.as_object()?;
    if obj.get("method").and_then(Value::as_str)? != "transcript" {
        return None;
    }
    let r#ref = obj.get("ref").and_then(Value::as_str)?;
    let seq: u64 = r#ref.parse().ok()?;
    if !request.range().contains(&seq) {
        return None;
    }
    Some(VerifiedBy {
        method: VerifyMethod::Transcript,
        r#ref: r#ref.to_string(),
    })
}

/// Extracts the JSON-array text from the answer: fenced blocks are
/// unwrapped; otherwise the substring from the first `[` to the last
/// `]`. `None` when no array shape exists at all.
fn answer_array_text(answer: &str) -> Option<&str> {
    let trimmed = answer.trim();
    let body = strip_fence(trimmed).unwrap_or(trimmed).trim();
    let start = body.find('[')?;
    let end = body.rfind(']')?;
    if start >= end {
        return None;
    }
    Some(&body[start..=end])
}

/// Unwraps a ```-fenced block (dropping a `json` language tag), if the
/// text is one.
fn strip_fence(text: &str) -> Option<&str> {
    let rest = text.strip_prefix("```")?;
    let end = rest.find("```")?;
    let inner = &rest[..end];
    let inner = inner
        .strip_prefix("json\n")
        .or_else(|| inner.strip_prefix("json\r\n"))
        .unwrap_or(inner);
    Some(inner.trim())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prompt::DreamHeader;
    use kian_core::memory::VerifyMethod;
    use kian_core::model::SessionId;
    use serde_json::json;

    const SOURCE: &str = "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b";

    fn request() -> DreamRequest {
        DreamRequest::from_header(
            DreamHeader {
                scope: "ops-agent".into(),
                source_session: SessionId(uuid::Uuid::parse_str(SOURCE).unwrap()),
                start_seq: 1,
                end_seq: 4,
            },
            20,
        )
    }

    fn derived() -> Vec<DerivedRange> {
        vec![DerivedRange {
            session: SessionId(uuid::Uuid::parse_str(SOURCE).unwrap()),
            start_seq: 1,
            end_seq: 4,
        }]
    }

    fn note(subject: &str, extra: Value) -> String {
        let mut v = json!({"subject": subject, "text": "text"});
        if let (Some(obj), Some(extra)) = (v.as_object_mut(), extra.as_object()) {
            for (k, val) in extra {
                obj.insert(k.clone(), val.clone());
            }
        }
        Value::Array(vec![v]).to_string()
    }

    /// Plain candidates parse with header-supplied provenance, order
    /// preserved.
    #[test]
    fn parses_candidates_with_header_provenance() {
        let answer = r#"[
            {"subject": "deploys via dsh bridge", "text": "Deploys go through the dsh bridge.", "status": "candidate"},
            {"subject": "incident process", "text": "Incidents are filed as INC-NN."}
        ]"#;
        let drafts = parse_notes(answer, &request()).expect("parses");
        assert_eq!(drafts.len(), 2);
        assert_eq!(drafts[0].subject, "deploys via dsh bridge");
        assert_eq!(drafts[0].status, NoteStatus::Candidate);
        assert_eq!(drafts[0].derived_from, derived());
        assert!(drafts[0].verified_by.is_none());
        // Absent status fail-closes to candidate (the fold's rule).
        assert_eq!(drafts[1].status, NoteStatus::Candidate);
    }

    /// `verified` survives only with method `transcript` and an
    /// in-range seq ref; every other shape demotes.
    #[test]
    fn verified_demotes_unless_transcript_ref_in_range() {
        let cases: Vec<(&str, Value, bool)> = vec![
            (
                "kept",
                json!({"status": "verified", "verified_by": {"method": "transcript", "ref": "3"}}),
                true,
            ),
            (
                "lower bound ref",
                json!({"status": "verified", "verified_by": {"method": "transcript", "ref": "1"}}),
                true,
            ),
            (
                "ref at end (exclusive)",
                json!({"status": "verified", "verified_by": {"method": "transcript", "ref": "4"}}),
                false,
            ),
            (
                "ref out of range",
                json!({"status": "verified", "verified_by": {"method": "transcript", "ref": "9"}}),
                false,
            ),
            (
                "non-numeric ref",
                json!({"status": "verified", "verified_by": {"method": "transcript", "ref": "evt_1"}}),
                false,
            ),
            (
                "wrong method",
                json!({"status": "verified", "verified_by": {"method": "human", "ref": "3"}}),
                false,
            ),
            ("absent verified_by", json!({"status": "verified"}), false),
        ];
        for (label, extra, keep) in cases {
            let drafts =
                parse_notes(&note("s", extra.clone()), &request()).expect("parses in all cases");
            let draft = &drafts[0];
            if keep {
                assert_eq!(draft.status, NoteStatus::Verified, "{label}");
                assert_eq!(
                    draft.verified_by,
                    Some(VerifiedBy {
                        method: VerifyMethod::Transcript,
                        r#ref: extra["verified_by"]["ref"].as_str().unwrap().to_string(),
                    }),
                    "{label}"
                );
            } else {
                assert_eq!(draft.status, NoteStatus::Candidate, "{label}");
                assert!(draft.verified_by.is_none(), "{label}");
            }
        }

        // An explicit stale passes through.
        let drafts = parse_notes(
            &note(
                "s",
                json!({"status": "stale", "verified_by": {"method": "human", "ref": "x"}}),
            ),
            &request(),
        )
        .expect("parses");
        assert_eq!(drafts[0].status, NoteStatus::Stale);
        assert!(drafts[0].verified_by.is_none(), "N3: stripped on stale");
    }

    /// Structural garbage is an honest refusal: nothing parses, nothing
    /// is fabricated.
    #[test]
    fn malformed_answers_fail_closed() {
        for (label, answer) in [
            ("prose", "I cannot answer in that format."),
            ("object not array", r#"{"subject": "s"}"#),
            ("truncated json", r#"[{"subject": "s""#),
            ("element not object", r#"["note"]"#),
            ("empty subject", r#"[{"subject": "", "text": "t"}]"#),
            ("absent text", r#"[{"subject": "s"}]"#),
            ("non-string text", r#"[{"subject": "s", "text": 3}]"#),
        ] {
            let result = parse_notes(answer, &request());
            assert!(result.is_err(), "{label} must be refused, got {result:?}");
        }
    }

    /// A fenced answer (the common model habit) parses; surrounding
    /// prose around a JSON array is tolerated.
    #[test]
    fn fenced_and_prose_wrapped_answers_parse() {
        let fenced = "```json\n[{\"subject\": \"s\", \"text\": \"t\"}]\n```";
        assert!(parse_notes(fenced, &request()).is_ok());
        let wrapped = "Here are the notes:\n[{\"subject\": \"s\", \"text\": \"t\"}]\nDone.";
        assert!(parse_notes(wrapped, &request()).is_ok());
    }

    /// An empty array is a valid honest zero, not a failure.
    #[test]
    fn empty_array_is_an_honest_zero() {
        assert_eq!(parse_notes("[]", &request()), Ok(Vec::new()));
    }

    /// Over the `max_notes` cap is a refusal, never a truncation.
    #[test]
    fn over_cap_is_refused_not_truncated() {
        let two = r#"[
            {"subject": "a", "text": "t"},
            {"subject": "b", "text": "t"}
        ]"#;
        let mut req = request();
        req.max_notes = 1;
        assert_eq!(
            parse_notes(two, &req),
            Err(ParseError::TooManyNotes { count: 2, max: 1 })
        );
    }

    /// The payload matches the EVENT-MODEL field names exactly and
    /// re-parses through the kian-core fold parser.
    #[test]
    fn payload_matches_the_event_contract() {
        let answer = r#"[{"subject": "s", "text": "t", "status": "verified",
            "verified_by": {"method": "transcript", "ref": "2"}}]"#;
        let drafts = parse_notes(answer, &request()).expect("parses");
        let payload = drafts[0].to_payload();
        assert_eq!(payload["subject"], json!("s"));
        assert_eq!(payload["status"], json!("verified"));
        assert_eq!(
            payload["derived_from"],
            json!([{"session": SOURCE, "start_seq": 1, "end_seq": 4}])
        );
        assert_eq!(
            payload["verified_by"],
            json!({"method": "transcript", "ref": "2"})
        );
        // Round-trip through the core parser (the fold's exact view).
        let note =
            kian_core::memory::parse_note(kian_core::events::EventKind::MemoryNote, &payload)
                .expect("the fold accepts the draft payload");
        assert_eq!(note.status, NoteStatus::Verified);
    }
}
