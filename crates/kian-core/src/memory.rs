//! The memory-note fold (MEMORY-MODEL §3 "Notes", round-8 plan §2.2).
//!
//! A note is a *claim* about the agent, carried as one `memory/note` event
//! on the memory session's stream. This module is the **projection** side:
//! it parses payloads, folds the latest revision per subject, and offers a
//! pure range verifier for the CLI. It owns no state — the stream is the
//! sole truth (constraint 1) and every value here is rebuildable from it.
//!
//! Fail-closed is the governing rule (MEMORY-MODEL L1): anything the
//! payload does not positively prove degrades to `candidate`, and anything
//! structurally unusable (empty subject/text, empty `derived_from`, an
//! inverted range, an unparseable session id) is dropped outright — an
//! invalid note never surfaces. Unknown payload keys are tolerated
//! (additions-only, constraint 3): parsing reads only the tabled contract.
//!
//! Note: the payload field names here are the tabled contract from the
//! round-8 plan §2.1 (`start_seq`/`end_seq`), which is what kian-84 froze
//! into the event catalog; MEMORY-MODEL's prose (`from_seq`/`to_seq`)
//! names the same half-open `[start, end)` range.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::events::{Event, EventKind};
use crate::model::SessionId;

/// Closed status vocabulary (MEMORY-MODEL §3 field table). Unknown or
/// absent payload values never reach this type — `parse_note` fail-closes
/// them to [`NoteStatus::Candidate`] first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NoteStatus {
    Candidate,
    Verified,
    Stale,
}

/// Closed verification-method vocabulary (MEMORY-MODEL N3): how a verified
/// note was checked. Rides only on `status = verified` notes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerifyMethod {
    Transcript,
    EvidenceGate,
    Human,
}

/// The provenance of a `verified` note: the closed method plus a non-empty
/// `ref` pointing at the evidence (an event id, a range citation, a human
/// review record).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VerifiedBy {
    pub method: VerifyMethod,
    pub r#ref: String,
}

/// One evidence range: the half-open `[start_seq, end_seq)` slice of
/// session `session`'s stream the claim was extracted from. Valid ranges
/// are non-empty (`start_seq < end_seq`) and name a parseable session id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DerivedRange {
    pub session: SessionId,
    pub start_seq: u64,
    pub end_seq: u64,
}

/// A parsed memory note — the projected form of one `memory/note` payload.
///
/// `seq` is stamped by [`project_notes`] with the carrying event's seq (the
/// latest-wins key); [`parse_note`] alone cannot know it and leaves it 0.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Note {
    #[serde(default)]
    pub seq: u64,
    /// Non-empty stable fold key within the memory scope.
    pub subject: String,
    /// Non-empty claim text.
    pub text: String,
    pub status: NoteStatus,
    /// Non-empty evidence list (an invalid note is dropped, never surfaced
    /// with empty provenance).
    pub derived_from: Vec<DerivedRange>,
    /// Present iff `status = verified` (MEMORY-MODEL N3); a `verified_by`
    /// riding on a candidate/stale payload is stripped by the parse.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verified_by: Option<VerifiedBy>,
}

/// Parse one `memory/note` payload, fail-closed.
///
/// Demotions (the note still surfaces, downgraded):
/// - absent, unknown or non-string `status` → `candidate`;
/// - `status = "verified"` without a *valid* `verified_by` (absent,
///   malformed, unknown method, or empty `ref`) → `candidate`;
/// - `verified_by` present on a `candidate`/`stale` payload → stripped
///   (N3: only verified notes carry provenance).
///
/// Drops (structurally invalid, returns `None` — the note never surfaces):
/// - `kind` is not [`EventKind::MemoryNote`];
/// - payload is not a JSON object, or `subject`/`text`/`derived_from` is
///   absent, not of the right shape, or `subject`/`text` is empty;
/// - `derived_from` is empty, or any range has an unparseable session id
///   or `start_seq >= end_seq`.
///
/// Downstream callers count the drops (metrics, honest-zero reporting);
/// the projection itself only ever emits valid notes.
pub fn parse_note(kind: EventKind, bytes: &Value) -> Option<Note> {
    if !matches!(kind, EventKind::MemoryNote) {
        return None;
    }
    let obj = bytes.as_object()?;

    let subject = obj.get("subject")?.as_str()?;
    if subject.is_empty() {
        return None;
    }
    let text = obj.get("text")?.as_str()?;
    if text.is_empty() {
        return None;
    }
    let derived_from = parse_derived_from(obj.get("derived_from")?)?;

    // Fail-closed status: only a positively valid `verified_by` upholds a
    // `verified` claim; everything unknown degrades to candidate.
    let raw_status = obj.get("status").and_then(Value::as_str);
    let (status, verified_by) = match raw_status {
        Some("verified") => match parse_verified_by(obj.get("verified_by")) {
            Some(vb) => (NoteStatus::Verified, Some(vb)),
            None => (NoteStatus::Candidate, None),
        },
        Some("stale") => (NoteStatus::Stale, None),
        _ => (NoteStatus::Candidate, None),
    };

    Some(Note {
        seq: 0,
        subject: subject.to_string(),
        text: text.to_string(),
        status,
        derived_from,
        verified_by,
    })
}

/// Parse the non-empty, all-valid `derived_from` list, or `None`.
fn parse_derived_from(value: &Value) -> Option<Vec<DerivedRange>> {
    let arr = value.as_array()?;
    if arr.is_empty() {
        return None;
    }
    arr.iter()
        .map(|entry| {
            let obj = entry.as_object()?;
            let session = SessionId(uuid::Uuid::parse_str(obj.get("session")?.as_str()?).ok()?);
            let start_seq = obj.get("start_seq")?.as_u64()?;
            let end_seq = obj.get("end_seq")?.as_u64()?;
            if start_seq >= end_seq {
                return None;
            }
            Some(DerivedRange {
                session,
                start_seq,
                end_seq,
            })
        })
        .collect()
}

/// Parse a `verified_by`: method must be in the closed set and `ref`
/// non-empty, else `None` (invalid — never partially surfaced).
fn parse_verified_by(value: Option<&Value>) -> Option<VerifiedBy> {
    let obj = value?.as_object()?;
    let method = match obj.get("method")?.as_str()? {
        "transcript" => VerifyMethod::Transcript,
        "evidence_gate" => VerifyMethod::EvidenceGate,
        "human" => VerifyMethod::Human,
        _ => return None,
    };
    let r#ref = obj.get("ref")?.as_str()?;
    if r#ref.is_empty() {
        return None;
    }
    Some(VerifiedBy {
        method,
        r#ref: r#ref.to_string(),
    })
}

/// Fold a stream into the effective note set (MEMORY-MODEL §3 fold +
/// N6 determinism).
///
/// For each `subject` the note carried by the greatest `seq` wins — the
/// comparison is on seq, not stream position, so the fold is
/// order-independent for any permutation of the same events. Output is
/// sorted lexicographically by subject (byte-deterministic). Non-note
/// events and invalid payloads are ignored (invalid notes never surface).
pub fn project_notes(events: &[Event]) -> Vec<Note> {
    let mut latest: BTreeMap<String, Note> = BTreeMap::new();
    for ev in events {
        if !matches!(ev.kind, EventKind::MemoryNote) {
            continue;
        }
        if let Some(mut note) = parse_note(ev.kind, &ev.bytes) {
            note.seq = ev.seq;
            let keep = match latest.get(&note.subject) {
                Some(prev) => prev.seq < note.seq,
                None => true,
            };
            if keep {
                latest.insert(note.subject.clone(), note);
            }
        }
    }
    latest.into_values().collect()
}

/// Why [`verify_note`] rejected a note.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VerifyError {
    #[error("note has no derived_from ranges")]
    EmptyDerived,
    #[error("derived_from[{index}] is inverted: start_seq {start_seq} >= end_seq {end_seq}")]
    InvertedRange {
        index: usize,
        start_seq: u64,
        end_seq: u64,
    },
    #[error("derived_from[{index}] names session {session}, not the source stream's session")]
    SessionMismatch { index: usize, session: String },
    #[error(
        "derived_from[{index}] range [{start_seq},{end_seq}) is not covered by the source stream"
    )]
    MissingEvents {
        index: usize,
        start_seq: u64,
        end_seq: u64,
    },
}

/// Pure range verification against a source session's stream — the helper
/// the CLI's `kian memory verify` path uses (round-8 plan §2.2: "verify
/// re-checks ranges against the source stream").
///
/// `events_of_source` is the event stream of a single source session (as
/// read back from the store). Every `derived_from` range must name that
/// session and be fully covered: each seq in the half-open
/// `[start_seq, end_seq)` interval exists in the stream, and
/// `start_seq < end_seq`. No I/O, no state — a note that verifies here is
/// one whose evidence really is in the stream.
pub fn verify_note(note: &Note, events_of_source: &[Event]) -> Result<(), VerifyError> {
    if note.derived_from.is_empty() {
        return Err(VerifyError::EmptyDerived);
    }
    // Fail-closed on an empty or unreadable source: the first range is
    // simply not covered.
    let source = match events_of_source.first() {
        Some(ev) => ev.session,
        None => {
            let r = &note.derived_from[0];
            return Err(VerifyError::MissingEvents {
                index: 0,
                start_seq: r.start_seq,
                end_seq: r.end_seq,
            });
        }
    };
    let seqs: BTreeSet<u64> = events_of_source.iter().map(|ev| ev.seq).collect();

    for (index, range) in note.derived_from.iter().enumerate() {
        if range.start_seq >= range.end_seq {
            return Err(VerifyError::InvertedRange {
                index,
                start_seq: range.start_seq,
                end_seq: range.end_seq,
            });
        }
        if range.session != source {
            return Err(VerifyError::SessionMismatch {
                index,
                session: range.session.0.to_string(),
            });
        }
        let covered = seqs.range(range.start_seq..range.end_seq).count() as u128
            == (range.end_seq - range.start_seq) as u128;
        if !covered {
            return Err(VerifyError::MissingEvents {
                index,
                start_seq: range.start_seq,
                end_seq: range.end_seq,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EndReason;
    use serde_json::json;

    const SOURCE: &str = "0f9c2a11-3b4d-4e5f-8a6b-7c8d9e0f1a2b";

    fn source_id() -> SessionId {
        SessionId(uuid::Uuid::parse_str(SOURCE).unwrap())
    }

    fn note_event(seq: u64, payload: Value) -> Event {
        Event {
            seq,
            session: SessionId::new(),
            epoch: crate::model::Epoch::GENESIS,
            ts: 0,
            kind: EventKind::MemoryNote,
            bytes: payload,
            version: 0,
        }
    }

    fn valid_payload(subject: &str) -> Value {
        json!({
            "subject": subject,
            "text": "The team deploys through the dsh bridge.",
            "status": "candidate",
            "derived_from": [{"session": SOURCE, "start_seq": 1, "end_seq": 4}],
        })
    }

    /// The full fail-closed matrix: valid verified, every demotion, every
    /// drop.
    #[test]
    fn parse_note_fail_closed_matrix() {
        // Valid verified note keeps its provenance.
        let verified = json!({
            "subject": "deploys via the dsh bridge",
            "text": "text",
            "status": "verified",
            "derived_from": [{"session": SOURCE, "start_seq": 1, "end_seq": 4}],
            "verified_by": {"method": "transcript", "ref": "range [1,4) re-read"},
        });
        let note = parse_note(EventKind::MemoryNote, &verified).unwrap();
        assert_eq!(note.status, NoteStatus::Verified);
        assert_eq!(
            note.verified_by,
            Some(VerifiedBy {
                method: VerifyMethod::Transcript,
                r#ref: "range [1,4) re-read".to_string(),
            })
        );

        // Demotions: still surfaces, as candidate.
        let mut absent = valid_payload("s");
        absent.as_object_mut().unwrap().remove("status");
        let mut unknown = valid_payload("s");
        unknown["status"] = json!("superseded");
        let mut non_string = valid_payload("s");
        non_string["status"] = json!(42);
        for (label, payload) in [
            ("absent status", absent),
            ("unknown status", unknown),
            ("non-string status", non_string),
        ] {
            let note = parse_note(EventKind::MemoryNote, &payload)
                .unwrap_or_else(|| panic!("{label} must surface as candidate"));
            assert_eq!(note.status, NoteStatus::Candidate, "{label}");
            assert!(note.verified_by.is_none(), "{label}");
        }

        // verified without a valid verified_by -> candidate, provenance gone.
        for label in [
            "absent verified_by",
            "unknown method",
            "empty ref",
            "non-string ref",
            "verified_by not an object",
        ] {
            let mut payload = valid_payload("s");
            payload["status"] = json!("verified");
            match label {
                "absent verified_by" => {
                    payload.as_object_mut().unwrap().remove("verified_by");
                }
                "unknown method" => {
                    payload["verified_by"] = json!({"method": "vibe_check", "ref": "looked fine"});
                }
                "empty ref" => {
                    payload["verified_by"] = json!({"method": "human", "ref": ""});
                }
                "non-string ref" => {
                    payload["verified_by"] = json!({"method": "human", "ref": 7});
                }
                _ => {
                    payload["verified_by"] = json!("checked");
                }
            }
            let note = parse_note(EventKind::MemoryNote, &payload)
                .unwrap_or_else(|| panic!("{label} must surface as candidate"));
            assert_eq!(note.status, NoteStatus::Candidate, "{label}");
            assert!(note.verified_by.is_none(), "{label}");
        }

        // N3: verified_by riding on candidate/stale is stripped, the note
        // still surfaces with its stated status.
        for status in ["candidate", "stale"] {
            let mut payload = valid_payload("s");
            payload["status"] = json!(status);
            payload["verified_by"] = json!({"method": "human", "ref": "reviewed"});
            let note = parse_note(EventKind::MemoryNote, &payload).unwrap();
            assert!(note.verified_by.is_none(), "{status} must not carry it");
        }

        // stale passes through untouched.
        let mut payload = valid_payload("s");
        payload["status"] = json!("stale");
        assert_eq!(
            parse_note(EventKind::MemoryNote, &payload).unwrap().status,
            NoteStatus::Stale
        );

        // Drops: structurally invalid, never surfaces.
        let drops: &[(&str, Value)] = &[
            ("empty subject", {
                let mut p = valid_payload("s");
                p["subject"] = json!("");
                p
            }),
            ("absent subject", {
                let mut p = valid_payload("s");
                p.as_object_mut().unwrap().remove("subject");
                p
            }),
            ("empty text", {
                let mut p = valid_payload("s");
                p["text"] = json!("");
                p
            }),
            ("absent text", {
                let mut p = valid_payload("s");
                p.as_object_mut().unwrap().remove("text");
                p
            }),
            ("absent derived_from", {
                let mut p = valid_payload("s");
                p.as_object_mut().unwrap().remove("derived_from");
                p
            }),
            ("empty derived_from", {
                let mut p = valid_payload("s");
                p["derived_from"] = json!([]);
                p
            }),
            ("derived_from not an array", {
                let mut p = valid_payload("s");
                p["derived_from"] = json!({"session": SOURCE});
                p
            }),
            ("inverted range", {
                let mut p = valid_payload("s");
                p["derived_from"] = json!([{"session": SOURCE, "start_seq": 4, "end_seq": 4}]);
                p
            }),
            ("backward range", {
                let mut p = valid_payload("s");
                p["derived_from"] = json!([{"session": SOURCE, "start_seq": 5, "end_seq": 4}]);
                p
            }),
            ("unparseable session id", {
                let mut p = valid_payload("s");
                p["derived_from"] =
                    json!([{"session": "not-a-uuid", "start_seq": 1, "end_seq": 4}]);
                p
            }),
            ("absent end_seq", {
                let mut p = valid_payload("s");
                p["derived_from"] = json!([{"session": SOURCE, "start_seq": 1}]);
                p
            }),
            ("payload not an object", json!("note")),
        ];
        for (label, payload) in drops {
            assert!(
                parse_note(EventKind::MemoryNote, payload).is_none(),
                "{label} must be dropped"
            );
        }

        // A non-memory/note kind is not a note, regardless of payload.
        assert!(parse_note(EventKind::SessionCreated, &valid_payload("s")).is_none());

        // Unknown additive keys are tolerated (additions-only, constraint 3).
        let mut payload = valid_payload("s");
        payload["future_key"] = json!({"anything": true});
        assert!(parse_note(EventKind::MemoryNote, &payload).is_some());
    }

    /// Latest revision per subject wins by seq, with subjects interleaved;
    /// the carried seq is stamped onto the note.
    #[test]
    fn fold_latest_wins_with_interleaved_subjects() {
        let mut a1 = valid_payload("alpha");
        a1["text"] = json!("alpha v1");
        let mut b1 = valid_payload("beta");
        b1["text"] = json!("beta v1");
        let mut a2 = valid_payload("alpha");
        a2["text"] = json!("alpha v2");
        let mut b2 = valid_payload("beta");
        b2["text"] = json!("beta v2");

        let events = vec![
            note_event(1, a1),
            note_event(2, b1),
            note_event(5, a2),
            note_event(7, b2),
        ];
        let notes = project_notes(&events);
        // Deterministic lexicographic subject order.
        let subjects: Vec<&str> = notes.iter().map(|n| n.subject.as_str()).collect();
        assert_eq!(subjects, ["alpha", "beta"]);
        // Latest text + stamped seq per subject.
        assert_eq!(notes[0].text, "alpha v2");
        assert_eq!(notes[0].seq, 5);
        assert_eq!(notes[1].text, "beta v2");
        assert_eq!(notes[1].seq, 7);
    }

    /// The fold is independent of the order events are fed in (the latest
    /// seq wins regardless of position) — projections may be rebuilt from
    /// any read of the same stream.
    #[test]
    fn fold_is_order_independent() {
        let mut subjects = vec!["alpha", "beta", "gamma"];
        let mut events = Vec::new();
        let mut seq = 0;
        for round in 0..3 {
            for subject in &subjects {
                seq += 1;
                let mut p = valid_payload(subject);
                p["text"] = json!(format!("{subject} r{round}"));
                events.push(note_event(seq, p));
            }
        }
        // One later revision arriving after everything else.
        seq += 1;
        let mut late = valid_payload("beta");
        late["text"] = json!("beta final");
        events.push(note_event(seq, late));

        let baseline = project_notes(&events);
        assert_eq!(baseline.len(), 3);

        // Deterministic across permutations of the same event multiset.
        let mut shuffled = events.clone();
        let mut seed = 7u64;
        for attempt in 0..8 {
            for i in (1..shuffled.len()).rev() {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let j = (seed >> 33) as usize % (i + 1);
                shuffled.swap(i, j);
            }
            let folded = project_notes(&shuffled);
            assert_eq!(folded, baseline, "attempt {attempt}");
        }

        subjects.sort_unstable();
        let order: Vec<&str> = baseline.iter().map(|n| n.subject.as_str()).collect();
        assert_eq!(order, subjects);
        assert!(baseline.iter().any(|n| n.text == "beta final"));
    }

    /// Invalid payloads never surface: the fold drops them and keeps the
    /// valid notes; non-note kinds are ignored entirely.
    #[test]
    fn fold_drops_invalid_notes_and_ignores_other_kinds() {
        let mut bad_text = valid_payload("bad");
        bad_text["text"] = json!("");
        let mut bad_range = valid_payload("worse");
        bad_range["derived_from"] = json!([{"session": SOURCE, "start_seq": 9, "end_seq": 2}]);

        let mut other = note_event(3, json!({"reason": "completed"}));
        other.kind = EventKind::SessionEnded {
            reason: EndReason::Completed,
        };

        let events = vec![
            note_event(1, bad_text),
            other,
            note_event(2, valid_payload("good")),
            note_event(4, bad_range),
        ];
        let notes = project_notes(&events);
        assert_eq!(notes.len(), 1);
        assert_eq!(notes[0].subject, "good");
        assert_eq!(notes[0].seq, 2);
    }

    /// Range verification: covered ranges pass; empty provenance, inverted
    /// ranges, wrong sessions and uncovered ranges fail — including the
    /// fail-closed empty-stream case.
    #[test]
    fn verify_note_checks_ranges_against_the_source_stream() {
        let source = source_id();
        let stream: Vec<Event> = (1..=6)
            .map(|seq| Event {
                seq,
                session: source,
                epoch: crate::model::Epoch::GENESIS,
                ts: 0,
                kind: EventKind::AssistantMessage,
                bytes: json!({"content": "chunk"}),
                version: 0,
            })
            .collect();

        let note = |ranges: Vec<DerivedRange>| Note {
            seq: 1,
            subject: "s".to_string(),
            text: "t".to_string(),
            status: NoteStatus::Candidate,
            derived_from: ranges,
            verified_by: None,
        };
        let range = |start_seq, end_seq| DerivedRange {
            session: source,
            start_seq,
            end_seq,
        };

        // Covered ranges (including the whole stream and single-seq ranges)
        // verify, and multiple ranges all get checked.
        assert!(verify_note(&note(vec![range(1, 6)]), &stream).is_ok());
        assert!(verify_note(&note(vec![range(2, 3)]), &stream).is_ok());
        assert!(verify_note(&note(vec![range(1, 2), range(4, 7)]), &stream).is_ok());

        // Empty provenance.
        assert_eq!(
            verify_note(&note(vec![]), &stream),
            Err(VerifyError::EmptyDerived)
        );

        // Inverted range.
        assert_eq!(
            verify_note(&note(vec![range(6, 6)]), &stream),
            Err(VerifyError::InvertedRange {
                index: 0,
                start_seq: 6,
                end_seq: 6
            })
        );

        // Range naming a different session.
        let alien = DerivedRange {
            session: SessionId::new(),
            start_seq: 1,
            end_seq: 2,
        };
        assert!(matches!(
            verify_note(&note(vec![alien]), &stream),
            Err(VerifyError::SessionMismatch { index: 0, .. })
        ));

        // Uncovered range: start beyond the stream's end.
        assert_eq!(
            verify_note(&note(vec![range(6, 8)]), &stream),
            Err(VerifyError::MissingEvents {
                index: 0,
                start_seq: 6,
                end_seq: 8
            })
        );

        // A hole inside the stream (seq 4 absent) breaks coverage.
        let gappy: Vec<Event> = stream.iter().filter(|ev| ev.seq != 4).cloned().collect();
        assert_eq!(
            verify_note(&note(vec![range(3, 5)]), &gappy),
            Err(VerifyError::MissingEvents {
                index: 0,
                start_seq: 3,
                end_seq: 5
            })
        );

        // Fail-closed: an empty source stream covers nothing.
        assert!(matches!(
            verify_note(&note(vec![range(1, 2)]), &[]),
            Err(VerifyError::MissingEvents { index: 0, .. })
        ));

        // The second range is checked too (index reported faithfully).
        assert_eq!(
            verify_note(&note(vec![range(1, 2), range(9, 11)]), &stream),
            Err(VerifyError::MissingEvents {
                index: 1,
                start_seq: 9,
                end_seq: 11
            })
        );
    }
}
