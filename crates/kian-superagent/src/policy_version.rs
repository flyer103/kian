//! policy/prompt versioning + drift marking.
//!
//! Prompts and policy rules are unversioned config today; the prototype
//! versions the Policy (rules + accept mode + source parts, 001/007) and
//! marks overridden defaults "default updated" on upgrade. Kian's v1
//! shape, zero new event kinds (§2):
//!
//! - **Version** — a [`PolicyVersion`] is a label plus the sha256
//!   fingerprints of the prompt bytes it was rendered from (templates
//!   with their config-derived variables substituted, e.g. the IM reply
//!   budget). The label rides the *rendered* bootstrap prompt as a header
//!   (`[policy <label>] …`, added at render — the template text is
//!   untouched, so `JUDGE_PROMPT_TEMPLATE` goldens stay valid and legacy
//!   persisted prompts replay byte-identically, constraint 3). Config:
//!   `superagent.policy.version: <label>`; absent = today's bytes,
//!   byte-identical.
//!
//! - **Drift** — at boot, assembly compares the *live* resident
//!   sessions' persisted bootstrap headers against the configured
//!   version ([`scan_drift`]). A drift is an audit line plus the
//!   `superagent.policy_drift` counter — **live sessions are never
//!   rewritten** (cache-byte stability): a drifted session keeps its
//!   persisted prompt, only the next bootstrap renders the new header.
//!
//! - **Notice** — config-gated (`superagent.policy.notify_live`,
//!   default `false`): a drift may deliver ONE ordinary admitted input
//!   to each affected live session with the marker text
//!   `[policy <old>→<new>] defaults updated; …` — synthetic-marked and
//!   strippable (constraint 2, the watchdog steer-row vocabulary), the
//!   prototype's policy-changed Input in kian words. Idempotent by the
//!   stable `(session, event id)` admit key.
//!
//! The version is replayable: it lives in persisted bootstrap prompts
//! and marker inputs — derivable from the stream, no kind (DESIGN §4.7).

use std::collections::BTreeMap;
use std::sync::Arc;

use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::inbox::{AdmitReceipt, InboxWriteError, InboxWriter};

/// Display label for "no policy header" in drift records, audit lines
/// and marker keys (`[policy none→v2]`). Stable — it is part of the
/// marker idempotency key, so it never changes spelling after.
pub const NO_POLICY_LABEL: &str = "none";

// ---------------------------------------------------------------------------
// PolicyVersion
// ---------------------------------------------------------------------------

/// One policy revision: the label rendered into bootstrap prompt
/// headers, plus the sha256 fingerprints (first 16 hex chars — a
/// display fingerprint, not a security boundary) of the prompt
/// templates the label was pinned against. The hashes let an operator
/// audit that label `X` still means the same template bytes; the label
/// is the only thing that rides persisted prompts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyVersion {
    label: String,
    prompt_hashes: BTreeMap<String, String>,
}

impl PolicyVersion {
    /// A version over `label` with no template fingerprints (the
    /// label-only shape tests and minimal configs use).
    #[must_use]
    pub fn new(label: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            prompt_hashes: BTreeMap::new(),
        }
    }

    /// A version over `label` fingerprinted against `templates`
    /// (name → template text). Deterministic and pure: the same label
    /// over the same template bytes always yields the same hashes.
    #[must_use]
    pub fn from_templates(label: impl Into<String>, templates: &[(&str, &str)]) -> Self {
        Self {
            label: label.into(),
            prompt_hashes: templates
                .iter()
                .map(|(name, text)| ((*name).to_string(), prompt_hash(text)))
                .collect(),
        }
    }

    /// The label rendered into the `[policy <label>]` header.
    #[must_use]
    pub fn label(&self) -> &str {
        &self.label
    }

    /// The template fingerprints (name → 16-hex sha256 prefix).
    #[must_use]
    pub fn prompt_hashes(&self) -> &BTreeMap<String, String> {
        &self.prompt_hashes
    }
}

/// The template fingerprint: the first 16 hex chars of sha256(text).
fn prompt_hash(text: &str) -> String {
    let digest = Sha256::digest(text.as_bytes());
    digest.iter().take(8).map(|b| format!("{b:02x}")).collect()
}

// ---------------------------------------------------------------------------
// Render header
// ---------------------------------------------------------------------------

/// The header prefix rendered once at bootstrap: `[policy <label>] `.
#[must_use]
pub fn policy_header(label: &str) -> String {
    format!("[policy {label}] ")
}

/// Wraps the **rendered** bootstrap prompt with the policy header.
/// `None` (absent config) returns `rendered` unchanged — today's bytes,
/// byte-identical. The header is added at render, so the template text
/// itself is never touched and legacy persisted prompts replay
/// byte-identically (constraint 3).
#[must_use]
pub fn with_policy_header(label: Option<&str>, rendered: String) -> String {
    match label {
        Some(label) => format!("{}{rendered}", policy_header(label)),
        None => rendered,
    }
}

/// Extracts the label from a persisted prompt's `[policy <label>]`
/// header, if any. `None` for legacy prompts (legacy bootstraps carry
/// no header — honestly reported as [`NO_POLICY_LABEL`] by
/// [`DriftRecord::persisted_label`], never guessed).
#[must_use]
pub fn parse_policy_header(prompt: &str) -> Option<&str> {
    let rest = prompt.strip_prefix("[policy ")?;
    let end = rest.find(']')?;
    let label = &rest[..end];
    if label.is_empty() {
        return None;
    }
    Some(label)
}

// ---------------------------------------------------------------------------
// Drift
// ---------------------------------------------------------------------------

/// One detected policy drift: the persisted bootstrap header's label vs
/// the currently configured one. Pure data — rendering the audit line
/// and marker text is below.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriftRecord {
    persisted: Option<String>,
    current: Option<String>,
}

impl DriftRecord {
    /// The persisted session's label ([`NO_POLICY_LABEL`] when its
    /// bootstrap predates policy versioning).
    #[must_use]
    pub fn persisted_label(&self) -> &str {
        self.persisted.as_deref().unwrap_or(NO_POLICY_LABEL)
    }

    /// The currently configured label ([`NO_POLICY_LABEL`] when the
    /// config pins no version).
    #[must_use]
    pub fn current_label(&self) -> &str {
        self.current.as_deref().unwrap_or(NO_POLICY_LABEL)
    }

    /// The boot-time audit line (one per drifted session).
    #[must_use]
    pub fn audit_line(&self) -> String {
        format!(
            "superagent: policy drift [policy {}] → [policy {}]: live session keeps its \
             persisted prompt; the next bootstrap renders the new header",
            self.persisted_label(),
            self.current_label()
        )
    }

    /// The policy-changed marker text (001/007 "default updated"): the
    /// content of the optional synthetic notice input.
    #[must_use]
    pub fn marker_text(&self) -> String {
        format!(
            "[policy {}→{}] defaults updated; your accepted rules changed: this session keeps \
             the rules it bootstrapped with, defaults applied from the next bootstrap on may \
             differ.",
            self.persisted_label(),
            self.current_label()
        )
    }
}

/// Pure drift detection: `Some` iff the persisted bootstrap header's
/// label differs from the currently configured label (either
/// direction — unsetting a version is a drift too). Both `None` = no
/// drift.
#[must_use]
pub fn drift(persisted_header: Option<&str>, current: Option<&str>) -> Option<DriftRecord> {
    if persisted_header == current {
        return None;
    }
    Some(DriftRecord {
        persisted: persisted_header.map(str::to_string),
        current: current.map(str::to_string),
    })
}

/// The stable event id of a drift's marker input:
/// `policy:drift:<old>-><new>`. Idempotent per `(session, key)` admit —
/// re-boots dedupe onto the same row; a *second* drift (a new old→new
/// pair) is a distinct key, so each upgrade notifies exactly once.
#[must_use]
pub fn policy_marker_key(record: &DriftRecord) -> String {
    format!(
        "policy:drift:{}->{}",
        record.persisted_label(),
        record.current_label()
    )
}

// ---------------------------------------------------------------------------
// Stream-derived surfaces (replayable, no kind)
// ---------------------------------------------------------------------------

/// The persisted bootstrap prompt of a resident session: the first
/// `input/admitted` payload's `content` field (the drain's consumption
/// write wraps every payload as `{"role":"user","content":…}`; the
/// bootstrap row is the session's first consumed input). `None` for a
/// session that never bootstrapped (a fresh stream) — such a session
/// cannot drift; its bootstrap will simply render the current header.
#[must_use]
pub fn persisted_bootstrap_prompt(events: &[Event]) -> Option<&str> {
    events
        .iter()
        .find(|event| event.kind == EventKind::InputAdmitted)
        .and_then(|event| event.bytes.get("content"))
        .and_then(Value::as_str)
}

/// Scans the **live** resident sessions' streams and returns exactly
/// the drifted ones. Callers pass only live sessions — ended (frozen)
/// predecessors are read-only history and never notified; a session
/// without a persisted bootstrap prompt (fresh — never bootstrapped)
/// cannot drift: its bootstrap simply renders the current header.
#[must_use]
pub fn scan_drift<'e>(
    residents: impl IntoIterator<Item = (SessionId, &'e [Event])>,
    current: Option<&str>,
) -> Vec<(SessionId, DriftRecord)> {
    residents
        .into_iter()
        .filter_map(|(session, events)| {
            // A fresh session (no persisted bootstrap) is skipped
            // entirely — `drift(None, Some(..))` is reserved for a
            // *legacy persisted prompt* (no header) under a versioned
            // config.
            let prompt = persisted_bootstrap_prompt(events)?;
            let persisted = parse_policy_header(prompt);
            drift(persisted, current).map(|record| (session, record))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Notice (config-gated)
// ---------------------------------------------------------------------------

/// Delivers one ordinary admitted input — the policy-changed marker,
/// synthetic-marked and strippable (the watchdog steer-row shape) — to
/// a drifted live session, under the stable
/// [`policy_marker_key`] idempotency key. The wake is advisory
/// (constraint 6): durable row first, then wake.
pub async fn notify_drift(
    inbox: &dyn InboxWriter,
    session: SessionId,
    record: &DriftRecord,
) -> Result<AdmitReceipt, InboxWriteError> {
    let payload: Value = json!({"synthetic": true, "content": record.marker_text()});
    let receipt = inbox
        .admit(session, payload, Some(policy_marker_key(record)))
        .await?;
    inbox.wake(session);
    Ok(receipt)
}

/// The outcome of one boot's drift marking.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DriftMarking {
    /// Every drifted live session with its record (audit + counter
    /// surface — assembly bumps `superagent.policy_drift` by the len).
    pub marked: Vec<(SessionId, DriftRecord)>,
    /// The sessions actually notified when `notify_live` is on, with
    /// each admit's `deduplicated` flag (true = this boot re-derived an
    /// already-delivered marker — still exactly one row).
    pub notified: Vec<(SessionId, bool)>,
}

/// The boot-time drift marking flow over the live resident sessions:
/// audit line per drift, and — only when `notify_live` is set — one
/// marker input per drifted session (idempotent by the stable event
/// id), admitted through **that session's mounted inbox**
/// (`inbox_of`): a parked resident re-drives on its mounted inbox's
/// residency counter, so a raw-engine admit would strand behind the
/// park. **Never rewrites a live session**: the only writes are the
/// optional marker admits; the persisted bootstrap prompts are
/// read-only (cache-byte stability).
pub async fn run_drift_marking<'e, F>(
    residents: impl IntoIterator<Item = (SessionId, &'e [Event])>,
    current: Option<&str>,
    notify_live: bool,
    inbox_of: F,
) -> Result<DriftMarking, InboxWriteError>
where
    F: Fn(SessionId) -> Arc<dyn InboxWriter>,
{
    let marked = scan_drift(residents, current);
    let mut notified = Vec::new();
    for (session, record) in &marked {
        tracing::info!(
            session = %session.0,
            from = record.persisted_label(),
            to = record.current_label(),
            "{}",
            record.audit_line()
        );
        if notify_live {
            let receipt = notify_drift(inbox_of(*session).as_ref(), *session, record).await?;
            notified.push((*session, receipt.deduplicated));
        }
    }
    Ok(DriftMarking { marked, notified })
}

#[cfg(test)]
mod tests {
    use super::{
        DriftMarking, DriftRecord, NO_POLICY_LABEL, PolicyVersion, drift, parse_policy_header,
        policy_header, policy_marker_key, prompt_hash, scan_drift, with_policy_header,
    };
    use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
    use kian_core::model::{Epoch, SessionId};
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn header_wraps_only_when_a_label_is_set() {
        // Absent config: today's bytes, byte-identical.
        assert_eq!(with_policy_header(None, "judge 0".into()), "judge 0");
        // Set: the header is a prefix — the template text is untouched.
        let wrapped = with_policy_header(Some("v1"), "judge 0".into());
        assert_eq!(wrapped, "[policy v1] judge 0");
        assert!(wrapped.starts_with(&policy_header("v1")));
    }

    #[test]
    fn header_parses_back_round_trip() {
        for label in ["v1", "2026-11", "a.b-c_d"] {
            let wrapped = with_policy_header(Some(label), String::new());
            assert_eq!(parse_policy_header(&wrapped), Some(label));
        }
        // Legacy prompts (and arbitrary text) parse to None — never a
        // guessed label.
        assert_eq!(parse_policy_header("You are the Router…"), None);
        assert_eq!(parse_policy_header(""), None);
        // A header must be a *prefix*; the marker text in the middle of
        // an input is not a bootstrap header.
        assert_eq!(parse_policy_header("note: [policy v1] mentioned"), None);
    }

    #[test]
    fn prompt_hashes_are_stable_and_content_addressed() {
        let a = PolicyVersion::from_templates("v1", &[("judge", "text a"), ("watchdog", "w")]);
        let b = PolicyVersion::from_templates("v1", &[("judge", "text a"), ("watchdog", "w")]);
        let c = PolicyVersion::from_templates("v1", &[("judge", "text b"), ("watchdog", "w")]);
        assert_eq!(a.prompt_hashes(), b.prompt_hashes());
        assert_ne!(a.prompt_hashes(), c.prompt_hashes());
        // 16-hex display fingerprint, deterministic.
        let hash = prompt_hash("text a");
        assert_eq!(hash.len(), 16);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(hash, prompt_hash("text a"));
        assert_eq!(a.label(), "v1");
    }

    #[test]
    fn drift_only_when_labels_differ() {
        assert_eq!(drift(None, None), None);
        assert_eq!(drift(Some("v1"), Some("v1")), None);
        let upgraded = drift(Some("v1"), Some("v2")).expect("upgrade drifts");
        assert_eq!(upgraded.persisted_label(), "v1");
        assert_eq!(upgraded.current_label(), "v2");
        // Both legacy directions are honest drifts, displayed as `none`.
        let legacy = drift(None, Some("v2")).expect("legacy bootstrap vs versioned config");
        assert_eq!(legacy.persisted_label(), NO_POLICY_LABEL);
        assert_eq!(legacy.current_label(), "v2");
        let unset = drift(Some("v1"), None).expect("version unset is a drift");
        assert_eq!(unset.current_label(), NO_POLICY_LABEL);
    }

    #[test]
    fn marker_key_is_stable_and_upgrade_scoped() {
        let a = drift(Some("v1"), Some("v2")).unwrap();
        let b = drift(Some("v1"), Some("v2")).unwrap();
        assert_eq!(policy_marker_key(&a), policy_marker_key(&b));
        assert_eq!(policy_marker_key(&a), "policy:drift:v1->v2");
        // A second upgrade is a distinct stable key.
        let next = drift(Some("v2"), Some("v3")).unwrap();
        assert_eq!(policy_marker_key(&next), "policy:drift:v2->v3");
        // Legacy side renders as `none`, never guessed.
        assert_eq!(
            policy_marker_key(&drift(None, Some("v2")).unwrap()),
            "policy:drift:none->v2"
        );
    }

    #[test]
    fn audit_and_marker_text_name_both_sides() {
        let record = DriftRecord {
            persisted: Some("v1".into()),
            current: Some("v2".into()),
        };
        assert!(record.audit_line().contains("[policy v1]"));
        assert!(record.audit_line().contains("[policy v2]"));
        assert!(record.audit_line().contains("keeps its persisted prompt"));
        let marker = record.marker_text();
        assert!(marker.starts_with("[policy v1→v2] defaults updated;"));
        assert!(marker.contains("rules changed"));
    }

    fn session(n: u8) -> SessionId {
        let mut bytes = [0u8; 16];
        bytes[0] = n;
        SessionId(Uuid::from_bytes(bytes))
    }

    /// A committed-stream shape: `session/created`, then the bootstrap
    /// `input/admitted` carrying `prompt` (the drain's consumption
    /// write wraps payloads as `{"role":"user","content":…}`).
    fn bootstrapped(session: SessionId, prompt: &str) -> Vec<Event> {
        vec![
            Event {
                seq: 1,
                session,
                epoch: Epoch(1),
                ts: 1,
                kind: EventKind::SessionCreated,
                bytes: json!({}),
                version: SCHEMA_VERSION,
            },
            Event {
                seq: 2,
                session,
                epoch: Epoch(1),
                ts: 2,
                kind: EventKind::InputAdmitted,
                bytes: json!({"role": "user", "content": prompt}),
                version: SCHEMA_VERSION,
            },
        ]
    }

    #[test]
    fn scan_marks_exactly_the_drifted_live_sessions() {
        let drifted = session(1);
        let aligned = session(2);
        let fresh = session(3);
        let drifted_stream = bootstrapped(drifted, "[policy v1] judge 0 inputs");
        let aligned_stream = bootstrapped(aligned, "[policy v2] judge 0 inputs");
        let marked = scan_drift(
            vec![
                (drifted, drifted_stream.as_slice()),
                (aligned, aligned_stream.as_slice()),
                (fresh, [].as_slice()),
            ],
            Some("v2"),
        );
        assert_eq!(marked.len(), 1, "only the drifted session is marked");
        assert_eq!(marked[0].0, drifted);
        assert_eq!(marked[0].1.persisted_label(), "v1");
        assert_eq!(marked[0].1.current_label(), "v2");
    }

    #[test]
    fn scan_of_a_legacy_bootstrap_against_a_versioned_config_drifts() {
        let legacy = session(4);
        let legacy_stream = bootstrapped(legacy, "You are the Router…");
        let marked = scan_drift(vec![(legacy, legacy_stream.as_slice())], Some("v2"));
        assert_eq!(marked.len(), 1);
        assert_eq!(marked[0].1.persisted_label(), NO_POLICY_LABEL);
    }

    #[test]
    fn drift_marking_default_is_plain_data() {
        let empty = DriftMarking::default();
        assert!(empty.marked.is_empty());
        assert!(empty.notified.is_empty());
    }
}
