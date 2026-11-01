//! The C53 dsh conformance suite (ADR-010, DESIGN §16): CI proves both
//! directions of the mapping, the steer/interrupt translation and the
//! pre-flight ordering rule against the scripted stdio fixture peer —
//! zero Node/dsh dependency (the live `KIAN_DSH_BIN` run is C54's
//! env-gated smoke, never a default-CI requirement).
//!
//! The fixture peer is `src/bin/dsh-fixture-peer.rs`; scripts live in
//! `tests/fixtures/dsh/`. `mapping_table.json` is the executable form of
//! the DESIGN §16 table.

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use kian_core::context::{Message, context_to_bytes, derive_model_context, strip_synthetic};
use kian_core::events::{Event, EventKind, SCHEMA_VERSION};
use kian_core::model::{Epoch, SessionId};
use kian_superagent::dsh::{
    ContractError, ControlFrame, MapCtx, Mapped, PeerSession, control_line,
};
use serde_json::{Value, json};

const SESSION: SessionId = SessionId(uuid::Uuid::nil());
const EPOCH: Epoch = Epoch(4);
const FIXTURE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/dsh");

fn map_ctx() -> MapCtx {
    MapCtx {
        slot: "main".to_owned(),
        provider: "openai-compat".to_owned(),
        model: "test-model".to_owned(),
    }
}

fn envelope(seq: u64, ts: u64, kind: EventKind, payload: Value) -> Event {
    Event {
        seq,
        session: SESSION,
        epoch: EPOCH,
        ts,
        kind,
        bytes: payload,
        version: SCHEMA_VERSION,
    }
}

/// A stream starts with `session/created` exactly as a real kian session
/// does; mapped events follow with monotone seq.
fn seeded_stream() -> Vec<Event> {
    vec![envelope(
        1,
        1_761_900_000_000,
        EventKind::SessionCreated,
        json!({}),
    )]
}

/// The scripted stdio peer (a real child process speaking the bridge
/// contract over pipes — the same shape C54's driver will speak with a
/// `dsh --profile sdk` child).
struct Peer {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

impl Peer {
    fn spawn(script: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_dsh-fixture-peer"))
            .arg(format!("{FIXTURE_DIR}/{script}"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn dsh-fixture-peer");
        let stdin = child.stdin.take().expect("peer stdin");
        let stdout = child.stdout.take().expect("peer stdout");
        Self {
            child,
            stdin: Some(stdin),
            stdout: BufReader::new(stdout),
        }
    }

    /// Reads one peer line; `None` on clean EOF.
    fn read_line(&mut self) -> Option<String> {
        let mut line = String::new();
        match self.stdout.read_line(&mut line) {
            Ok(0) => None,
            Ok(_) => Some(line.trim_end().to_owned()),
            Err(e) => panic!("reading peer stdout: {e}"),
        }
    }

    fn send(&mut self, frame: &ControlFrame) {
        let stdin = self.stdin.as_mut().expect("peer stdin open");
        writeln!(stdin, "{}", control_line(frame)).expect("write control frame");
        stdin.flush().expect("flush control frame");
    }

    /// Closes stdin and requires a clean peer exit (an unsatisfied
    /// `expect` or a frame mismatch fails here with the peer's stderr).
    fn finish(&mut self) {
        drop(self.stdin.take());
        let mut stderr = String::new();
        if let Some(mut pipe) = self.child.stderr.take() {
            let _ = pipe.read_to_string(&mut stderr);
        }
        let status = self.child.wait().expect("wait for peer");
        assert!(
            status.success(),
            "fixture peer exited {status} (script failed; stderr: {stderr})"
        );
    }
}

/// Runs a script to completion, forwarding the given control frames at
/// the points the script's `expect` lines demand (the caller supplies
/// them in order), and collects the mapped kian events.
fn run_script(script: &str, controls: &[ControlFrame]) -> (Vec<Event>, Peer) {
    let mut peer = Peer::spawn(script);
    let mut session = PeerSession::new(map_ctx());
    let mut events = seeded_stream();
    let mut controls = controls.to_vec();

    while let Some(line) = peer.read_line() {
        match session.accept_line(&line) {
            // The peer blocks on its next `expect` only after emitting
            // everything before it, so controls are consumed exactly at
            // the script's interleave points.
            Ok(None) => {}
            Ok(Some(Mapped::Event { kind, payload })) => {
                let seq = events.len() as u64 + 1;
                // Re-derive ts from the line for envelope honesty.
                let ts = serde_json::from_str::<Value>(&line)
                    .expect("peer line is JSON")
                    .pointer("/event/ts")
                    .and_then(Value::as_u64)
                    .unwrap_or(events.last().map(|e| e.ts).unwrap_or(0));
                events.push(envelope(seq, ts, kind, payload));
            }
            Ok(Some(Mapped::Absorbed { .. })) => unreachable!("session folds absorbed"),
            Err(e) => panic!("unexpected contract error in {script}: {e}"),
        }
        // Feed the next control frame when the peer has nothing more to
        // say and still expects one (peeked by trying: scripts drive the
        // order, so send only when the read would block). Simpler and
        // deterministic: scripts interleave strictly, so a control goes
        // out after every mapped event when one remains.
        if controls.is_empty() {
            continue;
        }
        // Send the next control only when the peer is expecting one;
        // detectable scripts always place `expect` right after their
        // previous send block, so sending immediately after reading the
        // last frame of a block is correct for every fixture here.
        if should_send_control(script, events.len()) {
            peer.send(&controls.remove(0));
        }
    }
    (events, peer)
}

/// Each fixture's interleave points: the stream lengths (including the
/// session/created seed) after which the peer waits for a control frame.
/// Kept explicit per script — fixtures are scripted, not adaptive.
fn should_send_control(script: &str, stream_len: usize) -> bool {
    match script {
        // input `expect` is the peer's first op after ready (len 1);
        // steer `expect` follows the first step/end (len 9);
        // interrupt `expect` follows the first attempt (len 5).
        "interrupt.jsonl" => matches!(stream_len, 1 | 5),
        "steer.jsonl" => matches!(stream_len, 1 | 9),
        "basic_turn.jsonl" => stream_len == 1,
        _ => false,
    }
}

fn input_control() -> ControlFrame {
    ControlFrame::Input {
        content: json!({"text": "deploy the service"}),
    }
}

#[test]
fn every_mapping_row_round_trips_dsh_fixture_event_to_projected_context() {
    let table: Value = serde_json::from_str(
        &std::fs::read_to_string(format!("{FIXTURE_DIR}/mapping_table.json"))
            .expect("mapping_table.json"),
    )
    .expect("mapping table fixture");

    let mut mapper = kian_superagent::dsh::DshMapper::new(map_ctx());
    let mut events = seeded_stream();

    for row in table["rows"].as_array().expect("rows") {
        let ev = &row["dsh"];
        let dsh_event = kian_superagent::dsh::DshEvent {
            kind: ev["kind"].as_str().expect("kind").to_owned(),
            payload: ev["payload"].clone(),
            ts: ev["ts"].as_u64().expect("ts"),
        };
        match mapper.map(&dsh_event).expect("mapping row succeeds") {
            Mapped::Absorbed { kind } => {
                assert_eq!(row.get("absorbed"), Some(&json!(true)), "row {kind}");
                assert_eq!(kind, ev["kind"].as_str().expect("kind"));
            }
            Mapped::Event { kind, payload } => {
                assert_eq!(
                    row.get("absorbed"),
                    None,
                    "row {} must not be absorbed",
                    kind.as_str()
                );
                assert_eq!(kind.as_str(), row["kian"].as_str().expect("kian"));
                let seq = events.len() as u64 + 1;
                events.push(envelope(seq, dsh_event.ts, kind, payload));
            }
        }
    }

    // A2-style byte assert: the projected context is exactly the
    // model-visible subset — messages ride verbatim, and no envelope or
    // audit kind (step/request, turn markers, session/created) leaks in.
    let derived = derive_model_context(&events).expect("context projects");
    let expected: Vec<Message> =
        serde_json::from_value(table["projected_context"].clone()).expect("expected context");
    assert_eq!(
        context_to_bytes(&derived),
        context_to_bytes(&expected),
        "projected context must equal the fixture's model-visible subset byte-for-byte"
    );
}

#[test]
fn fixture_turn_replays_byte_identically_after_reimport() {
    let controls = [input_control()];
    let (events, mut peer) = run_script("basic_turn.jsonl", &controls);
    peer.finish();

    // Export to JSONL (the C14 replay/export shape).
    let exported: Vec<String> = events
        .iter()
        .map(|e| serde_json::to_string(e).unwrap())
        .collect();

    // A second run of the same script maps identically (determinism).
    let (again, mut peer2) = run_script("basic_turn.jsonl", &controls);
    peer2.finish();
    let exported_again: Vec<String> = again
        .iter()
        .map(|e| serde_json::to_string(e).unwrap())
        .collect();
    assert_eq!(
        exported, exported_again,
        "fixture turn must map deterministically"
    );

    // Re-import: parse every line back and require byte-identical
    // re-serialization plus an identical projected context (S7 shape).
    let mut reimported = Vec::new();
    for line in &exported {
        let ev: Event = serde_json::from_str(line).expect("re-import line");
        assert_eq!(serde_json::to_string(&ev).unwrap(), *line);
        reimported.push(ev);
    }
    assert_eq!(
        context_to_bytes(&derive_model_context(&events).unwrap()),
        context_to_bytes(&derive_model_context(&reimported).unwrap()),
        "re-imported stream must project the identical context"
    );
}

#[test]
fn request_bytes_are_observed_before_the_response_is_offered() {
    let (events, mut peer) = run_script("basic_turn.jsonl", &[input_control()]);
    peer.finish();

    // Positive ordering witness: inside every provider turn the
    // step/request (the attempt's assembled request bytes) precedes the
    // assistant/message.
    for pair in events.windows(2) {
        if pair[1].kind == EventKind::AssistantMessage {
            let turn_start = events
                .iter()
                .rfind(|e| e.seq < pair[1].seq && e.kind == EventKind::TurnStart)
                .expect("assistant message inside a turn");
            let request = events
                .iter()
                .find(|e| {
                    e.seq > turn_start.seq
                        && e.seq < pair[1].seq
                        && e.kind == EventKind::StepRequest
                })
                .expect("request bytes persisted before the response");
            assert!(request.seq < pair[1].seq);
        }
    }

    // Negative: a peer that offers the response with no preceding
    // attempt violates the pre-flight rule — the honest contract error,
    // and nothing is mapped for the violating frame (the bridge stops;
    // never a best-effort post-hoc logger).
    let mut peer = Peer::spawn("preflight_violation.jsonl");
    let mut session = PeerSession::new(map_ctx());
    let mut events = seeded_stream();
    let mut violation = None;
    let mut ready = false;
    while let Some(line) = peer.read_line() {
        match session.accept_line(&line) {
            Ok(None) => {
                // The ready handshake: the peer now waits for the input.
                if !ready && line.contains("\"ready\"") {
                    ready = true;
                    peer.send(&input_control());
                }
            }
            Ok(Some(Mapped::Event { kind, payload })) => {
                let seq = events.len() as u64 + 1;
                let ts = serde_json::from_str::<Value>(&line)
                    .expect("JSON")
                    .pointer("/event/ts")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                events.push(envelope(seq, ts, kind, payload));
            }
            Ok(Some(Mapped::Absorbed { .. })) => unreachable!(),
            Err(e) => {
                violation = Some(e);
                break;
            }
        }
    }
    assert_eq!(
        violation,
        Some(ContractError::ResponseBeforeRequestBytes {
            kind: "assistant/message"
        }),
        "a response with no persisted request bytes must be refused"
    );
    assert!(
        !events.iter().any(|e| e.kind == EventKind::AssistantMessage),
        "the violating response must not reach the stream"
    );
    peer.finish();
}

#[test]
fn unknown_dsh_event_kinds_map_to_an_honest_contract_error() {
    let mut peer = Peer::spawn("unknown_kind.jsonl");
    let mut session = PeerSession::new(map_ctx());
    let mut events = seeded_stream();
    let mut error = None;
    let mut saw_user_message = false;
    let mut ready = false;
    while let Some(line) = peer.read_line() {
        match session.accept_line(&line) {
            Ok(None) => {
                if !ready && line.contains("\"ready\"") {
                    ready = true;
                    peer.send(&input_control());
                }
            }
            Ok(Some(Mapped::Event { kind, .. })) => {
                saw_user_message |= kind == EventKind::InputAdmitted;
                events.push(envelope(events.len() as u64 + 1, 0, kind, json!({})));
            }
            Ok(Some(Mapped::Absorbed { .. })) => unreachable!(),
            Err(e) => {
                error = Some(e);
                break;
            }
        }
    }
    assert!(saw_user_message, "events before the unknown kind mapped");
    // `request/header` is a real dsh durable kind outside contract v0:
    // the error is the extension point — never a silent drop.
    assert_eq!(
        error,
        Some(ContractError::UnknownKind {
            kind: "request/header".to_owned()
        })
    );
    assert_eq!(
        events.len(),
        2,
        "only session/created + the user message mapped"
    );
    peer.finish();
}

#[test]
fn foreign_generation_is_refused_before_any_mapping() {
    let mut peer = Peer::spawn("foreign_format.jsonl");
    let mut session = PeerSession::new(map_ctx());
    let ready = peer.read_line().expect("ready frame");
    // The peer declared logical format 2: generations must be decoded to
    // current logical events on the dsh side — Kian never stores foreign
    // generations, so the handshake is refused before any mapping.
    assert_eq!(
        session.accept_line(&ready),
        Err(ContractError::ForeignLogicalFormat {
            got: 2,
            supported: 0
        })
    );
    peer.finish();
}

#[test]
fn steer_translates_to_an_injected_synthetic_message_in_the_next_request() {
    let controls = [
        input_control(),
        ControlFrame::Steer {
            content: json!({"text": "switch to plan B"}),
        },
    ];
    let (events, mut peer) = run_script("steer.jsonl", &controls);
    // The peer's `expect` pinned the exact steer wire frame (a mismatch
    // would fail `finish` with the peer's diagnostic).
    peer.finish();

    // The injected message lands as a synthetic, strippable input.
    let steer_events: Vec<&Event> = events
        .iter()
        .filter(|e| {
            e.kind == EventKind::InputAdmitted
                && e.bytes.get("synthetic").and_then(Value::as_bool) == Some(true)
        })
        .collect();
    assert_eq!(steer_events.len(), 1, "exactly one synthetic input");
    assert_eq!(
        steer_events[0].bytes["content"],
        json!({"text": "switch to plan B"})
    );

    // Persist-what-you-send across the boundary: the next assembled
    // request (the attempt's bytes, persisted as step/request) contains
    // the injected message verbatim.
    let mut requests = events.iter().filter(|e| e.kind == EventKind::StepRequest);
    let second_request = requests.nth(1).expect("second step request");
    let body = &second_request.bytes["body"];
    let injected = body["messages"]
        .as_array()
        .expect("request messages")
        .iter()
        .any(|m| {
            m["content"] == json!({"text": "switch to plan B"})
                && m.get("synthetic").and_then(Value::as_bool) == Some(true)
        });
    assert!(
        injected,
        "the assembled request must embed the steer verbatim"
    );

    // And the projection shows it — strippable.
    let context = derive_model_context(&events).expect("context");
    assert!(context.iter().any(|m| m.synthetic));
    let stripped = strip_synthetic(&context);
    assert!(stripped.iter().all(|m| !m.synthetic));
    assert_eq!(stripped.len(), context.len() - 1);
}

#[test]
fn interrupt_translates_to_a_cancelled_attempt_with_no_assistant_message() {
    let controls = [input_control(), ControlFrame::Interrupt];
    let (events, mut peer) = run_script("interrupt.jsonl", &controls);
    // The peer's `expect` pinned the exact interrupt wire frame.
    peer.finish();

    // The attempt's request bytes were persisted (step/request exists)…
    assert!(
        events.iter().any(|e| e.kind == EventKind::StepRequest),
        "request bytes persisted before the interrupt"
    );
    // …and the honest failure trace is structural: no assistant/message
    // follows (interrupted, never completed — constraint 7).
    assert!(
        !events.iter().any(|e| e.kind == EventKind::AssistantMessage),
        "a cancelled step must not produce an assistant message"
    );
    let turn_end = events
        .iter()
        .find(|e| e.kind == EventKind::TurnEnd)
        .expect("turn ended");
    assert_eq!(turn_end.bytes["outcome"], json!("cancelled"));
}
