//! acceptance e2e: steer/interrupt semantics + honesty,
//! over the REAL assembled server (engine + ReAct loop + openai-compat
//! provider + local `exec` tool) with a long-running mock task.
//!
//! Proves, against the real promote-boundary implementation (no mocks
//! of the engine):
//!
//! - `steer("plan B")` mid-drain lands at the **next provider-turn safe
//!   boundary** — the synthetic `input/admitted` event never appears
//!   between a `turn/start` and its matching `turn/end` — and carries
//!   the strippable `synthetic` marker; the next request the provider
//!   receives includes it; `strip_synthetic` (the export strip) removes
//!   exactly the synthetic message and nothing else.
//! - `interrupt` mid-tool settles on abort: the in-flight tool's result
//!   is logged (log-before-execute honesty — never a silent drop), the
//!   end write records `interrupted`, the projection resumes `idle`
//!   (resumable), pending inputs stay durable+eligible, and a later
//!   wake completes the session with the honest intermediate
//!   `interrupted` end write preserved in events AND export.
//! - `interrupted ≠ failed` through projection, events, and export.
//! - `interrupt` of an idle or completed session is a no-op.
//! - `queue(msg)` is consumed within the same drain sweep (one
//!   `drain/claimed`, same epoch, same drain task).
//!
//! Runs with `NO_PROXY=127.0.0.1,localhost` (AGENTS.md gotcha).

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::{
    ConfigTuning, ScriptedResponse, force_no_proxy, spawn_scripted_provider, temp_dir,
    write_config_with,
};
use kian_cli::commands::{import_jsonl, run_export};
use kian_cli::server::Server;
use kian_core::context::{derive_model_context, strip_synthetic};
use kian_core::events::{EndReason, Event, EventKind};
use kian_core::model::SessionId;
use kian_core::state::{SessionState, project};
use kian_store::EventStore;

// -----------------------------------------------------------------------
// fixtures
// -----------------------------------------------------------------------

async fn rpc(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{base}/rpc"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .expect("rpc send");
    assert_eq!(resp.status(), 200);
    resp.json().await.expect("rpc json")
}

/// The operator plane (POST /ops/rpc) — used for `wake_now`.
async fn ops_rpc(
    client: &reqwest::Client,
    base: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let resp = client
        .post(format!("{base}/ops/rpc"))
        .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
        .send()
        .await
        .expect("ops rpc send");
    assert_eq!(resp.status(), 200);
    resp.json().await.expect("ops rpc json")
}

/// Spawns the assembled server on an ephemeral port; returns its base
/// URL and the server handle (kept alive for the test duration).
async fn spawn_server(config_path: &Path) -> (String, Arc<Server>) {
    let server = Arc::new(
        Server::from_config_file(config_path)
            .await
            .expect("assemble server")
            .0,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let router = server.router();
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    (format!("http://{addr}"), server)
}

/// One fixture: mock provider + config + live server + a read-only
/// store handle for event-stream assertions.
struct Fixture {
    base: String,
    client: reqwest::Client,
    sqlite: std::path::PathBuf,
    config: std::path::PathBuf,
    reader: EventStore,
    dir: std::path::PathBuf,
}

/// Long tool hold: the scripted `exec sleep <secs>` keeps the drain
/// strictly between `tool/call` and `tool/result` — the deterministic
/// mid-tool window steers/interrupts/queues into.
fn sleep_tool(secs: u64) -> ScriptedResponse {
    ScriptedResponse::ToolCall {
        id: "s2-sleep".into(),
        name: "exec".into(),
        arguments: serde_json::json!({"argv": ["sleep", secs.to_string()]}),
    }
}

async fn fixture(tag: &str, port: u16, script: Vec<ScriptedResponse>) -> Fixture {
    force_no_proxy();
    let dir = temp_dir(tag);
    let sqlite = dir.join("kian.sqlite");
    let (provider_base, _mock) = spawn_scripted_provider(script, 0).await;
    let config = write_config_with(
        &dir,
        "kian.yaml",
        &sqlite,
        &provider_base,
        &format!("127.0.0.1:{port}"),
        &ConfigTuning {
            tools: vec!["exec".into()],
            exec_timeout_ms: 60_000,
            ..ConfigTuning::default()
        },
    );
    let (base, _server) = spawn_server(&config).await;
    let reader = EventStore::open_read_only(&sqlite)
        .await
        .expect("read-only store");
    Fixture {
        base,
        client: reqwest::Client::new(),
        sqlite,
        config,
        reader,
        dir,
    }
}

impl Fixture {
    async fn create_session(&self) -> String {
        let out = rpc(
            &self.client,
            &self.base,
            "create_session",
            serde_json::json!({}),
        )
        .await;
        out["result"]["session"]
            .as_str()
            .expect("session id")
            .to_owned()
    }

    async fn submit(&self, sid: &str, content: &str) {
        let out = rpc(
            &self.client,
            &self.base,
            "submit",
            serde_json::json!({"session": sid, "content": content}),
        )
        .await;
        assert_eq!(out["result"]["deduplicated"], false, "fresh submit: {out}");
    }

    async fn steer(&self, sid: &str, content: &str) {
        let out = rpc(
            &self.client,
            &self.base,
            "steer",
            serde_json::json!({"session": sid, "content": content}),
        )
        .await;
        assert_eq!(out["result"]["deduplicated"], false, "fresh steer: {out}");
    }

    async fn queue(&self, sid: &str, content: &str) {
        let out = rpc(
            &self.client,
            &self.base,
            "queue",
            serde_json::json!({"session": sid, "content": content}),
        )
        .await;
        assert_eq!(out["result"]["deduplicated"], false, "fresh queue: {out}");
    }

    async fn interrupt(&self, sid: &str) -> String {
        let out = rpc(
            &self.client,
            &self.base,
            "interrupt",
            serde_json::json!({"session": sid}),
        )
        .await;
        assert!(out.get("result").is_some(), "interrupt errored: {out}");
        out["result"]["outcome"]
            .as_str()
            .expect("outcome")
            .to_owned()
    }

    async fn projection(&self, sid: &str) -> SessionState {
        let out = rpc(
            &self.client,
            &self.base,
            "projection",
            serde_json::json!({"session": sid}),
        )
        .await;
        let s = out["result"]["state"].as_str().expect("state string");
        serde_json::from_value(serde_json::json!(s)).expect("parse state")
    }

    async fn events(&self, sid: &str) -> Vec<Event> {
        let sid: SessionId = SessionId(sid.parse().expect("uuid"));
        self.reader.rebuild(sid).await.expect("rebuild events")
    }

    /// Polls every 25 ms until the events satisfy `check` (message on
    /// failure includes the final kinds for debugging).
    async fn wait_events(
        &self,
        sid: &str,
        timeout_ms: u64,
        mut check: impl FnMut(&[Event]) -> bool,
    ) -> Vec<Event> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            let events = self.events(sid).await;
            if check(&events) {
                return events;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out; kinds so far: {:?}",
                kinds_of(&events)
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Waits until the drain holds strictly between `tool/call` and
    /// `tool/result` (the deterministic mid-tool window), then returns.
    async fn wait_mid_tool(&self, sid: &str) {
        self.wait_events(sid, 15_000, |events| {
            has(events, EventKind::ToolCall) && !has(events, EventKind::ToolResult)
        })
        .await;
    }
}

fn has(events: &[Event], kind: EventKind) -> bool {
    events.iter().any(|e| e.kind == kind)
}

fn kinds_of(events: &[Event]) -> Vec<String> {
    events.iter().map(|e| format!("{:?}", e.kind)).collect()
}

/// The end-write reasons in stream order (honesty ordering evidence).
fn end_reasons(events: &[Event]) -> Vec<EndReason> {
    events
        .iter()
        .filter_map(|e| match e.kind {
            EventKind::SessionEnded { reason } => Some(reason),
            _ => None,
        })
        .collect()
}

/// **The ordering proof**: no `input/admitted` event may appear
/// inside a provider-turn bracket (between a `turn/start` and its
/// matching `turn/end`). This is the event-level view of "steer never
/// lands inside an in-flight provider turn" — asserted over every
/// input, not just the steer under test.
fn assert_no_input_lands_inside_a_provider_turn(events: &[Event]) {
    let mut open_since: Option<u64> = None;
    for e in events {
        match e.kind {
            EventKind::TurnStart => {
                assert!(
                    open_since.is_none(),
                    "unbalanced turn/start at seq {}",
                    e.seq
                );
                open_since = Some(e.seq);
            }
            EventKind::TurnEnd => {
                assert!(
                    open_since.is_some(),
                    "turn/end without turn/start at seq {}",
                    e.seq
                );
                open_since = None;
            }
            EventKind::InputAdmitted => {
                assert!(
                    open_since.is_none(),
                    "input/admitted at seq {} landed inside the in-flight provider turn starting at seq {:?} — promote-boundary violation",
                    e.seq,
                    open_since
                );
            }
            _ => {}
        }
    }
    assert!(
        open_since.is_none(),
        "stream ends with an unclosed provider turn (seq {open_since:?})"
    );
}

/// The synthetic (steer) `input/admitted` events in the stream.
fn synthetic_inputs(events: &[Event]) -> Vec<&Event> {
    events
        .iter()
        .filter(|e| {
            matches!(e.kind, EventKind::InputAdmitted)
                && e.bytes.get("synthetic").and_then(|v| v.as_bool()) == Some(true)
        })
        .collect()
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn sid_of(s: &str) -> SessionId {
    SessionId(s.parse().expect("uuid"))
}

// -----------------------------------------------------------------------
// .1 — steer lands at the next safe boundary, synthetic + strippable
// -----------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn steer_lands_at_next_safe_boundary_with_strippable_synthetic_marker() {
    // Turn 1: a 5 s `exec sleep` tool call — the mid-tool window we
    // steer into. Turn 2+: the (repeating) final answer.
    let f = fixture(
        "s2-steer",
        7341,
        vec![
            sleep_tool(5),
            ScriptedResponse::Text("Steered all done.".into()),
        ],
    )
    .await;
    let sid = f.create_session().await;
    f.submit(&sid, "original task").await;

    // Hold until the drain is strictly mid-tool, then steer mid-drain.
    f.wait_mid_tool(&sid).await;
    f.steer(&sid, "plan B").await;

    // The steer must be consumed: the session completes.
    let events = f
        .wait_events(&sid, 20_000, |events| {
            end_reasons(events) == vec![EndReason::Completed]
        })
        .await;

    // (a) Ordering: the steer event never landed inside a provider turn
    // (checked over every input event in the stream).
    assert_no_input_lands_inside_a_provider_turn(&events);

    // (b) The synthetic marker: exactly one steer input, wrapping
    // "plan B", and it lands immediately after a `turn/end` — the next
    // safe boundary after the in-flight turn closed.
    let synthetic = synthetic_inputs(&events);
    assert_eq!(
        synthetic.len(),
        1,
        "expected exactly one synthetic input; kinds: {:?}",
        kinds_of(&events)
    );
    let steer_ev = synthetic[0];
    assert_eq!(
        steer_ev.bytes,
        serde_json::json!({"role": "user", "content": "plan B", "synthetic": true}),
        "steer payload must carry the strippable synthetic marker"
    );
    let prev = events
        .iter()
        .find(|e| e.seq == steer_ev.seq - 1)
        .expect("event before the steer");
    assert!(
        matches!(prev.kind, EventKind::TurnEnd),
        "the steer (seq {}) must land at a turn boundary; preceding event is {:?} (seq {})",
        steer_ev.seq,
        prev.kind,
        prev.seq
    );

    // (c) The model actually saw it: the first request whose context
    // can include the steer carries "plan B" in its messages, and that
    // request is sequenced after the steer event.
    let request_with_steer = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::StepRequest))
        .find(|e| {
            e.seq > steer_ev.seq && {
                let body =
                    serde_json::to_string(&e.bytes.get("body").unwrap_or(&serde_json::Value::Null))
                        .unwrap_or_default();
                body.contains("plan B")
            }
        });
    let request_with_steer = request_with_steer.expect(
        "no step/request after the steer carries it in the request body — the model never saw the steer",
    );
    assert!(
        request_with_steer.seq > steer_ev.seq,
        "the request carrying the steer must be sequenced after the steer event"
    );

    // (d) Export with synthetic-strip restores the real context: the
    // exported stream projects a context whose stripped form equals the
    // context of the same stream with the synthetic input event removed
    // — byte-for-byte, nothing else changed.
    let jsonl = run_export(
        &f.config,
        Some(&f.sqlite),
        sid_of(&sid),
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("export");
    let exported: Vec<Event> = jsonl
        .lines()
        .map(|l| serde_json::from_str(l).expect("exported envelope"))
        .collect();
    assert_eq!(
        exported.len(),
        events.len(),
        "export must carry the full stream"
    );
    let context = derive_model_context(&exported).expect("context of export");
    assert!(
        context
            .iter()
            .any(|m| m.synthetic && m.content == serde_json::json!("plan B")),
        "the projected context must include the synthetic steer message"
    );
    let stripped = strip_synthetic(&context);
    let without_steer_events: Vec<Event> = exported
        .iter()
        .filter(|e| {
            !(matches!(e.kind, EventKind::InputAdmitted)
                && e.bytes.get("synthetic").and_then(|v| v.as_bool()) == Some(true))
        })
        .cloned()
        .collect();
    let real_context = derive_model_context(&without_steer_events).expect("real context");
    assert_eq!(
        stripped, real_context,
        "synthetic-strip must restore exactly the real context (A6)"
    );
    assert!(
        !stripped.iter().any(|m| m.synthetic),
        "strip must remove every synthetic marker"
    );
    let _ = std::fs::remove_dir_all(&f.dir);
}

// -----------------------------------------------------------------------
// .2 — interrupt mid-tool: settle-on-abort, honesty, resume
// -----------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_mid_tool_settles_on_abort_and_resumes_honestly() {
    let f = fixture(
        "s2-interrupt",
        7342,
        vec![
            sleep_tool(3),
            ScriptedResponse::Text("Resumed all done.".into()),
        ],
    )
    .await;
    let sid = f.create_session().await;
    f.submit(&sid, "task A").await;

    // Mid-tool: request the interrupt and queue follow-up work. The
    // follow-up stays a durable pending row across the abort (owned and
    // wakeable) — the whole point of settle-on-abort.
    f.wait_mid_tool(&sid).await;
    f.submit(&sid, "task B").await;
    assert_eq!(f.interrupt(&sid).await, "requested");

    // The drain settles on abort once the in-flight turn closes. The
    // interrupt does not kill the running tool: its result is logged
    // first (log-before-execute honesty — "either a logged result or an
    // explicit aborted-with-record event"; here it is the logged
    // result), the turn ends balanced, and exactly one end write with
    // reason `interrupted` lands.
    let events = f
        .wait_events(&sid, 20_000, |events| {
            end_reasons(events) == vec![EndReason::Interrupted]
        })
        .await;
    assert!(
        has(&events, EventKind::ToolResult),
        "interrupt during tool execution must leave a logged tool/result; kinds: {:?}",
        kinds_of(&events)
    );
    assert_no_input_lands_inside_a_provider_turn(&events);

    // Honesty, constraint 7: interrupted ≠ failed — the end write says
    // `interrupted`, never `failed`, and the projection returns the
    // session to resumable `idle` (not a terminal or failed state).
    assert_eq!(end_reasons(&events), vec![EndReason::Interrupted]);
    assert!(
        !end_reasons(&events).contains(&EndReason::Failed),
        "an abort must never be recorded as failed"
    );
    assert_eq!(f.projection(&sid).await, SessionState::Idle);

    // Pending inputs remain owned and wakeable: "task B" is still a
    // durable, eligible, unconsumed row.
    let pending = f
        .reader
        .eligible_inputs(sid_of(&sid), now_ms())
        .await
        .expect("eligible inputs");
    assert!(
        pending
            .iter()
            .any(|r| r.payload == serde_json::json!("task B")),
        "the follow-up input must remain eligible after the abort; pending: {:?}",
        pending
            .iter()
            .map(|r| r.payload.clone())
            .collect::<Vec<_>>()
    );

    // Resume: an operator wake re-drives the session. It completes with
    // the pending work consumed — and the intermediate `interrupted`
    // end write stays recorded between the two claims (honest
    // intermediate labeling).
    let out = ops_rpc(
        &f.client,
        &f.base,
        "wake_now",
        serde_json::json!({"session": sid}),
    )
    .await;
    assert_eq!(out["result"]["outcome"], "started", "wake_now: {out}");

    let events = f
        .wait_events(&sid, 20_000, |events| {
            end_reasons(events) == vec![EndReason::Interrupted, EndReason::Completed]
        })
        .await;
    assert_eq!(
        end_reasons(&events),
        vec![EndReason::Interrupted, EndReason::Completed],
        "the resumed session must complete while preserving the honest interrupted intermediate"
    );
    assert_eq!(f.projection(&sid).await, SessionState::Completed);
    // The follow-up was consumed by the resumed drain.
    let consumed = f
        .reader
        .eligible_inputs(sid_of(&sid), now_ms())
        .await
        .expect("eligible after resume");
    assert!(
        consumed.is_empty(),
        "no input may survive a completed drain; left: {:?}",
        consumed
            .iter()
            .map(|r| r.payload.clone())
            .collect::<Vec<_>>()
    );
    let context = derive_model_context(&events).expect("context");
    assert!(
        context
            .iter()
            .any(|m| m.role == "user" && m.content == serde_json::json!("task B")),
        "the resumed drain must have consumed task B"
    );

    // Honesty through export: the exported audit package carries BOTH
    // end writes in order, and a re-import projects the identical
    // (completed) state — no relabeling anywhere.
    let jsonl = run_export(
        &f.config,
        Some(&f.sqlite),
        sid_of(&sid),
        None,
        None,
        None,
        false,
        false,
    )
    .await
    .expect("export");
    let exported: Vec<Event> = jsonl
        .lines()
        .map(|l| serde_json::from_str(l).expect("exported envelope"))
        .collect();
    assert_eq!(
        end_reasons(&exported),
        vec![EndReason::Interrupted, EndReason::Completed],
        "export must preserve the honest intermediate labeling"
    );
    let fresh_path = f.dir.join("reimport.sqlite");
    let fresh = EventStore::open(&fresh_path).await.expect("fresh store");
    import_jsonl(&fresh, &jsonl).await.expect("re-import");
    let replayed = fresh.rebuild(sid_of(&sid)).await.expect("replayed");
    assert_eq!(
        project(&replayed).expect("project re-import"),
        SessionState::Completed,
        "a re-imported export must project the identical state"
    );
    fresh.close().await;

    let _ = std::fs::remove_dir_all(&f.dir);
}

// -----------------------------------------------------------------------
// .3 — interrupt of an idle / completed session is a no-op
// -----------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_of_idle_and_completed_sessions_is_a_noop() {
    let f = fixture(
        "s2-idle",
        7343,
        vec![ScriptedResponse::Text("Idle all done.".into())],
    )
    .await;

    // Idle: created but never drained.
    let idle = f.create_session().await;
    assert_eq!(f.interrupt(&idle).await, "not_draining");
    assert_eq!(f.projection(&idle).await, SessionState::Idle);
    let events = f.events(&idle).await;
    assert_eq!(
        events.len(),
        1,
        "interrupt of an idle session must append nothing; kinds: {:?}",
        kinds_of(&events)
    );
    assert!(matches!(events[0].kind, EventKind::SessionCreated));
    assert!(
        end_reasons(&events).is_empty(),
        "no end write may appear for a no-op interrupt"
    );

    // Completed: a terminal stream is frozen — interrupt neither
    // appends nor changes the projection.
    let done = f.create_session().await;
    f.submit(&done, "finish fast").await;
    let events = f
        .wait_events(&done, 15_000, |events| {
            end_reasons(events) == vec![EndReason::Completed]
        })
        .await;
    let before = events.len();
    assert_eq!(f.interrupt(&done).await, "not_draining");
    assert_eq!(f.projection(&done).await, SessionState::Completed);
    // Give any (buggy) concurrent writer a beat, then freeze-check.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let events = f.events(&done).await;
    assert_eq!(
        events.len(),
        before,
        "interrupt of a completed session must not append; kinds: {:?}",
        kinds_of(&events)
    );

    let _ = std::fs::remove_dir_all(&f.dir);
}

// -----------------------------------------------------------------------
// .4 — queue(msg) is consumed within the same drain sweep
// -----------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_message_is_consumed_within_the_same_drain_sweep() {
    let f = fixture(
        "s2-queue",
        7344,
        vec![
            sleep_tool(4),
            ScriptedResponse::Text("Queued all done.".into()),
        ],
    )
    .await;
    let sid = f.create_session().await;
    f.submit(&sid, "first task").await;

    // Queue the follow-up while the drain is mid-tool: the wake folds
    // and the in-flight drain's batch semantics must consume it before
    // releasing ownership — no second claim, no second drain task.
    f.wait_mid_tool(&sid).await;
    f.queue(&sid, "queued followup").await;

    let events = f
        .wait_events(&sid, 20_000, |events| {
            end_reasons(events) == vec![EndReason::Completed]
        })
        .await;

    // Exactly one claim in the whole stream: the queued message was
    // consumed by the SAME drain (drain-the-queue batch semantics).
    let claims: Vec<&Event> = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::DrainClaimed))
        .collect();
    assert_eq!(
        claims.len(),
        1,
        "the queued message must be consumed by the same drain (one drain/claimed); kinds: {:?}",
        kinds_of(&events)
    );
    let claim = claims[0];

    // The queued message became an input event after the tool turn
    // closed, under the same claim's epoch, and before the end write.
    let queued = events
        .iter()
        .find(|e| {
            matches!(e.kind, EventKind::InputAdmitted)
                && e.bytes.get("content") == Some(&serde_json::json!("queued followup"))
        })
        .expect("the queued message must be consumed as an input/admitted event");
    assert_eq!(
        queued.epoch, claim.epoch,
        "the queued input must land under the in-flight drain's epoch"
    );
    let ended_seq = events
        .iter()
        .find(|e| matches!(e.kind, EventKind::SessionEnded { .. }))
        .expect("end write")
        .seq;
    assert!(
        queued.seq < ended_seq,
        "the queued message must be consumed before the drain settles"
    );

    // Ordering hygiene held throughout, and the model saw the queued
    // message in a request inside the same drain.
    assert_no_input_lands_inside_a_provider_turn(&events);
    let saw_it = events.iter().any(|e| {
        matches!(e.kind, EventKind::StepRequest)
            && e.seq > queued.seq
            && serde_json::to_string(&e.bytes.get("body").unwrap_or(&serde_json::Value::Null))
                .unwrap_or_default()
                .contains("queued followup")
    });
    assert!(
        saw_it,
        "the provider request after the queue must carry the queued message; kinds: {:?}",
        kinds_of(&events)
    );

    let _ = std::fs::remove_dir_all(&f.dir);
}
