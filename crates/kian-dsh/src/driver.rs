//! `DshDriver` — the bridge [`LoopDriver`](kian_core::seams::LoopDriver)
//! (DESIGN §6.2 "dsh loop (being-integrated)" row).
//!
//! One `dsh --profile sdk` child process per drain episode, spoken to
//! over stdio JSON-lines with the contract (ready handshake,
//! logical session events out, input/steer/interrupt control frames
//! ). The child owns its interaction; kian owns durability: every
//! mapped event is appended through the [`DshCtx`] fenced path, and
//! nothing else is ever written.
//!
//! # Episode lifecycle
//!
//! `drive` is called once per drain episode (the engine's shape) and
//! spawns a fresh child. Continuation *is* the replay: dsh's own
//! durable session log is the child's truth, so a takeover drain (or a
//! folded-wake episode) spawns a new child that replays its log and
//! continues — exactly the takeover shape through the bridge. All
//! per-episode state lives inside the outcome stream; dropping the
//! stream (interrupt, fenced-out drain, settle) drops the child handle
//! and the pump kills the process — a dropped drain never leaks a dsh
//! child.
//!
//! # Input delivery
//!
//! Promoted inputs ride to the child as control frames: a plain row as
//! `input` (content = the row payload), a synthetic row as `steer`
//! (content = the wrapped content) — exactly the engine's inbox
//! conventions. At spawn time the driver additionally delivers any
//! `input/admitted` still unmatched in the replay multiset (the batch
//! the engine promoted before `drive`, plus anything a previous drain
//! promoted but died before delivering), and again at absorbed episode
//! boundaries (`turn/start`/`turn/end`).
//!
//! # Replay de-duplication (takeover)
//!
//! A fresh child replays its own durable session log from the start.
//! Every mapped event is first matched against a multiset of the
//! persisted stream's bridge-projectable events ([`Dedup`]); a match
//! means kian already persisted it (the previous drain did, or it was
//! a promotion) and nothing is appended — only genuinely new events
//! append. Multiset, not sequence, because the persisted order
//! interleaves the engine's own writes (promotions, claims, lineage)
//! with the mapped ones.
//!
//! # Honesty
//!
//! - a contract error or a child crash mid-step yields `SoftFailed` —
//!   the engine settles `failed`, and the stream keeps 's honest
//!   failure trace (an attempt with no `assistant/message`);
//! - a child crash is never reported as completion (constraint 7);
//! - the step budget yields `Stopped` exactly like the built-in loop's
//!   iteration budget;
//! - the end write stays the engine's (the bridge has no end path).

use std::collections::{BTreeSet, VecDeque};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{StreamExt, stream::BoxStream};
use kian_core::events::{Event, EventKind};
use kian_core::model::SessionId;
use kian_core::seams::{LoopDriver, LoopOutcome};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use crate::ctx::{DshCtx, DshCtxError};
use crate::mapping::{
    ControlFrame, MapCtx, Mapped, PeerFrame, PeerSession, control_line, parse_peer_frame,
};

/// Default child argv beyond the binary: `dsh --profile sdk`.
pub const DEFAULT_DSH_ARGS: &[&str] = &["--profile", "sdk"];

/// Default step budget (mapped `turn/start`s per session, derived from
/// the stream like the built-in loop's iteration counter).
pub const DEFAULT_MAX_STEPS: u64 = 32;

/// Default wait for the peer's `ready` frame.
pub const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

/// How often an idle drain re-checks the inbox while waiting for the
/// peer's next line. Constraint 6: wakes may fold, be lost, or
/// race — the drain never depends on one to make progress. A step-open
/// wait is unbounded (the provider call is in flight, exactly like the
/// built-in loop's); an idle safe-boundary wait is bounded by this
/// interval so a late-admitted input still lands.
pub const IDLE_INPUT_RECHECK: Duration = Duration::from_millis(50);

/// Default open-step inactivity bound (HARNESS-BRIDGE §4.5, ADR-044): how long an open dsh step
/// may produce no progress (no protocol line, no completed control write) before the child is
/// declared unresponsive. Generous on purpose — the bound ends a dead bridge, it does not police a
/// slow one. `0` disables the bound.
pub const DEFAULT_STEP_TIMEOUT_MS: u64 = 1_800_000;

/// Driver configuration (assembly-owned; `superagent.harness.dsh`).
#[derive(Debug, Clone)]
pub struct DshDriverConfig {
    /// The dsh binary (a real install, or the fixture peer in CI).
    pub command: String,
    /// Child argv. Default [`DEFAULT_DSH_ARGS`].
    pub args: Vec<String>,
    /// The slot facts stamped into mapped `step/request` envelopes
    /// (ADR-010): the bridge session's binding, from config.
    pub map: MapCtx,
    /// Mapped-step budget across the whole session. Default 32.
    pub max_steps: u64,
    /// How long the first frame (`ready`) may take. Default 10s.
    pub startup_timeout: Duration,
    /// Open-step inactivity bound (ADR-044). `0` disables the bound.
    pub step_timeout_ms: u64,
}

impl DshDriverConfig {
    #[must_use]
    pub fn new(command: impl Into<String>, map: MapCtx) -> Self {
        Self {
            command: command.into(),
            args: DEFAULT_DSH_ARGS.iter().map(|s| (*s).to_owned()).collect(),
            map,
            max_steps: DEFAULT_MAX_STEPS,
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            step_timeout_ms: DEFAULT_STEP_TIMEOUT_MS,
        }
    }
}

/// What the pump task forwards from the child.
#[derive(Debug)]
enum PeerMsg {
    Line(String),
    /// Stdout closed; the child exited with this code (`None` = killed
    /// by signal).
    Exit(Option<i32>),
    /// Pipe I/O failed (the child is unusable).
    Io(String),
}

/// The driver-side handle to one live child.
struct PeerHandle {
    ctrl_tx: mpsc::Sender<ControlFrame>,
    line_rx: mpsc::Receiver<PeerMsg>,
}

/// Spawns the child and its pump task. The pump owns the process: it
/// writes control frames, forwards stdout lines, and kills the child
/// when the driver side goes away (control channel closed) or the
/// child finishes — a dropped drain never leaks a dsh process.
fn spawn_peer(config: &DshDriverConfig) -> Result<PeerHandle, String> {
    let mut command = Command::new(&config.command);
    command
        .args(&config.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        // Peer diagnostics stay visible; the contract is stdout-only.
        .stderr(Stdio::inherit());
    let mut child = command
        .spawn()
        .map_err(|e| format!("spawn {}: {e}", config.command))?;
    let stdin = child.stdin.take().ok_or("peer stdin not piped")?;
    let stdout = child.stdout.take().ok_or("peer stdout not piped")?;
    let (ctrl_tx, ctrl_rx) = mpsc::channel(16);
    let (line_tx, line_rx) = mpsc::channel(64);
    tokio::spawn(pump(child, stdin, stdout, ctrl_rx, line_tx));
    Ok(PeerHandle { ctrl_tx, line_rx })
}

async fn pump(
    mut child: Child,
    mut stdin: tokio::process::ChildStdin,
    stdout: tokio::process::ChildStdout,
    mut ctrl_rx: mpsc::Receiver<ControlFrame>,
    line_tx: mpsc::Sender<PeerMsg>,
) {
    let mut stdout = BufReader::new(stdout).lines();
    loop {
        tokio::select! {
            line = stdout.next_line() => match line {
                Ok(Some(text)) => {
                    if line_tx.send(PeerMsg::Line(text)).await.is_err() {
                        // Driver gone: stop the child.
                        let _ = child.kill().await;
                        return;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    let _ = line_tx.send(PeerMsg::Io(e.to_string())).await;
                    let _ = child.kill().await;
                    return;
                }
            },
            frame = ctrl_rx.recv() => match frame {
                Some(frame) => {
                    let mut line = control_line(&frame);
                    line.push('\n');
                    if stdin.write_all(line.as_bytes()).await.is_err() {
                        let _ = line_tx
                            .send(PeerMsg::Io("peer stdin write failed".into()))
                            .await;
                        let _ = child.kill().await;
                        return;
                    }
                    let _ = stdin.flush().await;
                }
                None => {
                    // The driver dropped the handle: end the child.
                    let _ = child.kill().await;
                    return;
                }
            },
        }
    }
    let status = child.wait().await;
    let _ = line_tx
        .send(PeerMsg::Exit(status.ok().and_then(|s| s.code())))
        .await;
}

/// Multiset of the persisted stream's bridge-projectable events: a
/// mapped event arriving from the child that matches a remaining entry
/// was already persisted and appends nothing. Deterministic removal:
/// the first equal entry in stream order.
#[derive(Debug, Default)]
struct Dedup {
    entries: VecDeque<(EventKind, Value)>,
}

impl Dedup {
    fn build(events: &[Event]) -> Self {
        let mut dedup = Self::default();
        for ev in events {
            if is_bridge_kind(ev.kind) {
                dedup.entries.push_back((ev.kind, ev.bytes.clone()));
            }
        }
        dedup
    }

    /// True when the event was already persisted (consumes one entry).
    fn take(&mut self, kind: EventKind, payload: &Value) -> bool {
        if let Some(pos) = self
            .entries
            .iter()
            .position(|(k, p)| *k == kind && p == payload)
        {
            self.entries.remove(pos);
            true
        } else {
            false
        }
    }

    /// Records an event the driver knows was appended on the kian side
    /// after the spawn snapshot, so its echo from the child matches
    /// instead of double-appending.
    fn add(&mut self, kind: EventKind, payload: Value) {
        self.entries.push_back((kind, payload));
    }

    /// Persisted `input/admitted` user messages the child has not
    /// echoed — the delivery candidates.
    fn undelivered_user_inputs(&self, delivered: &BTreeSet<String>) -> Vec<Value> {
        self.entries
            .iter()
            .filter(|(kind, _)| matches!(kind, EventKind::InputAdmitted))
            .filter(|(_, payload)| payload.get("role").and_then(Value::as_str) == Some("user"))
            .filter(|(_, payload)| !delivered.contains(&canon(payload)))
            .map(|(_, payload)| payload.clone())
            .collect()
    }
}

/// Canonical string for a payload (serde_json's default map is
/// ordered, so equal `Value`s canonicalize equally).
fn canon(payload: &Value) -> String {
    serde_json::to_string(payload).unwrap_or_default()
}

fn is_bridge_kind(kind: EventKind) -> bool {
    matches!(
        kind,
        EventKind::InputAdmitted
            | EventKind::AssistantMessage
            | EventKind::StepRequest
            | EventKind::ToolCall
            | EventKind::ToolResult
            | EventKind::TurnStart
            | EventKind::TurnEnd
    )
}

/// One live bridge session: the child handle plus the mapper
/// state. Owned by the outcome stream while an episode runs; a stream
/// that ends between outcomes parks the session in the driver's table
/// so a later drive of the same hold continues the same child.
struct BridgeSession {
    peer: PeerHandle,
    mapper: PeerSession,
    dedup: Dedup,
    /// Canonical payloads of input frames already delivered to this
    /// child (never re-deliver to the same process).
    delivered: BTreeSet<String>,
    interrupt_sent: bool,
    /// The step's mapped `tool/call` names by call id (§3.5): the
    /// retrospective `tool/post` observation names the call through
    /// the step's own mapped record — recorded for replayed calls too
    /// (before the dedup skip), so a take-over drain can still resolve
    /// a name for a result whose call landed in an earlier drain.
    tool_names: std::collections::BTreeMap<String, String>,
}

/// Per-driver session table: the cross-drive home of a live child. An
/// entry is an `Option` because an active episode moves the session
/// into its stream (drains are per-session serial, so no concurrent
/// re-entry exists).
#[derive(Default)]
struct SessionTable {
    sessions: Mutex<std::collections::HashMap<SessionId, Option<BridgeSession>>>,
}

impl SessionTable {
    fn take(&self, session: SessionId) -> Option<BridgeSession> {
        self.sessions
            .lock()
            .expect("session table lock")
            .get_mut(&session)
            .and_then(Option::take)
    }

    fn park(&self, session: SessionId, sess: BridgeSession) {
        self.sessions
            .lock()
            .expect("session table lock")
            .insert(session, Some(sess));
    }
}

/// The bridge LoopDriver. Cheap to clone; per-session child state
/// lives behind the driver-wide table.
#[derive(Clone, Default)]
pub struct DshDriver {
    inner: Arc<DriverInner>,
}

#[derive(Default)]
struct DriverInner {
    config: Option<DshDriverConfig>,
    table: SessionTable,
    /// Port (d″) — the crate-local `tool/post` port (§3.5), injected
    /// at driver construction and implemented by assembly over the
    /// real hook bus. `None` observes nothing.
    tool_post: Option<Arc<dyn crate::toolpost::ToolPostPort>>,
}

impl DshDriver {
    #[must_use]
    pub fn new(config: DshDriverConfig) -> Self {
        Self {
            inner: Arc::new(DriverInner {
                config: Some(config),
                table: SessionTable::default(),
                tool_post: None,
            }),
        }
    }

    /// [`DshDriver::new`] with the `tool/post` port mounted (§3.5).
    /// Call before serving: the builder derives a fresh inner (no
    /// sessions are parked yet at construction time).
    #[must_use]
    pub fn with_tool_post_port(mut self, port: Arc<dyn crate::toolpost::ToolPostPort>) -> Self {
        let config = self.inner.config.clone();
        self.inner = Arc::new(DriverInner {
            config,
            table: SessionTable::default(),
            tool_post: Some(port),
        });
        self
    }

    fn config(&self) -> &DshDriverConfig {
        self.inner
            .config
            .as_ref()
            .expect("DshDriver is constructed with a config")
    }

    /// The configured open-step inactivity bound; `None` when disabled (`0`).
    fn step_bound(&self) -> Option<Duration> {
        (self.config().step_timeout_ms > 0)
            .then(|| Duration::from_millis(self.config().step_timeout_ms))
    }

    #[must_use]
    pub fn config_ref(&self) -> &DshDriverConfig {
        self.config()
    }
}

/// Unfold state. `Next` keeps the live session between outcomes (the
/// engine polls again after `TurnDone`); `Done` ends the stream (every
/// terminal outcome has already been yielded).
#[allow(clippy::large_enum_variant)]
enum St {
    Start(Arc<dyn DshCtx>),
    Next {
        ctx: Arc<dyn DshCtx>,
        parked: Parked,
        steps_in_stream: u64,
    },
    Done,
}

/// A session parked between outcomes: if the stream is dropped (or
// ends) while the child is still live, the session returns to the
/// driver's table so the next `drive` of the same hold continues the
/// same child; a consumed session (the episode moved it into `run`)
/// parks nothing.
struct Parked {
    driver: DshDriver,
    session: SessionId,
    sess: Mutex<Option<BridgeSession>>,
}

impl Parked {
    fn take(&self) -> Option<BridgeSession> {
        self.sess.lock().expect("parked session").take()
    }
}

impl Drop for Parked {
    fn drop(&mut self) {
        if let Some(sess) = self.sess.lock().expect("parked session").take() {
            self.driver.inner.table.park(self.session, sess);
        }
    }
}

impl LoopDriver for DshDriver {
    type Ctx = Arc<dyn DshCtx>;

    fn drive(&self, ctx: Arc<dyn DshCtx>) -> BoxStream<'static, LoopOutcome> {
        let driver = self.clone();
        futures::stream::unfold(St::Start(ctx), move |state| {
            let driver = driver.clone();
            async move {
                match driver.step(state).await {
                    Step::Yield(outcome, next) => Some((outcome, next)),
                    Step::End => None,
                }
            }
        })
        .boxed()
    }
}

#[allow(clippy::large_enum_variant)] // the Yield variant owns the parked session by design
enum Step {
    Yield(LoopOutcome, St),
    End,
}

impl DshDriver {
    /// One unfold step: run the episode until an outcome is due.
    async fn step(&self, state: St) -> Step {
        match state {
            // After a terminal outcome the stream ends, exactly like
            // the built-in loop's `done` flag.
            St::Done => Step::End,
            St::Start(ctx) => {
                let (outcome, next) = self.episode(ctx).await;
                Step::Yield(outcome, next)
            }
            St::Next {
                ctx,
                parked,
                steps_in_stream,
            } => match parked.take() {
                Some(sess) => {
                    let (outcome, next) = self.run(ctx, sess, steps_in_stream).await;
                    Step::Yield(outcome, next)
                }
                None => Step::End,
            },
        }
    }

    /// Continues a parked session or spawns a fresh child, then runs.
    async fn episode(&self, ctx: Arc<dyn DshCtx>) -> (LoopOutcome, St) {
        let session = ctx.session();
        if let Some(sess) = self.inner.table.take(session) {
            // Continuing child (a later drive of the same hold): the
            // mapper/dedup/delivered state rides along.
            let steps_in_stream = count_steps(&ctx).await;
            return self.run(ctx, sess, steps_in_stream).await;
        }
        let events = match ctx.events().await {
            Ok(events) => events,
            Err(e) => return (soft("stream read", e), St::Done),
        };
        let steps_in_stream = events
            .iter()
            .filter(|ev| matches!(ev.kind, EventKind::TurnStart))
            .count() as u64;
        if steps_in_stream >= self.config().max_steps {
            return (
                LoopOutcome::Stopped {
                    reason: format!("dsh step budget exhausted after {steps_in_stream} steps"),
                },
                St::Done,
            );
        }
        let bound = self.step_bound();
        let mut sess = match self.spawn(&events).await {
            Ok(sess) => sess,
            Err(reason) => return (LoopOutcome::SoftFailed { reason }, St::Done),
        };
        // Deliver this drain's promoted batch: the `input/admitted`
        // events appended after the last `drain/claimed` — everything
        // the engine promoted before `drive`. Inputs from earlier
        // drains are NOT re-delivered (the child learned them or the
        // drain died in the promote-before-deliver window — the
        // documented boundary; kian's stream keeps their durable
        // record either way).
        let fresh = fresh_promotions(&events);
        for payload in fresh {
            if let Err(reason) =
                deliver_admitted(&mut sess.peer, &mut sess.delivered, &payload, bound).await
            {
                return (LoopOutcome::SoftFailed { reason }, St::Done);
            }
        }
        self.run(ctx, sess, steps_in_stream).await
    }

    /// Spawns the child and runs the ready handshake.
    async fn spawn(&self, events: &[Event]) -> Result<BridgeSession, String> {
        let config = self.config();
        let mut peer = spawn_peer(config)?;
        let mut mapper = PeerSession::new(config.map.clone());
        let line = tokio::time::timeout(config.startup_timeout, peer.line_rx.recv())
            .await
            .map_err(|_| format!("peer never sent ready within {:?}", config.startup_timeout))?
            .ok_or("peer exited before ready".to_string())?;
        match line {
            PeerMsg::Line(text) => {
                // A session event before ready is refused by the
                // mapper itself (ready-first, ADR-010).
                if let Err(e) = mapper.accept_line(&text) {
                    return Err(e.to_string());
                }
            }
            PeerMsg::Exit(code) => {
                return Err(format!("peer exited before ready (code {code:?})"));
            }
            PeerMsg::Io(e) => return Err(format!("peer i/o before ready: {e}")),
        }
        Ok(BridgeSession {
            peer,
            mapper,
            dedup: Dedup::build(events),
            delivered: BTreeSet::new(),
            interrupt_sent: false,
            tool_names: std::collections::BTreeMap::new(),
        })
    }

    /// Delivers every persisted-but-unechoed user input to the child.
    async fn deliver_undelivered(
        &self,
        sess: &mut BridgeSession,
        bound: Option<Duration>,
    ) -> Result<(), String> {
        for payload in sess.dedup.undelivered_user_inputs(&sess.delivered) {
            deliver_admitted(&mut sess.peer, &mut sess.delivered, &payload, bound).await?;
        }
        Ok(())
    }

    /// Runs the session until an outcome.
    async fn run(
        &self,
        ctx: Arc<dyn DshCtx>,
        mut sess: BridgeSession,
        steps_in_stream: u64,
    ) -> (LoopOutcome, St) {
        let bound = self.step_bound();
        loop {
            // Safe boundary: no dsh step is open (mapper state — the
            // authoritative boundary here, since replayed turn events
            // do not ride the kian-side promote flag).
            if !sess.mapper.step_open() {
                if ctx.interrupt_requested() && !sess.interrupt_sent {
                    sess.interrupt_sent = true;
                    match within_step_bound(
                        bound,
                        "dsh interrupt control write",
                        sess.peer.ctrl_tx.send(ControlFrame::Interrupt),
                    )
                    .await
                    {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) => return (control_closed(), St::Done),
                        Err(reason) => return (LoopOutcome::SoftFailed { reason }, St::Done),
                    }
                }
                match ctx.take_eligible_inputs().await {
                    Ok(rows) => {
                        for row in rows {
                            // The consumption write already appended
                            // the input/admitted event; register its
                            // payload so the child's echo de-duplicates.
                            let payload = consumption_payload(&row.payload);
                            sess.dedup.add(EventKind::InputAdmitted, payload.clone());
                            if let Err(reason) = deliver_admitted(
                                &mut sess.peer,
                                &mut sess.delivered,
                                &payload,
                                bound,
                            )
                            .await
                            {
                                return (LoopOutcome::SoftFailed { reason }, St::Done);
                            }
                        }
                    }
                    Err(e) => return (soft("input take", e), St::Done),
                }
            }
            // Constraint 6: wakes may fold, be lost, or race. While a
            // step is open the peer will speak (the provider call is in
            // flight — unbounded, like the built-in loop's model call);
            // at an idle safe boundary the peer may legitimately stay
            // silent until *we* deliver the next input, so the line
            // wait is bounded by [`IDLE_INPUT_RECHECK`] and the loop
            // top re-checks interrupt + eligible inputs. Without this,
            // an input admitted after the boundary check deadlocks the
            // episode (peer blocked on `expect`, driver on the line).
            let msg = if sess.mapper.step_open() {
                match within_step_bound(
                    bound,
                    "dsh step (no protocol line)",
                    sess.peer.line_rx.recv(),
                )
                .await
                {
                    Ok(msg) => msg,
                    Err(reason) => return (LoopOutcome::SoftFailed { reason }, St::Done),
                }
            } else {
                tokio::select! {
                    m = sess.peer.line_rx.recv() => m,
                    // Cancel-safe: the mpsc recv is dropped, not the
                    // message — an arrived line stays queued.
                    _ = tokio::time::sleep(IDLE_INPUT_RECHECK) => {
                        continue;
                    }
                }
            };
            match msg {
                Some(PeerMsg::Line(text)) => {
                    match sess.mapper.accept_line(&text) {
                        Err(e) => {
                            return (
                                LoopOutcome::SoftFailed {
                                    reason: format!("dsh bridge contract: {e}"),
                                },
                                St::Done,
                            );
                        }
                        Ok(None) => {
                            // The ready handshake or an absorbed episode
                            // envelope. At an absorbed `turn/end` — the
                            // end of the child's episode, after which
                            // every delivered input's echo has arrived —
                            // deliver anything the child turns out not
                            // to know (the promote-before-deliver
                            // window's closure; the echo of a late
                            // delivery then matches the multiset).
                            if absorbed_turn_end(&text)
                                && let Err(reason) =
                                    self.deliver_undelivered(&mut sess, bound).await
                            {
                                return (LoopOutcome::SoftFailed { reason }, St::Done);
                            }
                        }
                        Ok(Some(Mapped::Absorbed { .. })) => {}
                        Ok(Some(Mapped::Event { kind, payload })) => {
                            // §3.5: record the step's mapped `tool/call`
                            // names (by call id) BEFORE the dedup skip, so
                            // a replayed call from an earlier drain still
                            // resolves a name for its retrospective
                            // `tool/post` observation.
                            if matches!(kind, EventKind::ToolCall)
                                && let (Some(id), Some(name)) = (
                                    payload
                                        .pointer("/content/id")
                                        .and_then(serde_json::Value::as_str),
                                    payload
                                        .pointer("/content/name")
                                        .and_then(serde_json::Value::as_str),
                                )
                            {
                                sess.tool_names.insert(id.to_string(), name.to_string());
                            }
                            if sess.dedup.take(kind, &payload) {
                                // Already persisted (previous drain or a
                                // promotion): append nothing.
                                continue;
                            }
                            let budget = steps_in_stream + sess.mapper.steps_emitted();
                            let appended = match kind {
                                EventKind::TurnStart => {
                                    if budget >= self.config().max_steps {
                                        return (
                                            LoopOutcome::Stopped {
                                                reason: format!(
                                                    "dsh step budget exhausted after {budget} steps"
                                                ),
                                            },
                                            St::Done,
                                        );
                                    }
                                    ctx.begin_turn(payload.clone()).await
                                }
                                EventKind::TurnEnd => ctx.end_turn(payload.clone()).await,
                                ref other => ctx.append(*other, payload.clone()).await,
                            };
                            if let Err(e) = appended {
                                return (soft("append", e), St::Done);
                            }
                            // Port (d″) — `tool/post`, dsh site (§3.5):
                            // the post-append branch, retrospective and
                            // observe-only, after `ctx.append(ToolResult)`
                            // returns `Ok`. The name comes from the
                            // step's mapped `tool/call` for that call_id;
                            // a result with no matching mapped call is
                            // SKIPPED with one audit line — never
                            // dispatched with a fabricated name.
                            if matches!(kind, EventKind::ToolResult)
                                && let Some(port) = self.inner.tool_post.clone()
                                && let Some(content) = payload.get("content")
                            {
                                let call_id = content
                                    .get("call_id")
                                    .and_then(serde_json::Value::as_str)
                                    .unwrap_or_default();
                                match sess.tool_names.get(call_id) {
                                    Some(name) => {
                                        let output = content
                                            .get("output")
                                            .cloned()
                                            .unwrap_or(serde_json::Value::Null);
                                        let bytes = serde_json::to_vec(&output).unwrap_or_default();
                                        port.tool_post(crate::toolpost::ToolPostView {
                                            call_id: call_id.to_string(),
                                            name: name.clone(),
                                            is_error: content
                                                .get("is_error")
                                                .and_then(serde_json::Value::as_bool)
                                                .unwrap_or(false),
                                            output_bytes: bytes.len() as u64,
                                            output_sha256: crate::toolpost::sha256_hex(&bytes),
                                            // The child's executor owns any
                                            // scrubbing; the bridge has no
                                            // redaction provenance to report.
                                            redactions: Vec::new(),
                                        })
                                        .await;
                                    }
                                    None => {
                                        tracing::warn!(
                                            target: "kian::dsh",
                                            call_id = %call_id,
                                            "dsh tool/post skipped: no matching mapped tool/call"
                                        );
                                    }
                                }
                            }
                            if matches!(kind, EventKind::TurnEnd) {
                                // A provider turn completed: a safe
                                // boundary and the engine's supervision
                                // point. The session parks inside the
                                // stream state; the next poll continues
                                // it, a dropped stream returns it to
                                // the driver's table, and a terminal
                                // outcome (below) ends the episode with
                                // the child.
                                let session = ctx.session();
                                let driver = self.clone();
                                return (
                                    LoopOutcome::TurnDone,
                                    St::Next {
                                        ctx,
                                        parked: Parked {
                                            driver,
                                            session,
                                            sess: Mutex::new(Some(sess)),
                                        },
                                        steps_in_stream,
                                    },
                                );
                            }
                        }
                    }
                }
                Some(PeerMsg::Exit(code)) => {
                    let mid_step = sess.mapper.step_open();
                    if mid_step {
                        // 's honest failure trace: an attempt with no
                        // assistant/message — `interrupted`, never
                        // `completed` (constraint 7).
                        return (
                            LoopOutcome::SoftFailed {
                                reason: format!(
                                    "dsh child exited mid-step (code {code:?}) — interrupted, never completed"
                                ),
                            },
                            St::Done,
                        );
                    }
                    return (LoopOutcome::Settled, St::Done);
                }
                Some(PeerMsg::Io(e)) => {
                    return (
                        LoopOutcome::SoftFailed {
                            reason: format!("dsh child i/o failed: {e}"),
                        },
                        St::Done,
                    );
                }
                None => {
                    return (
                        LoopOutcome::SoftFailed {
                            reason: "dsh child pump vanished".into(),
                        },
                        St::Done,
                    );
                }
            }
        }
    }
}

fn control_closed() -> LoopOutcome {
    LoopOutcome::SoftFailed {
        reason: "dsh child control channel closed".into(),
    }
}

/// Steps already persisted in the stream (the budget counter's
/// cross-episode piece).
async fn count_steps(ctx: &Arc<dyn DshCtx>) -> u64 {
    ctx.events()
        .await
        .map(|events| {
            events
                .iter()
                .filter(|ev| matches!(ev.kind, EventKind::TurnStart))
                .count() as u64
        })
        .unwrap_or(0)
}

fn soft(what: &str, e: DshCtxError) -> LoopOutcome {
    LoopOutcome::SoftFailed {
        reason: format!("dsh bridge {what}: {e}"),
    }
}

/// The `input/admitted` events appended after the last
/// `drain/claimed` — this drain's promoted batch (the engine promotes
/// after appending its claim and before `drive`).
fn fresh_promotions(events: &[Event]) -> Vec<Value> {
    let last_claim = events
        .iter()
        .rposition(|ev| matches!(ev.kind, EventKind::DrainClaimed))
        .map(|idx| idx + 1)
        .unwrap_or(0);
    events[last_claim..]
        .iter()
        .filter(|ev| ev.kind == EventKind::InputAdmitted)
        .map(|ev| ev.bytes.clone())
        .collect()
}

/// Whether a wire line is the dsh episode envelope's `turn/end` (the
/// absorbed frame that ends the child's episode).
fn absorbed_turn_end(line: &str) -> bool {
    matches!(
        parse_peer_frame(line),
        Ok(PeerFrame::SessionEvent(ev)) if ev.kind == "turn/end"
    )
}

/// The engine's input-row convention (mirrors `consumption_plan` in
/// kian-orchestrator): a synthetic row wraps `{"synthetic", "content"}`;
/// the `{content, spill}` envelope admits its message; anything else is
/// the content verbatim.
fn consumption_payload(row_payload: &Value) -> Value {
    if let Some(obj) = row_payload.as_object() {
        if obj.get("synthetic").and_then(Value::as_bool) == Some(true)
            && let Some(content) = obj.get("content")
        {
            return serde_json::json!({"role": "user", "content": content, "synthetic": true});
        }
        if obj.len() == 2
            && let (Some(content), Some(_spill)) = (obj.get("content"), obj.get("spill"))
        {
            return serde_json::json!({"role": "user", "content": content});
        }
    }
    serde_json::json!({"role": "user", "content": row_payload})
}

/// Delivers one admitted user message: plain rows ride `input`,
/// synthetic rows ride `steer` ('s translation of the engine's
/// inbox conventions).
async fn deliver_admitted(
    peer: &mut PeerHandle,
    delivered: &mut BTreeSet<String>,
    admitted: &Value,
    bound: Option<Duration>,
) -> Result<(), String> {
    let key = canon(admitted);
    if delivered.contains(&key) {
        return Ok(());
    }
    let content = admitted.get("content").cloned().unwrap_or(Value::Null);
    let frame = if admitted.get("synthetic").and_then(Value::as_bool) == Some(true) {
        ControlFrame::Steer { content }
    } else {
        ControlFrame::Input { content }
    };
    match within_step_bound(
        bound,
        "dsh input delivery (control write)",
        peer.ctrl_tx.send(frame),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(format!("deliver input: {e}")),
        Err(reason) => return Err(reason),
    }
    delivered.insert(key);
    Ok(())
}

/// Bounds one wait on the dsh child by the configured open-step inactivity bound (ADR-044).
///
/// `step_timeout_ms = 0` disables the bound (the pre-ADR wait). The `Err` is the driver's honest
/// `SoftFailed` reason: the child is unresponsive, never "completed".
async fn within_step_bound<T>(
    bound: Option<Duration>,
    what: &'static str,
    fut: impl std::future::Future<Output = T>,
) -> Result<T, String> {
    let Some(bound) = bound else {
        return Ok(fut.await);
    };
    match tokio::time::timeout(bound, fut).await {
        Ok(value) => Ok(value),
        Err(_) => Err(format!(
            "harness {what}: no progress within the step bound ({} ms, HARNESS-BRIDGE §4.5, \
             ADR-044) — the child is unresponsive; ending honestly",
            bound.as_millis()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumption_payload_mirrors_the_engine_conventions() {
        // Plain row: payload is the content verbatim.
        assert_eq!(
            consumption_payload(&serde_json::json!({"text": "deploy"})),
            serde_json::json!({"role": "user", "content": {"text": "deploy"}})
        );
        // Steer row: unwrapped content plus the strippable marker.
        assert_eq!(
            consumption_payload(&serde_json::json!({"synthetic": true, "content": "plan B"})),
            serde_json::json!({"role": "user", "content": "plan B", "synthetic": true})
        );
        // Spill envelope: the admitted message only.
        assert_eq!(
            consumption_payload(&serde_json::json!({"content": "big", "spill": {"raw": 1}})),
            serde_json::json!({"role": "user", "content": "big"})
        );
    }

    #[test]
    fn dedup_takes_only_what_was_persisted() {
        let mut dedup = Dedup::default();
        dedup.add(EventKind::AssistantMessage, serde_json::json!({"v": 1}));
        assert!(dedup.take(EventKind::AssistantMessage, &serde_json::json!({"v": 1})));
        assert!(!dedup.take(EventKind::AssistantMessage, &serde_json::json!({"v": 1})));
    }
}
