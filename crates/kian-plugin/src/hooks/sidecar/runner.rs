//! The sidecar runner (PLUGIN-SPEC §6.3–§6.5, §7.3–§7.6, §10.3, §11).
//!
//! One dedicated **std** thread per sidecar owns the child process for
//! the server's lifetime: it performs the hardened spawn (§10.3), the
//! `hello`→`ready` handshake with identity checking (§7.3, rules 12/13),
//! and serves dispatches one at a time (§7.5's single in-flight rule —
//! a second dispatch fails fast to `Busy` and never queues). Deadlines
//! are enforced by a watchdog thread that kills the child's whole
//! process group, after which the child is respawned with the digest
//! re-verified and re-handshaked (§6.3). Failures accumulate in §6.4's
//! window (kind-tagged, cleared by any well-formed reply) until they
//! trip quarantine; `frame_overflow` and `protocol_violation` quarantine
//! immediately, one strike (§6.5). Quarantine is one-way for the
//! process lifetime and the instance is skipped at the bus from then
//! on.
//!
//! The async surface is a thin adapter: [`SidecarRunner::hook_fns`]
//! turns the runner into tier-0-shaped handlers whose unreachability
//! maps onto the self-declared `err:deadline` / `err:crashed` replies
//! [`resolve_outcome`](super::super::failure::resolve_outcome) already
//! classifies — so the §11.1 grant-keyed class defaults, the audit
//! lines and the first-deny-wins ordering all come from the one bus
//! code path (K6), never a second classifier.
//!
//! No `libc` edge exists this round (K10 allows only `sha2`), so the
//! group kill shells out to the absolute-path `/bin/kill` (never a
//! shell) and falls back to the leader's `SIGKILL` through std.

use std::collections::VecDeque;
use std::io::Read;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::channel::mpsc::Sender;
use futures::channel::{mpsc, oneshot};
use futures::{FutureExt, StreamExt};

use super::super::failure::Cause;
use super::super::registry::{HookFn, HookPayload, HookReply, ReplyErrCode};
use super::super::wire::sha256_hex;
use super::frame::{
    DecodeError, Hello, HookFrame, ReplyFrame, decode_ready, decode_reply, encode_bye,
};
use super::line::{BoundedLineReader, LineError, write_line};
use super::manifest::{LimitsSection, Manifest, Refusal, RefusalCode, Required};

/// §7.5's graceful-stop grace before the group kill.
const STOP_GRACE: Duration = Duration::from_millis(2_000);
/// Bounded reap after a kill: a stuck group must not hang the reaper
/// (§10.3 step 5).
const REAP_BOUND: Duration = Duration::from_millis(2_000);

// ---------------------------------------------------------------------
// Failure window and quarantine (§6.4, §6.5)
// ---------------------------------------------------------------------

/// The kind tag every window entry carries — the deque is kind-tagged
/// so a tripping window can say whether it contains only stderr-rate
/// entries (§6.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// `err:deadline` or a deadline kill.
    Deadline,
    /// A crash: process exit while a dispatch or handshake was pending.
    Crash,
    /// An `err:malformed` / `err:unauthorized` reply, or an unparseable
    /// line (§6.4's window-counted speech failures).
    ReplyDefect,
    /// The retained stderr-rate bound (§10.5).
    StderrRate,
}

/// §6.5's quarantine reasons — each names the mechanism that produced
/// it (window trip vs. immediate one strike).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuarantineReason {
    /// The §6.4 window tripped with at least one non-stderr entry.
    RestartsExhausted { failures: usize, window_ms: u64 },
    /// The window tripped on stderr-rate entries only.
    StderrRate { failures: usize, window_ms: u64 },
    /// Unknown / duplicate / unsolicited frame id — immediate (§6.5).
    ProtocolViolation { detail: String },
    /// A line over `max_payload_kib` in either direction — immediate.
    FrameOverflow { direction: &'static str },
    /// The binary's digest no longer matches at respawn — immediate
    /// (at boot the same condition is a `plugin/refuse`).
    DigestMismatch,
}

/// The §6.4 state machine: one kind-tagged window plus a reported (not
/// thresholded) respawn counter.
#[derive(Debug, Default)]
pub struct Counters {
    failures: Mutex<VecDeque<(Instant, FailureKind)>>,
    restarts: AtomicU64,
}

impl Counters {
    /// Records a window-counted failure: push `now()`, drop entries
    /// older than the window, trip quarantine at the threshold. A
    /// success (any well-formed reply) clears the deque — the failures
    /// are consecutive by definition (§6.4).
    pub fn record_failure(
        &self,
        kind: FailureKind,
        limits: &LimitsSection,
    ) -> Option<QuarantineReason> {
        let mut failures = self
            .failures
            .lock()
            .expect("kian-plugin: sidecar counters lock poisoned");
        failures.push_back((Instant::now(), kind));
        let window = Duration::from_millis(limits.restart_window_ms);
        while failures
            .front()
            .is_some_and(|(at, _)| at.elapsed() > window)
        {
            failures.pop_front();
        }
        if failures.len() as u64 >= limits.restarts_per_window {
            let only_stderr = failures.iter().all(|(_, k)| *k == FailureKind::StderrRate);
            let count = failures.len();
            return Some(if only_stderr {
                QuarantineReason::StderrRate {
                    failures: count,
                    window_ms: limits.restart_window_ms,
                }
            } else {
                QuarantineReason::RestartsExhausted {
                    failures: count,
                    window_ms: limits.restart_window_ms,
                }
            });
        }
        None
    }

    /// A well-formed reply clears the consecutive-failure window.
    pub fn record_success(&self) {
        self.failures
            .lock()
            .expect("kian-plugin: sidecar counters lock poisoned")
            .clear();
    }

    /// Counts an actual respawn — reported, never thresholded (§6.4).
    pub fn record_restart(&self) -> u64 {
        self.restarts.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn restarts(&self) -> u64 {
        self.restarts.load(Ordering::Relaxed)
    }

    pub fn window_len(&self) -> usize {
        self.failures
            .lock()
            .expect("kian-plugin: sidecar counters lock poisoned")
            .len()
    }
}

// ---------------------------------------------------------------------
// Jobs and outcomes
// ---------------------------------------------------------------------

enum Job {
    Dispatch {
        frame: HookFrame,
        reply: oneshot::Sender<ReplyOutcome>,
    },
    Stop {
        done: oneshot::Sender<()>,
    },
}

/// What one dispatch resolved to, before the adapter shapes it for the
/// bus's classifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplyOutcome {
    /// A decoded reply frame (including `err` replies — the bus
    /// classifies those, not the runner).
    Reply(HookReply),
    /// A protocol violation whose dispatch outcome is as-if-absent
    /// (§7.4): quarantining is the runner's job, deciding is not.
    AsIfAbsent { detail: String },
    /// §7.5's fail-fast: one dispatch is in flight; never queued.
    Busy,
    /// The handler cannot answer — the adapter maps this to the
    /// self-declared unreachability reply so the §11.1 grant-keyed
    /// class default applies.
    Unreachable { cause: Cause, detail: String },
}

// ---------------------------------------------------------------------
// The runner
// ---------------------------------------------------------------------

struct Inner {
    name: String,
    manifest: Manifest,
    canonical_path: String,
    /// The job slot (§7.6: bounded internal channels — one per
    /// sidecar). Mutex'd because the futures sender's `try_send`
    /// needs `&mut`; the in-flight flag below is what actually
    /// serializes dispatches.
    tx: Mutex<Sender<Job>>,
    next_dispatch_id: AtomicU64,
    counters: Counters,
    quarantine: Mutex<Option<QuarantineReason>>,
    /// §7.5's single in-flight rule, enforced before the job channel:
    /// while a dispatch is unanswered the runner is busy, and a second
    /// dispatch **never queues** — the channel alone could not express
    /// that (a buffered slot is a queue).
    in_flight: AtomicBool,
}

/// Resets the in-flight flag when dropped — cancellation-safe across
/// the await point.
struct InFlightGuard<'a>(&'a AtomicBool);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// A running sidecar: one child process, one io thread, one in-flight
/// dispatch. Cheap to clone (an `Arc` inside); [`SidecarRunner::stop`]
/// is the graceful shutdown (`bye`, half-close, bounded grace, group
/// kill — §7.5).
#[derive(Clone)]
pub struct SidecarRunner {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for SidecarRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SidecarRunner")
            .field("name", &self.inner.name)
            .field("quarantined", &self.quarantined())
            .finish_non_exhaustive()
    }
}

/// A watchdog: sleeps the deadline, then — only if the awaited event
/// has not arrived — records the deadline cause and kills the child's
/// process group (§10.3 step 5). The read side consults the cause when
/// its blocking read unblocks, which distinguishes a deadline kill
/// from a crash.
#[derive(Clone)]
struct WatchdogHandle {
    answered: Arc<AtomicBool>,
    deadline_fired: Arc<Mutex<bool>>,
    pid: Arc<AtomicU64>,
}

impl WatchdogHandle {
    fn arm(&self, timeout: Duration) {
        let handle = self.clone();
        std::thread::spawn(move || {
            std::thread::sleep(timeout);
            if !handle.answered.load(Ordering::SeqCst) {
                // Set the cause *before* the kill so the reader that
                // unblocks on the dying pipe sees it (§11.2: the child
                // is killed for a timeout, never left running).
                *handle.deadline_fired.lock().unwrap() = true;
                kill_group(handle.pid.load(Ordering::SeqCst) as u32);
            }
        });
    }

    fn mark_answered(&self) -> bool {
        self.answered.swap(true, Ordering::SeqCst)
    }
}

/// One live child plus its carrier halves.
struct Session {
    child: Child,
    /// `Option` so the graceful stop can half-close (§7.5); `None`
    /// after that is a crash-class unreachability for any late write.
    stdin: Option<ChildStdin>,
    stdout: BoundedLineReader<ChildStdout>,
    pid: u32,
    /// Cleared when the session must not be reused (killed, crashed,
    /// overflowed); the io loop respawns then.
    alive: bool,
}

/// Why a spawn+handshake failed — mapped to boot refusals (§6.2 rules
/// 6/12/13) on the first spawn and to window failures on respawns.
enum HandshakeFailure {
    DigestMismatch,
    Spawn(String),
    Timeout,
    ReadyMismatch(String),
    AbiMismatch,
    Crashed(String),
}

impl SidecarRunner {
    /// Spawns and handshakes one sidecar instance (§6.3's
    /// `configured → spawning → handshaking → ready`). Blocks for at
    /// most the manifest's `startup_timeout_ms` plus slack; a failure
    /// is the typed per-instance refusal the caller audits
    /// (`plugin/refuse`, fatal iff `required` — the caller's policy,
    /// §6.2).
    pub fn start(manifest: Manifest, canonical_path: &std::path::Path) -> Result<Self, Refusal> {
        let canonical =
            std::fs::canonicalize(canonical_path).unwrap_or_else(|_| canonical_path.to_path_buf());
        let name = manifest.plugin.name.clone();
        let (tx, rx) = mpsc::channel(1);
        let inner = Arc::new(Inner {
            name: name.clone(),
            manifest,
            canonical_path: canonical.display().to_string(),
            tx: Mutex::new(tx),
            next_dispatch_id: AtomicU64::new(0),
            counters: Counters::default(),
            quarantine: Mutex::new(None),
            in_flight: AtomicBool::new(false),
        });
        let (handshake_tx, handshake_rx) = std::sync::mpsc::channel::<Result<(), Refusal>>();
        let thread_inner = Arc::clone(&inner);
        let spawn_result = std::thread::Builder::new()
            .name(format!("kian-sidecar-{name}"))
            .spawn(move || io_thread(thread_inner, rx, handshake_tx));
        if spawn_result.is_err() {
            return Err(Refusal {
                code: RefusalCode::SpawnFailed,
                message: format!("could not spawn the io thread for `{name}`"),
                required: inner.manifest.required(),
            });
        }
        let slack = Duration::from_millis(2_000);
        let bound = Duration::from_millis(inner.manifest.process.startup_timeout_ms) + slack;
        match handshake_rx.recv_timeout(bound) {
            Ok(Ok(())) => Ok(Self { inner }),
            Ok(Err(refusal)) => {
                audit_refuse(&refusal);
                Err(refusal)
            }
            Err(_) => Err(Refusal {
                code: RefusalCode::SpawnFailed,
                message: format!(
                    "`{name}`: hello→ready did not complete within startup_timeout_ms"
                ),
                required: inner.manifest.required(),
            }),
        }
    }

    /// The instance's plugin name (audit identity).
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The counters handle (§6.4) — reported, never thresholded
    /// outside the runner.
    pub fn counters(&self) -> &Counters {
        &self.inner.counters
    }

    /// The quarantine reason, if the instance is quarantined (§6.5:
    /// one-way for the process lifetime).
    pub fn quarantined(&self) -> Option<QuarantineReason> {
        self.inner
            .quarantine
            .lock()
            .expect("kian-plugin: quarantine lock poisoned")
            .clone()
    }

    /// Dispatches one frame to the sidecar: fails fast to
    /// [`ReplyOutcome::Busy`] while a dispatch is in flight (§7.5 —
    /// never queued), resolves unreachability to the cause the adapter
    /// turns into the §11.1 class default.
    pub async fn dispatch(&self, frame: HookFrame) -> ReplyOutcome {
        if let Some(reason) = self.quarantined() {
            return ReplyOutcome::Unreachable {
                cause: Cause::Quarantine,
                detail: format!("quarantined: {reason:?}"),
            };
        }
        if self.inner.in_flight.swap(true, Ordering::SeqCst) {
            return ReplyOutcome::Busy;
        }
        let _in_flight = InFlightGuard(&self.inner.in_flight);
        let (reply_tx, reply_rx) = oneshot::channel();
        let sent = self
            .inner
            .tx
            .lock()
            .expect("kian-plugin: sidecar job-slot lock poisoned")
            .try_send(Job::Dispatch {
                frame,
                reply: reply_tx,
            });
        match sent {
            Ok(()) => match reply_rx.await {
                Ok(outcome) => outcome,
                Err(_) => ReplyOutcome::Unreachable {
                    cause: Cause::Crash,
                    detail: "sidecar stopped".to_string(),
                },
            },
            // With the in-flight flag held, a send failure means the
            // receiver is gone — quarantined or stopped (the futures
            // `TrySendError` does not distinguish; the quarantine
            // check above already reported the reason if there is
            // one).
            Err(_) => ReplyOutcome::Unreachable {
                cause: Cause::Quarantine,
                detail: "sidecar io thread exited (quarantined or stopped)".to_string(),
            },
        }
    }

    /// Graceful stop (§7.5): `bye`, close stdin (half-close), wait a
    /// bounded grace period, then kill the whole process group. Blocks
    /// for at most the grace; call at shutdown, not from async code.
    pub fn stop(&self) {
        let (done_tx, done_rx) = oneshot::channel();
        let sent = self
            .inner
            .tx
            .lock()
            .expect("kian-plugin: sidecar job-slot lock poisoned")
            .try_send(Job::Stop { done: done_tx });
        if sent.is_ok() {
            let _ = futures::executor::block_on(done_rx);
        }
    }

    /// A fresh dispatch id. Process-unique per sidecar (§7.3 wants a
    /// fresh uuid; K10 forbids the `uuid` edge this round, so the id
    /// is the sidecar-tagged counter the same way the tier-0 bus ids
    /// are — uniqueness and reply attribution are what the protocol
    /// needs).
    fn next_dispatch_id(&self) -> String {
        format!(
            "sc-{}-{}",
            self.inner.name,
            self.inner.next_dispatch_id.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Tier-0-shaped handlers for the bus: one per subscribed hook,
    /// carrying the manifest's grants and order. The plugin name is
    /// leaked for the `&'static str` the tier-0 `HandlerId` requires —
    /// one process per plugin per server lifetime (R11), so the leak
    /// is bounded by the instance count.
    pub fn hook_fns(self: &Arc<Self>) -> Vec<(super::super::registry::HandlerId, HookFn)> {
        let mut out = Vec::new();
        for hook in self.inner.manifest.grants.hooks.clone() {
            let plugin: &'static str = Box::leak(self.inner.name.clone().into_boxed_str());
            let runner = Arc::clone(self);
            let hook_for_closure = hook.clone();
            let deadline_ms = self.inner.manifest.limits.deadline_ms;
            let handler = super::super::registry::HandlerId {
                plugin,
                hook,
                grants: self.inner.manifest.grants.grants.clone(),
                order: self.inner.manifest.plugin.order,
            };
            let f: HookFn = Arc::new(move |payload: &HookPayload| {
                let runner = Arc::clone(&runner);
                // Extract everything borrowed before the async block.
                let frame_info = payload.frame().cloned();
                let payload_value = payload_value(payload);
                let dispatch_id = runner.next_dispatch_id();
                let hook = hook_for_closure.clone();
                async move {
                    let Some(frame_info) = frame_info else {
                        tracing::warn!(
                            target: "kian::hook",
                            hook = %hook,
                            plugin = plugin,
                            "sidecar dispatch without frame identity — skipped (R11: no implicit identity)"
                        );
                        return HookReply::Ack { note: None };
                    };
                    let hook_for_log = hook.clone();
                    let frame = match HookFrame::new(
                        hook,
                        &frame_info,
                        dispatch_id,
                        deadline_ms,
                        payload_value,
                    ) {
                        Ok(frame) => frame,
                        Err(e) => {
                            // The epoch rule's local guard (§7.2): the
                            // call site built an incoherent frame. Skip
                            // the sidecar rather than deny on a
                            // harness-side defect.
                            tracing::error!(
                                target: "kian::hook",
                                hook = %hook_for_log,
                                plugin = plugin,
                                error = %e.message,
                                "sidecar frame rejected — dispatch skipped"
                            );
                            return HookReply::Ack { note: None };
                        }
                    };
                    match runner.dispatch(frame).await {
                        ReplyOutcome::Reply(reply) => reply,
                        ReplyOutcome::AsIfAbsent { .. } => {
                            tracing::info!(
                                target: "kian::hook",
                                hook = %hook_for_log,
                                plugin = plugin,
                                outcome = "discarded",
                                "hook-discard"
                            );
                            HookReply::Ack { note: None }
                        }
                        ReplyOutcome::Busy => {
                            // §7.5's busy audit line — never queued.
                            tracing::info!(
                                target: "kian::hook",
                                hook = %hook_for_log,
                                plugin = plugin,
                                busy = true,
                                "hook-default"
                            );
                            HookReply::Err {
                                code: ReplyErrCode::Crashed,
                                message: "busy: one dispatch in flight".to_string(),
                            }
                        }
                        ReplyOutcome::Unreachable { cause, detail } => HookReply::Err {
                            code: match cause {
                                Cause::Deadline => ReplyErrCode::Deadline,
                                _ => ReplyErrCode::Crashed,
                            },
                            message: format!("{detail} ({})", cause.as_str()),
                        },
                    }
                }
                .boxed()
            });
            out.push((handler, f));
        }
        out
    }
}

// ---------------------------------------------------------------------
// The io thread
// ---------------------------------------------------------------------

fn io_thread(
    inner: Arc<Inner>,
    mut rx: mpsc::Receiver<Job>,
    handshake: std::sync::mpsc::Sender<Result<(), Refusal>>,
) {
    let mut session = match spawn_session(&inner, true) {
        Ok(session) => {
            let _ = handshake.send(Ok(()));
            session
        }
        Err(failure) => {
            let refusal = boot_refusal(&inner, failure);
            let _ = handshake.send(Err(refusal.clone()));
            audit_refuse(&refusal);
            return;
        }
    };
    loop {
        if inner.quarantine_locked().is_some() {
            // §6.5: a quarantined instance is skipped at the bus and
            // never respawns; the io thread retires.
            return;
        }
        let job = match futures::executor::block_on(rx.next()) {
            Some(job) => job,
            None => {
                graceful_stop(&mut session);
                return;
            }
        };
        match job {
            Job::Stop { done } => {
                graceful_stop(&mut session);
                let _ = done.send(());
                return;
            }
            Job::Dispatch { frame, reply } => {
                let outcome = serve_dispatch(&inner, &mut session, frame);
                let _ = reply.send(outcome);
                if !session.alive && inner.quarantine_locked().is_none() {
                    // §6.3: respawn after a crash or deadline kill,
                    // inside the failure window, digest re-verified,
                    // re-handshaked before the next dispatch.
                    session = match respawn_until_ready(&inner) {
                        Some(session) => session,
                        None => return, // quarantined (or window tripped)
                    };
                }
            }
        }
    }
}

/// Serves exactly one dispatch on a live session (§7.5: the io thread
/// is the serialization point; nothing else touches the child).
fn serve_dispatch(inner: &Arc<Inner>, session: &mut Session, frame: HookFrame) -> ReplyOutcome {
    let max = inner.manifest.limits.max_frame_bytes();
    let line = frame.encode();
    let Some(stdin) = session.stdin.as_mut() else {
        record_window_failure(inner, FailureKind::Crash);
        kill_session(session);
        return ReplyOutcome::Unreachable {
            cause: Cause::Crash,
            detail: "stdin already closed (stop in progress)".to_string(),
        };
    };
    match write_line(stdin, &line, max) {
        Err(LineError::Overflow { scanned, max }) => {
            quarantine(
                inner,
                QuarantineReason::FrameOverflow {
                    direction: "outbound",
                },
            );
            kill_session(session);
            return ReplyOutcome::Unreachable {
                cause: Cause::Crash,
                detail: format!("frame_overflow: outbound line {scanned} > {max} bytes"),
            };
        }
        Err(LineError::Io(e)) => {
            // The write itself failed: a crash.
            if let Some(reason) = record_window_failure(inner, FailureKind::Crash) {
                quarantine(inner, reason);
            }
            kill_session(session);
            return ReplyOutcome::Unreachable {
                cause: Cause::Crash,
                detail: format!("stdin write failed: {e}"),
            };
        }
        Ok(()) => {}
    }

    let watchdog = WatchdogHandle {
        answered: Arc::new(AtomicBool::new(false)),
        deadline_fired: Arc::new(Mutex::new(false)),
        pid: Arc::new(AtomicU64::new(u64::from(session.pid))),
    };
    watchdog.arm(Duration::from_millis(inner.manifest.limits.deadline_ms));

    match session.stdout.read_line(max) {
        Err(LineError::Overflow { scanned, max }) => {
            // §7.6: immediate one-strike quarantine, and the class
            // default for this dispatch — never a window increment.
            quarantine(
                inner,
                QuarantineReason::FrameOverflow {
                    direction: "inbound",
                },
            );
            tracing::info!(
                target: "kian::hook",
                plugin = %inner.name,
                direction = "inbound",
                scanned = scanned,
                max = max,
                "hook-quarantine-overflow"
            );
            kill_session(session);
            ReplyOutcome::Unreachable {
                cause: Cause::Crash,
                detail: format!("frame_overflow: inbound line {scanned} > {max} bytes"),
            }
        }
        Err(LineError::Io(e)) => {
            let deadline = *watchdog.deadline_fired.lock().unwrap();
            kill_session(session);
            let kind = if deadline {
                FailureKind::Deadline
            } else {
                // EOF without the expected reply is a crash, never an
                // empty allow (§7.5).
                FailureKind::Crash
            };
            if let Some(reason) = record_window_failure(inner, kind) {
                // The window tripped with this very failure (§6.4).
                quarantine(inner, reason);
            }
            if deadline {
                ReplyOutcome::Unreachable {
                    cause: Cause::Deadline,
                    detail: "deadline: child killed over the dispatch deadline".to_string(),
                }
            } else {
                ReplyOutcome::Unreachable {
                    cause: Cause::Crash,
                    detail: format!("crash: child pipe closed ({e})"),
                }
            }
        }
        Ok(line) => {
            watchdog.mark_answered();
            match decode_reply(&line) {
                Err(DecodeError::Malformed(detail)) => {
                    // An unparseable reply is the err:malformed class
                    // — discarded as an opinion at the bus and
                    // window-counted here (§6.4, §11.1).
                    record_window_failure(inner, FailureKind::ReplyDefect);
                    ReplyOutcome::Reply(HookReply::Err {
                        code: ReplyErrCode::Malformed,
                        message: detail,
                    })
                }
                Ok((reply_id, reply_frame)) => {
                    let id_ok = reply_id.as_ref().map(|id| id == &frame.id).unwrap_or(true); // id-less err: the one in-flight
                    if !id_ok {
                        // §7.4: a second/unsolicited reply is an
                        // immediate quarantine, and this dispatch is
                        // as-if-absent.
                        quarantine(
                            inner,
                            QuarantineReason::ProtocolViolation {
                                detail: format!(
                                    "reply id `{reply_id:?}` ≠ dispatch id `{}`",
                                    frame.id
                                ),
                            },
                        );
                        kill_session(session);
                        return ReplyOutcome::AsIfAbsent {
                            detail: "protocol_violation: reply for another dispatch id".to_string(),
                        };
                    }
                    match reply_frame {
                        ReplyFrame::Err { code, message }
                            if matches!(
                                code,
                                ReplyErrCode::Deadline
                                    | ReplyErrCode::Malformed
                                    | ReplyErrCode::Unauthorized
                            ) =>
                        {
                            // §6.4's window-counted err replies.
                            let kind = if code == ReplyErrCode::Deadline {
                                FailureKind::Deadline
                            } else {
                                FailureKind::ReplyDefect
                            };
                            if let Some(reason) = record_window_failure(inner, kind) {
                                quarantine(inner, reason);
                                kill_session(session);
                            }
                            ReplyOutcome::Reply(HookReply::Err { code, message })
                        }
                        other => {
                            // Any well-formed reply is a success — it
                            // clears the consecutive window (§6.4).
                            inner.counters.record_success();
                            ReplyOutcome::Reply(frame_to_reply(other))
                        }
                    }
                }
            }
        }
    }
}

fn frame_to_reply(frame: ReplyFrame) -> HookReply {
    match frame {
        ReplyFrame::Ack { note } => HookReply::Ack { note },
        ReplyFrame::FlowNext => HookReply::FlowNext,
        ReplyFrame::FlowStop { reason } => HookReply::FlowStop { reason },
        ReplyFrame::BailNone => HookReply::BailNone,
        ReplyFrame::BailDeny { reason } => HookReply::BailDeny { reason },
        ReplyFrame::Err { code, message } => HookReply::Err { code, message },
    }
}

/// Respawns after a crash/deadline kill until the child is ready again
/// or the failure window trips quarantine (§6.3, §6.4) — each failed
/// attempt counts, so the loop is bounded by `restarts_per_window`.
fn respawn_until_ready(inner: &Arc<Inner>) -> Option<Session> {
    loop {
        match spawn_session(inner, false) {
            Ok(session) => {
                let restarts = inner.counters.record_restart();
                tracing::info!(
                    target: "kian::hook",
                    plugin = %inner.name,
                    restarts = restarts,
                    digest_ok = true,
                    "hook-respawn"
                );
                return Some(session);
            }
            Err(HandshakeFailure::DigestMismatch) => {
                // §6.5: digest mismatch at respawn — immediate.
                quarantine(inner, QuarantineReason::DigestMismatch);
                return None;
            }
            Err(_) => {
                if let Some(reason) = record_window_failure(inner, FailureKind::Crash) {
                    quarantine(inner, reason);
                    return None;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------
// Spawn + handshake (§10.3, §7.3)
// ---------------------------------------------------------------------

fn spawn_session(inner: &Arc<Inner>, first: bool) -> Result<Session, HandshakeFailure> {
    let manifest = &inner.manifest;
    // §10.3 step 6: digest verification at spawn and at every respawn.
    match digest_of(&manifest.process.argv[0]) {
        Some(digest) if digest == manifest.process.sha256 => {}
        Some(_) => return Err(HandshakeFailure::DigestMismatch),
        None => {
            return Err(HandshakeFailure::Spawn(format!(
                "argv[0] `{}` unreadable for digest verification",
                manifest.process.argv[0]
            )));
        }
    }

    let mut command = Command::new(&manifest.process.argv[0]);
    command
        .args(&manifest.process.argv[1..])
        // §10.3 step 2: env_clear first, then exactly env_allow with
        // the server's values — no PATH, HOME, proxies or credentials.
        .env_clear();
    for name in &manifest.process.env_allow {
        if let Ok(value) = std::env::var(name) {
            command.env(name, value);
        }
    }
    // §10.3 step 3: dedicated cwd (the manifest's, or a fresh empty
    // directory under the server's own temp — never an inherited one).
    let _owned_cwd;
    match &manifest.process.cwd {
        Some(cwd) => {
            command.current_dir(cwd);
        }
        None => {
            let dir = std::env::temp_dir().join(format!(
                "kian-sidecar-{}-{}",
                manifest.plugin.name,
                std::process::id()
            ));
            if std::fs::create_dir_all(&dir).is_err() {
                return Err(HandshakeFailure::Spawn(format!(
                    "could not create the runtime cwd `{}`",
                    dir.display()
                )));
            }
            _owned_cwd = dir;
            command.current_dir(&_owned_cwd);
        }
    }
    // §10.3 step 4: pipe exactly stdin/stdout/stderr; step 5: a
    // killable process group.
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command
        .spawn()
        .map_err(|e| HandshakeFailure::Spawn(format!("spawn failed: {e}")))?;
    let pid = child.id();
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| HandshakeFailure::Spawn("stdin pipe missing".to_string()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| HandshakeFailure::Spawn("stdout pipe missing".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| HandshakeFailure::Spawn("stderr pipe missing".to_string()))?;

    // §7.6/§10.5: stderr is drained concurrently, bounded, tagged,
    // never parsed, never logged with content.
    let stderr_inner = Arc::clone(inner);
    let _stderr_thread = std::thread::Builder::new()
        .name(format!("kian-sidecar-{}-stderr", manifest.plugin.name))
        .spawn(move || drain_stderr(stderr_inner, stderr));

    let mut reader = BoundedLineReader::new(stdout);
    let hello = Hello {
        manifest: inner.canonical_path.clone(),
        name: manifest.plugin.name.clone(),
        version: manifest.plugin.version.clone(),
        abi: manifest.plugin.abi,
        hooks: manifest
            .grants
            .hooks
            .iter()
            .map(|h| h.to_string())
            .collect(),
        capabilities: manifest
            .grants
            .grants
            .iter()
            .map(|g| g.as_str().to_string())
            .collect(),
        deadline_default_ms: manifest.limits.deadline_ms,
        max_frame_bytes: manifest.limits.max_frame_bytes(),
    };
    let max = manifest.limits.max_frame_bytes();
    match write_line(&mut stdin, &hello.encode(), max) {
        Err(LineError::Overflow { .. }) => {
            kill_child(&mut child);
            return Err(HandshakeFailure::ReadyMismatch(
                "hello over the frame bound".to_string(),
            ));
        }
        Err(LineError::Io(e)) => {
            kill_child(&mut child);
            return Err(HandshakeFailure::Crashed(format!(
                "hello write failed: {e}"
            )));
        }
        Ok(()) => {}
    }

    let watchdog = WatchdogHandle {
        answered: Arc::new(AtomicBool::new(false)),
        deadline_fired: Arc::new(Mutex::new(false)),
        pid: Arc::new(AtomicU64::new(u64::from(pid))),
    };
    // Rule 13 (§6.2): hello→ready within startup_timeout_ms.
    watchdog.arm(Duration::from_millis(manifest.process.startup_timeout_ms));

    let ready_line = match reader.read_line(max) {
        Ok(line) => {
            watchdog.mark_answered();
            line
        }
        Err(LineError::Overflow { .. }) => {
            kill_child(&mut child);
            return Err(HandshakeFailure::ReadyMismatch(
                "ready over the frame bound".to_string(),
            ));
        }
        Err(LineError::Io(e)) => {
            let deadline = *watchdog.deadline_fired.lock().unwrap();
            kill_child(&mut child);
            return Err(if deadline {
                HandshakeFailure::Timeout
            } else {
                HandshakeFailure::Crashed(format!("child closed stdout before ready ({e})"))
            });
        }
    };
    let ready = match decode_ready(&ready_line) {
        Ok(ready) => ready,
        Err(DecodeError::Malformed(detail)) => {
            kill_child(&mut child);
            return Err(HandshakeFailure::ReadyMismatch(detail));
        }
    };
    // §7.3 step 2: the ready must restate the identity exactly, with
    // set-equal hooks/capabilities — typed refusals otherwise.
    if ready.abi != manifest.plugin.abi {
        kill_child(&mut child);
        return Err(HandshakeFailure::AbiMismatch);
    }
    if ready.manifest != inner.canonical_path
        || ready.name != manifest.plugin.name
        || ready.version != manifest.plugin.version
        || !set_equal(&ready.hooks, &hello.hooks)
        || !set_equal(&ready.capabilities, &hello.capabilities)
    {
        kill_child(&mut child);
        return Err(HandshakeFailure::ReadyMismatch(format!(
            "ready restates a different identity (manifest={}, name={}, version={}, hooks/capabilities sets differ)",
            ready.manifest, ready.name, ready.version
        )));
    }
    let _ = first; // boot refusal mapping happens in the caller
    Ok(Session {
        child,
        stdin: Some(stdin),
        stdout: reader,
        pid,
        alive: true,
    })
}

/// §10.5's stderr discipline: per-line bound (drop + one count per
/// drop-burst), retained-rate bound (one count per minute), all
/// content discarded — never parsed, never logged.
///
/// A *drop burst* is a run of oversized lines not separated by more
/// than a second: coalescing is meant to keep one fragmented huge
/// line from counting many times, not to make an endless flood count
/// once (§6.5's stderr-only window mechanism requires the flood to be
/// able to trip it).
fn drain_stderr(inner: Arc<Inner>, stderr: std::process::ChildStderr) {
    let limits = inner.manifest.limits;
    let line_max = (limits.max_stderr_line_kib as usize) * 1024;
    let rate_max = (limits.stderr_rate_kib_per_min as usize) * 1024;
    let mut reader = BoundedLineReader::new(stderr);
    let mut last_drop: Option<Instant> = None;
    let mut retained: VecDeque<(Instant, usize)> = VecDeque::new();
    let mut retained_bytes = 0usize;
    let mut last_rate_note: Option<Instant> = None;
    loop {
        match reader.read_line(line_max) {
            Ok(line) => {
                last_drop = None; // an in-bounds line ends any burst
                let now = Instant::now();
                retained.push_back((now, line.len()));
                retained_bytes = retained_bytes.saturating_add(line.len());
                while retained
                    .front()
                    .is_some_and(|(at, _)| now.duration_since(*at) > Duration::from_secs(60))
                {
                    let (_, size) = retained.pop_front().unwrap();
                    retained_bytes = retained_bytes.saturating_sub(size);
                }
                if retained_bytes > rate_max
                    && last_rate_note
                        .is_none_or(|at| now.duration_since(at) > Duration::from_secs(60))
                {
                    last_rate_note = Some(now);
                    tracing::info!(
                        target: "kian::hook",
                        plugin = %inner.name,
                        kind = "stderr_rate",
                        "stderr rate bound exceeded — excess dropped"
                    );
                    if let Some(reason) = record_window_failure(&inner, FailureKind::StderrRate) {
                        quarantine(&inner, reason);
                        return;
                    }
                }
            }
            Err(LineError::Overflow { scanned, max }) => {
                // Drop the oversized line; count at most one
                // stderr_overflow per second — a fragmented huge line
                // counts once, an endless flood counts once per second
                // and can trip the window (§6.4, §6.5).
                tracing::info!(
                    target: "kian::hook",
                    plugin = %inner.name,
                    kind = "stderr_line",
                    scanned = scanned,
                    max = max,
                    "stderr line over bound — dropped"
                );
                let now = Instant::now();
                if last_drop.is_none_or(|at| now.duration_since(at) >= Duration::from_secs(1)) {
                    // Stamp only when counting: a drop that does not
                    // count must not push the window forward.
                    last_drop = Some(now);
                    if let Some(reason) = record_window_failure(&inner, FailureKind::StderrRate) {
                        quarantine(&inner, reason);
                        return;
                    }
                }
                if !reader.discard_line() {
                    return;
                }
            }
            Err(LineError::Io(_)) => return, // EOF: the child is gone
        }
    }
}

fn set_equal(a: &[String], b: &[String]) -> bool {
    let mut left: Vec<&String> = a.iter().collect();
    let mut right: Vec<&String> = b.iter().collect();
    left.sort();
    right.sort();
    left == right
}

fn digest_of(path: &str) -> Option<String> {
    let mut file = std::fs::File::open(path).ok()?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).ok()?;
    Some(sha256_hex(&buf))
}

// ---------------------------------------------------------------------
// Quarantine, kills, stops
// ---------------------------------------------------------------------

impl Inner {
    fn quarantine_locked(&self) -> Option<QuarantineReason> {
        self.quarantine
            .lock()
            .expect("kian-plugin: quarantine lock poisoned")
            .clone()
    }
}

/// Quarantines the instance (§6.5): one-way for the process lifetime,
/// one `hook-quarantine` audit line with name, reason, failure count
/// and window. There is no un-quarantine surface in v1 (D3).
fn quarantine(inner: &Arc<Inner>, reason: QuarantineReason) {
    let mut slot = inner
        .quarantine
        .lock()
        .expect("kian-plugin: quarantine lock poisoned");
    if slot.is_some() {
        return; // the first reason stands
    }
    let failures = inner.counters.window_len();
    let window_ms = inner.manifest.limits.restart_window_ms;
    tracing::info!(
        target: "kian::hook",
        plugin = %inner.name,
        reason = %reason.as_audit_str(),
        failures = failures,
        window_ms = window_ms,
        lifetime = "server-process",
        "hook-quarantine"
    );
    *slot = Some(reason);
}

impl QuarantineReason {
    /// The audit-line spelling.
    pub fn as_audit_str(&self) -> String {
        match self {
            QuarantineReason::RestartsExhausted {
                failures,
                window_ms,
            } => {
                format!("restarts_exhausted(failures={failures}, window_ms={window_ms})")
            }
            QuarantineReason::StderrRate {
                failures,
                window_ms,
            } => {
                format!("stderr_overflow(failures={failures}, window_ms={window_ms})")
            }
            QuarantineReason::ProtocolViolation { detail } => {
                format!("protocol_violation({detail})")
            }
            QuarantineReason::FrameOverflow { direction } => {
                format!("frame_overflow({direction})")
            }
            QuarantineReason::DigestMismatch => "digest_mismatch(respawn)".to_string(),
        }
    }
}

/// Records a window-counted failure and quarantines when the window
/// trips (§6.4).
fn record_window_failure(inner: &Arc<Inner>, kind: FailureKind) -> Option<QuarantineReason> {
    inner.counters.record_failure(kind, &inner.manifest.limits)
}

fn kill_session(session: &mut Session) {
    session.alive = false;
    kill_group(session.pid);
    let _ = session.child.kill();
    reap_bounded(&mut session.child);
}

fn kill_child(child: &mut Child) {
    let pid = child.id();
    kill_group(pid);
    let _ = child.kill();
    reap_bounded(child);
}

/// Kills the child's whole process group (§10.3 step 5). Without a
/// `libc` edge (K10) the group signal goes through the absolute-path
/// `/bin/kill` — a direct binary, never a shell; the leader itself is
/// always killed through std afterwards regardless.
fn kill_group(pid: u32) {
    if pid == 0 {
        return;
    }
    #[cfg(unix)]
    {
        let target = format!("-{pid}");
        for killer in ["/bin/kill", "/usr/bin/kill"] {
            if Command::new(killer)
                .arg("-9")
                .arg(&target)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok()
            {
                return;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
    }
}

fn reap_bounded(child: &mut Child) {
    let deadline = Instant::now() + REAP_BOUND;
    while Instant::now() < deadline {
        match child.try_wait() {
            Ok(Some(_)) | Err(_) => return,
            Ok(None) => std::thread::sleep(Duration::from_millis(10)),
        }
    }
    tracing::warn!(
        target: "kian::hook",
        pid = child.id(),
        "sidecar child not reaped within the bounded grace"
    );
}

/// §7.5's graceful stop: `bye`, half-close stdin, bounded grace, then
/// kill the whole group. EOF without the expected reply was already
/// handled as a crash during dispatch; this is the orderly path.
fn graceful_stop(session: &mut Session) {
    if let Some(mut stdin) = session.stdin.take() {
        let _ = write_line(&mut stdin, &encode_bye(), 64);
    }
    // The take above already half-closed stdin when there was one.
    let deadline = Instant::now() + STOP_GRACE;
    while Instant::now() < deadline {
        match session.child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => std::thread::sleep(Duration::from_millis(20)),
            Err(_) => return,
        }
    }
    kill_session(session);
}

fn boot_refusal(inner: &Arc<Inner>, failure: HandshakeFailure) -> Refusal {
    let required = inner.manifest.required();
    let refusal = |code, message| Refusal {
        code,
        message,
        required,
    };
    match failure {
        HandshakeFailure::DigestMismatch => refusal(
            RefusalCode::DigestMismatch,
            format!(
                "`{}`: argv[0] digest does not match the manifest sha256 at spawn",
                inner.name
            ),
        ),
        HandshakeFailure::Spawn(detail) => refusal(
            RefusalCode::SpawnFailed,
            format!("`{}`: {}", inner.name, detail),
        ),
        HandshakeFailure::Timeout => refusal(
            RefusalCode::SpawnFailed,
            format!(
                "`{}`: hello→ready exceeded startup_timeout_ms (rule 13)",
                inner.name
            ),
        ),
        HandshakeFailure::ReadyMismatch(detail) => refusal(
            RefusalCode::ReadyMismatch,
            format!("`{}`: {detail}", inner.name),
        ),
        HandshakeFailure::AbiMismatch => refusal(
            RefusalCode::AbiMismatch,
            format!(
                "`{}`: ready restates a different abi than the manifest",
                inner.name
            ),
        ),
        HandshakeFailure::Crashed(detail) => refusal(
            RefusalCode::SpawnFailed,
            format!("`{}`: {detail}", inner.name),
        ),
    }
}

/// §10.6's `hook-refuse` audit line.
fn audit_refuse(refusal: &Refusal) {
    tracing::info!(
        target: "kian::hook",
        code = refusal.code.as_str(),
        message = %refusal.message,
        required = match refusal.required {
            Required::True => "true",
            Required::False => "false",
            Required::Unknown => "unknown",
        },
        "hook-refuse"
    );
}

/// Serializes a payload view for the wire (§7.2's `payload` is the
/// §3.2 view; the in-process `frame` identity is skipped by serde and
/// rides the frame envelope instead).
fn payload_value(payload: &HookPayload) -> serde_json::Value {
    use HookPayload as P;
    let result = match payload {
        P::AdmitDraft(v) => serde_json::to_value(v),
        P::AdmitReceipt(v) => serde_json::to_value(v),
        P::TurnRequest(v) => serde_json::to_value(v),
        P::TurnResponse(v) => serde_json::to_value(v),
        P::ToolPre(v) => serde_json::to_value(v),
        P::ToolPost(v) => serde_json::to_value(v),
        P::Recovery(v) => serde_json::to_value(v),
        P::Settle(v) => serde_json::to_value(v),
    };
    result.unwrap_or(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> LimitsSection {
        LimitsSection {
            deadline_ms: 500,
            max_payload_kib: 256,
            restarts_per_window: 3,
            restart_window_ms: 60_000,
            max_settle_vetoes: 0,
            max_stderr_line_kib: 8,
            stderr_rate_kib_per_min: 1024,
        }
    }

    #[test]
    fn window_trips_at_the_threshold_and_tags_the_reason() {
        let counters = Counters::default();
        let limits = limits();
        for _ in 0..2 {
            assert!(
                counters
                    .record_failure(FailureKind::Crash, &limits)
                    .is_none()
            );
        }
        let reason = counters
            .record_failure(FailureKind::Deadline, &limits)
            .expect("third failure trips the window");
        assert_eq!(
            reason,
            QuarantineReason::RestartsExhausted {
                failures: 3,
                window_ms: 60_000
            }
        );
        // A success clears the window — the failures are consecutive.
        counters.record_success();
        for _ in 0..2 {
            assert!(
                counters
                    .record_failure(FailureKind::Crash, &limits)
                    .is_none()
            );
        }
    }

    #[test]
    fn stderr_only_window_names_stderr_overflow() {
        let counters = Counters::default();
        let limits = limits();
        for _ in 0..2 {
            assert!(
                counters
                    .record_failure(FailureKind::StderrRate, &limits)
                    .is_none()
            );
        }
        let reason = counters
            .record_failure(FailureKind::StderrRate, &limits)
            .expect("stderr-only window trips");
        assert!(matches!(
            reason,
            QuarantineReason::StderrRate { failures: 3, .. }
        ));
        // A mixed window names restarts_exhausted, not stderr.
        let counters = Counters::default();
        counters.record_failure(FailureKind::StderrRate, &limits);
        for _ in 0..2 {
            counters.record_failure(FailureKind::Crash, &limits);
        }
        // (3 total with at least one crash entry)
    }

    #[test]
    fn restarts_are_counted_not_thresholded() {
        let counters = Counters::default();
        assert_eq!(counters.record_restart(), 1);
        assert_eq!(counters.record_restart(), 2);
        assert_eq!(counters.restarts(), 2);
        assert_eq!(counters.window_len(), 0, "restarts never touch the window");
    }
}
